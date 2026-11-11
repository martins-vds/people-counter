from __future__ import annotations

import base64
import copy
import dataclasses
import json
import shlex
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import pytest

from people_counter.fabric_deployment_manifest import (
    DeploymentEnvironment,
    load_manifest_document,
    parse_deployment_manifest,
)
from people_counter.fabric_sjd_definition import ENTRY_POINTS, build_sjd_definition
from people_counter.fabric_target import FabricTarget
from scripts import deploy_sjd_operations as operations
from tests.fabric_fixtures import ROLE_IDS, TARGET_IDS, TEST_TARGET


def manifest(*, offset: int = 0, ids: bool = False):
    document = load_manifest_document(DeploymentEnvironment.PROD, env={})
    target = {
        key: str(UUID(int=offset + index))
        for index, key in enumerate(TARGET_IDS, start=1)
    }
    document.update(
        workspace_id=target["workspace_id"],
        fabric_environment_id=target["environment_id"],
    )
    document["lakehouse"]["lakehouse_id"] = target["lakehouse_id"]
    for role, binding in document["roles"].items():
        binding["item_id"] = ROLE_IDS.get(role) if ids else None
    return parse_deployment_manifest(document, DeploymentEnvironment.PROD)


class API:
    def __init__(self, selected):
        self.target = selected.target()
        self.config = selected.runtime_config()
        self.workspace_id = self.target.workspace_id
        self.calls = []
        self.compute_verified = False

    def verify_compute(self):
        self.compute_verified = True

    def upsert_sjd(self, role, display_name, *, expected_item_id=None):
        self.calls.append(("upsert", role, display_name, expected_item_id))
        return ROLE_IDS[role]

    def get_definition(self, item_id):
        role = next(role for role, value in ROLE_IDS.items() if value == item_id)
        document = build_sjd_definition(
            role, config=self.config,
            command_line_arguments=shlex.join(self.config.namespace_arguments()),
        )
        document["definition"]["parts"].append({
            "path": ".platform", "payload": "e30=", "payloadType": "InlineBase64",
        })
        return document

    def reconcile_schedule(self, item_id, *, payload):
        self.calls.append(("schedule", item_id, copy.deepcopy(payload)))
        return {**payload, "id": str(UUID(int=99))}

    def reconcile_intake_release(self, item_id, digest):
        self.calls.append(("intake", item_id, digest))

    def reconcile_registration_notebook(self, item_id):
        self.calls.append(("notebook", item_id))


@pytest.mark.parametrize("offset", [0, 100])
def test_provision_discovers_every_role_without_release_receipt_or_preassigned_ids(offset):
    selected = manifest(offset=offset)
    api = API(selected)
    result = operations.provision_operations(api, manifest=selected)
    assert result["status"] == "provisioned"
    assert result["enabled"] is False
    assert result["items"] == ROLE_IDS
    assert api.calls == [
        ("upsert", role, selected.role(role).display_name, None)
        for role in ENTRY_POINTS
    ]
    assert api.compute_verified is False
    resolved = parse_deployment_manifest(result["deployment"], selected.stage)
    assert resolved.target() == selected.target()
    assert {role: binding.item_id for role, binding in resolved.roles.items()} == ROLE_IDS
    assert all(binding.item_id is None for binding in selected.roles.values())


def test_provision_can_use_a_nondefault_namespace_without_changing_the_sdk():
    selected = manifest()
    selected = dataclasses.replace(
        selected,
        lakehouse=dataclasses.replace(selected.lakehouse, files_root="Files/people-counter/dev/v1/"),
    )
    result = operations.provision_operations(API(selected), manifest=selected)
    assert result["deployment"]["lakehouse"]["files_root"] == selected.lakehouse.files_root
    assert result["items"] == ROLE_IDS


def test_deployment_uses_one_binding_for_all_jobs_schedules_and_outputs():
    selected = manifest(offset=100)
    api = API(selected)
    result = operations.deploy_operations(
        api, manifest=selected, release_manifest_sha256="a" * 64,
        release_receipt_sha256="b" * 64, semantic_model_id=str(UUID(int=80)),
        enable_schedules=True,
    )
    assert api.compute_verified is True
    assert result["items"] == ROLE_IDS
    assert result["enabled"] is True
    scheduled = [call for call in api.calls if call[0] == "schedule"]
    assert len(scheduled) == 10
    assert all(call[2]["enabled"] is False for call in scheduled[:5])
    assert all(call[2]["enabled"] is True for call in scheduled[5:])
    for _, item_id, payload in scheduled:
        assert item_id != ROLE_IDS["process"]
        arguments = shlex.split(payload["executionData"]["commandLineArguments"])
        assert arguments[:4] == selected.target().command_line_arguments()
    refresh = next(call[2] for call in scheduled if call[1] == ROLE_IDS["refresh"])
    arguments = shlex.split(refresh["executionData"]["commandLineArguments"])
    assert arguments[arguments.index("--workspace-id") + 1] == selected.workspace_id
    assert arguments[arguments.index("--semantic-model-id") + 1] == str(UUID(int=80))
    assert result["deployment"]["workspace_id"] == selected.workspace_id
    assert result["deployment"]["release"]["manifest_sha256"] == "a" * 64
    assert result["deployment"]["release"]["receipt_sha256"] == "b" * 64


@pytest.mark.parametrize("field,value", [
    ("release_manifest_sha256", ""),
    ("release_manifest_sha256", "z" * 64),
    ("release_receipt_sha256", "0" * 63),
    ("semantic_model_id", ""),
    ("semantic_model_id", str(UUID(int=0))),
])
def test_invalid_activation_inputs_fail_before_any_cloud_call(field, value):
    selected = manifest()
    api = API(selected)
    values = {
        "release_manifest_sha256": "a" * 64,
        "release_receipt_sha256": "b" * 64,
        "semantic_model_id": str(UUID(int=80)),
        "enable_schedules": True,
        field: value,
    }
    with pytest.raises((ValueError, operations.OperationsDeploymentError)):
        operations.deploy_operations(api, manifest=selected, **values)
    assert api.calls == []
    assert api.compute_verified is False


def test_mixed_target_is_rejected_before_publication_or_compute_reads():
    selected = manifest()
    api = API(manifest(offset=100))
    with pytest.raises(operations.OperationsDeploymentError, match="target"):
        operations.provision_operations(api, manifest=selected)
    assert api.calls == []
    assert api.compute_verified is False


def test_duplicate_returned_role_id_is_not_persisted_or_scheduled():
    selected = manifest()
    api = API(selected)
    api.upsert_sjd = Mock(return_value=ROLE_IDS["control"])
    with pytest.raises(operations.OperationsDeploymentError, match="multiple roles"):
        operations.provision_operations(api, manifest=selected)
    assert not any(call[0] == "schedule" for call in api.calls)


@pytest.mark.parametrize("bad_readback", [None, {}, {"enabled": False}, {"enabled": True}])
def test_schedule_readback_must_match_before_any_schedule_is_enabled(bad_readback):
    selected = manifest()
    api = API(selected)
    api.reconcile_schedule = Mock(return_value=bad_readback)
    with pytest.raises(operations.OperationsDeploymentError, match="readback"):
        operations.deploy_operations(
            api, manifest=selected, release_manifest_sha256="a" * 64,
            release_receipt_sha256="b" * 64, semantic_model_id=str(UUID(int=80)),
            enable_schedules=True,
        )
    assert all(
        call.kwargs["payload"]["enabled"] is False
        for call in api.reconcile_schedule.call_args_list
    )


def test_compute_verification_uses_the_supplied_environment(monkeypatch):
    target = FabricTarget(**{key: str(UUID(int=100 + i)) for i, key in enumerate(TARGET_IDS, 1)})
    api = operations.FabricApi(target, token="test-token")
    from people_counter.fabric_sjd_definition import reviewed_compute_settings

    request = Mock(side_effect=[
        (200, {}, reviewed_compute_settings({})),
        (200, {}, {"pool": {"customizeComputeEnabled": True, "starterPool": {
            "maxNodeCount": 3, "maxExecutors": 2,
        }}}),
    ])
    monkeypatch.setattr(api, "request", request)
    api.verify_compute()
    assert request.call_args_list[0].args == (
        "GET",
        f"/workspaces/{target.workspace_id}/environments/{target.environment_id}/sparkcompute?beta=false",
    )
    assert request.call_args_list[1].args == (
        "GET", f"/workspaces/{target.workspace_id}/spark/settings",
    )


@pytest.mark.parametrize("status", [201, 202])
def test_sjd_creation_captures_fabric_id_and_uses_selected_bindings(monkeypatch, status):
    target = FabricTarget(**TARGET_IDS)
    api = operations.FabricApi(target, token="test-token")
    item = {"id": ROLE_IDS["process"], "displayName": "new-process"}
    listing = Mock(side_effect=[[], [item]])
    request = Mock(return_value=(status, {"Location": "operation"}, item))
    monkeypatch.setattr(api, "list_sjds", listing)
    monkeypatch.setattr(api, "request", request)
    wait = Mock()
    monkeypatch.setattr(api, "wait", wait)
    assert api.upsert_sjd("process", "new-process") == item["id"]
    method, path, body = request.call_args.args
    assert (method, path) == ("POST", f"/workspaces/{target.workspace_id}/sparkJobDefinitions")
    metadata = next(part for part in body["definition"]["parts"] if part["path"].endswith(".json"))
    decoded = json.loads(base64.b64decode(metadata["payload"]))
    assert decoded["defaultLakehouseArtifactId"] == target.lakehouse_id
    assert decoded["environmentArtifactId"] == target.environment_id
    assert wait.call_count == (status == 202)


@pytest.mark.parametrize("items", [
    [{"id": ROLE_IDS["process"], "displayName": "existing"}] * 2,
    [{"id": "", "displayName": "existing"}],
    [{"displayName": "existing"}],
])
def test_sjd_discovery_rejects_ambiguous_or_invalid_ids_before_mutation(monkeypatch, items):
    api = operations.FabricApi(FabricTarget(**TARGET_IDS), token="test-token")
    monkeypatch.setattr(api, "list_sjds", lambda: items)
    request = Mock()
    monkeypatch.setattr(api, "request", request)
    with pytest.raises((ValueError, operations.OperationsDeploymentError)):
        api.upsert_sjd("process", "existing")
    request.assert_not_called()


def test_sjd_discovery_does_not_replace_a_pinned_id_with_another_job(monkeypatch):
    api = operations.FabricApi(FabricTarget(**TARGET_IDS), token="test-token")
    monkeypatch.setattr(api, "list_sjds", lambda: [
        {"id": ROLE_IDS["control"], "displayName": "existing"},
    ])
    request = Mock()
    monkeypatch.setattr(api, "request", request)
    with pytest.raises(operations.OperationsDeploymentError, match="configured"):
        api.upsert_sjd("process", "existing", expected_item_id=ROLE_IDS["process"])
    request.assert_not_called()


def test_sjd_listing_follows_pages_and_rejects_repeated_continuation(monkeypatch):
    api = operations.FabricApi(FabricTarget(**TARGET_IDS), token="test-token")
    request = Mock(side_effect=[
        (200, {}, {"value": [{"id": ROLE_IDS["control"], "displayName": "control"}], "continuationUri": "/page-2"}),
        (200, {}, {"value": [{"id": ROLE_IDS["process"], "displayName": "process"}]}),
    ])
    monkeypatch.setattr(api, "request", request)
    assert api.list_sjds() == [
        {"id": ROLE_IDS["control"], "displayName": "control"},
        {"id": ROLE_IDS["process"], "displayName": "process"},
    ]
    assert request.call_args_list[1].args == ("GET", "/page-2")
    request.side_effect = [
        (200, {}, {"value": [], "continuationUri": "/page-2"}),
        (200, {}, {"value": [], "continuationUri": "/page-2"}),
    ]
    with pytest.raises(operations.OperationsDeploymentError, match="continuation"):
        api.list_sjds()


def test_provisioned_definition_keeps_the_deployment_namespace(monkeypatch):
    selected = manifest()
    selected = dataclasses.replace(
        selected,
        lakehouse=dataclasses.replace(selected.lakehouse, files_root="Files/people-counter/dev/v1/"),
    )
    config = selected.runtime_config()
    api = operations.FabricApi(config, token="test-token")
    monkeypatch.setattr(api, "list_sjds", lambda: [])
    request = Mock(return_value=(201, {}, {"id": ROLE_IDS["control"]}))
    monkeypatch.setattr(api, "request", request)
    assert api.upsert_sjd("control", "new-control") == ROLE_IDS["control"]
    parts = request.call_args.args[2]["definition"]["parts"]
    metadata = json.loads(base64.b64decode(next(
        part["payload"] for part in parts if part["path"] == "SparkJobDefinitionV1.json"
    )))
    assert shlex.split(metadata["commandLineArguments"]) == [
        "--files-root", config.files_root,
    ]


@pytest.mark.parametrize("duplicate", ["display_name", "item_id"])
def test_duplicate_role_bindings_fail_before_provisioning(duplicate):
    document = load_manifest_document(DeploymentEnvironment.PROD, env={})
    first = document["roles"]["control"]
    second = document["roles"]["process"]
    if duplicate == "item_id":
        first[duplicate] = ROLE_IDS["control"]
    second[duplicate] = first[duplicate]
    with pytest.raises(ValueError, match="duplicate"):
        parse_deployment_manifest(document, DeploymentEnvironment.PROD)


@pytest.mark.parametrize("value", [None, 0, 1, "false"])
def test_schedule_payload_requires_a_real_boolean(value):
    with pytest.raises(ValueError, match="enabled"):
        operations.schedule_payload(
            interval_minutes=1, command_line_arguments="", enabled=value,
        )


@pytest.mark.parametrize("arguments", [
    ["--provision-only", "--enable-schedules"],
    ["--release-manifest-sha256", "a" * 64],
    [
        "--release-manifest-sha256", "wrong", "--release-receipt-sha256", "b" * 64,
        "--semantic-model-id", ROLE_IDS["gold"],
    ],
    [
        "--release-manifest-sha256", "a" * 64, "--release-receipt-sha256", "b" * 64,
        "--semantic-model-id", "wrong",
    ],
])
def test_cli_invalid_configuration_never_requests_a_token(monkeypatch, tmp_path, arguments):
    config = tmp_path / "input.json"
    selected = manifest()
    config.write_text(json.dumps({
        "schema": "people-counter-deployment-manifest-v1",
        **dataclasses.asdict(selected), "stage": selected.stage.value,
    }))
    token = Mock(side_effect=AssertionError("invalid input requested a token"))
    monkeypatch.setattr(operations, "_token", token)
    with pytest.raises((SystemExit, ValueError, operations.OperationsDeploymentError)):
        operations.main([
            "--config", str(config), "--output", str(tmp_path / "resolved.json"), *arguments,
        ])
    token.assert_not_called()
    assert not (tmp_path / "resolved.json").exists()


def test_cli_provision_persists_ids_and_can_resume_from_its_own_output(monkeypatch, tmp_path, capsys):
    config = tmp_path / "input.json"
    selected = manifest()
    config.write_text(json.dumps({
        "schema": "people-counter-deployment-manifest-v1",
        **dataclasses.asdict(selected), "stage": selected.stage.value,
    }))
    target_calls = []
    api = API(selected)

    def create(target, *, token):
        assert token == "test-token"
        target_calls.append(target)
        return api

    monkeypatch.setattr(operations, "_token", lambda: "test-token")
    monkeypatch.setattr(operations, "FabricApi", create)
    output = tmp_path / "nested" / "resolved.json"
    assert operations.main([
        "--config", str(config), "--output", str(output), "--provision-only",
    ]) == 0
    persisted = json.loads(output.read_text())
    assert {role: row["item_id"] for role, row in persisted["roles"].items()} == ROLE_IDS
    assert json.loads(capsys.readouterr().out)["status"] == "provisioned"
    assert operations.main([
        "--config", str(output), "--output", str(output), "--provision-only",
    ]) == 0
    assert json.loads(output.read_text()) == persisted
    assert target_calls == [selected.runtime_config()] * 2
    assert [call[-1] for call in api.calls[-6:]] == list(ROLE_IDS.values())


def test_existing_sjd_is_disabled_before_its_definition_is_updated(monkeypatch):
    api = operations.FabricApi(FabricTarget(**TARGET_IDS), token="test-token")
    item = {"id": ROLE_IDS["process"], "displayName": "existing"}
    monkeypatch.setattr(api, "list_sjds", lambda: [item])
    order = []
    monkeypatch.setattr(
        api, "disable_schedules",
        lambda item_id, *, remove: order.append(("disable", item_id, remove)),
    )
    request = Mock(side_effect=lambda *args: (order.append(("update", args)), (204, {}, {}))[1])
    monkeypatch.setattr(api, "request", request)
    assert api.upsert_sjd("process", "existing", expected_item_id=item["id"]) == item["id"]
    assert order[0] == ("disable", item["id"], True)
    assert request.call_args.args[0:2] == (
        "POST", f"/workspaces/{TARGET_IDS['workspace_id']}/items/{item['id']}/updateDefinition",
    )


@pytest.mark.parametrize("status", [200, 202, 204])
def test_existing_sjd_update_handles_success_and_lro_responses(monkeypatch, status):
    api = operations.FabricApi(FabricTarget(**TARGET_IDS), token="test-token")
    item = {"id": ROLE_IDS["process"], "displayName": "existing"}
    monkeypatch.setattr(api, "list_sjds", lambda: [item])
    monkeypatch.setattr(api, "disable_schedules", Mock())
    monkeypatch.setattr(api, "request", Mock(return_value=(status, {"Location": "operation"}, {})))
    wait = Mock()
    monkeypatch.setattr(api, "wait", wait)
    assert api.upsert_sjd("process", "existing") == item["id"]
    assert wait.call_count == (status == 202)


@pytest.mark.parametrize("response", [{}, {"value": None}, {"value": {}}, {"value": [None]}])
def test_malformed_sjd_listing_fails_closed(monkeypatch, response):
    api = operations.FabricApi(FabricTarget(**TARGET_IDS), token="test-token")
    monkeypatch.setattr(api, "request", Mock(return_value=(200, {}, response)))
    with pytest.raises(operations.OperationsDeploymentError, match="listing"):
        api.list_sjds()


@pytest.mark.parametrize("listing,expected_patches", [
    ([], 0),
    ([{"id": str(UUID(int=90)), "enabled": False}], 0),
    ([{"id": str(UUID(int=90)), "enabled": True}], 1),
])
def test_schedule_disabling_is_idempotent_and_verified(monkeypatch, listing, expected_patches):
    api = operations.FabricApi(FabricTarget(**TARGET_IDS), token="test-token")
    expected = operations.schedule_payload(
        interval_minutes=5, command_line_arguments="existing", enabled=False,
    )
    for row in listing:
        row.update(configuration=expected["configuration"], executionData=expected["executionData"])
    request = Mock(side_effect=[
        (200, {}, {"value": listing}),
        (204, {}, {}),
        (200, {}, {"id": str(UUID(int=90)), **expected}),
    ])
    monkeypatch.setattr(api, "request", request)
    api.disable_schedules(ROLE_IDS["process"])
    patches = [call for call in request.call_args_list if call.args[0] == "PATCH"]
    assert len(patches) == expected_patches
    if expected_patches:
        assert patches[0].args[2] == expected
        assert request.call_args_list[-1].args[0] == "GET"


@pytest.mark.parametrize("listing", [None, {}, [None], [{}, {}], [{"id": "", "enabled": True}],
    [{"id": str(UUID(int=90)), "enabled": "false"}]])
def test_invalid_schedule_listing_never_updates_a_definition(monkeypatch, listing):
    api = operations.FabricApi(FabricTarget(**TARGET_IDS), token="test-token")
    request = Mock(return_value=(200, {}, {"value": listing}))
    monkeypatch.setattr(api, "request", request)
    with pytest.raises((ValueError, operations.OperationsDeploymentError)):
        api.disable_schedules(ROLE_IDS["process"])
    assert request.call_count == 1


def test_schedule_disable_readback_mismatch_is_not_success(monkeypatch):
    api = operations.FabricApi(FabricTarget(**TARGET_IDS), token="test-token")
    expected = operations.schedule_payload(
        interval_minutes=5, command_line_arguments="existing", enabled=False,
    )
    monkeypatch.setattr(api, "request", Mock(side_effect=[
        (200, {}, {"value": [{"id": str(UUID(int=90)), **expected, "enabled": True}]}),
        (200, {}, {}),
        (200, {}, {"id": str(UUID(int=90)), **expected, "enabled": True}),
    ]))
    with pytest.raises(operations.OperationsDeploymentError, match="disable readback"):
        api.disable_schedules(ROLE_IDS["process"])


@pytest.mark.parametrize("role", ["control", "dispatcher", "reconciliation", "gold", "refresh"])
def test_resolved_manifest_supplies_target_checked_schedule_arguments(role):
    selected = manifest()
    document = {
        "schema": "people-counter-deployment-manifest-v1",
        **dataclasses.asdict(selected), "stage": selected.stage.value,
    }
    document["release"].update(manifest_sha256="a" * 64, receipt_sha256="b" * 64)
    document["analytics_reporting"]["semantic_model"]["target_item_id"] = str(UUID(int=80))
    selected = parse_deployment_manifest(document, selected.stage)
    arguments = shlex.split(selected.schedule_arguments(role, enabled=True))
    assert arguments[:4] == selected.target().command_line_arguments()
    if role == "dispatcher":
        assert arguments[arguments.index("--release-manifest-sha256") + 1] == "a" * 64
        assert arguments[arguments.index("--release-receipt-sha256") + 1] == "b" * 64
    if role == "refresh":
        assert arguments[arguments.index("--semantic-model-id") + 1] == str(UUID(int=80))


@pytest.mark.parametrize("role", ["dispatcher", "refresh"])
def test_missing_admission_or_reporting_bindings_cannot_enable_recurring_work(role):
    selected = manifest()
    assert selected.schedule_arguments(role, enabled=False)
    with pytest.raises(ValueError, match="release|semantic model"):
        selected.schedule_arguments(role, enabled=True)


def test_sjd_listing_also_handles_an_encoded_continuation_token(monkeypatch):
    api = operations.FabricApi(FabricTarget(**TARGET_IDS), token="test-token")
    item = {"id": ROLE_IDS["control"], "displayName": "control"}
    request = Mock(side_effect=[
        (200, {}, {"value": [], "continuationToken": "a/b+ c"}),
        (200, {}, {"value": [item]}),
    ])
    monkeypatch.setattr(api, "request", request)
    assert api.list_sjds() == [item]
    assert request.call_args_list[1].args == (
        "GET",
        f"/workspaces/{TARGET_IDS['workspace_id']}/sparkJobDefinitions?continuationToken=a%2Fb%2B+c",
    )


@pytest.mark.parametrize("response", [{}, {"value": None}, {"value": [None]}, {"value": [{"id": ""}]}])
def test_schedule_reconciliation_rejects_bad_listing_before_creating_or_updating(monkeypatch, response):
    api = operations.FabricApi(FabricTarget(**TARGET_IDS), token="test-token")
    request = Mock(return_value=(200, {}, response))
    monkeypatch.setattr(api, "request", request)
    with pytest.raises((ValueError, operations.OperationsDeploymentError)):
        api.reconcile_schedule(ROLE_IDS["control"], payload={})
    assert request.call_count == 1


def test_definition_readback_rejects_invalid_json_with_an_actionable_error():
    observed = build_sjd_definition("control", config=FabricTarget(**TARGET_IDS))
    part = next(
        part for part in observed["definition"]["parts"]
        if part["path"] == "SparkJobDefinitionV1.json"
    )
    part["payload"] = base64.b64encode(b"not-json").decode()
    with pytest.raises(operations.OperationsDeploymentError, match="JSON"):
        operations.validate_definition_readback("control", observed, config=FabricTarget(**TARGET_IDS))


def test_definition_readback_does_not_accept_a_different_definition_format():
    observed = build_sjd_definition("control", config=FabricTarget(**TARGET_IDS))
    observed["definition"]["format"] = "wrong"
    with pytest.raises(operations.OperationsDeploymentError, match="format"):
        operations.validate_definition_readback("control", observed, config=FabricTarget(**TARGET_IDS))


@pytest.mark.parametrize("kind", [
    "definition", "parts_none", "part_not_object", "duplicate", "missing", "extra",
    "payload_type", "payload_missing", "base64", "metadata", "source", "utf8",
])
def test_definition_readback_rejects_all_untrusted_shapes(kind):
    observed = build_sjd_definition("control", config=TEST_TARGET)
    parts = observed["definition"]["parts"]
    if kind == "definition":
        observed["definition"] = None
    elif kind == "parts_none":
        observed["definition"]["parts"] = None
    elif kind == "part_not_object":
        parts.append(None)
    elif kind == "duplicate":
        parts.append(copy.deepcopy(parts[0]))
    elif kind == "missing":
        parts.pop()
    elif kind == "extra":
        parts.append({"path": "unexpected.txt", "payloadType": "InlineBase64", "payload": "e30="})
    elif kind == "payload_type":
        parts[0]["payloadType"] = "InlineText"
    elif kind == "payload_missing":
        del parts[0]["payload"]
    elif kind == "base64":
        parts[0]["payload"] = "not valid base64!"
    elif kind == "metadata":
        metadata = json.loads(base64.b64decode(parts[1]["payload"]))
        metadata["environmentArtifactId"] = str(UUID(int=999))
        parts[1]["payload"] = base64.b64encode(json.dumps(metadata).encode()).decode()
    elif kind == "source":
        parts[0]["payload"] = base64.b64encode(b"raise SystemExit(0)\n").decode()
    else:
        parts[1]["payload"] = base64.b64encode(b"\xff").decode()
    with pytest.raises(operations.OperationsDeploymentError):
        operations.validate_definition_readback("control", observed, config=TEST_TARGET)


@pytest.mark.parametrize("rollback_fails", [False, True])
def test_activation_failure_attempts_to_disable_every_recurring_schedule(rollback_fails):
    selected = manifest()
    api = API(selected)
    states = {}
    activation_failed = False

    def reconcile(item_id, *, payload):
        nonlocal activation_failed
        api.calls.append(("schedule", item_id, copy.deepcopy(payload)))
        if payload["enabled"] and item_id == ROLE_IDS["reconciliation"]:
            activation_failed = True
            raise operations.OperationsDeploymentError("injected activation failure")
        if activation_failed and rollback_fails and item_id == ROLE_IDS["control"]:
            raise OSError("injected rollback connection failure")
        states[item_id] = payload["enabled"]
        return {**payload, "id": str(UUID(int=90))}

    api.reconcile_schedule = reconcile
    with pytest.raises(operations.OperationsDeploymentError) as error:
        operations.deploy_operations(
            api, manifest=selected, release_manifest_sha256="a" * 64,
            release_receipt_sha256="b" * 64, semantic_model_id=str(UUID(int=80)),
            enable_schedules=True,
        )
    final_calls = [call for call in api.calls if call[0] == "schedule"][-5:]
    assert len(final_calls) == 5
    assert {call[1] for call in final_calls} == set(ROLE_IDS.values()) - {ROLE_IDS["process"]}
    assert all(call[2]["enabled"] is False for call in final_calls)
    assert error.value.__cause__ is not None
    if rollback_fails:
        assert "could not confirm" in str(error.value)
    else:
        assert states and all(value is False for value in states.values())
        assert "disabled again" in str(error.value)


def test_schedule_readback_does_not_treat_boolean_interval_as_integer_one():
    expected = operations.schedule_payload(
        interval_minutes=1, command_line_arguments="", enabled=False,
    )
    observed = copy.deepcopy(expected)
    observed["configuration"]["interval"] = True
    with pytest.raises(operations.OperationsDeploymentError, match="readback"):
        operations._verify_schedule("control", observed, expected)
