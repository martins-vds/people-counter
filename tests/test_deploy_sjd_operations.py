from __future__ import annotations

import base64
import json
from datetime import datetime, timezone

import pytest

from scripts.deploy_sjd_operations import (
    FabricApi,
    ITEM_NAMES,
    OperationsDeploymentError,
    deploy_operations,
    schedule_payload,
    validate_definition_readback,
    bind_intake_release,
    bind_registration_notebook,
)


def test_fabric_api_uses_the_supplied_bearer_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class Response:
        status = 200
        headers: dict[str, str] = {}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def read() -> bytes:
            return b"{}"

    def open_request(request, *, timeout):
        observed["authorization"] = request.get_header("Authorization")
        observed["timeout"] = timeout
        return Response()

    monkeypatch.setattr(
        "scripts.deploy_sjd_operations.urllib.request.urlopen",
        open_request,
    )
    api = FabricApi("workspace-1", token="token-1")

    status, _, body = api.request("GET", "/workspaces/workspace-1/items")

    assert status == 200
    assert body == {}
    assert observed == {
        "authorization": "Bearer token-1",
        "timeout": 120,
    }


class FakeApi:
    workspace_id = "workspace-1"

    def __init__(self) -> None:
        self.upserts: list[tuple[str, str]] = []
        self.schedules: list[tuple[str, dict[str, object]]] = []
        self.intake_bindings: list[tuple[str, str]] = []
        self.notebook_bindings: list[str] = []
        self.compute_verified = False

    def verify_compute(self) -> None:
        self.compute_verified = True

    def reconcile_intake_release(self, item_id: str, release_digest: str) -> None:
        assert not self.schedules
        self.intake_bindings.append((item_id, release_digest))

    def reconcile_registration_notebook(self, item_id: str) -> None:
        assert not self.schedules
        self.notebook_bindings.append(item_id)

    def upsert_sjd(self, kind: str, display_name: str) -> str:
        self.upserts.append((kind, display_name))
        return f"{kind}-id"

    @staticmethod
    def get_definition(item_id: str) -> dict[str, object]:
        from people_counter.fabric_sjd_definition import build_sjd_definition

        definition = build_sjd_definition(item_id.removesuffix("-id"))
        definition["definition"]["parts"].append(
            {
                "path": ".platform",
                "payload": base64.b64encode(b"{}").decode("ascii"),
                "payloadType": "InlineBase64",
            }
        )
        return definition

    def reconcile_schedule(
        self,
        item_id: str,
        *,
        payload: dict[str, object],
    ) -> dict[str, object]:
        self.schedules.append((item_id, payload))
        return {
            "configuration": payload["configuration"],
            "enabled": payload["enabled"],
            "id": f"{item_id}-schedule",
        }


def test_schedule_payload_is_bounded_deterministic_and_disabled_by_default() -> None:
    result = schedule_payload(
        interval_minutes=5,
        command_line_arguments="recover",
        enabled=False,
        now=datetime(2026, 10, 9, 20, 0, tzinfo=timezone.utc),
    )

    assert result == {
        "configuration": {
            "endDateTime": "2036-10-06T20:05:00Z",
            "interval": 5,
            "localTimeZoneId": "UTC",
            "startDateTime": "2026-10-09T20:05:00Z",
            "type": "Cron",
        },
        "enabled": False,
        "executionData": {"commandLineArguments": "recover"},
    }
    with pytest.raises(ValueError, match="between 1 and 10080"):
        schedule_payload(
            interval_minutes=0,
            command_line_arguments="recover",
            enabled=False,
        )


def test_deploy_operations_reconciles_all_stable_jobs_and_schedules() -> None:
    api = FakeApi()
    result = deploy_operations(
        api,
        release_manifest_sha256="a" * 64,
        release_receipt_sha256="b" * 64,
        semantic_model_id="model-1",
        additional_semantic_model_ids=("model-2",),
        enable_schedules=False,
        intake_pipeline_id="intake-id",
        registration_notebook_id="metadata-notebook",
        now=datetime(2026, 10, 9, 20, 0, tzinfo=timezone.utc),
    )

    assert api.upserts == list(ITEM_NAMES.items())
    scheduled_ids = [item_id for item_id, _ in api.schedules]
    assert scheduled_ids == [
        "dispatcher-id",
        "6df10e00-517c-409a-8dd6-40ae9bc62003",
        "c94fc6fb-7397-4d11-9bd8-c4d3e778b501",
        "621391c9-ba0c-4965-8f1e-52fbb46b59eb",
        "refresh-id",
    ]
    assert all(
        payload["enabled"] is False for _, payload in api.schedules
    )
    dispatcher = api.schedules[0][1]["executionData"][
        "commandLineArguments"
    ]
    assert "--maximum-active-batches 1" in dispatcher
    assert "--release-manifest-sha256 " + "a" * 64 in dispatcher
    refresh = api.schedules[-1][1]["executionData"][
        "commandLineArguments"
    ]
    assert refresh == (
        "--workspace-id workspace-1 --semantic-model-id model-1 "
        "--semantic-model-id model-2"
    )
    assert result["enabled"] is False
    assert api.compute_verified is True
    assert api.intake_bindings == [("intake-id", "a" * 64)]
    assert api.notebook_bindings == ["metadata-notebook"]


def test_deploy_operations_fails_closed_on_definition_drift() -> None:
    api = FakeApi()
    api.get_definition = lambda _item_id: {"definition": {"format": "wrong"}}

    with pytest.raises(
        OperationsDeploymentError,
        match="parts readback is invalid",
    ):
        deploy_operations(
            api,
            release_manifest_sha256="a" * 64,
            release_receipt_sha256="b" * 64,
            semantic_model_id="model-1",
            enable_schedules=False,
            intake_pipeline_id="intake-id",
            registration_notebook_id="metadata-notebook",
        )


def test_intake_binding_uses_same_release_and_preserves_other_configuration() -> None:
    content = {
        "properties": {"activities": [{
            "name": "ValidateManifest", "type": "TridentNotebook",
            "typeProperties": {"parameters": {
                "RELEASE_MANIFEST_SHA256": {"type": "string", "value": "a" * 64},
                "CAMERA_ID": {"type": "string", "value": "preserve"},
            }},
        }]},
    }
    original = {
        "definition": {"parts": [
            {"path": "pipeline-content.json", "payloadType": "InlineBase64",
             "payload": base64.b64encode(json.dumps(content).encode()).decode()},
            {"path": ".platform", "payloadType": "InlineBase64", "payload": "e30="},
        ]},
    }
    updated = bind_intake_release(original, "b" * 64)
    observed = json.loads(base64.b64decode(updated["definition"]["parts"][0]["payload"]))
    params = observed["properties"]["activities"][0]["typeProperties"]["parameters"]
    assert params["RELEASE_MANIFEST_SHA256"]["value"] == "b" * 64
    assert params["CAMERA_ID"]["value"] == "preserve"
    assert updated["definition"]["parts"][1] == original["definition"]["parts"][1]
    assert json.loads(base64.b64decode(original["definition"]["parts"][0]["payload"])) == content
    with pytest.raises(OperationsDeploymentError):
        bind_intake_release(original, "not-a-digest")
    content["properties"]["activities"].append(content["properties"]["activities"][0])
    ambiguous = {"definition": {"parts": [{
        "path": "pipeline-content.json", "payloadType": "InlineBase64",
        "payload": base64.b64encode(json.dumps(content).encode()).decode(),
    }]}}
    with pytest.raises(OperationsDeploymentError, match="exactly one"):
        bind_intake_release(ambiguous, "b" * 64)


def test_registration_notebook_uses_sdk_release_without_literal_versions(monkeypatch) -> None:
    from unittest.mock import Mock

    source = (
        "# PARAMETERS\nRELEASE_MANIFEST_SHA256 = ''\n"
        "# VALIDATE\nrelease_digest = require_sha256(RELEASE_MANIFEST_SHA256, 'RELEASE_MANIFEST_SHA256')\n"
        "result = {'release_digest': release_digest}\n"
    )
    definition = {"definition": {"parts": [{
        "path": "notebook-content.py", "payloadType": "InlineBase64",
        "payload": base64.b64encode(source.encode()).decode(),
    }]}}
    updated = bind_registration_notebook(definition)
    observed = base64.b64decode(updated["definition"]["parts"][0]["payload"]).decode()
    assert "registration_release_digest(" in observed
    assert "# PARAMETERS\nRELEASE_MANIFEST_SHA256 = ''" in observed
    assert "result = {'release_digest': release_digest}" in observed
    assert bind_registration_notebook(updated) == updated
    assert base64.b64decode(definition["definition"]["parts"][0]["payload"]).decode() == source
    files = Mock()
    resolver = Mock(return_value="b" * 64)
    monkeypatch.setattr("people_counter.fabric_control_store.NotebookUtilsOneLakeFiles", lambda: files)
    monkeypatch.setattr("people_counter.fabric_release_provenance.registration_release_digest", resolver)
    namespace = {}
    exec(compile(observed, "<registration-metadata>", "exec"), namespace)
    resolver.assert_called_once_with(files.read_text, expected_digest="")
    assert namespace["result"] == {"release_digest": "b" * 64}


def test_reviewed_compute_configuration_is_fixed_and_preserves_unrelated_properties():
    from people_counter.fabric_sjd_definition import reviewed_compute_settings, validate_reviewed_compute_settings

    existing = {
        "dynamicExecutorAllocation": {"enabled": True, "minExecutors": 1, "maxExecutors": 9},
        "sparkProperties": [{"key": "spark.sql.execution.pythonUDF.arrow.enabled", "value": "true"}],
    }
    configured = reviewed_compute_settings(existing)
    assert configured == {
        "driverCores": 8, "driverMemory": "56g", "executorCores": 8, "executorMemory": "56g",
        "dynamicExecutorAllocation": {"enabled": False}, "runtimeVersion": "2.0",
        "sparkProperties": [
            {"key": "spark.executor.instances", "value": "2"},
            {"key": "spark.speculation", "value": "false"},
            {"key": "spark.sql.execution.pythonUDF.arrow.enabled", "value": "true"},
            {"key": "spark.task.cpus", "value": "1"},
        ],
    }
    assert configured["dynamicExecutorAllocation"] == {"enabled": False}
    assert configured["executorCores"] == 8
    assert configured["executorMemory"] == "56g"
    props = {row["key"]: row["value"] for row in configured["sparkProperties"]}
    assert props["spark.speculation"] == "false"
    assert props["spark.task.cpus"] == "1"
    assert props["spark.executor.instances"] == "2"
    assert props["spark.sql.execution.pythonUDF.arrow.enabled"] == "true"
    validate_reviewed_compute_settings(configured)
    with pytest.raises(RuntimeError, match="fixed allocation"):
        validate_reviewed_compute_settings(existing)


@pytest.mark.parametrize("field,value", [
    ("driverCores", 4), ("driverMemory", "28g"), ("executorCores", 4),
    ("executorMemory", "28g"), ("runtimeVersion", "1.3"),
    ("dynamicExecutorAllocation", {"enabled": True}),
    ("sparkProperties", [{"key": "spark.speculation", "value": "true"}]),
])
def test_reviewed_compute_validation_rejects_each_contract_drift(field, value):
    from people_counter.fabric_sjd_definition import reviewed_compute_settings, validate_reviewed_compute_settings

    settings = reviewed_compute_settings({})
    settings[field] = value
    with pytest.raises(RuntimeError, match="fixed allocation"):
        validate_reviewed_compute_settings(settings)


def test_workspace_pool_limits_are_required_before_enabling_compute(monkeypatch):
    from unittest.mock import Mock
    from people_counter.fabric_sjd_definition import reviewed_compute_settings
    from people_counter.fabric_release_provenance import RELEASE_IDENTITY

    api = FabricApi("workspace", token="token")
    base = "/workspaces/workspace/environments/" + RELEASE_IDENTITY.environment_id + "/sparkcompute?beta=false"
    policy = {"pool": {"customizeComputeEnabled": True, "starterPool": {"maxNodeCount": 10, "maxExecutors": 9}}}
    request = Mock(side_effect=[
        (200, {}, reviewed_compute_settings({})),
        (200, {}, policy),
    ])
    monkeypatch.setattr(api, "request", request)
    with pytest.raises(OperationsDeploymentError, match="Starter Pool"):
        api.verify_compute()
    policy["pool"]["starterPool"] = {"maxNodeCount": 3, "maxExecutors": 2}
    request.side_effect = [(200, {}, reviewed_compute_settings({})), (200, {}, policy)]
    api.verify_compute()
    calls = request.call_args_list
    assert calls[0].args == ("GET", base)
    assert calls[1].args == ("GET", "/workspaces/workspace/spark/settings")


def test_live_allocation_verifier_checks_runtime_not_just_published_properties():
    from scripts.deploy_sjd_operations import validate_live_allocation

    properties = {"spark.executor.instances": "9", "spark.executor.cores": "8",
                  "spark.executor.memory": "56g", "spark.dynamicAllocation.enabled": "false"}
    workers = [{"id": str(i), "totalCores": 8} for i in range(1, 3)]
    with pytest.raises(OperationsDeploymentError, match="2 fixed executors"):
        validate_live_allocation(properties, workers)
    properties["spark.executor.instances"] = "2"
    validate_live_allocation(properties, workers)
    with pytest.raises(OperationsDeploymentError, match="2 observed executors"):
        validate_live_allocation(properties, workers + [{"id": "3", "totalCores": 8}])


@pytest.mark.parametrize(("field", "value"), [
    ("spark.executor.cores", "4"),
    ("spark.executor.memory", "28g"),
    ("spark.dynamicAllocation.enabled", "true"),
    ("spark.executor.instances", "1"),
])
def test_live_allocation_rejects_each_native_setting_drift(field, value):
    from scripts.deploy_sjd_operations import validate_live_allocation

    properties = {"spark.executor.instances": "2", "spark.executor.cores": "8",
                  "spark.executor.memory": "56g", "spark.dynamicAllocation.enabled": "false"}
    properties[field] = value
    with pytest.raises(OperationsDeploymentError):
        validate_live_allocation(properties, [{"id": "1", "totalCores": 8}, {"id": "2", "totalCores": 8}])


@pytest.mark.parametrize("workers", [
    [], [{"id": "1", "totalCores": 8}],
    [{"id": "1", "totalCores": 4}, {"id": "2", "totalCores": 8}],
])
def test_live_allocation_rejects_incomplete_or_wrong_worker_observations(workers):
    from scripts.deploy_sjd_operations import validate_live_allocation

    properties = {"spark.executor.instances": "2", "spark.executor.cores": "8",
                  "spark.executor.memory": "56g", "spark.dynamicAllocation.enabled": "false"}
    with pytest.raises(OperationsDeploymentError, match="2 observed executors"):
        validate_live_allocation(properties, workers)


def test_live_allocation_excludes_driver():
    from scripts.deploy_sjd_operations import validate_live_allocation

    properties = {"spark.executor.instances": "2", "spark.executor.cores": "8",
                  "spark.executor.memory": "56g", "spark.dynamicAllocation.enabled": "false"}
    validate_live_allocation(properties, [
        {"id": "driver", "totalCores": 8}, {"id": "1", "totalCores": 8}, {"id": "2", "totalCores": 8},
    ])


@pytest.mark.parametrize(("field", "value"), [
    ("maxNodeCount", 10), ("maxExecutors", 9), ("customizeComputeEnabled", False),
])
def test_workspace_pool_rejects_each_limit_drift(field, value):
    from scripts.deploy_sjd_operations import validate_workspace_pool_policy

    policy = {"pool": {"customizeComputeEnabled": True, "starterPool": {"maxNodeCount": 3, "maxExecutors": 2}}}
    target = policy["pool"] if field == "customizeComputeEnabled" else policy["pool"]["starterPool"]
    target[field] = value
    with pytest.raises(OperationsDeploymentError, match="Starter Pool must allow 3 nodes / 2 executors"):
        validate_workspace_pool_policy(policy)


@pytest.mark.parametrize("field", [
    "spark.executor.instances", "spark.executor.cores", "spark.executor.memory",
    "spark.dynamicAllocation.enabled",
])
def test_live_allocation_rejects_missing_native_properties(field):
    from scripts.deploy_sjd_operations import validate_live_allocation

    properties = {"spark.executor.instances": "2", "spark.executor.cores": "8",
                  "spark.executor.memory": "56g", "spark.dynamicAllocation.enabled": "false"}
    del properties[field]
    with pytest.raises(OperationsDeploymentError):
        validate_live_allocation(properties, [{"id": "1", "totalCores": 8}, {"id": "2", "totalCores": 8}])


@pytest.mark.parametrize("settings", [{}, {"pool": {}}, {"pool": {"customizeComputeEnabled": True}}])
def test_workspace_pool_rejects_missing_policy(settings):
    from scripts.deploy_sjd_operations import validate_workspace_pool_policy

    with pytest.raises(OperationsDeploymentError, match="Starter Pool must allow 3 nodes / 2 executors"):
        validate_workspace_pool_policy(settings)


@pytest.mark.parametrize("kind", ["intake", "notebook"])
@pytest.mark.parametrize("status", [200, 202, 204])
def test_release_producer_reconcilers_update_and_verify_live_readback(monkeypatch, kind, status):
    from unittest.mock import Mock

    if kind == "intake":
        content = {"properties": {"activities": [{
            "name": "ValidateManifest", "typeProperties": {"parameters": {
                "RELEASE_MANIFEST_SHA256": {"type": "string", "value": "a" * 64},
            }},
        }]}}
        path, payload = "pipeline-content.json", json.dumps(content).encode()
    else:
        path = "notebook-content.py"
        payload = b"RELEASE_MANIFEST_SHA256 = ''\nrelease_digest = require_sha256(RELEASE_MANIFEST_SHA256, 'RELEASE_MANIFEST_SHA256')\n"
    original = {"definition": {"parts": [{
        "path": path, "payloadType": "InlineBase64",
        "payload": base64.b64encode(payload).decode(),
    }]}}
    expected = bind_intake_release(original, "b" * 64) if kind == "intake" else bind_registration_notebook(original)
    api = FabricApi("workspace", token="token")
    readback = Mock(side_effect=[original, expected])
    request = Mock(return_value=(status, {"Location": "operation"}, {}))
    wait = Mock()
    monkeypatch.setattr(api, "get_definition", readback)
    monkeypatch.setattr(api, "request", request)
    monkeypatch.setattr(api, "wait", wait)
    if kind == "intake":
        api.reconcile_intake_release("item", "b" * 64)
    else:
        api.reconcile_registration_notebook("item")
    assert request.call_args.args[:2] == ("POST", "/workspaces/workspace/items/item/updateDefinition")
    assert readback.call_count == 2
    assert wait.call_count == (1 if status == 202 else 0)
    readback.side_effect = [original, original]
    with pytest.raises(OperationsDeploymentError, match="readback differs"):
        if kind == "intake":
            api.reconcile_intake_release("item", "b" * 64)
        else:
            api.reconcile_registration_notebook("item")

def test_definition_readback_accepts_fabric_json_formatting_only() -> None:
    from people_counter.fabric_sjd_definition import build_sjd_definition

    observed = build_sjd_definition("dispatcher")
    parts = observed["definition"]["parts"]
    metadata = next(
        part for part in parts if part["path"] == "SparkJobDefinitionV1.json"
    )
    value = json.loads(base64.b64decode(metadata["payload"]))
    metadata["payload"] = base64.b64encode(
        json.dumps(value, indent=2).encode("utf-8")
    ).decode("ascii")
    parts.append(
        {
            "path": ".platform",
            "payload": base64.b64encode(b"{}").decode("ascii"),
            "payloadType": "InlineBase64",
        }
    )
    parts.append(
        {
            "path": ".schedules",
            "payload": base64.b64encode(b"{}").decode("ascii"),
            "payloadType": "InlineBase64",
        }
    )

    validate_definition_readback("dispatcher", observed)
