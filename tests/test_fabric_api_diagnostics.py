import base64
import copy
import io
import json
import urllib.error
from unittest.mock import Mock, call
from uuid import UUID

import pytest

from scripts import deploy_sjd_operations as operations
from tests.fabric_fixtures import ROLE_IDS, TEST_TARGET
from tests.test_fabric_operations_api import payload


def api():
    return operations.FabricApi(TEST_TARGET, token="test-token", sleep=Mock())


@pytest.mark.parametrize("headers,message", [
    ({}, "Fabric accepted an operation without a Location header"),
    ({"Location": "/"}, "Fabric operation response omitted its operation ID"),
])
def test_missing_operation_metadata_has_an_explicit_diagnostic(headers, message):
    with pytest.raises(operations.OperationsDeploymentError) as error:
        api().wait(headers)
    assert str(error.value) == message


@pytest.mark.parametrize("status", ["Failed", "Cancelled"])
def test_operation_failure_preserves_canonical_remote_failure_details(monkeypatch, status):
    client = api()
    value = {"status": status, "details": {"z": "last", "a": "first"}}
    request = Mock(return_value=(200, {}, value))
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.wait({"Location": "/operations/accepted"})
    assert str(error.value) == "Fabric operation failed: " + json.dumps(
        value, allow_nan=False, sort_keys=True,
    )
    request.assert_called_once_with("GET", "/operations/accepted")
    client.sleep.assert_not_called()


def test_operation_wait_timeout_has_a_specific_diagnostic(monkeypatch):
    client = api()
    monkeypatch.setattr(client, "request", Mock(return_value=(200, {}, {"status": "Running"})))
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.wait({"Location": "/operations/accepted"})
    assert str(error.value) == "Fabric operation polling timed out"
    assert client.request.call_count == 120


@pytest.mark.parametrize("response,message", [
    ({}, "invalid Spark Job Definition listing"),
    ({"value": {}}, "invalid Spark Job Definition listing"),
    ({"value": [None]}, "invalid Spark Job Definition listing"),
    ({"value": [{"id": ROLE_IDS["control"]}]}, "SJD listing omitted a display name"),
    ({"value": [], "continuationUri": ""}, "invalid SJD listing continuation"),
    ({"value": [], "continuationUri": 1}, "invalid SJD listing continuation"),
    ({"value": [], "continuationToken": 1}, "invalid SJD listing continuation token"),
])
def test_listing_diagnostics_distinguish_bad_items_and_bad_continuations(monkeypatch, response, message):
    client = api()
    monkeypatch.setattr(client, "request", Mock(return_value=(200, {}, response)))
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.list_sjds()
    assert str(error.value) == message
    assert client.request.call_count == 1


def test_listing_cycle_is_detected_before_reissuing_a_page(monkeypatch):
    client = api()
    response = (200, {}, {
        "value": [], "continuationUri": "/another-page",
    })
    request = Mock(side_effect=[response, response])
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.list_sjds()
    assert str(error.value) == "repeated SJD listing continuation"
    assert request.call_count == 2


@pytest.mark.parametrize("kind,message", [
    ("duplicate", "multiple Spark Job Definitions named 'chosen'"),
    ("pinned", "configured SJD ID for 'chosen' does not match discovery"),
    ("missing_create_id", "create SJD 'chosen' returned no item ID"),
    ("wrong_create_status", "create SJD 'chosen' returned no item ID"),
    ("ambiguous_after_create", "created SJD 'chosen' has no unique readback"),
    ("failed_update", "update SJD 'chosen' returned 500"),
])
def test_upsert_errors_name_the_requested_role_and_failed_stage(monkeypatch, kind, message):
    client = api()
    item = {"id": ROLE_IDS["control"], "displayName": "chosen"}
    listing = [item]
    request = Mock(return_value=(204, {}, {}))
    if kind == "duplicate":
        listing = [item, item]
    elif kind in {"missing_create_id", "wrong_create_status", "ambiguous_after_create"}:
        listing = []
        request.return_value = (200 if kind == "wrong_create_status" else 201, {}, {})
        if kind == "ambiguous_after_create":
            request.return_value = (202, {"Location": "/operation/accepted"}, {})
    elif kind == "failed_update":
        request.return_value = (500, {}, {})
    monkeypatch.setattr(client, "list_sjds", Mock(return_value=listing))
    monkeypatch.setattr(client, "request", request)
    monkeypatch.setattr(client, "disable_schedules", Mock())
    monkeypatch.setattr(client, "wait", Mock(return_value="accepted"))
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.upsert_sjd(
            "control", "chosen",
            expected_item_id=ROLE_IDS["process"] if kind == "pinned" else None,
        )
    assert str(error.value) == message


def test_creation_request_retains_ownership_description_and_all_definition_parts(monkeypatch):
    client = api()
    monkeypatch.setattr(client, "list_sjds", lambda: [])
    request = Mock(return_value=(201, {}, {"id": ROLE_IDS["control"]}))
    monkeypatch.setattr(client, "request", request)
    assert client.upsert_sjd("control", "chosen") == ROLE_IDS["control"]
    method, path, value = request.call_args.args
    assert (method, path) == ("POST", f"/workspaces/{TEST_TARGET.workspace_id}/sparkJobDefinitions")
    assert value == {
        "displayName": "chosen",
        "description": "Stable people-counter production control Spark Job Definition",
        "definition": operations.build_sjd_definition("control", config=client.config)["definition"],
    }


@pytest.mark.parametrize("listing,message", [
    (None, "invalid or ambiguous SJD schedule listing"),
    ([{}, {}], "invalid or ambiguous SJD schedule listing"),
    ([None], "invalid SJD schedule"),
    ([{"id": str(UUID(int=90))}], "schedule readback omitted enabled state"),
    ([{"id": str(UUID(int=90)), "enabled": True}], "schedule configuration is missing; cannot disable safely"),
])
def test_disabling_schedule_errors_are_distinguishable(monkeypatch, listing, message):
    client = api()
    request = Mock(return_value=(200, {}, {"value": listing}))
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.disable_schedules(ROLE_IDS["control"])
    assert str(error.value) == message
    assert request.call_count == 1


def test_on_demand_schedule_deletion_error_is_not_a_disable_success(monkeypatch):
    client = api()
    request = Mock(side_effect=[
        (200, {}, {"value": [{"id": str(UUID(int=90)), "enabled": False}]}),
        (500, {}, {}),
    ])
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.disable_schedules(ROLE_IDS["process"], remove=True)
    assert str(error.value) == "on-demand schedule deletion failed"
    assert request.call_count == 2


@pytest.mark.parametrize("kind,message", [
    ("disable_id", "schedule disable readback ID differs"),
    ("disable_content", "schedule disable readback differs"),
])
def test_disable_readback_failure_has_a_stage_specific_diagnostic(monkeypatch, kind, message):
    client = api()
    schedule_id = str(UUID(int=90))
    expected = payload()
    observed = {"id": schedule_id, **expected}
    if kind == "disable_id":
        observed["id"] = str(UUID(int=91))
    else:
        observed["enabled"] = True
    request = Mock(side_effect=[
        (200, {}, {"value": [{"id": schedule_id, **expected, "enabled": True}]}),
        (200, {}, {}),
        (200, {}, observed),
    ])
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.disable_schedules(ROLE_IDS["control"])
    assert str(error.value) == message
    assert request.call_count == 3


@pytest.mark.parametrize("response,message", [
    ({}, "invalid schedule listing"),
    ({"value": [None]}, "invalid schedule listing"),
    ({"value": [{}, {}]}, f"SJD {ROLE_IDS['control']} has multiple schedules; refusing ambiguous update"),
])
def test_schedule_reconciliation_rejects_bad_listings_without_a_write(monkeypatch, response, message):
    client = api()
    monkeypatch.setattr(client, "request", Mock(return_value=(200, {}, response)))
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.reconcile_schedule(ROLE_IDS["control"], payload=payload())
    assert str(error.value) == message
    assert client.request.call_count == 1


@pytest.mark.parametrize("listing", [[], [{}, {}], [None]])
def test_missing_created_schedule_identity_requires_unique_discovery(monkeypatch, listing):
    client = api()
    monkeypatch.setattr(client, "request", Mock(side_effect=[
        (200, {}, {"value": []}), (201, {}, {}), (200, {}, {"value": listing}),
    ]))
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.reconcile_schedule(ROLE_IDS["control"], payload=payload())
    assert str(error.value) == "created schedule could not be uniquely read back"


def test_schedule_mutation_ack_with_wrong_http_status_stops_before_readback(monkeypatch):
    client = api()
    request = Mock(side_effect=[(200, {}, {"value": []}), (500, {}, {})])
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.reconcile_schedule(ROLE_IDS["control"], payload=payload())
    assert str(error.value) == "schedule update returned unexpected HTTP 500"
    assert request.call_count == 2


def test_schedule_readback_cannot_change_the_identity(monkeypatch):
    client = api()
    request = Mock(side_effect=[
        (200, {}, {"value": []}),
        (201, {}, {"id": str(UUID(int=90))}),
        (200, {}, {"id": str(UUID(int=91)), **payload()}),
    ])
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.reconcile_schedule(ROLE_IDS["control"], payload=payload())
    assert str(error.value) == "schedule readback ID differs from the requested schedule"


def test_http_error_text_is_decoded_without_hiding_invalid_bytes(monkeypatch):
    client = api()
    failure = urllib.error.HTTPError(
        "https://api.fabric.microsoft.com/v1/items", 403, "denied", {},
        io.BytesIO(b"access denied \xff"),
    )
    monkeypatch.setattr(operations.urllib.request, "urlopen", Mock(side_effect=failure))
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.request("GET", "/items")
    assert str(error.value) == "GET https://api.fabric.microsoft.com/v1/items returned 403: access denied \ufffd"


def intake_definition(content=None):
    if content is None:
        content = {"properties": {"activities": [{
            "name": "ValidateManifest", "typeProperties": {"parameters": {
                "RELEASE_MANIFEST_SHA256": {"type": "string", "value": "a" * 64},
                "CAMERA_ID": {"type": "string", "value": "preserve"},
            }},
        }]}}
    return {"definition": {"parts": [{
        "path": "pipeline-content.json", "payloadType": "InlineBase64",
        "payload": base64.b64encode(json.dumps(content).encode()).decode(),
    }]}}


@pytest.mark.parametrize("kind,message", [
    ("digest", "release digest must be a SHA-256 value"),
    ("missing_definition", "expected exactly one pipeline-content.json part"),
    ("missing_parts", "expected exactly one pipeline-content.json part"),
    ("missing_part", "expected exactly one pipeline-content.json part"),
    ("payload_type", "intake definition must use InlineBase64"),
    ("invalid_payload", "intake pipeline definition is invalid"),
    ("missing_payload", "intake pipeline definition is invalid"),
    ("invalid_json", "intake pipeline definition is invalid"),
    ("no_properties", "expected exactly one ValidateManifest activity"),
    ("no_activities", "expected exactly one ValidateManifest activity"),
    ("no_parameters", "intake release parameter must be a string"),
    ("parameter_type", "intake release parameter must be a string"),
])
def test_intake_binding_errors_identify_the_invalid_layer(kind, message):
    source = intake_definition()
    digest = "b" * 64
    part = source["definition"]["parts"][0]
    if kind == "digest":
        digest = "invalid"
    elif kind == "missing_definition":
        source = {}
    elif kind == "missing_parts":
        source["definition"] = {}
    elif kind == "missing_part":
        source["definition"]["parts"] = []
    elif kind == "payload_type":
        part["payloadType"] = "Text"
    elif kind == "invalid_payload":
        part["payload"] += "!"
    elif kind == "missing_payload":
        del part["payload"]
    elif kind == "invalid_json":
        part["payload"] = base64.b64encode(b"not-json").decode()
    elif kind == "no_properties":
        source = intake_definition({})
    elif kind == "no_activities":
        source = intake_definition({"properties": {}})
    else:
        source = intake_definition({"properties": {"activities": [{
            "name": "ValidateManifest",
            **({"typeProperties": {"parameters": {
                "RELEASE_MANIFEST_SHA256": {"type": "integer"},
            }}} if kind == "parameter_type" else {}),
        }]}})
    before = copy.deepcopy(source)
    with pytest.raises(operations.OperationsDeploymentError) as error:
        operations.bind_intake_release(source, digest)
    assert str(error.value) == message
    assert source == before


def test_intake_binding_payload_is_canonical_and_does_not_change_other_parameters():
    source = intake_definition()
    expected = json.loads(base64.b64decode(source["definition"]["parts"][0]["payload"]))
    expected["properties"]["activities"][0]["typeProperties"]["parameters"][
        "RELEASE_MANIFEST_SHA256"
    ]["value"] = "b" * 64
    result = operations.bind_intake_release(source, "b" * 64)
    assert base64.b64decode(result["definition"]["parts"][0]["payload"]) == json.dumps(
        expected, sort_keys=True, separators=(",", ":"),
    ).encode()


def notebook_definition(source):
    return {"definition": {"parts": [{
        "path": "notebook-content.py", "payloadType": "InlineBase64",
        "payload": base64.b64encode(source.encode()).decode(),
    }]}}


@pytest.mark.parametrize("kind,message", [
    ("missing_definition", "expected exactly one notebook-content.py part"),
    ("missing_parts", "expected exactly one notebook-content.py part"),
    ("missing_part", "expected exactly one notebook-content.py part"),
    ("payload_type", "expected exactly one notebook-content.py part"),
    ("bad_base64", "registration notebook source is invalid"),
    ("bad_syntax", "registration notebook source is invalid"),
    ("missing_payload", "registration notebook source is invalid"),
    ("missing_assignment", "expected exactly one release_digest assignment"),
    ("duplicate_assignment", "expected exactly one release_digest assignment"),
])
def test_registration_notebook_binding_errors_identify_the_failed_layer(kind, message):
    source = notebook_definition("release_digest = require_sha256('a')\n")
    part = source["definition"]["parts"][0]
    if kind == "missing_definition":
        source = {}
    elif kind == "missing_parts":
        source["definition"] = {}
    elif kind == "missing_part":
        source["definition"]["parts"] = []
    elif kind == "payload_type":
        part["payloadType"] = "Text"
    elif kind == "bad_base64":
        part["payload"] += "!"
    elif kind == "missing_payload":
        del part["payload"]
    elif kind == "bad_syntax":
        source = notebook_definition("release_digest = (\n")
    elif kind == "missing_assignment":
        source = notebook_definition("other_name = 'unchanged'\n")
    else:
        source = notebook_definition("release_digest = 'a'\nrelease_digest = 'b'\n")
    before = copy.deepcopy(source)
    with pytest.raises(operations.OperationsDeploymentError) as error:
        operations.bind_registration_notebook(source)
    assert str(error.value) == message
    assert source == before


@pytest.mark.parametrize("producer", ["intake", "notebook"])
def test_producer_update_and_readback_use_exact_target_and_format(monkeypatch, producer):
    client = api()
    source = (
        intake_definition() if producer == "intake"
        else notebook_definition("release_digest = require_sha256('a')\n")
    )
    expected = (
        operations.bind_intake_release(source, "b" * 64) if producer == "intake"
        else operations.bind_registration_notebook(source)
    )
    read = Mock(side_effect=[source, expected])
    request = Mock(return_value=(204, {}, {}))
    monkeypatch.setattr(client, "get_definition", read)
    monkeypatch.setattr(client, "request", request)
    if producer == "intake":
        client.reconcile_intake_release(ROLE_IDS["control"], "b" * 64)
    else:
        client.reconcile_registration_notebook(ROLE_IDS["control"])
    assert read.call_args_list == [call(ROLE_IDS["control"], format_name=None)] * 2
    path = f"/workspaces/{TEST_TARGET.workspace_id}/items/{ROLE_IDS['control']}/updateDefinition"
    assert request.call_args.args[0:2] == ("POST", path)
    written = request.call_args.args[2]
    assert written["definition"] == expected["definition"]


@pytest.mark.parametrize("producer", ["intake", "notebook"])
def test_failed_producer_update_reports_its_stage_before_readback(monkeypatch, producer):
    client = api()
    source = (
        intake_definition() if producer == "intake"
        else notebook_definition("release_digest = require_sha256('a')\n")
    )
    read = Mock(return_value=source)
    monkeypatch.setattr(client, "get_definition", read)
    monkeypatch.setattr(client, "request", Mock(return_value=(500, {}, {})))
    with pytest.raises(operations.OperationsDeploymentError) as error:
        if producer == "intake":
            client.reconcile_intake_release(ROLE_IDS["control"], "b" * 64)
        else:
            client.reconcile_registration_notebook(ROLE_IDS["control"])
    assert str(error.value) == (
        "intake definition update returned 500" if producer == "intake"
        else "registration notebook update returned 500"
    )
    assert read.call_count == 1


@pytest.mark.parametrize("kind,message", [
    ("missing", "control SJD definition readback is missing"),
    ("duplicate", "control SJD readback has duplicate parts"),
    ("unexpected", "control SJD definition readback has unexpected parts"),
    ("payload_type", "control SJD part 'Main/main.py' has an invalid payload type"),
    ("payload", "control SJD part 'Main/main.py' has an invalid payload"),
    ("metadata", "control SJD metadata readback differs from deployment"),
    ("source", "control SJD source readback differs from deployment"),
])
def test_definition_readback_error_names_the_changed_artifact(kind, message):
    observed = operations.build_sjd_definition("control", config=TEST_TARGET)
    parts = observed["definition"]["parts"]
    if kind == "missing":
        observed = {}
    elif kind == "duplicate":
        parts.append(copy.deepcopy(parts[0]))
    elif kind == "unexpected":
        parts.pop()
    elif kind == "payload_type":
        parts[0]["payloadType"] = "Text"
    elif kind == "payload":
        parts[0]["payload"] += "!"
    elif kind == "metadata":
        content = json.loads(base64.b64decode(parts[1]["payload"]))
        content["environmentArtifactId"] = str(UUID(int=999))
        parts[1]["payload"] = base64.b64encode(json.dumps(content).encode()).decode()
    else:
        parts[0]["payload"] = base64.b64encode(b"raise SystemExit(0)\n").decode()
    with pytest.raises(operations.OperationsDeploymentError) as error:
        operations.validate_definition_readback("control", observed, config=TEST_TARGET)
    assert str(error.value) == message


@pytest.mark.parametrize("failure_roles", [[], ["control", "refresh"]])
def test_activation_failure_reports_every_unconfirmed_rollback_and_keeps_its_cause(failure_roles):
    values = {role: payload() for role in ("dispatcher", "control", "refresh")}
    items = {role: ROLE_IDS[role] for role in values}
    client = Mock()
    activating = True

    def update(item_id, *, payload):
        nonlocal activating
        role = next(role for role, value in items.items() if value == item_id)
        if payload["enabled"]:
            activating = False
            raise operations.OperationsDeploymentError("original activation failure")
        if role in failure_roles:
            raise OSError("rollback failure")
        return payload

    client.reconcile_schedule.side_effect = update
    with pytest.raises(operations.OperationsDeploymentError) as error:
        operations._activate_schedules(client, items, values)
    if failure_roles:
        expected = (
            "Schedule activation failed; could not confirm disabled schedules for "
            "control, refresh. Inspect the target workspace before retrying."
        )
    else:
        expected = (
            "Schedule activation failed; all recurring schedules were disabled again. "
            "Previously accepted jobs are not cancelled."
        )
    assert str(error.value) == expected
    assert str(error.value.__cause__) == "original activation failure"
    assert activating is False
    assert client.reconcile_schedule.call_count == 4


@pytest.mark.parametrize("phase", ["activate", "rollback"])
def test_wrong_schedule_readback_preserves_the_correct_role_in_the_failure_cause(phase):
    values = {"control": payload()}
    client = Mock()
    if phase == "activate":
        client.reconcile_schedule.side_effect = [payload(), payload()]
    else:
        client.reconcile_schedule.side_effect = [
            operations.OperationsDeploymentError("original activation failure"),
            {**payload(), "enabled": True},
        ]
    with pytest.raises(operations.OperationsDeploymentError) as error:
        operations._activate_schedules(client, {"control": ROLE_IDS["control"]}, values)
    if phase == "activate":
        assert str(error.value.__cause__) == "control schedule readback differs"
    else:
        assert "could not confirm disabled schedules for control." in str(error.value)


def test_compute_diagnostics_distinguish_memory_and_pool_admission():
    properties = {
        "spark.executor.instances": "2", "spark.executor.cores": "8",
        "spark.executor.memory": "28g", "spark.dynamicAllocation.enabled": "false",
    }
    with pytest.raises(operations.OperationsDeploymentError) as error:
        operations.validate_live_allocation(
            properties, [{"id": "1", "totalCores": 8}, {"id": "2", "totalCores": 8}],
        )
    assert str(error.value) == "Canary executor memory differs from the reviewed allocation"
    with pytest.raises(operations.OperationsDeploymentError) as error:
        operations.validate_workspace_pool_policy({})
    assert str(error.value) == (
        "Starter Pool must allow 3 nodes / 2 executors; "
        "Environment properties alone do not prove the live allocation"
    )


@pytest.mark.parametrize("existing,status", [
    (False, 200), (False, 201), (False, 202),
    (True, 200), (True, 202), (True, 204),
])
def test_schedule_create_and_update_preserve_lro_headers_and_exact_request_paths(
    monkeypatch, existing, status,
):
    client = api()
    schedule_id = str(UUID(int=90))
    expected = payload()
    headers = {"Location": "/operations/accepted", "x-ms-operation-id": "accepted"}
    request = Mock(side_effect=[
        (200, {}, {"value": [{"id": schedule_id}] if existing else []}),
        (status, headers, {"id": schedule_id}),
        (200, {}, {"id": schedule_id, **expected}),
    ])
    wait = Mock()
    monkeypatch.setattr(client, "request", request)
    monkeypatch.setattr(client, "wait", wait)
    assert client.reconcile_schedule(ROLE_IDS["control"], payload=expected) == {
        "id": schedule_id, **expected,
    }
    base = f"/workspaces/{TEST_TARGET.workspace_id}/items/{ROLE_IDS['control']}/jobs/sparkjob/schedules"
    assert request.call_args_list == [
        call("GET", base),
        call("PATCH" if existing else "POST", base + "/" + schedule_id if existing else base, expected),
        call("GET", base + "/" + schedule_id),
    ]
    if status == 202:
        wait.assert_called_once_with(headers)
    else:
        wait.assert_not_called()


def test_discovery_after_schedule_creation_uses_the_same_collection_endpoint(monkeypatch):
    client = api()
    schedule_id = str(UUID(int=90))
    expected = payload()
    request = Mock(side_effect=[
        (200, {}, {"value": []}), (201, {}, {}),
        (200, {}, {"value": [{"id": schedule_id}]}),
        (200, {}, {"id": schedule_id, **expected}),
    ])
    monkeypatch.setattr(client, "request", request)
    client.reconcile_schedule(ROLE_IDS["control"], payload=expected)
    base = f"/workspaces/{TEST_TARGET.workspace_id}/items/{ROLE_IDS['control']}/jobs/sparkjob/schedules"
    assert request.call_args_list == [
        call("GET", base), call("POST", base, expected), call("GET", base),
        call("GET", base + "/" + schedule_id),
    ]


@pytest.mark.parametrize("existing", [False, True])
def test_schedule_id_validation_names_the_failed_identity(monkeypatch, existing):
    client = api()
    replies = [(200, {}, {"value": [{"id": "invalid"}] if existing else []})]
    if not existing:
        replies.append((201, {}, {"id": "invalid"}))
    request = Mock(side_effect=replies)
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(ValueError) as error:
        client.reconcile_schedule(ROLE_IDS["control"], payload=payload())
    assert str(error.value) == "schedule ID must be a canonical UUID"
    assert request.call_count == (1 if existing else 2)


@pytest.mark.parametrize("status", [200, 202, 204])
def test_sjd_update_sends_the_full_selected_definition_and_lro_headers(monkeypatch, status):
    client = api()
    item_id = ROLE_IDS["control"]
    monkeypatch.setattr(client, "list_sjds", lambda: [{"id": item_id, "displayName": "chosen"}])
    disable = Mock()
    headers = {"Location": "/operations/accepted"}
    request = Mock(return_value=(status, headers, {}))
    wait = Mock()
    monkeypatch.setattr(client, "disable_schedules", disable)
    monkeypatch.setattr(client, "request", request)
    monkeypatch.setattr(client, "wait", wait)
    assert client.upsert_sjd("control", "chosen") == item_id
    disable.assert_called_once_with(item_id, remove=False)
    request.assert_called_once_with(
        "POST", f"/workspaces/{TEST_TARGET.workspace_id}/items/{item_id}/updateDefinition",
        operations.build_sjd_definition("control", config=client.config),
    )
    if status == 202:
        wait.assert_called_once_with(headers)
    else:
        wait.assert_not_called()


def test_sjd_creation_passes_the_actual_operation_headers_to_polling(monkeypatch):
    client = api()
    monkeypatch.setattr(client, "list_sjds", Mock(side_effect=[
        [], [{"id": ROLE_IDS["control"], "displayName": "chosen"}],
    ]))
    headers = {"Location": "/operations/accepted", "Retry-After": "9"}
    request = Mock(return_value=(202, headers, {}))
    wait = Mock()
    monkeypatch.setattr(client, "request", request)
    monkeypatch.setattr(client, "wait", wait)
    assert client.upsert_sjd("control", "chosen") == ROLE_IDS["control"]
    wait.assert_called_once_with(headers)


@pytest.mark.parametrize("stage", ["configured", "discovered", "created_sync", "created_async"])
def test_sjd_identity_validation_identifies_the_actual_source_of_the_bad_id(monkeypatch, stage):
    client = api()
    item = {"id": ROLE_IDS["control"], "displayName": "chosen"}
    listing = [item]
    expected_id = None
    request = Mock(return_value=(201, {}, {"id": "invalid"}))
    if stage == "configured":
        expected_id = "invalid"
    elif stage == "discovered":
        item["id"] = "invalid"
    elif stage == "created_sync":
        listing = []
    else:
        listing = None
        request.return_value = (202, {"Location": "/operations/accepted"}, {})
    monkeypatch.setattr(
        client, "list_sjds",
        Mock(side_effect=[[], [{"id": "invalid", "displayName": "chosen"}]])
        if listing is None else Mock(return_value=listing),
    )
    monkeypatch.setattr(client, "request", request)
    monkeypatch.setattr(client, "wait", Mock())
    with pytest.raises(ValueError) as error:
        client.upsert_sjd("control", "chosen", expected_item_id=expected_id)
    label = "configured" if stage == "configured" else "discovered" if stage == "discovered" else "created"
    assert str(error.value) == f"{label} SJD ID must be a canonical UUID"


def test_disable_requests_use_the_same_verified_schedule_path(monkeypatch):
    client = api()
    schedule_id = str(UUID(int=90))
    expected = payload()
    request = Mock(side_effect=[
        (200, {}, {"value": [{"id": schedule_id, **expected, "enabled": True}]}),
        (200, {}, {}), (200, {}, {"id": schedule_id, **expected}),
    ])
    monkeypatch.setattr(client, "request", request)
    client.disable_schedules(ROLE_IDS["control"])
    base = f"/workspaces/{TEST_TARGET.workspace_id}/items/{ROLE_IDS['control']}/jobs/sparkjob/schedules"
    assert request.call_args_list == [
        call("GET", base), call("PATCH", base + "/" + schedule_id, expected),
        call("GET", base + "/" + schedule_id),
    ]


def test_invalid_schedule_id_and_truthy_nonobject_configuration_are_not_sent_to_fabric(monkeypatch):
    client = api()
    request = Mock(return_value=(200, {}, {"value": [{"id": "invalid", "enabled": False}]}))
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(ValueError) as error:
        client.disable_schedules(ROLE_IDS["control"])
    assert str(error.value) == "schedule ID must be a canonical UUID"
    request.return_value = (200, {}, {"value": [{
        "id": str(UUID(int=90)), "enabled": True, "configuration": ["invalid"],
    }]})
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.disable_schedules(ROLE_IDS["control"])
    assert str(error.value) == "schedule configuration is missing; cannot disable safely"


@pytest.mark.parametrize("status", [200, 204])
def test_process_schedule_delete_accepts_both_documented_empty_success_forms(monkeypatch, status):
    client = api()
    request = Mock(side_effect=[
        (200, {}, {"value": [{"id": str(UUID(int=90)), "enabled": False}]}),
        (status, {}, {}), (200, {}, {"value": []}),
    ])
    monkeypatch.setattr(client, "request", request)
    client.disable_schedules(ROLE_IDS["process"], remove=True)
    assert request.call_count == 3


def test_readback_failure_reports_the_requested_item_id_not_an_unknown_role(monkeypatch):
    client = api()
    expected = payload()
    observed = {"id": str(UUID(int=90)), **expected, "enabled": True}
    request = Mock(side_effect=[
        (200, {}, {"value": []}), (201, {}, {"id": str(UUID(int=90))}), (200, {}, observed),
    ])
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.reconcile_schedule(ROLE_IDS["control"], payload=expected)
    assert str(error.value) == f"{ROLE_IDS['control']} schedule readback differs"


def test_nonfinite_json_payload_is_rejected_before_any_http_request(monkeypatch):
    transport = Mock(side_effect=AssertionError("invalid JSON reached the network"))
    monkeypatch.setattr(operations.urllib.request, "urlopen", transport)
    with pytest.raises(ValueError) as error:
        api().request("POST", "/items", {"duration": float("nan")})
    assert "Out of range float values are not JSON compliant" in str(error.value)
    transport.assert_not_called()


def test_invalid_nonfinite_failure_details_are_not_serialized_as_valid_json(monkeypatch):
    client = api()
    monkeypatch.setattr(client, "request", Mock(return_value=(200, {}, {
        "status": "Failed", "diagnostic": float("nan"),
    })))
    with pytest.raises(ValueError) as error:
        client.wait({"Location": "/operations/accepted"})
    assert "Out of range float values are not JSON compliant" in str(error.value)
    client.sleep.assert_not_called()


def test_notebook_readback_accepts_crlf_without_rewriting_semantics_and_polls_actual_headers(monkeypatch):
    client = api()
    source = notebook_definition("release_digest = require_sha256('a')\nresult = 'keep'\n")
    expected = operations.bind_registration_notebook(source)
    observed = copy.deepcopy(expected)
    part = observed["definition"]["parts"][0]
    text = base64.b64decode(part["payload"]).decode().replace("\n", "\r\n")
    part["payload"] = base64.b64encode(text.encode()).decode()
    read = Mock(side_effect=[source, observed])
    request = Mock(return_value=(202, {"Location": "/operations/accepted"}, {}))
    wait = Mock()
    monkeypatch.setattr(client, "get_definition", read)
    monkeypatch.setattr(client, "request", request)
    monkeypatch.setattr(client, "wait", wait)
    client.reconcile_registration_notebook(ROLE_IDS["control"])
    wait.assert_called_once_with({"Location": "/operations/accepted"})
    assert read.call_args_list == [call(ROLE_IDS["control"], format_name=None)] * 2
    read.side_effect = [source, source]
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.reconcile_registration_notebook(ROLE_IDS["control"])
    assert str(error.value) == "registration notebook source readback differs"


def test_intake_lro_polls_the_actual_headers_and_reports_changed_readback(monkeypatch):
    client = api()
    source = intake_definition()
    observed = operations.bind_intake_release(source, "b" * 64)
    read = Mock(side_effect=[source, observed])
    request = Mock(return_value=(202, {"Location": "/operations/accepted"}, {}))
    wait = Mock()
    monkeypatch.setattr(client, "get_definition", read)
    monkeypatch.setattr(client, "request", request)
    monkeypatch.setattr(client, "wait", wait)
    client.reconcile_intake_release(ROLE_IDS["control"], "b" * 64)
    wait.assert_called_once_with({"Location": "/operations/accepted"})
    read.side_effect = [source, source]
    with pytest.raises(operations.OperationsDeploymentError) as error:
        client.reconcile_intake_release(ROLE_IDS["control"], "b" * 64)
    assert str(error.value) == "intake release definition readback differs"


def test_operation_id_extraction_accepts_trailing_slashes_without_losing_the_id(monkeypatch):
    client = api()
    request = Mock(return_value=(200, {}, {"status": "Succeeded"}))
    monkeypatch.setattr(client, "request", request)
    assert client.wait({"Location": "/operations/accepted///"}) == "accepted"
    request.assert_called_once_with("GET", "/operations/accepted///")


def test_definition_lro_passes_the_actual_headers_to_operation_polling(monkeypatch):
    client = api()
    headers = {"Location": "/operations/accepted"}
    expected = {"definition": {"format": "SparkJobDefinitionV2", "parts": []}}
    monkeypatch.setattr(client, "request", Mock(side_effect=[
        (202, headers, {}), (200, {}, expected),
    ]))
    wait = Mock(return_value="accepted")
    monkeypatch.setattr(client, "wait", wait)
    assert client.get_definition(ROLE_IDS["control"]) == expected
    wait.assert_called_once_with(headers)


def test_listing_rejects_an_invalid_returned_identifier_with_its_source_label(monkeypatch):
    client = api()
    monkeypatch.setattr(client, "request", Mock(return_value=(200, {}, {
        "value": [{"id": "invalid", "displayName": "chosen"}],
    })))
    with pytest.raises(ValueError) as error:
        client.list_sjds()
    assert str(error.value) == "listed SJD ID must be a canonical UUID"


def test_whitespace_wrapped_token_is_rejected_rather_than_sent_to_fabric(monkeypatch):
    monkeypatch.setattr(
        operations.subprocess, "check_output",
        Mock(return_value='{"accessToken":" test-token "}'),
    )
    with pytest.raises(operations.OperationsDeploymentError) as error:
        operations._token()
    assert str(error.value) == "Azure CLI returned an invalid or empty Fabric token"


def test_schedule_enabled_value_has_an_exact_boolean_diagnostic():
    with pytest.raises(ValueError) as error:
        operations.schedule_payload(interval_minutes=5, command_line_arguments="", enabled=1)
    assert str(error.value) == "schedule enabled must be a boolean"


def test_default_schedule_start_time_uses_utc_even_on_a_non_utc_host(monkeypatch):
    import time
    from datetime import datetime, timedelta, timezone

    if not hasattr(time, "tzset"):
        pytest.skip("This test requires POSIX timezone selection")
    try:
        with monkeypatch.context() as context:
            context.setenv("TZ", "UTC+05")
            time.tzset()
            before = datetime.now(timezone.utc).replace(microsecond=0)
            actual = operations.schedule_payload(interval_minutes=5, command_line_arguments="", enabled=False)
            after = datetime.now(timezone.utc)
            start = datetime.fromisoformat(actual["configuration"]["startDateTime"].replace("Z", "+00:00"))
            assert before + timedelta(minutes=5) <= start <= after + timedelta(minutes=5)
    finally:
        time.tzset()


@pytest.mark.usefixtures("fabric_runtime_context")
def test_notebook_binding_preserves_executable_code_before_and_after_the_assignment(monkeypatch):
    from people_counter import fabric_release_provenance as provenance

    resolver = Mock(return_value="d" * 64)
    monkeypatch.setattr(provenance, "registration_release_digest", resolver)
    source = notebook_definition(
        "# preserved header\n"
        "prior = 'keep'\n"
        "release_digest = require_sha256('old')\n"
        "result = prior + ':' + release_digest\n"
    )
    updated = operations.bind_registration_notebook(source)
    text = base64.b64decode(updated["definition"]["parts"][0]["payload"]).decode()
    assert text.startswith("# preserved header\nprior = 'keep'\n")
    assert text.endswith("result = prior + ':' + release_digest\n")
    namespace = {"RELEASE_MANIFEST_SHA256": ""}
    exec(compile(text, "bound-notebook", "exec"), namespace)
    assert namespace["result"] == "keep:" + "d" * 64
    resolver.assert_called_once()
