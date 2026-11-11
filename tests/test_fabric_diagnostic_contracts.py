import copy
import json
from types import SimpleNamespace
from uuid import UUID

import pytest

from people_counter import fabric_sjd_jobs as jobs
from people_counter import manual_fabric_publication as publication
from people_counter.fabric_target import FabricTarget
from tests.fabric_fixtures import RUNTIME_CONTEXT
from tests.test_fabric_manual_bindings import Files, NAMES, catalog, state
from unittest.mock import Mock


@pytest.mark.parametrize("context", [None, [], "", 1])
def test_missing_fabric_context_is_identified_consistently(context):
    with pytest.raises(ValueError) as error:
        FabricTarget.from_runtime_context(context)
    assert str(error.value) == "Fabric runtime context must be a mapping"
    with pytest.raises(ValueError) as error:
        FabricTarget(**{
            "workspace_id": RUNTIME_CONTEXT["currentWorkspaceId"],
            "lakehouse_id": RUNTIME_CONTEXT["defaultLakehouseId"],
            "environment_id": RUNTIME_CONTEXT["environmentId"],
        }).validate_runtime_context(context)
    assert str(error.value) == "Fabric runtime context must be a mapping"


@pytest.mark.parametrize("field", [
    "defaultLakehouseWorkspaceId", "defaultLakehouseId", "environmentId",
])
def test_incomplete_fabric_context_names_the_required_artifact_set(field):
    context = dict(RUNTIME_CONTEXT)
    del context[field]
    with pytest.raises(ValueError) as error:
        FabricTarget.from_runtime_context(context)
    assert str(error.value) == "Fabric runtime context must identify its Lakehouse and Environment"


def test_unavailable_runtime_context_does_not_hide_the_explicit_target_requirement(monkeypatch):
    import sys

    monkeypatch.setitem(sys.modules, "notebookutils", None)
    with pytest.raises(RuntimeError) as error:
        FabricTarget.current()
    assert str(error.value) == (
        "Fabric runtime context is unavailable; supply an explicit Fabric target"
    )
    monkeypatch.setitem(sys.modules, "notebookutils", SimpleNamespace())
    with pytest.raises(ValueError) as error:
        FabricTarget.current()
    assert str(error.value) == "Fabric runtime context must be a mapping"


@pytest.mark.parametrize("names,message", [
    ({}, "SJD role names must be non-empty"),
    ({"control": ""}, "SJD role names must be non-empty"),
    ({"control": "x", "process": "X"}, "SJD role names must be unique"),
])
def test_sjd_name_errors_are_actionable_and_do_not_query_fabric(names, message):
    client = Mock()
    with pytest.raises(ValueError) as error:
        publication.resolve_sjd_ids(client, names)
    assert str(error.value) == message
    client.list_items.assert_not_called()


@pytest.mark.parametrize("kind,message", [
    ("missing", "Missing or ambiguous SJD: control"),
    ("duplicate", "Missing or ambiguous SJD: control"),
    ("same_id", "The same SJD ID was returned for multiple roles"),
])
def test_discovery_errors_identify_the_ambiguous_role(kind, message):
    items = catalog()
    if kind == "missing":
        items.pop(0)
    elif kind == "duplicate":
        items.append(items[0])
    else:
        items[1]["id"] = items[0]["id"]
    client = Mock()
    client.list_items.return_value = items
    with pytest.raises(ValueError) as error:
        publication.resolve_sjd_ids(client, NAMES)
    assert str(error.value) == message


@pytest.mark.parametrize("change,message", [
    ("schema", "Unsupported deployment state schema"),
    ("lakehouse", "Deployment state Lakehouse must be an object"),
    ("roles", "Deployment state must contain the six SJD roles"),
    ("role", "Invalid deployment state role control"),
    ("duplicate", "Deployment state SJD IDs must be unique"),
])
def test_invalid_deployment_state_has_a_stable_diagnostic_and_is_never_written(change, message):
    document = state()
    if change == "schema":
        document["schema"] = "other"
    elif change == "lakehouse":
        document["lakehouse"] = []
    elif change == "roles":
        document["roles"] = {}
    elif change == "role":
        document["roles"]["control"] = None
    else:
        document["roles"]["process"]["item_id"] = document["roles"]["control"]["item_id"]
    files = Files()
    with pytest.raises(ValueError) as error:
        publication.save_deployment_state(files, "state", document)
    assert str(error.value) == message
    assert files.writes == []


@pytest.mark.parametrize("change,message", [
    ("target", "Deployment state belongs to another target"),
    ("namespace", "Deployment state namespace differs"),
    ("prefix", "Deployment state namespace differs"),
    ("roles", "Deployment state role set differs; review the migration"),
    ("id", "Deployment state conflicts with the discovered control ID"),
])
def test_conflicting_state_error_preserves_the_original_and_identifies_the_conflict(change, message):
    document = state()
    previous = copy.deepcopy(document)
    if change == "target":
        previous["workspace_id"] = str(UUID(int=999))
    elif change == "namespace":
        previous["lakehouse"]["files_root"] = "Files/other/"
    elif change == "prefix":
        previous["lakehouse"]["table_prefix"] = "another_"
    elif change == "roles":
        del previous["roles"]["control"]
    else:
        previous["roles"]["control"]["item_id"] = str(UUID(int=999))
    files = Files()
    files.content["state"] = json.dumps(previous)
    before = dict(files.content)
    with pytest.raises(ValueError) as error:
        publication.save_deployment_state(files, "state", document)
    assert str(error.value) == message
    assert files.content == before
    assert files.writes == []


@pytest.mark.parametrize("failure,message", [
    ("write", "Deployment state write failed"),
    ("readback", "Deployment state readback differs"),
])
def test_state_io_failure_is_not_reported_as_success(failure, message):
    files = Files()
    files.fail_write = failure == "write"
    files.corrupt_readback = failure == "readback"
    with pytest.raises(OSError) as error:
        publication.save_deployment_state(files, "state", state())
    assert str(error.value) == message


def test_workspace_token_must_agree_with_the_explicit_workspace_name():
    with pytest.raises(ValueError) as error:
        publication.render_definition(
            {"parts": []}, {"TargetWorkspaceName": "another-workspace"}, workspace_name="chosen",
        )
    assert str(error.value) == "Workspace name token differs from the selected workspace"
    assert publication.render_definition(
        {"parts": []}, {"TargetWorkspaceName": "chosen"}, workspace_name="chosen",
    ) == {"parts": []}


def test_unresolved_token_error_names_the_part():
    import base64

    value = {"parts": [{
        "path": "definition.tmdl", "payload": base64.b64encode(b"{{ .Missing }}").decode(),
        "payloadType": "InlineBase64",
    }]}
    with pytest.raises(ValueError) as error:
        publication.render_definition(value, {})
    assert str(error.value) == "Unresolved definition token in definition.tmdl"


@pytest.mark.usefixtures("fabric_runtime_context")
def test_reconciliation_usage_error_has_a_stable_role_specific_message(monkeypatch):
    from people_counter import fabric_sjd_runtime

    run = Mock(side_effect=AssertionError("Invalid reconciliation arguments reached the runtime"))
    monkeypatch.setattr(fabric_sjd_runtime, "control_main", run)
    with pytest.raises(ValueError) as error:
        jobs.reconciliation_main(["recover"])
    assert str(error.value) == "stable reconciliation accepts only the reconcile command"
    run.assert_not_called()


def test_partial_artifact_arguments_show_the_three_distinct_required_ids(capsys):
    with pytest.raises(SystemExit) as error:
        jobs._runtime_binding(["--artifacts", "workspace-only"])
    assert error.value.code == 2
    text = capsys.readouterr().err
    assert "--artifacts WORKSPACE LAKEHOUSE" in text
    import re
    assert re.search(r"\bENVIRONMENT\b", text)
    assert text.rstrip().endswith("argument --artifacts/--manual-artifacts: expected 3 arguments")


def test_short_help_does_not_require_a_fabric_runtime(monkeypatch):
    import sys

    monkeypatch.setitem(sys.modules, "notebookutils", None)
    config, remaining = jobs._runtime_binding(["-h"])
    assert config is None
    assert remaining == ["-h"]


@pytest.mark.parametrize("kind,message", [
    ("configuration", "schedule configuration must be an object"),
    ("interval", "schedule interval must be an integer"),
    ("missing_start", "schedule startDateTime must be an ISO datetime"),
    ("invalid_end", "schedule endDateTime must be an ISO datetime"),
    ("enabled", "schedule enabled readback differs"),
    ("changed", "schedule configuration readback differs"),
    ("arguments", "schedule execution data readback differs"),
])
def test_schedule_diagnostics_identify_the_invalid_readback_field(kind, message):
    from people_counter.fabric_sjd_definition import validate_schedule_readback
    from tests.test_fabric_schedule_contract import schedule

    expected = schedule()
    observed = copy.deepcopy(expected)
    if kind == "configuration":
        observed["configuration"] = None
    elif kind == "interval":
        observed["configuration"]["interval"] = True
    elif kind == "missing_start":
        del observed["configuration"]["startDateTime"]
    elif kind == "invalid_end":
        observed["configuration"]["endDateTime"] = "not-a-time"
    elif kind == "enabled":
        observed["enabled"] = None
    elif kind == "changed":
        observed["configuration"]["interval"] += 1
    else:
        observed["executionData"] = {}
    with pytest.raises(ValueError) as error:
        validate_schedule_readback(observed, expected)
    assert str(error.value) == message


def test_utc_schedule_normalization_is_independent_of_the_process_local_timezone(monkeypatch):
    import time
    from people_counter.fabric_sjd_definition import _schedule_configuration

    if not hasattr(time, "tzset"):
        pytest.skip("This test requires POSIX timezone selection")
    try:
        with monkeypatch.context() as context:
            context.setenv("TZ", "UTC+05")
            time.tzset()
            normalized = _schedule_configuration({
                "type": "Cron", "interval": 5, "localTimeZoneId": "UTC",
                "startDateTime": "2026-11-11T00:00:00",
                "endDateTime": "2026-11-12T01:00:00+01:00",
            })
            assert normalized == {
                "type": "Cron", "interval": 5, "localTimeZoneId": "UTC",
                "startDateTime": "2026-11-11T00:00:00+00:00",
                "endDateTime": "2026-11-12T00:00:00+00:00",
            }
    finally:
        time.tzset()


@pytest.mark.parametrize("field,label", [
    ("workspace_id", "workspace_id"),
    ("fabric_environment_id", "fabric_environment_id"),
    ("lakehouse", "lakehouse_id"),
    ("role", "control SJD ID"),
])
def test_state_invalid_artifact_diagnostic_names_the_specific_id(field, label):
    document = state()
    if field == "lakehouse":
        document["lakehouse"]["lakehouse_id"] = None
    elif field == "role":
        document["roles"]["control"]["item_id"] = None
    else:
        document[field] = None
    with pytest.raises(ValueError) as error:
        publication.save_deployment_state(Files(), "state", document)
    assert str(error.value) == f"{label} must be a canonical UUID"


def test_state_json_rejects_nonfinite_external_evidence_before_a_write():
    document = state()
    document["externalEvidence"] = {"duration": float("nan")}
    files = Files()
    with pytest.raises(ValueError) as error:
        publication.save_deployment_state(files, "state", document)
    assert "Out of range float values are not JSON compliant" in str(error.value)
    assert files.writes == []


def test_manual_artifact_validator_preserves_its_diagnostic_on_invalid_nil_values():
    with pytest.raises(ValueError) as error:
        publication._artifact_id(None, "test artifact")
    assert str(error.value) == "test artifact must be a canonical UUID"
    with pytest.raises(ValueError) as error:
        publication._artifact_id(str(UUID(int=0)), "test artifact")
    assert str(error.value) == "test artifact must be a canonical UUID"


def test_missing_refresh_model_and_invalid_explicit_model_have_distinct_diagnostics():
    from tests.test_fabric_operations_portable import manifest

    selected = manifest()
    with pytest.raises(ValueError) as error:
        selected.schedule_arguments("refresh", enabled=True)
    assert str(error.value) == "Refresh activation requires a resolved semantic model ID"
    with pytest.raises(ValueError) as error:
        selected.schedule_arguments("refresh", enabled=True, semantic_model_ids=["invalid"])
    assert str(error.value) == "semantic model ID must be a canonical UUID"


def test_binary_resource_does_not_prevent_later_definition_tokens_from_being_rendered():
    import base64

    source = {"parts": [
        {"path": "binary.png", "payloadType": "InlineBase64", "payload": base64.b64encode(b"\xff").decode()},
        {"path": "definition.tmdl", "payloadType": "InlineBase64",
         "payload": base64.b64encode(b"{{ .Target }}").decode()},
    ]}
    rendered = publication.render_definition(source, {"Target": "chosen"})
    assert rendered["parts"][0] == source["parts"][0]
    assert base64.b64decode(rendered["parts"][1]["payload"]) == b"chosen"


def test_sjd_discovery_error_names_the_actual_invalid_role_id():
    items = catalog()
    items[0]["id"] = None
    client = Mock()
    client.list_items.return_value = items
    with pytest.raises(ValueError) as error:
        publication.resolve_sjd_ids(client, NAMES)
    assert str(error.value) == "control SJD ID must be a canonical UUID"
