import copy
import dataclasses
from unittest.mock import Mock, call

import pytest

from people_counter import fabric_reconcilers as reconcile
from people_counter.fabric_automation import DeploymentEvidence
from tests.test_fabric_reconcilers import _FakeClient, _NOW, _resolved_manifest, _uid


def schedule_arguments(**values):
    return {
        "ws": "workspace", "role": "control", "item_id": "item",
        "enabled": False, "interval_minutes": 5, "command_line_arguments": "checked",
        "now": _NOW, **values,
    }


@pytest.mark.parametrize("role,interval,message", [
    ("unknown", 5, "unsupported SJD role 'unknown'"),
    ("control", None, "role 'control' schedule presence disagrees with its scheduled/on-demand class"),
    ("process", 5, "role 'process' schedule presence disagrees with its scheduled/on-demand class"),
])
def test_invalid_role_classification_is_reported_before_any_api_calls(role, interval, message):
    client = _FakeClient()
    with pytest.raises(reconcile.ReconcileError) as error:
        reconcile.reconcile_role_schedule(client, **schedule_arguments(role=role, interval_minutes=interval))
    assert str(error.value) == message
    assert client.calls == []


def test_on_demand_schedule_absence_and_deletion_have_exact_evidence():
    base = "/workspaces/workspace/items/item/jobs/sparkjob/schedules"
    client = _FakeClient()
    result = reconcile.reconcile_role_schedule(client, **schedule_arguments(
        role="process", interval_minutes=None,
    ))
    assert result == DeploymentEvidence(
        action="schedule:process", status="succeeded", resource={"state": "absent"},
    )
    assert client.calls == [("LIST", base, "value", ())]
    client = _FakeClient(list_pages_results={base: [{"id": "assigned"}]})
    result = reconcile.reconcile_role_schedule(client, **schedule_arguments(
        role="process", interval_minutes=None,
    ))
    assert result == DeploymentEvidence(
        action="schedule:process", status="succeeded", resource={"state": "deleted", "id": "assigned"},
    )
    assert client.calls == [
        ("LIST", base, "value", ()),
        ("DELETE", base + "/assigned", None, "application/json", (200,)),
    ]


@pytest.mark.parametrize("existing", [False, True])
def test_schedule_readback_is_preserved_in_evidence_without_losing_operation_metadata(existing):
    base = "/workspaces/workspace/items/item/jobs/sparkjob/schedules"
    client = Mock()
    client.list_pages.return_value = [{"id": "assigned"}] if existing else []
    acknowledgement = DeploymentEvidence(
        action="schedule:control", status="succeeded", operation_id="operation",
        attempts=3, resource={"id": "assigned", "extra": "keep"},
    )
    client.run_operation.return_value = acknowledgement
    payload = reconcile._schedule_payload(interval=5, enabled=False, args="checked", now=_NOW)
    observed = {"id": "assigned", **payload, "serverMetadata": "retain"}
    client.request_json.return_value = observed
    result = reconcile.reconcile_role_schedule(client, **schedule_arguments())
    assert result == dataclasses.replace(
        acknowledgement, resource={"id": "assigned", "extra": "keep", "readback": observed},
    )
    client.run_operation.assert_called_once_with(
        "PATCH" if existing else "POST", base + "/assigned" if existing else base,
        action="schedule:control", value=payload,
        accepted=(200, 202, 204) if existing else (200, 201, 202),
    )
    client.request_json.assert_called_once_with("GET", base + "/assigned", expected=(200,))
    assert acknowledgement.resource == {"id": "assigned", "extra": "keep"}


@pytest.mark.parametrize("existing", [False, True])
def test_failed_schedule_write_evidence_is_returned_without_readback(existing):
    client = Mock()
    client.list_pages.return_value = [{"id": "assigned"}] if existing else []
    failure = DeploymentEvidence(action="schedule:control", status="failed", error="denied")
    client.run_operation.return_value = failure
    assert reconcile.reconcile_role_schedule(client, **schedule_arguments()) is failure
    client.request_json.assert_not_called()


def test_created_schedule_without_acknowledgement_id_is_discovered_before_readback():
    client = Mock()
    client.list_pages.side_effect = [[], [{"id": "assigned"}]]
    acknowledgement = DeploymentEvidence(action="schedule:control", status="succeeded")
    client.run_operation.return_value = acknowledgement
    payload = reconcile._schedule_payload(interval=5, enabled=False, args="checked", now=_NOW)
    observed = {"id": "assigned", **payload}
    client.request_json.return_value = observed
    assert reconcile.reconcile_role_schedule(client, **schedule_arguments()) == dataclasses.replace(
        acknowledgement, resource={"id": "assigned", "readback": observed},
    )
    base = "/workspaces/workspace/items/item/jobs/sparkjob/schedules"
    assert client.list_pages.call_args_list == [call(base), call(base)]
    client.request_json.assert_called_once_with("GET", base + "/assigned", expected=(200,))


@pytest.mark.parametrize("kind,message", [
    ("multiple", "SJD item (control) has multiple schedules; refusing to update"),
    ("existing_id", "control schedule readback omitted its ID"),
    ("discovered_id", "control schedule readback omitted its ID"),
    ("discovery_empty", "control created schedule could not be uniquely read back"),
    ("discovery_ambiguous", "control created schedule could not be uniquely read back"),
    ("discovery_shape", "control created schedule could not be uniquely read back"),
    ("observed_id", "control schedule readback ID differs"),
    ("observed_value", "control schedule readback differs: schedule enabled readback differs"),
])
def test_reconciliation_readback_errors_identify_the_exact_failed_stage(kind, message):
    client = Mock()
    client.list_pages.return_value = []
    client.run_operation.return_value = DeploymentEvidence(
        action="schedule:control", status="succeeded", resource={"id": "assigned"},
    )
    observed = {"id": "assigned", **reconcile._schedule_payload(
        interval=5, enabled=False, args="checked", now=_NOW,
    )}
    client.request_json.return_value = observed
    if kind == "multiple":
        client.list_pages.return_value = [{"id": "a"}, {"id": "b"}]
    elif kind == "existing_id":
        client.list_pages.return_value = [{}]
    elif kind in {"discovered_id", "discovery_empty", "discovery_ambiguous", "discovery_shape"}:
        client.run_operation.return_value = DeploymentEvidence(
            action="schedule:control", status="succeeded", resource=None,
        )
        values = {
            "discovered_id": [{}], "discovery_empty": [],
            "discovery_ambiguous": [{}, {}], "discovery_shape": [None],
        }[kind]
        client.list_pages.side_effect = [[], values]
    elif kind == "observed_id":
        observed["id"] = "another"
    else:
        observed["enabled"] = True
    with pytest.raises(reconcile.ReconcileError) as error:
        reconcile.reconcile_role_schedule(client, **schedule_arguments())
    assert str(error.value) == message


@pytest.mark.parametrize("key", ["semantic_model_ids", "report_ids"])
def test_reporting_outputs_must_be_maps_not_values_that_can_be_silently_ignored(key):
    document = _resolved_manifest().to_document()
    with pytest.raises(reconcile.ReconcileError) as error:
        reconcile.merge_terraform_output(document, {key: {"value": "invalid"}})
    assert str(error.value) == "Terraform reporting outputs must be objects"


@pytest.mark.parametrize("document_key,artifact,output", [
    ("analytics_reporting", "semantic_model", "semantic_model_ids"),
    ("analytics_reporting", "report", "report_ids"),
    ("operational_reporting", "semantic_model", "semantic_model_ids"),
    ("operational_reporting", "report", "report_ids"),
])
def test_reporting_output_conflicts_name_the_exact_artifact(document_key, artifact, output):
    document = _resolved_manifest().to_document()
    before = copy.deepcopy(document)
    bundle = "analytics" if document_key == "analytics_reporting" else "operational"
    with pytest.raises(reconcile.ReconcileError) as error:
        reconcile.merge_terraform_output(document, {output: {"value": {bundle: _uid("another")}}})
    assert str(error.value) == f"{document_key}.{artifact}.target_item_id conflicts with the configured deployment"
    assert document == before


@pytest.mark.parametrize("kind,message", [
    ("workspace", "schedule reconciliation requires a workspace ID"),
    ("role", "roles.control.item_id is unresolved"),
])
def test_schedule_preflight_error_does_not_leave_partially_written_schedules(kind, message):
    manifest = _resolved_manifest()
    if kind == "workspace":
        manifest = dataclasses.replace(manifest, workspace_id=None)
    else:
        roles = dict(manifest.roles)
        roles["control"] = dataclasses.replace(roles["control"], item_id=None)
        manifest = dataclasses.replace(manifest, roles=roles)
    client = _FakeClient()
    with pytest.raises(reconcile.ReconcileError) as error:
        reconcile._reconcile_all_schedules(client, manifest, enabled=False, now=_NOW)
    assert str(error.value) == message
    assert client.calls == []


def test_all_schedule_requests_keep_the_configured_ids_and_explicit_start_time():
    manifest = _resolved_manifest()
    client = _FakeClient()
    result = reconcile._reconcile_all_schedules(client, manifest, enabled=False, now=_NOW)
    assert set(result) == reconcile.SJD_ROLES
    expected_start = "2026-01-01T00:05:00Z"
    expected_end = "2035-12-30T00:05:00Z"
    for role in reconcile.SCHEDULED_ROLES:
        item_id = manifest.role(role).item_id
        path = f"/workspaces/{manifest.workspace_id}/items/{item_id}/jobs/sparkjob/schedules"
        request = next(call for call in client.calls if call[0] == "POST" and call[1] == path)
        assert request[2] == {
            "configuration": {
                "startDateTime": expected_start, "endDateTime": expected_end,
                "interval": manifest.role(role).schedule.interval_minutes,
                "localTimeZoneId": "UTC", "type": "Cron",
            },
            "enabled": False,
            "executionData": {
                "commandLineArguments": manifest.schedule_arguments(role, enabled=False),
            },
        }


@pytest.mark.parametrize("failed_rollback", [[], ["control", "refresh"]])
def test_workflow_rollback_diagnostic_keeps_all_unconfirmed_roles_and_original_cause(failed_rollback):
    value = _resolved_manifest()
    value = dataclasses.replace(
        value,
        release=dataclasses.replace(value.release, manifest_sha256="a" * 64, receipt_sha256="b" * 64),
    )
    client = _FakeClient()
    original = client.run_operation
    failed = False

    def operation(method, path, *, action, value, accepted):
        nonlocal failed
        role = action.split(":")[1]
        if role == "dispatcher" and value["enabled"]:
            failed = True
            return DeploymentEvidence(action=action, status="failed", error="original failure")
        if failed and role in failed_rollback:
            return DeploymentEvidence(action=action, status="failed", error="rollback failure")
        return original(method, path, action=action, value=value, accepted=accepted)

    client.run_operation = operation
    with pytest.raises(reconcile.ReconcileError) as error:
        reconcile._reconcile_all_schedules(client, value, enabled=True, now=_NOW)
    assert str(error.value) == (
        "Schedule activation failed; could not confirm disabled schedules for "
        "control, refresh. Inspect the workspace before retrying."
        if failed_rollback else
        "Schedule activation failed; recurring schedules were disabled again. "
        "Previously accepted jobs are not cancelled."
    )
    assert str(error.value.__cause__) == "dispatcher schedule reconciliation failed: original failure"


def test_empty_foundation_document_can_be_completed_from_actual_provisioning_outputs():
    result = reconcile.merge_terraform_output({}, {
        "workspace_id": {"value": _uid("ws")}, "environment_id": {"value": _uid("env")},
        "lakehouse_id": {"value": _uid("lakehouse")},
    })
    assert result == {
        "workspace_id": _uid("ws"), "fabric_environment_id": _uid("env"),
        "lakehouse": {"lakehouse_id": _uid("lakehouse")},
    }


def test_reporting_merge_does_not_stop_when_only_the_second_bundle_is_present():
    document = _resolved_manifest().to_document()
    document.pop("analytics_reporting")
    document["operational_reporting"]["semantic_model"]["target_item_id"] = None
    result = reconcile.merge_terraform_output(document, {
        "semantic_model_ids": {"value": {"operational": _uid("new-operational")}},
    })
    assert result["operational_reporting"]["semantic_model"]["target_item_id"] == _uid("new-operational")


@pytest.mark.parametrize("identifier", [None, "", 7])
def test_discovered_schedule_id_must_be_a_nonempty_string(identifier):
    client = Mock()
    client.list_pages.side_effect = [[], [{"id": identifier}]]
    client.run_operation.return_value = DeploymentEvidence(
        action="schedule:control", status="succeeded", resource=None,
    )
    with pytest.raises(reconcile.ReconcileError) as error:
        reconcile.reconcile_role_schedule(client, **schedule_arguments())
    assert str(error.value) == "control schedule readback omitted its ID"
    client.request_json.assert_not_called()


def test_default_schedule_arguments_do_not_invent_an_unrequested_command():
    client = _FakeClient()
    reconcile.reconcile_role_schedule(
        client, ws="workspace", role="control", item_id="item",
        enabled=False, interval_minutes=5, now=_NOW,
    )
    request = next(call for call in client.calls if call[0] == "POST")
    assert request[2]["executionData"] == {"commandLineArguments": ""}


@pytest.mark.parametrize("identifier", [None, "", 7])
def test_existing_schedule_missing_identity_is_rejected_before_update(identifier):
    client = Mock()
    client.list_pages.return_value = [{"id": identifier}]
    with pytest.raises(reconcile.ReconcileError) as error:
        reconcile.reconcile_role_schedule(client, **schedule_arguments())
    assert str(error.value) == "control schedule readback omitted its ID"
    client.run_operation.assert_not_called()
    client.request_json.assert_not_called()


def test_empty_created_schedule_id_can_be_recovered_by_unique_discovery():
    client = Mock()
    client.list_pages.side_effect = [[], [{"id": "discovered"}]]
    acknowledgement = DeploymentEvidence(
        action="schedule:control", status="succeeded", resource={"id": ""},
    )
    client.run_operation.return_value = acknowledgement
    expected = reconcile._schedule_payload(interval=5, enabled=False, args="checked", now=_NOW)
    observed = {"id": "discovered", **expected}
    client.request_json.return_value = observed
    result = reconcile.reconcile_role_schedule(client, **schedule_arguments())
    assert result.resource["id"] == "discovered"
    assert result.resource["readback"] == observed
    assert client.list_pages.call_count == 2
