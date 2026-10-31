"""Local customer-demo safety checks and exact-input execution evidence."""

from __future__ import annotations

import json
import base64
import re
import shlex
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from people_counter.adls import PublicationConflictError, PublicationStorage
from people_counter.manifest import ManifestPlan
from people_counter.manifest_publisher import CheckpointStore, PublisherConfig, publish_plan


def azure_cli(arguments: list[str]) -> Any:
    completed = subprocess.run(
        ["az", *arguments, "--output", "json", "--only-show-errors"],
        capture_output=True, text=True, timeout=120, check=False,
    )
    if completed.returncode:
        raise RuntimeError(f"Azure CLI failed: {completed.stderr.strip()}")
    return json.loads(completed.stdout) if completed.stdout.strip() else {}


def ensure_capacity(
    resource_id: str,
    *,
    allow_resume: bool = False,
    cli: Callable[[list[str]], Any] = azure_cli,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    timeout: float = 600,
) -> str:
    deadline = monotonic() + timeout
    resume_requested = False
    while True:
        capacity = cli(["resource", "show", "--ids", resource_id, "--api-version", "2023-11-01"])
        state = capacity["properties"]["state"]
        if state == "Active":
            return state
        if state == "Paused":
            if not allow_resume and not resume_requested:
                raise RuntimeError("Capacity is paused. Approve capacity resume; F64 billing will start.")
            if not resume_requested:
                cli(["rest", "--method", "post", "--url",
                     f"https://management.azure.com{resource_id}/resume?api-version=2023-11-01"])
                resume_requested = True
        elif state not in {"Resuming", "Updating", "Provisioning"}:
            raise RuntimeError(f"Capacity cannot run the demo: {state}")
        if monotonic() >= deadline:
            raise TimeoutError("Capacity did not become Active before the timeout")
        sleep(10)


def ensure_storage_role(
    storage_resource_id: str,
    filesystem: str,
    principal_id: str,
    principal_type: str,
    *,
    allow_grant: bool = False,
    cli: Callable[[list[str]], Any] = azure_cli,
) -> None:
    scope = f"{storage_resource_id}/blobServices/default/containers/{filesystem}"
    command = ["role", "assignment", "list", "--assignee", principal_id,
               "--scope", scope, "--include-inherited"]
    if principal_type == "User":
        command.append("--include-groups")
    assignments = cli(command)
    if any(row["roleDefinitionName"] in {
        "Storage Blob Data Contributor", "Storage Blob Data Owner",
    } for row in assignments):
        return
    if not allow_grant:
        raise RuntimeError(
            "The CLI identity needs Storage Blob Data Contributor on the videos container. "
            "An Owner/User Access Administrator must approve the scoped grant. "
            "Custom-role/ACL access requires operator review; it is not inferred."
        )
    cli(["role", "assignment", "create", "--assignee-object-id", principal_id,
         "--assignee-principal-type", principal_type,
         "--role", "Storage Blob Data Contributor", "--scope", scope])


def _verify_staged(storage: PublicationStorage, plan: ManifestPlan) -> None:
    remote = storage.stat(plan.staging_video_path)
    if remote is None or remote.size != plan.inspection.size_bytes:
        raise PublicationConflictError(f"Missing or incomplete staged video: {plan.staging_video_path}")
    if storage.sha256(plan.staging_video_path) != plan.inspection.sha256:
        raise PublicationConflictError(f"Staged video checksum mismatch: {plan.staging_video_path}")


def stage_videos(storage: PublicationStorage, plans: Sequence[ManifestPlan]) -> None:
    for plan in plans:
        if storage.stat(plan.incoming_video_path) or storage.stat(plan.incoming_manifest_path):
            raise PublicationConflictError(
                f"Input already exists in incoming: {plan.relative_path}. "
                "Resume promotion/monitoring, or use a new RUN_LABEL for a new arrival."
            )
        if storage.stat(plan.staging_video_path) is None:
            storage.upload_file(
                plan.local_path, plan.staging_video_path, plan.content_type,
                plan.inspection.size_bytes, plan.inspection.sha256,
            )
        _verify_staged(storage, plan)


def promote_staged(
    storage: PublicationStorage,
    checkpoint: CheckpointStore,
    config: PublisherConfig,
    plans: Sequence[ManifestPlan],
) -> int:
    for plan in plans:
        if storage.stat(plan.incoming_video_path) is None:
            _verify_staged(storage, plan)
    return sum(publish_plan(storage, checkpoint, config, plan) for plan in plans)


def read_demo_delta(
    connection: Any,
    table_prefix_uri: str,
    suffix: str,
    columns: Sequence[str],
    filters: Sequence[tuple[str, str, Any]] | None = None,
) -> list[dict[str, Any]]:
    if suffix not in {"work", "gold_video", "gold_work_operations"}:
        raise ValueError("Only demo control/result tables may be read")
    if not columns or any(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column) is None for column in columns):
        raise ValueError("Columns must be nonempty SQL identifiers")
    predicates = []
    parameters: list[Any] = [table_prefix_uri + suffix]
    for column, operator, value in filters or ():
        if (column, operator) == ("created_at", ">="):
            predicates.append('"created_at" >= ?')
        elif (column, operator) == ("work_id", "in"):
            predicates.append('"work_id" IN (SELECT unnest(?))')
        else:
            raise ValueError("Only created_at >= and work_id in filters are supported")
        parameters.append(value)
    projection = ", ".join(f'"{column}"' for column in columns)
    query = f"SELECT {projection} FROM delta_scan(?)"
    if predicates:
        query += " WHERE " + " AND ".join(predicates)
    cursor = connection.execute(query, parameters)
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _running_arrival_node(nodes: Sequence[Mapping[str, Any]], label: str) -> Mapping[str, Any]:
    if len(nodes) != 1:
        raise RuntimeError(f"Expected exactly one {label}; found {len(nodes)}")
    node = nodes[0]
    if node.get("status") != "Running" or node.get("error") is not None:
        raise RuntimeError(
            f"{label} {node.get('name')!r} is {node.get('status')}: {node.get('error')}. "
            "Open the Eventstream in Fabric Live view and activate the arrival destination "
            "if paused; verify its source and stream, then rerun preflight. "
            "For retained events, review 'When streaming was last stopped' before activation. "
            "Do not re-upload or delete incoming files to force another event."
        )
    return node


def _verify_arrival_rule(
    client: Any, workspace_id: str, activator_id: str, intake_id: str,
) -> None:
    from people_counter.fabric_activator import validate_reflex_definition

    readback = client.run_operation(
        "POST", f"/workspaces/{workspace_id}/items/{activator_id}/getDefinition",
        action="verify_arrival_rule",
    )
    if readback.status != "succeeded" or readback.resource is None:
        raise RuntimeError(f"Cannot verify arrival Activator rule: {readback.error}")
    evidence = validate_reflex_definition(readback.resource)
    rules = [rule for rule in evidence.rules if rule.name == "run_pc_event_intake_on_manifest_renamed"]
    if len(rules) != 1:
        raise RuntimeError(f"Expected exactly one manifest-arrival rule; found {len(rules)}")
    rule = rules[0]
    if not rule.enabled:
        raise RuntimeError("Manifest-arrival Activator rule is disabled. Start it in Fabric, then rerun preflight.")
    actions = [action for action in evidence.actions if action.rule_id == rule.rule_id]
    if len(actions) != 1 or (
        actions[0].kind, actions[0].target_workspace_id,
        actions[0].target_item_id, actions[0].target_item_type,
    ) != ("FabricItemInvocation", workspace_id, intake_id, "Pipeline"):
        raise RuntimeError("Manifest-arrival Activator rule must invoke only the selected intake pipeline")


def verify_arrival_runtime(
    client: Any,
    *,
    workspace_id: str,
    eventstream_id: str,
    activator_id: str,
    intake_id: str,
    storage_resource_id: str,
) -> dict[str, Any]:
    topology = client.request_json(
        "GET", f"/workspaces/{workspace_id}/eventstreams/{eventstream_id}/topology",
    )
    sources = [
        node for node in topology.get("sources", [])
        if node.get("type") == "AzureBlobStorageEvents" and any(
            event.get("azureResourceId", "").lower() == storage_resource_id.lower()
            for event in node.get("properties", {}).get("azureBlobStorageEvents", [])
        )
    ]
    destinations = [
        node for node in topology.get("destinations", [])
        if node.get("type") == "Activator"
        and node.get("properties", {}).get("workspaceId") == workspace_id
        and node.get("properties", {}).get("itemId") == activator_id
    ]
    source = _running_arrival_node(sources, "arrival storage source")
    destination = _running_arrival_node(destinations, "arrival Activator destination")
    stream = _running_arrival_node(topology.get("streams", []), "arrival stream")
    _verify_arrival_rule(client, workspace_id, activator_id, intake_id)
    return {
        "source_status": source["status"], "stream_status": stream["status"],
        "destination_status": destination["status"], "rule_enabled": True,
    }


def _read_definition_part(client: Any, workspace_id: str, item_id: str, path: str) -> Any:
    readback = client.run_operation(
        "POST", f"/workspaces/{workspace_id}/items/{item_id}/getDefinition",
        action="verify_release_alignment",
    )
    if readback.status != "succeeded" or readback.resource is None:
        raise RuntimeError(f"Cannot read release configuration: {readback.error}")
    parts = readback.resource.get("definition", {}).get("parts", [])
    matches = [part for part in parts if part.get("path") == path]
    if len(matches) != 1 or matches[0].get("payloadType") != "InlineBase64":
        raise RuntimeError(f"Expected exactly one InlineBase64 {path} definition part")
    try:
        return json.loads(base64.b64decode(matches[0]["payload"], validate=True))
    except (ValueError, KeyError) as error:
        raise RuntimeError(f"Invalid release configuration in {path}") from error


def verify_release_alignment(
    client: Any, workspace_id: str, intake_id: str, dispatcher_id: str,
) -> str:
    pipeline = _read_definition_part(client, workspace_id, intake_id, "pipeline-content.json")
    schedules = _read_definition_part(client, workspace_id, dispatcher_id, ".schedules")
    activities = [
        row for row in pipeline["properties"]["activities"]
        if row.get("name") == "ValidateManifest"
    ]
    if len(activities) != 1 or len(schedules.get("schedules", [])) != 1:
        raise RuntimeError("Release alignment requires one intake validation activity and one Dispatcher schedule")
    intake = activities[0]["typeProperties"]["parameters"]["RELEASE_MANIFEST_SHA256"]["value"]
    arguments = shlex.split(schedules["schedules"][0]["executionData"]["commandLineArguments"])
    flag = "--release-manifest-sha256"
    if arguments.count(flag) != 1 or arguments.index(flag) + 1 >= len(arguments):
        raise RuntimeError("Dispatcher schedule must provide one release-manifest digest")
    dispatcher = arguments[arguments.index(flag) + 1]
    if not isinstance(intake, str) or re.fullmatch(r"[0-9a-f]{64}", intake) is None or intake != dispatcher:
        raise RuntimeError(
            f"Intake/Dispatcher release mismatch: intake={intake}, dispatcher={dispatcher}. "
            "Deploy both from one verified release before uploading files."
        )
    return dispatcher


def verify_compute_policy(client: Any, workspace_id: str, environment_id: str) -> None:
    from people_counter.fabric_sjd_definition import validate_reviewed_compute_settings

    settings = client.request_json(
        "GET", f"/workspaces/{workspace_id}/environments/{environment_id}/sparkcompute?beta=false",
    )
    validate_reviewed_compute_settings(settings)


def raise_processing_failure(
    diagnostics: Sequence[Mapping[str, Any]], work_ids: Sequence[str], *, started_at: float,
) -> None:
    for diagnostic in diagnostics:
        if float(diagnostic["captured_at"]) < started_at:
            continue
        message = str(diagnostic["error_message"])
        shared_blocker = (
            diagnostic["error_type"] == "ReleaseProvenanceError"
            or message.startswith(("Fabric SJD requires ", "dispatcher release binding "))
        )
        if shared_blocker or any(work_id in message for work_id in work_ids):
            raise RuntimeError(
                f"Processing failed: {diagnostic['error_type']}: {message}. "
                "A Completed Fabric job does not override this work-specific failure; "
                "stop polling and investigate the release/attempt diagnostic."
            )


def asset_progress(
    plans: Sequence[ManifestPlan],
    work_rows: Sequence[Mapping[str, Any]],
    gold_rows: Sequence[Mapping[str, Any]],
    *,
    started_at: float,
    shortcut: str = "videos",
) -> list[dict[str, Any]]:
    progress = []
    for plan in plans:
        source = f"/lakehouse/default/Files/{shortcut}/{plan.incoming_video_path}"
        matches = []
        for row in work_rows:
            payload = json.loads(row["payload_json"])
            if payload.get("source_video") == source and payload.get("source_sha256") == plan.asset_version:
                matches.append(row)
        if len(matches) > 1:
            raise RuntimeError(f"Multiple registrations match {plan.relative_path}; investigate before presenting results")
        result: dict[str, Any] = {
            "file": plan.relative_path, "asset_id": plan.asset_id,
            "sha256": plan.asset_version, "state": "WAITING_REGISTRATION",
        }
        if matches:
            row = matches[0]
            result.update(work_id=row["work_id"], work_status=row["status"],
                          committed_attempt_id=row["committed_attempt_id"])
            if float(row["created_at"]) < started_at:
                result["state"] = "PREVIOUS_RUN"
            elif row["status"] in {"DEAD", "DEAD_LETTER", "FAILED", "QUARANTINED"}:
                raise RuntimeError(f"Demo work {row['work_id']} is {row['status']}: {row.get('last_error')}")
            elif row["status"] == "SUCCEEDED" and row["committed_attempt_id"]:
                committed = [
                    gold for gold in gold_rows
                    if gold["work_id"] == row["work_id"]
                    and gold["attempt_id"] == row["committed_attempt_id"]
                ]
                result["state"] = "WAITING_RESULTS"
                if len(committed) == 1:
                    result.update(committed[0])
                    result["state"] = "COMPLETED"
            else:
                result["state"] = row["status"]
        progress.append(result)
    return progress
