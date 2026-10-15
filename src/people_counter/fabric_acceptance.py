"""Live deployment verification for Microsoft Fabric environments."""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import re
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote

from people_counter.fabric_automation import (
    FabricApiError,
    HTTPTransport,
    make_resource_token_provider,
    redact_evidence,
)
from people_counter.fabric_deployment_manifest import (
    DeploymentEnvironment,
    DeploymentManifest,
    ON_DEMAND_ROLES,
    SCHEDULED_ROLES,
    SJD_ROLES,
    parse_deployment_manifest,
)
from people_counter.fabric_reconcilers import (
    EVIDENCE_SCHEMA as POSTCONFIGURE_EVIDENCE_SCHEMA,
    RELEASE_MANIFEST_SCHEMA,
    RUNTIME_CONTRACT_SCHEMA,
    merge_terraform_output,
)
from people_counter.fabric_sjd import BOOTSTRAP_TABLE_SUFFIXES

ACCEPTANCE_EVIDENCE_SCHEMA = "people-counter-fabric-deployment-verification-v1"
_SJD_DEFINITION_ROOT = (
    Path(__file__).resolve().parents[2] / "fabric" / "spark-job-definitions"
)
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_DEFINITION_ROUTES = {
    "SemanticModel": "semanticModels",
    "Report": "reports",
    "SparkJobDefinition": "sparkJobDefinitions",
    "KQLDatabase": "kqlDatabases",
    "KQLQueryset": "kqlQuerysets",
    "KQLDashboard": "kqlDashboards",
    "Eventhouse": "eventhouses",
    "Reflex": "reflexes",
}


class CleanroomAcceptanceError(RuntimeError):
    """The deployed environment does not satisfy the verification contract."""


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _definition_hashes(value: object) -> dict[str, str]:
    hashes: dict[str, str] = {}

    def walk(item: object, path: str) -> None:
        if isinstance(item, Mapping):
            for key, child in item.items():
                qualified = f"{path}.{key}" if path else str(key)
                if (
                    str(key).lower().endswith("sha256")
                    and isinstance(child, str)
                    and re.fullmatch(r"[0-9a-fA-F]{64}", child)
                ):
                    hashes[qualified] = child.lower()
                else:
                    walk(child, qualified)
        elif isinstance(item, list):
            for index, child in enumerate(item):
                walk(child, f"{path}[{index}]")

    walk(value, "")
    return hashes


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CleanroomAcceptanceError(f"{label} must be an object")
    return value


def _terraform_value(outputs: Mapping[str, Any], key: str) -> Any:
    entry = outputs.get(key)
    if not isinstance(entry, Mapping):
        raise CleanroomAcceptanceError(f"Terraform output {key!r} is absent")
    return entry.get("value")


def _required_mapping(outputs: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    return _mapping(_terraform_value(outputs, key), f"Terraform output {key}")


def _definition_parts(
    value: Mapping[str, Any], label: str
) -> dict[str, bytes]:
    definition = _mapping(value.get("definition"), f"{label} definition")
    parts = definition.get("parts")
    if not isinstance(parts, list) or not parts:
        raise CleanroomAcceptanceError(f"{label} definition has no parts")
    decoded: dict[str, bytes] = {}
    seen: set[str] = set()
    for index, raw in enumerate(parts):
        part = _mapping(raw, f"{label} definition part {index}")
        path = part.get("path")
        if not isinstance(path, str) or not path or path in seen:
            raise CleanroomAcceptanceError(f"{label} definition part path is invalid")
        seen.add(path)
        if part.get("payloadType") != "InlineBase64":
            raise CleanroomAcceptanceError(
                f"{label} definition part {path!r} is not InlineBase64"
            )
        payload = part.get("payload")
        if not isinstance(payload, str):
            raise CleanroomAcceptanceError(
                f"{label} definition part {path!r} has no payload"
            )
        try:
            content = base64.b64decode(payload, validate=True)
        except (ValueError, binascii.Error) as error:
            raise CleanroomAcceptanceError(
                f"{label} definition part {path!r} is invalid base64"
            ) from error
        decoded[path] = content
    return decoded


def _definition_text(value: Mapping[str, Any], label: str) -> str:
    return "\n".join(
        item
        for path, content in _definition_parts(value, label).items()
        for item in (path, content.decode("utf-8", errors="replace"))
    )


def _get_definition(
    client: Any,
    *,
    workspace_id: str,
    item_id: str,
    item_type: str,
    label: str,
) -> str:
    route = _DEFINITION_ROUTES[item_type]
    evidence = client.run_operation(
        "POST",
        f"/workspaces/{workspace_id}/{route}/{item_id}/getDefinition",
        action=f"acceptance_get_definition:{label}",
        accepted=(200, 202),
    )
    if evidence.status != "succeeded" or evidence.resource is None:
        raise CleanroomAcceptanceError(
            f"{label} definition readback failed: {evidence.error}"
        )
    return _definition_text(evidence.resource, label)


def _get_definition_parts(
    client: Any,
    *,
    workspace_id: str,
    item_id: str,
    label: str,
) -> dict[str, bytes]:
    evidence = client.run_operation(
        "POST",
        f"/workspaces/{workspace_id}/sparkJobDefinitions/{item_id}/getDefinition"
        "?format=SparkJobDefinitionV2",
        action=f"acceptance_get_definition:{label}",
        accepted=(200, 202),
    )
    if evidence.status != "succeeded" or evidence.resource is None:
        raise CleanroomAcceptanceError(
            f"{label} definition readback failed: {evidence.error}"
        )
    return _definition_parts(evidence.resource, label)


def _assert_contains(text: str, expected: Sequence[str], label: str) -> None:
    missing = [value for value in expected if value not in text]
    if missing:
        raise CleanroomAcceptanceError(
            f"{label} definition is missing references: {missing!r}"
        )


def _verify_item(
    client: Any,
    *,
    workspace_id: str,
    item_id: str,
    expected_type: str,
    label: str,
) -> Mapping[str, Any]:
    item = client.request_json(
        "GET", f"/workspaces/{workspace_id}/items/{item_id}"
    )
    if item.get("id") not in {None, item_id} or item.get("type") != expected_type:
        raise CleanroomAcceptanceError(
            f"{label} item readback does not match {expected_type}/{item_id}"
        )
    return item


def _verify_sjds(
    client: Any,
    manifest: DeploymentManifest,
    outputs: Mapping[str, Any],
) -> dict[str, Any]:
    ids = _required_mapping(outputs, "spark_job_definition_ids")
    if set(ids) != SJD_ROLES:
        raise CleanroomAcceptanceError(
            f"SJD outputs must contain exactly {sorted(SJD_ROLES)!r}"
        )
    if manifest.workspace_id is None or manifest.fabric_environment_id is None:
        raise CleanroomAcceptanceError("SJD verification requires workspace/environment IDs")
    lakehouse_id = manifest.lakehouse.lakehouse_id
    if lakehouse_id is None:
        raise CleanroomAcceptanceError("SJD verification requires a Lakehouse ID")
    definitions: dict[str, Any] = {}
    for role in sorted(SJD_ROLES):
        item_id = str(ids[role])
        _verify_item(
            client,
            workspace_id=manifest.workspace_id,
            item_id=item_id,
            expected_type="SparkJobDefinition",
            label=f"SJD {role}",
        )
        parts = _get_definition_parts(
            client,
            workspace_id=manifest.workspace_id,
            item_id=item_id,
            label=f"SJD {role}",
        )
        source = parts.get("Main/main.py")
        canonical_path = _SJD_DEFINITION_ROOT / role / "Main" / "main.py"
        try:
            canonical_source = canonical_path.read_bytes()
        except OSError as error:
            raise CleanroomAcceptanceError(
                f"SJD {role} canonical source could not be read"
            ) from error
        if source is None or hashlib.sha256(source).digest() != hashlib.sha256(
            canonical_source
        ).digest():
            raise CleanroomAcceptanceError(
                f"SJD {role} Main/main.py does not match the canonical source hash"
            )
        metadata = parts.get("SparkJobDefinitionV1.json", b"").decode(
            "utf-8", errors="replace"
        )
        _assert_contains(
            metadata,
            (
                manifest.fabric_environment_id,
                lakehouse_id,
            ),
            f"SJD {role}",
        )
        definitions[role] = {
            "id": item_id,
            "referencesVerified": True,
            "sourceSha256": hashlib.sha256(canonical_source).hexdigest(),
        }
    return definitions


def _verify_schedules(
    client: Any,
    manifest: DeploymentManifest,
    outputs: Mapping[str, Any],
) -> dict[str, Any]:
    if manifest.workspace_id is None:
        raise CleanroomAcceptanceError("schedule verification requires a workspace ID")
    ids = _required_mapping(outputs, "spark_job_definition_ids")
    result: dict[str, Any] = {}
    for role in sorted(SJD_ROLES):
        schedules = client.list_pages(
            f"/workspaces/{manifest.workspace_id}/items/{ids[role]}"
            "/jobs/sparkjob/schedules"
        )
        expected = 1 if role in SCHEDULED_ROLES else 0
        if len(schedules) != expected:
            raise CleanroomAcceptanceError(
                f"{role} has {len(schedules)} schedules; expected {expected}"
            )
        if any(schedule.get("enabled") is not False for schedule in schedules):
            raise CleanroomAcceptanceError(
                f"{role} schedules must be disabled before the final gate"
            )
        result[role] = {
            "count": len(schedules),
            "enabled": False if schedules else None,
        }
    if set(result) != SCHEDULED_ROLES | ON_DEMAND_ROLES:
        raise AssertionError("schedule role partition changed during validation")
    return result


def _onelake_path(manifest: DeploymentManifest, relative: str) -> str:
    if manifest.workspace_id is None or manifest.lakehouse.lakehouse_id is None:
        raise CleanroomAcceptanceError("OneLake verification requires resolved IDs")
    files_path = manifest.lakehouse.file_path(relative)
    return "/" + quote(
        f"{manifest.workspace_id}/{manifest.lakehouse.lakehouse_id}/{files_path}",
        safe="/",
    )


def _live_shortcut_identity(value: Mapping[str, Any]) -> tuple[str, ...]:
    target = _mapping(value.get("target"), "live shortcut target")
    adls = _mapping(target.get("adlsGen2"), "live shortcut ADLS Gen2 target")
    return (
        str(value.get("path")).lstrip("/"),
        str(value.get("name")),
        "AdlsGen2",
        str(adls.get("location")),
        str(adls.get("subpath")),
        str(adls.get("connectionId")),
    )


def _expected_shortcut_identity(
    key: str, value: object
) -> tuple[str, ...]:
    shortcut = _mapping(value, f"managed_shortcuts.{key}")
    if shortcut.get("target_type") != "AdlsGen2":
        raise CleanroomAcceptanceError(
            f"managed_shortcuts.{key}.target_type must be AdlsGen2"
        )
    return (
        str(shortcut.get("path")),
        str(shortcut.get("name")),
        "AdlsGen2",
        str(shortcut.get("location")),
        str(shortcut.get("subpath")),
        str(shortcut.get("connection_id")),
    )


def _list_lakehouse_tables(
    client: Any,
    *,
    workspace_id: str,
    lakehouse_id: str,
) -> list[Mapping[str, Any]]:
    base_path = (
        f"/{workspace_id}/{lakehouse_id}/api/2.1/unity-catalog/tables"
        f"?catalog_name={lakehouse_id}&schema_name=dbo"
    )
    path = base_path
    seen_tokens: set[str] = set()
    tables: list[Mapping[str, Any]] = []
    while True:
        page = client.request_json("GET", path)
        values = page.get("tables")
        if not isinstance(values, list) or any(
            not isinstance(value, Mapping) for value in values
        ):
            raise CleanroomAcceptanceError(
                "OneLake Delta table response must contain a tables array"
            )
        tables.extend(values)
        token = page.get("next_page_token")
        if token is None:
            return tables
        if not isinstance(token, str) or not token or token in seen_tokens:
            raise CleanroomAcceptanceError(
                "OneLake Delta table pagination token is invalid"
            )
        seen_tokens.add(token)
        next_path = f"{base_path}&page_token={quote(token, safe='')}"
        if next_path == path:
            raise CleanroomAcceptanceError(
                "OneLake Delta table pagination token is invalid"
            )
        path = next_path


def _verify_lakehouse(
    client: Any,
    onelake_client: Any,
    onelake_table_client: Any,
    manifest: DeploymentManifest,
    outputs: Mapping[str, Any],
    runtime_contract: Mapping[str, Any],
    *,
    commit_sha: str,
    terraform_sha: str,
) -> dict[str, Any]:
    if manifest.workspace_id is None or manifest.lakehouse.lakehouse_id is None:
        raise CleanroomAcceptanceError("Lakehouse verification requires resolved IDs")
    _verify_item(
        client,
        workspace_id=manifest.workspace_id,
        item_id=manifest.lakehouse.lakehouse_id,
        expected_type="Lakehouse",
        label="Lakehouse",
    )
    tables = _list_lakehouse_tables(
        onelake_table_client,
        workspace_id=manifest.workspace_id,
        lakehouse_id=manifest.lakehouse.lakehouse_id,
    )
    expected_tables = {
        manifest.lakehouse.table(suffix) for suffix in BOOTSTRAP_TABLE_SUFFIXES
    }
    actual_tables = {str(table.get("name")) for table in tables}
    if expected_tables - actual_tables:
        raise CleanroomAcceptanceError(
            f"Lakehouse tables are missing: {sorted(expected_tables - actual_tables)!r}"
        )
    lakehouse_contract = _mapping(runtime_contract.get("lakehouse", {}), "lakehouse")
    folders = lakehouse_contract.get("folders", [])
    if not isinstance(folders, list):
        raise CleanroomAcceptanceError("lakehouse.folders must be an array")
    for folder in folders:
        status, _, _ = onelake_client.request_response(
            "HEAD", _onelake_path(manifest, str(folder)), expected=(200, 404)
        )
        if status != 200:
            raise CleanroomAcceptanceError(
                f"OneLake folder {folder!r} is missing"
            )
    shortcuts = client.list_pages(
        f"/workspaces/{manifest.workspace_id}/items/"
        f"{manifest.lakehouse.lakehouse_id}/shortcuts"
    )
    shortcut_outputs = _required_mapping(outputs, "managed_shortcuts")
    expected_shortcuts = Counter(
        _expected_shortcut_identity(str(key), value)
        for key, value in shortcut_outputs.items()
    )
    live_shortcuts = Counter(
        _live_shortcut_identity(item) for item in shortcuts
    )
    if live_shortcuts != expected_shortcuts:
        raise CleanroomAcceptanceError(
            "live Lakehouse shortcuts do not exactly match Terraform outputs"
        )
    release_path = _onelake_path(manifest, f"releases/{commit_sha}.json")
    status, _, body = onelake_client.request_response(
        "GET", release_path, expected=(200, 404)
    )
    if status != 200:
        raise CleanroomAcceptanceError("immutable release manifest is missing")
    try:
        release = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CleanroomAcceptanceError("release manifest is invalid JSON") from error
    if (
        release.get("schema") != RELEASE_MANIFEST_SCHEMA
        or release.get("commitSha") != commit_sha
        or release.get("terraformOutputSha256") != terraform_sha
        or release.get("status") != "succeeded"
    ):
        raise CleanroomAcceptanceError(
            "release manifest provenance does not match this deployment"
        )
    return {
        "folderCount": len(folders),
        "releaseManifestSha256": _sha256(release),
        "shortcutCount": len(shortcuts),
        "tableCount": len(expected_tables),
    }


def _verify_reporting(
    client: Any,
    manifest: DeploymentManifest,
    outputs: Mapping[str, Any],
    postconfigure: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if manifest.workspace_id is None or manifest.lakehouse.lakehouse_id is None:
        raise CleanroomAcceptanceError("reporting verification requires resolved IDs")
    models = _required_mapping(outputs, "semantic_model_ids")
    reports = _required_mapping(outputs, "report_ids")
    if set(models) != {"analytics", "operational"} or set(reports) != set(models):
        raise CleanroomAcceptanceError(
            "reporting outputs must contain analytics and operational bundles"
        )
    result: dict[str, Any] = {}
    for bundle in sorted(models):
        model_id = str(models[bundle])
        report_id = str(reports[bundle])
        _verify_item(
            client,
            workspace_id=manifest.workspace_id,
            item_id=model_id,
            expected_type="SemanticModel",
            label=f"{bundle} semantic model",
        )
        model_text = _get_definition(
            client,
            workspace_id=manifest.workspace_id,
            item_id=model_id,
            item_type="SemanticModel",
            label=f"{bundle} semantic model",
        )
        _assert_contains(
            model_text,
            (manifest.workspace_id, manifest.lakehouse.lakehouse_id),
            f"{bundle} semantic model",
        )
        if "directlake" not in model_text.replace(" ", "").lower():
            raise CleanroomAcceptanceError(
                f"{bundle} semantic model is not bound for Direct Lake"
            )
        _verify_item(
            client,
            workspace_id=manifest.workspace_id,
            item_id=report_id,
            expected_type="Report",
            label=f"{bundle} report",
        )
        report_text = _get_definition(
            client,
            workspace_id=manifest.workspace_id,
            item_id=report_id,
            item_type="Report",
            label=f"{bundle} report",
        )
        _assert_contains(report_text, (model_id,), f"{bundle} report")
        for index, evidence in enumerate(postconfigure):
            refresh = _mapping(
                evidence.get("reporting_refresh", {}),
                f"postconfigure[{index}].reporting_refresh",
            ).get(bundle)
            if not isinstance(refresh, Mapping) or refresh.get("status") != "succeeded":
                raise CleanroomAcceptanceError(
                    f"{bundle} refresh did not succeed on deployment run {index + 1}"
                )
        result[bundle] = {
            "modelId": model_id,
            "refreshRuns": len(postconfigure),
            "reportId": report_id,
        }
    return result


def _verify_rti(
    client: Any,
    manifest: DeploymentManifest,
    outputs: Mapping[str, Any],
) -> dict[str, str]:
    if manifest.workspace_id is None:
        raise CleanroomAcceptanceError("RTI verification requires a workspace ID")
    specifications = {
        "eventhouse_id": "Eventhouse",
        "kql_database_id": "KQLDatabase",
        "kql_queryset_id": "KQLQueryset",
        "kql_dashboard_id": "KQLDashboard",
    }
    result: dict[str, str] = {}
    for output_name, item_type in specifications.items():
        item_id = _terraform_value(outputs, output_name)
        if not isinstance(item_id, str) or not item_id:
            raise CleanroomAcceptanceError(
                f"required Terraform output {output_name!r} is unresolved"
            )
        _verify_item(
            client,
            workspace_id=manifest.workspace_id,
            item_id=item_id,
            expected_type=item_type,
            label=item_type,
        )
        result[output_name] = item_id
    expected_references = {
        "kql_queryset_id": (result["kql_database_id"],),
        "kql_dashboard_id": (result["kql_database_id"],),
    }
    for output_name, references in expected_references.items():
        item_type = specifications[output_name]
        text = _get_definition(
            client,
            workspace_id=manifest.workspace_id,
            item_id=result[output_name],
            item_type=item_type,
            label=item_type,
        )
        _assert_contains(text, references, item_type)
    return result


def _verify_activator(
    client: Any,
    manifest: DeploymentManifest,
    outputs: Mapping[str, Any],
    postconfigure: Sequence[Mapping[str, Any]],
    *,
    require_action_checkpoint: bool = True,
) -> dict[str, Any]:
    if manifest.workspace_id is None:
        raise CleanroomAcceptanceError("Activator verification requires a workspace ID")
    activators = _required_mapping(outputs, "activator_ids")
    required_activators = ("operations",)
    resolved: dict[str, str] = {}
    for name in required_activators:
        activator_id = activators.get(name)
        if not isinstance(activator_id, str) or not activator_id:
            raise CleanroomAcceptanceError(f"{name} Activator output is unresolved")
        _verify_item(
            client,
            workspace_id=manifest.workspace_id,
            item_id=activator_id,
            expected_type="Reflex",
            label=f"{name} Activator",
        )
        resolved[name] = activator_id
    for index, evidence in enumerate(postconfigure):
        activator = evidence.get("activator")
        if not isinstance(activator, Mapping) or activator.get("status") != "succeeded":
            raise CleanroomAcceptanceError(
                f"Activator definition/action verification failed on run {index + 1}"
            )
        resource = activator.get("resource")
        if not isinstance(resource, Mapping):
            raise CleanroomAcceptanceError(
                f"Activator action evidence is absent on run {index + 1}"
            )
        if require_action_checkpoint:
            accepted = resource.get("actionExecutionVerified") is True
        else:
            accepted = (
                resource.get("actionExecutionVerified") is True
                or resource.get("actionExecutionRequired") is False
            )
        if not accepted:
            raise CleanroomAcceptanceError(
                f"Activator action evidence is absent on run {index + 1}"
            )
    return {
        "actionRuns": len(postconfigure),
        "id": resolved["operations"],
        "ids": resolved,
    }


def _verify_postconfigure(
    evidence: Sequence[Mapping[str, Any]],
    *,
    commit_sha: str,
) -> dict[str, Any]:
    if len(evidence) != 2:
        raise CleanroomAcceptanceError(
            "deployment verification requires exactly two evidence documents"
        )
    for index, item in enumerate(evidence):
        if (
            item.get("schema") != POSTCONFIGURE_EVIDENCE_SCHEMA
            or item.get("status") != "succeeded"
            or item.get("schedulesEnabled") is not False
        ):
            raise CleanroomAcceptanceError(
                f"postconfiguration run {index + 1} is not a successful safe-state run"
            )
        context = _mapping(item.get("context"), f"postconfigure[{index}].context")
        if context.get("commitSha") != commit_sha:
            raise CleanroomAcceptanceError(
                f"postconfiguration run {index + 1} commit does not match"
            )
        if item.get("operatorCheckpoints"):
            raise CleanroomAcceptanceError(
                f"postconfiguration run {index + 1} has unresolved checkpoints"
            )
    if evidence[0].get("resources") != evidence[1].get("resources"):
        raise CleanroomAcceptanceError(
            "deployment resource IDs changed between identical runs"
        )
    if evidence[0].get("context") != evidence[1].get("context"):
        raise CleanroomAcceptanceError(
            "deployment provenance changed between identical runs"
        )
    return {
        "repeatRunCount": 2,
        "resourceIdentityStable": True,
        "schedulesDisabledBeforeGate": True,
    }


def _managed_item_ids(outputs: Mapping[str, Any]) -> set[str]:
    scalar_outputs = (
        "lakehouse_id",
        "environment_id",
        "eventhouse_id",
        "kql_database_id",
        "kql_queryset_id",
        "kql_dashboard_id",
    )
    mapping_outputs = (
        "spark_job_definition_ids",
        "semantic_model_ids",
        "report_ids",
        "activator_ids",
    )
    identifiers = [
        value
        for key in scalar_outputs
        if isinstance((value := _terraform_value(outputs, key)), str) and value
    ]
    identifiers.extend(
        str(value)
        for key in mapping_outputs
        for value in _required_mapping(outputs, key).values()
    )
    expected_ids = set(identifiers)
    if len(expected_ids) != len(identifiers):
        raise CleanroomAcceptanceError("Terraform outputs contain duplicate item IDs")
    return expected_ids


def _verify_no_duplicate_items(
    client: Any,
    manifest: DeploymentManifest,
    outputs: Mapping[str, Any],
) -> dict[str, Any]:
    if manifest.workspace_id is None:
        raise CleanroomAcceptanceError("duplicate detection requires a workspace ID")
    expected_ids = _managed_item_ids(outputs)
    live = client.list_pages(f"/workspaces/{manifest.workspace_id}/items")
    by_id = {str(item.get("id")): item for item in live}
    if expected_ids - set(by_id):
        raise CleanroomAcceptanceError(
            f"managed items are missing: {sorted(expected_ids - set(by_id))!r}"
        )
    managed_names = [
        (str(by_id[item_id].get("type")), str(by_id[item_id].get("displayName")))
        for item_id in expected_ids
    ]
    live_names = Counter(
        (str(item.get("type")), str(item.get("displayName"))) for item in live
    )
    duplicated = sorted(name for name in managed_names if live_names[name] != 1)
    if duplicated:
        raise CleanroomAcceptanceError(
            f"managed item names are duplicated or ambiguous: {duplicated!r}"
        )
    return {"managedItemCount": len(expected_ids), "duplicates": 0}


def validate_cleanroom(
    client: Any,
    onelake_client: Any,
    onelake_table_client: Any,
    manifest: DeploymentManifest,
    *,
    terraform_output: Mapping[str, Any],
    runtime_contract: Mapping[str, Any],
    postconfigure_evidence: Sequence[Mapping[str, Any]],
    definition_evidence: Mapping[str, Any],
    commit_sha: str,
    terraform_drift_exit_code: int,
    require_reporting: bool,
    require_rti: bool,
    require_activator: bool,
    require_activator_action_checkpoint: bool = True,
) -> dict[str, Any]:
    """Validate a repeated deployment and return durable acceptance evidence."""
    if _COMMIT.fullmatch(commit_sha) is None:
        raise CleanroomAcceptanceError("commitSha must be a full lowercase commit SHA")
    if terraform_drift_exit_code != 0:
        raise CleanroomAcceptanceError(
            "repeat Terraform plan must exit 0 with zero drift"
        )
    if runtime_contract.get("schema") != RUNTIME_CONTRACT_SCHEMA:
        raise CleanroomAcceptanceError("unsupported runtime contract schema")
    postconfigure = _verify_postconfigure(
        postconfigure_evidence, commit_sha=commit_sha
    )
    context = _mapping(postconfigure_evidence[0].get("context"), "context")
    terraform_sha = _sha256(terraform_output)
    if context.get("terraformOutputSha256") != terraform_sha:
        raise CleanroomAcceptanceError(
            "postconfiguration Terraform output provenance does not match"
        )
    expected_definition_hashes = _mapping(
        context.get("definitionHashes"), "context.definitionHashes"
    )
    actual_definition_hashes = _definition_hashes(definition_evidence)
    if dict(expected_definition_hashes) != actual_definition_hashes:
        raise CleanroomAcceptanceError(
            "definition provenance does not match source validation evidence"
        )
    result = {
        "schema": ACCEPTANCE_EVIDENCE_SCHEMA,
        "status": "succeeded",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "stage": manifest.stage.value,
        "commitSha": commit_sha,
        "checks": {
            "terraform": {
                "driftExitCode": terraform_drift_exit_code,
                "outputSha256": terraform_sha,
            },
            "postconfigure": postconfigure,
            "items": _verify_no_duplicate_items(client, manifest, terraform_output),
            "sparkJobDefinitions": _verify_sjds(
                client, manifest, terraform_output
            ),
            "schedules": _verify_schedules(client, manifest, terraform_output),
            "lakehouse": _verify_lakehouse(
                client,
                onelake_client,
                onelake_table_client,
                manifest,
                terraform_output,
                runtime_contract,
                commit_sha=commit_sha,
                terraform_sha=terraform_sha,
            ),
        },
    }
    checks = result["checks"]
    if require_reporting:
        checks["reporting"] = _verify_reporting(
            client, manifest, terraform_output, postconfigure_evidence
        )
    else:
        checks["reporting"] = {"status": "not-required"}
    if require_rti:
        checks["realTimeIntelligence"] = _verify_rti(
            client, manifest, terraform_output
        )
    else:
        checks["realTimeIntelligence"] = {"status": "not-required"}
    if require_activator:
        checks["activator"] = _verify_activator(
            client,
            manifest,
            terraform_output,
            postconfigure_evidence,
            require_action_checkpoint=require_activator_action_checkpoint,
        )
    else:
        checks["activator"] = {"status": "not-required"}
    return redact_evidence(result)


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CleanroomAcceptanceError(
            f"{label} at {path} could not be read: {error}"
        ) from error
    if not isinstance(value, dict):
        raise CleanroomAcceptanceError(f"{label} at {path} must be an object")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-path", required=True, type=Path)
    parser.add_argument("--runtime-contract-path", required=True, type=Path)
    parser.add_argument("--terraform-output-path", required=True, type=Path)
    parser.add_argument("--definition-evidence-path", required=True, type=Path)
    parser.add_argument(
        "--postconfigure-evidence-path",
        required=True,
        action="append",
        type=Path,
    )
    parser.add_argument(
        "--stage",
        required=True,
        choices=[stage.value for stage in DeploymentEnvironment],
    )
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--terraform-drift-exit-code", required=True, type=int)
    parser.add_argument("--require-reporting", action="store_true")
    parser.add_argument("--require-rti", action="store_true")
    parser.add_argument("--require-activator", action="store_true")
    parser.add_argument(
        "--allow-unverified-activator-actions",
        action="store_true",
        help=(
            "accept definition/topology verification when action execution "
            "evidence is deferred by deployment policy"
        ),
    )
    parser.add_argument("--api-root", default="https://api.fabric.microsoft.com/v1")
    parser.add_argument(
        "--onelake-api-root",
        default="https://onelake.dfs.fabric.microsoft.com",
    )
    parser.add_argument(
        "--onelake-table-api-root",
        default="https://onelake.table.fabric.microsoft.com/delta",
    )
    return parser


def _execute(arguments: argparse.Namespace) -> dict[str, Any]:
    from people_counter.fabric_automation import FabricAutomationClient
    from people_counter.fabric_rest import UrllibTransport

    terraform_output = _load_json(arguments.terraform_output_path, "Terraform output")
    document = merge_terraform_output(
        _load_json(arguments.manifest_path, "deployment manifest"),
        terraform_output,
    )
    manifest = parse_deployment_manifest(
        document, DeploymentEnvironment(arguments.stage)
    )
    transport = cast(HTTPTransport, UrllibTransport())
    client = FabricAutomationClient(
        make_resource_token_provider("https://api.fabric.microsoft.com"),
        transport=transport,
        api_root=arguments.api_root,
    )
    onelake_token_provider = make_resource_token_provider(
        "https://storage.azure.com/"
    )
    onelake_client = FabricAutomationClient(
        onelake_token_provider,
        transport=transport,
        api_root=arguments.onelake_api_root,
        default_headers={"x-ms-version": "2023-11-03"},
    )
    onelake_table_client = FabricAutomationClient(
        onelake_token_provider,
        transport=transport,
        api_root=arguments.onelake_table_api_root,
    )
    return validate_cleanroom(
        client,
        onelake_client,
        onelake_table_client,
        manifest,
        terraform_output=terraform_output,
        runtime_contract=_load_json(
            arguments.runtime_contract_path, "runtime contract"
        ),
        postconfigure_evidence=[
            _load_json(path, "postconfiguration evidence")
            for path in arguments.postconfigure_evidence_path
        ],
        definition_evidence=_load_json(
            arguments.definition_evidence_path, "definition evidence"
        ),
        commit_sha=arguments.commit_sha,
        terraform_drift_exit_code=arguments.terraform_drift_exit_code,
        require_reporting=arguments.require_reporting,
        require_rti=arguments.require_rti,
        require_activator=arguments.require_activator,
        require_activator_action_checkpoint=not (
            arguments.allow_unverified_activator_actions
        ),
    )


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        result = _execute(arguments)
    except (CleanroomAcceptanceError, FabricApiError, ValueError) as error:
        result = {
            "schema": ACCEPTANCE_EVIDENCE_SCHEMA,
            "status": "failed",
            "stage": arguments.stage,
            "commitSha": arguments.commit_sha,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "error": redact_evidence(str(error)),
        }
        print(json.dumps(result, indent=2, sort_keys=True))
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
