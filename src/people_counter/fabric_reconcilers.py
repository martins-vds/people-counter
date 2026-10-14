"""Idempotent Microsoft Fabric post-provisioning reconciliation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from people_counter.fabric_activator import (
    ActivatorAutomationError,
    verify_activator_runtime,
)
from people_counter.fabric_automation import (
    AzureCliResourceTokenProvider,
    DeploymentEvidence,
    FabricApiError,
    redact_evidence,
)
from people_counter.fabric_deployment_manifest import (
    DeploymentEnvironment, DeploymentManifest, SCHEDULED_ROLES, SJD_ROLES, parse_deployment_manifest,
)
from people_counter.fabric_sjd import TABLE_SUFFIXES

JOB_TYPE = "sparkjob"
_BETA_OFF = "beta=false"
_PUBLISH_OK = "Success"
_PUBLISH_FAIL = frozenset({"Failed", "Cancelled"})
_JOB_SUCCESS = frozenset({"completed", "succeeded"})
_JOB_FAILURE = frozenset({"cancelled", "canceled", "failed", "deduped"})
RUNTIME_CONTRACT_SCHEMA = "people-counter-fabric-runtime-contract-v1"
EVIDENCE_SCHEMA = "people-counter-fabric-postconfigure-evidence-v1"
RELEASE_MANIFEST_SCHEMA = "people-counter-fabric-release-manifest-v1"
_ENV_NAME = re.compile(r"[A-Z_][A-Z0-9_]*\Z")
_UUID = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\Z"
)
_CONNECTIVITY_TYPES = frozenset(
    {
        "ShareableCloud",
        "PersonalCloud",
        "OnPremisesGateway",
        "OnPremisesGatewayPersonal",
        "VirtualNetworkGateway",
        "StreamingVirtualNetworkGateway",
    }
)
_AUTH_FIELDS = {
    "Anonymous": frozenset(),
    "WorkspaceIdentity": frozenset(),
    "Basic": frozenset({"username", "password"}),
    "Key": frozenset({"key"}),
    "SharedAccessSignature": frozenset({"token"}),
    "ServicePrincipal": frozenset(
        {"servicePrincipalClientId", "servicePrincipalSecret", "tenantId"}
    ),
    "KeyPair": frozenset({"identifier", "privateKey", "passphrase"}),
}
_POWER_BI_DATASOURCE_TYPES = frozenset(
    {
        "Sql",
        "AzureSql",
        "AnalysisServices",
        "AzureSynapse",
        "OData",
        "SharePoint",
        "Teradata",
        "SapHana",
    }
)
_POWER_BI_CONNECTION_FIELDS = frozenset(
    {
        "account",
        "classInfo",
        "database",
        "domain",
        "emailAddress",
        "kind",
        "loginServer",
        "path",
        "server",
        "url",
    }
)


class ReconcileError(RuntimeError):
    pass


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _require_keys(
    value: Mapping[str, Any],
    *,
    label: str,
    allowed: set[str] | frozenset[str],
    required: set[str] | frozenset[str] = frozenset(),
) -> None:
    missing = sorted(required - set(value))
    extra = sorted(set(value) - allowed)
    if missing or extra:
        raise ReconcileError(
            f"{label} has invalid fields; missing={missing!r} extra={extra!r}"
        )


def _as_mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReconcileError(f"{label} must be an object")
    return value


def _as_list(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ReconcileError(f"{label} must be a list")
    return value


def _as_uuid(value: object, label: str) -> str:
    text = str(value)
    if _UUID.fullmatch(text) is None:
        raise ReconcileError(f"{label} must be a UUID")
    return text


def _operator_checkpoint(
    checkpoint_id: str,
    *,
    area: str,
    reason: str,
    required_action: str,
    evidence_contract: Sequence[str],
) -> dict[str, Any]:
    return {
        "id": checkpoint_id,
        "area": area,
        "status": "operator_checkpoint_required",
        "reason": reason,
        "requiredAction": required_action,
        "evidenceContract": list(evidence_contract),
    }


def validate_runtime_contract(document: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the non-secret runtime contract used after Terraform apply."""
    _require_keys(
        document,
        label="runtime contract",
        allowed={
            "schema",
            "lakehouse",
            "connections",
            "powerBi",
            "access",
        },
        required={"schema"},
    )
    if document.get("schema") != RUNTIME_CONTRACT_SCHEMA:
        raise ReconcileError("unsupported Fabric runtime contract schema")
    contract = json.loads(json.dumps(document))
    contract.setdefault("lakehouse", {})
    contract.setdefault("connections", [])
    contract.setdefault("powerBi", {})
    contract.setdefault("access", {})
    _as_mapping(contract["lakehouse"], "runtime contract lakehouse")
    _as_list(contract["connections"], "runtime contract connections")
    _as_mapping(contract["powerBi"], "runtime contract powerBi")
    _as_mapping(contract["access"], "runtime contract access")
    return contract


def _onelake_path(
    manifest: DeploymentManifest,
    relative_path: str,
) -> tuple[str, str]:
    if manifest.workspace_id is None or manifest.lakehouse.lakehouse_id is None:
        raise ReconcileError("OneLake reconciliation requires workspace/lakehouse IDs")
    files_path = manifest.lakehouse.file_path(relative_path)
    encoded = quote(
        f"{manifest.workspace_id}/{manifest.lakehouse.lakehouse_id}/{files_path}",
        safe="/",
    )
    return files_path, f"/{encoded}"


def ensure_onelake_directory(
    client: Any,
    manifest: DeploymentManifest,
    relative_path: str,
) -> DeploymentEvidence:
    files_path, path = _onelake_path(manifest, relative_path)
    status, _, _ = client.request_response("HEAD", path, expected=(200, 404))
    if status == 200:
        return DeploymentEvidence(
            action=f"onelake_directory:{relative_path}",
            status="succeeded",
            resource={"path": files_path, "state": "present"},
        )
    client.request_response(
        "PUT",
        f"{path}?resource=directory",
        expected=(201,),
    )
    return DeploymentEvidence(
        action=f"onelake_directory:{relative_path}",
        status="succeeded",
        resource={"path": files_path, "state": "created"},
    )


def publish_immutable_onelake_json(
    client: Any,
    manifest: DeploymentManifest,
    relative_path: str,
    value: Mapping[str, Any],
) -> DeploymentEvidence:
    files_path, path = _onelake_path(manifest, relative_path)
    payload = _canonical_json_bytes(value)
    status, _, existing = client.request_response("GET", path, expected=(200, 404))
    if status == 200:
        if existing != payload:
            raise ReconcileError(
                f"immutable OneLake JSON conflicts with existing {files_path}"
            )
        state = "present"
    else:
        client.request_response("PUT", f"{path}?resource=file", expected=(201,))
        client.request_response(
            "PATCH",
            f"{path}?action=append&position=0",
            value=payload,
            expected=(202,),
            content_type="application/octet-stream",
        )
        client.request_response(
            "PATCH",
            f"{path}?action=flush&position={len(payload)}",
            expected=(200,),
        )
        state = "created"
    return DeploymentEvidence(
        action="publish_release_manifest",
        status="succeeded",
        resource={
            "path": files_path,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "state": state,
        },
    )


def verify_lakehouse_tables(
    client: Any,
    manifest: DeploymentManifest,
    *,
    sleep=time.sleep,
    monotonic=time.monotonic,
) -> DeploymentEvidence:
    control_id = manifest.role("control").item_id
    if (
        manifest.workspace_id is None
        or manifest.lakehouse.lakehouse_id is None
        or control_id is None
    ):
        raise ReconcileError(
            "Lakehouse table verification requires provisioned workspace, "
            "Lakehouse, and control SJD IDs"
        )
    verification = run_spark_job(
        client,
        ws=manifest.workspace_id,
        item_id=control_id,
        command_line_arguments="bootstrap",
        sleep=sleep,
        monotonic=monotonic,
    )
    if verification.status != "succeeded":
        raise ReconcileError(
            "Schema-aware Lakehouse table verification failed: "
            f"{verification.error}"
        )
    expected = sorted(
        manifest.lakehouse.table(suffix) for suffix in TABLE_SUFFIXES
    )
    return DeploymentEvidence(
        action="verify_lakehouse_tables",
        status="succeeded",
        operation_id=verification.operation_id,
        resource={
            "expected": expected,
            "method": "control-sjd-bootstrap",
            "job": dict(verification.resource or {}),
        },
        attempts=verification.attempts,
    )


def _resolve_connection_credentials(
    authentication: Mapping[str, Any],
    *,
    env: Mapping[str, str],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    _require_keys(
        authentication,
        label="connection authentication",
        allowed={"type", "valuesFromEnvironment"},
        required={"type"},
    )
    auth_type = str(authentication["type"])
    if auth_type == "OAuth2":
        return None, _operator_checkpoint(
            "connection-oauth2-authorization",
            area="connections",
            reason=(
                "The public Fabric connection API does not accept a reusable OAuth2 "
                "authorization grant in this deployment contract."
            ),
            required_action=(
                "Authorize the connection in Fabric and record immutable evidence "
                "that the intended principal and connection were tested."
            ),
            evidence_contract=(
                "connectionId",
                "authorizedPrincipal",
                "testedAt",
                "testResult",
                "evidenceReference",
            ),
        )
    if auth_type not in _AUTH_FIELDS:
        raise ReconcileError(f"unsupported connection authentication type {auth_type!r}")
    references = _as_mapping(
        authentication.get("valuesFromEnvironment", {}),
        "connection authentication valuesFromEnvironment",
    )
    required = _AUTH_FIELDS[auth_type]
    optional = {"passphrase"} if auth_type == "KeyPair" else set()
    if set(references) - required or not (required - optional) <= set(references):
        raise ReconcileError(
            f"{auth_type} environment references do not match its documented fields"
        )
    credentials: dict[str, Any] = {"credentialType": auth_type}
    for name, variable in references.items():
        variable_name = str(variable)
        if _ENV_NAME.fullmatch(variable_name) is None:
            raise ReconcileError(
                f"connection environment reference {variable_name!r} is invalid"
            )
        resolved = env.get(variable_name)
        if not resolved:
            raise ReconcileError(
                f"required connection environment variable {variable_name} is unset"
            )
        credentials[str(name)] = resolved
    return credentials, None


def reconcile_connection_authentication(
    client: Any,
    *,
    connection_ids: Mapping[str, Any],
    configurations: Sequence[object],
    env: Mapping[str, str],
) -> tuple[dict[str, DeploymentEvidence], list[dict[str, Any]]]:
    evidence: dict[str, DeploymentEvidence] = {}
    checkpoints: list[dict[str, Any]] = []
    for index, raw in enumerate(configurations):
        config = _as_mapping(raw, f"connections[{index}]")
        _require_keys(
            config,
            label=f"connections[{index}]",
            allowed={
                "key",
                "connectivityType",
                "authentication",
                "connectionEncryption",
                "singleSignOnType",
                "skipTestConnection",
            },
            required={"key", "connectivityType", "authentication"},
        )
        key = str(config["key"])
        if key in evidence:
            raise ReconcileError(f"duplicate connection runtime key {key!r}")
        connection_id = connection_ids.get(key)
        if connection_id is None:
            raise ReconcileError(f"Terraform did not output connection key {key!r}")
        connection_id = _as_uuid(connection_id, f"connection_ids.{key}")
        connectivity_type = str(config["connectivityType"])
        if connectivity_type not in _CONNECTIVITY_TYPES:
            raise ReconcileError(
                f"unsupported connection connectivityType {connectivity_type!r}"
            )
        if connectivity_type in {
            "OnPremisesGateway",
            "OnPremisesGatewayPersonal",
        }:
            checkpoint = _operator_checkpoint(
                f"connection-on-premises-{key}",
                area="connections",
                reason=(
                    "The documented on-premises update contract requires "
                    "gateway-public-key encrypted credential payloads."
                ),
                required_action=(
                    "Rotate the gateway credential with an approved gateway "
                    "credential utility and attach a successful connection test."
                ),
                evidence_contract=(
                    "connectionId",
                    "gatewayId",
                    "credentialType",
                    "testedAt",
                    "testResult",
                    "evidenceReference",
                ),
            )
            checkpoint["connectionKey"] = key
            checkpoint["connectionId"] = connection_id
            checkpoints.append(checkpoint)
            continue
        credentials, checkpoint = _resolve_connection_credentials(
            _as_mapping(config["authentication"], f"connections[{index}].authentication"),
            env=env,
        )
        if checkpoint is not None:
            checkpoint["connectionKey"] = key
            checkpoint["connectionId"] = connection_id
            checkpoints.append(checkpoint)
            continue
        details = {
            "credentials": credentials,
            "connectionEncryption": str(
                config.get("connectionEncryption", "Encrypted")
            ),
            "singleSignOnType": str(config.get("singleSignOnType", "None")),
            "skipTestConnection": bool(config.get("skipTestConnection", False)),
        }
        resource = client.request_json(
            "PATCH",
            f"/connections/{connection_id}",
            value={
                "connectivityType": connectivity_type,
                "credentialDetails": details,
            },
            expected=(200,),
        )
        if resource.get("id") not in {None, connection_id}:
            raise ReconcileError(f"connection {key!r} update returned a different ID")
        evidence[key] = DeploymentEvidence(
            action=f"connection_authentication:{key}",
            status="succeeded",
            resource={
                "id": connection_id,
                "connectivityType": resource.get(
                    "connectivityType", connectivity_type
                ),
                "authenticationType": (
                    resource.get("credentialDetails", {}) or {}
                ).get("credentialType"),
                "tested": not details["skipTestConnection"],
            },
        )
    return evidence, checkpoints


def _assignment_principal_id(assignment: Mapping[str, Any]) -> str | None:
    principal = assignment.get("principal")
    return (
        str(principal.get("id"))
        if isinstance(principal, Mapping) and principal.get("id")
        else None
    )


def _remove_role_assignments(
    client: Any,
    *,
    base: str,
    deprecated_principals: set[str],
    protect_last_admin: bool,
) -> DeploymentEvidence:
    assignments = client.list_pages(base)
    admins = [item for item in assignments if item.get("role") == "Admin"]
    matched = [
        item
        for item in assignments
        if _assignment_principal_id(item) in deprecated_principals
    ]
    if protect_last_admin and any(
        item.get("role") == "Admin" for item in matched
    ) and len(admins) <= 1:
        raise ReconcileError("refusing to remove the workspace's last Admin")
    removed: list[str] = []
    for assignment in matched:
        assignment_id = _as_uuid(assignment.get("id"), "role assignment id")
        client.request_json("DELETE", f"{base}/{assignment_id}", expected=(200,))
        removed.append(assignment_id)
    return DeploymentEvidence(
        action=f"remove_deprecated_access:{base}",
        status="succeeded",
        resource={
            "removedRoleAssignmentIds": sorted(removed),
            "unmatchedPrincipalIds": sorted(
                deprecated_principals
                - {
                    principal
                    for item in matched
                    if (principal := _assignment_principal_id(item)) is not None
                }
            ),
        },
    )


def reconcile_deprecated_access(
    client: Any,
    *,
    workspace_id: str,
    connection_ids: Mapping[str, Any],
    configuration: Mapping[str, Any],
) -> tuple[dict[str, DeploymentEvidence], list[dict[str, Any]]]:
    _require_keys(
        configuration,
        label="access",
        allowed={
            "deprecatedWorkspacePrincipals",
            "deprecatedConnectionPrincipals",
            "deprecatedItemAccess",
        },
    )
    result: dict[str, DeploymentEvidence] = {}
    workspace_principals = {
        _as_uuid(item, "deprecated workspace principal")
        for item in _as_list(
            configuration.get("deprecatedWorkspacePrincipals", []),
            "access.deprecatedWorkspacePrincipals",
        )
    }
    if workspace_principals:
        result["workspace"] = _remove_role_assignments(
            client,
            base=f"/workspaces/{workspace_id}/roleAssignments",
            deprecated_principals=workspace_principals,
            protect_last_admin=True,
        )
    connection_config = _as_mapping(
        configuration.get("deprecatedConnectionPrincipals", {}),
        "access.deprecatedConnectionPrincipals",
    )
    for key, raw_principals in connection_config.items():
        if key not in connection_ids:
            raise ReconcileError(f"unknown connection access key {key!r}")
        principals = {
            _as_uuid(item, f"deprecated connection principal {key}")
            for item in _as_list(raw_principals, f"connection access {key}")
        }
        result[f"connection:{key}"] = _remove_role_assignments(
            client,
            base=f"/connections/{_as_uuid(connection_ids[key], f'connection {key}')}"
            "/roleAssignments",
            deprecated_principals=principals,
            protect_last_admin=False,
        )
    checkpoints = []
    item_access = _as_list(
        configuration.get("deprecatedItemAccess", []),
        "access.deprecatedItemAccess",
    )
    if item_access:
        checkpoints.append(
            _operator_checkpoint(
                "deprecated-item-access",
                area="permissions",
                reason=(
                    "The public Fabric Items API can enumerate item access through "
                    "admin preview APIs but does not expose supported item-level "
                    "access deletion."
                ),
                required_action=(
                    "Remove the listed item shares in the Fabric administration "
                    "experience and attach readback evidence."
                ),
                evidence_contract=(
                    "workspaceId",
                    "itemId",
                    "principalId",
                    "removedAt",
                    "verifiedBy",
                    "evidenceReference",
                ),
            )
        )
    return result, checkpoints


def _power_bi_model_id(manifest: DeploymentManifest, bundle: str) -> str:
    selected = {
        "analytics": manifest.analytics_reporting,
        "operational": manifest.operational_reporting,
    }.get(bundle)
    if selected is None or selected.semantic_model.target_item_id is None:
        raise ReconcileError(f"Power BI bundle {bundle!r} is unresolved")
    return selected.semantic_model.target_item_id


def _validate_connection_details(value: object, label: str) -> dict[str, str]:
    details = _as_mapping(value, label)
    if not details or set(details) - _POWER_BI_CONNECTION_FIELDS:
        raise ReconcileError(f"{label} contains unsupported datasource fields")
    if any(not isinstance(item, str) or not item for item in details.values()):
        raise ReconcileError(f"{label} values must be non-empty strings")
    return {str(key): str(item) for key, item in details.items()}


def reconcile_power_bi_runtime(
    client: Any,
    manifest: DeploymentManifest,
    configuration: Mapping[str, Any],
) -> tuple[dict[str, list[DeploymentEvidence]], list[dict[str, Any]]]:
    if manifest.workspace_id is None:
        raise ReconcileError("Power BI reconciliation requires a workspace ID")
    if set(configuration) - {"analytics", "operational"}:
        raise ReconcileError("powerBi supports only analytics and operational bundles")
    result: dict[str, list[DeploymentEvidence]] = {}
    checkpoints: list[dict[str, Any]] = []
    for bundle, raw in configuration.items():
        config = _as_mapping(raw, f"powerBi.{bundle}")
        _require_keys(
            config,
            label=f"powerBi.{bundle}",
            allowed={
                "takeOver",
                "datasourceUpdates",
                "gatewayBinding",
                "refreshSchedule",
                "directLakeBinding",
            },
        )
        model_id = _power_bi_model_id(manifest, bundle)
        base = (
            f"/groups/{manifest.workspace_id}/datasets/{model_id}"
        )
        operations: list[DeploymentEvidence] = []
        if config.get("takeOver", False):
            client.request_json(
                "POST", f"{base}/Default.TakeOver", expected=(200,)
            )
            operations.append(
                DeploymentEvidence(
                    action=f"power_bi_takeover:{bundle}",
                    status="succeeded",
                    resource={"datasetId": model_id},
                )
            )
        updates = _as_list(
            config.get("datasourceUpdates", []),
            f"powerBi.{bundle}.datasourceUpdates",
        )
        if updates:
            update_details = []
            for index, raw_update in enumerate(updates):
                update = _as_mapping(
                    raw_update, f"powerBi.{bundle}.datasourceUpdates[{index}]"
                )
                _require_keys(
                    update,
                    label=f"powerBi.{bundle}.datasourceUpdates[{index}]",
                    allowed={"datasourceType", "selector", "target"},
                    required={"datasourceType", "selector", "target"},
                )
                datasource_type = str(update["datasourceType"])
                if datasource_type not in _POWER_BI_DATASOURCE_TYPES:
                    raise ReconcileError(
                        f"Power BI datasource type {datasource_type!r} is not "
                        "supported by UpdateDatasources"
                    )
                update_details.append(
                    {
                        "datasourceSelector": {
                            "datasourceType": datasource_type,
                            "connectionDetails": _validate_connection_details(
                                update["selector"], "datasource selector"
                            ),
                        },
                        "connectionDetails": _validate_connection_details(
                            update["target"], "datasource target"
                        ),
                    }
                )
            client.request_json(
                "POST",
                f"{base}/Default.UpdateDatasources",
                value={"updateDetails": update_details},
                expected=(200,),
            )
            operations.append(
                DeploymentEvidence(
                    action=f"power_bi_datasources:{bundle}",
                    status="succeeded",
                    resource={
                        "datasetId": model_id,
                        "updatedDatasourceCount": len(update_details),
                    },
                )
            )
        gateway = config.get("gatewayBinding")
        if gateway is not None:
            gateway_config = _as_mapping(
                gateway, f"powerBi.{bundle}.gatewayBinding"
            )
            _require_keys(
                gateway_config,
                label=f"powerBi.{bundle}.gatewayBinding",
                allowed={"gatewayId", "datasourceIds"},
                required={"gatewayId"},
            )
            payload: dict[str, Any] = {
                "gatewayObjectId": _as_uuid(
                    gateway_config["gatewayId"], "Power BI gatewayId"
                )
            }
            if "datasourceIds" in gateway_config:
                payload["datasourceObjectIds"] = [
                    _as_uuid(item, "Power BI datasourceId")
                    for item in _as_list(
                        gateway_config["datasourceIds"],
                        "Power BI datasourceIds",
                    )
                ]
            client.request_json(
                "POST",
                f"{base}/Default.BindToGateway",
                value=payload,
                expected=(200,),
            )
            operations.append(
                DeploymentEvidence(
                    action=f"power_bi_gateway:{bundle}",
                    status="succeeded",
                    resource={
                        "datasetId": model_id,
                        "gatewayId": payload["gatewayObjectId"],
                        "datasourceIds": payload.get("datasourceObjectIds", []),
                    },
                )
            )
        schedule = config.get("refreshSchedule")
        if schedule is not None:
            schedule_value = _as_mapping(
                schedule, f"powerBi.{bundle}.refreshSchedule"
            )
            _require_keys(
                schedule_value,
                label=f"powerBi.{bundle}.refreshSchedule",
                allowed={
                    "enabled",
                    "days",
                    "times",
                    "localTimeZoneId",
                    "notifyOption",
                },
            )
            if schedule_value.get("notifyOption", "NoNotification") != "NoNotification":
                raise ReconcileError(
                    "service-principal refresh schedules require NoNotification"
                )
            client.request_json(
                "PATCH",
                f"{base}/refreshSchedule",
                value={"value": dict(schedule_value)},
                expected=(200,),
            )
            operations.append(
                DeploymentEvidence(
                    action=f"power_bi_refresh_schedule:{bundle}",
                    status="succeeded",
                    resource={"datasetId": model_id, "schedule": schedule_value},
                )
            )
        if config.get("directLakeBinding") is not None:
            checkpoints.append(
                _operator_checkpoint(
                    f"direct-lake-binding-{bundle}",
                    area="power-bi",
                    reason=(
                        "No supported Power BI REST operation can rewrite a "
                        "Direct Lake semantic model connection after deployment."
                    ),
                    required_action=(
                        "Deploy the reviewed TMDL connection binding or update it "
                        "with a supported XMLA/TOM administrative process, then "
                        "attach a model readback."
                    ),
                    evidence_contract=(
                        "workspaceId",
                        "datasetId",
                        "expectedLakehouseId",
                        "observedConnection",
                        "verifiedAt",
                        "evidenceReference",
                    ),
                )
            )
        result[str(bundle)] = operations
    return result, checkpoints


def _env_path(ws: str, env_id: str, suffix: str = "") -> str:
    return f"/workspaces/{ws}/environments/{env_id}{suffix}"


def reconcile_environment_library(client: Any, *, ws: str, env_id: str, whl_path: Path) -> DeploymentEvidence:
    if whl_path.suffix != ".whl" or not whl_path.is_file():
        raise ReconcileError(f"{whl_path} is not a .whl file")
    name = whl_path.name
    if not name.startswith("people_counter-"):
        raise ReconcileError(f"{whl_path} is not a people_counter wheel")
    staged = client.list_pages(
        _env_path(ws, env_id, f"/staging/libraries?{_BETA_OFF}"),
        value_key="libraries",
        empty_error_codes=("EnvironmentLibrariesNotFound",),
    )
    stale = sorted(
        str(item.get("name"))
        for item in staged
        if item.get("libraryType") == "Custom"
        and str(item.get("name", "")).startswith("people_counter-")
        and item.get("name") != name
    )
    for stale_name in stale:
        client.request_json("DELETE", _env_path(ws, env_id, f"/staging/libraries/{stale_name}"), expected=(200,))
    client.request_json(
        "POST", _env_path(ws, env_id, f"/staging/libraries/{name}"), value=whl_path.read_bytes(),
        expected=(200,), content_type="application/octet-stream",
    )
    return DeploymentEvidence(
        action="upload_environment_library", status="succeeded", resource={"name": name, "removed": stale}
    )


def reconcile_environment_publish(
    client: Any, *, ws: str, env_id: str, sleep=time.sleep, monotonic=time.monotonic,
    poll_seconds: float = 15.0, timeout_seconds: float = 1800.0,
) -> DeploymentEvidence:
    submitted = client.run_operation(
        "POST", _env_path(ws, env_id, f"/staging/publish?{_BETA_OFF}"), action="publish_environment", accepted=(200, 202)
    )
    if submitted.status != "succeeded":
        raise ReconcileError(
            f"environment publish failed to submit: "
            f"{submitted.error or submitted.status}"
        )
    deadline = monotonic() + timeout_seconds
    while True:
        environment = client.request_json("GET", _env_path(ws, env_id))
        properties = environment.get("properties")
        source = properties if isinstance(properties, Mapping) else environment
        state = str((source.get("publishDetails") or {}).get("state", ""))
        if state == _PUBLISH_OK:
            return DeploymentEvidence(action="publish_environment", status="succeeded", resource=environment)
        if state in _PUBLISH_FAIL:
            raise ReconcileError(f"environment publish failed: state={state!r}")
        if monotonic() >= deadline:
            raise ReconcileError("timed out polling environment publish")
        sleep(poll_seconds)


def _schedule_payload(*, interval: int, enabled: bool, args: str, now: datetime | None) -> dict[str, Any]:
    if type(interval) is not int or not 1 <= interval <= 10_080:
        raise ReconcileError("interval_minutes must be between 1 and 10080")
    start = (now or datetime.now(timezone.utc)) + timedelta(minutes=5)
    end = start + timedelta(days=3650)
    return {
        "configuration": {
            "endDateTime": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "interval": interval,
            "localTimeZoneId": "UTC",
            "startDateTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "type": "Cron",
        },
        "enabled": enabled,
        "executionData": {"commandLineArguments": args},
    }


def reconcile_role_schedule(
    client: Any, *, ws: str, role: str, item_id: str, enabled: bool, interval_minutes: int | None,
    command_line_arguments: str = "", now: datetime | None = None,
) -> DeploymentEvidence:
    if role not in SJD_ROLES:
        raise ReconcileError(f"unsupported SJD role {role!r}")
    if (role in SCHEDULED_ROLES) == (interval_minutes is None):
        raise ReconcileError(f"role {role!r} schedule presence disagrees with its scheduled/on-demand class")
    base = f"/workspaces/{ws}/items/{item_id}/jobs/{JOB_TYPE}/schedules"
    schedules = client.list_pages(base)
    if len(schedules) > 1:
        raise ReconcileError(f"SJD {item_id} ({role}) has multiple schedules; refusing to update")
    existing = schedules[0] if schedules else None

    def _id() -> str:
        schedule_id = str(existing.get("id", "")) if existing else ""
        if not schedule_id:
            raise ReconcileError(f"{role} schedule readback omitted its ID")
        return schedule_id

    if interval_minutes is None:
        if existing is None:
            return DeploymentEvidence(action=f"schedule:{role}", status="succeeded", resource={"state": "absent"})
        schedule_id = _id()
        client.request_json("DELETE", f"{base}/{schedule_id}", expected=(200,))
        return DeploymentEvidence(
            action=f"schedule:{role}", status="succeeded", resource={"state": "deleted", "id": schedule_id}
        )

    payload = _schedule_payload(interval=interval_minutes, enabled=enabled, args=command_line_arguments, now=now)
    if existing is None:
        return client.run_operation("POST", base, action=f"schedule:{role}", value=payload, accepted=(200, 201, 202))
    return client.run_operation(
        "PATCH", f"{base}/{_id()}", action=f"schedule:{role}", value=payload, accepted=(200, 202)
    )


def run_item_job(
    client: Any,
    *,
    ws: str,
    item_id: str,
    job_type: str,
    action: str,
    execution_data: Mapping[str, Any] | None = None,
    sleep=time.sleep,
    monotonic=time.monotonic,
    poll_seconds: float = 15.0,
    timeout_seconds: float = 1800.0,
) -> DeploymentEvidence:
    path = f"/workspaces/{ws}/items/{item_id}/jobs/instances?jobType={job_type}"
    _, headers, _ = client.request_response(
        "POST",
        path,
        value={"executionData": dict(execution_data)} if execution_data else None,
        expected=(202,),
    )
    location = next(
        (
            str(value)
            for key, value in headers.items()
            if key.lower() == "location"
        ),
        None,
    )
    if not location:
        raise ReconcileError("Fabric job submission omitted the Location header")

    deadline = monotonic() + timeout_seconds
    attempts = 0
    while True:
        attempts += 1
        instance = client.request_json("GET", location)
        status = str(instance.get("status", "")).lower()
        instance_id = instance.get("id")
        if status in _JOB_SUCCESS:
            return DeploymentEvidence(
                action=action,
                status="succeeded",
                operation_id=str(instance_id) if instance_id else None,
                resource=instance,
                attempts=attempts,
            )
        if status in _JOB_FAILURE:
            return DeploymentEvidence(
                action=action,
                status="failed",
                operation_id=str(instance_id) if instance_id else None,
                resource=instance,
                error=f"Fabric Spark job failed: {instance!r}",
                attempts=attempts,
            )
        if monotonic() >= deadline:
            return DeploymentEvidence(
                action=action,
                status="failed",
                operation_id=str(instance_id) if instance_id else None,
                resource=instance,
                error="timed out waiting for Fabric Spark job",
                attempts=attempts,
            )
        sleep(poll_seconds)


def run_spark_job(
    client: Any,
    *,
    ws: str,
    item_id: str,
    command_line_arguments: str,
    sleep=time.sleep,
    monotonic=time.monotonic,
    poll_seconds: float = 15.0,
    timeout_seconds: float = 1800.0,
) -> DeploymentEvidence:
    return run_item_job(
        client,
        ws=ws,
        item_id=item_id,
        job_type=JOB_TYPE,
        action=f"spark_job:{command_line_arguments}",
        execution_data={"commandLineArguments": command_line_arguments},
        sleep=sleep,
        monotonic=monotonic,
        poll_seconds=poll_seconds,
        timeout_seconds=timeout_seconds,
    )


def reconcile_reporting_refresh(
    client: Any,
    manifest: DeploymentManifest,
    *,
    sleep=time.sleep,
    monotonic=time.monotonic,
) -> dict[str, DeploymentEvidence]:
    if manifest.workspace_id is None:
        raise ReconcileError("reporting refresh requires a workspace ID")
    bundles = {
        "analytics": manifest.analytics_reporting,
        "operational": manifest.operational_reporting,
    }
    missing = [
        name
        for name, bundle in bundles.items()
        if bundle.semantic_model.target_item_id is None
        or bundle.report.target_item_id is None
    ]
    if missing:
        raise ReconcileError(
            f"reporting items are unresolved for bundles: {sorted(missing)!r}"
        )

    evidence: dict[str, DeploymentEvidence] = {}
    for name, bundle in bundles.items():
        model_id = bundle.semantic_model.target_item_id
        report_id = bundle.report.target_item_id
        if model_id is None or report_id is None:
            raise AssertionError("reporting IDs were validated above")
        model = client.request_json(
            "GET",
            f"/workspaces/{manifest.workspace_id}/items/{model_id}",
        )
        report = client.request_json(
            "GET",
            f"/workspaces/{manifest.workspace_id}/items/{report_id}",
        )
        if model.get("type") != "SemanticModel" or report.get("type") != "Report":
            raise ReconcileError(f"{name} reporting item types are invalid")
        refreshed = run_item_job(
            client,
            ws=manifest.workspace_id,
            item_id=model_id,
            job_type="Refresh",
            action=f"semantic_model_refresh:{name}",
            sleep=sleep,
            monotonic=monotonic,
        )
        if refreshed.status != "succeeded":
            raise ReconcileError(
                f"{name} semantic model refresh failed: {refreshed.error}"
            )
        evidence[name] = refreshed
    return evidence


def _terraform_value(terraform_output: Mapping[str, Any], key: str) -> Any:
    entry = terraform_output.get(key)
    return entry.get("value") if isinstance(entry, Mapping) else None


def _merge_foundation_ids(
    merged: dict[str, Any],
    terraform_output: Mapping[str, Any],
) -> None:
    lakehouse = merged.setdefault("lakehouse", {})
    for target, source, key in (
        (merged, "workspace_id", "workspace_id"),
        (merged, "fabric_environment_id", "environment_id"),
        (lakehouse, "lakehouse_id", "lakehouse_id"),
    ):
        if target.get(source) is None:
            target[source] = _terraform_value(terraform_output, key)


def _merge_role_ids(
    merged: dict[str, Any],
    terraform_output: Mapping[str, Any],
) -> None:
    role_ids = _terraform_value(terraform_output, "spark_job_definition_ids") or {}
    if not isinstance(role_ids, Mapping):
        raise ReconcileError("terraform output spark_job_definition_ids must be an object")
    for role, binding in (merged.get("roles") or {}).items():
        if isinstance(binding, Mapping) and binding.get("item_id") is None and role in role_ids:
            binding["item_id"] = role_ids[role]


def _merge_reporting_ids(
    merged: dict[str, Any],
    terraform_output: Mapping[str, Any],
) -> None:
    semantic_model_ids = _terraform_value(terraform_output, "semantic_model_ids") or {}
    report_ids = _terraform_value(terraform_output, "report_ids") or {}
    if not isinstance(semantic_model_ids, Mapping) or not isinstance(
        report_ids, Mapping
    ):
        raise ReconcileError("Terraform reporting outputs must be objects")
    for bundle_name, document_key in (
        ("analytics", "analytics_reporting"),
        ("operational", "operational_reporting"),
    ):
        bundle = merged.get(document_key)
        if not isinstance(bundle, Mapping):
            continue
        for artifact_key, outputs in (
            ("semantic_model", semantic_model_ids),
            ("report", report_ids),
        ):
            artifact = bundle.get(artifact_key)
            if (
                isinstance(artifact, Mapping)
                and artifact.get("target_item_id") is None
                and bundle_name in outputs
            ):
                artifact["target_item_id"] = outputs[bundle_name]


def _merge_rti_ids(
    merged: dict[str, Any],
    terraform_output: Mapping[str, Any],
) -> None:
    rti = merged.get("real_time_intelligence")
    if isinstance(rti, Mapping):
        for manifest_key, output_key in (
            ("eventhouse_id", "eventhouse_id"),
            ("kql_database_id", "kql_database_id"),
            ("queryset_id", "kql_queryset_id"),
            ("dashboard_id", "kql_dashboard_id"),
        ):
            if rti.get(manifest_key) is None:
                rti[manifest_key] = _terraform_value(terraform_output, output_key)


def _merge_activator_ids(
    merged: dict[str, Any],
    terraform_output: Mapping[str, Any],
) -> None:
    activator = merged.get("activator")
    activator_ids = _terraform_value(terraform_output, "activator_ids") or {}
    if not isinstance(activator_ids, Mapping):
        raise ReconcileError("Terraform activator_ids output must be an object")
    if (
        isinstance(activator, Mapping)
        and activator.get("reflex_id") is None
        and "operations" in activator_ids
    ):
        activator["reflex_id"] = activator_ids["operations"]


def merge_terraform_output(
    document: Mapping[str, Any],
    terraform_output: Mapping[str, Any],
) -> dict[str, Any]:
    merged: dict[str, Any] = json.loads(json.dumps(document))
    _merge_foundation_ids(merged, terraform_output)
    _merge_role_ids(merged, terraform_output)
    _merge_reporting_ids(merged, terraform_output)
    _merge_rti_ids(merged, terraform_output)
    _merge_activator_ids(merged, terraform_output)
    return merged


def _terraform_mapping(
    terraform_output: Mapping[str, Any],
    key: str,
) -> Mapping[str, Any]:
    entry = terraform_output.get(key)
    value = entry.get("value", {}) if isinstance(entry, Mapping) else {}
    if not isinstance(value, Mapping):
        raise ReconcileError(f"Terraform {key} output must be an object")
    return value


def _reconcile_lakehouse_runtime(
    client: Any,
    onelake_client: Any | None,
    manifest: DeploymentManifest,
    contract: Mapping[str, Any],
    *,
    sleep: Any,
    monotonic: Any,
) -> tuple[DeploymentEvidence | None, dict[str, DeploymentEvidence]]:
    _require_keys(
        contract,
        label="lakehouse",
        allowed={"folders", "verifyTables", "publishReleaseManifest"},
    )
    table_verification = (
        verify_lakehouse_tables(
            client,
            manifest,
            sleep=sleep,
            monotonic=monotonic,
        )
        if contract.get("verifyTables", False)
        else None
    )
    folders = _as_list(contract.get("folders", []), "lakehouse.folders")
    if folders and onelake_client is None:
        raise ReconcileError("OneLake folder reconciliation needs a OneLake client")
    directories: dict[str, DeploymentEvidence] = {}
    for folder in folders:
        relative = str(folder).strip("/")
        if not relative:
            raise ReconcileError("OneLake folder names must be non-empty")
        directories[relative] = ensure_onelake_directory(
            onelake_client, manifest, relative
        )
    return table_verification, directories


def _reconcile_external_runtime(
    client: Any,
    power_bi_client: Any | None,
    manifest: DeploymentManifest,
    contract: Mapping[str, Any],
    connection_ids: Mapping[str, Any],
    *,
    env: Mapping[str, str],
) -> tuple[
    dict[str, DeploymentEvidence],
    dict[str, DeploymentEvidence],
    dict[str, list[DeploymentEvidence]],
    list[dict[str, Any]],
]:
    connections, checkpoints = reconcile_connection_authentication(
        client,
        connection_ids=connection_ids,
        configurations=_as_list(contract["connections"], "connections"),
        env=env,
    )
    access, access_checkpoints = reconcile_deprecated_access(
        client,
        workspace_id=str(manifest.workspace_id),
        connection_ids=connection_ids,
        configuration=_as_mapping(contract["access"], "access"),
    )
    checkpoints.extend(access_checkpoints)
    power_bi_config = _as_mapping(contract["powerBi"], "powerBi")
    if power_bi_config and power_bi_client is None:
        raise ReconcileError("Power BI runtime reconciliation needs a Power BI client")
    power_bi, power_bi_checkpoints = reconcile_power_bi_runtime(
        power_bi_client,
        manifest,
        power_bi_config,
    )
    checkpoints.extend(power_bi_checkpoints)
    return connections, access, power_bi, checkpoints


def _reconcile_activator_gate(
    client: Any,
    manifest: DeploymentManifest,
    *,
    enabled: bool,
    bundle_path: Path | None,
    checkpoint_path: Path | None,
    now: datetime | None,
) -> tuple[Any | None, list[dict[str, Any]]]:
    if not enabled:
        return None, []
    if bundle_path is None:
        raise ReconcileError(
            "Activator enablement requires a source-controlled definition bundle"
        )
    try:
        activator = verify_activator_runtime(
            client,
            manifest,
            bundle_path=bundle_path,
            checkpoint_path=checkpoint_path,
            now=now,
        )
    except ActivatorAutomationError as error:
        raise ReconcileError(f"Activator verification failed: {error}") from error
    if activator.status == "succeeded":
        return activator, []
    return activator, [
        _operator_checkpoint(
            "activator-action-execution",
            area="activator",
            reason=activator.error or f"Activator status is {activator.status}",
            required_action=(
                "Exercise every configured Activator action and provide the "
                "checkpoint described by the Activator evidence contract."
            ),
            evidence_contract=(
                "workspaceId",
                "reflexId",
                "triggerName",
                "definitionSha256",
                "actions",
                "verifiedAt",
                "expiresAt",
                "verifiedBy",
                "evidenceReference",
            ),
        )
    ]


def _reconcile_all_schedules(
    client: Any,
    manifest: DeploymentManifest,
    *,
    enabled: bool,
    now: datetime | None,
) -> dict[str, DeploymentEvidence]:
    if manifest.workspace_id is None:
        raise ReconcileError("schedule reconciliation requires a workspace ID")
    schedules: dict[str, DeploymentEvidence] = {}
    for role in sorted(SJD_ROLES):
        binding = manifest.role(role)
        if binding.item_id is None:
            raise ReconcileError(f"roles.{role}.item_id is unresolved")
        evidence = reconcile_role_schedule(
            client,
            ws=manifest.workspace_id,
            role=role,
            item_id=binding.item_id,
            enabled=enabled,
            interval_minutes=(
                binding.schedule.interval_minutes if binding.schedule else None
            ),
            now=now,
        )
        if evidence.status != "succeeded":
            raise ReconcileError(
                f"{role} schedule reconciliation failed: {evidence.error}"
            )
        schedules[role] = evidence
    return schedules


def _publish_release_manifest(
    onelake_client: Any | None,
    manifest: DeploymentManifest,
    lakehouse_contract: Mapping[str, Any],
    context: Mapping[str, Any],
    result: Mapping[str, Any],
) -> DeploymentEvidence | None:
    if (
        not lakehouse_contract.get("publishReleaseManifest", False)
        or result.get("status") != "succeeded"
    ):
        return None
    if onelake_client is None:
        raise ReconcileError("release manifest publication needs a OneLake client")
    commit_sha = str(context.get("commitSha", ""))
    if re.fullmatch(r"[0-9a-fA-F]{7,64}", commit_sha) is None:
        raise ReconcileError(
            "release manifest publication requires a hexadecimal commitSha"
        )
    release_document = {
        "schema": RELEASE_MANIFEST_SCHEMA,
        "stage": manifest.stage.value,
        "commitSha": commit_sha,
        "status": result["status"],
        "definitionHashes": context.get("definitionHashes", {}),
        "terraformOutputSha256": context.get("terraformOutputSha256"),
        "resources": result["resources"],
    }
    return publish_immutable_onelake_json(
        onelake_client,
        manifest,
        f"releases/{commit_sha}.json",
        release_document,
    )


def _run_lakehouse_bootstrap(
    client: Any,
    manifest: DeploymentManifest,
    *,
    enabled: bool,
    sleep: Any,
    monotonic: Any,
) -> DeploymentEvidence | None:
    if not enabled:
        return None
    control_id = manifest.role("control").item_id
    if control_id is None or manifest.workspace_id is None:
        raise ReconcileError("roles.control.item_id/workspace_id is unresolved")
    bootstrap = run_spark_job(
        client,
        ws=manifest.workspace_id,
        item_id=control_id,
        command_line_arguments="bootstrap",
        sleep=sleep,
        monotonic=monotonic,
    )
    if bootstrap.status != "succeeded":
        raise ReconcileError(f"Lakehouse bootstrap failed: {bootstrap.error}")
    return bootstrap


def _runtime_evidence(
    manifest: DeploymentManifest,
    *,
    requested_schedules: bool,
    enabled_schedules: bool,
    generated_at: str,
    context: Mapping[str, Any],
    connection_ids: Mapping[str, Any],
    library: DeploymentEvidence,
    publish: DeploymentEvidence,
    bootstrap: DeploymentEvidence | None,
    table_verification: DeploymentEvidence | None,
    directories: Mapping[str, DeploymentEvidence],
    connections: Mapping[str, DeploymentEvidence],
    power_bi: Mapping[str, Sequence[DeploymentEvidence]],
    access: Mapping[str, DeploymentEvidence],
    reporting: Mapping[str, DeploymentEvidence],
    activator: Any | None,
    schedules: Mapping[str, DeploymentEvidence],
    checkpoints: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    operations = [
        library,
        publish,
        *([bootstrap] if bootstrap is not None else []),
        *([table_verification] if table_verification is not None else []),
        *directories.values(),
        *connections.values(),
        *(item for items in power_bi.values() for item in items),
        *access.values(),
        *reporting.values(),
        *([activator] if activator is not None else []),
        *schedules.values(),
    ]
    statuses = {str(operation.status) for operation in operations}
    unexpected = statuses - {"succeeded", "operator_checkpoint_required"}
    checkpoint_without_evidence = (
        "operator_checkpoint_required" in statuses and not checkpoints
    )
    if unexpected or checkpoint_without_evidence:
        overall_status = "failed"
    elif "operator_checkpoint_required" in statuses or checkpoints:
        overall_status = "operator_checkpoint_required"
    else:
        overall_status = "succeeded"
    return {
        "schema": EVIDENCE_SCHEMA,
        "status": overall_status,
        "generatedAt": generated_at,
        "stage": manifest.stage.value,
        "enable_schedules": requested_schedules,
        "schedulesEnabled": enabled_schedules,
        "context": redact_evidence(context),
        "resources": {
            "workspaceId": manifest.workspace_id,
            "environmentId": manifest.fabric_environment_id,
            "lakehouseId": manifest.lakehouse.lakehouse_id,
            "connectionIds": dict(connection_ids),
            "sparkJobDefinitionIds": {
                role: manifest.role(role).item_id for role in sorted(SJD_ROLES)
            },
        },
        "library": library.to_dict(),
        "publish": publish.to_dict(),
        "lakehouse_bootstrap": bootstrap.to_dict() if bootstrap else None,
        "lakehouse_tables": (
            table_verification.to_dict() if table_verification else None
        ),
        "lakehouse_folders": {
            path: item.to_dict() for path, item in directories.items()
        },
        "connections": {
            key: item.to_dict() for key, item in connections.items()
        },
        "powerBi": {
            bundle: [item.to_dict() for item in operations]
            for bundle, operations in power_bi.items()
        },
        "access": {key: item.to_dict() for key, item in access.items()},
        "reporting_refresh": {
            name: item.to_dict() for name, item in reporting.items()
        },
        "activator": activator.to_dict() if activator else None,
        "schedules": {
            role: evidence.to_dict() for role, evidence in schedules.items()
        },
        "operatorCheckpoints": list(checkpoints),
        "releaseManifest": None,
    }


def reconcile_runtime(
    client: Any, manifest: DeploymentManifest, *, whl_path: Path, enable_schedules: bool,
    bootstrap_lakehouse: bool = False,
    refresh_reporting: bool = False,
    enable_activator: bool = False,
    activator_bundle_path: Path | None = None,
    activator_checkpoint_path: Path | None = None,
    runtime_contract: Mapping[str, Any] | None = None,
    terraform_output: Mapping[str, Any] | None = None,
    onelake_client: Any | None = None,
    power_bi_client: Any | None = None,
    evidence_context: Mapping[str, Any] | None = None,
    env: Mapping[str, str] | None = None,
    now: datetime | None = None, sleep=time.sleep, monotonic=time.monotonic,
) -> dict[str, Any]:
    ws, env_id = manifest.workspace_id, manifest.fabric_environment_id
    if ws is None or env_id is None:
        raise ReconcileError(f"{manifest.stage.value} is missing workspace/environment identity")
    contract = (
        validate_runtime_contract(runtime_contract)
        if runtime_contract is not None
        else {
            "schema": RUNTIME_CONTRACT_SCHEMA,
            "lakehouse": {},
            "connections": [],
            "powerBi": {},
            "access": {},
        }
    )
    tf_output = terraform_output or {}
    connection_ids = _terraform_mapping(tf_output, "connection_ids")

    _reconcile_all_schedules(
        client,
        manifest,
        enabled=False,
        now=now,
    )
    library = reconcile_environment_library(client, ws=ws, env_id=env_id, whl_path=whl_path)
    publish = reconcile_environment_publish(client, ws=ws, env_id=env_id, sleep=sleep, monotonic=monotonic)
    bootstrap = _run_lakehouse_bootstrap(
        client,
        manifest,
        enabled=bootstrap_lakehouse,
        sleep=sleep,
        monotonic=monotonic,
    )

    lakehouse_contract = _as_mapping(contract["lakehouse"], "lakehouse")
    table_verification, directories = _reconcile_lakehouse_runtime(
        client,
        onelake_client,
        manifest,
        lakehouse_contract,
        sleep=sleep,
        monotonic=monotonic,
    )
    connections, access, power_bi, checkpoints = _reconcile_external_runtime(
        client,
        power_bi_client,
        manifest,
        contract,
        connection_ids=connection_ids,
        env=env or os.environ,
    )
    reporting = (
        reconcile_reporting_refresh(
            client,
            manifest,
            sleep=sleep,
            monotonic=monotonic,
        )
        if refresh_reporting
        else {}
    )
    activator, activator_checkpoints = _reconcile_activator_gate(
        client,
        manifest,
        enabled=enable_activator,
        bundle_path=activator_bundle_path,
        checkpoint_path=activator_checkpoint_path,
        now=now,
    )
    checkpoints.extend(activator_checkpoints)
    effective_enable_schedules = enable_schedules and not checkpoints
    schedules = _reconcile_all_schedules(
        client,
        manifest,
        enabled=effective_enable_schedules,
        now=now,
    )
    generated_at = (now or datetime.now(timezone.utc)).isoformat()
    context = dict(evidence_context or {})
    result = _runtime_evidence(
        manifest,
        requested_schedules=enable_schedules,
        enabled_schedules=effective_enable_schedules,
        generated_at=generated_at,
        context=context,
        connection_ids=connection_ids,
        library=library,
        publish=publish,
        bootstrap=bootstrap,
        table_verification=table_verification,
        directories=directories,
        connections=connections,
        power_bi=power_bi,
        access=access,
        reporting=reporting,
        activator=activator,
        schedules=schedules,
        checkpoints=checkpoints,
    )
    release = _publish_release_manifest(
        onelake_client,
        manifest,
        lakehouse_contract,
        context,
        result,
    )
    if release is not None:
        result["releaseManifest"] = release.to_dict()
    return redact_evidence(result)


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ReconcileError(f"{label} at {path} could not be read as JSON: {error}") from error
    if not isinstance(document, dict):
        raise ReconcileError(f"{label} at {path} must be a JSON object")
    return document


def _definition_hashes(document: Mapping[str, Any]) -> dict[str, str]:
    hashes: dict[str, str] = {}

    def _walk(value: object, path: str) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                qualified = f"{path}.{key}" if path else str(key)
                if (
                    str(key).lower().endswith("sha256")
                    and isinstance(item, str)
                    and re.fullmatch(r"[0-9a-fA-F]{64}", item)
                ):
                    hashes[qualified] = item.lower()
                else:
                    _walk(item, qualified)
        elif isinstance(value, list):
            for index, item in enumerate(value):
                _walk(item, f"{path}[{index}]")

    _walk(document, "")
    return hashes


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-path", required=True, type=Path)
    parser.add_argument("--stage", required=True, choices=[s.value for s in DeploymentEnvironment])
    parser.add_argument("--terraform-output-path", type=Path, default=None)
    parser.add_argument("--whl-path", required=True, type=Path)
    parser.add_argument("--enable-schedules", action="store_true")
    parser.add_argument("--bootstrap-lakehouse", action="store_true")
    parser.add_argument("--refresh-reporting", action="store_true")
    parser.add_argument("--enable-activator", action="store_true")
    parser.add_argument("--activator-bundle-path", type=Path)
    parser.add_argument("--activator-checkpoint-path", type=Path)
    parser.add_argument("--runtime-contract-path", type=Path)
    parser.add_argument("--definition-evidence-path", type=Path)
    parser.add_argument("--commit-sha", default=os.environ.get("GITHUB_SHA"))
    parser.add_argument("--api-root", default="https://api.fabric.microsoft.com/v1")
    parser.add_argument(
        "--power-bi-api-root",
        default="https://api.powerbi.com/v1.0/myorg",
    )
    parser.add_argument(
        "--onelake-api-root",
        default="https://onelake.dfs.fabric.microsoft.com",
    )
    return parser


def _execute(arguments: argparse.Namespace) -> tuple[DeploymentEnvironment, dict[str, Any]]:
    from people_counter.fabric_automation import FabricAutomationClient
    from people_counter.fabric_rest import UrllibTransport

    stage = DeploymentEnvironment(arguments.stage)
    document = _load_json(arguments.manifest_path, "deployment manifest")
    terraform_output: dict[str, Any] = {}
    if arguments.terraform_output_path is not None:
        terraform_output = _load_json(
            arguments.terraform_output_path, "terraform output"
        )
        document = merge_terraform_output(document, terraform_output)
    runtime_contract = (
        _load_json(arguments.runtime_contract_path, "runtime contract")
        if arguments.runtime_contract_path is not None
        else None
    )
    definition_evidence = (
        _load_json(arguments.definition_evidence_path, "definition evidence")
        if arguments.definition_evidence_path is not None
        else {}
    )
    context = {
        "commitSha": arguments.commit_sha,
        "terraformOutputSha256": (
            _sha256(terraform_output) if terraform_output else None
        ),
        "definitionHashes": _definition_hashes(definition_evidence),
    }
    manifest = parse_deployment_manifest(document, stage)
    transport = UrllibTransport()
    client = FabricAutomationClient(
        AzureCliResourceTokenProvider("https://api.fabric.microsoft.com"),
        transport=transport,
        api_root=arguments.api_root,
    )
    onelake_client = FabricAutomationClient(
        AzureCliResourceTokenProvider("https://storage.azure.com/"),
        transport=transport,
        api_root=arguments.onelake_api_root,
        default_headers={"x-ms-version": "2023-11-03"},
    )
    power_bi_client = FabricAutomationClient(
        AzureCliResourceTokenProvider(
            "https://analysis.windows.net/powerbi/api"
        ),
        transport=transport,
        api_root=arguments.power_bi_api_root,
    )
    result = reconcile_runtime(
        client,
        manifest,
        whl_path=arguments.whl_path,
        enable_schedules=arguments.enable_schedules,
        bootstrap_lakehouse=arguments.bootstrap_lakehouse,
        refresh_reporting=arguments.refresh_reporting,
        enable_activator=arguments.enable_activator,
        activator_bundle_path=arguments.activator_bundle_path,
        activator_checkpoint_path=arguments.activator_checkpoint_path,
        runtime_contract=runtime_contract,
        terraform_output=terraform_output,
        onelake_client=onelake_client,
        power_bi_client=power_bi_client,
        evidence_context=context,
    )
    return stage, result


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _argument_parser().parse_args(argv)
    stage = DeploymentEnvironment(arguments.stage)
    try:
        _, result = _execute(arguments)
    except (FabricApiError, ReconcileError, ValueError) as error:
        failure = {
            "schema": EVIDENCE_SCHEMA,
            "status": "failed",
            "stage": stage.value,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "error": redact_evidence(str(error)),
        }
        print(json.dumps(failure, allow_nan=False, indent=2, sort_keys=True))
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, allow_nan=False, indent=2, sort_keys=True))
    return {
        "succeeded": 0,
        "operator_checkpoint_required": 2,
    }.get(str(result.get("status")), 1)


if __name__ == "__main__":
    raise SystemExit(main())
