"""Validation and live verification for Fabric Activator (Reflex) items."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from people_counter.fabric_automation import DeploymentEvidence
from people_counter.fabric_definition_bundle import (
    METADATA_FILE,
    DefinitionBundleError,
    read_definition_bundle,
)
from people_counter.fabric_deployment_manifest import DeploymentManifest

ACTIVATOR_CHECKPOINT_SCHEMA = "people-counter-activator-action-checkpoint-v1"
ACTIVATOR_EVIDENCE_SCHEMA = "people-counter-activator-verification-v1"
REFLEX_ENTITIES_PART = "ReflexEntities.json"
_PLATFORM_PART = ".platform"
_SUPPORTED_ENTITY_TYPES = frozenset(
    {
        "container-v1",
        "eventstreamSource-v1",
        "fabricItemAction-v1",
        "kqlSource-v1",
        "realTimeHubSource-v1",
        "simulatorSource-v1",
        "timeSeriesView-v1",
    }
)
_SUPPORTED_SOURCE_TYPES = frozenset(
    {
        "eventstreamSource-v1",
        "kqlSource-v1",
        "realTimeHubSource-v1",
        "simulatorSource-v1",
    }
)
_SUPPORTED_ACTION_KINDS = frozenset(
    {"EmailMessage", "FabricItemInvocation", "TeamsMessage"}
)
_UNSUPPORTED_LIFECYCLE_SOURCE_TYPES = frozenset(
    {"azureBlobStorageEventsSource-v1", "powerBISource-v1"}
)
_UNSUPPORTED_LIFECYCLE_ACTION_ITEM_TYPES = frozenset({"UserDataFunction"})


class ActivatorAutomationError(RuntimeError):
    """An Activator definition or live contract cannot be verified."""


class UnsupportedActivatorConfiguration(ActivatorAutomationError):
    """The definition uses a documented lifecycle-management exclusion."""


@dataclass(frozen=True, slots=True)
class ActivatorAction:
    action_id: str
    rule_id: str
    rule_name: str
    kind: str
    target_workspace_id: str | None = None
    target_item_id: str | None = None
    target_item_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "actionId": self.action_id,
            "kind": self.kind,
            "ruleId": self.rule_id,
            "ruleName": self.rule_name,
            "targetItemId": self.target_item_id,
            "targetItemType": self.target_item_type,
            "targetWorkspaceId": self.target_workspace_id,
        }


@dataclass(frozen=True, slots=True)
class ActivatorRule:
    rule_id: str
    name: str
    enabled: bool

    def to_dict(self) -> dict[str, Any]:
        return {"enabled": self.enabled, "name": self.name, "ruleId": self.rule_id}


@dataclass(frozen=True, slots=True)
class ActivatorDefinitionEvidence:
    reflex_entities_sha256: str
    entity_count: int
    source_types: tuple[str, ...]
    rules: tuple[ActivatorRule, ...]
    actions: tuple[ActivatorAction, ...]
    bundle_content_sha256: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "actions": [action.to_dict() for action in self.actions],
            "bundleContentSha256": self.bundle_content_sha256,
            "entityCount": self.entity_count,
            "reflexEntitiesSha256": self.reflex_entities_sha256,
            "rules": [rule.to_dict() for rule in self.rules],
            "schema": ACTIVATOR_EVIDENCE_SCHEMA,
            "sourceTypes": list(self.source_types),
        }


def _uuid_text(value: object, context: str, *, allow_token: bool = False) -> str:
    if allow_token and isinstance(value, str) and value.startswith("{{ .") and value.endswith(" }}"):
        return value
    if not isinstance(value, str):
        raise ActivatorAutomationError(f"{context} must be a UUID")
    try:
        return str(uuid.UUID(value))
    except ValueError as error:
        raise ActivatorAutomationError(f"{context} must be a UUID") from error


def _mapping(value: object, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ActivatorAutomationError(f"{context} must be an object")
    return value


def _list(value: object, context: str) -> list[Any]:
    if not isinstance(value, list):
        raise ActivatorAutomationError(f"{context} must be an array")
    return value


def _decode_definition_parts(
    value: Mapping[str, Any],
) -> tuple[str, dict[str, bytes]]:
    definition = _mapping(value.get("definition"), "Reflex definition")
    format_name = definition.get("format", "Default")
    if format_name != "Default":
        raise ActivatorAutomationError(
            f"Reflex definition format must be 'Default', got {format_name!r}"
        )
    parts: dict[str, bytes] = {}
    for index, raw_part in enumerate(
        _list(definition.get("parts"), "Reflex definition parts")
    ):
        part = _mapping(raw_part, f"Reflex definition part {index}")
        path = part.get("path")
        if not isinstance(path, str) or path not in {
            REFLEX_ENTITIES_PART,
            _PLATFORM_PART,
        }:
            raise ActivatorAutomationError(
                f"unsupported Reflex definition part {path!r}"
            )
        if path in parts:
            raise ActivatorAutomationError(f"duplicate Reflex definition part {path!r}")
        if part.get("payloadType") != "InlineBase64":
            raise ActivatorAutomationError(
                f"Reflex definition part {path!r} must use InlineBase64"
            )
        payload = part.get("payload")
        if not isinstance(payload, str):
            raise ActivatorAutomationError(
                f"Reflex definition part {path!r} has no payload"
            )
        try:
            parts[path] = base64.b64decode(payload, validate=True)
        except (ValueError, binascii.Error) as error:
            raise ActivatorAutomationError(
                f"Reflex definition part {path!r} is not valid base64"
            ) from error
    if REFLEX_ENTITIES_PART not in parts:
        raise ActivatorAutomationError(
            f"Reflex definition requires {REFLEX_ENTITIES_PART}"
        )
    if _PLATFORM_PART in parts:
        try:
            platform = json.loads(parts[_PLATFORM_PART])
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ActivatorAutomationError(".platform must be valid JSON") from error
        if not isinstance(platform, Mapping):
            raise ActivatorAutomationError(".platform must contain a JSON object")
    return str(format_name), parts


def _instance(
    definition: Mapping[str, Any], entity_id: str
) -> Mapping[str, Any] | None:
    raw = definition.get("instance")
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ActivatorAutomationError(
            f"timeSeriesView-v1 {entity_id} definition.instance must be a JSON string"
        )
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ActivatorAutomationError(
            f"timeSeriesView-v1 {entity_id} definition.instance is invalid JSON"
        ) from error
    return _mapping(value, f"timeSeriesView-v1 {entity_id} definition.instance")


def _argument_entity_id(row: Mapping[str, Any]) -> str | None:
    stack: list[object] = [row.get("arguments", [])]
    for current in stack:
        if isinstance(current, list):
            stack.extend(current)
        elif isinstance(current, Mapping):
            if current.get("name") == "entityId" and isinstance(
                current.get("value"), str
            ):
                return str(current["value"])
            stack.extend(current.values())
    return None


def _fabric_action_targets(
    entities: Sequence[Mapping[str, Any]],
) -> dict[str, tuple[str, str, str]]:
    targets: dict[str, tuple[str, str, str]] = {}
    for entity in entities:
        if entity.get("type") != "fabricItemAction-v1":
            continue
        entity_id = _uuid_text(entity.get("uniqueIdentifier"), "Fabric action ID")
        payload = _mapping(entity.get("payload"), f"Fabric action {entity_id} payload")
        item = _mapping(
            payload.get("fabricItem"), f"Fabric action {entity_id} fabricItem"
        )
        item_type = item.get("itemType")
        if not isinstance(item_type, str) or not item_type:
            raise ActivatorAutomationError(
                f"Fabric action {entity_id} itemType is missing"
            )
        if item_type in _UNSUPPORTED_LIFECYCLE_ACTION_ITEM_TYPES:
            raise UnsupportedActivatorConfiguration(
                f"Activator lifecycle management does not support "
                f"{item_type!r} actions"
            )
        targets[entity_id] = (
            _uuid_text(
                item.get("workspaceId"),
                f"Fabric action {entity_id} workspaceId",
                allow_token=True,
            ),
            _uuid_text(
                item.get("itemId"),
                f"Fabric action {entity_id} itemId",
                allow_token=True,
            ),
            item_type,
        )
    return targets


def _rule_actions(
    entity_id: str,
    rule_name: str,
    instance: Mapping[str, Any],
    targets: Mapping[str, tuple[str, str, str]],
) -> list[ActivatorAction]:
    actions: list[ActivatorAction] = []
    steps = _list(instance.get("steps"), f"rule {rule_name!r} steps")
    for step_index, raw_step in enumerate(steps):
        step = _mapping(raw_step, f"rule {rule_name!r} step {step_index}")
        step_name = step.get("name")
        rows = _list(step.get("rows"), f"rule {rule_name!r} step rows")
        action_step = step_name in {"ActStep", "ActionStep"}
        for row_index, raw_row in enumerate(rows):
            row = _mapping(raw_row, f"rule {rule_name!r} action row")
            kind = row.get("kind")
            if action_step and kind not in _SUPPORTED_ACTION_KINDS:
                raise UnsupportedActivatorConfiguration(
                    f"rule {rule_name!r} uses unsupported action kind {kind!r}"
                )
            if kind not in _SUPPORTED_ACTION_KINDS:
                continue
            target = (None, None, None)
            if kind == "FabricItemInvocation":
                target_id = _argument_entity_id(row)
                if target_id is None or target_id not in targets:
                    raise ActivatorAutomationError(
                        f"rule {rule_name!r} FabricItemInvocation does not reference "
                        "a fabricItemAction-v1 entity"
                    )
                target = targets[target_id]
            step_id = step.get("id")
            action_id = f"{entity_id}:{step_id or step_index}:{row_index}"
            actions.append(
                ActivatorAction(
                    action_id=action_id,
                    rule_id=entity_id,
                    rule_name=rule_name,
                    kind=str(kind),
                    target_workspace_id=target[0],
                    target_item_id=target[1],
                    target_item_type=target[2],
                )
            )
    return actions


def validate_reflex_definition(
    value: Mapping[str, Any],
    *,
    bundle_content_sha256: str | None = None,
) -> ActivatorDefinitionEvidence:
    """Validate the documented public Reflex definition contract."""
    _, parts = _decode_definition_parts(value)
    try:
        entities_value = json.loads(parts[REFLEX_ENTITIES_PART])
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ActivatorAutomationError(
            f"{REFLEX_ENTITIES_PART} must be valid JSON"
        ) from error
    entities = [
        _mapping(item, f"{REFLEX_ENTITIES_PART} entity {index}")
        for index, item in enumerate(
            _list(entities_value, REFLEX_ENTITIES_PART)
        )
    ]
    if not entities:
        raise ActivatorAutomationError(f"{REFLEX_ENTITIES_PART} cannot be empty")

    ids: set[str] = set()
    source_types: list[str] = []
    for entity in entities:
        entity_id = _uuid_text(entity.get("uniqueIdentifier"), "Reflex entity ID")
        if entity_id in ids:
            raise ActivatorAutomationError(f"duplicate Reflex entity ID {entity_id}")
        ids.add(entity_id)
        entity_type = entity.get("type")
        if entity_type in _UNSUPPORTED_LIFECYCLE_SOURCE_TYPES:
            raise UnsupportedActivatorConfiguration(
                f"Activator lifecycle management does not support "
                f"{entity_type!r} sources"
            )
        if entity_type not in _SUPPORTED_ENTITY_TYPES:
            raise UnsupportedActivatorConfiguration(
                f"unsupported Reflex entity type {entity_type!r}"
            )
        _mapping(entity.get("payload"), f"Reflex entity {entity_id} payload")
        if entity_type in _SUPPORTED_SOURCE_TYPES:
            source_types.append(str(entity_type))

    targets = _fabric_action_targets(entities)
    rules: list[ActivatorRule] = []
    actions: list[ActivatorAction] = []
    for entity in entities:
        if entity.get("type") != "timeSeriesView-v1":
            continue
        entity_id = _uuid_text(entity.get("uniqueIdentifier"), "time series view ID")
        payload = _mapping(entity.get("payload"), f"time series view {entity_id} payload")
        definition = _mapping(
            payload.get("definition"), f"time series view {entity_id} definition"
        )
        instance = _instance(definition, entity_id)
        if definition.get("type") != "Rule":
            continue
        name = payload.get("name")
        if not isinstance(name, str) or not name:
            raise ActivatorAutomationError(f"rule {entity_id} name is missing")
        settings = definition.get("settings", {})
        settings = _mapping(settings, f"rule {name!r} settings")
        enabled = settings.get("shouldRun", False)
        if not isinstance(enabled, bool):
            raise ActivatorAutomationError(
                f"rule {name!r} settings.shouldRun must be boolean"
            )
        if instance is None:
            raise ActivatorAutomationError(f"rule {name!r} has no template instance")
        rules.append(ActivatorRule(entity_id, name, enabled))
        actions.extend(_rule_actions(entity_id, name, instance, targets))

    return ActivatorDefinitionEvidence(
        reflex_entities_sha256=hashlib.sha256(
            parts[REFLEX_ENTITIES_PART]
        ).hexdigest(),
        entity_count=len(entities),
        source_types=tuple(sorted(source_types)),
        rules=tuple(sorted(rules, key=lambda item: item.rule_id)),
        actions=tuple(sorted(actions, key=lambda item: item.action_id)),
        bundle_content_sha256=bundle_content_sha256,
    )


def validate_reflex_bundle(path: Path) -> ActivatorDefinitionEvidence:
    """Validate bundle integrity, Reflex item metadata, and public definition."""
    try:
        definition = read_definition_bundle(path)
        metadata = json.loads((path / METADATA_FILE).read_text(encoding="utf-8"))
    except (DefinitionBundleError, OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ActivatorAutomationError(f"invalid Reflex bundle at {path}: {error}") from error
    if metadata.get("itemType") != "Reflex":
        raise ActivatorAutomationError("Activator bundle itemType must be 'Reflex'")
    content_sha = metadata.get("contentSha256")
    if not isinstance(content_sha, str):
        raise ActivatorAutomationError("Activator bundle content hash is missing")
    return validate_reflex_definition(
        definition, bundle_content_sha256=content_sha
    )


def _selected_actions(
    evidence: ActivatorDefinitionEvidence, trigger_name: str
) -> tuple[ActivatorRule, tuple[ActivatorAction, ...]]:
    matching = tuple(rule for rule in evidence.rules if rule.name == trigger_name)
    if len(matching) != 1:
        raise ActivatorAutomationError(
            f"expected exactly one Activator rule named {trigger_name!r}, "
            f"found {len(matching)}"
        )
    rule = matching[0]
    actions = tuple(
        action for action in evidence.actions if action.rule_id == rule.rule_id
    )
    if not actions:
        raise ActivatorAutomationError(
            f"Activator rule {trigger_name!r} has no supported actions"
        )
    return rule, actions


def _parse_timestamp(value: object, context: str) -> datetime:
    if not isinstance(value, str):
        raise ActivatorAutomationError(f"{context} must be an ISO 8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ActivatorAutomationError(
            f"{context} must be an ISO 8601 timestamp"
        ) from error
    if parsed.tzinfo is None:
        raise ActivatorAutomationError(f"{context} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _checkpoint_requirement(
    *,
    workspace_id: str,
    reflex_id: str,
    trigger_name: str,
    evidence: ActivatorDefinitionEvidence,
    actions: Sequence[ActivatorAction],
) -> dict[str, Any]:
    return {
        "actions": [
            {
                "actionId": action.action_id,
                "evidenceReference": "<durable operator evidence URI or ID>",
                "kind": action.kind,
                "result": "succeeded",
            }
            for action in actions
        ],
        "expiresAt": "<ISO 8601 timestamp>",
        "reflexEntitiesSha256": evidence.reflex_entities_sha256,
        "reflexId": reflex_id,
        "schema": ACTIVATOR_CHECKPOINT_SCHEMA,
        "triggerName": trigger_name,
        "verifiedAt": "<ISO 8601 timestamp>",
        "verifiedBy": "<operator identity>",
        "workspaceId": workspace_id,
    }


def validate_action_checkpoint(
    value: Mapping[str, Any],
    *,
    workspace_id: str,
    reflex_id: str,
    trigger_name: str,
    evidence: ActivatorDefinitionEvidence,
    actions: Sequence[ActivatorAction],
    now: datetime | None = None,
) -> dict[str, Any]:
    allowed = {
        "actions",
        "expiresAt",
        "reflexEntitiesSha256",
        "reflexId",
        "schema",
        "triggerName",
        "verifiedAt",
        "verifiedBy",
        "workspaceId",
    }
    if set(value) != allowed:
        raise ActivatorAutomationError(
            "Activator checkpoint fields do not match the required contract"
        )
    expected_scalars = {
        "schema": ACTIVATOR_CHECKPOINT_SCHEMA,
        "workspaceId": workspace_id,
        "reflexId": reflex_id,
        "triggerName": trigger_name,
        "reflexEntitiesSha256": evidence.reflex_entities_sha256,
    }
    for field, expected in expected_scalars.items():
        if value.get(field) != expected:
            raise ActivatorAutomationError(
                f"Activator checkpoint {field} does not match the live definition"
            )
    verifier = value.get("verifiedBy")
    if not isinstance(verifier, str) or not verifier.strip():
        raise ActivatorAutomationError("Activator checkpoint verifiedBy is required")
    verified_at = _parse_timestamp(value.get("verifiedAt"), "verifiedAt")
    expires_at = _parse_timestamp(value.get("expiresAt"), "expiresAt")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if verified_at > current or expires_at <= verified_at or current >= expires_at:
        raise ActivatorAutomationError(
            "Activator checkpoint is not currently valid"
        )

    actual: dict[str, Mapping[str, Any]] = {}
    for raw in _list(value.get("actions"), "Activator checkpoint actions"):
        item = _mapping(raw, "Activator checkpoint action")
        if set(item) != {"actionId", "evidenceReference", "kind", "result"}:
            raise ActivatorAutomationError(
                "Activator checkpoint action fields are invalid"
            )
        action_id = item.get("actionId")
        if not isinstance(action_id, str) or action_id in actual:
            raise ActivatorAutomationError(
                "Activator checkpoint actionId is invalid or duplicated"
            )
        if item.get("result") != "succeeded":
            raise ActivatorAutomationError(
                f"Activator action {action_id!r} was not verified as succeeded"
            )
        reference = item.get("evidenceReference")
        if not isinstance(reference, str) or not reference.strip():
            raise ActivatorAutomationError(
                f"Activator action {action_id!r} has no durable evidence reference"
            )
        actual[action_id] = item
    expected = {action.action_id: action for action in actions}
    if set(actual) != set(expected):
        raise ActivatorAutomationError(
            "Activator checkpoint does not cover exactly the live rule actions"
        )
    for action_id, action in expected.items():
        if actual[action_id].get("kind") != action.kind:
            raise ActivatorAutomationError(
                f"Activator checkpoint kind mismatch for {action_id!r}"
            )
    return {
        "expiresAt": expires_at.isoformat(),
        "verifiedAt": verified_at.isoformat(),
        "verifiedBy": verifier,
    }


def _topology(
    evidence: ActivatorDefinitionEvidence, trigger_name: str
) -> tuple[tuple[str, ...], str, tuple[tuple[str, str, str | None], ...]]:
    rule, actions = _selected_actions(evidence, trigger_name)
    return (
        evidence.source_types,
        rule.rule_id,
        tuple(
            sorted(
                (action.action_id, action.kind, action.target_item_type)
                for action in actions
            )
        ),
    )


def verify_activator_runtime(
    client: Any,
    manifest: DeploymentManifest,
    *,
    bundle_path: Path,
    checkpoint_path: Path | None = None,
    action_target_probe: Callable[[ActivatorAction], DeploymentEvidence] | None = None,
    now: datetime | None = None,
) -> DeploymentEvidence:
    """Verify deployed configuration and require execution evidence for actions."""
    workspace_id = manifest.workspace_id
    reflex_id = manifest.activator.reflex_id
    trigger_name = manifest.activator.trigger_name
    if workspace_id is None or reflex_id is None or trigger_name is None:
        raise ActivatorAutomationError(
            "Activator verification requires workspace_id, activator.reflex_id, "
            "and activator.trigger_name"
        )
    expected = validate_reflex_bundle(bundle_path)
    item = client.request_json(
        "GET", f"/workspaces/{workspace_id}/reflexes/{reflex_id}"
    )
    if (
        item.get("id") != reflex_id
        or item.get("workspaceId") != workspace_id
        or item.get("type") != "Reflex"
    ):
        raise ActivatorAutomationError(
            "live Activator identity/type does not match the deployment manifest"
        )
    readback = client.run_operation(
        "POST",
        f"/workspaces/{workspace_id}/reflexes/{reflex_id}/getDefinition",
        action="get_activator_definition",
        accepted=(200, 202),
    )
    if readback.status != "succeeded" or readback.resource is None:
        return DeploymentEvidence(
            action="verify_activator",
            status="failed",
            operation_id=readback.operation_id,
            resource=readback.resource,
            error=readback.error or "Activator definition readback returned no resource",
            attempts=readback.attempts,
        )
    live = validate_reflex_definition(readback.resource)
    if _topology(expected, trigger_name) != _topology(live, trigger_name):
        raise ActivatorAutomationError(
            "live Activator rule/action topology differs from the source bundle"
        )
    rule, actions = _selected_actions(live, trigger_name)
    if not rule.enabled:
        raise ActivatorAutomationError(
            f"Activator rule {trigger_name!r} is not enabled in live readback"
        )

    target_evidence = []
    for action in actions:
        if action.target_item_id is None:
            continue
        if action_target_probe is not None:
            result = action_target_probe(action)
        else:
            target = client.request_json(
                "GET",
                f"/workspaces/{action.target_workspace_id}/items/"
                f"{action.target_item_id}",
            )
            status = (
                "succeeded"
                if target.get("id") == action.target_item_id
                and target.get("type") == action.target_item_type
                else "failed"
            )
            result = DeploymentEvidence(
                action=f"verify_activator_target:{action.action_id}",
                status=status,
                resource=target,
                error=None if status == "succeeded" else "Fabric action target mismatch",
            )
        if result.status != "succeeded":
            raise ActivatorAutomationError(
                f"Activator target verification failed for {action.action_id}: "
                f"{result.error}"
            )
        target_evidence.append(result.to_dict())

    requirement = _checkpoint_requirement(
        workspace_id=workspace_id,
        reflex_id=reflex_id,
        trigger_name=trigger_name,
        evidence=live,
        actions=actions,
    )
    resource = {
        "actionExecutionVerified": False,
        "definition": live.to_dict(),
        "item": dict(item),
        "operatorCheckpointRequired": requirement,
        "sourceBundle": expected.to_dict(),
        "targetVerification": target_evidence,
    }
    if checkpoint_path is None:
        return DeploymentEvidence(
            action="verify_activator",
            status="operator_checkpoint_required",
            resource=resource,
            error=(
                "Fabric exposes no generic public API to force an Activator rule and "
                "prove its side effect; provide the required operator checkpoint"
            ),
        )
    try:
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ActivatorAutomationError(
            f"Activator checkpoint at {checkpoint_path} is invalid: {error}"
        ) from error
    checkpoint = _mapping(checkpoint, "Activator checkpoint")
    verification = validate_action_checkpoint(
        checkpoint,
        workspace_id=workspace_id,
        reflex_id=reflex_id,
        trigger_name=trigger_name,
        evidence=live,
        actions=actions,
        now=now,
    )
    resource["actionExecutionVerified"] = True
    resource["operatorCheckpoint"] = verification
    resource.pop("operatorCheckpointRequired")
    return DeploymentEvidence(
        action="verify_activator",
        status="succeeded",
        resource=resource,
    )


def probe_service_principal_crud_contract(
    client: Any,
    *,
    workspace_id: str,
    probe_id: str | None = None,
) -> DeploymentEvidence:
    """Probe documented Reflex CRUD APIs using the caller's current identity."""
    suffix = probe_id or uuid.uuid4().hex
    display_name = f"pc-activator-sp-contract-{suffix}"
    updated_name = f"{display_name}-updated"
    base = f"/workspaces/{workspace_id}/reflexes"
    reflex_id: str | None = None
    steps: list[dict[str, Any]] = []
    primary_error: Exception | None = None
    try:
        created = client.run_operation(
            "POST",
            base,
            action="probe_activator_create",
            value={
                "description": "Disposable service-principal CRUD contract probe.",
                "displayName": display_name,
            },
            accepted=(201, 202),
        )
        if created.status != "succeeded" or created.resource is None:
            raise ActivatorAutomationError(
                f"Activator create probe failed: {created.error}"
            )
        steps.append(created.to_dict())
        reflex_id = _uuid_text(
            created.resource.get("id"), "created Activator probe ID"
        )
        read = client.request_json("GET", f"{base}/{reflex_id}")
        if read.get("type") != "Reflex" or read.get("displayName") != display_name:
            raise ActivatorAutomationError("Activator create/read probe mismatch")
        steps.append(
            DeploymentEvidence(
                action="probe_activator_read",
                status="succeeded",
                resource=read,
            ).to_dict()
        )
        updated = client.request_json(
            "PATCH",
            f"{base}/{reflex_id}",
            value={"displayName": updated_name},
        )
        if updated.get("displayName") != updated_name:
            raise ActivatorAutomationError("Activator update probe mismatch")
        steps.append(
            DeploymentEvidence(
                action="probe_activator_update",
                status="succeeded",
                resource=updated,
            ).to_dict()
        )
        listed = client.list_pages(base)
        if not any(item.get("id") == reflex_id for item in listed):
            raise ActivatorAutomationError("Activator list probe omitted created item")
        steps.append(
            DeploymentEvidence(
                action="probe_activator_list",
                status="succeeded",
                resource={"id": reflex_id},
            ).to_dict()
        )
    except Exception as error:
        primary_error = error
    finally:
        cleanup_error: Exception | None = None
        if reflex_id is not None:
            try:
                client.request_json(
                    "DELETE",
                    f"{base}/{reflex_id}?hardDelete=true",
                    expected=(200,),
                )
                steps.append(
                    DeploymentEvidence(
                        action="probe_activator_delete",
                        status="succeeded",
                        resource={"id": reflex_id, "hardDelete": True},
                    ).to_dict()
                )
            except Exception as error:
                cleanup_error = error
        if primary_error is not None or cleanup_error is not None:
            detail = f"probe failed: {primary_error}" if primary_error else ""
            if cleanup_error is not None:
                detail += f"; cleanup failed: {cleanup_error}"
            raise ActivatorAutomationError(detail.strip("; ")) from (
                primary_error or cleanup_error
            )
    return DeploymentEvidence(
        action="probe_activator_service_principal_crud",
        status="succeeded",
        resource={
            "contract": "documented-reflex-v1-crud",
            "steps": steps,
            "workspaceId": workspace_id,
        },
    )
