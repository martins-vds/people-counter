from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from people_counter.fabric_activator import (
    ACTIVATOR_CHECKPOINT_SCHEMA,
    ActivatorAutomationError,
    UnsupportedActivatorConfiguration,
    probe_service_principal_crud_contract,
    validate_action_checkpoint,
    validate_reflex_bundle,
    validate_reflex_definition,
    verify_activator_runtime,
)
from people_counter.fabric_automation import DeploymentEvidence
from people_counter.fabric_definition_bundle import write_definition_bundle
from people_counter.fabric_deployment_manifest import (
    DeploymentEnvironment,
    parse_deployment_manifest,
)
from scripts.validate_activator_definition import main as validate_main

_WS = "11111111-1111-1111-1111-111111111111"
_REFLEX = "22222222-2222-2222-2222-222222222222"
_CONTAINER = "33333333-3333-3333-3333-333333333333"
_SOURCE = "44444444-4444-4444-4444-444444444444"
_RULE = "55555555-5555-5555-5555-555555555555"
_ACTION = "66666666-6666-6666-6666-666666666666"
_TARGET = "77777777-7777-7777-7777-777777777777"
_NOW = datetime(2026, 10, 13, tzinfo=timezone.utc)


def _entities(
    *,
    source_type: str = "kqlSource-v1",
    action_kind: str = "FabricItemInvocation",
    enabled: bool = True,
    target_type: str = "Pipeline",
) -> list[dict[str, Any]]:
    action_arguments = (
        [{"name": "entityId", "type": "string", "value": _ACTION}]
        if action_kind == "FabricItemInvocation"
        else []
    )
    return [
        {
            "uniqueIdentifier": _CONTAINER,
            "payload": {"name": "Operations", "type": "kqlQueries"},
            "type": "container-v1",
        },
        {
            "uniqueIdentifier": _SOURCE,
            "payload": {
                "name": "Counts",
                "parentContainer": {"targetUniqueIdentifier": _CONTAINER},
            },
            "type": source_type,
        },
        {
            "uniqueIdentifier": _ACTION,
            "payload": {
                "name": "Run pipeline",
                "fabricItem": {
                    "itemId": _TARGET,
                    "workspaceId": _WS,
                    "itemType": target_type,
                },
                "jobType": target_type,
                "parentContainer": {"targetUniqueIdentifier": _CONTAINER},
            },
            "type": "fabricItemAction-v1",
        },
        {
            "uniqueIdentifier": _RULE,
            "payload": {
                "name": "Crowding detected",
                "parentContainer": {"targetUniqueIdentifier": _CONTAINER},
                "definition": {
                    "type": "Rule",
                    "instance": json.dumps(
                        {
                            "templateId": "AttributeTrigger",
                            "templateVersion": "1.1",
                            "steps": [
                                {
                                    "name": "ActStep",
                                    "id": "action-step",
                                    "rows": [
                                        {
                                            "name": "Action",
                                            "kind": action_kind,
                                            "arguments": action_arguments,
                                        }
                                    ],
                                }
                            ],
                        },
                        separators=(",", ":"),
                    ),
                    "settings": {"shouldRun": enabled},
                },
            },
            "type": "timeSeriesView-v1",
        },
    ]


def _definition(
    entities: list[dict[str, Any]] | None = None,
    *,
    format_name: str = "Default",
    extra_parts: dict[str, bytes] | None = None,
) -> dict[str, Any]:
    parts = {
        "ReflexEntities.json": json.dumps(
            entities or _entities(), separators=(",", ":")
        ).encode(),
        ".platform": b'{"metadata":{"type":"Reflex"}}',
        **(extra_parts or {}),
    }
    return {
        "definition": {
            "format": format_name,
            "parts": [
                {
                    "path": path,
                    "payload": base64.b64encode(content).decode(),
                    "payloadType": "InlineBase64",
                }
                for path, content in parts.items()
            ],
        }
    }


def _bundle(tmp_path: Path, entities: list[dict[str, Any]] | None = None) -> Path:
    path = tmp_path / "operations"
    write_definition_bundle(
        _definition(entities), path, item_type="Reflex", expected_format="Default"
    )
    return path


def _manifest() -> object:
    path = Path("src/people_counter/data/deploy/dev.json")
    document = json.loads(path.read_text())
    document["workspace_id"] = _WS
    document["activator"] = {
        "reflex_id": _REFLEX,
        "trigger_name": "Crowding detected",
    }
    return parse_deployment_manifest(document, DeploymentEnvironment.DEV)


def test_reflex_bundle_validator_reports_supported_definition_and_cli(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _bundle(tmp_path)

    evidence = validate_reflex_bundle(path)

    assert evidence.entity_count == 4
    assert evidence.source_types == ("kqlSource-v1",)
    assert evidence.rules[0].enabled is True
    assert evidence.actions[0].target_item_type == "Pipeline"
    output = tmp_path / "evidence.json"
    assert validate_main([str(path), "--output", str(output)]) == 0
    assert json.loads(output.read_text())["schema"].endswith("verification-v1")
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize(
    ("entities", "message"),
    [
        (_entities(source_type="powerBISource-v1"), "does not support"),
        (_entities(source_type="futureSource-v2"), "unsupported Reflex entity"),
        (_entities(action_kind="WebhookAction"), "unsupported action kind"),
        (_entities(target_type="UserDataFunction"), "does not support"),
    ],
)
def test_reflex_validator_rejects_unsupported_sources_and_actions(
    entities: list[dict[str, Any]], message: str
) -> None:
    with pytest.raises(UnsupportedActivatorConfiguration, match=message):
        validate_reflex_definition(_definition(entities))


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (_definition(format_name="TMDL"), "format"),
        (_definition(extra_parts={"secret.bin": b"x"}), "unsupported.*part"),
        (
            {
                "definition": {
                    "format": "Default",
                    "parts": [
                        {
                            "path": "ReflexEntities.json",
                            "payload": "not-base64",
                            "payloadType": "InlineBase64",
                        }
                    ],
                }
            },
            "base64",
        ),
    ],
)
def test_reflex_validator_rejects_invalid_public_definition(
    value: dict[str, Any], message: str
) -> None:
    with pytest.raises(ActivatorAutomationError, match=message):
        validate_reflex_definition(value)


@dataclass
class _RuntimeClient:
    definition: dict[str, Any]
    calls: list[tuple[str, str]] = field(default_factory=list)

    def request_json(self, method: str, path: str, **_: object) -> dict[str, Any]:
        self.calls.append((method, path))
        if "/reflexes/" in path:
            return {"id": _REFLEX, "workspaceId": _WS, "type": "Reflex"}
        if path.endswith(f"/items/{_TARGET}"):
            return {"id": _TARGET, "type": "Pipeline"}
        raise AssertionError(path)

    def run_operation(self, method: str, path: str, **_: object) -> DeploymentEvidence:
        self.calls.append((method, path))
        return DeploymentEvidence(
            action="get_activator_definition",
            status="succeeded",
            resource=self.definition,
        )


def _checkpoint(requirement: dict[str, Any]) -> dict[str, Any]:
    return {
        **requirement,
        "verifiedAt": (_NOW - timedelta(minutes=5)).isoformat(),
        "expiresAt": (_NOW + timedelta(hours=1)).isoformat(),
        "verifiedBy": "operator@example.com",
        "actions": [
            {
                **action,
                "evidenceReference": "change://CAB-1234/action-run",
            }
            for action in requirement["actions"]
        ],
    }


def test_runtime_verification_requires_then_accepts_bound_operator_checkpoint(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    client = _RuntimeClient(_definition())

    pending = verify_activator_runtime(
        client, _manifest(), bundle_path=bundle, now=_NOW  # type: ignore[arg-type]
    )

    assert pending.status == "operator_checkpoint_required"
    assert pending.resource is not None
    assert pending.resource["actionExecutionVerified"] is False
    assert (
        "POST",
        f"/workspaces/{_WS}/reflexes/{_REFLEX}/getDefinition",
    ) in client.calls

    deferred = verify_activator_runtime(
        client,
        _manifest(),  # type: ignore[arg-type]
        bundle_path=bundle,
        require_action_checkpoint=False,
        now=_NOW,
    )
    assert deferred.action == "verify_activator"
    assert deferred.status == "succeeded"
    assert deferred.resource is not None
    assert deferred.resource["actionExecutionRequired"] is False
    assert deferred.resource["actionExecutionVerified"] is False
    assert "not enforced" in deferred.resource["actionExecutionDeferral"]
    assert "operatorCheckpointRequired" not in deferred.resource

    requirement = pending.resource["operatorCheckpointRequired"]
    assert requirement["schema"] == ACTIVATOR_CHECKPOINT_SCHEMA
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(json.dumps(_checkpoint(requirement)))

    verified = verify_activator_runtime(
        client,
        _manifest(),  # type: ignore[arg-type]
        bundle_path=bundle,
        checkpoint_path=checkpoint_path,
        now=_NOW,
    )

    assert verified.status == "succeeded"
    assert verified.resource is not None
    assert verified.resource["actionExecutionVerified"] is True
    assert verified.resource["operatorCheckpoint"]["verifiedBy"] == (
        "operator@example.com"
    )


def test_runtime_verification_rejects_disabled_rule_and_stale_checkpoint(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    with pytest.raises(ActivatorAutomationError, match="not enabled"):
        verify_activator_runtime(
            _RuntimeClient(_definition(_entities(enabled=False))),
            _manifest(),  # type: ignore[arg-type]
            bundle_path=bundle,
            now=_NOW,
        )

    client = _RuntimeClient(_definition())
    pending = verify_activator_runtime(
        client, _manifest(), bundle_path=bundle, now=_NOW  # type: ignore[arg-type]
    )
    assert pending.resource is not None
    stale = _checkpoint(pending.resource["operatorCheckpointRequired"])
    stale["expiresAt"] = (_NOW - timedelta(minutes=1)).isoformat()
    checkpoint_path = tmp_path / "stale.json"
    checkpoint_path.write_text(json.dumps(stale))
    with pytest.raises(ActivatorAutomationError, match="not currently valid"):
        verify_activator_runtime(
            client,
            _manifest(),  # type: ignore[arg-type]
            bundle_path=bundle,
            checkpoint_path=checkpoint_path,
            now=_NOW,
        )


def test_action_checkpoint_rejects_tampering_and_incomplete_evidence(
    tmp_path: Path,
) -> None:
    evidence = validate_reflex_bundle(_bundle(tmp_path))
    actions = evidence.actions
    client = _RuntimeClient(_definition())
    pending = verify_activator_runtime(
        client, _manifest(), bundle_path=tmp_path / "operations", now=_NOW  # type: ignore[arg-type]
    )
    assert pending.resource is not None
    valid = _checkpoint(pending.resource["operatorCheckpointRequired"])
    arguments = {
        "workspace_id": _WS,
        "reflex_id": _REFLEX,
        "trigger_name": "Crowding detected",
        "evidence": evidence,
        "actions": actions,
        "now": _NOW,
    }

    invalid_values: list[tuple[dict[str, Any], str]] = []
    missing_field = dict(valid)
    missing_field.pop("verifiedBy")
    invalid_values.append((missing_field, "fields"))
    wrong_reflex = dict(valid, reflexId=_TARGET)
    invalid_values.append((wrong_reflex, "does not match"))
    no_verifier = dict(valid, verifiedBy=" ")
    invalid_values.append((no_verifier, "verifiedBy"))
    naive_timestamp = dict(valid, verifiedAt="2026-10-13T00:00:00")
    invalid_values.append((naive_timestamp, "timezone"))
    future = dict(valid, verifiedAt=(_NOW + timedelta(minutes=1)).isoformat())
    invalid_values.append((future, "not currently valid"))
    no_actions = dict(valid, actions=[])
    invalid_values.append((no_actions, "cover exactly"))
    failed_action = json.loads(json.dumps(valid))
    failed_action["actions"][0]["result"] = "failed"
    invalid_values.append((failed_action, "not verified"))
    missing_reference = json.loads(json.dumps(valid))
    missing_reference["actions"][0]["evidenceReference"] = ""
    invalid_values.append((missing_reference, "no durable"))
    wrong_kind = json.loads(json.dumps(valid))
    wrong_kind["actions"][0]["kind"] = "EmailMessage"
    invalid_values.append((wrong_kind, "kind mismatch"))
    extra_action_field = json.loads(json.dumps(valid))
    extra_action_field["actions"][0]["unexpected"] = True
    invalid_values.append((extra_action_field, "action fields"))
    duplicate_action = json.loads(json.dumps(valid))
    duplicate_action["actions"].append(dict(duplicate_action["actions"][0]))
    invalid_values.append((duplicate_action, "duplicated"))

    for value, message in invalid_values:
        with pytest.raises(ActivatorAutomationError, match=message):
            validate_action_checkpoint(value, **arguments)


def test_runtime_verification_fails_closed_on_identity_readback_and_target(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    unresolved = parse_deployment_manifest(
        json.loads(Path("src/people_counter/data/deploy/dev.json").read_text()),
        DeploymentEnvironment.DEV,
    )
    with pytest.raises(ActivatorAutomationError, match="requires workspace_id"):
        verify_activator_runtime(
            _RuntimeClient(_definition()),
            unresolved,
            bundle_path=bundle,
            now=_NOW,
        )

    class BadIdentityClient(_RuntimeClient):
        def request_json(self, method: str, path: str, **kwargs: object) -> dict[str, Any]:
            value = super().request_json(method, path, **kwargs)
            if "/reflexes/" in path:
                value["type"] = "Notebook"
            return value

    with pytest.raises(ActivatorAutomationError, match="identity/type"):
        verify_activator_runtime(
            BadIdentityClient(_definition()),
            _manifest(),  # type: ignore[arg-type]
            bundle_path=bundle,
            now=_NOW,
        )

    class FailedReadbackClient(_RuntimeClient):
        def run_operation(self, method: str, path: str, **_: object) -> DeploymentEvidence:
            return DeploymentEvidence(
                action="get_activator_definition",
                status="failed",
                error="readback failed",
            )

    failure = verify_activator_runtime(
        FailedReadbackClient(_definition()),
        _manifest(),  # type: ignore[arg-type]
        bundle_path=bundle,
        now=_NOW,
    )
    assert failure.status == "failed"
    assert failure.error == "readback failed"

    class BadTargetClient(_RuntimeClient):
        def request_json(self, method: str, path: str, **kwargs: object) -> dict[str, Any]:
            value = super().request_json(method, path, **kwargs)
            if path.endswith(f"/items/{_TARGET}"):
                value["type"] = "Notebook"
            return value

    with pytest.raises(ActivatorAutomationError, match="target verification"):
        verify_activator_runtime(
            BadTargetClient(_definition()),
            _manifest(),  # type: ignore[arg-type]
            bundle_path=bundle,
            now=_NOW,
        )

    changed_source = _definition(_entities(source_type="eventstreamSource-v1"))
    with pytest.raises(ActivatorAutomationError, match="topology differs"):
        verify_activator_runtime(
            _RuntimeClient(changed_source),
            _manifest(),  # type: ignore[arg-type]
            bundle_path=bundle,
            now=_NOW,
        )


@dataclass
class _ProbeClient:
    calls: list[tuple[str, str, object | None]] = field(default_factory=list)

    def run_operation(
        self, method: str, path: str, *, value: object, **_: object
    ) -> DeploymentEvidence:
        self.calls.append((method, path, value))
        return DeploymentEvidence(
            action="create",
            status="succeeded",
            resource={
                "id": _REFLEX,
                "displayName": value["displayName"],  # type: ignore[index]
                "type": "Reflex",
            },
        )

    def request_json(
        self, method: str, path: str, *, value: object | None = None, **_: object
    ) -> dict[str, Any]:
        self.calls.append((method, path, value))
        if method == "GET":
            return {
                "id": _REFLEX,
                "displayName": "pc-activator-sp-contract-test",
                "type": "Reflex",
            }
        if method == "PATCH":
            return {"id": _REFLEX, "displayName": value["displayName"]}  # type: ignore[index]
        return {}

    def list_pages(self, path: str) -> list[dict[str, str]]:
        self.calls.append(("LIST", path, None))
        return [{"id": _REFLEX}]


def test_service_principal_crud_probe_uses_documented_paths_and_hard_deletes() -> None:
    client = _ProbeClient()

    evidence = probe_service_principal_crud_contract(
        client, workspace_id=_WS, probe_id="test"
    )

    assert evidence.status == "succeeded"
    assert [call[0] for call in client.calls] == [
        "POST",
        "GET",
        "PATCH",
        "LIST",
        "DELETE",
    ]
    assert client.calls[-1][1] == (
        f"/workspaces/{_WS}/reflexes/{_REFLEX}?hardDelete=true"
    )
