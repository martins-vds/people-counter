from __future__ import annotations

import base64
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from people_counter.fabric_definition_bundle import (
    DefinitionBundleError,
    TokenReplacement,
    definition_bundle_metadata,
    write_definition_bundle,
)
from scripts.check_fabric_definition_drift import detect_definition_drift


WORKSPACE_ID = "11111111-1111-1111-1111-111111111111"
ITEM_ID = "22222222-2222-2222-2222-222222222222"


def _response(content: bytes) -> dict[str, object]:
    return {
        "definition": {
            "format": "TMDL",
            "parts": [
                {
                    "path": "definition/model.tmdl",
                    "payload": base64.b64encode(content).decode(),
                    "payloadType": "InlineBase64",
                }
            ],
        }
    }


def _manifest(optional: bool = False) -> dict[str, object]:
    return {
        "schema": "people-counter-fabric-definition-drift-v1",
        "workspaceIdOutput": ["workspace_id"],
        "items": [
            {
                "destination": "semantic-models/analytics",
                "format": "TMDL",
                "itemIdOutput": ["semantic_model_ids", "analytics"],
                "itemType": "SemanticModel",
                "optional": optional,
                "tokens": [
                    {
                        "name": "TargetWorkspaceId",
                        "output": ["workspace_id"],
                    }
                ],
            }
        ],
    }


def _outputs() -> dict[str, object]:
    return {
        "workspace_id": {"value": WORKSPACE_ID},
        "semantic_model_ids": {"value": {"analytics": ITEM_ID}},
    }


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


class _Client:
    def __init__(self, response: dict[str, object]) -> None:
        self.response = response
        self.requests: list[str] = []

    def run_operation(self, method: str, path: str, **_: object) -> object:
        assert method == "POST"
        self.requests.append(path)
        return SimpleNamespace(
            error=None,
            operation_id="operation-secret",
            resource=self.response,
            status="succeeded",
        )


def test_definition_metadata_hashes_sanitized_content_without_writing(
    tmp_path: Path,
) -> None:
    metadata = definition_bundle_metadata(
        _response(f"workspace={WORKSPACE_ID}".encode()),
        replacements=(TokenReplacement(WORKSPACE_ID, "TargetWorkspaceId"),),
        item_type="SemanticModel",
        expected_format="TMDL",
    )

    assert len(metadata["contentSha256"]) == 64
    assert metadata["tokens"] == [
        {
            "name": "TargetWorkspaceId",
            "occurrences": 1,
            "parts": ["definition/model.tmdl"],
        }
    ]
    assert list(tmp_path.iterdir()) == []


def test_definition_metadata_enforces_expected_format() -> None:
    with pytest.raises(DefinitionBundleError, match="expected 'PBIR'"):
        definition_bundle_metadata(
            _response(b"model"),
            expected_format="PBIR",
        )


def test_definition_drift_compares_only_hash_evidence(tmp_path: Path) -> None:
    source_root = tmp_path / "fabric"
    bundle = source_root / "semantic-models/analytics"
    response = _response(f"workspace={WORKSPACE_ID}".encode())
    write_definition_bundle(
        response,
        bundle,
        replacements=(TokenReplacement(WORKSPACE_ID, "TargetWorkspaceId"),),
        item_type="SemanticModel",
        expected_format="TMDL",
    )
    manifest = tmp_path / "drift.json"
    outputs = tmp_path / "outputs.json"
    _write_json(manifest, _manifest())
    _write_json(outputs, _outputs())
    client = _Client(response)

    result = detect_definition_drift(
        client,  # type: ignore[arg-type]
        manifest_path=manifest,
        terraform_outputs_path=outputs,
        source_root=source_root,
    )

    assert result["status"] == "clean"
    assert result["driftCount"] == 0
    assert client.requests == [
        (
            f"/workspaces/{WORKSPACE_ID}/items/{ITEM_ID}/"
            "getDefinition?format=TMDL"
        )
    ]
    serialized = json.dumps(result)
    assert WORKSPACE_ID not in serialized
    assert ITEM_ID not in serialized
    assert "operation-secret" not in serialized


def test_definition_drift_reports_changed_hash(tmp_path: Path) -> None:
    source_root = tmp_path / "fabric"
    write_definition_bundle(
        _response(f"workspace={WORKSPACE_ID}".encode()),
        source_root / "semantic-models/analytics",
        replacements=(TokenReplacement(WORKSPACE_ID, "TargetWorkspaceId"),),
        item_type="SemanticModel",
    )
    manifest = tmp_path / "drift.json"
    outputs = tmp_path / "outputs.json"
    _write_json(manifest, _manifest())
    _write_json(outputs, _outputs())

    result = detect_definition_drift(
        _Client(_response(f"workspace={WORKSPACE_ID}\nchanged=true".encode())),  # type: ignore[arg-type]
        manifest_path=manifest,
        terraform_outputs_path=outputs,
        source_root=source_root,
    )

    assert result["status"] == "drift"
    assert result["driftCount"] == 1
    assert result["items"][0]["status"] == "drift"  # type: ignore[index]
    assert (
        result["items"][0]["actualContentSha256"]  # type: ignore[index]
        != result["items"][0]["expectedContentSha256"]  # type: ignore[index]
    )


def test_optional_uncommitted_bundle_is_not_requested(tmp_path: Path) -> None:
    manifest = tmp_path / "drift.json"
    outputs = tmp_path / "outputs.json"
    _write_json(manifest, _manifest(optional=True))
    _write_json(outputs, _outputs())
    client = _Client(_response(b"unused"))

    result = detect_definition_drift(
        client,  # type: ignore[arg-type]
        manifest_path=manifest,
        terraform_outputs_path=outputs,
        source_root=tmp_path / "fabric",
    )

    assert result == {
        "count": 0,
        "driftCount": 0,
        "items": [],
        "schema": "people-counter-fabric-definition-drift-evidence-v1",
        "status": "clean",
    }
    assert client.requests == []


def test_required_bundle_and_uuid_outputs_are_validated(tmp_path: Path) -> None:
    manifest = tmp_path / "drift.json"
    outputs = tmp_path / "outputs.json"
    _write_json(manifest, _manifest())
    _write_json(outputs, _outputs())

    with pytest.raises(DefinitionBundleError, match="required definition bundle"):
        detect_definition_drift(
            _Client(_response(b"unused")),  # type: ignore[arg-type]
            manifest_path=manifest,
            terraform_outputs_path=outputs,
            source_root=tmp_path / "fabric",
        )

    source_root = tmp_path / "fabric"
    write_definition_bundle(
        _response(f"workspace={WORKSPACE_ID}".encode()),
        source_root / "semantic-models/analytics",
        replacements=(TokenReplacement(WORKSPACE_ID, "TargetWorkspaceId"),),
        item_type="SemanticModel",
    )
    invalid_outputs = _outputs()
    invalid_outputs["semantic_model_ids"] = {"value": {"analytics": "not-a-uuid"}}
    _write_json(outputs, invalid_outputs)
    with pytest.raises(DefinitionBundleError, match="item ID must be a UUID"):
        detect_definition_drift(
            _Client(_response(b"unused")),  # type: ignore[arg-type]
            manifest_path=manifest,
            terraform_outputs_path=outputs,
            source_root=source_root,
        )
