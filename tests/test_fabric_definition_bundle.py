from __future__ import annotations

import base64
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from people_counter.fabric_definition_bundle import (
    BUNDLE_SCHEMA,
    DEFINITION_EXPORT_SCHEMA,
    DefinitionBundleError,
    TokenReplacement,
    load_definition_export_manifest,
    read_definition_bundle,
    write_definition_bundle,
)
from scripts.export_fabric_definition import export_definition, export_manifest
from scripts.validate_fabric_definitions import validate_tree


def _response(parts: dict[str, bytes], format_name: str = "TMDL") -> dict[str, object]:
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


def test_bundle_round_trips_and_tokenizes_deterministically(tmp_path: Path) -> None:
    source = _response(
        {
            "definition/model.tmdl": b"workspace=old-workspace",
            "definition.pbism": b'{"version":"4.0"}',
        }
    )

    metadata = write_definition_bundle(
        source,
        tmp_path,
        replacements={"old-workspace": "TargetWorkspaceId"},
        item_type="SemanticModel",
        expected_format="TMDL",
    )

    assert metadata["schema"] == BUNDLE_SCHEMA
    assert metadata["itemType"] == "SemanticModel"
    assert metadata["tokens"] == [
        {
            "name": "TargetWorkspaceId",
            "occurrences": 1,
            "parts": ["definition/model.tmdl"],
        }
    ]
    assert (tmp_path / "definition/model.tmdl").read_bytes() == (
        b"workspace={{ .TargetWorkspaceId }}"
    )
    rebuilt = read_definition_bundle(tmp_path)
    assert rebuilt["definition"]["format"] == "TMDL"
    assert [part["path"] for part in rebuilt["definition"]["parts"]] == [
        "definition.pbism",
        "definition/model.tmdl",
    ]


def test_token_replacement_is_simultaneous_not_recursive(tmp_path: Path) -> None:
    write_definition_bundle(
        _response(
            {
                "definition.json": (
                    b'{"first":"source-a","again":"source-a","second":"source-b"}'
                )
            }
        ),
        tmp_path,
        replacements=(
            TokenReplacement("source-a", "First"),
            TokenReplacement("source-b", "Second"),
        ),
    )

    assert (tmp_path / "definition.json").read_text() == (
        '{"first":"{{ .First }}","again":"{{ .First }}",'
        '"second":"{{ .Second }}"}'
    )
    metadata = json.loads((tmp_path / ".fabric-item.json").read_text())
    assert metadata["tokens"][0]["occurrences"] == 2


def test_token_replacement_can_be_scoped_to_parts(tmp_path: Path) -> None:
    write_definition_bundle(
        _response({"one.json": b"source", "two.json": b"source"}),
        tmp_path,
        replacements=(TokenReplacement("source", "Target", ("one.json",)),),
    )

    assert (tmp_path / "one.json").read_text() == "{{ .Target }}"
    assert (tmp_path / "two.json").read_text() == "source"


@pytest.mark.parametrize(
    ("replacements", "message"),
    [
        (
            (TokenReplacement("same", "One"), TokenReplacement("same", "Two")),
            "duplicate token source",
        ),
        (
            (TokenReplacement("short", "One"), TokenReplacement("shorter", "Two")),
            "overlapping token source",
        ),
        (
            (TokenReplacement("one", "Same"), TokenReplacement("two", "Same")),
            "duplicate token name",
        ),
    ],
)
def test_bundle_rejects_ambiguous_token_replacements(
    tmp_path: Path,
    replacements: tuple[TokenReplacement, ...],
    message: str,
) -> None:
    with pytest.raises(DefinitionBundleError, match=message):
        write_definition_bundle(
            _response({"definition.json": b"same short shorter one two"}),
            tmp_path,
            replacements=replacements,
        )


def test_bundle_rejects_token_match_in_binary_part(tmp_path: Path) -> None:
    with pytest.raises(DefinitionBundleError, match="non-UTF-8"):
        write_definition_bundle(
            _response({"image.bin": b"\xffsource"}),
            tmp_path,
            replacements={"source": "Target"},
        )


def test_bundle_rejects_preexisting_target_placeholder(tmp_path: Path) -> None:
    with pytest.raises(DefinitionBundleError, match="already exists"):
        write_definition_bundle(
            _response({"definition.json": b"source {{ .Target }}"}),
            tmp_path,
            replacements={"source": "Target"},
        )


def test_bundle_update_removes_managed_stale_parts(tmp_path: Path) -> None:
    write_definition_bundle(
        _response({"definition/old.tmdl": b"old", "definition/keep.tmdl": b"keep"}),
        tmp_path,
    )

    write_definition_bundle(
        _response({"definition/keep.tmdl": b"updated"}),
        tmp_path,
    )

    assert not (tmp_path / "definition/old.tmdl").exists()
    assert (tmp_path / "definition/keep.tmdl").read_text() == "updated"


def test_bundle_refuses_to_overwrite_unlisted_file(tmp_path: Path) -> None:
    write_definition_bundle(_response({"definition.json": b"old"}), tmp_path)
    (tmp_path / "notes.txt").write_text("unmanaged")

    with pytest.raises(DefinitionBundleError, match="unlisted files"):
        write_definition_bundle(_response({"definition.json": b"new"}), tmp_path)

    assert (tmp_path / "definition.json").read_text() == "old"


@pytest.mark.parametrize(
    "path",
    [
        "../secret",
        "/absolute",
        r"definition\\part.tmdl",
        ".fabric-item.json",
        "definition/.fabric-item.json",
        "definition//part.tmdl",
        "./definition.tmdl",
    ],
)
def test_bundle_rejects_unsafe_paths(tmp_path: Path, path: str) -> None:
    with pytest.raises(DefinitionBundleError, match="path"):
        write_definition_bundle(_response({path: b"value"}), tmp_path)


def test_bundle_rejects_file_directory_path_conflict(tmp_path: Path) -> None:
    with pytest.raises(DefinitionBundleError, match="conflicts"):
        write_definition_bundle(
            _response({"definition": b"file", "definition/model.tmdl": b"nested"}),
            tmp_path,
        )


def test_bundle_rejects_missing_token_source_or_scope(tmp_path: Path) -> None:
    with pytest.raises(DefinitionBundleError, match="was not found"):
        write_definition_bundle(
            _response({"definition/model.tmdl": b"value"}),
            tmp_path,
            replacements={"missing": "TargetId"},
        )
    with pytest.raises(DefinitionBundleError, match="missing parts"):
        write_definition_bundle(
            _response({"definition/model.tmdl": b"value"}),
            tmp_path,
            replacements=(
                TokenReplacement("value", "TargetId", ("other.tmdl",)),
            ),
        )


def test_bundle_rejects_unexpected_format(tmp_path: Path) -> None:
    with pytest.raises(DefinitionBundleError, match="expected 'PBIR'"):
        write_definition_bundle(
            _response({"definition.json": b"value"}, "PBIR-Legacy"),
            tmp_path,
            expected_format="PBIR",
        )


def test_bundle_uses_requested_format_when_fabric_omits_it(
    tmp_path: Path,
) -> None:
    response = _response({"definition/model.tmdl": b"table T"})
    del response["definition"]["format"]

    metadata = write_definition_bundle(
        response,
        tmp_path,
        expected_format="TMDL",
    )

    assert metadata["format"] == "TMDL"


def test_bundle_detects_modified_missing_and_unlisted_parts(tmp_path: Path) -> None:
    write_definition_bundle(
        _response({"definition/model.tmdl": b"value"}),
        tmp_path,
    )
    (tmp_path / "definition/model.tmdl").write_text("changed")
    with pytest.raises(DefinitionBundleError, match="hash mismatch"):
        read_definition_bundle(tmp_path)

    (tmp_path / "definition/model.tmdl").write_text("value")
    (tmp_path / "extra.txt").write_text("extra")
    with pytest.raises(DefinitionBundleError, match="unlisted files"):
        read_definition_bundle(tmp_path)

    (tmp_path / "extra.txt").unlink()
    (tmp_path / "definition/model.tmdl").unlink()
    with pytest.raises(DefinitionBundleError, match="missing"):
        read_definition_bundle(tmp_path)


def test_bundle_metadata_is_canonical_and_content_addressed(tmp_path: Path) -> None:
    metadata = write_definition_bundle(
        _response({"definition/model.tmdl": b"value"}),
        tmp_path,
    )
    raw = (tmp_path / ".fabric-item.json").read_bytes()

    assert raw.endswith(b"\n")
    assert raw == (
        json.dumps(metadata, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )
    assert len(metadata["contentSha256"]) == 64
    assert metadata["parts"] == [
        {
            "path": "definition/model.tmdl",
            "sha256": (
                "cd42404d52ad55ccfa9aca4adc828aa5800ad9d385a0671fbcbf724118320619"
            ),
            "size": 5,
        }
    ]


def test_reexport_of_identical_definition_is_byte_identical(tmp_path: Path) -> None:
    response = _response(
        {"z.json": b"source", "a.json": b"value"},
        format_name="CustomDefinition",
    )
    first = write_definition_bundle(
        response,
        tmp_path,
        replacements={"source": "Target"},
        item_type="CustomItem",
    )
    first_bytes = {
        path.relative_to(tmp_path).as_posix(): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    second = write_definition_bundle(
        response,
        tmp_path,
        replacements={"source": "Target"},
        item_type="CustomItem",
    )
    second_bytes = {
        path.relative_to(tmp_path).as_posix(): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    assert second == first
    assert second_bytes == first_bytes


def test_bundle_rejects_noncanonical_or_invalid_token_metadata(
    tmp_path: Path,
) -> None:
    write_definition_bundle(
        _response({"definition.json": b"source"}),
        tmp_path,
        replacements={"source": "Target"},
    )
    metadata_path = tmp_path / ".fabric-item.json"
    metadata = json.loads(metadata_path.read_text())
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
    with pytest.raises(DefinitionBundleError, match="not canonical"):
        read_definition_bundle(tmp_path)

    metadata["tokens"][0]["occurrences"] = 3
    metadata_path.write_text(
        json.dumps(metadata, separators=(",", ":"), sort_keys=True) + "\n"
    )
    with pytest.raises(DefinitionBundleError, match="token evidence"):
        read_definition_bundle(tmp_path)


def test_validate_tree_reports_schema_and_hash_evidence(tmp_path: Path) -> None:
    write_definition_bundle(
        _response({"definition/model.tmdl": b"value"}),
        tmp_path / "semantic-models" / "analytics",
        item_type="SemanticModel",
    )

    result = validate_tree(tmp_path)

    assert result["count"] == 1
    assert result["sourceBundleSchema"] == BUNDLE_SCHEMA
    assert len(result["treeSha256"]) == 64
    assert result["bundles"][0]["path"] == "semantic-models/analytics"
    assert result["bundles"][0]["itemType"] == "SemanticModel"
    assert len(result["bundles"][0]["bundleSha256"]) == 64
    assert len(result["bundles"][0]["contentSha256"]) == 64


def _definition_export_manifest() -> dict[str, object]:
    return {
        "schema": DEFINITION_EXPORT_SCHEMA,
        "workspaceIdEnv": "SOURCE_WORKSPACE_ID",
        "items": [
            {
                "destination": "semantic-models/analytics",
                "format": "TMDL",
                "itemIdEnv": "SOURCE_MODEL_ID",
                "itemType": "SemanticModel",
                "tokens": [
                    {
                        "name": "TargetWorkspaceId",
                        "valueEnv": "SOURCE_WORKSPACE_ID",
                    }
                ],
            }
        ],
    }


def test_load_definition_export_manifest_resolves_environment_values(
    tmp_path: Path,
) -> None:
    path = tmp_path / "definition-export.json"
    path.write_text(json.dumps(_definition_export_manifest()))

    manifest = load_definition_export_manifest(
        path,
        {
            "SOURCE_WORKSPACE_ID": "11111111-1111-1111-1111-111111111111",
            "SOURCE_MODEL_ID": "22222222-2222-2222-2222-222222222222",
        },
    )

    assert manifest.workspace_id == "11111111-1111-1111-1111-111111111111"
    assert manifest.items[0].destination == tmp_path / "semantic-models/analytics"
    assert manifest.items[0].replacements == (
        TokenReplacement(
            "11111111-1111-1111-1111-111111111111",
            "TargetWorkspaceId",
        ),
    )


def test_repository_definition_export_manifest_covers_item_types() -> None:
    path = Path("fabric/definition-export-manifest.example.json")
    raw = json.loads(path.read_text())
    environment_names = {raw["workspaceIdEnv"]}
    for item in raw["items"]:
        environment_names.add(item["itemIdEnv"])
        environment_names.update(token["valueEnv"] for token in item["tokens"])
    environment = {
        name: f"value-for-{name.lower()}" for name in environment_names
    }

    manifest = load_definition_export_manifest(path, environment)

    assert {item.item_type for item in manifest.items} == {
        "Eventhouse",
        "KQLDashboard",
        "KQLDatabase",
        "KQLQueryset",
        "Reflex",
        "Report",
        "SemanticModel",
        "VariableLibrary",
    }
    assert len(manifest.items) == 10


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.update({"typo": True}), "unknown fields"),
        (
            lambda value: value["items"][0].update(
                {"destination": "../outside"}
            ),
            "unsafe",
        ),
        (
            lambda value: value["items"].append(dict(value["items"][0])),
            "duplicate export destination",
        ),
    ],
)
def test_load_definition_export_manifest_rejects_unsafe_entries(
    tmp_path: Path,
    mutate: object,
    message: str,
) -> None:
    value = _definition_export_manifest()
    mutate(value)  # type: ignore[operator]
    path = tmp_path / "definition-export.json"
    path.write_text(json.dumps(value))

    with pytest.raises(DefinitionBundleError, match=message):
        load_definition_export_manifest(
            path,
            {
                "SOURCE_WORKSPACE_ID": "11111111-1111-1111-1111-111111111111",
                "SOURCE_MODEL_ID": "22222222-2222-2222-2222-222222222222",
            },
        )


def test_export_manifest_batches_items_and_writes_bundle(tmp_path: Path) -> None:
    path = tmp_path / "definition-export.json"
    path.write_text(json.dumps(_definition_export_manifest()))
    environment = {
        "SOURCE_WORKSPACE_ID": "11111111-1111-1111-1111-111111111111",
        "SOURCE_MODEL_ID": "22222222-2222-2222-2222-222222222222",
    }

    class Client:
        requests: list[str] = []

        def run_operation(self, method: str, request_path: str, **_: object) -> object:
            assert method == "POST"
            self.requests.append(request_path)
            return SimpleNamespace(
                error=None,
                operation_id="operation-id",
                resource=_response(
                    {
                        "definition/model.tmdl": (
                            b"workspace=11111111-1111-1111-1111-111111111111"
                        )
                    }
                ),
                status="succeeded",
            )

    client = Client()
    evidence = export_manifest(client, path, environment)  # type: ignore[arg-type]

    assert evidence["count"] == 1
    assert client.requests == [
        (
            "/workspaces/11111111-1111-1111-1111-111111111111/items/"
            "22222222-2222-2222-2222-222222222222/getDefinition?format=TMDL"
        )
    ]
    assert (
        tmp_path / "semantic-models/analytics/definition/model.tmdl"
    ).read_text() == "workspace={{ .TargetWorkspaceId }}"


def test_export_default_format_is_local_fallback_not_api_query(
    tmp_path: Path,
) -> None:
    class Client:
        request_path = ""

        def run_operation(self, method: str, request_path: str, **_: object) -> object:
            assert method == "POST"
            self.request_path = request_path
            response = _response({"RealTimeQueryset.json": b"{}"}, "Default")
            del response["definition"]["format"]
            return SimpleNamespace(
                error=None,
                operation_id="operation-id",
                resource=response,
                status="succeeded",
            )

    client = Client()
    result = export_definition(
        client,  # type: ignore[arg-type]
        workspace_id="11111111-1111-1111-1111-111111111111",
        item_id="22222222-2222-2222-2222-222222222222",
        destination=tmp_path / "queryset",
        format_name="Default",
        replacements={},
        item_type="KQLQueryset",
    )

    assert "?" not in client.request_path
    assert result["format"] == "Default"
