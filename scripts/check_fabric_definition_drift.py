from __future__ import annotations

import argparse
import json
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any, cast

from people_counter.fabric_automation import FabricAutomationClient, HTTPTransport
from people_counter.fabric_definition_bundle import (
    DefinitionBundleError,
    TokenReplacement,
    definition_bundle_metadata,
    read_definition_bundle,
)
from people_counter.fabric_rest import AzureCliTokenProvider, UrllibTransport


DRIFT_SCHEMA = "people-counter-fabric-definition-drift-v1"
RESULT_SCHEMA = "people-counter-fabric-definition-drift-evidence-v1"


def _object(value: object, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DefinitionBundleError(f"{context} must be an object")
    return value


def _keys(value: Mapping[str, Any], allowed: set[str], context: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise DefinitionBundleError(
            f"{context} has unknown fields: {', '.join(unknown)}"
        )


def _path(value: object, context: str) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise DefinitionBundleError(f"{context} must be a relative POSIX path")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise DefinitionBundleError(f"{context} must be a safe relative POSIX path")
    return path


def _output_path(value: object, context: str) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(part, str) or not part for part in value)
    ):
        raise DefinitionBundleError(f"{context} must be a non-empty string list")
    return tuple(value)


def _output_value(
    outputs: Mapping[str, Any], path: Sequence[str], context: str
) -> str:
    first, *remaining = path
    root = _object(outputs.get(first), f"{context} Terraform output")
    value: object = root.get("value")
    for part in remaining:
        current = _object(value, f"{context} Terraform output value")
        value = current.get(part)
    if not isinstance(value, str) or not value:
        raise DefinitionBundleError(f"{context} Terraform output is not a string")
    return value


def _metadata(path: Path) -> Mapping[str, Any]:
    read_definition_bundle(path)
    try:
        metadata = json.loads((path / ".fabric-item.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DefinitionBundleError(f"invalid bundle metadata at {path}") from error
    return _object(metadata, f"bundle metadata at {path}")


def _uuid(value: str, context: str) -> str:
    try:
        return str(uuid.UUID(value))
    except (ValueError, AttributeError) as error:
        raise DefinitionBundleError(f"{context} must be a UUID") from error


def detect_definition_drift(
    client: FabricAutomationClient,
    *,
    manifest_path: Path,
    terraform_outputs_path: Path,
    source_root: Path,
) -> dict[str, object]:
    try:
        raw_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        raw_outputs = json.loads(terraform_outputs_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DefinitionBundleError("invalid drift manifest or Terraform outputs") from error
    manifest = _object(raw_manifest, "definition drift manifest")
    outputs = _object(raw_outputs, "Terraform outputs")
    _keys(
        manifest,
        {"items", "schema", "workspaceIdOutput"},
        "definition drift manifest",
    )
    if manifest.get("schema") != DRIFT_SCHEMA:
        raise DefinitionBundleError("definition drift manifest schema is unsupported")
    workspace_id = _uuid(
        _output_value(
            outputs,
            _output_path(manifest.get("workspaceIdOutput"), "workspaceIdOutput"),
            "workspace ID",
        ),
        "workspace ID",
    )
    raw_items = manifest.get("items")
    if not isinstance(raw_items, list) or not raw_items:
        raise DefinitionBundleError("definition drift manifest must contain items")

    results: list[dict[str, object]] = []
    destinations: set[PurePosixPath] = set()
    for index, raw_item in enumerate(raw_items):
        context = f"definition drift item {index}"
        item = _object(raw_item, context)
        _keys(
            item,
            {"destination", "format", "itemIdOutput", "itemType", "optional", "tokens"},
            context,
        )
        destination = _path(item.get("destination"), f"{context} destination")
        if destination in destinations:
            raise DefinitionBundleError(
                f"duplicate definition drift destination {destination.as_posix()!r}"
            )
        destinations.add(destination)
        bundle_path = source_root.joinpath(*destination.parts)
        optional = item.get("optional", False)
        if not isinstance(optional, bool):
            raise DefinitionBundleError(f"{context} optional must be a boolean")
        if not (bundle_path / ".fabric-item.json").is_file():
            if optional:
                continue
            raise DefinitionBundleError(
                f"required definition bundle is missing: {destination.as_posix()}"
            )

        item_type = item.get("itemType")
        if not isinstance(item_type, str) or not item_type:
            raise DefinitionBundleError(f"{context} itemType is invalid")
        format_name = item.get("format")
        if format_name is not None and (
            not isinstance(format_name, str) or not format_name
        ):
            raise DefinitionBundleError(f"{context} format is invalid")
        item_id = _uuid(
            _output_value(
                outputs,
                _output_path(item.get("itemIdOutput"), f"{context} itemIdOutput"),
                f"{context} item ID",
            ),
            f"{context} item ID",
        )
        raw_tokens = item.get("tokens", [])
        if not isinstance(raw_tokens, list):
            raise DefinitionBundleError(f"{context} tokens must be a list")
        replacements = []
        for token_index, raw_token in enumerate(raw_tokens):
            token_context = f"{context} token {token_index}"
            token = _object(raw_token, token_context)
            _keys(token, {"name", "output"}, token_context)
            name = token.get("name")
            if not isinstance(name, str):
                raise DefinitionBundleError(f"{token_context} name is invalid")
            replacements.append(
                TokenReplacement(
                    value=_output_value(
                        outputs,
                        _output_path(token.get("output"), f"{token_context} output"),
                        token_context,
                    ),
                    name=name,
                )
            )

        query = f"?format={format_name}" if format_name else ""
        operation = client.run_operation(
            "POST",
            f"/workspaces/{workspace_id}/items/{item_id}/getDefinition{query}",
            action=f"definition_drift:{destination.as_posix()}",
            accepted=(200, 202),
        )
        if operation.status != "succeeded" or operation.resource is None:
            raise DefinitionBundleError(
                operation.error or f"getDefinition failed for {destination.as_posix()}"
            )
        expected = _metadata(bundle_path)
        actual = definition_bundle_metadata(
            operation.resource,
            replacements=tuple(replacements),
            item_type=item_type,
            expected_format=format_name,
        )
        expected_hash = expected.get("contentSha256")
        actual_hash = actual.get("contentSha256")
        status = "clean" if actual_hash == expected_hash else "drift"
        results.append(
            {
                "actualContentSha256": actual_hash,
                "expectedContentSha256": expected_hash,
                "itemType": item_type,
                "path": destination.as_posix(),
                "status": status,
            }
        )

    results.sort(key=lambda result: str(result["path"]))
    drift_count = sum(result["status"] == "drift" for result in results)
    return {
        "count": len(results),
        "driftCount": drift_count,
        "items": results,
        "schema": RESULT_SCHEMA,
        "status": "drift" if drift_count else "clean",
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare live Fabric definition hashes without writing definitions."
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--terraform-outputs", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, default=Path("fabric"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--api-root",
        default="https://api.fabric.microsoft.com/v1",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    client = FabricAutomationClient(
        AzureCliTokenProvider(),
        transport=cast(HTTPTransport, UrllibTransport()),
        api_root=arguments.api_root,
    )
    try:
        result = detect_definition_drift(
            client,
            manifest_path=arguments.manifest,
            terraform_outputs_path=arguments.terraform_outputs,
            source_root=arguments.source_root,
        )
    except DefinitionBundleError as error:
        raise SystemExit(f"definition drift check failed: {error}") from error
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 2 if result["status"] == "drift" else 0


if __name__ == "__main__":
    raise SystemExit(main())
