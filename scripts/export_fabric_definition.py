from __future__ import annotations

import argparse
import json
import os
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from urllib.parse import urlencode

from people_counter.fabric_automation import FabricAutomationClient
from people_counter.fabric_definition_bundle import (
    DefinitionBundleError,
    TokenReplacement,
    load_migration_manifest,
    write_definition_bundle,
)
from people_counter.fabric_rest import AzureCliTokenProvider, UrllibTransport


def _uuid(value: str) -> str:
    try:
        return str(uuid.UUID(value))
    except (ValueError, AttributeError) as error:
        raise argparse.ArgumentTypeError("ID must be a UUID") from error


def _resolved_uuid(value: str, context: str) -> str:
    try:
        return str(uuid.UUID(value))
    except (ValueError, AttributeError) as error:
        raise DefinitionBundleError(f"{context} must resolve to a UUID") from error


def _token_environment(value: str) -> tuple[str, str]:
    token_name, separator, environment_name = value.partition("=")
    if not separator or not token_name or not environment_name:
        raise argparse.ArgumentTypeError("token must use TokenName=ENVIRONMENT_VARIABLE")
    return token_name, environment_name


def export_definition(
    client: FabricAutomationClient,
    *,
    workspace_id: str,
    item_id: str,
    destination: Path,
    format_name: str | None,
    replacements: Mapping[str, str] | Sequence[TokenReplacement],
    item_type: str | None = None,
) -> dict[str, object]:
    query = (
        f"?{urlencode({'format': format_name})}"
        if format_name and format_name != "Default"
        else ""
    )
    evidence = client.run_operation(
        "POST",
        f"/workspaces/{workspace_id}/items/{item_id}/getDefinition{query}",
        action=f"export_definition:{item_id}",
        accepted=(200, 202),
    )
    if evidence.status != "succeeded" or evidence.resource is None:
        raise DefinitionBundleError(
            evidence.error or f"getDefinition failed for item {item_id}"
        )
    metadata = write_definition_bundle(
        evidence.resource,
        destination,
        replacements=replacements,
        item_type=item_type,
        expected_format=format_name,
    )
    return {
        "contentSha256": metadata["contentSha256"],
        "destination": str(destination),
        "format": metadata["format"],
        "itemId": item_id,
        "itemType": item_type,
        "operationId": evidence.operation_id,
        "parts": len(metadata["parts"]),
        "workspaceId": workspace_id,
    }


def export_manifest(
    client: FabricAutomationClient,
    manifest_path: Path,
    environment: Mapping[str, str],
) -> dict[str, object]:
    manifest = load_migration_manifest(manifest_path, environment)
    workspace_id = _resolved_uuid(manifest.workspace_id, "workspace ID")
    exports = []
    for item in manifest.items:
        exports.append(
            export_definition(
                client,
                workspace_id=workspace_id,
                item_id=_resolved_uuid(item.item_id, "item ID"),
                destination=item.destination,
                format_name=item.format_name,
                replacements=item.replacements,
                item_type=item.item_type,
            )
        )
    exports.sort(key=lambda item: str(item["destination"]))
    return {
        "count": len(exports),
        "exports": exports,
        "manifest": str(manifest_path),
        "schema": "people-counter-fabric-definition-export-evidence-v1",
        "workspaceId": workspace_id,
    }


def _single_replacements(
    values: Sequence[tuple[str, str]],
    environment: Mapping[str, str],
) -> tuple[TokenReplacement, ...]:
    replacements = []
    names: set[str] = set()
    for name, environment_name in values:
        if name in names:
            raise DefinitionBundleError(f"duplicate token name {name!r}")
        source = environment.get(environment_name)
        if not source:
            raise DefinitionBundleError(
                f"required environment variable {environment_name!r} is not set"
            )
        names.add(name)
        replacements.append(TokenReplacement(value=source, name=name))
    return tuple(replacements)


def _write_evidence(result: Mapping[str, object], path: Path | None) -> None:
    output = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if path is None:
        print(output, end="")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(output, encoding="utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export Fabric getDefinition responses into deterministic source bundles."
        )
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--manifest",
        type=Path,
        help="batch migration manifest; destinations are relative to this file",
    )
    mode.add_argument("--item-id", type=_uuid, help="single Fabric item UUID")
    parser.add_argument("--workspace-id", type=_uuid)
    parser.add_argument("--destination", type=Path)
    parser.add_argument("--item-type")
    parser.add_argument("--format")
    parser.add_argument(
        "--token-env",
        action="append",
        default=[],
        type=_token_environment,
        metavar="TokenName=ENVIRONMENT_VARIABLE",
        help="replace an environment-sourced exact value without exposing it in argv",
    )
    parser.add_argument("--evidence", type=Path)
    parser.add_argument(
        "--api-root",
        default="https://api.fabric.microsoft.com/v1",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    arguments = parser.parse_args(argv)
    if arguments.manifest is not None:
        forbidden = [
            name
            for name in ("workspace_id", "destination", "item_type", "format")
            if getattr(arguments, name) is not None
        ]
        if arguments.token_env:
            forbidden.append("token_env")
        if forbidden:
            parser.error(
                "--manifest cannot be combined with single-item arguments: "
                + ", ".join(f"--{name.replace('_', '-')}" for name in forbidden)
            )
    elif arguments.workspace_id is None or arguments.destination is None:
        parser.error("--workspace-id and --destination are required with --item-id")

    client = FabricAutomationClient(
        AzureCliTokenProvider(),
        transport=UrllibTransport(),
        api_root=arguments.api_root,
    )
    try:
        if arguments.manifest is not None:
            result = export_manifest(client, arguments.manifest, os.environ)
        else:
            result = export_definition(
                client,
                workspace_id=arguments.workspace_id,
                item_id=arguments.item_id,
                destination=arguments.destination,
                format_name=arguments.format,
                replacements=_single_replacements(arguments.token_env, os.environ),
                item_type=arguments.item_type,
            )
    except DefinitionBundleError as error:
        parser.error(str(error))
    _write_evidence(result, arguments.evidence)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
