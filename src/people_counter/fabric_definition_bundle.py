"""Deterministic source-controlled Fabric item definition bundles."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any


BUNDLE_SCHEMA = "people-counter-fabric-definition-bundle-v2"
DEFINITION_EXPORT_SCHEMA = "people-counter-fabric-definition-export-v1"
METADATA_FILE = ".fabric-item.json"
_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_TOKEN_NAME = re.compile(r"[A-Za-z][A-Za-z0-9_]*\Z")


class DefinitionBundleError(ValueError):
    """A Fabric definition cannot be represented as a safe bundle."""


@dataclass(frozen=True)
class TokenReplacement:
    """An exact source value replaced by one Fabric provider template token."""

    value: str
    name: str
    parts: tuple[str, ...] = ()


@dataclass(frozen=True)
class DefinitionExportItem:
    """One live Fabric item from a definition export manifest."""

    item_id: str
    item_type: str
    destination: Path
    format_name: str | None
    replacements: tuple[TokenReplacement, ...]


@dataclass(frozen=True)
class DefinitionExportManifest:
    """Resolved environment-backed inputs for a batch definition export."""

    workspace_id: str
    items: tuple[DefinitionExportItem, ...]


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _part_path(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\\" in value
        or "\x00" in value
    ):
        raise DefinitionBundleError("definition part path must be a POSIX path")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or METADATA_FILE in path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise DefinitionBundleError(f"unsafe definition part path {value!r}")
    return value


def _relative_destination(value: object) -> PurePosixPath:
    path_value = _part_path(value)
    path = PurePosixPath(path_value)
    if path.name == METADATA_FILE:
        raise DefinitionBundleError("export destination must be a directory")
    return path


def _definition(
    value: Mapping[str, Any],
    expected_format: str | None = None,
) -> tuple[str, Sequence[Mapping[str, Any]]]:
    definition = value.get("definition")
    if not isinstance(definition, Mapping):
        raise DefinitionBundleError("Fabric response has no definition object")
    format_name = definition.get("format")
    parts = definition.get("parts")
    if (not isinstance(format_name, str) or not format_name) and expected_format:
        format_name = expected_format
    if not isinstance(format_name, str) or not format_name:
        raise DefinitionBundleError("Fabric definition format is missing")
    if not isinstance(parts, list) or not parts:
        raise DefinitionBundleError("Fabric definition has no parts")
    if any(not isinstance(part, Mapping) for part in parts):
        raise DefinitionBundleError("Fabric definition part must be an object")
    return format_name, parts


def _validate_path_tree(paths: Sequence[str]) -> None:
    path_set = set(paths)
    if len(path_set) != len(paths):
        raise DefinitionBundleError("duplicate definition part path")
    for value in path_set:
        parents = PurePosixPath(value).parents
        if any(parent.as_posix() in path_set for parent in parents[:-1]):
            raise DefinitionBundleError(
                f"definition part path conflicts with a directory: {value!r}"
            )


def _decode_parts(parts: Sequence[Mapping[str, Any]]) -> dict[str, bytes]:
    decoded: dict[str, bytes] = {}
    for part in parts:
        path = _part_path(part.get("path"))
        if path in decoded:
            raise DefinitionBundleError(f"duplicate definition part {path!r}")
        if part.get("payloadType") != "InlineBase64":
            raise DefinitionBundleError(
                f"definition part {path!r} is not InlineBase64"
            )
        payload = part.get("payload")
        if not isinstance(payload, str):
            raise DefinitionBundleError(
                f"definition part {path!r} has no payload"
            )
        try:
            decoded[path] = base64.b64decode(payload, validate=True)
        except (ValueError, binascii.Error) as error:
            raise DefinitionBundleError(
                f"definition part {path!r} is not valid base64"
            ) from error
    _validate_path_tree(list(decoded))
    return decoded


def _normalize_replacements(
    replacements: Mapping[str, str] | Sequence[TokenReplacement],
    parts: Mapping[str, bytes],
) -> tuple[TokenReplacement, ...]:
    if isinstance(replacements, Mapping):
        values = tuple(
            TokenReplacement(value=value, name=name)
            for value, name in replacements.items()
        )
    else:
        values = tuple(replacements)
    if any(not isinstance(item, TokenReplacement) for item in values):
        raise DefinitionBundleError("token replacements must be TokenReplacement values")

    names: set[str] = set()
    sources: set[str] = set()
    for item in values:
        if not item.value:
            raise DefinitionBundleError("token replacement value cannot be empty")
        if _TOKEN_NAME.fullmatch(item.name) is None:
            raise DefinitionBundleError(f"invalid token name {item.name!r}")
        if item.name in names:
            raise DefinitionBundleError(f"duplicate token name {item.name!r}")
        if item.value in sources:
            raise DefinitionBundleError("duplicate token source value")
        names.add(item.name)
        sources.add(item.value)
        normalized_parts = tuple(_part_path(path) for path in item.parts)
        if len(set(normalized_parts)) != len(normalized_parts):
            raise DefinitionBundleError(
                f"duplicate part scope for token {item.name!r}"
            )
        missing = sorted(set(normalized_parts) - set(parts))
        if missing:
            raise DefinitionBundleError(
                f"token {item.name!r} references missing parts: {', '.join(missing)}"
            )
        placeholder = f"{{{{ .{item.name} }}}}".encode()
        scoped_parts = normalized_parts or tuple(parts)
        if any(placeholder in parts[path] for path in scoped_parts):
            raise DefinitionBundleError(
                f"token placeholder for {item.name!r} already exists"
            )

    ordered_sources = sorted(sources)
    for index, source in enumerate(ordered_sources):
        for other in ordered_sources[index + 1 :]:
            if source in other:
                raise DefinitionBundleError(
                    "overlapping token source values are not allowed"
                )
    return tuple(sorted(values, key=lambda item: item.name))


def _tokenize(
    parts: Mapping[str, bytes],
    replacements: Mapping[str, str] | Sequence[TokenReplacement],
) -> tuple[dict[str, bytes], list[dict[str, object]]]:
    normalized = _normalize_replacements(replacements, parts)
    if not normalized:
        return dict(parts), []

    result: dict[str, bytes] = {}
    counts = {item.name: 0 for item in normalized}
    touched = {item.name: set() for item in normalized}
    for path, content in parts.items():
        applicable = [
            item for item in normalized if not item.parts or path in item.parts
        ]
        raw_sources = [
            item for item in applicable if item.value.encode("utf-8") in content
        ]
        if not raw_sources:
            result[path] = content
            continue
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError as error:
            names = ", ".join(item.name for item in raw_sources)
            raise DefinitionBundleError(
                f"token source for {names} occurs in non-UTF-8 part {path!r}"
            ) from error
        by_value = {item.value: item for item in applicable}
        pattern = re.compile(
            "|".join(
                re.escape(value)
                for value in sorted(by_value, key=lambda value: (-len(value), value))
            )
        )

        def replace(match: re.Match[str]) -> str:
            item = by_value[match.group(0)]
            counts[item.name] += 1
            touched[item.name].add(path)
            return f"{{{{ .{item.name} }}}}"

        result[path] = pattern.sub(replace, text).encode("utf-8")

    token_metadata = []
    for item in normalized:
        if counts[item.name] == 0:
            raise DefinitionBundleError(
                f"token source value for {item.name!r} was not found"
            )
        token_metadata.append(
            {
                "name": item.name,
                "occurrences": counts[item.name],
                "parts": sorted(touched[item.name]),
            }
        )
    return result, token_metadata


def _load_metadata(destination: Path) -> Mapping[str, Any] | None:
    path = destination / METADATA_FILE
    if not path.exists():
        return None
    if path.is_symlink():
        raise DefinitionBundleError(f"bundle metadata cannot be a symlink: {path}")
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DefinitionBundleError(f"invalid bundle metadata at {path}") from error
    if not isinstance(value, Mapping) or value.get("schema") != BUNDLE_SCHEMA:
        raise DefinitionBundleError(f"unmanaged existing destination {destination}")
    if raw != _canonical_bytes(value) + b"\n":
        raise DefinitionBundleError(f"bundle metadata is not canonical: {path}")
    return value


def _part_metadata(parts: Mapping[str, bytes]) -> list[dict[str, object]]:
    return [
        {
            "path": path,
            "sha256": hashlib.sha256(parts[path]).hexdigest(),
            "size": len(parts[path]),
        }
        for path in sorted(parts)
    ]


def _content_sha(
    format_name: str,
    item_type: str | None,
    part_metadata: Sequence[Mapping[str, object]],
) -> str:
    descriptor: dict[str, object] = {
        "format": format_name,
        "parts": list(part_metadata),
    }
    if item_type is not None:
        descriptor["itemType"] = item_type
    return hashlib.sha256(_canonical_bytes(descriptor)).hexdigest()


def _assert_safe_existing_tree(destination: Path) -> Mapping[str, Any] | None:
    if destination.is_symlink():
        raise DefinitionBundleError(f"bundle destination cannot be a symlink: {destination}")
    metadata = _load_metadata(destination)
    if not destination.exists():
        return metadata
    for path in destination.rglob("*"):
        if path.is_symlink():
            raise DefinitionBundleError(f"bundle cannot contain symlinks: {path}")
    existing_files = [path for path in destination.rglob("*") if path.is_file()]
    if metadata is None and existing_files:
        raise DefinitionBundleError(f"unmanaged existing destination {destination}")
    if metadata is not None:
        read_definition_bundle(destination)
    return metadata


def _prepare_definition_bundle(
    value: Mapping[str, Any],
    *,
    replacements: Mapping[str, str] | Sequence[TokenReplacement] | None = None,
    item_type: str | None = None,
    expected_format: str | None = None,
) -> tuple[dict[str, bytes], dict[str, object]]:
    format_name, raw_parts = _definition(value, expected_format)
    if expected_format is not None and format_name != expected_format:
        raise DefinitionBundleError(
            f"Fabric returned format {format_name!r}, expected {expected_format!r}"
        )
    if item_type is not None and (
        not isinstance(item_type, str)
        or not item_type
        or item_type.strip() != item_type
    ):
        raise DefinitionBundleError("item type must be a non-empty string")
    decoded = _decode_parts(raw_parts)
    parts, token_metadata = _tokenize(decoded, replacements or ())
    part_metadata = _part_metadata(parts)
    metadata: dict[str, object] = {
        "contentSha256": _content_sha(format_name, item_type, part_metadata),
        "format": format_name,
        "parts": part_metadata,
        "schema": BUNDLE_SCHEMA,
        "tokens": token_metadata,
    }
    if item_type is not None:
        metadata["itemType"] = item_type
    return parts, metadata


def definition_bundle_metadata(
    value: Mapping[str, Any],
    *,
    replacements: Mapping[str, str] | Sequence[TokenReplacement] | None = None,
    item_type: str | None = None,
    expected_format: str | None = None,
) -> Mapping[str, object]:
    """Hash a sanitized Fabric definition without writing its parts to disk."""
    _, metadata = _prepare_definition_bundle(
        value,
        replacements=replacements,
        item_type=item_type,
        expected_format=expected_format,
    )
    return metadata


def _remove_empty_directories(destination: Path) -> None:
    directories = sorted(
        (path for path in destination.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    for path in directories:
        try:
            path.rmdir()
        except OSError:
            pass


def write_definition_bundle(
    value: Mapping[str, Any],
    destination: Path,
    *,
    replacements: Mapping[str, str] | Sequence[TokenReplacement] | None = None,
    item_type: str | None = None,
    expected_format: str | None = None,
) -> Mapping[str, Any]:
    """Decode a Fabric getDefinition response into a deterministic directory."""
    parts, metadata = _prepare_definition_bundle(
        value,
        replacements=replacements,
        item_type=item_type,
        expected_format=expected_format,
    )
    existing = _assert_safe_existing_tree(destination)
    destination.mkdir(parents=True, exist_ok=True)

    current_paths = set(parts)
    if existing is not None:
        old_parts = existing.get("parts")
        if not isinstance(old_parts, list):
            raise DefinitionBundleError("existing bundle metadata has no parts list")
        for old_part in old_parts:
            if not isinstance(old_part, Mapping):
                raise DefinitionBundleError("existing bundle part metadata is invalid")
            old_path = _part_path(old_part.get("path"))
            if old_path not in current_paths:
                stale = destination.joinpath(*PurePosixPath(old_path).parts)
                if stale.is_file():
                    stale.unlink()
        _remove_empty_directories(destination)

    for path in sorted(parts):
        target = destination.joinpath(*PurePosixPath(path).parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(parts[path])

    (destination / METADATA_FILE).write_bytes(_canonical_bytes(metadata) + b"\n")
    return metadata


def _validated_metadata(
    metadata: Mapping[str, Any],
) -> tuple[str, str | None, list[Mapping[str, Any]]]:
    allowed = {
        "contentSha256",
        "format",
        "itemType",
        "parts",
        "schema",
        "tokens",
    }
    unknown = sorted(set(metadata) - allowed)
    if unknown:
        raise DefinitionBundleError(
            f"bundle metadata has unknown fields: {', '.join(unknown)}"
        )
    format_name = metadata.get("format")
    item_type = metadata.get("itemType")
    part_metadata = metadata.get("parts")
    tokens = metadata.get("tokens")
    content_sha = metadata.get("contentSha256")
    if not isinstance(format_name, str) or not format_name:
        raise DefinitionBundleError("bundle metadata format is invalid")
    if item_type is not None and (
        not isinstance(item_type, str)
        or not item_type
        or item_type.strip() != item_type
    ):
        raise DefinitionBundleError("bundle metadata item type is invalid")
    if not isinstance(part_metadata, list) or not part_metadata:
        raise DefinitionBundleError("bundle metadata parts are invalid")
    if not isinstance(tokens, list):
        raise DefinitionBundleError("bundle metadata tokens are invalid")
    if not isinstance(content_sha, str) or _SHA256.fullmatch(content_sha) is None:
        raise DefinitionBundleError("bundle metadata content hash is invalid")
    token_names: set[str] = set()
    for token in tokens:
        if not isinstance(token, Mapping) or set(token) != {
            "name",
            "occurrences",
            "parts",
        }:
            raise DefinitionBundleError("bundle token metadata is invalid")
        name = token.get("name")
        occurrences = token.get("occurrences")
        token_parts = token.get("parts")
        if (
            not isinstance(name, str)
            or _TOKEN_NAME.fullmatch(name) is None
            or name in token_names
            or not isinstance(occurrences, int)
            or isinstance(occurrences, bool)
            or occurrences < 1
            or not isinstance(token_parts, list)
            or not token_parts
            or any(not isinstance(part, str) for part in token_parts)
            or token_parts != sorted(set(token_parts))
        ):
            raise DefinitionBundleError("bundle token metadata is invalid")
        token_names.add(name)
    return format_name, item_type, part_metadata


def read_definition_bundle(source: Path) -> dict[str, Any]:
    """Validate and rebuild an InlineBase64 Fabric definition from a bundle."""
    metadata = _load_metadata(source)
    if metadata is None:
        raise DefinitionBundleError(f"bundle metadata is missing from {source}")
    format_name, item_type, part_metadata = _validated_metadata(metadata)

    parts = []
    observed_metadata = []
    content_by_path: dict[str, bytes] = {}
    listed_paths: list[str] = []
    for item in part_metadata:
        if not isinstance(item, Mapping) or set(item) != {"path", "sha256", "size"}:
            raise DefinitionBundleError("bundle part metadata is invalid")
        path = _part_path(item.get("path"))
        expected_sha = item.get("sha256")
        expected_size = item.get("size")
        if not isinstance(expected_sha, str) or _SHA256.fullmatch(expected_sha) is None:
            raise DefinitionBundleError(f"bundle part hash is invalid: {path}")
        if not isinstance(expected_size, int) or isinstance(expected_size, bool) or expected_size < 0:
            raise DefinitionBundleError(f"bundle part size is invalid: {path}")
        content_path = source.joinpath(*PurePosixPath(path).parts)
        if content_path.is_symlink():
            raise DefinitionBundleError(f"bundle part cannot be a symlink: {path}")
        try:
            content = content_path.read_bytes()
        except OSError as error:
            raise DefinitionBundleError(f"bundle part is missing: {path}") from error
        observed_sha = hashlib.sha256(content).hexdigest()
        if observed_sha != expected_sha:
            raise DefinitionBundleError(f"bundle part hash mismatch: {path}")
        if len(content) != expected_size:
            raise DefinitionBundleError(f"bundle part size mismatch: {path}")
        listed_paths.append(path)
        content_by_path[path] = content
        observed_metadata.append(
            {"path": path, "sha256": observed_sha, "size": len(content)}
        )
        parts.append(
            {
                "path": path,
                "payload": base64.b64encode(content).decode("ascii"),
                "payloadType": "InlineBase64",
            }
        )
    _validate_path_tree(listed_paths)
    if listed_paths != sorted(listed_paths):
        raise DefinitionBundleError("bundle part metadata is not sorted")
    listed_path_set = set(listed_paths)
    for token in metadata["tokens"]:
        token_parts = token["parts"]
        if any(_part_path(path) not in listed_path_set for path in token_parts):
            raise DefinitionBundleError("bundle token references an unlisted part")
        placeholder = f"{{{{ .{token['name']} }}}}".encode()
        observed_parts = sorted(
            path for path, content in content_by_path.items() if placeholder in content
        )
        observed_occurrences = sum(
            content.count(placeholder) for content in content_by_path.values()
        )
        if (
            token_parts != observed_parts
            or token["occurrences"] != observed_occurrences
        ):
            raise DefinitionBundleError("bundle token evidence does not match parts")

    expected_content_sha = _content_sha(format_name, item_type, observed_metadata)
    if metadata["contentSha256"] != expected_content_sha:
        raise DefinitionBundleError("bundle content hash mismatch")

    listed_files = {METADATA_FILE, *listed_paths}
    actual_files: set[str] = set()
    for path in source.rglob("*"):
        if path.is_symlink():
            raise DefinitionBundleError(f"bundle cannot contain symlinks: {path}")
        if path.is_file():
            actual_files.add(path.relative_to(source).as_posix())
    extras = sorted(actual_files - listed_files)
    if extras:
        raise DefinitionBundleError(
            f"bundle contains unlisted files: {', '.join(extras)}"
        )
    return {"definition": {"format": format_name, "parts": parts}}


def _manifest_object(value: object, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DefinitionBundleError(f"{context} must be an object")
    return value


def _manifest_keys(
    value: Mapping[str, Any], allowed: set[str], context: str
) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise DefinitionBundleError(
            f"{context} has unknown fields: {', '.join(unknown)}"
        )


def _environment_value(
    environment: Mapping[str, str], name: object, context: str
) -> str:
    if not isinstance(name, str) or _ENVIRONMENT_NAME.fullmatch(name) is None:
        raise DefinitionBundleError(f"{context} environment variable name is invalid")
    value = environment.get(name)
    if not value:
        raise DefinitionBundleError(
            f"required environment variable {name!r} is not set"
        )
    return value


def load_definition_export_manifest(
    path: Path,
    environment: Mapping[str, str],
) -> DefinitionExportManifest:
    """Resolve a strict batch export manifest without storing live IDs in Git."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DefinitionBundleError(f"invalid export manifest at {path}") from error
    manifest = _manifest_object(raw, "definition export manifest")
    _manifest_keys(manifest, {"items", "schema", "workspaceIdEnv"}, "export manifest")
    if manifest.get("schema") != DEFINITION_EXPORT_SCHEMA:
        raise DefinitionBundleError("definition export schema is unsupported")
    workspace_id = _environment_value(
        environment, manifest.get("workspaceIdEnv"), "workspace ID"
    )
    raw_items = manifest.get("items")
    if not isinstance(raw_items, list) or not raw_items:
        raise DefinitionBundleError("definition export manifest must contain items")

    items: list[DefinitionExportItem] = []
    destinations: set[PurePosixPath] = set()
    item_ids: set[str] = set()
    for index, raw_item in enumerate(raw_items):
        context = f"definition export item {index}"
        item = _manifest_object(raw_item, context)
        _manifest_keys(
            item,
            {"destination", "format", "itemIdEnv", "itemType", "tokens"},
            context,
        )
        item_id = _environment_value(
            environment, item.get("itemIdEnv"), f"{context} item ID"
        )
        item_type = item.get("itemType")
        if (
            not isinstance(item_type, str)
            or not item_type
            or item_type.strip() != item_type
        ):
            raise DefinitionBundleError(f"{context} item type is invalid")
        destination = _relative_destination(item.get("destination"))
        format_name = item.get("format")
        if format_name is not None and (
            not isinstance(format_name, str) or not format_name
        ):
            raise DefinitionBundleError(f"{context} format is invalid")
        raw_tokens = item.get("tokens", [])
        if not isinstance(raw_tokens, list):
            raise DefinitionBundleError(f"{context} tokens must be a list")
        replacements = []
        for token_index, raw_token in enumerate(raw_tokens):
            token_context = f"{context} token {token_index}"
            token = _manifest_object(raw_token, token_context)
            _manifest_keys(token, {"name", "parts", "valueEnv"}, token_context)
            name = token.get("name")
            if not isinstance(name, str):
                raise DefinitionBundleError(f"{token_context} name is invalid")
            raw_parts = token.get("parts", [])
            if not isinstance(raw_parts, list) or any(
                not isinstance(part, str) for part in raw_parts
            ):
                raise DefinitionBundleError(f"{token_context} parts are invalid")
            replacements.append(
                TokenReplacement(
                    value=_environment_value(
                        environment, token.get("valueEnv"), token_context
                    ),
                    name=name,
                    parts=tuple(raw_parts),
                )
            )
        if destination in destinations:
            raise DefinitionBundleError(
                f"duplicate export destination {destination.as_posix()!r}"
            )
        if item_id in item_ids:
            raise DefinitionBundleError(f"duplicate export item ID {item_id!r}")
        destinations.add(destination)
        item_ids.add(item_id)
        items.append(
            DefinitionExportItem(
                item_id=item_id,
                item_type=item_type,
                destination=path.parent.joinpath(*destination.parts),
                format_name=format_name,
                replacements=tuple(replacements),
            )
        )
    return DefinitionExportManifest(workspace_id=workspace_id, items=tuple(items))
