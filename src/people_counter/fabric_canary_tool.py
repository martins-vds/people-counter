"""Fail-closed tooling for the isolated Fabric SparkJobDefinition canary.

The module deliberately uses only the standard library at import time.  Azure
Identity is imported only when its corresponding authentication mode is used.
"""

from __future__ import annotations

import argparse
import ast
import base64
import binascii
import contextlib
import datetime as dt
import email.utils
import hashlib
import io
import importlib.metadata
import json
import os
import posixpath
import re
import stat
import subprocess
import sys
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Protocol


WORKSPACE_ID = "c31ee864-230d-4005-8fd5-7c7130ebf774"
LAKEHOUSE_ID = "883cff91-eaa8-40be-870f-6e9716303cb2"
DISPLAY_NAME = "pc-ca-r20-sjd-canary-v001"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_RESOURCE = "https://api.fabric.microsoft.com"
FABRIC_API_ROOT = "https://api.fabric.microsoft.com/v1"
METADATA_PATH = "SparkJobDefinitionV1.json"
MAIN_PATH = "Main/main.py"
MANIFEST_PATH = "Main/main.py::_CANARY_MANIFEST_B64"
LEGACY_MANIFEST_PATH = "Libs/canary_manifest.py"
PLATFORM_PATH = ".platform"
REQUIRED_RUN_ID = "__REQUIRED__"
REQUIRED_DEFINITION_SHA256 = "0" * 64
DEFINITION_DESCRIPTION = "Isolated people-counter R20 canary; managed by fabric_canary_tool"
WRITE_SCOPE = "_canary/people-counter/runtime2-sjd/v1"
SAFETY_TOKEN = "PC_CANARY_ONLY_V1"
TERMINAL_SUCCEEDED = frozenset({"succeeded", "completed"})
TERMINAL_FAILED = frozenset({"failed", "cancelled", "canceled"})
SOURCE_ARCHIVE_MAX_BYTES = 4 * 1024 * 1024
SOURCE_ARCHIVE_MAX_MEMBER_BYTES = 1024 * 1024
SOURCE_ARCHIVE_MAX_MEMBERS = 256
SOURCE_ARCHIVE_MAX_RATIO = 100
SOURCE_ARCHIVE_REQUIRED_MEMBERS = frozenset(
    {
        "people_counter/__init__.py",
        "people_counter/fabric_runtime2_canary.py",
        "people_counter/sjd_process.py",
    }
)


class CanaryError(RuntimeError):
    """Base error for canary validation and Fabric operations."""


class SecretScanError(CanaryError):
    """Decoded definition content appears to contain a secret."""


class PruneRefusedError(CanaryError):
    """A complete replacement would discard an unaccounted-for part."""


class FabricHTTPError(CanaryError):
    """Fabric returned an unexpected or unsuccessful response."""


class DefinitionMismatchError(CanaryError):
    """Fabric readback does not equal the locally serialized definition."""


def canonical_json_bytes(value: object) -> bytes:
    """Serialize JSON with one stable, whitespace-free representation."""
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _as_bytes(value: str | bytes) -> bytes:
    return value.encode("utf-8") if isinstance(value, str) else bytes(value)


def _validate_part_path(path: str) -> str:
    pure = PurePosixPath(path)
    if (
        not path
        or path.startswith("/")
        or "\\" in path
        or ".." in pure.parts
        or "." in pure.parts
    ):
        raise ValueError(f"Unsafe definition part path: {path!r}")
    return path


def encode_part(path: str, content: str | bytes) -> dict[str, str]:
    """Create an InlineBase64 definition part."""
    return {
        "path": _validate_part_path(path),
        "payload": base64.b64encode(_as_bytes(content)).decode("ascii"),
        "payloadType": "InlineBase64",
    }


def decode_part(part: Mapping[str, object]) -> bytes:
    if part.get("payloadType") != "InlineBase64":
        raise CanaryError(
            f"Unsupported payloadType for {part.get('path')!r}; "
            "only InlineBase64 can be inspected safely"
        )
    payload = part.get("payload")
    if not isinstance(payload, str):
        raise CanaryError(f"Definition part {part.get('path')!r} has no string payload")
    try:
        decoded = base64.b64decode(payload, validate=True)
    except (ValueError, binascii.Error) as error:
        raise CanaryError(
            f"Definition part {part.get('path')!r} is not canonical base64"
        ) from error
    if base64.b64encode(decoded).decode("ascii") != payload:
        raise CanaryError(
            f"Definition part {part.get('path')!r} is not canonical base64"
        )
    return decoded


def _definition_object(payload: Mapping[str, object]) -> Mapping[str, object]:
    definition = payload.get("definition")
    if not isinstance(definition, Mapping):
        raise CanaryError("Payload must contain a definition object")
    return definition


def definition_parts(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    parts = _definition_object(payload).get("parts")
    if not isinstance(parts, list):
        raise CanaryError("Definition must contain a parts list")
    result: list[Mapping[str, object]] = []
    seen: set[str] = set()
    for part in parts:
        if not isinstance(part, Mapping) or not isinstance(part.get("path"), str):
            raise CanaryError("Every definition part must be an object with a path")
        path = _validate_part_path(str(part["path"]))
        if path in seen:
            raise CanaryError(f"Duplicate definition part: {path}")
        seen.add(path)
        result.append(part)
    return result


def decoded_parts(payload: Mapping[str, object]) -> dict[str, bytes]:
    return {str(part["path"]): decode_part(part) for part in definition_parts(payload)}


def _normalize_libs(
    libs: Mapping[str, str | bytes] | None,
) -> dict[str, bytes]:
    normalized: dict[str, bytes] = {}
    for supplied_path, content in (libs or {}).items():
        if supplied_path in {MANIFEST_PATH, LEGACY_MANIFEST_PATH}:
            raise ValueError("generated canary manifest cannot be supplied")
        path = supplied_path
        if not path.startswith("Libs/"):
            path = f"Libs/{path}"
        _validate_part_path(path)
        if PurePosixPath(path).parent != PurePosixPath("Libs"):
            raise ValueError("Inline libraries must be direct children of Libs/")
        if path == LEGACY_MANIFEST_PATH:
            raise ValueError("generated canary manifest cannot be supplied")
        normalized[path] = _as_bytes(content)
    return normalized


def _normalized_zip_path(name: str) -> str:
    """Return one canonical safe member name or reject it."""
    if (
        not name
        or "\x00" in name
        or "\\" in name
        or name.startswith("/")
        or not name.isascii()
    ):
        raise CanaryError(f"Unsafe source archive member path: {name!r}")
    normalized = unicodedata.normalize("NFC", posixpath.normpath(name))
    pure = PurePosixPath(normalized)
    if (
        normalized != name
        or normalized in {".", ".."}
        or ".." in pure.parts
        or "." in pure.parts
        or any(not part for part in pure.parts)
    ):
        raise CanaryError(f"Non-canonical source archive member path: {name!r}")
    return normalized


def _source_member_allowed(path: str) -> bool:
    pure = PurePosixPath(path)
    if pure.parts[0] == "people_counter":
        return pure.name == "py.typed" or pure.suffix == ".py"
    return (
        len(pure.parts) == 2
        and re.fullmatch(r"people_counter-[A-Za-z0-9_.+-]+\.dist-info", pure.parts[0])
        is not None
        and pure.name == "METADATA"
    )


def _scan_text_for_secrets(path: str, content: bytes) -> list["SecretFinding"]:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as error:
        raise CanaryError(
            f"Source archive member is not strict UTF-8 text: {path}"
        ) from error
    findings: list[SecretFinding] = []
    for rule, pattern in _SECRET_RULES:
        for match in pattern.finditer(text):
            findings.append(
                SecretFinding(path, rule, text.count("\n", 0, match.start()) + 1)
            )
    return findings


def inspect_source_archive(content: bytes) -> dict[str, object]:
    """Fully parse, bound, decode, hash, and secret-scan an exact-source ZIP."""
    archive, infos = _open_source_archive(content)
    seen: set[str] = set()
    total_size = 0
    members: list[dict[str, object]] = []
    findings: list[SecretFinding] = []
    with archive:
        for info in infos:
            path = _validate_source_member_info(info, seen, total_size)
            decoded = _read_source_member(archive, info, path)
            findings.extend(_scan_text_for_secrets(path, decoded))
            total_size += len(decoded)
            members.append(
                {
                    "path": path,
                    "sha256": sha256_bytes(decoded),
                    "size": len(decoded),
                }
            )
    _validate_source_archive_members(members, findings)
    return {
        "sha256": sha256_bytes(content),
        "size": len(content),
        "members": sorted(members, key=lambda member: str(member["path"])),
    }


def _open_source_archive(
    content: bytes,
) -> tuple[zipfile.ZipFile, list[zipfile.ZipInfo]]:
    if not content or len(content) > SOURCE_ARCHIVE_MAX_BYTES:
        raise CanaryError("Source archive size is outside the allowed bounds")
    if not content.startswith(b"PK\x03\x04"):
        raise CanaryError("Source archive has an invalid or prepended ZIP header")
    end = content.rfind(b"PK\x05\x06")
    if end < 0 or end + 22 > len(content):
        raise CanaryError("Source archive has no valid ZIP end record")
    comment_size = int.from_bytes(content[end + 20 : end + 22], "little")
    if comment_size or end + 22 != len(content):
        raise CanaryError("Source archive has trailing or malformed data")
    try:
        archive = zipfile.ZipFile(io.BytesIO(content))
        infos = archive.infolist()
    except (OSError, zipfile.BadZipFile) as error:
        raise CanaryError("Source archive is not a parseable ZIP") from error
    if not infos or len(infos) > SOURCE_ARCHIVE_MAX_MEMBERS:
        archive.close()
        raise CanaryError("Source archive member count is outside allowed bounds")
    return archive, infos


def _validate_source_member_info(
    info: zipfile.ZipInfo,
    seen: set[str],
    total_size: int,
) -> str:
    path = _normalized_zip_path(info.filename)
    duplicate_key = path.casefold()
    if duplicate_key in seen:
        raise CanaryError(f"Duplicate normalized source archive member: {path}")
    seen.add(duplicate_key)
    kind = stat.S_IFMT(info.external_attr >> 16)
    if info.is_dir() or kind not in {0, stat.S_IFREG}:
        raise CanaryError(f"Source archive member is not a regular file: {path}")
    if info.flag_bits & 0x1:
        raise CanaryError(f"Encrypted source archive member: {path}")
    if info.flag_bits & ~(0x800 | 0x08):
        raise CanaryError(f"Unsupported source archive member flags: {path}")
    if info.extra or info.comment or info.extract_version > 45:
        raise CanaryError(f"Unsupported source archive member metadata: {path}")
    if info.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
        raise CanaryError(f"Unsupported source archive compression: {path}")
    if (
        info.file_size > SOURCE_ARCHIVE_MAX_MEMBER_BYTES
        or total_size + info.file_size > SOURCE_ARCHIVE_MAX_BYTES
    ):
        raise CanaryError("Source archive expanded size exceeds its limit")
    if info.file_size and (
        info.compress_size == 0
        or info.file_size / info.compress_size > SOURCE_ARCHIVE_MAX_RATIO
    ):
        raise CanaryError(
            f"Source archive compression ratio exceeds its limit: {path}"
        )
    if not _source_member_allowed(path):
        raise CanaryError(f"Unexpected source archive root or extension: {path}")
    return path


def _read_source_member(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    path: str,
) -> bytes:
    try:
        decoded = archive.read(info)
    except (OSError, RuntimeError, zipfile.BadZipFile) as error:
        raise CanaryError(
            f"Source archive member cannot be decoded: {path}"
        ) from error
    if len(decoded) != info.file_size:
        raise CanaryError(f"Source archive member size mismatch: {path}")
    return decoded


def _validate_source_archive_members(
    members: Sequence[Mapping[str, object]],
    findings: Sequence["SecretFinding"],
) -> None:
    missing = sorted(
        SOURCE_ARCHIVE_REQUIRED_MEMBERS
        - {str(member["path"]) for member in members}
    )
    if missing:
        raise CanaryError(
            "Source archive is missing required members: " + ", ".join(missing)
        )
    if findings:
        summary = ", ".join(
            f"{finding.path}:{finding.line} ({finding.rule})"
            for finding in findings
        )
        raise SecretScanError(
            f"Decoded source archive failed secret scan: {summary}"
        )


def build_source_archive(source_root: Path) -> bytes:
    """Build a deterministic stored ZIP from one exact ``people_counter`` tree."""
    package_root = source_root.resolve()
    if package_root.name != "people_counter" or not package_root.is_dir():
        raise CanaryError("source_root must be the people_counter package directory")
    paths = sorted(
        path
        for path in package_root.rglob("*")
        if path.is_file()
        and "__pycache__" not in path.parts
        and (path.suffix == ".py" or path.name == "py.typed")
    )
    metadata = (
        f"Name: people-counter\nVersion: {_project_version()}\n"
    ).encode("utf-8")
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as archive:
        for path in paths:
            relative = path.relative_to(package_root).as_posix()
            info = zipfile.ZipInfo(
                f"people_counter/{relative}",
                date_time=(1980, 1, 1, 0, 0, 0),
            )
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | 0o600) << 16
            archive.writestr(info, path.read_bytes())
        info = zipfile.ZipInfo(
            f"people_counter-{_project_version()}.dist-info/METADATA",
            date_time=(1980, 1, 1, 0, 0, 0),
        )
        info.create_system = 3
        info.external_attr = (stat.S_IFREG | 0o600) << 16
        archive.writestr(info, metadata)
    content = output.getvalue()
    inspect_source_archive(content)
    return content


def build_source_bootstrap(
    source_archive: bytes,
    manifest: Mapping[str, object] | None = None,
) -> bytes:
    """Generate the only accepted bootstrap for a verified source archive."""
    identity = inspect_source_archive(source_archive)
    digest = str(identity["sha256"])
    encoded = base64.b64encode(source_archive).decode("ascii")
    manifest_line = ""
    if manifest is not None:
        encoded_manifest = base64.b64encode(
            canonical_json_bytes(manifest)
        ).decode("ascii")
        manifest_line = f'_CANARY_MANIFEST_B64 = "{encoded_manifest}"\n'
    source = f'''"""Generated exact-worktree Fabric canary source bootstrap."""
import base64
import hashlib
import os
import sys
import tempfile
from pathlib import Path
_SOURCE_SHA256 = "{digest}"
_SOURCE = "{encoded}"
{manifest_line}\
payload = base64.b64decode(_SOURCE, validate=True)
if hashlib.sha256(payload).hexdigest() != _SOURCE_SHA256:
    raise RuntimeError("embedded source archive hash mismatch")
with tempfile.NamedTemporaryFile(
    prefix="people-counter-canary-",
    suffix=".zip",
    delete=False,
) as handle:
    handle.write(payload)
    handle.flush()
    os.fsync(handle.fileno())
    archive = Path(handle.name)
if str(archive) not in sys.path:
    sys.path.insert(0, str(archive))
os.environ["PC_CANARY_SOURCE_ARCHIVE_SHA256"] = _SOURCE_SHA256
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.addPyFile(archive.as_uri())
from people_counter.fabric_runtime2_canary import main
raise SystemExit(main())
'''
    return source.encode("utf-8")


def source_archive_from_bootstrap(main_source: bytes) -> tuple[bytes, dict[str, object]]:
    """Extract only a canonical generated bootstrap; reject opaque source."""
    archive, identity = _audited_archive_from_bootstrap(main_source)
    manifest = _optional_bootstrap_manifest(main_source)
    if main_source != build_source_bootstrap(archive, manifest):
        raise CanaryError("Bootstrap source is not the canonical audited template")
    return archive, identity


def _optional_bootstrap_manifest(
    main_source: bytes,
) -> Mapping[str, object] | None:
    try:
        module = ast.parse(main_source.decode("utf-8"))
    except (UnicodeDecodeError, SyntaxError) as error:
        raise CanaryError("Bootstrap source is not parseable strict UTF-8 Python") from error
    for node in module.body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "_CANARY_MANIFEST_B64"
        ):
            try:
                encoded = ast.literal_eval(node.value)
                decoded = base64.b64decode(encoded, validate=True)
                value = json.loads(decoded)
            except (
                ValueError,
                TypeError,
                binascii.Error,
                UnicodeDecodeError,
                json.JSONDecodeError,
            ) as error:
                raise CanaryError("Embedded canary manifest is invalid") from error
            if (
                not isinstance(value, dict)
                or canonical_json_bytes(value) != decoded
            ):
                raise CanaryError("Embedded canary manifest is not canonical")
            return value
    return None


def _audited_archive_from_bootstrap(
    main_source: bytes,
) -> tuple[bytes, dict[str, object]]:
    """Parse and scan a live bootstrap before it may be snapshotted."""
    try:
        module = ast.parse(main_source.decode("utf-8"))
    except (UnicodeDecodeError, SyntaxError) as error:
        raise CanaryError("Bootstrap source is not parseable strict UTF-8 Python") from error
    assignments: dict[str, object] = {}
    for node in module.body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id in {"_SOURCE", "_SOURCE_SHA256"}
        ):
            try:
                assignments[node.targets[0].id] = ast.literal_eval(node.value)
            except (ValueError, TypeError) as error:
                raise CanaryError("Bootstrap source identity is not literal") from error
    encoded = assignments.get("_SOURCE")
    declared = assignments.get("_SOURCE_SHA256")
    if not isinstance(encoded, str) or not isinstance(declared, str):
        raise CanaryError("Opaque bootstrap source has no exact-source archive")
    try:
        archive = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as error:
        raise CanaryError("Bootstrap exact-source archive is not canonical base64") from error
    identity = inspect_source_archive(archive)
    if identity["sha256"] != declared:
        raise CanaryError("Bootstrap exact-source archive hash mismatch")
    return archive, identity


def audit_definition_for_snapshot(
    payload: Mapping[str, object],
) -> dict[str, object]:
    """Decode and scan all outer and nested source before snapshot persistence."""
    if _definition_object(payload).get("format") != "SparkJobDefinitionV2":
        raise CanaryError("Definition format must be SparkJobDefinitionV2")
    parts = decoded_parts(payload)
    if MAIN_PATH not in parts:
        raise CanaryError("Live definition is missing bootstrap source")
    assert_no_secrets(payload)
    _, identity = _audited_archive_from_bootstrap(parts[MAIN_PATH])
    return identity


def build_definition(
    main_source: str | bytes,
    environment_id: str,
    *,
    libs: Mapping[str, str | bytes] | None = None,
    source_commit: str = "unknown",
    project_version: str = "unknown",
) -> dict[str, object]:
    """Build the deterministic, complete replacement definition.

    Retained library parts must be supplied explicitly in ``libs``.  The
    generated manifest hashes decoded bytes, rather than their base64 spelling.
    """
    try:
        uuid.UUID(environment_id)
    except (ValueError, AttributeError) as error:
        raise ValueError("environment_id must be a UUID") from error
    main_bytes = _as_bytes(main_source)
    source_archive, source_archive_identity = source_archive_from_bootstrap(main_bytes)
    normalized_libs = _normalize_libs(libs)
    package_identity = _installed_package_identity()
    release_digest = _release_digest(
        main_bytes,
        normalized_libs,
        source_commit=source_commit,
        project_version=project_version,
        package_identity=package_identity,
        source_archive_identity=source_archive_identity,
    )
    saved_arguments = _command_line_arguments(
        REQUIRED_RUN_ID,
        release_digest,
        project_version,
        package_identity,
        REQUIRED_DEFINITION_SHA256,
        str(source_archive_identity["sha256"]),
    )
    metadata = {
        "additionalLakehouseIds": [],
        "additionalLibraryUris": sorted(
            PurePosixPath(path).name for path in normalized_libs
        ),
        "commandLineArguments": saved_arguments,
        "defaultLakehouseArtifactId": LAKEHOUSE_ID,
        "environmentArtifactId": environment_id,
        "executableFile": PurePosixPath(MAIN_PATH).name,
        "language": "Python",
        "mainClass": "",
        "retryPolicy": None,
    }
    contents_without_main: dict[str, bytes] = {
        METADATA_PATH: canonical_json_bytes(metadata),
        **normalized_libs,
    }
    manifest = {
        "bootstrapSha256": sha256_bytes(main_bytes),
        "displayName": DISPLAY_NAME,
        "lakehouseId": LAKEHOUSE_ID,
        "partHashes": {
            path: sha256_bytes(content)
            for path, content in sorted(contents_without_main.items())
        },
        "projectVersion": project_version,
        "packageIdentity": package_identity,
        "releaseDigest": release_digest,
        "schemaVersion": 1,
        "sourceArchive": source_archive_identity,
        "sourceCommit": source_commit,
        "workspaceId": WORKSPACE_ID,
    }
    contents: dict[str, bytes] = {
        MAIN_PATH: build_source_bootstrap(source_archive, manifest),
        **contents_without_main,
    }
    payload: dict[str, object] = {
        "definition": {
            "format": "SparkJobDefinitionV2",
            "parts": [
                encode_part(path, contents[path])
                for path in sorted(contents)
            ]
        }
    }
    assert_no_secrets(payload)
    return payload


def _release_digest(
    main: bytes,
    libs: Mapping[str, bytes],
    *,
    source_commit: str,
    project_version: str,
    package_identity: str,
    source_archive_identity: Mapping[str, object],
) -> str:
    release_material = {
        "libs": {
            path: sha256_bytes(content)
            for path, content in sorted(libs.items())
        },
        "mainSha256": sha256_bytes(main),
        "packageIdentity": package_identity,
        "projectVersion": project_version,
        "sourceCommit": source_commit,
        "sourceArchive": source_archive_identity,
    }
    return sha256_bytes(canonical_json_bytes(release_material))


def _command_line_arguments(
    run_id: str,
    release_digest: str,
    project_version: str,
    package_identity: str,
    definition_sha256: str,
    source_archive_sha256: str,
) -> str:
    """Return the exact complete canary argument override in stable order."""
    return " ".join(
        (
            "--workspace-id",
            WORKSPACE_ID,
            "--lakehouse-id",
            LAKEHOUSE_ID,
            "--run-id",
            run_id,
            "--write-scope",
            WRITE_SCOPE,
            "--safety-token",
            SAFETY_TOKEN,
            "--release-digest",
            release_digest,
            "--project-version",
            project_version,
            "--package-identity",
            package_identity,
            "--definition-sha256",
            definition_sha256,
            "--source-archive-sha256",
            source_archive_sha256,
        )
    )


def _installed_package_identity() -> str:
    from people_counter.fabric_runtime2_canary import installed_package_identity

    return installed_package_identity()


def retained_libs(payload: Mapping[str, object]) -> dict[str, bytes]:
    """Decode all live ``Libs/`` parts except the generated manifest."""
    return {
        path: content
        for path, content in decoded_parts(payload).items()
        if path.startswith("Libs/")
        and path not in {MANIFEST_PATH, LEGACY_MANIFEST_PATH}
    }


def definition_hash(payload: Mapping[str, object]) -> str:
    """Hash the canonical complete replacement payload."""
    return sha256_bytes(canonical_json_bytes(payload))


def _decoded_part_hashes(
    payload: Mapping[str, object],
) -> dict[str, str]:
    return {
        path: _part_hash(path, content)
        for path, content in sorted(decoded_parts(payload).items())
        if path != PLATFORM_PATH
    }


def _part_hash(path: str, content: bytes) -> str:
    if path == METADATA_PATH:
        return sha256_bytes(
            canonical_json_bytes(_decoded_json_object(content, METADATA_PATH))
        )
    return sha256_bytes(content)


@dataclass(frozen=True)
class SecretFinding:
    path: str
    rule: str
    line: int


_SECRET_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "private-key",
        re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    ),
    (
        "azure-storage-account-key",
        re.compile(r"(?i)\bAccountKey\s*=\s*[A-Za-z0-9+/]{20,}={0,2}"),
    ),
    (
        "connection-string",
        re.compile(
            r"(?i)\b(?:DefaultEndpointsProtocol|Server|Data Source)\s*="
            r"[^;\r\n]+;[^;\r\n]+="
        ),
    ),
    (
        "sas-signature",
        re.compile(r"(?i)(?:[?&;]|^)\s*sig=[A-Za-z0-9%+/=_-]{12,}"),
    ),
    (
        "bearer-token",
        re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{16,}"),
    ),
    (
        "jwt",
        re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b"),
    ),
    (
        "assigned-secret",
        re.compile(
            r"""(?ix)
            \b(?:client[_-]?secret|secret|password|passwd|api[_-]?key|
                access[_-]?token|token)
            \s*(?:=|:)\s*["']
            (?!__REQUIRED__|REDACTED|CHANGEME|<[^>]+>|\$\{[^}]+\})
            [^"'\r\n]{8,}["']
            """
        ),
    ),
)


def scan_decoded_parts(
    payload: Mapping[str, object],
) -> list[SecretFinding]:
    """Scan decoded text parts; binary parts are inspected as lossy text."""
    findings: list[SecretFinding] = []
    for path, content in sorted(decoded_parts(payload).items()):
        text = content.decode("utf-8", errors="replace")
        for rule, pattern in _SECRET_RULES:
            for match in pattern.finditer(text):
                findings.append(
                    SecretFinding(path, rule, text.count("\n", 0, match.start()) + 1)
                )
    return findings


def assert_no_secrets(payload: Mapping[str, object]) -> None:
    findings = scan_decoded_parts(payload)
    if findings:
        summary = ", ".join(
            f"{finding.path}:{finding.line} ({finding.rule})"
            for finding in findings
        )
        raise SecretScanError(f"Decoded definition failed secret scan: {summary}")


def refuse_unmanaged_prune(
    live_payload: Mapping[str, object],
    replacement_payload: Mapping[str, object],
) -> None:
    """Refuse any complete replacement that silently removes a live part."""
    live_paths = {str(part["path"]) for part in definition_parts(live_payload)}
    replacement_paths = {
        str(part["path"]) for part in definition_parts(replacement_payload)
    }
    removed = sorted(
        (live_paths - replacement_paths)
        - {PLATFORM_PATH, LEGACY_MANIFEST_PATH}
    )
    if removed:
        raise PruneRefusedError(
            "Complete replacement would prune unmanaged parts: " + ", ".join(removed)
        )


def verify_definition(payload: Mapping[str, object]) -> dict[str, object]:
    """Validate the complete payload and return its decoded manifest."""
    if _definition_object(payload).get("format") != "SparkJobDefinitionV2":
        raise CanaryError("Definition format must be SparkJobDefinitionV2")
    parts = decoded_parts(payload)
    required = {METADATA_PATH, MAIN_PATH}
    missing = sorted(required - parts.keys())
    if missing:
        raise CanaryError("Definition is missing required parts: " + ", ".join(missing))
    if [str(part["path"]) for part in definition_parts(payload)] != sorted(parts):
        raise CanaryError("Definition parts are not in canonical path order")
    assert_no_secrets(payload)
    metadata = _decoded_json_object(parts[METADATA_PATH], "metadata")
    manifest = _decoded_manifest(parts[MAIN_PATH])
    if PLATFORM_PATH in parts:
        _verify_platform_part(parts[PLATFORM_PATH])
    _verify_manifest_digests(manifest)
    _verify_definition_metadata(metadata, manifest, parts)
    _verify_manifest_part_hashes(manifest, parts)
    _verify_manifest_identity(manifest)
    _verify_release_identity(manifest, parts)
    return manifest


def _decoded_json_object(content: bytes, name: str) -> dict[str, object]:
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CanaryError(f"{name} must be UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise CanaryError(f"{name} must contain a JSON object")
    return value


def _decoded_manifest(content: bytes) -> dict[str, object]:
    manifest = _optional_bootstrap_manifest(content)
    if manifest is None:
        raise CanaryError("Bootstrap has no embedded canary manifest")
    return dict(manifest)


def _verify_platform_part(content: bytes) -> None:
    platform = _decoded_json_object(content, PLATFORM_PATH)
    metadata = platform.get("metadata")
    if not isinstance(metadata, Mapping) or (
        metadata.get("type") != "SparkJobDefinition"
        or metadata.get("displayName") != DISPLAY_NAME
        or metadata.get("description") != DEFINITION_DESCRIPTION
    ):
        raise CanaryError("Fabric-generated platform metadata is not the canary")


def _verify_manifest_digests(manifest: Mapping[str, object]) -> None:
    release_digest = manifest.get("releaseDigest")
    if not isinstance(release_digest, str) or not re.fullmatch(
        r"[0-9a-f]{64}", release_digest
    ):
        raise CanaryError("Manifest releaseDigest must be lowercase SHA-256")
    package_identity = manifest.get("packageIdentity")
    if not isinstance(package_identity, str) or not re.fullmatch(
        r"[0-9a-f]{64}", package_identity
    ):
        raise CanaryError("Manifest packageIdentity must be lowercase SHA-256")
    source_archive = manifest.get("sourceArchive")
    if not isinstance(source_archive, Mapping):
        raise CanaryError("Manifest sourceArchive must be an object")
    archive_digest = source_archive.get("sha256")
    if not isinstance(archive_digest, str) or not re.fullmatch(
        r"[0-9a-f]{64}", archive_digest
    ):
        raise CanaryError("Manifest sourceArchive sha256 must be lowercase SHA-256")


def _verify_definition_metadata(
    metadata: Mapping[str, object],
    manifest: Mapping[str, object],
    parts: Mapping[str, bytes],
) -> None:
    release_digest = str(manifest["releaseDigest"])
    package_identity = str(manifest["packageIdentity"])
    expected_metadata = {
        "additionalLakehouseIds": [],
        "additionalLibraryUris": sorted(
            PurePosixPath(path).name
            for path in parts
            if path.startswith("Libs/")
        ),
        "commandLineArguments": _command_line_arguments(
            REQUIRED_RUN_ID,
            release_digest,
            str(manifest.get("projectVersion", "")),
            package_identity,
            REQUIRED_DEFINITION_SHA256,
            str(manifest["sourceArchive"]["sha256"]),
        ),
        "defaultLakehouseArtifactId": LAKEHOUSE_ID,
        "environmentArtifactId": metadata.get("environmentArtifactId"),
        "executableFile": PurePosixPath(MAIN_PATH).name,
        "language": "Python",
        "mainClass": "",
        "retryPolicy": None,
    }
    if metadata != expected_metadata:
        raise CanaryError("SparkJobDefinition metadata violates the canary contract")
    try:
        uuid.UUID(metadata["environmentArtifactId"])
    except (ValueError, TypeError, AttributeError) as error:
        raise CanaryError("environmentArtifactId must be a UUID") from error


def _verify_manifest_part_hashes(
    manifest: Mapping[str, object],
    parts: Mapping[str, bytes],
) -> None:
    expected_hashes = {
        path: _part_hash(path, content)
        for path, content in sorted(parts.items())
        if path not in {MAIN_PATH, PLATFORM_PATH}
    }
    if manifest.get("partHashes") != expected_hashes:
        raise CanaryError("Decoded part hashes do not match the manifest")
    archive, _ = _audited_archive_from_bootstrap(parts[MAIN_PATH])
    if manifest.get("bootstrapSha256") != sha256_bytes(
        build_source_bootstrap(archive)
    ):
        raise CanaryError("Bootstrap hash does not match the manifest")


def _verify_manifest_identity(manifest: Mapping[str, object]) -> None:
    expected_identity = {
        "displayName": DISPLAY_NAME,
        "lakehouseId": LAKEHOUSE_ID,
        "schemaVersion": 1,
        "workspaceId": WORKSPACE_ID,
    }
    for key, expected in expected_identity.items():
        if manifest.get(key) != expected:
            raise CanaryError(f"Manifest {key} does not match the isolated canary")
    for key in ("projectVersion", "sourceCommit"):
        if not isinstance(manifest.get(key), str) or not manifest[key]:
            raise CanaryError(f"Manifest {key} must be a non-empty string")


def _verify_release_identity(
    manifest: Mapping[str, object],
    parts: Mapping[str, bytes],
) -> None:
    package_identity = str(manifest["packageIdentity"])
    archive, observed_source_archive = source_archive_from_bootstrap(
        parts[MAIN_PATH]
    )
    if manifest.get("sourceArchive") != observed_source_archive:
        raise CanaryError("Manifest sourceArchive does not match bootstrap archive")
    recomputed_release = _release_digest(
        build_source_bootstrap(archive),
        {
            path: content
            for path, content in parts.items()
            if path.startswith("Libs/") and path != LEGACY_MANIFEST_PATH
        },
        source_commit=str(manifest["sourceCommit"]),
        project_version=str(manifest["projectVersion"]),
        package_identity=package_identity,
        source_archive_identity=observed_source_archive,
    )
    if manifest["releaseDigest"] != recomputed_release:
        raise CanaryError("Manifest releaseDigest does not match release contents")


def _wheel_names(bundle: str | os.PathLike[str] | bytes | Iterable[str]) -> list[str]:
    if isinstance(bundle, bytes):
        import io

        with zipfile.ZipFile(io.BytesIO(bundle)) as archive:
            return [PurePosixPath(name).name for name in archive.namelist() if name.endswith(".whl")]
    if isinstance(bundle, (str, os.PathLike)):
        path = Path(bundle)
        if path.suffix == ".whl":
            return [path.name]
        if not zipfile.is_zipfile(path):
            raise CanaryError(f"Bundle is not a wheel or zip archive: {path}")
        with zipfile.ZipFile(path) as archive:
            return [PurePosixPath(name).name for name in archive.namelist() if name.endswith(".whl")]
    return [PurePosixPath(name).name for name in bundle if str(name).endswith(".whl")]


def preflight_bundle(
    bundle: str | os.PathLike[str] | bytes | Iterable[str],
) -> list[str]:
    """Require a CPython 3.13 Linux-compatible wheelhouse.

    Pure Python wheels are accepted.  Any cp312 wheel is rejected even if a
    cp313 counterpart is also present, preventing accidental mixed bundles.
    """
    names = sorted(_wheel_names(bundle))
    if not names:
        raise CanaryError("Bundle contains no wheels")
    for name in names:
        normalized = name.lower()
        if re.search(r"(?:^|[-.])cp312(?:[-.]|$)", normalized):
            raise CanaryError(f"CPython 3.12 wheel is forbidden: {name}")
        if "-cp" in normalized and not re.search(r"-cp313(?:t)?-", normalized):
            raise CanaryError(f"Wheel is not built for CPython 3.13: {name}")
        if not normalized.endswith("-any.whl") and not re.search(
            r"(?:manylinux|musllinux|linux)[^/]*\.whl$", normalized
        ):
            raise CanaryError(f"Wheel is not Linux compatible: {name}")
    return names


def redact(value: str, secrets: Iterable[str] = ()) -> str:
    """Redact authorization material from diagnostic text."""
    redacted = re.sub(
        r"(?i)\b(authorization\s*[:=]\s*bearer\s+)[^\s,;]+",
        r"\1<redacted>",
        value,
    )
    redacted = re.sub(
        r"(?i)\b(bearer\s+)[A-Za-z0-9._~+/=-]+",
        r"\1<redacted>",
        redacted,
    )
    redacted = re.sub(
        r"(?i)(client_secret|access_token|sig)=([^&\s]+)",
        r"\1=<redacted>",
        redacted,
    )
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "<redacted>")
    return redacted


class TokenProvider(Protocol):
    def get_token(self) -> str: ...


class StaticTokenProvider:
    """Test-friendly token provider whose repr never reveals the token."""

    def __init__(self, token: str) -> None:
        if not token:
            raise ValueError("token must not be empty")
        self._token = token

    def get_token(self) -> str:
        return self._token

    def __repr__(self) -> str:
        return "StaticTokenProvider(<redacted>)"


class AzureCliTokenProvider:
    def get_token(self) -> str:
        try:
            process = subprocess.run(
                [
                    "az",
                    "account",
                    "get-access-token",
                    "--resource",
                    FABRIC_RESOURCE,
                    "--query",
                    "accessToken",
                    "--output",
                    "tsv",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as error:
            raise CanaryError(
                "Azure CLI was not found; install az and run 'az login', or select "
                "managed identity/service principal authentication"
            ) from error
        except subprocess.CalledProcessError as error:
            raise CanaryError(
                "Azure CLI could not acquire a Fabric token; run 'az login' and "
                "confirm the selected tenant/subscription"
            ) from error
        token = process.stdout.strip()
        if not token:
            raise CanaryError("Azure CLI returned an empty access token")
        return token

    def __repr__(self) -> str:
        return "AzureCliTokenProvider()"


class AzureIdentityTokenProvider:
    """Lazy adapter for Default, managed identity, or service principal auth."""

    def __init__(
        self,
        mode: str,
        *,
        tenant_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> None:
        self._mode = mode
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._credential: object | None = None

    def _build_credential(self) -> object:
        try:
            from azure.identity import (  # type: ignore[import-not-found]
                ClientSecretCredential,
                DefaultAzureCredential,
                ManagedIdentityCredential,
            )
        except ImportError as error:
            raise CanaryError(
                "azure-identity is required for this authentication mode"
            ) from error
        if self._mode == "default":
            return DefaultAzureCredential()
        if self._mode == "managed-identity":
            return ManagedIdentityCredential(client_id=self._client_id)
        if self._mode == "service-principal":
            values = (self._tenant_id, self._client_id, self._client_secret)
            if not all(values):
                raise CanaryError(
                    "Service principal auth requires tenant ID, client ID, and secret"
                )
            return ClientSecretCredential(*values)
        raise ValueError(f"Unknown Azure Identity mode: {self._mode}")

    def get_token(self) -> str:
        if self._credential is None:
            self._credential = self._build_credential()
        try:
            access_token = self._credential.get_token(FABRIC_SCOPE)  # type: ignore[attr-defined]
        except Exception as error:
            raise CanaryError(
                "Azure Identity could not acquire a Fabric token; verify identity "
                "configuration, tenant access, and Fabric workspace roles"
            ) from error
        token = getattr(access_token, "token", None)
        if not isinstance(token, str) or not token:
            raise CanaryError("Azure Identity returned an empty access token")
        return token

    def __repr__(self) -> str:
        return f"AzureIdentityTokenProvider(mode={self._mode!r}, credentials=<redacted>)"


def make_token_provider(
    mode: str,
    *,
    tenant_id: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
) -> TokenProvider:
    if mode == "azure-cli":
        return AzureCliTokenProvider()
    return AzureIdentityTokenProvider(
        mode,
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )


@dataclass(frozen=True)
class HTTPResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes = b""

    def json(self) -> object:
        if not self.body:
            return {}
        try:
            return json.loads(self.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise FabricHTTPError("Fabric returned invalid JSON") from error


class HTTPTransport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> HTTPResponse: ...


class UrllibTransport:
    """Minimal stdlib HTTPS transport."""

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> HTTPResponse:
        request = urllib.request.Request(
            url=url,
            data=body,
            headers=dict(headers),
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return HTTPResponse(
                    response.status,
                    dict(response.headers.items()),
                    response.read(),
                )
        except urllib.error.HTTPError as error:
            return HTTPResponse(
                error.code,
                dict(error.headers.items()) if error.headers else {},
                error.read(),
            )


def _header(headers: Mapping[str, str], name: str) -> str | None:
    lowered = name.lower()
    return next(
        (str(value) for key, value in headers.items() if key.lower() == lowered),
        None,
    )


def _retry_after_seconds(headers: Mapping[str, str], now: Callable[[], float]) -> float:
    value = _header(headers, "Retry-After")
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            parsed = email.utils.parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return max(0.0, parsed.timestamp() - now())
        except (TypeError, ValueError, OverflowError):
            return 1.0


_LOCK_GUARD = threading.Lock()
_THREAD_LOCKS: dict[str, threading.Lock] = {}


class DeploymentLock:
    """Serialize local deployers with a thread lock and Linux advisory lock."""

    def __init__(
        self,
        path: Path | None = None,
        *,
        timeout: float = 30.0,
        poll_interval: float = 0.05,
    ) -> None:
        self.path = (
            path
            if path is not None
            else Path.home() / ".cache" / "people-counter" / "fabric-canary.lock"
        )
        self.timeout = timeout
        self.poll_interval = poll_interval
        self._file: Any = None
        self._thread_lock: threading.Lock | None = None

    def __enter__(self) -> DeploymentLock:
        if self.timeout < 0:
            raise ValueError("lock timeout must not be negative")
        key = str(self.path.resolve())
        with _LOCK_GUARD:
            thread_lock = _THREAD_LOCKS.setdefault(key, threading.Lock())
        if not thread_lock.acquire(timeout=self.timeout):
            raise TimeoutError(f"Timed out acquiring local deployment lock {self.path}")
        self._thread_lock = thread_lock
        try:
            import fcntl

            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._file = self.path.open("a+b")
            os.chmod(self.path, 0o600)
            deadline = time.monotonic() + self.timeout
            while True:
                try:
                    fcntl.flock(
                        self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB
                    )
                    return self
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            f"Timed out acquiring local deployment lock {self.path}"
                        )
                    time.sleep(self.poll_interval)
        except BaseException:
            if self._file is not None:
                self._file.close()
                self._file = None
            thread_lock.release()
            self._thread_lock = None
            raise

    def __exit__(self, *_: object) -> None:
        if self._file is not None:
            import fcntl

            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
            self._file.close()
            self._file = None
        if self._thread_lock is not None:
            self._thread_lock.release()
            self._thread_lock = None


@dataclass(frozen=True)
class RunSubmission:
    item_id: str
    job_instance_id: str
    run_id: str
    response: Mapping[str, object]


class FabricClient:
    """Small Fabric REST client with injectable transport and LRO timing."""

    def __init__(
        self,
        token_provider: TokenProvider,
        *,
        transport: HTTPTransport | None = None,
        api_root: str = FABRIC_API_ROOT,
        timeout: float = 30.0,
        lro_timeout: float = 300.0,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        wall_time: Callable[[], float] = time.time,
        lock_factory: Callable[[], contextlib.AbstractContextManager[object]] = DeploymentLock,
    ) -> None:
        if not api_root.lower().startswith("https://"):
            raise ValueError("Fabric API root must use HTTPS")
        self.token_provider = token_provider
        self.transport = transport or UrllibTransport()
        self.api_root = api_root.rstrip("/")
        self.timeout = timeout
        self.lro_timeout = lro_timeout
        self.sleep = sleep
        self.monotonic = monotonic
        self.wall_time = wall_time
        self.lock_factory = lock_factory

    def _url(self, path_or_url: str) -> str:
        if path_or_url.startswith("https://"):
            return path_or_url
        return f"{self.api_root}/{path_or_url.lstrip('/')}"

    def _request(
        self,
        method: str,
        path_or_url: str,
        *,
        payload: object | None = None,
        expected: Iterable[int] = (200,),
    ) -> HTTPResponse:
        token = self.token_provider.get_token()
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        body = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            body = canonical_json_bytes(payload)
        response = self.transport.request(
            method,
            self._url(path_or_url),
            headers=headers,
            body=body,
            timeout=self.timeout,
        )
        if response.status not in set(expected):
            detail = response.body.decode("utf-8", errors="replace")[:2000]
            raise FabricHTTPError(
                redact(
                    f"Fabric {method} {self._url(path_or_url)} returned "
                    f"HTTP {response.status}: {detail}",
                    (token,),
                )
            )
        return response

    def _json_mapping(self, response: HTTPResponse) -> dict[str, object]:
        value = response.json()
        if not isinstance(value, Mapping):
            raise FabricHTTPError("Fabric JSON response must be an object")
        return dict(value)
    def _finish_lro(self, response: HTTPResponse) -> dict[str, object]:
        if response.status != 202:
            return self._json_mapping(response)
        location = _header(response.headers, "Location")
        operation_id = _header(response.headers, "x-ms-operation-id")
        if not location and operation_id:
            location = f"/operations/{urllib.parse.quote(operation_id, safe='')}"
        if not location:
            raise FabricHTTPError(
                "Fabric accepted an operation without Location or x-ms-operation-id"
            )
        deadline = self.monotonic() + self.lro_timeout
        current = response
        while True:
            if self.monotonic() >= deadline:
                raise TimeoutError("Timed out waiting for Fabric long-running operation")
            self.sleep(_retry_after_seconds(current.headers, self.wall_time))
            current = self._request("GET", location, expected=(200, 202, 429))
            if current.status == 429:
                continue
            result = self._json_mapping(current)
            status = str(result.get("status", "")).lower()
            if status in TERMINAL_FAILED:
                raise FabricHTTPError(
                    "Fabric long-running operation ended with status "
                    + redact(str(result))
                )
            if status in TERMINAL_SUCCEEDED or (
                current.status == 200 and not status
            ):
                resource = (
                    result.get("resourceLocation")
                    or result.get("resultLocation")
                    or _header(current.headers, "Location")
                )
                if isinstance(resource, str) and resource != location:
                    return self._json_mapping(
                        self._request("GET", resource, expected=(200,))
                    )
                return result

    @property
    def _items_root(self) -> str:
        return f"/workspaces/{WORKSPACE_ID}/items"

    @property
    def _definitions_root(self) -> str:
        return f"/workspaces/{WORKSPACE_ID}/sparkJobDefinitions"

    def list_definitions(self) -> list[dict[str, object]]:
        response = self._request(
            "GET",
            f"{self._items_root}?type=SparkJobDefinition",
            expected=(200,),
        )
        body = self._json_mapping(response)
        values = body.get("value", [])
        if not isinstance(values, list) or not all(
            isinstance(item, Mapping) for item in values
        ):
            raise FabricHTTPError("Fabric list response has an invalid value array")
        return [dict(item) for item in values]

    def find_canary(self) -> dict[str, object] | None:
        matches = [
            item
            for item in self.list_definitions()
            if item.get("displayName") == DISPLAY_NAME
            and item.get("type", "SparkJobDefinition") == "SparkJobDefinition"
        ]
        if len(matches) > 1:
            raise CanaryError("Multiple SparkJobDefinitions use the fixed canary name")
        return matches[0] if matches else None

    def get_item(self, item_id: str) -> dict[str, object]:
        return self._json_mapping(
            self._request(
                "GET",
                f"{self._items_root}/{_uuid_segment(item_id, 'item_id')}",
                expected=(200,),
            )
        )

    def get_definition(self, item_id: str) -> dict[str, object]:
        response = self._request(
            "POST",
            f"{self._definitions_root}/{_uuid_segment(item_id, 'item_id')}"
            "/getDefinition?format=SparkJobDefinitionV2",
            expected=(200, 202),
        )
        result = self._finish_lro(response)
        nested = result.get("result")
        if isinstance(nested, Mapping) and isinstance(nested.get("definition"), Mapping):
            result = dict(nested)
        definition = result.get("definition")
        if not isinstance(definition, Mapping):
            raise FabricHTTPError("Fabric definition readback has no definition object")
        normalized = dict(definition)
        normalized["format"] = "SparkJobDefinitionV2"
        parts = normalized.get("parts")
        if isinstance(parts, list):
            normalized["parts"] = sorted(
                parts,
                key=lambda part: (
                    str(part.get("path", ""))
                    if isinstance(part, Mapping)
                    else ""
                ),
            )
        return {"definition": normalized}

    def create_definition(
        self, replacement_payload: Mapping[str, object]
    ) -> dict[str, object]:
        verify_definition(replacement_payload)
        body = {
            "definition": _definition_object(replacement_payload),
            "description": DEFINITION_DESCRIPTION,
            "displayName": DISPLAY_NAME,
        }
        return self._finish_lro(
            self._request(
                "POST", self._definitions_root, payload=body, expected=(201, 202)
            )
        )

    def update_definition(
        self, item_id: str, replacement_payload: Mapping[str, object]
    ) -> dict[str, object]:
        verify_definition(replacement_payload)
        body = {"definition": _definition_object(replacement_payload)}
        response = self._request(
            "POST",
            f"{self._definitions_root}/{_uuid_segment(item_id, 'item_id')}"
            "/updateDefinition",
            payload=body,
            expected=(200, 202),
        )
        return self._finish_lro(response)

    def verify_environment(self, environment_id: str) -> dict[str, object]:
        """Verify published/runtime state when those fields are returned."""
        environment_path = (
            f"/workspaces/{WORKSPACE_ID}/environments/"
            f"{_uuid_segment(environment_id, 'environment_id')}"
        )
        environment = self._json_mapping(
            self._request(
                "GET",
                environment_path,
                expected=(200,),
            )
        )
        published_compute = self._json_mapping(
            self._request(
                "GET",
                f"{environment_path}/sparkcompute?beta=false",
                expected=(200,),
            )
        )
        status_values = _find_values(
            environment, {"publishState", "publishedState"}
        )
        publish_details = environment.get("publishDetails")
        if isinstance(publish_details, Mapping) and publish_details.get("state"):
            status_values.append(publish_details["state"])
        for value in status_values:
            lowered = str(value).lower()
            if lowered not in {"published", "succeeded", "success"}:
                raise CanaryError(f"Fabric Environment is not published: {value}")
        runtime_values = _find_values(
            published_compute, {"runtimeVersion", "sparkRuntimeVersion"}
        )
        if not runtime_values:
            raise CanaryError(
                "Published Fabric Environment settings do not expose a runtime version"
            )
        for value in runtime_values:
            if str(value).strip() not in {"2.0", "2"}:
                raise CanaryError(
                    f"Fabric Environment runtime must be 2.0, observed {value!r}"
                )
        return {
            "environment": environment,
            "publishedSparkCompute": published_compute,
        }

    def deploy(
        self,
        replacement_payload: Mapping[str, object],
        *,
        item_id: str | None = None,
        allow_prune: bool = False,
    ) -> dict[str, object]:
        """Create/update, then require byte-equivalent definition readback."""
        verify_definition(replacement_payload)
        metadata = json.loads(decoded_parts(replacement_payload)[METADATA_PATH])
        self.verify_environment(metadata["environmentArtifactId"])
        with self.lock_factory():
            existing = (
                self.get_item(item_id)
                if item_id is not None
                else self.find_canary()
            )
            if existing is not None and existing.get("displayName") != DISPLAY_NAME:
                raise CanaryError(
                    "Refusing to update an item whose display name is not the canary"
                )
            if existing is None:
                result = self.create_definition(replacement_payload)
                item_id = result.get("id")
                if not isinstance(item_id, str):
                    location_id = result.get("itemId")
                    item_id = location_id if isinstance(location_id, str) else None
                if item_id is None:
                    found = self.find_canary()
                    item_id = found.get("id") if found else None
            else:
                item_id = existing.get("id")
                if not isinstance(item_id, str):
                    raise FabricHTTPError("Existing canary has no item ID")
                live = self.get_definition(item_id)
                if not allow_prune:
                    refuse_unmanaged_prune(live, replacement_payload)
                result = self.update_definition(item_id, replacement_payload)
            if not isinstance(item_id, str):
                raise FabricHTTPError("Fabric create response has no item ID")
            readback = self.get_definition(item_id)
            verify_definition(readback)
            if _decoded_part_hashes(readback) != _decoded_part_hashes(
                replacement_payload
            ):
                raise DefinitionMismatchError(
                    "Fabric definition readback differs from complete replacement"
                )
            return {
                "definitionSha256": definition_hash(replacement_payload),
                "itemId": item_id,
                "operation": result,
            }

    def run(self, item_id: str, *, run_id: str | None = None) -> RunSubmission:
        item = _uuid_segment(item_id, "item_id")
        generated = str(uuid.uuid4()) if run_id is None else _uuid_segment(run_id, "run_id")
        if generated == REQUIRED_RUN_ID:
            raise ValueError("run_id must be a fresh UUID")
        definition = self.get_definition(item)
        manifest = verify_definition(definition)
        payload = {
            "executionData": {
                "commandLineArguments": _command_line_arguments(
                    generated,
                    str(manifest["releaseDigest"]),
                    str(manifest["projectVersion"]),
                    str(manifest["packageIdentity"]),
                    definition_hash(definition),
                    str(manifest["sourceArchive"]["sha256"]),
                )
            }
        }
        response = self._request(
            "POST",
            f"{self._definitions_root}/{item}/jobs/sparkjob/instances",
            payload=payload,
            expected=(202,),
        )
        result = self._json_mapping(response)
        instance_id = result.get("id") or result.get("jobInstanceId")
        if not isinstance(instance_id, str):
            location = _header(response.headers, "Location")
            instance_id = location.rstrip("/").split("/")[-1] if location else None
        if not isinstance(instance_id, str):
            instance_id = _header(response.headers, "x-ms-operation-id")
        if not isinstance(instance_id, str):
            raise FabricHTTPError("Fabric run response has no job instance ID")
        instance_id = _uuid_segment(instance_id, "job_instance_id")
        return RunSubmission(item, instance_id, generated, result)

    def status(self, item_id: str, job_instance_id: str) -> dict[str, object]:
        return self._json_mapping(
            self._request(
                "GET",
                _job_path(item_id, job_instance_id),
                expected=(200,),
            )
        )

    def wait_status(
        self,
        item_id: str,
        job_instance_id: str,
        *,
        poll_timeout: float = 1800.0,
    ) -> dict[str, object]:
        if poll_timeout <= 0:
            raise ValueError("poll_timeout must be positive")
        deadline = self.monotonic() + poll_timeout
        while True:
            response = self._request(
                "GET",
                _job_path(item_id, job_instance_id),
                expected=(200, 429),
            )
            if response.status == 429:
                if self.monotonic() >= deadline:
                    raise TimeoutError("Timed out after Fabric job throttling")
                self.sleep(_retry_after_seconds(response.headers, self.wall_time))
                continue
            result = self._json_mapping(response)
            status = str(result.get("status", "")).lower()
            if status in TERMINAL_SUCCEEDED:
                return {**result, "terminal": True, "succeeded": True}
            if status in TERMINAL_FAILED:
                return {
                    **result,
                    "terminal": True,
                    "succeeded": False,
                    "failure": result.get("failureReason")
                    or result.get("error")
                    or "Fabric job failed without failure details",
                }
            if self.monotonic() >= deadline:
                raise TimeoutError("Timed out waiting for Fabric job instance")
            self.sleep(_retry_after_seconds(response.headers, self.wall_time))

    def cancel(self, item_id: str, job_instance_id: str) -> dict[str, object]:
        response = self._request(
            "POST",
            f"{_job_path(item_id, job_instance_id)}/cancel",
            expected=(200, 202),
        )
        result = self._json_mapping(response)
        if result:
            return result
        return {
            "jobInstanceId": _uuid_segment(job_instance_id, "job_instance_id"),
            "status": "CancelRequested",
        }


def _find_values(value: object, keys: set[str]) -> list[object]:
    found: list[object] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            if key in keys and child is not None:
                found.append(child)
            found.extend(_find_values(child, keys))
    elif isinstance(value, list):
        for child in value:
            found.extend(_find_values(child, keys))
    return found


def _uuid_segment(value: str, name: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as error:
        raise ValueError(f"{name} must be a UUID") from error
    return str(parsed)


def _job_path(item_id: str, instance_id: str) -> str:
    return (
        f"/workspaces/{WORKSPACE_ID}/items/{_uuid_segment(item_id, 'item_id')}"
        f"/jobs/instances/{_uuid_segment(instance_id, 'job_instance_id')}"
    )


def validate_result(
    result: Mapping[str, object],
    *,
    success: Mapping[str, object],
    pointer: Mapping[str, object],
    records: Sequence[Mapping[str, object]],
    pointer_rows: Sequence[Mapping[str, object]],
    expected_run_id: str,
    expected_release_digest: str,
    expected_definition_hash: str,
    expected_package_identity: str,
    expected_source_archive_sha256: str,
    expected_project_version: str,
    expected_workspace_id: str = WORKSPACE_ID,
    expected_lakehouse_id: str = LAKEHOUSE_ID,
    expected_write_scope: str = WRITE_SCOPE,
) -> dict[str, object]:
    """Validate all live canary artifacts rather than trusting one JSON file."""
    identities = {
        "workspaceId": expected_workspace_id,
        "lakehouseId": expected_lakehouse_id,
        "writeScope": expected_write_scope,
        "runId": _uuid_segment(expected_run_id, "expected run_id"),
    }
    for name, document in (
        ("success", success),
        ("pointer", pointer),
        ("result", result),
    ):
        for key, expected in identities.items():
            if document.get(key) != expected:
                raise CanaryError(f"{name} {key} identity mismatch")
    expected_staging = {
        "pointerDeltaPath": "pointer_delta",
        "recordsPath": "attempts/records",
        "runRoot": f"Files/{expected_write_scope}/run={expected_run_id}",
    }
    if pointer.get("stagingIdentity") != expected_staging:
        raise CanaryError("Pointer staging identity mismatch")
    record_values = [dict(record) for record in records]
    records_hash = _runtime_json_hash(record_values)
    pointer_hash = _runtime_json_hash(dict(pointer))
    result_hash = _runtime_json_hash(dict(result))
    if [dict(row) for row in pointer_rows] != [dict(pointer)]:
        raise CanaryError("Pointer Delta rows do not match pointer.json")
    for name, document in (
        ("pointer", pointer),
        ("result", result),
        ("success", success),
    ):
        if document.get("recordsSha256") != records_hash:
            raise CanaryError(f"{name} records hash mismatch")
        if document.get("recordCount") != len(record_values):
            raise CanaryError(f"{name} record count mismatch")
    if result.get("pointerSha256") != pointer_hash:
        raise CanaryError("Result pointer hash mismatch")
    if success.get("pointerSha256") != pointer_hash:
        raise CanaryError("Success pointer hash mismatch")
    if success.get("resultSha256") != result_hash:
        raise CanaryError("Success result hash mismatch")
    if result.get("status") != "SUCCEEDED" or success.get("status") != "SUCCEEDED":
        raise CanaryError("Canary artifact status is not SUCCEEDED")
    expected_result = {
        "definitionSha256": expected_definition_hash,
        "releaseDigest": expected_release_digest,
        "packageIdentity": expected_package_identity,
        "sourceArchiveSha256": expected_source_archive_sha256,
    }
    for key, expected in expected_result.items():
        if result.get(key) != expected:
            raise CanaryError(f"Result {key} mismatch")
    runtime = result.get("runtime")
    if not isinstance(runtime, Mapping):
        raise CanaryError("Result runtime is missing")
    _validate_result_runtime(
        runtime,
        expected_release_digest=expected_release_digest,
        expected_project_version=expected_project_version,
    )
    provenance = _provenance_from_records(
        record_values,
        expected_release_digest=expected_release_digest,
        expected_package_identity=expected_package_identity,
        expected_project_version=expected_project_version,
        runtime=runtime,
    )
    if result.get("provenance") != provenance:
        raise CanaryError("Result provenance does not match live records")
    return dict(result)


def _runtime_json_hash(value: object) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return sha256_bytes(encoded)


def _validate_result_runtime(
    runtime: Mapping[str, object],
    *,
    expected_release_digest: str,
    expected_project_version: str,
) -> None:
    required = {"python", "spark", "java", "package_version", "release_digest"}
    if not required.issubset(runtime):
        raise CanaryError("Result runtime is incomplete")
    if runtime.get("release_digest") != expected_release_digest:
        raise CanaryError("Result runtime release identity mismatch")
    if runtime.get("package_version") != expected_project_version:
        raise CanaryError("Result runtime package version mismatch")
    versions = {
        "python": (3, 13),
        "spark": (4, 1),
    }
    for name, expected in versions.items():
        match = re.search(r"(\d+)\.(\d+)", str(runtime[name]))
        if match is None or tuple(map(int, match.groups())) != expected:
            raise CanaryError(f"Result runtime {name} version mismatch")
    if not re.search(r"(?:^|\D)21(?:\.|\D|$)", str(runtime["java"])):
        raise CanaryError("Result runtime Java version mismatch")


def _provenance_from_records(
    records: Sequence[Mapping[str, object]],
    *,
    expected_release_digest: str,
    expected_package_identity: str,
    expected_project_version: str,
    runtime: Mapping[str, object],
) -> dict[str, object]:
    executors: set[str] = set()
    tasks: set[tuple[int, int, int]] = set()
    terminals = 0
    for record in records:
        expected_record = {
            "status": "SUCCEEDED",
            "release_digest": expected_release_digest,
            "package_identity": expected_package_identity,
            "project_version": expected_project_version,
            "package_version": expected_project_version,
        }
        for key, expected in expected_record.items():
            if record.get(key) != expected:
                raise CanaryError(f"Live record {key} mismatch")
        _validate_executor_record_runtime(record)
        identity = record.get("executor_identity")
        host = record.get("executor_host")
        if (
            not isinstance(identity, str)
            or not isinstance(host, str)
            or identity.rpartition("@")[2] != host
        ):
            raise CanaryError("Live record executor identity mismatch")
        task_values = tuple(
            record.get(key)
            for key in ("stage_id", "partition_id", "task_attempt_id")
        )
        if any(type(value) is not int or int(value) < 0 for value in task_values):
            raise CanaryError("Live record task provenance is invalid")
        task = tuple(int(value) for value in task_values)
        if task in tasks:
            raise CanaryError("Live records contain duplicate task provenance")
        tasks.add(task)
        executors.add(identity)
        if record.get("record_type") == "video_result":
            terminals += 1
    if terminals != len(records):
        raise CanaryError("Live record terminal count mismatch")
    return {
        "executorIdentities": sorted(executors),
        "taskIdentities": [list(task) for task in sorted(tasks)],
        "terminalCount": terminals,
    }


def _validate_executor_record_runtime(record: Mapping[str, object]) -> None:
    versions = {
        "executor_python": (3, 13),
        "executor_spark": (4, 1),
    }
    for name, expected in versions.items():
        match = re.search(r"(\d+)\.(\d+)", str(record.get(name, "")))
        if match is None or tuple(map(int, match.groups())) != expected:
            raise CanaryError(f"Live record {name} version mismatch")
    if not re.search(r"(?:^|\D)21(?:\.|\D|$)", str(record.get("executor_java", ""))):
        raise CanaryError("Live record executor Java version mismatch")
    optional_versions = {
        "executor_scala": (2, 13),
        "executor_delta": (4, 2),
    }
    for name, expected in optional_versions.items():
        value = record.get(name)
        if value is None:
            continue
        match = re.search(r"(\d+)\.(\d+)", str(value))
        if match is None or tuple(map(int, match.groups())) != expected:
            raise CanaryError(f"Live record {name} version mismatch")


def _discover_commit() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _project_version() -> str:
    try:
        return importlib.metadata.version("people-counter")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _add_auth_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workspace-id", default=WORKSPACE_ID)
    parser.add_argument(
        "--auth",
        choices=("azure-cli", "default", "managed-identity", "service-principal"),
        default="azure-cli",
    )
    parser.add_argument("--tenant-id", default=os.environ.get("AZURE_TENANT_ID"))
    parser.add_argument("--client-id", default=os.environ.get("AZURE_CLIENT_ID"))
    parser.add_argument(
        "--client-secret", default=os.environ.get("AZURE_CLIENT_SECRET")
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pc-fabric-canary",
        description="Build and operate the isolated Fabric SJD V2 canary.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser("build", help="build a deterministic definition")
    build.add_argument("--main", type=Path)
    build.add_argument("--environment-id", required=True)
    build.add_argument("--lib", action="append", type=Path, default=[])
    build.add_argument("--bundle", type=Path)
    build.add_argument("--source-commit")
    build.add_argument("--project-version")
    build.add_argument("--output", required=True, type=Path)

    inspect = subparsers.add_parser("inspect-live", help="read live canary definition")
    _add_auth_arguments(inspect)
    inspect.add_argument("--item-id")

    deploy = subparsers.add_parser("deploy", help="deploy a built definition")
    _add_auth_arguments(deploy)
    deploy.add_argument("--environment-id", required=True)
    deploy.add_argument("--item-id")
    deploy.add_argument("--main", type=Path)
    deploy.add_argument("--lib", action="append", type=Path, default=[])
    deploy.add_argument("--bundle", type=Path)
    deploy.add_argument("--source-commit")
    deploy.add_argument("--project-version")
    deploy.add_argument("--allow-prune", action="store_true")
    deploy.add_argument("--snapshot-output", type=Path)

    run = subparsers.add_parser("run", help="start the canary with a fresh run ID")
    _add_auth_arguments(run)
    run.add_argument("--item-id", required=True)
    run.add_argument("--run-id")

    status = subparsers.add_parser("status", help="read a canary job status")
    _add_auth_arguments(status)
    status.add_argument("--item-id", required=True)
    status.add_argument("--job-instance-id", required=True)
    status.add_argument("--wait", action="store_true")
    status.add_argument("--poll-timeout", type=float, default=1800.0)

    cancel = subparsers.add_parser("cancel", help="cancel a canary job")
    _add_auth_arguments(cancel)
    cancel.add_argument("--item-id", required=True)
    cancel.add_argument("--job-instance-id", required=True)

    validate = subparsers.add_parser(
        "validate-result", help="validate the complete canary artifact set"
    )
    validate.add_argument("--result", required=True, type=Path)
    validate.add_argument("--success", required=True, type=Path)
    validate.add_argument("--pointer", required=True, type=Path)
    validate.add_argument("--records", required=True, type=Path)
    validate.add_argument("--pointer-rows", required=True, type=Path)
    validate.add_argument("--run-id", required=True)
    validate.add_argument("--release-digest", required=True)
    validate.add_argument("--definition-sha256", required=True)
    validate.add_argument("--package-identity", required=True)
    validate.add_argument("--source-archive-sha256", required=True)
    validate.add_argument("--project-version", required=True)
    return parser


def _client_from_args(args: argparse.Namespace) -> FabricClient:
    if args.workspace_id != WORKSPACE_ID:
        raise CanaryError(
            f"This canary is confined to workspace {WORKSPACE_ID}; "
            f"received {args.workspace_id!r}"
        )
    provider = make_token_provider(
        args.auth,
        tenant_id=args.tenant_id,
        client_id=args.client_id,
        client_secret=args.client_secret,
    )
    return FabricClient(provider)


def _load_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise CanaryError(f"{path} must contain a JSON object")
    return value


def _load_json_rows(path: Path) -> list[dict[str, object]]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list) or not all(
        isinstance(row, dict) for row in value
    ):
        raise CanaryError(f"{path} must contain a JSON array of objects")
    return value


def _print_json(value: object) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def _build_command(args: argparse.Namespace) -> dict[str, object]:
    if args.bundle is not None:
        preflight_bundle(args.bundle)
    libs = {f"Libs/{path.name}": path.read_bytes() for path in args.lib}
    payload = build_definition(
        _main_source(args.main),
        args.environment_id,
        libs=libs,
        source_commit=args.source_commit or _discover_commit(),
        project_version=args.project_version or _project_version(),
    )
    args.output.write_bytes(canonical_json_bytes(payload) + b"\n")
    return {
        "definitionSha256": definition_hash(payload),
        "output": str(args.output),
    }


def _main_source(path: Path | None) -> bytes:
    if path is not None:
        return path.read_bytes()
    package_root = Path(__file__).resolve().parent
    return build_source_bootstrap(
        build_source_archive(package_root)
    )


def _inspect_command(
    client: FabricClient, args: argparse.Namespace
) -> dict[str, object]:
    if args.item_id:
        return client.get_definition(args.item_id)
    item = client.find_canary()
    if item is None or not isinstance(item.get("id"), str):
        raise CanaryError("Fixed canary item was not found")
    return client.get_definition(str(item["id"]))


def _run_command(
    client: FabricClient, args: argparse.Namespace
) -> dict[str, object]:
    submission = client.run(args.item_id, run_id=args.run_id)
    return {
        "itemId": submission.item_id,
        "jobInstanceId": submission.job_instance_id,
        "runId": submission.run_id,
        "response": submission.response,
    }


def _deploy_command(
    client: FabricClient, args: argparse.Namespace
) -> dict[str, object]:
    _preflight_optional_bundle(args.bundle)
    existing = _selected_live_item(client, args.item_id)
    live, snapshot = _snapshot_live_definition(client, existing, args)
    libs = _deployment_libs(live, args.lib, allow_prune=args.allow_prune)
    replacement = _deployment_replacement(args, libs)
    _validate_live_prune(live, replacement, allow_prune=args.allow_prune)
    result = client.deploy(
        replacement,
        item_id=args.item_id,
        allow_prune=args.allow_prune,
    )
    return _deployment_result(result, existing, snapshot)


def _preflight_optional_bundle(bundle: Path | None) -> None:
    if bundle is not None:
        preflight_bundle(bundle)


def _deployment_replacement(
    args: argparse.Namespace,
    libs: Mapping[str, bytes],
) -> dict[str, object]:
    return build_definition(
        _main_source(args.main),
        args.environment_id,
        libs=libs,
        source_commit=args.source_commit or _discover_commit(),
        project_version=args.project_version or _project_version(),
    )


def _validate_live_prune(
    live: Mapping[str, object] | None,
    replacement: Mapping[str, object],
    *,
    allow_prune: bool,
) -> None:
    if live is not None and not allow_prune:
        refuse_unmanaged_prune(live, replacement)


def _deployment_result(
    result: dict[str, object],
    existing: Mapping[str, object] | None,
    snapshot: Path | None,
) -> dict[str, object]:
    if existing is not None:
        result["snapshot"] = str(snapshot)
    return result


def _selected_live_item(
    client: FabricClient,
    item_id: str | None,
) -> dict[str, object] | None:
    return client.get_item(item_id) if item_id is not None else client.find_canary()


def _snapshot_live_definition(
    client: FabricClient,
    existing: Mapping[str, object] | None,
    args: argparse.Namespace,
) -> tuple[dict[str, object] | None, Path | None]:
    if existing is None:
        return None, None
    live_id = existing.get("id")
    if not isinstance(live_id, str):
        raise CanaryError("Existing canary item has no ID")
    live = client.get_definition(live_id)
    audit_definition_for_snapshot(live)
    snapshot = args.snapshot_output or _default_snapshot_path(live_id)
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(
            snapshot,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_json_bytes(live) + b"\n")
    except FileExistsError as error:
        raise CanaryError(f"Snapshot already exists: {snapshot}") from error
    return live, snapshot


def _default_snapshot_path(item_id: str) -> Path:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("build") / "fabric-canary-snapshots" / f"{item_id}-{stamp}.json"


def _deployment_libs(
    live: Mapping[str, object] | None,
    paths: Sequence[Path],
    *,
    allow_prune: bool,
) -> dict[str, bytes]:
    supplied = {f"Libs/{path.name}": path.read_bytes() for path in paths}
    if live is None or allow_prune:
        return supplied
    return {**retained_libs(live), **supplied}


def _live_command(
    client: FabricClient, args: argparse.Namespace
) -> dict[str, object]:
    handlers: dict[str, Callable[[], dict[str, object]]] = {
        "cancel": lambda: client.cancel(args.item_id, args.job_instance_id),
        "deploy": lambda: _deploy_command(client, args),
        "inspect-live": lambda: _inspect_command(client, args),
        "run": lambda: _run_command(client, args),
        "status": lambda: (
            client.wait_status(
                args.item_id,
                args.job_instance_id,
                poll_timeout=args.poll_timeout,
            )
            if args.wait
            else client.status(args.item_id, args.job_instance_id)
        ),
    }
    try:
        handler = handlers[args.command]
    except KeyError as error:
        raise AssertionError(f"Unhandled command: {args.command}") from error
    return handler()


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        if args.command == "build":
            result = _build_command(args)
        elif args.command == "validate-result":
            result = validate_result(
                _load_json(args.result),
                success=_load_json(args.success),
                pointer=_load_json(args.pointer),
                records=_load_json_rows(args.records),
                pointer_rows=_load_json_rows(args.pointer_rows),
                expected_run_id=args.run_id,
                expected_release_digest=args.release_digest,
                expected_definition_hash=args.definition_sha256,
                expected_package_identity=args.package_identity,
                expected_source_archive_sha256=args.source_archive_sha256,
                expected_project_version=args.project_version,
            )
        else:
            result = _live_command(_client_from_args(args), args)
        _print_json(result)
        return 0
    except (CanaryError, OSError, ValueError) as error:
        print(
            json.dumps(
                {
                    "event": "fabric_canary_tool_failed",
                    "errorType": type(error).__name__,
                    "message": redact(str(error)),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
