"""Deterministic, installed-wheel-only migration SJD definition generation."""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from typing import Any

from people_counter.fabric_production_migration import ENVIRONMENT_ID, LAKEHOUSE_ID


SJD_EXPORT_NAME = "migration.SparkJobDefinitionV2.json"


def migration_main_source() -> bytes:
    return r'''"""Generated production migration SJD entry point with fail-safe diagnostics."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import re
import sys
import traceback
from pathlib import Path


_ROOT = "Files/people-counter/migrations/people_counter_ca_0001"
_SCHEMA = "people-counter-production-migration-diagnostic-v1"
_SENSITIVE = (
    "--inventory-hmac-key",
    "--lease-token",
    "--recovery-token",
    "--safety-token",
)
_SAFE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def _argument(arguments, name):
    for index, value in enumerate(arguments):
        if value == name and index + 1 < len(arguments):
            return str(arguments[index + 1])
        if value.startswith(name + "="):
            return value.split("=", 1)[1]
    return None


def _identity(arguments, name, label):
    value = _argument(arguments, name)
    if value and _SAFE.fullmatch(value):
        return value
    if value:
        digest = hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()
        return "invalid-" + label + "-" + digest[:16]
    return "unbound-" + label


def _redacted(arguments):
    result = []
    hide_next = False
    for raw in arguments:
        value = str(raw)
        if hide_next:
            result.append("<redacted>")
            hide_next = False
            continue
        matched = next(
            (name for name in _SENSITIVE if value.startswith(name + "=")), None
        )
        if matched is not None:
            result.append(matched + "=<redacted>")
            continue
        result.append(value)
        hide_next = value in _SENSITIVE
    return result


def _sanitize(value, secrets):
    text = str(value)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    for name in _SENSITIVE:
        text = re.sub(
            r"(" + re.escape(name) + r"(?:=|\s+))([^\s,'\"\]\)]+)",
            r"\1<redacted>",
            text,
        )
    text = re.sub(
        r"\beyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}"
        r"(?:\.[A-Za-z0-9_-]{8,})?\b",
        "<redacted-jwt>",
        text,
    )
    return text[:65536]


def _bytes(value):
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _write(path, content, allow_identical=False):
    import notebookutils

    if not path.startswith(_ROOT + "/") or ".." in path.split("/") or "\\" in path:
        raise RuntimeError("diagnostic path escaped fixed migration root")
    if notebookutils.fs.exists(path):
        observed = notebookutils.fs.head(path, 16 * 1024 * 1024)
        observed = observed if isinstance(observed, bytes) else str(observed).encode("utf-8")
        if not allow_identical or observed != content:
            raise FileExistsError(path)
    else:
        if notebookutils.fs.put(path, content.decode("utf-8"), False) is False:
            raise OSError("OneLake diagnostic create failed")
    observed = notebookutils.fs.head(path, 16 * 1024 * 1024)
    observed = observed if isinstance(observed, bytes) else str(observed).encode("utf-8")
    if observed != content:
        raise OSError("OneLake diagnostic readback differs")
    return hashlib.sha256(observed).hexdigest()


def _runtime():
    value = {
        "fabric_runtime": os.environ.get("FABRIC_RUNTIME_VERSION"),
        "java": None,
        "python": platform.python_version(),
        "spark": None,
    }
    try:
        SparkSession = getattr(sys.modules.get("pyspark.sql"), "SparkSession")
        spark = SparkSession.getActiveSession()
        if spark is not None:
            value["spark"] = str(spark.version)
            value["java"] = str(
                spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
            )
    except Exception:
        pass
    return value


def _binding():
    try:
        SparkSession = getattr(sys.modules.get("pyspark.sql"), "SparkSession")
        spark = SparkSession.getActiveSession()
        if spark is None:
            return None
        result = {}
        keys = {
            "workspace_id": (
                "spark.microsoft.fabric.workspace.id",
                "trident.workspace.id",
            ),
            "lakehouse_id": (
                "spark.microsoft.fabric.lakehouse.id",
                "trident.lakehouse.id",
            ),
            "environment_id": (
                "spark.microsoft.fabric.environment.id",
                "trident.environment.id",
            ),
        }
        for field, choices in keys.items():
            values = set()
            for key in choices:
                for getter in (
                    spark.conf.get,
                    spark.sparkContext.getConf().get,
                ):
                    try:
                        observed = getter(key)
                    except Exception:
                        continue
                    if observed not in (None, ""):
                        values.add(str(observed))
            try:
                import notebookutils

                context_keys = {
                    "workspace_id": ("currentWorkspaceId", "workspaceId"),
                    "lakehouse_id": ("defaultLakehouseId", "lakehouseId"),
                    "environment_id": ("environmentId", "currentEnvironmentId"),
                }
                for key in context_keys[field]:
                    observed = notebookutils.runtime.context.get(key)
                    if observed not in (None, ""):
                        values.add(str(observed))
            except Exception:
                pass
            if len(values) == 1:
                result[field] = values.pop()
        return result
    except Exception:
        return None


def _package():
    try:
        version = importlib.metadata.version("people-counter")
    except importlib.metadata.PackageNotFoundError:
        version = None
    try:
        source = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    except Exception:
        source = None
    return {"source_sha256": source, "version": version}


def _run():
    arguments = list(sys.argv[1:])
    redacted = _redacted(arguments)
    run_id = _identity(arguments, "--run-id", "run")
    invocation_id = _identity(arguments, "--invocation-id", "invocation")
    root = _ROOT + "/diagnostics/" + run_id + "/" + invocation_id
    marker = {
        "invocation_id": invocation_id,
        "run_id": run_id,
        "schema": _SCHEMA,
        "sequence": 0,
        "stage": "bootstrap",
    }
    error = None
    try:
        _write(root + "/stages/00-bootstrap.json", _bytes(marker), True)
        from people_counter.fabric_production_migration_live import main

        return main(arguments)
    except BaseException as caught:
        error = caught
        secrets = tuple(
            value
            for value in (_argument(arguments, name) for name in _SENSITIVE)
            if value
        )
        formatted = "".join(
            traceback.TracebackException.from_exception(
                caught, capture_locals=False
            ).format(chain=True)
        )
        envelope = {
            "artifact_binding": _binding(),
            "exception": {
                "message": _sanitize(caught, secrets),
                "traceback": _sanitize(formatted, secrets),
                "type": type(caught).__name__,
            },
            "handler": "wrapper",
            "input_hashes": {
                "redacted_arguments_sha256": hashlib.sha256(
                    _bytes(redacted)
                ).hexdigest()
            },
            "invocation_id": invocation_id,
            "package": _package(),
            "redacted_arguments": redacted,
            "run_id": run_id,
            "runtime": _runtime(),
            "schema": _SCHEMA,
            "stage": "bootstrap",
            "status": "failed",
        }
        try:
            path = root + "/wrapper-failure.json"
            digest = _write(path, _bytes(envelope))
            print(
                "diagnostic_evidence path=" + path + " sha256=" + digest,
                file=sys.stderr,
            )
        except BaseException as diagnostic_error:
            note = (
                "diagnostic-write failure: "
                + type(diagnostic_error).__name__
                + ": "
                + _sanitize(diagnostic_error, secrets)
            )
            print(note, file=sys.stderr)
            if hasattr(error, "add_note"):
                error.add_note(note)
        raise


if __name__ == "__main__":
    raise SystemExit(_run())
'''.encode("utf-8")


def _part(path: str, payload: bytes) -> dict[str, str]:
    return {
        "path": path,
        "payload": base64.b64encode(payload).decode("ascii"),
        "payloadType": "InlineBase64",
    }


def build_migration_sjd_definition() -> dict[str, object]:
    """Return the one safe definition; saved arguments are intentionally empty."""

    metadata = {
        "additionalLakehouseIds": [],
        "additionalLibraryUris": [],
        "commandLineArguments": "",
        "defaultLakehouseArtifactId": LAKEHOUSE_ID,
        "environmentArtifactId": ENVIRONMENT_ID,
        "executableFile": "main.py",
        "language": "Python",
        "mainClass": "",
        "retryPolicy": None,
    }
    metadata_bytes = json.dumps(
        metadata,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return {
        "definition": {
            "format": "SparkJobDefinitionV2",
            "parts": [
                _part("Main/main.py", migration_main_source()),
                _part("SparkJobDefinitionV1.json", metadata_bytes),
            ],
        }
    }


def migration_sjd_bytes() -> bytes:
    return (
        json.dumps(
            build_migration_sjd_definition(),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def export_migration_sjd(destination: str | Path) -> dict[str, str]:
    """Create the deterministic local export and refuse differing overwrite."""

    root = Path(destination)
    if root.absolute().resolve(strict=False) != root.absolute():
        raise ValueError("SJD export destination cannot traverse symlinks")
    if root.exists() and (not root.is_dir() or root.is_symlink()):
        raise ValueError("SJD export destination must be a real directory")
    root.mkdir(parents=True, exist_ok=True)
    path = root / SJD_EXPORT_NAME
    content = migration_sjd_bytes()
    if path.is_symlink():
        raise ValueError("refusing to overwrite an SJD symlink")
    if path.exists() and path.read_bytes() != content:
        raise FileExistsError(f"refusing to replace differing SJD export {path}")
    if not path.exists():
        path.write_bytes(content)
    return {
        "path": str(path),
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def decoded_metadata(definition: dict[str, Any]) -> dict[str, Any]:
    """Test/review helper returning the embedded V1 metadata."""

    parts = definition["definition"]["parts"]
    encoded = next(
        item["payload"]
        for item in parts
        if item["path"] == "SparkJobDefinitionV1.json"
    )
    return json.loads(base64.b64decode(encoded))
