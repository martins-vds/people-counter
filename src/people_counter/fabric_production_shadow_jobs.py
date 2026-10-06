"""Deterministic production-shadow Spark entry and SJD support.

This module is deliberately deployment-free.  It binds definitions to the
reviewed Fabric artifacts and provides create-only runtime diagnostics for the
installed-wheel entry modules.
"""

from __future__ import annotations

import base64
import hashlib
import importlib.metadata
import json
import os
import platform
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence
from uuid import uuid4

from people_counter.fabric_candidate_a import (
    ENVIRONMENT_ID,
    FABRIC_RUNTIME,
    LAKEHOUSE_ID,
    PRODUCTION_SHADOW_FILES_ROOT,
    WORKSPACE_ID,
)


PACKAGE_DISTRIBUTION = "people-counter"
PACKAGE_VERSION = "0.9.11"
PYTHON_RUNTIME = "3.13"
SPARK_RUNTIME = "4.1.1"
JAVA_RUNTIME = "21"
DIAGNOSTICS_ROOT = f"{PRODUCTION_SHADOW_FILES_ROOT}diagnostics"
SJD_NAMES = ("control", "process", "reconcile")

_JOB_MODULES = {
    "control": "people_counter.fabric_production_shadow_control.main",
    "process": "people_counter.fabric_production_shadow_process.main",
    "reconcile": "people_counter.fabric_production_shadow_reconcile.main",
}
_SAFE_SEGMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._=-]{0,127}\Z")


class CreateOnlyTextWriter(Protocol):
    """The sole file operation needed by production-shadow diagnostics."""

    def create_text(self, path: str, content: str) -> None: ...


class NotebookUtilsCreateOnlyWriter:
    """Create and read back one text file without overwrite semantics."""

    @staticmethod
    def _fs() -> Any:
        import notebookutils

        return notebookutils.fs

    def create_text(self, path: str, content: str) -> None:
        _require_diagnostic_path(path)
        if self._fs().exists(path):
            raise FileExistsError(path)
        if self._fs().put(path, content, False) is False:
            raise OSError(f"OneLake create failed for {path}")
        observed = self._fs().head(path, 16 * 1024 * 1024)
        observed_text = (
            observed.decode("utf-8") if isinstance(observed, bytes) else str(observed)
        )
        if observed_text != content:
            raise OSError(f"OneLake diagnostic readback differs for {path}")


def canonical_json_bytes(value: object) -> bytes:
    """Encode the canonical JSON representation used by exports and evidence."""

    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def fixed_provenance() -> dict[str, dict[str, str]]:
    """Return the reviewed package and Fabric runtime provenance."""

    return {
        "package": {
            "distribution": PACKAGE_DISTRIBUTION,
            "version": PACKAGE_VERSION,
        },
        "runtime": {
            "fabric": FABRIC_RUNTIME,
            "java": JAVA_RUNTIME,
            "python": PYTHON_RUNTIME,
            "spark": SPARK_RUNTIME,
        },
    }


def observed_provenance() -> dict[str, object]:
    """Collect provenance without creating a Spark session or calling Fabric."""

    try:
        package_version: str | None = importlib.metadata.version(
            PACKAGE_DISTRIBUTION
        )
    except importlib.metadata.PackageNotFoundError:
        package_version = None
    observed: dict[str, object] = {
        "fabric": os.environ.get("FABRIC_RUNTIME_VERSION"),
        "java": None,
        "package_version": package_version,
        "python": platform.python_version(),
        "spark": None,
    }
    pyspark_sql = sys.modules.get("pyspark.sql")
    spark_session = getattr(pyspark_sql, "SparkSession", None)
    if spark_session is None:
        return observed
    try:
        spark = spark_session.getActiveSession()
        if spark is not None:
            observed["spark"] = str(spark.version)
            observed["java"] = str(
                spark.sparkContext._jvm.java.lang.System.getProperty(
                    "java.version"
                )
            )
    except Exception:
        pass
    return observed


def validate_observed_provenance(observed: Mapping[str, object]) -> None:
    """Fail before any job write when installed/runtime provenance differs."""

    if observed.get("package_version") != PACKAGE_VERSION:
        raise RuntimeError("installed people-counter package version differs")
    fabric = observed.get("fabric")
    if fabric is not None and str(fabric) != FABRIC_RUNTIME:
        raise RuntimeError("Fabric runtime version differs")
    spark = observed.get("spark")
    if fabric is not None and (
        spark is None or observed.get("java") is None
    ):
        raise RuntimeError("Fabric Spark/Java runtime provenance is missing")
    if spark is None:
        return
    if not str(spark).startswith(SPARK_RUNTIME):
        raise RuntimeError("Spark runtime version differs")
    if not str(observed.get("python")).startswith(PYTHON_RUNTIME):
        raise RuntimeError("Python runtime version differs")
    if not str(observed.get("java")).startswith(JAVA_RUNTIME):
        raise RuntimeError("Java runtime version differs")


def _safe_segment(value: str, label: str) -> str:
    if _SAFE_SEGMENT.fullmatch(value) is None:
        raise ValueError(f"unsafe production-shadow {label}")
    return value


def _require_diagnostic_path(path: str) -> str:
    prefix = f"{DIAGNOSTICS_ROOT}/"
    if (
        not path.startswith(prefix)
        or "\\" in path
        or any(part in {"", ".", ".."} for part in path.split("/"))
    ):
        raise ValueError("production-shadow diagnostic escaped its fixed root")
    return path


@dataclass(slots=True)
class ShadowDiagnostics:
    """Create-only, monotonically staged evidence for one Spark invocation."""

    job: str
    writer: CreateOnlyTextWriter = field(
        default_factory=NotebookUtilsCreateOnlyWriter
    )
    invocation_id: str = field(default_factory=lambda: str(uuid4()))
    _sequence: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        if self.job not in SJD_NAMES:
            raise ValueError(f"unsupported production-shadow job {self.job!r}")
        _safe_segment(self.invocation_id, "invocation_id")

    def mark(
        self,
        stage: str,
        status: str,
        detail: Mapping[str, object] | None = None,
    ) -> tuple[str, str]:
        """Create one immutable stage and return its path and SHA-256."""

        _safe_segment(stage, "diagnostic stage")
        payload = {
            "artifact_binding": {
                "environment_id": ENVIRONMENT_ID,
                "lakehouse_id": LAKEHOUSE_ID,
                "workspace_id": WORKSPACE_ID,
            },
            "detail": dict(detail or {}),
            "fixed_provenance": fixed_provenance(),
            "invocation_id": self.invocation_id,
            "job": self.job,
            "namespace": "PRODUCTION_SHADOW",
            "observed_provenance": observed_provenance(),
            "schema": "people-counter-production-shadow-stage-v1",
            "sequence": self._sequence,
            "stage": stage,
            "status": status,
        }
        content = canonical_json_bytes(payload).decode("utf-8") + "\n"
        path = _require_diagnostic_path(
            f"{DIAGNOSTICS_ROOT}/job={self.job}/"
            f"invocation={self.invocation_id}/stages/"
            f"{self._sequence:02d}-{stage}.json"
        )
        self.writer.create_text(path, content)
        self._sequence += 1
        return path, hashlib.sha256(content.encode("utf-8")).hexdigest()


def run_shadow_entry(
    job: str,
    runner: Callable[[Sequence[str] | None], int],
    argv: Sequence[str] | None = None,
    *,
    diagnostics: ShadowDiagnostics | None = None,
) -> int:
    """Run one installed-wheel entry with create-only stage evidence."""

    evidence = diagnostics or ShadowDiagnostics(job)
    if evidence.job != job:
        raise ValueError("diagnostic job differs from production-shadow entry")
    validate_observed_provenance(observed_provenance())
    evidence.mark("started", "RUNNING")
    try:
        result = runner(argv)
    except BaseException as error:
        detail = {
            "exception_message_sha256": hashlib.sha256(
                str(error).encode("utf-8", errors="replace")
            ).hexdigest(),
            "exception_type": type(error).__name__,
        }
        try:
            evidence.mark("failed", "FAILED", detail)
        except BaseException as diagnostic_error:
            if hasattr(error, "add_note"):
                error.add_note(
                    "production-shadow diagnostic failure: "
                    f"{type(diagnostic_error).__name__}"
                )
        raise
    status = "SUCCEEDED" if result == 0 else "FAILED"
    stage = "completed" if result == 0 else "failed"
    evidence.mark(stage, status, {"exit_code": result})
    return result


def _encode_part(path: str, content: bytes) -> dict[str, str]:
    if (
        path.startswith("/")
        or "\\" in path
        or any(part in {"", ".", ".."} for part in path.split("/"))
    ):
        raise ValueError(f"unsafe production-shadow definition path {path!r}")
    return {
        "path": path,
        "payload": base64.b64encode(content).decode("ascii"),
        "payloadType": "InlineBase64",
    }


def thin_main_source(job: str) -> bytes:
    """Return an installed-wheel-only wrapper for one fixed SJD name."""

    try:
        target = _JOB_MODULES[job]
    except KeyError as error:
        raise ValueError(
            f"unsupported production-shadow job {job!r}"
        ) from error
    module, function = target.rsplit(".", 1)
    return (
        '"""Generated Candidate A production-shadow SJD entry point."""\n\n'
        f"from {module} import {function} as main\n\n"
        'if __name__ == "__main__":\n'
        "    raise SystemExit(main())\n"
    ).encode("utf-8")


def build_sjd_v2_definition(job: str) -> dict[str, object]:
    """Build one fixed definition with no saved arguments or extra libraries."""

    if job not in SJD_NAMES:
        raise ValueError(f"unsupported production-shadow job {job!r}")
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
    return {
        "definition": {
            "format": "SparkJobDefinitionV2",
            "parts": [
                _encode_part("Main/main.py", thin_main_source(job)),
                _encode_part(
                    "SparkJobDefinitionV1.json",
                    canonical_json_bytes(metadata),
                ),
            ],
        }
    }


def sjd_definition_bytes(job: str) -> bytes:
    """Return stable, newline-terminated definition bytes."""

    return canonical_json_bytes(build_sjd_v2_definition(job)) + b"\n"


def export_sjd_definitions(destination: str | Path) -> dict[str, dict[str, str]]:
    """Create deterministic exports, refusing symlinks or differing content."""

    root = Path(destination)
    if root.absolute().resolve(strict=False) != root.absolute():
        raise ValueError("SJD export destination cannot traverse symlinks")
    if root.exists() and (root.is_symlink() or not root.is_dir()):
        raise ValueError("SJD export destination must be a real directory")
    root.mkdir(parents=True, exist_ok=True)
    exported: dict[str, dict[str, str]] = {}
    for job in SJD_NAMES:
        path = root / f"{job}.SparkJobDefinitionV2.json"
        content = sjd_definition_bytes(job)
        if path.is_symlink():
            raise ValueError("refusing to overwrite an SJD symlink")
        if path.exists() and path.read_bytes() != content:
            raise FileExistsError(f"refusing to replace differing export {path}")
        if not path.exists():
            path.write_bytes(content)
        exported[job] = {
            "path": str(path),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
    return exported
