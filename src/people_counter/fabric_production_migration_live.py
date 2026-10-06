"""Reviewed live Fabric driver for the fixed Candidate A migration.

The module accepts no SQL, table, or filesystem path from an operator.  Live
state comes from Spark plus a fresh HMAC-authenticated invocation inventory;
all OneLake paths are derived from the fixed migration ID and a safe run ID.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import importlib.metadata
import json
import math
import os
import platform
import re
import sys
import time
import traceback
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Protocol, TextIO
from uuid import uuid4

from people_counter.fabric_production_migration import (
    ENVIRONMENT_ID,
    LAKEHOUSE_ID,
    MIGRATION_ID,
    JOURNAL_TABLE,
    TABLE_ALLOWLIST,
    WORKSPACE_ID,
    ApplyStatus,
    LeaseSnapshot,
    LegacyObjectSnapshot,
    MigrationPlan,
    PreconditionError,
    SparkMigrationBackend,
    WriterQuiescenceProof,
    apply_plan,
    build_plan,
    canonical_json,
    evidence_hash,
    status_report,
    token_hash,
    validate_apply_plan,
    verify_backend,
)
from people_counter.fabric_reflex_definition import (
    REFLEX_ID,
    ReflexRuleSnapshot,
    parse_reflex_rule_definition,
)


INVENTORY_SCHEMA = "people-counter-production-migration-inventory-v2"
EVIDENCE_ROOT = f"Files/people-counter/migrations/{MIGRATION_ID}"
DIAGNOSTIC_SCHEMA = "people-counter-production-migration-diagnostic-v1"
EXPECTED_PACKAGE_VERSION = "0.9.11"
DEFAULT_LAKEHOUSE_NAME = "people_counter_dev"
SJD_JOB_TIMEOUT_SECONDS = 3600.0
SJD_COLD_START_BUDGET_SECONDS = 900.0
INVENTORY_MAX_AGE_SECONDS = (
    SJD_JOB_TIMEOUT_SECONDS + SJD_COLD_START_BUDGET_SECONDS
)
FAILED_RECOVERY_RUN_ID = "plan-20261004-03"
FAILED_RECOVERY_PLAN_SHA256 = (
    "4b65faa1925888b6da5f19ca592b5d8e1d879b86494b728587a732aee3b1c06e"
)
FAILED_RECOVERY_JOB_ID = "7dc7a799-22a3-4a1e-8b3a-8655d2aefbfa"
FAILED_RECOVERY_INVOCATION_ID = "e80a3c11-fdff-4f7a-9af8-12d80fe4807a"
FAILED_RECOVERY_DIAGNOSTIC_SHA256 = (
    "03de1347cdbe36cd57aa4baed1485893b32201cb7b2982b98dfb01852740b118"
)
RECOVERY_SCHEMA = "people-counter-production-lock-recovery-v2"
LOCK_TABLE = "people_counter_control_writer"
DISPATCHER_LEASE_TABLE = "people_counter_dispatcher_leases"
REGISTRATION_LEASE_TABLE = "people_counter_registration_leases"
WORK_TABLE = "people_counter_video_work"
COMMITTED_VIEWS = (
    "people_counter_line_counts_committed",
    "people_counter_runs_committed",
    "people_counter_telemetry_committed",
)
GOLD_TABLES = (
    "people_counter_gold_dim_camera",
    "people_counter_gold_dim_date",
    "people_counter_gold_dim_location",
    "people_counter_gold_dim_model_config",
    "people_counter_gold_dim_time",
    "people_counter_gold_dim_video",
    "people_counter_gold_flow_hour",
    "people_counter_gold_flow_minute",
    "people_counter_gold_operations_hour",
    "people_counter_gold_video",
)
LEGACY_TABLES = (
    "people_counter_dispatcher_leases",
    "people_counter_event_receipts",
    "people_counter_line_count_attempts",
    "people_counter_processing_benchmarks",
    "people_counter_reconciliation_findings",
    "people_counter_registration_leases",
    "people_counter_replay_requests",
    "people_counter_telemetry_attempts",
    "people_counter_video_attempts",
    "people_counter_video_work",
    "people_counter_worker_event_receipts",
    "people_counter_worker_events",
) + GOLD_TABLES
_ACTIVE_JOB_STATES = frozenset({"inprogress", "queued", "running", "starting"})
_TERMINAL_JOB_STATES = frozenset({"cancelled", "completed", "failed"})
_ACTIVE_WORK_STATES = frozenset({"LEASED", "RUNNING", "STAGING", "WRITING"})
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_OWNER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9@._-]{0,127}$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_SENSITIVE_ARGUMENTS = frozenset(
    {
        "--inventory-hmac-key",
        "--lease-token",
        "--recovery-token",
        "--safety-token",
    }
)
_STAGES = (
    "bootstrap",
    "args",
    "inventory-read",
    "inventory-verify",
    "spark-binding",
    "catalog-evidence",
    "plan-build",
    "plan-write",
    "pre-lock-validated",
    "lock-acquired",
    "operation-loop",
)
_RUNTIME_BINDING_CONF = {
    "workspace_id": (
        "spark.microsoft.fabric.workspace.id",
        "spark.trident.workspace.id",
        "trident.workspace.id",
    ),
    "lakehouse_id": (
        "spark.microsoft.fabric.lakehouse.id",
        "spark.trident.lakehouse.id",
        "trident.lakehouse.id",
    ),
    "environment_id": (
        "spark.microsoft.fabric.environment.id",
        "spark.fabric.environment.id",
        "spark.trident.environment.id",
        "trident.environment.id",
    ),
}
_RUNTIME_CONTEXT_KEYS = {
    "workspace_id": ("currentWorkspaceId", "workspaceId"),
    "lakehouse_id": ("defaultLakehouseId", "lakehouseId"),
    "environment_id": ("environmentId", "currentEnvironmentId"),
}


class LiveMigrationError(RuntimeError):
    """The reviewed live driver refused or could not prove an operation."""


class EvidenceFiles(Protocol):
    def exists(self, path: str) -> bool: ...

    def read_bytes(self, path: str) -> bytes: ...

    def create_bytes(self, path: str, content: bytes) -> None: ...


class NotebookUtilsEvidenceFiles:
    """Create-only access to the single fixed migration evidence root."""

    @staticmethod
    def _fs() -> Any:
        import notebookutils

        return notebookutils.fs

    @staticmethod
    def _path(path: str) -> str:
        candidate = PurePosixPath(path)
        if (
            "\\" in path
            or "\x00" in path
            or candidate.is_absolute()
            or str(candidate) != path
            or not path.startswith(EVIDENCE_ROOT + "/")
            or ".." in candidate.parts
        ):
            raise LiveMigrationError("evidence path is outside the fixed migration root")
        return path

    def exists(self, path: str) -> bool:
        return bool(self._fs().exists(self._path(path)))

    def read_bytes(self, path: str) -> bytes:
        value = self._fs().head(self._path(path), 100 * 1024 * 1024)
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
        raise OSError("notebookutils.fs.head returned unsupported content")

    def create_bytes(self, path: str, content: bytes) -> None:
        checked = self._path(path)
        if self.exists(checked):
            raise FileExistsError(checked)
        if self._fs().put(checked, content.decode("utf-8"), False) is False:
            raise OSError(f"OneLake create failed for {checked}")
        if self.read_bytes(checked) != content:
            raise OSError(f"OneLake create readback differs for {checked}")


def _safe_run_id(value: str) -> str:
    if _RUN_ID.fullmatch(value) is None:
        raise LiveMigrationError("run ID must be a safe 1-128 character identifier")
    return value


def _safe_owner(value: str) -> str:
    if _OWNER.fullmatch(value) is None:
        raise LiveMigrationError("owner must be a safe 1-128 character identifier")
    return value


def inventory_path(run_id: str) -> str:
    return f"{EVIDENCE_ROOT}/inventory/{_safe_run_id(run_id)}.json"


def plan_path(run_id: str) -> str:
    return f"{EVIDENCE_ROOT}/plans/{_safe_run_id(run_id)}.json"


def result_path(run_id: str, plan_sha256: str) -> str:
    if _HEX64.fullmatch(plan_sha256) is None:
        raise LiveMigrationError("plan hash must be lowercase SHA-256")
    return f"{EVIDENCE_ROOT}/runs/{_safe_run_id(run_id)}/{plan_sha256}.json"


def report_path(run_id: str, invocation_id: str, command: str) -> str:
    if command not in {"plan", "verify", "status"}:
        raise LiveMigrationError("report command is not supported")
    return (
        f"{EVIDENCE_ROOT}/reports/{_safe_run_id(run_id)}/"
        f"{_safe_run_id(invocation_id)}/{command}.json"
    )


def diagnostic_root(run_id: str, invocation_id: str) -> str:
    return (
        f"{EVIDENCE_ROOT}/diagnostics/{_safe_run_id(run_id)}/"
        f"{_safe_run_id(invocation_id)}"
    )


def diagnose_path(run_id: str, invocation_id: str) -> str:
    return f"{diagnostic_root(run_id, invocation_id)}/diagnose.json"


def failure_path(run_id: str, invocation_id: str, handler: str = "live") -> str:
    if handler not in {"live", "wrapper"}:
        raise LiveMigrationError("failure handler is not supported")
    return f"{diagnostic_root(run_id, invocation_id)}/{handler}-failure.json"


def recovery_root(job_id: str, invocation_id: str) -> str:
    if (
        job_id != FAILED_RECOVERY_JOB_ID
        or invocation_id != FAILED_RECOVERY_INVOCATION_ID
    ):
        raise LiveMigrationError("lock recovery identity is not the reviewed failure")
    return f"{EVIDENCE_ROOT}/recovery/{job_id}/{invocation_id}/stable-v2"


def recovery_plan_path(job_id: str, invocation_id: str) -> str:
    return f"{recovery_root(job_id, invocation_id)}/plan.json"


def recovery_review_path(job_id: str, invocation_id: str) -> str:
    return f"{recovery_root(job_id, invocation_id)}/review.json"


def recovery_result_path(job_id: str, invocation_id: str) -> str:
    return f"{recovery_root(job_id, invocation_id)}/result.json"


def stage_marker_path(run_id: str, invocation_id: str, stage: str) -> str:
    try:
        sequence = _STAGES.index(stage)
    except ValueError as error:
        raise LiveMigrationError("diagnostic stage is not supported") from error
    return (
        f"{diagnostic_root(run_id, invocation_id)}/stages/"
        f"{sequence:02d}-{stage}.json"
    )


def _canonical_bytes(value: object) -> bytes:
    return canonical_json(value).encode("utf-8") + b"\n"


def _create_verified(
    files: EvidenceFiles,
    path: str,
    content: bytes,
    *,
    allow_identical: bool = False,
) -> str:
    """Create evidence, require exact readback, and return its SHA-256."""

    if files.exists(path):
        if not allow_identical or files.read_bytes(path) != content:
            raise FileExistsError(path)
    else:
        files.create_bytes(path, content)
    readback = files.read_bytes(path)
    if readback != content:
        raise OSError(f"evidence readback differs for {path}")
    return hashlib.sha256(readback).hexdigest()


def _redacted_arguments(arguments: Sequence[str]) -> list[str]:
    """Redact both ``--secret value`` and ``--secret=value`` spellings."""

    redacted: list[str] = []
    hide_next = False
    for raw in arguments:
        value = str(raw)
        if hide_next:
            redacted.append("<redacted>")
            hide_next = False
            continue
        matched = next(
            (name for name in _SENSITIVE_ARGUMENTS if value.startswith(name + "=")),
            None,
        )
        if matched is not None:
            redacted.append(matched + "=<redacted>")
            continue
        redacted.append(value)
        hide_next = value in _SENSITIVE_ARGUMENTS
    return redacted


def _argument_value(arguments: Sequence[str], name: str) -> str | None:
    for index, value in enumerate(arguments):
        if value == name and index + 1 < len(arguments):
            return str(arguments[index + 1])
        if value.startswith(name + "="):
            return value.split("=", 1)[1]
    return None


def _diagnostic_identity(arguments: Sequence[str]) -> tuple[str, str]:
    def confined(value: str | None, label: str) -> str:
        if value and _RUN_ID.fullmatch(value) is not None:
            return value
        if value:
            digest = hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()
            return f"invalid-{label}-{digest[:16]}"
        return f"unbound-{label}"

    return (
        confined(_argument_value(arguments, "--run-id"), "run"),
        confined(_argument_value(arguments, "--invocation-id"), "invocation"),
    )


def _known_secrets(args: argparse.Namespace | None) -> tuple[str, ...]:
    if args is None:
        return ()
    return tuple(
        str(value)
        for value in (
            getattr(args, "inventory_hmac_key", None),
            getattr(args, "lease_token", None),
            getattr(args, "recovery_token", None),
            getattr(args, "safety_token", None),
        )
        if isinstance(value, str) and value
    )


def _sanitize_text(value: object, secrets: Sequence[str]) -> str:
    text = str(value)
    for secret in secrets:
        text = text.replace(secret, "<redacted>")
    for name in _SENSITIVE_ARGUMENTS:
        text = re.sub(
            rf"({re.escape(name)}(?:=|\s+))([^\s,'\"\]\)]+)",
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


def _source_fingerprint() -> str | None:
    try:
        return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    except OSError:
        return None


def _package_version() -> str | None:
    try:
        return importlib.metadata.version("people-counter")
    except importlib.metadata.PackageNotFoundError:
        return None


def _runtime_evidence(spark_session: Any | None) -> dict[str, object]:
    evidence: dict[str, object] = {
        "fabric_runtime": os.environ.get("FABRIC_RUNTIME_VERSION"),
        "java": None,
        "python": platform.python_version(),
        "spark": None,
    }
    if spark_session is None:
        return evidence
    evidence["spark"] = str(getattr(spark_session, "version", "")) or None
    try:
        evidence["java"] = str(
            spark_session.sparkContext._jvm.java.lang.System.getProperty(
                "java.version"
            )
        )
    except Exception:
        pass
    return evidence


@dataclass
class DiagnosticRecorder:
    """Create-only stage and terminal evidence for one invocation."""

    files: EvidenceFiles
    run_id: str
    invocation_id: str
    redacted_arguments: tuple[str, ...]
    input_hashes: dict[str, str]
    artifact_binding: Mapping[str, str] | None = None
    last_stage: str | None = None

    @classmethod
    def create(
        cls,
        files: EvidenceFiles,
        arguments: Sequence[str],
    ) -> DiagnosticRecorder:
        redacted = tuple(_redacted_arguments(arguments))
        run_id, invocation_id = _diagnostic_identity(arguments)
        return cls(
            files=files,
            run_id=run_id,
            invocation_id=invocation_id,
            redacted_arguments=redacted,
            input_hashes={
                "redacted_arguments_sha256": hashlib.sha256(
                    _canonical_bytes(list(redacted))
                ).hexdigest()
            },
        )

    def mark(self, stage: str) -> str:
        sequence = _STAGES.index(stage)
        if self.last_stage is not None and sequence < _STAGES.index(self.last_stage):
            raise LiveMigrationError("diagnostic stages cannot move backwards")
        marker = {
            "invocation_id": self.invocation_id,
            "run_id": self.run_id,
            "schema": DIAGNOSTIC_SCHEMA,
            "sequence": sequence,
            "stage": stage,
        }
        path = stage_marker_path(self.run_id, self.invocation_id, stage)
        digest = _create_verified(
            self.files, path, _canonical_bytes(marker), allow_identical=True
        )
        self.last_stage = stage
        return digest

    def failure_envelope(
        self,
        error: BaseException,
        *,
        args: argparse.Namespace | None,
        spark_session: Any | None,
        handler: str = "live",
    ) -> dict[str, object]:
        secrets = _known_secrets(args)
        formatted = "".join(
            traceback.TracebackException.from_exception(
                error, capture_locals=False
            ).format(chain=True)
        )
        return {
            "artifact_binding": (
                dict(self.artifact_binding) if self.artifact_binding is not None else None
            ),
            "exception": {
                "message": _sanitize_text(error, secrets),
                "traceback": _sanitize_text(formatted, secrets),
                "type": type(error).__name__,
            },
            "handler": handler,
            "input_hashes": dict(sorted(self.input_hashes.items())),
            "invocation_id": self.invocation_id,
            "package": {
                "source_sha256": _source_fingerprint(),
                "version": _package_version(),
            },
            "redacted_arguments": list(self.redacted_arguments),
            "run_id": self.run_id,
            "runtime": _runtime_evidence(spark_session),
            "schema": DIAGNOSTIC_SCHEMA,
            "stage": self.last_stage,
            "status": "failed",
        }

    def write_failure(
        self,
        error: BaseException,
        *,
        args: argparse.Namespace | None,
        spark_session: Any | None,
        handler: str = "live",
    ) -> tuple[str, str]:
        path = failure_path(self.run_id, self.invocation_id, handler)
        digest = _create_verified(
            self.files,
            path,
            _canonical_bytes(
                self.failure_envelope(
                    error,
                    args=args,
                    spark_session=spark_session,
                    handler=handler,
                )
            ),
        )
        return path, digest


def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        if isinstance(value, datetime) and value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def _row_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "asDict"):
        return dict(row.asDict(recursive=True))
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row)


@dataclass(frozen=True)
class InvocationInventory:
    payload: Mapping[str, Any]
    payload_sha256: str
    signature_sha256: str
    captured_at: float
    expires_at: float
    active_run_ids: tuple[str, ...]
    active_writer_ids: tuple[str, ...]
    stopped_writer_ids: tuple[str, ...]
    reflex: ReflexRuleSnapshot

    @classmethod
    def from_bytes(
        cls,
        content: bytes,
        *,
        hmac_key: bytes,
        now: float,
    ) -> InvocationInventory:
        payload, payload_sha256, signature = _authenticated_payload(content, hmac_key)
        _validate_inventory_identity(payload)
        captured, expires = _inventory_window(payload, now)
        active_runs = _active_inventory_runs(payload)
        active_writers, stopped_writers = _writer_schedule_state(payload)
        reflex = _inventory_reflex(payload)
        if reflex.enabled:
            active_writers = tuple(sorted((*active_writers, f"reflex:{REFLEX_ID}")))
        else:
            stopped_writers = tuple(sorted((*stopped_writers, f"reflex:{REFLEX_ID}")))
        return cls(
            payload=payload,
            payload_sha256=payload_sha256,
            signature_sha256=signature,
            captured_at=captured,
            expires_at=expires,
            active_run_ids=active_runs,
            active_writer_ids=active_writers,
            stopped_writer_ids=stopped_writers,
            reflex=reflex,
        )

    @property
    def passed(self) -> bool:
        return not self.active_run_ids and not self.active_writer_ids and not self.reflex.enabled

    def to_dict(self) -> dict[str, object]:
        return {
            "active_run_ids": list(self.active_run_ids),
            "active_writer_ids": list(self.active_writer_ids),
            "captured_at": self.captured_at,
            "expires_at": self.expires_at,
            "passed": self.passed,
            "payload_sha256": self.payload_sha256,
            "reflex": self.reflex.to_dict(),
            "signature_sha256": self.signature_sha256,
            "stopped_writer_ids": list(self.stopped_writer_ids),
        }


def _finite_time(value: object, name: str) -> float:
    if isinstance(value, bool):
        raise LiveMigrationError(f"inventory {name} must be a finite timestamp")
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise LiveMigrationError(f"inventory {name} must be a finite timestamp") from error
    if not math.isfinite(result) or result < 0:
        raise LiveMigrationError(f"inventory {name} must be a finite timestamp")
    return result


def _authenticated_payload(
    content: bytes, hmac_key: bytes
) -> tuple[Mapping[str, Any], str, str]:
    try:
        envelope = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LiveMigrationError("inventory is not valid UTF-8 JSON") from error
    fields = {"payload", "payload_sha256", "signature_sha256"}
    if not isinstance(envelope, Mapping) or set(envelope) != fields:
        raise LiveMigrationError("inventory envelope fields differ from the contract")
    payload = envelope["payload"]
    if not isinstance(payload, Mapping):
        raise LiveMigrationError("inventory payload must be an object")
    payload_bytes = canonical_json(payload).encode("utf-8")
    payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()
    signature = hmac.new(hmac_key, payload_bytes, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(str(envelope["payload_sha256"]), payload_sha256):
        raise LiveMigrationError("inventory payload hash mismatch")
    if not hmac.compare_digest(str(envelope["signature_sha256"]), signature):
        raise LiveMigrationError("inventory signature mismatch")
    return payload, payload_sha256, signature


def _validate_inventory_identity(payload: Mapping[str, Any]) -> None:
    if payload.get("schema") != INVENTORY_SCHEMA:
        raise LiveMigrationError("inventory schema is not supported")
    expected = {
        "environment_id": ENVIRONMENT_ID,
        "lakehouse_id": LAKEHOUSE_ID,
        "workspace_id": WORKSPACE_ID,
    }
    if payload.get("artifact_binding") != expected:
        raise LiveMigrationError("inventory artifact binding mismatch")


def _inventory_window(
    payload: Mapping[str, Any], now: float
) -> tuple[float, float]:
    captured = _finite_time(payload.get("captured_at"), "captured_at")
    expires = _finite_time(payload.get("expires_at"), "expires_at")
    if captured > now or now > expires or now - captured > INVENTORY_MAX_AGE_SECONDS:
        raise LiveMigrationError("inventory is stale, expired, or future-dated")
    return captured, expires


def _active_inventory_runs(payload: Mapping[str, Any]) -> tuple[str, ...]:
    jobs = _objects(payload.get("fabric_jobs"), "fabric_jobs")
    identifiers = [str(item.get("run_id", "")) for item in jobs]
    if any(not value for value in identifiers) or len(set(identifiers)) != len(identifiers):
        raise LiveMigrationError("Fabric job run IDs must be nonempty and unique")
    states = [str(item.get("state", "")).lower() for item in jobs]
    if any(
        value not in _ACTIVE_JOB_STATES | _TERMINAL_JOB_STATES
        for value in states
    ):
        raise LiveMigrationError("Fabric job states must be recognized and explicit")
    return tuple(
        sorted(
            str(item["run_id"])
            for item in jobs
            if str(item.get("state", "")).lower() in _ACTIVE_JOB_STATES
        )
    )


def _writer_schedule_state(
    payload: Mapping[str, Any],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    schedules = _objects(payload.get("writer_schedules"), "writer_schedules")
    identifiers = [str(item.get("schedule_id", "")) for item in schedules]
    if any(not value for value in identifiers) or len(set(identifiers)) != len(identifiers):
        raise LiveMigrationError("writer schedule IDs must be nonempty and unique")
    if any(type(item.get("enabled")) is not bool for item in schedules):
        raise LiveMigrationError("writer schedule enabled state must be boolean")
    if any(
        _HEX64.fullmatch(str(item.get("definition_sha256", ""))) is None
        for item in schedules
    ):
        raise LiveMigrationError("writer schedule definition hash is invalid")
    active = tuple(
        sorted(str(item["schedule_id"]) for item in schedules if item["enabled"])
    )
    stopped = tuple(
        sorted(str(item["schedule_id"]) for item in schedules if not item["enabled"])
    )
    return active, stopped


def _inventory_reflex(payload: Mapping[str, Any]) -> ReflexRuleSnapshot:
    if payload.get("reflex_id") != REFLEX_ID:
        raise LiveMigrationError("inventory Reflex artifact ID is not exact")
    encoded = payload.get("reflex_definition_base64")
    if not isinstance(encoded, str):
        raise LiveMigrationError("inventory has no Reflex definition bytes")
    try:
        reflex_bytes = base64.b64decode(encoded, validate=True)
    except ValueError as error:
        raise LiveMigrationError("Reflex definition is not valid base64") from error
    return parse_reflex_rule_definition(reflex_bytes)


def _objects(value: object, name: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(item, Mapping) for item in value):
        raise LiveMigrationError(f"inventory {name} must be a list of objects")
    return list(value)


class SparkEvidenceReader:
    """Fixed-name, read-only Spark evidence callbacks for the live backend."""

    def __init__(self, spark_session: Any, *, clock: Callable[[], float]) -> None:
        if spark_session is None:
            raise ValueError("spark_session is required")
        self.spark = spark_session
        self.clock = clock

    def _rows(self, name: str) -> list[dict[str, Any]]:
        return [_row_dict(row) for row in self.spark.table(name).collect()]

    def _exists(self, name: str) -> bool:
        return bool(self.spark.catalog.tableExists(name))

    def _schema_sha256(self, name: str) -> str:
        fields = self.spark.table(name).schema.fields
        return evidence_hash(
            [
                {
                    "data_type": str(field.dataType.simpleString()).lower(),
                    "name": field.name,
                    "nullable": bool(field.nullable),
                }
                for field in fields
            ]
        )

    def _version(self, name: str) -> int:
        rows = self.spark.sql(f"DESCRIBE HISTORY `{name}` LIMIT 1").collect()
        if len(rows) != 1:
            raise LiveMigrationError(f"Delta history is ambiguous for {name}")
        return int(_row_dict(rows[0])["version"])

    def _canonical_rows_sha256(self, name: str) -> tuple[int, str]:
        frame = self.spark.table(name)
        columns = tuple(sorted(str(column) for column in frame.columns))
        iterator = frame.select(*columns).orderBy(*columns).toLocalIterator()
        digest = hashlib.sha256()
        count = 0
        for row in iterator:
            value = {key: _json_value(item) for key, item in _row_dict(row).items()}
            digest.update(canonical_json(value).encode("utf-8"))
            digest.update(b"\n")
            count += 1
        return count, digest.hexdigest()

    def control_writer_row(self) -> Mapping[str, Any]:
        if not self._exists(LOCK_TABLE):
            raise LiveMigrationError(f"{LOCK_TABLE} is missing")
        rows = [
            row for row in self._rows(LOCK_TABLE) if row.get("lock_name") == "global"
        ]
        if len(rows) != 1:
            raise LiveMigrationError("control writer global row is missing or duplicate")
        return rows[0]

    def control_owner(self, *, migration_owner_id: str | None = None) -> str | None:
        owner = self.control_writer_row().get("owner_id")
        if owner is None or owner == migration_owner_id:
            return None
        return str(owner)

    def active_leases(self) -> tuple[str, ...]:
        now = float(self.clock())
        active: list[str] = []
        for name in (DISPATCHER_LEASE_TABLE, REGISTRATION_LEASE_TABLE):
            if not self._exists(name):
                raise LiveMigrationError(f"{name} is missing")
            for row in self._rows(name):
                owner = row.get("owner_id")
                expiry = row.get("expires_at")
                epoch = expiry.timestamp() if isinstance(expiry, datetime) else float(expiry)
                if owner not in (None, "") and epoch > now:
                    active.append(f"{name}:{row.get('lock_name')}:{owner}")
        if not self._exists(WORK_TABLE):
            raise LiveMigrationError(f"{WORK_TABLE} is missing")
        for row in self._rows(WORK_TABLE):
            if str(row.get("status")) in _ACTIVE_WORK_STATES:
                expiry = row.get("lease_expires_at")
                epoch = (
                    expiry.timestamp()
                    if isinstance(expiry, datetime)
                    else None if expiry is None else float(expiry)
                )
                if epoch is None or epoch > now:
                    active.append(f"{WORK_TABLE}:{row.get('work_id')}:{row.get('status')}")
        return tuple(sorted(active))

    def legacy_objects(self) -> tuple[LegacyObjectSnapshot, ...]:
        values: list[LegacyObjectSnapshot] = []
        for name in LEGACY_TABLES:
            if not self._exists(name):
                values.append(LegacyObjectSnapshot(name, "table", False))
                continue
            count, content = self._canonical_rows_sha256(name)
            values.append(
                LegacyObjectSnapshot(
                    name=name,
                    object_type="table",
                    exists=True,
                    schema_sha256=self._schema_sha256(name),
                    row_count=count,
                    version=self._version(name),
                    content_sha256=content,
                )
            )
        for name in COMMITTED_VIEWS:
            if not self._exists(name):
                values.append(LegacyObjectSnapshot(name, "view", False))
                continue
            count, content = self._canonical_rows_sha256(name)
            values.append(
                LegacyObjectSnapshot(
                    name=name,
                    object_type="view",
                    exists=True,
                    schema_sha256=self._schema_sha256(name),
                    row_count=count,
                    content_sha256=content,
                )
            )
        return tuple(sorted(values))

    def committed_pointer_sha256(self) -> str:
        if not self._exists(WORK_TABLE):
            raise LiveMigrationError(f"{WORK_TABLE} is missing")
        rows = [
            {
                "committed_attempt_id": row.get("committed_attempt_id"),
                "status": row.get("status"),
                "work_id": row.get("work_id"),
            }
            for row in self._rows(WORK_TABLE)
            if row.get("committed_attempt_id") not in (None, "")
        ]
        return evidence_hash(sorted(rows, key=lambda row: str(row["work_id"])))

    def committed_view_fingerprints(self) -> tuple[tuple[str, str], ...]:
        objects = {item.name: item for item in self.legacy_objects()}
        return tuple(
            (
                name,
                evidence_hash(objects[name].to_dict()),
            )
            for name in COMMITTED_VIEWS
        )

    def gold_sha256(self) -> str:
        objects = {item.name: item for item in self.legacy_objects()}
        return evidence_hash([objects[name].to_dict() for name in GOLD_TABLES])


def migration_owner_id(owner: str, run_id: str, lease_token: str) -> str:
    return (
        f"migration:{_safe_owner(owner)}:{_safe_run_id(run_id)}:"
        f"{token_hash(lease_token)[:24]}"
    )


def _runtime_conf_values(
    spark_session: Any, keys: Sequence[str]
) -> dict[str, str]:
    values: dict[str, str] = {}
    for key in keys:
        for source, getter in (
            ("session", lambda name: spark_session.conf.get(name)),
            (
                "context",
                lambda name: spark_session.sparkContext.getConf().get(name),
            ),
        ):
            try:
                value = getter(key)
            except Exception:
                continue
            if value not in (None, ""):
                values[f"{source}:{key}"] = str(value)
    return values


def _runtime_context_values(field: str) -> dict[str, str]:
    try:
        import notebookutils

        context = notebookutils.runtime.context
    except Exception:
        return {}
    values: dict[str, str] = {}
    for key in _RUNTIME_CONTEXT_KEYS[field]:
        try:
            value = context.get(key)
        except Exception:
            try:
                value = context[key]
            except Exception:
                continue
        if value not in (None, ""):
            values[f"context:{key}"] = str(value)
    return values


def observed_artifact_binding(spark_session: Any) -> dict[str, str]:
    """Read actual fixed artifact IDs from the Fabric Spark runtime."""

    observed: dict[str, str] = {}
    for field, keys in _RUNTIME_BINDING_CONF.items():
        sources = {
            **_runtime_conf_values(spark_session, keys),
            **_runtime_context_values(field),
        }
        values = set(sources.values())
        if len(values) != 1:
            raise LiveMigrationError(
                f"Fabric runtime {field} is missing or ambiguous"
            )
        observed[field] = values.pop()
    return observed


def _diagnostic_check(
    checks: list[dict[str, object]],
    name: str,
    operation: Callable[[], object],
) -> object | None:
    try:
        details = operation()
    except Exception as error:
        checks.append(
            {
                "error": {
                    "message": _sanitize_text(error, ()),
                    "type": type(error).__name__,
                },
                "name": name,
                "passed": False,
            }
        )
        return None
    checks.append({"details": _json_value(details), "name": name, "passed": True})
    return details


def _import_diagnostics() -> dict[str, object]:
    import importlib

    modules: dict[str, str] = {}
    for name in (
        "people_counter.fabric_production_migration_live",
        "pyspark",
        "delta",
    ):
        module = importlib.import_module(name)
        modules[name] = str(getattr(module, "__file__", "<built-in>"))
    version = _package_version()
    if version != EXPECTED_PACKAGE_VERSION:
        raise LiveMigrationError(
            "installed people-counter version does not match diagnostic version"
        )
    return {
        "modules": modules,
        "package_source_sha256": _source_fingerprint(),
        "package_version": version,
    }


def _notebookutils_diagnostics() -> dict[str, object]:
    import notebookutils

    missing = [
        name
        for name in ("exists", "head", "put")
        if not callable(getattr(notebookutils.fs, name, None))
    ]
    if missing:
        raise LiveMigrationError(
            "notebookutils.fs is missing required methods: " + ",".join(missing)
        )
    return {"fs_methods": ["exists", "head", "put"]}


def _runtime_binding_diagnostics(spark_session: Any) -> dict[str, object]:
    values: dict[str, dict[str, str]] = {}
    for field, keys in _RUNTIME_BINDING_CONF.items():
        values[field] = {
            **_runtime_conf_values(spark_session, keys),
            **_runtime_context_values(field),
        }
    observed = observed_artifact_binding(spark_session)
    expected = {
        "environment_id": ENVIRONMENT_ID,
        "lakehouse_id": LAKEHOUSE_ID,
        "workspace_id": WORKSPACE_ID,
    }
    if observed != expected:
        raise LiveMigrationError("Fabric runtime artifact binding is not exact")
    return {"keys": values, "observed": observed}


def _catalog_diagnostics(spark_session: Any) -> dict[str, object]:
    current = str(spark_session.catalog.currentDatabase())
    if not current:
        raise LiveMigrationError("Spark catalog has no default Lakehouse")
    visibility = {
        name: bool(spark_session.catalog.tableExists(name))
        for name in (LOCK_TABLE, *LEGACY_TABLES, *COMMITTED_VIEWS)
    }
    required = {
        LOCK_TABLE,
        DISPATCHER_LEASE_TABLE,
        REGISTRATION_LEASE_TABLE,
        WORK_TABLE,
    }
    missing = sorted(name for name in required if not visibility[name])
    if missing:
        raise LiveMigrationError(
            "required migration evidence objects are missing: " + ",".join(missing)
        )
    return {
        "current_database": current,
        "required": sorted(required),
        "visibility": visibility,
    }


def _delta_history_diagnostics(spark_session: Any) -> dict[str, int]:
    versions: dict[str, int] = {}
    for name in (LOCK_TABLE, *LEGACY_TABLES):
        if not spark_session.catalog.tableExists(name):
            continue
        rows = spark_session.sql(f"DESCRIBE HISTORY `{name}` LIMIT 1").collect()
        if len(rows) != 1:
            raise LiveMigrationError(f"Delta history is ambiguous for {name}")
        versions[name] = int(_row_dict(rows[0])["version"])
    return versions


def _diagnose_command(
    *,
    spark_session: Any,
    files: EvidenceFiles,
    inventory: InvocationInventory,
    inventory_run_id: str,
    diagnostics: DiagnosticRecorder,
    output: TextIO,
) -> int:
    """Probe only read surfaces and write one confined diagnostic artifact."""

    checks: list[dict[str, object]] = []
    _diagnostic_check(checks, "imports-package-version", _import_diagnostics)
    _diagnostic_check(checks, "notebookutils", _notebookutils_diagnostics)
    _diagnostic_check(
        checks,
        "fixed-path-access",
        lambda: {
            "inventory_exists": files.exists(inventory_path(inventory_run_id)),
            "inventory_path": inventory_path(inventory_run_id),
            "inventory_sha256": hashlib.sha256(
                files.read_bytes(inventory_path(inventory_run_id))
            ).hexdigest(),
        },
    )
    diagnostics.mark("spark-binding")
    binding = _diagnostic_check(
        checks,
        "runtime-binding",
        lambda: _runtime_binding_diagnostics(spark_session),
    )
    if isinstance(binding, Mapping):
        observed = binding.get("observed")
        if isinstance(observed, Mapping):
            diagnostics.artifact_binding = {
                str(key): str(value) for key, value in observed.items()
            }
    diagnostics.mark("catalog-evidence")
    _diagnostic_check(
        checks, "spark-catalog-default-and-visibility", lambda: _catalog_diagnostics(spark_session)
    )
    _diagnostic_check(
        checks, "delta-history-access", lambda: _delta_history_diagnostics(spark_session)
    )
    body: dict[str, object] = {
        "checks": checks,
        "input_hashes": dict(sorted(diagnostics.input_hashes.items())),
        "inventory": inventory.to_dict(),
        "invocation_id": diagnostics.invocation_id,
        "package": {
            "source_sha256": _source_fingerprint(),
            "version": _package_version(),
        },
        "run_id": diagnostics.run_id,
        "runtime": _runtime_evidence(spark_session),
        "schema": DIAGNOSTIC_SCHEMA,
        "status": (
            "passed" if all(bool(check["passed"]) for check in checks) else "failed"
        ),
    }
    body["evidence_body_sha256"] = hashlib.sha256(_canonical_bytes(body)).hexdigest()
    path = diagnose_path(diagnostics.run_id, diagnostics.invocation_id)
    digest = _create_verified(files, path, _canonical_bytes(body))
    print(
        canonical_json(
            {
                **body,
                "evidence_path": path,
                "evidence_readback_sha256": digest,
            }
        ),
        file=output,
    )
    return 0


def _recovery_job(inventory: InvocationInventory) -> Mapping[str, Any]:
    value = inventory.payload.get("lock_recovery")
    if not isinstance(value, Mapping):
        raise LiveMigrationError("recovery inventory has no Fabric REST job evidence")
    expected = {
        "invocation_id": FAILED_RECOVERY_INVOCATION_ID,
        "item_id": "463803d9-ebe1-4162-8a7c-881081da9ee5",
        "job_id": FAILED_RECOVERY_JOB_ID,
        "job_type": "sparkjob",
        "status": "Failed",
    }
    for name, required in expected.items():
        if value.get(name) != required:
            raise LiveMigrationError(
                f"recovery Fabric REST {name} does not match the reviewed failure"
            )
    for name in (
        "end_time_utc",
        "failure_reason_sha256",
        "job_state_sha256",
        "start_time_utc",
    ):
        if not isinstance(value.get(name), str) or not value[name]:
            raise LiveMigrationError(f"recovery Fabric REST {name} is missing")
    for name in ("failure_reason_sha256", "job_state_sha256"):
        if _HEX64.fullmatch(str(value[name])) is None:
            raise LiveMigrationError(f"recovery Fabric REST {name} is invalid")
    return value


def _parse_utc(value: object, label: str) -> float:
    if not isinstance(value, str) or not value:
        raise LiveMigrationError(f"{label} is missing")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    # Fabric emits seven fractional digits; Python accepts at most six.
    normalized = re.sub(r"(\.\d{6})\d+(?=(?:[+-]\d\d:\d\d)?$)", r"\1", normalized)
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise LiveMigrationError(f"{label} is not an ISO timestamp") from error
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _failed_apply_diagnostic(files: EvidenceFiles) -> dict[str, object]:
    path = failure_path(
        FAILED_RECOVERY_RUN_ID,
        FAILED_RECOVERY_INVOCATION_ID,
        "live",
    )
    content = files.read_bytes(path)
    digest = hashlib.sha256(content).hexdigest()
    if digest != FAILED_RECOVERY_DIAGNOSTIC_SHA256:
        raise LiveMigrationError("failed apply diagnostic hash is not exact")
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LiveMigrationError("failed apply diagnostic is invalid JSON") from error
    if not isinstance(value, Mapping):
        raise LiveMigrationError("failed apply diagnostic is not an object")
    exception = value.get("exception")
    if not isinstance(exception, Mapping):
        raise LiveMigrationError("failed apply diagnostic exception is missing")
    traceback_text = str(exception.get("traceback", ""))
    observed_shape = {
        "exception_message": exception.get("message"),
        "exception_type": exception.get("type"),
        "invocation_id": value.get("invocation_id"),
        "run_id": value.get("run_id"),
        "stage": value.get("stage"),
    }
    expected_shape = {
        "exception_message": (
            "independent writer-quiescence proof is stale or future-dated"
        ),
        "exception_type": "PreconditionError",
        "invocation_id": FAILED_RECOVERY_INVOCATION_ID,
        "run_id": FAILED_RECOVERY_RUN_ID,
        "stage": "spark-binding",
    }
    required_trace = all(
        part in traceback_text
        for part in (
            "in _apply_command",
            "in apply_plan",
            "in _check_apply_preconditions",
            "in _check_runtime_safety",
        )
    )
    forbidden_trace = (
        "apply_operation" in traceback_text
        or "operation receipt" in traceback_text.lower()
    )
    if observed_shape != expected_shape or not required_trace or forbidden_trace:
        raise LiveMigrationError(
            "failed apply diagnostic does not prove a pre-operation failure"
        )
    arguments = value.get("redacted_arguments")
    if not isinstance(arguments, list):
        raise LiveMigrationError("failed apply diagnostic arguments are missing")
    exact_arguments = {
        "--invocation-id": FAILED_RECOVERY_INVOCATION_ID,
        "--owner": "martins-vds",
        "--plan-sha256": FAILED_RECOVERY_PLAN_SHA256,
        "--run-id": FAILED_RECOVERY_RUN_ID,
    }
    for name, expected in exact_arguments.items():
        if _argument_value([str(item) for item in arguments], name) != expected:
            raise LiveMigrationError(
                f"failed apply diagnostic {name} binding is not exact"
            )
    return {
        "exception_message": exception["message"],
        "exception_type": exception["type"],
        "path": path,
        "sha256": digest,
        "stage": value["stage"],
    }


def _table_path_exists(name: str) -> bool:
    import notebookutils

    return bool(notebookutils.fs.exists(f"Tables/{name}"))


def _recovery_owner_matches(owner: str, diagnostic: Mapping[str, object]) -> None:
    expected_prefix = f"migration:martins-vds:{FAILED_RECOVERY_RUN_ID}:"
    suffix = owner.removeprefix(expected_prefix)
    if (
        not owner.startswith(expected_prefix)
        or re.fullmatch(r"[0-9a-f]{24}:[0-9a-f]{32}", suffix) is None
        or diagnostic.get("sha256") != FAILED_RECOVERY_DIAGNOSTIC_SHA256
    ):
        raise LiveMigrationError(
            "physical control owner does not correlate to the failed invocation"
        )


def _recovery_control_row(
    reader: SparkEvidenceReader,
    *,
    job: Mapping[str, Any],
    diagnostic: Mapping[str, object],
) -> Mapping[str, Any]:
    row = reader.control_writer_row()
    if set(row) != {"acquired_at", "lock_name", "owner_id"}:
        raise LiveMigrationError("control-writer global row is not exact")
    owner = row.get("owner_id")
    acquired_at = row.get("acquired_at")
    if row.get("lock_name") != "global" or not isinstance(owner, str):
        raise LiveMigrationError("control-writer global row is not recovery-owned")
    if acquired_at is None:
        raise LiveMigrationError("control-writer global row is not recovery-owned")
    _recovery_owner_matches(owner, diagnostic)
    acquired_epoch = (
        acquired_at.timestamp()
        if isinstance(acquired_at, datetime)
        else _parse_utc(str(acquired_at), "control acquired_at")
    )
    started = _parse_utc(job["start_time_utc"], "job start time")
    ended = _parse_utc(job["end_time_utc"], "job end time")
    if not started <= acquired_epoch <= ended:
        raise LiveMigrationError(
            "control owner acquisition does not fall within the failed invocation"
        )
    return row


def _require_migration_objects_absent(
    spark_session: Any,
    table_path_exists: Callable[[str], bool],
) -> None:
    present_catalog = tuple(
        name for name in TABLE_ALLOWLIST if spark_session.catalog.tableExists(name)
    )
    present_paths = tuple(name for name in TABLE_ALLOWLIST if table_path_exists(name))
    if present_catalog or present_paths:
        raise LiveMigrationError("migration table or OneLake path evidence is present")


def _failed_plan_file(files: EvidenceFiles) -> bytes:
    content = files.read_bytes(plan_path(FAILED_RECOVERY_RUN_ID))
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LiveMigrationError("failed apply plan evidence is invalid") from error
    if (
        not isinstance(value, Mapping)
        or value.get("plan_sha256") != FAILED_RECOVERY_PLAN_SHA256
    ):
        raise LiveMigrationError("failed apply plan evidence is not exact")
    return content


def _recovery_facts(
    *,
    spark_session: Any,
    files: EvidenceFiles,
    inventory: InvocationInventory,
    reader: SparkEvidenceReader,
    table_path_exists: Callable[[str], bool],
) -> tuple[dict[str, object], Mapping[str, Any]]:
    if not inventory.passed:
        raise LiveMigrationError("recovery inventory is not independently quiescent")
    job = _recovery_job(inventory)
    if inventory.captured_at <= _parse_utc(job["end_time_utc"], "job end time"):
        raise LiveMigrationError("recovery inventory predates the failed job terminal state")
    diagnostic = _failed_apply_diagnostic(files)
    row = _recovery_control_row(reader, job=job, diagnostic=diagnostic)
    owner = str(row["owner_id"])
    active_leases = reader.active_leases()
    if active_leases:
        raise LiveMigrationError("newer lease or work activity exists")
    _require_migration_objects_absent(spark_session, table_path_exists)
    failed_plan_content = _failed_plan_file(files)
    failed_result = result_path(
        FAILED_RECOVERY_RUN_ID, FAILED_RECOVERY_PLAN_SHA256
    )
    if files.exists(failed_result):
        raise LiveMigrationError("failed apply result or operation receipts are present")
    facts: dict[str, object] = {
        "active_leases": [],
        "catalog_tables_absent": list(TABLE_ALLOWLIST),
        "control_row": {
            key: _json_value(value) for key, value in sorted(row.items())
        },
        "control_row_sha256": evidence_hash(
            {key: _json_value(value) for key, value in sorted(row.items())}
        ),
        "failed_diagnostic": diagnostic,
        "failed_invocation_id": FAILED_RECOVERY_INVOCATION_ID,
        "failed_job": dict(job),
        "failed_job_id": FAILED_RECOVERY_JOB_ID,
        "failed_plan_file_sha256": hashlib.sha256(
            failed_plan_content
        ).hexdigest(),
        "failed_plan_sha256": FAILED_RECOVERY_PLAN_SHA256,
        "failed_result_absent": failed_result,
        "journal": {
            "path_absent": JOURNAL_TABLE in TABLE_ALLOWLIST,
            "successful_evidence_absent": True,
            "table_absent": JOURNAL_TABLE in TABLE_ALLOWLIST,
        },
        "migration_id": MIGRATION_ID,
        "onelake_table_paths_absent": list(TABLE_ALLOWLIST),
        "physical_owner": owner,
        "schema": RECOVERY_SCHEMA,
    }
    return facts, row


def _recovery_plan(facts: Mapping[str, object]) -> dict[str, object]:
    evidence_sha256 = evidence_hash(facts)
    binding = {
        "control_row_sha256": facts["control_row_sha256"],
        "evidence_sha256": evidence_sha256,
        "failed_invocation_id": FAILED_RECOVERY_INVOCATION_ID,
        "failed_job_id": FAILED_RECOVERY_JOB_ID,
        "migration_id": MIGRATION_ID,
        "physical_owner": facts["physical_owner"],
    }
    token = f"recover-v1:{evidence_hash(binding)}"
    body = {
        "binding": binding,
        "evidence": dict(facts),
        "evidence_sha256": evidence_sha256,
        "recovery_token": token,
        "schema": RECOVERY_SCHEMA,
    }
    return {**body, "plan_sha256": evidence_hash(body)}


def _validated_recovery_review(
    files: EvidenceFiles,
    plan: Mapping[str, object],
    *,
    now: float,
) -> Mapping[str, Any]:
    try:
        receipt = json.loads(
            files.read_bytes(
                recovery_review_path(
                    FAILED_RECOVERY_JOB_ID, FAILED_RECOVERY_INVOCATION_ID
                )
            )
        )
    except (KeyError, OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LiveMigrationError("recovery review receipt is invalid JSON") from error
    if not isinstance(receipt, Mapping):
        raise LiveMigrationError("recovery review receipt is not an object")
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    reviewed_at = float(receipt.get("reviewed_at", math.inf))
    if (
        receipt.get("receipt_sha256") != evidence_hash(body)
        or receipt.get("schema") != RECOVERY_SCHEMA
        or receipt.get("failed_job_id") != FAILED_RECOVERY_JOB_ID
        or receipt.get("failed_invocation_id")
        != FAILED_RECOVERY_INVOCATION_ID
        or receipt.get("plan_sha256") != plan.get("plan_sha256")
        or receipt.get("recovery_token_sha256")
        != token_hash(str(plan.get("recovery_token")))
        or reviewed_at > now
        or float(receipt.get("expires_at", -1)) < now
    ):
        raise LiveMigrationError("recovery review receipt binding is invalid or expired")
    return receipt


def delta_recovery_clear_cas(
    spark_session: Any,
    *,
    expected_owner: str,
    expected_acquired_at: Any,
) -> None:
    """Clear only the exact failed physical owner and acquisition timestamp."""

    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    condition = F.col("lock_name") == "global"
    condition &= F.col("owner_id") == expected_owner
    condition &= F.col("acquired_at") == F.lit(expected_acquired_at).cast("timestamp")
    DeltaTable.forName(spark_session, LOCK_TABLE).update(
        condition=condition,
        set={
            "owner_id": F.lit(None).cast("string"),
            "acquired_at": F.lit(None).cast("timestamp"),
        },
    )


def _recover_lock_command(
    args: argparse.Namespace,
    *,
    spark_session: Any,
    files: EvidenceFiles,
    inventory: InvocationInventory,
    reader: SparkEvidenceReader,
    clock: Callable[[], float],
    output: TextIO,
    table_path_exists: Callable[[str], bool] = _table_path_exists,
    clear_cas: Callable[[str, Any], None] | None = None,
) -> int:
    if (
        args.run_id != FAILED_RECOVERY_RUN_ID
        or args.failed_job_id != FAILED_RECOVERY_JOB_ID
        or args.failed_invocation_id != FAILED_RECOVERY_INVOCATION_ID
    ):
        raise LiveMigrationError("lock recovery accepts only the reviewed failure")
    facts, raw_row = _recovery_facts(
        spark_session=spark_session,
        files=files,
        inventory=inventory,
        reader=reader,
        table_path_exists=table_path_exists,
    )
    plan = _recovery_plan(facts)
    path = recovery_plan_path(
        FAILED_RECOVERY_JOB_ID, FAILED_RECOVERY_INVOCATION_ID
    )
    _create_verified(files, path, _canonical_bytes(plan), allow_identical=True)
    if not args.recovery_execute:
        print(canonical_json({"execute": False, "plan": plan}), file=output)
        return 0
    if args.recovery_token != plan["recovery_token"]:
        raise LiveMigrationError("recovery token does not bind the current exact evidence")
    stored = files.read_bytes(path)
    if stored != _canonical_bytes(plan):
        raise LiveMigrationError("stored recovery plan differs from current evidence")
    receipt = _validated_recovery_review(files, plan, now=float(clock()))
    owner = str(raw_row["owner_id"])
    acquired_at = raw_row["acquired_at"]
    operation = clear_cas or (
        lambda expected_owner, expected_time: delta_recovery_clear_cas(
            spark_session,
            expected_owner=expected_owner,
            expected_acquired_at=expected_time,
        )
    )
    operation(owner, acquired_at)
    readback = reader.control_writer_row()
    if (
        set(readback) != {"acquired_at", "lock_name", "owner_id"}
        or readback.get("lock_name") != "global"
        or readback.get("owner_id") is not None
        or readback.get("acquired_at") is not None
    ):
        raise LiveMigrationError("recovery CAS clear has ambiguous exact readback")
    result_body = {
        "after": {key: _json_value(value) for key, value in sorted(readback.items())},
        "before_sha256": facts["control_row_sha256"],
        "cas_id": evidence_hash(
            {
                "acquired_at": _json_value(acquired_at),
                "owner": owner,
                "plan_sha256": plan["plan_sha256"],
            }
        ),
        "failed_invocation_id": FAILED_RECOVERY_INVOCATION_ID,
        "failed_job_id": FAILED_RECOVERY_JOB_ID,
        "plan_sha256": plan["plan_sha256"],
        "recovery_token_sha256": token_hash(str(plan["recovery_token"])),
        "review_receipt_sha256": receipt["receipt_sha256"],
        "schema": RECOVERY_SCHEMA,
        "status": "cleared",
    }
    result = {**result_body, "result_sha256": evidence_hash(result_body)}
    _create_verified(
        files,
        recovery_result_path(
            FAILED_RECOVERY_JOB_ID, FAILED_RECOVERY_INVOCATION_ID
        ),
        _canonical_bytes(result),
    )
    print(canonical_json(result), file=output)
    return 0


class ControlWriterMigrationLock:
    """Exact-owner CAS lock with mandatory readback and fail-closed ambiguity."""

    def __init__(
        self,
        reader: SparkEvidenceReader,
        cas: Callable[[str | None, str | None], None],
    ) -> None:
        self.reader = reader
        self.cas = cas

    def _owner(self) -> str | None:
        value = self.reader.control_writer_row().get("owner_id")
        return None if value is None else str(value)

    def acquire(self, owner_id: str) -> None:
        observed = self._owner()
        if observed is not None:
            raise PreconditionError(f"control writer is owned by {observed!r}")
        error: Exception | None = None
        try:
            self.cas(None, owner_id)
        except Exception as caught:
            error = caught
        readback = self._owner()
        if readback == owner_id:
            return
        if error is not None:
            raise LiveMigrationError(
                f"ambiguous migration lock acquisition; readback owner={readback!r}"
            ) from error
        raise PreconditionError(
            f"migration lock CAS lost; readback owner={readback!r}"
        )

    def release(self, owner_id: str) -> None:
        if self._owner() != owner_id:
            raise PreconditionError("migration lock owner changed before release")
        error: Exception | None = None
        try:
            self.cas(owner_id, None)
        except Exception as caught:
            error = caught
        readback = self._owner()
        if readback is None:
            return
        raise LiveMigrationError(
            f"ambiguous migration lock release; retained owner={readback!r}"
        ) from error


def delta_control_writer_cas(spark_session: Any) -> Callable[[str | None, str | None], None]:
    """Construct the sole fixed-table Delta CAS callback used by the driver."""

    def update(expected: str | None, replacement: str | None) -> None:
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F

        condition = F.col("lock_name") == "global"
        condition &= (
            F.col("owner_id").isNull()
            if expected is None
            else F.col("owner_id") == expected
        )
        DeltaTable.forName(spark_session, LOCK_TABLE).update(
            condition=condition,
            set={
                "owner_id": F.lit(replacement).cast("string"),
                "acquired_at": (
                    F.current_timestamp()
                    if replacement is not None
                    else F.lit(None).cast("timestamp")
                ),
            },
        )

    return update


def construct_live_backend(
    spark_session: Any,
    inventory: InvocationInventory,
    *,
    clock: Callable[[], float],
    owner_id: str,
    physical_owner_id: str,
    lease_token: str,
    reader: SparkEvidenceReader | None = None,
    artifact_binding: Callable[[], Mapping[str, str]] | None = None,
) -> SparkMigrationBackend:
    """Construct every reviewed callback around ``SparkMigrationBackend``."""

    evidence = reader or SparkEvidenceReader(spark_session, clock=clock)
    proof = WriterQuiescenceProof(
        inventory_sha256=inventory.payload_sha256,
        captured_at=inventory.captured_at,
        passed=inventory.passed,
        active_writer_ids=inventory.active_writer_ids,
        stopped_trigger_ids=inventory.stopped_writer_ids,
    )

    raw_control_row = evidence.control_writer_row()
    if set(raw_control_row) != {"acquired_at", "lock_name", "owner_id"}:
        raise LiveMigrationError("Spark control-writer row is not exact")
    planned_control_row = {
        key: _json_value(value) for key, value in raw_control_row.items()
    }
    if (
        planned_control_row["lock_name"] != "global"
        or planned_control_row["owner_id"] is not None
    ):
        raise LiveMigrationError("Spark control-writer row is owned or invalid")

    def legacy_evidence() -> tuple[LegacyObjectSnapshot, ...]:
        observed = {
            key: _json_value(value)
            for key, value in evidence.control_writer_row().items()
            if key in planned_control_row
        }
        observed_owner = observed.get("owner_id")
        if observed_owner == physical_owner_id:
            observed = planned_control_row
        if observed != planned_control_row:
            raise LiveMigrationError("control-writer row differs from signed inventory")
        control = LegacyObjectSnapshot(
            name=LOCK_TABLE,
            object_type="control-writer-row",
            exists=True,
            row_count=1,
            content_sha256=evidence_hash(planned_control_row),
        )
        return tuple(sorted((*evidence.legacy_objects(), control)))

    return SparkMigrationBackend(
        spark_session,
        artifact_binding=artifact_binding or (
            lambda: observed_artifact_binding(spark_session)
        ),
        active_run_ids=lambda: inventory.active_run_ids,
        active_lease_ids=evidence.active_leases,
        control_owner=lambda: evidence.control_owner(
            migration_owner_id=physical_owner_id
        ),
        lease=lambda: LeaseSnapshot(
            owner=owner_id,
            token_sha256=token_hash(lease_token),
            expires_at=inventory.expires_at,
        ),
        legacy_objects=legacy_evidence,
        committed_pointer_sha256=evidence.committed_pointer_sha256,
        committed_view_fingerprints=evidence.committed_view_fingerprints,
        gold_sha256=evidence.gold_sha256,
        writer_quiescence=lambda: proof,
        clock=clock,
    )


def load_inventory(
    files: EvidenceFiles,
    run_id: str,
    *,
    hmac_key: bytes,
    now: float,
) -> InvocationInventory:
    if not hmac_key:
        raise LiveMigrationError("inventory HMAC key is required")
    return InvocationInventory.from_bytes(
        files.read_bytes(inventory_path(run_id)),
        hmac_key=hmac_key,
        now=now,
    )


def _write_create_only_json(files: EvidenceFiles, path: str, value: object) -> None:
    files.create_bytes(path, canonical_json(value).encode("utf-8") + b"\n")


def _redacted_plan(plan: MigrationPlan) -> dict[str, object]:
    return {
        **plan.evidence(),
        "plan_sha256": plan.sha256,
        "safety_token": "<redacted>",
        "safety_token_sha256": token_hash(plan.safety_token),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pc-production-migration-live")
    parser.add_argument(
        "command",
        choices=(
            "inventory",
            "diagnose",
            "plan",
            "apply",
            "verify",
            "status",
            "recover-lock",
        ),
        nargs="?",
        default="inventory",
    )
    parser.add_argument("--run-id")
    parser.add_argument("--inventory-run-id")
    parser.add_argument("--inventory-hmac-key")
    parser.add_argument("--invocation-id")
    parser.add_argument("--owner")
    parser.add_argument("--lease-token")
    parser.add_argument("--plan-sha256")
    parser.add_argument("--safety-token")
    parser.add_argument("--failed-job-id")
    parser.add_argument("--failed-invocation-id")
    parser.add_argument("--recovery-token")
    parser.add_argument("--recovery-execute", action="store_true")
    parser.add_argument("--execute", action="store_true")
    return parser


def _required_execution_identity(args: argparse.Namespace) -> tuple[str, str, str]:
    if not args.run_id or not args.owner or not args.lease_token:
        raise LiveMigrationError("--run-id, --owner, and --lease-token are required")
    return (
        _safe_run_id(args.run_id),
        _safe_owner(args.owner),
        str(args.lease_token),
    )


def _inventory_command(
    args: argparse.Namespace,
    *,
    files: EvidenceFiles,
    key: bytes,
    now: float,
    output: TextIO,
) -> int:
    if not args.run_id:
        raise LiveMigrationError("inventory requires --run-id")
    inventory = load_inventory(files, args.run_id, hmac_key=key, now=now)
    print(canonical_json(inventory.to_dict()), file=output)
    return 0 if inventory.passed else 2


def _read_command(
    command: str,
    backend: SparkMigrationBackend,
    *,
    files: EvidenceFiles,
    run_id: str,
    invocation_id: str,
    output: TextIO,
    diagnostics: DiagnosticRecorder,
) -> int:
    if command == "plan":
        diagnostics.mark("catalog-evidence")
        snapshot = backend.discover()
        diagnostics.mark("plan-build")
        plan = build_plan(snapshot)
        evidence = _redacted_plan(plan)
        diagnostics.input_hashes["plan_sha256"] = plan.sha256
        diagnostics.mark("plan-write")
        _write_create_only_json(files, plan_path(run_id), evidence)
        _write_create_only_json(
            files, report_path(run_id, invocation_id, command), evidence
        )
        print(canonical_json(evidence), file=output)
        return 0 if plan.compatibility.compatible else 2
    if command == "status":
        evidence = status_report(backend)
        _write_create_only_json(
            files, report_path(run_id, invocation_id, command), evidence
        )
        print(canonical_json(evidence), file=output)
        return 0
    report = verify_backend(backend)
    evidence = report.to_dict()
    _write_create_only_json(
        files, report_path(run_id, invocation_id, command), evidence
    )
    print(canonical_json(evidence), file=output)
    return 0 if report.valid else 2


def _stored_current_plan(
    args: argparse.Namespace,
    files: EvidenceFiles,
    backend: SparkMigrationBackend,
    run_id: str,
    *,
    owner_id: str,
    lease_token: str,
) -> MigrationPlan:
    if not args.plan_sha256 or not args.safety_token:
        raise LiveMigrationError(
            "apply --execute requires --plan-sha256 and --safety-token"
        )
    try:
        stored = json.loads(files.read_bytes(plan_path(run_id)))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LiveMigrationError("stored plan is not valid JSON") from error
    if not isinstance(stored, Mapping) or stored.get("plan_sha256") != args.plan_sha256:
        raise LiveMigrationError("stored plan does not match --plan-sha256")
    fresh = backend.discover()
    stored_discovery = stored.get("discovery")
    if not isinstance(stored_discovery, Mapping):
        raise LiveMigrationError("stored plan discovery is not an object")
    stored_lease = stored_discovery.get("lease")
    stored_proof = stored_discovery.get("writer_quiescence")
    if not isinstance(stored_lease, Mapping) or not isinstance(
        stored_proof, Mapping
    ):
        raise LiveMigrationError("stored plan has invalid live-state evidence")
    planned_discovery = replace(
        fresh,
        lease=replace(
            fresh.lease,
            expires_at=stored_lease.get("expires_at"),
        ),
        writer_quiescence=replace(
            fresh.writer_quiescence,
            captured_at=float(stored_proof.get("captured_at", -1)),
            inventory_sha256=str(stored_proof.get("inventory_sha256", "")),
        ),
    )
    if planned_discovery.state_sha256 != fresh.state_sha256:
        raise LiveMigrationError("fresh discovery state normalization is invalid")
    plan = build_plan(planned_discovery)
    if _redacted_plan(plan) != stored or plan.sha256 != args.plan_sha256:
        raise LiveMigrationError("fresh discovery does not exactly match stored plan")
    if plan.safety_token != args.safety_token:
        raise LiveMigrationError("safety token does not exactly match stored plan")
    validate_apply_plan(
        backend,
        plan,
        owner=owner_id,
        lease_token=lease_token,
        current=fresh,
    )
    return plan


def _no_operation_began(
    backend: SparkMigrationBackend,
    plan: MigrationPlan,
    files: EvidenceFiles,
    run_id: str,
) -> bool:
    if files.exists(result_path(run_id, plan.sha256)):
        return False
    observed = backend.discover()
    return all(
        observed.table(name).to_dict() == plan.discovery.table(name).to_dict()
        for name in TABLE_ALLOWLIST
    )


def _apply_command(
    args: argparse.Namespace,
    *,
    spark_session: Any,
    files: EvidenceFiles,
    inventory: InvocationInventory,
    backend: SparkMigrationBackend,
    reader: SparkEvidenceReader,
    run_id: str,
    owner_id: str,
    physical_owner_id: str,
    lease_token: str,
    output: TextIO,
    diagnostics: DiagnosticRecorder,
    lock_factory: Callable[
        [SparkEvidenceReader], ControlWriterMigrationLock
    ] | None,
) -> int:
    if not args.execute:
        print(canonical_json({"status": "read_only", "execute": False}), file=output)
        return 0
    if inventory.reflex.enabled:
        raise LiveMigrationError("exact Reflex rule is active")
    plan = _stored_current_plan(
        args,
        files,
        backend,
        run_id,
        owner_id=owner_id,
        lease_token=lease_token,
    )
    diagnostics.mark("pre-lock-validated")
    lock = (
        lock_factory(reader)
        if lock_factory is not None
        else ControlWriterMigrationLock(reader, delta_control_writer_cas(spark_session))
    )
    lock.acquire(physical_owner_id)
    diagnostics.mark("lock-acquired")
    try:
        result = apply_plan(
            backend,
            plan,
            owner=owner_id,
            lease_token=lease_token,
            safety_token=args.safety_token,
            on_operations_start=lambda: diagnostics.mark("operation-loop"),
        )
    except PreconditionError:
        try:
            safe_to_release = _no_operation_began(backend, plan, files, run_id)
        except Exception as proof_error:
            raise LiveMigrationError(
                "pre-operation failure retained lock because independent "
                "no-operation proof was ambiguous"
            ) from proof_error
        if not safe_to_release:
            raise LiveMigrationError(
                "pre-operation failure retained lock because operation "
                "absence was not exact"
            )
        lock.release(physical_owner_id)
        raise
    evidence = {
        "inventory": inventory.to_dict(),
        "lock_owner_sha256": token_hash(physical_owner_id),
        "migration_owner_id": owner_id,
        "plan_sha256": plan.sha256,
        "result": result.to_dict(),
        "run_id": run_id,
    }
    _write_create_only_json(files, result_path(run_id, plan.sha256), evidence)
    if result.status in {ApplyStatus.APPLIED, ApplyStatus.NOOP}:
        lock.release(physical_owner_id)
    print(canonical_json(evidence), file=output)
    return 0 if result.status in {ApplyStatus.APPLIED, ApplyStatus.NOOP} else 3


def _run_command(
    args: argparse.Namespace,
    *,
    spark_session: Any,
    files: EvidenceFiles,
    key: bytes,
    clock: Callable[[], float],
    output: TextIO,
    diagnostics: DiagnosticRecorder,
    lock_factory: Callable[
        [SparkEvidenceReader], ControlWriterMigrationLock
    ] | None,
) -> int:
    now = float(clock())
    if args.command == "apply" and not args.execute:
        print(canonical_json({"status": "read_only", "execute": False}), file=output)
        return 0
    diagnostics.mark("inventory-read")
    if args.command == "inventory":
        result = _inventory_command(
            args, files=files, key=key, now=now, output=output
        )
        diagnostics.mark("inventory-verify")
        return result
    if not args.run_id:
        raise LiveMigrationError(f"{args.command} requires --run-id")
    if not args.invocation_id:
        raise LiveMigrationError(
            f"{args.command} requires --invocation-id"
        )
    inventory_run_id = args.inventory_run_id or args.run_id
    inventory = load_inventory(files, inventory_run_id, hmac_key=key, now=now)
    diagnostics.input_hashes["inventory_payload_sha256"] = inventory.payload_sha256
    diagnostics.input_hashes["inventory_signature_sha256"] = (
        inventory.signature_sha256
    )
    diagnostics.mark("inventory-verify")
    if args.command == "diagnose":
        return _diagnose_command(
            spark_session=spark_session,
            files=files,
            inventory=inventory,
            inventory_run_id=inventory_run_id,
            diagnostics=diagnostics,
            output=output,
        )
    if args.command == "recover-lock":
        diagnostics.mark("spark-binding")
        binding = observed_artifact_binding(spark_session)
        expected_binding = {
            "environment_id": ENVIRONMENT_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "workspace_id": WORKSPACE_ID,
        }
        if binding != expected_binding:
            raise LiveMigrationError("Fabric runtime artifact binding is not exact")
        diagnostics.artifact_binding = binding
        diagnostics.mark("catalog-evidence")
        return _recover_lock_command(
            args,
            spark_session=spark_session,
            files=files,
            inventory=inventory,
            reader=SparkEvidenceReader(spark_session, clock=clock),
            clock=clock,
            output=output,
        )
    run_id, owner, lease_token = _required_execution_identity(args)
    diagnostics.mark("spark-binding")
    binding = observed_artifact_binding(spark_session)
    expected_binding = {
        "environment_id": ENVIRONMENT_ID,
        "lakehouse_id": LAKEHOUSE_ID,
        "workspace_id": WORKSPACE_ID,
    }
    if binding != expected_binding:
        raise LiveMigrationError("Fabric runtime artifact binding is not exact")
    diagnostics.artifact_binding = binding
    owner_id = migration_owner_id(owner, run_id, lease_token)
    physical_owner_id = f"{owner_id}:{uuid4().hex}"
    reader = SparkEvidenceReader(spark_session, clock=clock)
    backend = construct_live_backend(
        spark_session,
        inventory,
        clock=clock,
        owner_id=owner_id,
        physical_owner_id=physical_owner_id,
        lease_token=lease_token,
        reader=reader,
        artifact_binding=lambda: binding,
    )
    if args.command != "apply":
        if args.command != "plan":
            diagnostics.mark("catalog-evidence")
        return _read_command(
            args.command,
            backend,
            files=files,
            run_id=run_id,
            invocation_id=_safe_run_id(args.invocation_id),
            output=output,
            diagnostics=diagnostics,
        )
    return _apply_command(
        args,
        spark_session=spark_session,
        files=files,
        inventory=inventory,
        backend=backend,
        reader=reader,
        run_id=run_id,
        owner_id=owner_id,
        physical_owner_id=physical_owner_id,
        lease_token=lease_token,
        output=output,
        diagnostics=diagnostics,
        lock_factory=lock_factory,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    spark_session: Any | None = None,
    files: EvidenceFiles | None = None,
    hmac_key: bytes | None = None,
    clock: Callable[[], float] = time.time,
    output: TextIO = sys.stdout,
    errors: TextIO = sys.stderr,
    lock_factory: Callable[
        [SparkEvidenceReader], ControlWriterMigrationLock
    ] | None = None,
) -> int:
    """Run the fixed live command set; no arguments defaults to inventory."""

    raw_arguments = list(sys.argv[1:] if argv is None else argv)
    args: argparse.Namespace | None = None
    evidence_files = files or NotebookUtilsEvidenceFiles()
    diagnostics: DiagnosticRecorder | None = None
    try:
        diagnostics = DiagnosticRecorder.create(evidence_files, raw_arguments)
        diagnostics.mark("bootstrap")
        args = _build_parser().parse_args(raw_arguments)
        diagnostics.mark("args")
        if spark_session is None:
            from pyspark.sql import SparkSession

            spark_session = SparkSession.getActiveSession()
        if spark_session is None:
            raise LiveMigrationError("an active Spark session is required")
        if hmac_key is not None:
            key = hmac_key
        elif args.inventory_hmac_key:
            try:
                key = bytes.fromhex(args.inventory_hmac_key)
            except ValueError as error:
                raise LiveMigrationError(
                    "inventory HMAC key argument must be hexadecimal"
                ) from error
        else:
            key = os.environ.get(
                "PC_MIGRATION_INVENTORY_HMAC_KEY", ""
            ).encode("utf-8")
        return _run_command(
            args,
            spark_session=spark_session,
            files=evidence_files,
            key=key,
            clock=clock,
            output=output,
            diagnostics=diagnostics,
            lock_factory=lock_factory,
        )
    except BaseException as error:
        secrets = _known_secrets(args)
        diagnostic_failure: BaseException | None = None
        if diagnostics is not None:
            try:
                path, digest = diagnostics.write_failure(
                    error,
                    args=args,
                    spark_session=spark_session,
                )
                print(
                    f"diagnostic_evidence path={path} sha256={digest}",
                    file=errors,
                )
            except BaseException as caught:
                diagnostic_failure = caught
        print(
            f"{type(error).__name__}: {_sanitize_text(error, secrets)}",
            file=errors,
        )
        if diagnostic_failure is not None:
            note = (
                "diagnostic-write failure: "
                f"{type(diagnostic_failure).__name__}: "
                f"{_sanitize_text(diagnostic_failure, secrets)}"
            )
            print(note, file=errors)
            if hasattr(error, "add_note"):
                error.add_note(note)
        raise
