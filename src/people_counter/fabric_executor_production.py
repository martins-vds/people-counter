"""Pure production contracts used by the Fabric executor worker notebook."""

from __future__ import annotations

import hashlib
import json
import math
import shutil
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Final, Iterator, TypedDict

from people_counter.api import line_count_records, telemetry_records
from people_counter.fabric_dispatch import (
    EXECUTOR_PARTITION,
    compatible_queue_engine,
)
from people_counter.fabric_executor_partition import RuntimeProcessor, SdkRuntimeProcessor


ACTIVE_WORK_STATES: Final[frozenset[str]] = frozenset(
    {"LEASED", "STAGING", "RUNNING", "WRITING"}
)
ATTEMPT_TRANSITIONS: Final[dict[str, frozenset[str]]] = {
    "LEASED": frozenset({"STAGING"}),
    "STAGING": frozenset({"RUNNING"}),
    "RUNNING": frozenset({"WRITING"}),
    "WRITING": frozenset({"SUCCEEDED", "FAILED", "RETRY_WAIT", "DEAD_LETTER"}),
}
TERMINAL_RECORD_TYPES: Final[frozenset[str]] = frozenset(
    {"video_result", "error"}
)


class ExecutorPreflightError(RuntimeError):
    """The claimed batch cannot safely start executor inference."""


class StagingConflictError(RuntimeError):
    """A durable staging transaction is incomplete or conflicts with its claim."""


class ClaimedItem(TypedDict):
    work_id: str
    attempt_id: str


class ProductionStagingRecord(TypedDict):
    attempt_id: str
    work_id: str
    worker_execution_id: str
    status: str
    error_type: str | None
    error_message: str | None
    retryable: bool | None
    input_sha256: str | None
    source_size_bytes: int | None
    source_duration_seconds: float | None
    source_fps: float | None
    total_source_frames: int | None
    processed_frames: int | None
    processing_seconds: float | None
    distinct_people: int | None
    line_in_count: int | None
    line_out_count: int | None
    record_type: str
    record_sequence: int
    payload_json: str
    txn_app_id: str
    txn_version: int
    emitted_at_utc: datetime
    capture_date: object


@dataclass(frozen=True)
class LeaseSafetyControls:
    minimum_speed_x: float
    safety_factor: float
    margin_seconds: int

    @classmethod
    def create(
        cls,
        minimum_speed_x: object,
        safety_factor: object,
        margin_seconds: object,
        heartbeat_seconds: object,
    ) -> LeaseSafetyControls:
        speed = _positive_finite_float(
            minimum_speed_x,
            "MIN_APPROVED_SINGLE_VIDEO_SPEED_X",
        )
        factor = _positive_finite_float(safety_factor, "LEASE_SAFETY_FACTOR")
        if factor < 1.0:
            raise ValueError("LEASE_SAFETY_FACTOR must be at least 1.0")
        margin = _positive_int(margin_seconds, "LEASE_SAFETY_MARGIN_SECONDS")
        heartbeat = _positive_int(heartbeat_seconds, "HEARTBEAT_SECONDS")
        if margin < heartbeat * 2:
            raise ValueError(
                "LEASE_SAFETY_MARGIN_SECONDS must be at least two heartbeat intervals"
            )
        return cls(speed, factor, margin)

    def projected_wall_seconds(self, duration_seconds: object) -> float:
        duration = _positive_finite_float(duration_seconds, "duration_seconds")
        return duration / self.minimum_speed_x * self.safety_factor


@dataclass(frozen=True)
class StagingTransaction:
    app_id: str
    version: int


class DriverAttemptStates:
    """Track per-attempt status without advancing pending waves."""

    def __init__(self, attempt_ids: Iterable[str]) -> None:
        identities = [_required_text(value, "attempt_id") for value in attempt_ids]
        if len(set(identities)) != len(identities):
            raise ValueError("attempt_ids must be unique")
        self._states = {attempt_id: "LEASED" for attempt_id in identities}

    def transition(self, attempt_id: str, target: str) -> None:
        identity = _required_text(attempt_id, "attempt_id")
        if identity not in self._states:
            raise KeyError(f"Unknown attempt_id: {identity}")
        current = self._states[identity]
        if target not in ATTEMPT_TRANSITIONS.get(current, frozenset()):
            raise ValueError(f"Invalid attempt transition: {current} -> {target}")
        self._states[identity] = target

    def status(self, attempt_id: str) -> str:
        identity = _required_text(attempt_id, "attempt_id")
        if identity not in self._states:
            raise KeyError(f"Unknown attempt_id: {identity}")
        return self._states[identity]

    def nonterminal(self) -> dict[str, str]:
        return {
            attempt_id: status
            for attempt_id, status in self._states.items()
            if status not in {"SUCCEEDED", "FAILED", "RETRY_WAIT", "DEAD_LETTER"}
        }


def parse_claimed_items(value: object, maximum: object) -> list[ClaimedItem]:
    """Parse and bound one claim result without legacy single-item fallback."""
    limit = _positive_int(maximum, "MAX_ITEMS_PER_WORKER")
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError as error:
            raise ValueError("WORK_ITEMS_JSON must be valid JSON") from error
    else:
        decoded = value
    if not isinstance(decoded, list):
        raise ValueError("WORK_ITEMS_JSON must decode to an array")
    if len(decoded) > limit:
        raise ValueError(
            f"WORK_ITEMS_JSON contains {len(decoded)} items; maximum is {limit}"
        )
    items: list[ClaimedItem] = []
    identities: set[tuple[str, str]] = set()
    for item in decoded:
        if not isinstance(item, Mapping):
            raise ValueError("Every work item must be an object")
        normalized: ClaimedItem = {
            "work_id": _required_text(item.get("work_id"), "work_id"),
            "attempt_id": _required_text(item.get("attempt_id"), "attempt_id"),
        }
        identity = (normalized["work_id"], normalized["attempt_id"])
        if identity in identities:
            raise ValueError(f"Duplicate work/attempt pair: {identity!r}")
        identities.add(identity)
        items.append(normalized)
    return items


def validate_claimed_rows(
    items: Sequence[ClaimedItem],
    rows: Sequence[Mapping[str, Any]],
    *,
    dispatcher_id: str,
) -> list[dict[str, Any]]:
    """Validate lease ownership, engine, runtime affinity, and duration."""
    owner = _required_text(dispatcher_id, "dispatcher_id")
    expected = {(item["work_id"], item["attempt_id"]) for item in items}
    by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    runtime_keys: set[str] = set()
    for raw in rows:
        row = dict(raw)
        work_id = _required_text(row.get("work_id"), "work_id")
        attempt_id = _required_text(
            row.get("lease_owner_attempt_id"),
            "lease_owner_attempt_id",
        )
        identity = (work_id, attempt_id)
        if identity in by_identity:
            raise ExecutorPreflightError(f"Duplicate queue row for {identity!r}")
        if identity not in expected:
            raise ExecutorPreflightError(
                f"Queue row does not match the claimed batch: {identity!r}"
            )
        if row.get("lease_dispatcher_id") != owner:
            raise ExecutorPreflightError(f"Lease owner mismatch for {work_id}")
        if row.get("status") not in ACTIVE_WORK_STATES:
            raise ExecutorPreflightError(
                f"Work {work_id} is not active; status={row.get('status')!r}"
            )
        if compatible_queue_engine(row.get("processing_engine")) != EXECUTOR_PARTITION:
            raise ExecutorPreflightError(
                f"Work {work_id} is not routed to {EXECUTOR_PARTITION}"
            )
        duration = _positive_finite_float(
            row.get("duration_seconds"),
            f"duration_seconds for {work_id}",
        )
        runtime_key = row.get("runtime_sha256") or row.get("config_sha256")
        runtime_keys.add(_required_text(runtime_key, "runtime_sha256"))
        row["duration_seconds"] = duration
        by_identity[identity] = row
    missing = expected - set(by_identity)
    if missing:
        raise ExecutorPreflightError(
            f"Claimed queue rows are missing: {sorted(missing)!r}"
        )
    if len(runtime_keys) != 1:
        raise ExecutorPreflightError(
            "Claimed worker batch must contain exactly one compatible model runtime"
        )
    return [by_identity[(item["work_id"], item["attempt_id"])] for item in items]


def require_configured_lease_budget(
    rows: Sequence[Mapping[str, Any]],
    controls: LeaseSafetyControls,
    lease_minutes: object,
) -> list[float]:
    """Reject a batch whose slowest video crosses the configured lease margin."""
    minutes = _positive_int(lease_minutes, "LEASE_MINUTES")
    projections = [
        controls.projected_wall_seconds(row.get("duration_seconds"))
        for row in rows
    ]
    if projections and max(projections) + controls.margin_seconds >= minutes * 60:
        raise ExecutorPreflightError(
            "Projected single-video wall time does not fit inside the lease "
            "safety margin"
        )
    return projections


def require_live_wave_budget(
    projected_wave_seconds: object,
    lease_expirations: Iterable[datetime],
    *,
    margin_seconds: object,
    now: datetime | None = None,
) -> datetime:
    """Return the projected finish time when all live leases remain safe."""
    projected = _positive_finite_float(
        projected_wave_seconds,
        "projected_wave_seconds",
    )
    margin = _positive_int(margin_seconds, "margin_seconds")
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    expirations = list(lease_expirations)
    if not expirations:
        raise ValueError("lease_expirations must not be empty")
    normalized: list[datetime] = []
    for expiration in expirations:
        if not isinstance(expiration, datetime):
            raise ValueError("lease_expirations must contain datetimes")
        if expiration.tzinfo is None:
            expiration = expiration.replace(tzinfo=timezone.utc)
        normalized.append(expiration)
    projected_finish = current + timedelta(seconds=projected)
    safe_until = min(normalized) - timedelta(seconds=margin)
    if projected_finish >= safe_until:
        raise ExecutorPreflightError(
            "Projected wave completion does not fit inside the live lease margin"
        )
    return projected_finish


def plan_largest_cost_first(
    rows: Sequence[Mapping[str, Any]],
    concurrency: object,
) -> list[list[dict[str, Any]]]:
    """Assign whole videos to deterministic least-loaded partitions."""
    limit = _positive_int(concurrency, "planned_concurrency")
    if not rows:
        return []
    partition_count = min(limit, len(rows))
    partitions: list[list[dict[str, Any]]] = [[] for _ in range(partition_count)]
    costs = [0.0] * partition_count
    ordered = sorted(
        (dict(row) for row in rows),
        key=lambda row: (
            -_positive_finite_float(row.get("duration_seconds"), "duration_seconds"),
            _required_text(row.get("work_id"), "work_id"),
        ),
    )
    for row in ordered:
        partition = min(range(partition_count), key=lambda index: (costs[index], index))
        partitions[partition].append(row)
        costs[partition] += float(row["duration_seconds"])
    return partitions


def select_production_line_counts(
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Select count changes plus the final cumulative record as notebook 04 does."""
    selected: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if (
            record.get("frame_in_count", 0) > 0
            or record.get("frame_out_count", 0) > 0
            or index == len(records) - 1
        ):
            selected.append(dict(record))
    return selected


@contextmanager
def staged_executor_source(
    source: Path,
    *,
    attempt_id: object,
    expected_size_bytes: object,
    expected_sha256: object,
    temporary_root: Path = Path("/tmp"),
) -> Iterator[tuple[Path, str, int]]:
    """Copy one source to an attempt-specific directory and validate provenance."""
    identity = _required_text(attempt_id, "attempt_id")
    expected_size = _positive_int(expected_size_bytes, "expected_size_bytes")
    expected_digest = _required_text(expected_sha256, "expected_sha256").lower()
    if len(expected_digest) != 64 or any(
        character not in "0123456789abcdef" for character in expected_digest
    ):
        raise ValueError("expected_sha256 must be 64 lowercase hexadecimal characters")
    source_path = Path(source)
    if not source_path.is_file():
        raise FileNotFoundError(f"Source video not found: {source_path}")
    required_bytes = math.ceil(expected_size * 1.10)
    if shutil.disk_usage(temporary_root).free < required_bytes:
        raise RuntimeError(
            f"Insufficient local staging space for attempt {identity}"
        )
    directory = Path(
        tempfile.mkdtemp(
            prefix=f"people-counter-{identity}-",
            dir=temporary_root,
        )
    )
    staged = directory / f"input{source_path.suffix or '.video'}"
    try:
        shutil.copyfile(source_path, staged)
        actual_size = staged.stat().st_size
        if actual_size != expected_size:
            raise ValueError(
                f"Source size mismatch: expected {expected_size}, got {actual_size}"
            )
        digest = hashlib.sha256()
        with staged.open("rb") as stream:
            while chunk := stream.read(8 * 1024 * 1024):
                digest.update(chunk)
        actual_digest = digest.hexdigest()
        if actual_digest != expected_digest:
            raise ValueError("Source SHA-256 does not match the manifest")
        yield staged, actual_digest, actual_size
    finally:
        if staged.exists():
            staged.unlink()
        directory.rmdir()


def process_production_partition(
    rows: Iterable[Mapping[str, Any]],
    *,
    source_resolver: Callable[[Mapping[str, Any]], Path],
    config_builder: Callable[[Mapping[str, Any], Path], Any],
    worker_execution_id: object,
    transaction: StagingTransaction,
    processor: RuntimeProcessor | None = None,
    error_classifier: Callable[[Exception], tuple[bool, str]] | None = None,
) -> Iterator[ProductionStagingRecord]:
    """Process whole videos independently and emit production staging rows."""
    execution_id = _required_text(worker_execution_id, "worker_execution_id")
    runner = processor if processor is not None else SdkRuntimeProcessor()
    for raw in rows:
        work = dict(raw)
        try:
            work_id = _required_text(work.get("work_id"), "work_id")
            attempt_id = _required_text(work.get("attempt_id"), "attempt_id")
            with staged_executor_source(
                source_resolver(work),
                attempt_id=attempt_id,
                expected_size_bytes=work.get("expected_size_bytes"),
                expected_sha256=work.get("expected_sha256"),
            ) as (staged, input_sha256, source_size):
                result = runner.run(config_builder(work, staged))
                provenance = {
                    "input_sha256": input_sha256,
                    "source_size_bytes": source_size,
                    "source_duration_seconds": _positive_finite_float(
                        work.get("duration_seconds"),
                        "duration_seconds",
                    ),
                    "source_fps": float(result.fps) if result.fps > 0 else None,
                    "total_source_frames": int(result.total_source_frames),
                }
                summary = {
                    "total_source_frames": result.total_source_frames,
                    "total_sampled_frames": result.total_sampled_frames,
                    "source_frames_read": result.source_frames_read,
                    "effective_sample_fps": result.effective_sample_fps,
                    "line_in_count": result.line_in_count,
                    "line_out_count": result.line_out_count,
                    "ended_early": result.ended_early,
                }
                yield _production_record(
                    work,
                    execution_id,
                    transaction,
                    "video_result",
                    0,
                    "SUCCEEDED",
                    summary,
                    provenance=provenance,
                    processed_frames=int(result.processed_frames),
                    processing_seconds=float(result.processing_seconds),
                    distinct_people=len(result.telemetry),
                    line_in_count=int(result.line_in_count),
                    line_out_count=int(result.line_out_count),
                )
                for sequence, payload in enumerate(telemetry_records(result)):
                    yield _production_record(
                        work,
                        execution_id,
                        transaction,
                        "telemetry",
                        sequence,
                        "SUCCEEDED",
                        payload,
                        provenance=provenance,
                    )
                selected_counts = select_production_line_counts(
                    line_count_records(result)
                )
                for sequence, payload in enumerate(selected_counts):
                    yield _production_record(
                        work,
                        execution_id,
                        transaction,
                        "line_count",
                        sequence,
                        "SUCCEEDED",
                        payload,
                        provenance=provenance,
                    )
        except Exception as error:
            work_id = _required_text(work.get("work_id"), "work_id")
            attempt_id = _required_text(work.get("attempt_id"), "attempt_id")
            retryable, category = (
                error_classifier(error)
                if error_classifier is not None
                else (True, "RUNTIME")
            )
            message = f"{type(error).__name__}: {error}"[:4000]
            yield _production_record(
                work,
                execution_id,
                transaction,
                "error",
                0,
                "FAILED",
                {
                    "error_category": category,
                    "error_type": type(error).__name__,
                    "error_message": message,
                },
                error_type=type(error).__name__,
                error_message=message,
                retryable=retryable,
            )


def staging_transaction(
    worker_execution_id: object,
    wave_index: object,
) -> StagingTransaction:
    """Build the stable Delta transaction identity for one materialized wave."""
    execution_id = _required_text(worker_execution_id, "worker_execution_id")
    wave = _nonnegative_int(wave_index, "wave_index")
    digest = hashlib.sha256(execution_id.encode("utf-8")).hexdigest()
    return StagingTransaction(f"people-counter-executor:{digest}", wave)


def validate_staging_records(
    records: Sequence[Mapping[str, Any]],
    items: Sequence[ClaimedItem],
    transaction: StagingTransaction,
) -> None:
    """Require one terminal envelope and unique logical rows for every claim."""
    expected = {(item["work_id"], item["attempt_id"]) for item in items}
    envelopes = {identity: 0 for identity in expected}
    logical_rows: set[tuple[str, str, str, int]] = set()
    for record in records:
        identity = (
            _required_text(record.get("work_id"), "work_id"),
            _required_text(record.get("attempt_id"), "attempt_id"),
        )
        if identity not in expected:
            raise StagingConflictError(f"Unexpected staging identity: {identity!r}")
        if record.get("txn_app_id") != transaction.app_id:
            raise StagingConflictError("Staging txn_app_id does not match the wave")
        if record.get("txn_version") != transaction.version:
            raise StagingConflictError("Staging txn_version does not match the wave")
        record_type = _required_text(record.get("record_type"), "record_type")
        sequence = _nonnegative_int(record.get("record_sequence"), "record_sequence")
        logical_identity = (*identity, record_type, sequence)
        if logical_identity in logical_rows:
            raise StagingConflictError(
                f"Duplicate staging logical identity: {logical_identity!r}"
            )
        logical_rows.add(logical_identity)
        if record_type in TERMINAL_RECORD_TYPES:
            envelopes[identity] += 1
    invalid = [identity for identity, count in envelopes.items() if count != 1]
    if invalid:
        raise StagingConflictError(
            f"Expected one terminal staging envelope for: {invalid!r}"
        )


def _required_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _production_record(
    work: Mapping[str, Any],
    worker_execution_id: str,
    transaction: StagingTransaction,
    record_type: str,
    record_sequence: int,
    status: str,
    payload: Mapping[str, Any],
    *,
    provenance: Mapping[str, Any] | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    retryable: bool | None = None,
    processed_frames: int | None = None,
    processing_seconds: float | None = None,
    distinct_people: int | None = None,
    line_in_count: int | None = None,
    line_out_count: int | None = None,
) -> ProductionStagingRecord:
    source = provenance or {}
    return {
        "attempt_id": _required_text(work.get("attempt_id"), "attempt_id"),
        "work_id": _required_text(work.get("work_id"), "work_id"),
        "worker_execution_id": worker_execution_id,
        "status": status,
        "error_type": error_type,
        "error_message": error_message,
        "retryable": retryable,
        "input_sha256": source.get("input_sha256"),
        "source_size_bytes": source.get("source_size_bytes"),
        "source_duration_seconds": source.get("source_duration_seconds"),
        "source_fps": source.get("source_fps"),
        "total_source_frames": source.get("total_source_frames"),
        "processed_frames": processed_frames,
        "processing_seconds": processing_seconds,
        "distinct_people": distinct_people,
        "line_in_count": line_in_count,
        "line_out_count": line_out_count,
        "record_type": record_type,
        "record_sequence": record_sequence,
        "payload_json": json.dumps(
            payload,
            default=str,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
        "txn_app_id": transaction.app_id,
        "txn_version": transaction.version,
        "emitted_at_utc": datetime.now(timezone.utc),
        "capture_date": work.get("capture_date"),
    }


def _positive_finite_float(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a positive finite number")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return parsed


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer")
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be a positive integer") from error
    if parsed < 1 or str(parsed) != str(value).strip():
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def _nonnegative_int(value: object, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a non-negative integer")
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be a non-negative integer") from error
    if parsed < 0 or str(parsed) != str(value).strip():
        raise ValueError(f"{name} must be a non-negative integer")
    return parsed
