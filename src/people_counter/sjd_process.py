"""Candidate A local process Spark Job Definition.

The module keeps Spark and Delta imports behind the Spark harness so importing
the package, constructing plans, and displaying CLI help work without PySpark.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import socket
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

from people_counter.cpu_runtime import configure_cpu_runtime
from people_counter.fabric_executor_partition import (
    ExecutorRuntimeCache,
    SdkRuntimeProcessor,
    process_video_partition,
)
from people_counter.fabric_executor_production import select_production_line_counts
from people_counter.local_spark import ProbeRuntimeProcessor, _config_from_work
from people_counter.sjd_control import (
    BatchValidationError,
    ImmutableConflictError,
    LeaseLostError,
    SQLiteControlStore,
)


Mode = Literal["probe", "sdk"]
TERMINAL_TYPES = frozenset({"video_result", "error"})
RECORD_TYPES = frozenset({"video_result", "error", "telemetry", "line_count"})
DETECTOR_BATCH_SIZES = frozenset({1, 2, 4})
MIB = 1024 * 1024
GIB = 1024 * MIB


class ProcessValidationError(RuntimeError):
    """The process input, staging data, or execution profile is invalid."""


class LeaseAdmissionError(ProcessValidationError):
    """The complete plan cannot safely finish before the live lease fence."""


class StagingConflictError(ProcessValidationError):
    """Attempt-scoped staging is partial or differs from immutable content."""


class UnsupportedAttemptStoreError(ProcessValidationError):
    """The requested attempt store is not implemented by this local SJD."""


@dataclass(frozen=True)
class ExecutionProfile:
    name: str
    executor_instances: int
    executor_cores: int
    executor_memory_bytes: int
    task_cpus: int
    fixed_allocation: bool
    speculation: bool
    memory_reserve_bytes: int
    heartbeat_seconds: float
    minimum_speed_x: float
    lease_safety_factor: float
    lease_margin_seconds: float

    @property
    def physical_partitions(self) -> tuple[int, ...]:
        return tuple(range(self.executor_instances))

    @property
    def spark_settings(self) -> dict[str, str]:
        return {
            "spark.dynamicAllocation.enabled": "false",
            "spark.speculation": "false",
            "spark.executor.instances": str(self.executor_instances),
            "spark.executor.cores": str(self.executor_cores),
            "spark.executor.memory": "1g",
            "spark.task.cpus": str(self.task_cpus),
        }


LOCAL_TWO_WORKERS = ExecutionProfile(
    name="local-two-workers",
    executor_instances=2,
    executor_cores=1,
    executor_memory_bytes=GIB,
    task_cpus=1,
    fixed_allocation=True,
    speculation=False,
    memory_reserve_bytes=256 * MIB,
    heartbeat_seconds=10.0,
    minimum_speed_x=1.0,
    lease_safety_factor=1.25,
    lease_margin_seconds=30.0,
)


def resolve_profile(
    name: str,
    *,
    spark_overrides: Mapping[str, object] | None = None,
) -> ExecutionProfile:
    """Resolve the reviewed fixed two-worker profile and reject drift."""
    if name not in {"local-two-workers", "candidate-a-local"}:
        raise ProcessValidationError(f"unsupported execution profile: {name!r}")
    profile = LOCAL_TWO_WORKERS
    supplied = {key: str(value).lower() for key, value in (spark_overrides or {}).items()}
    unknown = set(supplied) - set(profile.spark_settings)
    if unknown:
        raise ProcessValidationError(
            f"unsupported Spark profile overrides: {sorted(unknown)!r}"
        )
    conflicts = {
        key: (value, profile.spark_settings[key])
        for key, value in supplied.items()
        if value != profile.spark_settings[key].lower()
    }
    if conflicts:
        raise ProcessValidationError(
            f"Spark overrides conflict with fixed profile: {conflicts!r}"
        )
    if (
        not profile.fixed_allocation
        or profile.speculation
        or profile.executor_instances != 2
        or profile.executor_cores != 1
        or profile.task_cpus != 1
    ):
        raise ProcessValidationError("local profile no longer describes two fixed 1-core workers")
    return profile


@dataclass(frozen=True)
class EnvelopeItem:
    ordinal: int
    work_id: str
    attempt_id: str
    fence: int
    payload_sha256: str
    payload: dict[str, Any]
    config_sha256: str
    release_digest: str
    duration_seconds: float
    runtime_key: str


@dataclass(frozen=True)
class VerifiedEnvelope:
    batch_id: str
    owner: str
    runtime_key: str
    claimed_at: float
    lease_expires_at: float
    membership_sha256: str
    envelope_sha256: str
    items: tuple[EnvelopeItem, ...]

    @property
    def execution_attempt_id(self) -> str:
        identity = {
            "batch_id": self.batch_id,
            "membership_sha256": self.membership_sha256,
            "attempt_ids": [item.attempt_id for item in self.items],
        }
        return "process-" + _sha256(identity)[:24]


def verify_envelope(
    envelope: Mapping[str, Any],
    *,
    batch_id: str,
    envelope_sha256: str,
) -> VerifiedEnvelope:
    """Verify immutable identity, payload hashes, membership, and affinity."""
    if envelope.get("schema_version") != 1 or envelope.get("batch_id") != batch_id:
        raise BatchValidationError("claim envelope version or batch mismatch")
    owner = _text(envelope.get("owner"), "owner")
    runtime_key = _text(envelope.get("runtime_key"), "runtime_key")
    membership = _sha_text(envelope.get("membership_sha256"), "membership_sha256")
    raw_items = envelope.get("items")
    if not isinstance(raw_items, list) or not raw_items:
        raise BatchValidationError("claim envelope items must be a non-empty array")
    items: list[EnvelopeItem] = []
    identities: set[tuple[str, str]] = set()
    for expected_ordinal, raw in enumerate(raw_items):
        if not isinstance(raw, Mapping):
            raise BatchValidationError("every claim item must be an object")
        ordinal = _nonnegative_int(raw.get("ordinal"), "ordinal")
        if ordinal != expected_ordinal:
            raise BatchValidationError("claim ordinals must be contiguous and ordered")
        payload = raw.get("payload")
        if not isinstance(payload, Mapping):
            raise BatchValidationError("claim payload must be an object")
        payload_dict = dict(payload)
        payload_sha256 = _sha_text(raw.get("payload_sha256"), "payload_sha256")
        if _sha256(payload_dict) != payload_sha256:
            raise BatchValidationError("claim payload SHA-256 mismatch")
        work_id = _text(raw.get("work_id"), "work_id")
        attempt_id = _text(raw.get("attempt_id"), "attempt_id")
        identity = (work_id, attempt_id)
        if identity in identities:
            raise BatchValidationError(f"duplicate claim identity: {identity!r}")
        identities.add(identity)
        item_runtime = str(raw.get("runtime_key", runtime_key))
        if item_runtime != runtime_key:
            raise BatchValidationError("claim contains mixed runtime affinity")
        batch_size = payload_dict.get("batch_size", 1)
        if type(batch_size) is not int or batch_size not in DETECTOR_BATCH_SIZES:
            raise ProcessValidationError(
                f"detector batch_size for {work_id!r} must be one of "
                f"{sorted(DETECTOR_BATCH_SIZES)!r}"
            )
        items.append(
            EnvelopeItem(
                ordinal=ordinal,
                work_id=work_id,
                attempt_id=attempt_id,
                fence=_positive_int(raw.get("fence"), "fence"),
                payload_sha256=payload_sha256,
                payload=payload_dict,
                config_sha256=_text(raw.get("config_sha256"), "config_sha256"),
                release_digest=_text(raw.get("release_digest"), "release_digest"),
                duration_seconds=_positive_float(
                    raw.get("duration_seconds"), "duration_seconds"
                ),
                runtime_key=item_runtime,
            )
        )
    calculated_membership = _sha256(
        [
            {
                "work_id": item.work_id,
                "attempt_id": item.attempt_id,
                "fence": item.fence,
                "payload_sha256": item.payload_sha256,
            }
            for item in items
        ]
    )
    if calculated_membership != membership:
        raise BatchValidationError("claim membership SHA-256 mismatch")
    return VerifiedEnvelope(
        batch_id=batch_id,
        owner=owner,
        runtime_key=runtime_key,
        claimed_at=_finite(envelope.get("claimed_at"), "claimed_at"),
        lease_expires_at=_finite(envelope.get("lease_expires_at"), "lease_expires_at"),
        membership_sha256=membership,
        envelope_sha256=_sha_text(envelope_sha256, "envelope_sha256"),
        items=tuple(items),
    )


@dataclass(frozen=True)
class ConcurrencyDecision:
    concurrency: int
    item_limit: int
    profile_limit: int
    cpu_limit: int
    memory_limit: int | None
    peak_rss_bytes: int | None
    duration_only_fallback: bool


def conservative_concurrency(
    item_count: int,
    profile: ExecutionProfile,
    *,
    peak_rss_bytes: int | None,
) -> ConcurrencyDecision:
    """Bound concurrency by items, placement, CPU, and measured peak RSS."""
    count = _positive_int(item_count, "item_count")
    profile_limit = profile.executor_instances
    cpu_limit = (
        profile.executor_instances
        * profile.executor_cores
        // profile.task_cpus
    )
    memory_limit: int | None = None
    fallback = peak_rss_bytes is None
    if peak_rss_bytes is not None:
        peak = _positive_int(peak_rss_bytes, "peak_rss_bytes")
        usable = profile.executor_memory_bytes - profile.memory_reserve_bytes
        if peak > usable:
            raise ProcessValidationError(
                "measured peak RSS does not fit in one configured executor"
            )
        memory_per_executor = usable // peak
        memory_limit = profile.executor_instances * memory_per_executor
    limits = [count, profile_limit, cpu_limit]
    if memory_limit is not None:
        limits.append(memory_limit)
    return ConcurrencyDecision(
        concurrency=min(limits),
        item_limit=count,
        profile_limit=profile_limit,
        cpu_limit=cpu_limit,
        memory_limit=memory_limit,
        peak_rss_bytes=peak_rss_bytes,
        duration_only_fallback=fallback,
    )


@dataclass(frozen=True)
class PlannedItem:
    item: EnvelopeItem
    bucket_id: int
    physical_partition: int
    wave_index: int
    planned_cost_seconds: float


@dataclass(frozen=True)
class BucketPlan:
    bucket_id: int
    runtime_key: str
    physical_partition: int
    total_cost_seconds: float
    items: tuple[PlannedItem, ...]


@dataclass(frozen=True)
class WavePlan:
    index: int
    projected_cost_seconds: float
    items: tuple[PlannedItem, ...]


@dataclass(frozen=True)
class ExecutionPlan:
    concurrency: ConcurrencyDecision
    buckets: tuple[BucketPlan, ...]
    waves: tuple[WavePlan, ...]
    projected_makespan_seconds: float


def plan_duration_lpt(
    items: Sequence[EnvelopeItem],
    profile: ExecutionProfile,
    *,
    peak_rss_bytes: int | None,
) -> ExecutionPlan:
    """Plan deterministic duration LPT buckets with explicit Spark partitions."""
    if not items:
        raise ProcessValidationError("cannot plan an empty claim")
    decision = conservative_concurrency(
        len(items), profile, peak_rss_bytes=peak_rss_bytes
    )
    grouped: dict[str, list[EnvelopeItem]] = {}
    for item in items:
        grouped.setdefault(item.runtime_key, []).append(item)
    if len(grouped) != 1:
        raise ProcessValidationError("one process SJD may contain one runtime affinity")
    runtime_key, group = next(iter(sorted(grouped.items())))
    buckets: list[list[EnvelopeItem]] = [[] for _ in range(decision.concurrency)]
    costs = [0.0] * decision.concurrency
    for item in sorted(
        group, key=lambda candidate: (-candidate.duration_seconds, candidate.work_id)
    ):
        target = min(range(decision.concurrency), key=lambda index: (costs[index], index))
        buckets[target].append(item)
        costs[target] += item.duration_seconds
    planned_buckets: list[BucketPlan] = []
    planned_by_wave: dict[int, list[PlannedItem]] = {}
    for bucket_id, bucket in enumerate(buckets):
        physical = profile.physical_partitions[bucket_id]
        planned_items = tuple(
            PlannedItem(
                item=item,
                bucket_id=bucket_id,
                physical_partition=physical,
                wave_index=wave,
                planned_cost_seconds=item.duration_seconds,
            )
            for wave, item in enumerate(bucket)
        )
        for planned in planned_items:
            planned_by_wave.setdefault(planned.wave_index, []).append(planned)
        planned_buckets.append(
            BucketPlan(
                bucket_id=bucket_id,
                runtime_key=runtime_key,
                physical_partition=physical,
                total_cost_seconds=costs[bucket_id],
                items=planned_items,
            )
        )
    waves = tuple(
        WavePlan(
            index=index,
            projected_cost_seconds=max(
                item.planned_cost_seconds for item in planned_by_wave[index]
            ),
            items=tuple(
                sorted(planned_by_wave[index], key=lambda item: item.bucket_id)
            ),
        )
        for index in sorted(planned_by_wave)
    )
    return ExecutionPlan(
        concurrency=decision,
        buckets=tuple(planned_buckets),
        waves=waves,
        projected_makespan_seconds=max(costs),
    )


def admit_lease(
    plan: ExecutionPlan,
    profile: ExecutionProfile,
    *,
    lease_expires_at: float,
    now: float,
) -> float:
    """Fail closed unless the conservative complete-plan finish is before margin."""
    remaining = _finite(lease_expires_at, "lease_expires_at") - _finite(now, "now")
    projected = (
        plan.projected_makespan_seconds
        / profile.minimum_speed_x
        * profile.lease_safety_factor
    )
    required = projected + profile.lease_margin_seconds
    if remaining <= required:
        raise LeaseAdmissionError(
            f"lease admission rejected: remaining={remaining:g}s required={required:g}s"
        )
    return projected


@dataclass(frozen=True)
class TaskIdentity:
    stage_id: int
    partition_id: int
    task_attempt_id: int
    attempt_number: int
    executor_identity: str
    executor_host: str


class _CountingSdkProcessor:
    def __init__(self) -> None:
        self._cache = ExecutorRuntimeCache()
        self._processor = SdkRuntimeProcessor(cache=self._cache)

    def run(self, config: Any) -> Any:
        return self._processor.run(config)

    @property
    def loads(self) -> int:
        return self._cache.loads

    @property
    def hits(self) -> int:
        return self._cache.hits


def sparse_line_records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Keep crossing changes and the final cumulative line-count record."""
    return select_production_line_counts(records)


def execute_sjd_partition(
    rows: Iterable[Mapping[str, Any]],
) -> Iterable[dict[str, Any]]:
    """Installed top-level Spark callable with one runtime cache per partition."""
    materialized = [dict(row) for row in rows]
    if not materialized:
        return []
    identity = _task_identity(materialized[0])
    expected = {
        _nonnegative_int(row.get("physical_partition"), "physical_partition")
        for row in materialized
    }
    if expected != {identity.partition_id}:
        raise ProcessValidationError(
            f"Spark partition mismatch: expected {sorted(expected)!r}, "
            f"got {identity.partition_id}"
        )
    profile_cores = _positive_int(materialized[0].get("executor_cores"), "executor_cores")
    task_cpus = _positive_int(materialized[0].get("task_cpus"), "task_cpus")
    if task_cpus != 1 or profile_cores != 1:
        raise ProcessValidationError("executor callable requires the fixed one-core profile")
    budget = configure_cpu_runtime(profile_cores, 1)
    mode = materialized[0].get("mode")
    if mode not in {"probe", "sdk"}:
        raise ProcessValidationError(f"unsupported processor mode: {mode!r}")
    if mode == "sdk":
        processor: Any = _CountingSdkProcessor()
    else:
        processor = ProbeRuntimeProcessor(
            max(float(row.get("probe_delay_seconds", 0.0)) for row in materialized)
        )
    emitted: list[dict[str, Any]] = []
    for row in materialized:
        raw = list(
            process_video_partition(
                [row],
                config_builder=_config_from_work,
                processor=processor,
            )
        )
        line_rows = [
            json.loads(record["payload_json"])
            for record in raw
            if record["record_type"] == "line_count"
        ]
        selected_positions = {
            int(payload["_sjd_position"])
            for payload in sparse_line_records(
                [
                    {**payload, "_sjd_position": index}
                    for index, payload in enumerate(line_rows)
                ]
            )
        }
        line_position = 0
        sequence = 0
        for record in raw:
            payload = json.loads(record["payload_json"])
            if record["record_type"] == "line_count":
                keep = line_position in selected_positions
                line_position += 1
                if not keep:
                    continue
            normalized = {
                **record,
                "emitted_at_utc": record["emitted_at_utc"].isoformat(),
                "batch_id": row["batch_id"],
                "process_attempt_id": row["process_attempt_id"],
                "envelope_sha256": row["envelope_sha256"],
                "manifest_sha256": row["manifest_sha256"],
                "membership_sha256": row["membership_sha256"],
                "fence": row["fence"],
                "input_payload_sha256": row["input_payload_sha256"],
                "record_payload_sha256": _sha256(payload),
                "config_sha256": row["config_sha256"],
                "model_identity": row["model_identity"],
                "release_digest": row["release_digest"],
                "runtime_key": row["runtime_key"],
                "duration_seconds": row["duration_seconds"],
                "planned_cost_seconds": row["planned_cost_seconds"],
                "planned_concurrency": row["planned_concurrency"],
                "peak_rss_bytes": row["peak_rss_bytes"],
                "duration_only_fallback": row["duration_only_fallback"],
                "bucket_id": row["bucket_id"],
                "wave_index": row["wave_index"],
                "physical_partition": row["physical_partition"],
                "physical_partition_id": row["physical_partition"],
                "stage_id": identity.stage_id,
                "partition_id": identity.partition_id,
                "task_attempt_id": identity.task_attempt_id,
                "task_attempt_number": identity.attempt_number,
                "executor_identity": identity.executor_identity,
                "executor_host": identity.executor_host,
                "record_sequence": sequence,
                "detector_batch_size": row.get("batch_size", 1),
                "cpu_threads": budget.threads_per_worker,
                "runtime_loads": getattr(processor, "loads", 0),
                "runtime_cache_hits": getattr(processor, "hits", 0),
            }
            emitted.append(normalized)
            sequence += 1
    return emitted


def execute_sjd_bucket_partition(
    containers: Iterable[Mapping[str, Any] | None],
) -> Iterable[dict[str, Any]]:
    """Top-level Spark adapter from one physical bucket to executor rows."""
    values = list(containers)
    if not values or values[0] is None:
        return []
    if len(values) != 1 or not isinstance(values[0].get("rows"), list):
        raise ProcessValidationError(
            "each physical Spark partition must contain exactly one bucket"
        )
    return execute_sjd_partition(values[0]["rows"])


def _task_identity(row: Mapping[str, Any]) -> TaskIdentity:
    injected = row.get("_task_identity")
    if isinstance(injected, Mapping):
        executor_identity = _text(
            injected.get("executor_identity", "direct@localhost"),
            "executor_identity",
        )
        default_host = executor_identity.rpartition("@")[2]
        return TaskIdentity(
            stage_id=_nonnegative_int(injected.get("stage_id", 0), "stage_id"),
            partition_id=_nonnegative_int(
                injected.get("partition_id"), "partition_id"
            ),
            task_attempt_id=_nonnegative_int(
                injected.get("task_attempt_id", 0), "task_attempt_id"
            ),
            attempt_number=_nonnegative_int(
                injected.get("attempt_number", 0), "attempt_number"
            ),
            executor_identity=executor_identity,
            executor_host=_text(
                injected.get("executor_host", default_host),
                "executor_host",
            ),
        )
    from pyspark import TaskContext

    context = TaskContext.get()
    if context is None:
        raise ProcessValidationError("executor callable requires a Spark TaskContext")
    host = socket.gethostname()
    executor_id = os.environ.get("SPARK_EXECUTOR_ID", host)
    return TaskIdentity(
        stage_id=context.stageId(),
        partition_id=context.partitionId(),
        task_attempt_id=context.taskAttemptId(),
        attempt_number=context.attemptNumber(),
        executor_identity=f"{executor_id}@{host}",
        executor_host=host,
    )


def _executor_row(
    planned: PlannedItem,
    envelope: VerifiedEnvelope,
    profile: ExecutionProfile,
    mode: Mode,
    concurrency: ConcurrencyDecision,
) -> dict[str, Any]:
    item = planned.item
    return {
        **item.payload,
        "work_id": item.work_id,
        "attempt_id": item.attempt_id,
        "batch_id": envelope.batch_id,
        "process_attempt_id": envelope.execution_attempt_id,
        "envelope_sha256": envelope.envelope_sha256,
        "manifest_sha256": envelope.envelope_sha256,
        "membership_sha256": envelope.membership_sha256,
        "fence": item.fence,
        "input_payload_sha256": item.payload_sha256,
        "config_sha256": item.config_sha256,
        "model_identity": _model_identity(item.payload),
        "release_digest": item.release_digest,
        "runtime_key": item.runtime_key,
        "duration_seconds": item.duration_seconds,
        "planned_cost_seconds": planned.planned_cost_seconds,
        "planned_concurrency": concurrency.concurrency,
        "peak_rss_bytes": concurrency.peak_rss_bytes,
        "duration_only_fallback": concurrency.duration_only_fallback,
        "bucket_id": planned.bucket_id,
        "wave_index": planned.wave_index,
        "physical_partition": planned.physical_partition,
        "physical_partition_id": planned.physical_partition,
        "executor_cores": profile.executor_cores,
        "task_cpus": profile.task_cpus,
        "mode": mode,
    }


class ExecutionHarness(Protocol):
    def execute(
        self,
        envelope: VerifiedEnvelope,
        plan: ExecutionPlan,
        profile: ExecutionProfile,
        mode: Mode,
    ) -> list[dict[str, Any]]: ...


class DirectExecutionHarness:
    """Deterministic non-Spark harness using the same installed executor callable."""

    def execute(
        self,
        envelope: VerifiedEnvelope,
        plan: ExecutionPlan,
        profile: ExecutionProfile,
        mode: Mode,
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for bucket in plan.buckets:
            rows = [
                {
                    **_executor_row(
                        item,
                        envelope,
                        profile,
                        mode,
                        plan.concurrency,
                    ),
                    "_task_identity": {
                        "stage_id": 0,
                        "partition_id": bucket.physical_partition,
                        "task_attempt_id": bucket.bucket_id,
                        "attempt_number": 0,
                        "executor_identity": (
                            f"direct-{bucket.physical_partition}@localhost"
                        ),
                        "executor_host": "localhost",
                    },
                }
                for item in bucket.items
            ]
            records.extend(execute_sjd_partition(rows))
        return records


class SparkExecutionHarness:
    """Spark mapPartitions seam; importing this module still does not import Spark."""

    def __init__(self, spark_session: Any) -> None:
        self.spark_session = spark_session

    def execute(
        self,
        envelope: VerifiedEnvelope,
        plan: ExecutionPlan,
        profile: ExecutionProfile,
        mode: Mode,
    ) -> list[dict[str, Any]]:
        _verify_live_spark_settings(self.spark_session, profile)
        by_partition = {
            bucket.physical_partition: [
                _executor_row(
                    item,
                    envelope,
                    profile,
                    mode,
                    plan.concurrency,
                )
                for item in bucket.items
            ]
            for bucket in plan.buckets
        }
        ordered: list[dict[str, Any] | None] = [
            None for _ in profile.physical_partitions
        ]
        for partition, rows in by_partition.items():
            ordered[partition] = {"rows": rows}
        rdd = self.spark_session.sparkContext.parallelize(
            ordered, len(profile.physical_partitions)
        )

        return list(rdd.mapPartitions(execute_sjd_bucket_partition).collect())


def _verify_live_spark_settings(spark: Any, profile: ExecutionProfile) -> None:
    conflicts: dict[str, tuple[str, str]] = {}
    for key, expected in profile.spark_settings.items():
        actual = str(spark.conf.get(key, expected)).lower()
        if actual != expected.lower():
            conflicts[key] = (actual, expected)
    if conflicts:
        raise ProcessValidationError(
            f"live Spark configuration conflicts with fixed profile: {conflicts!r}"
        )


class AttemptAdapter(Protocol):
    def attempt_path(self, batch_id: str, process_attempt_id: str) -> Path: ...

    def load_complete(
        self, batch_id: str, process_attempt_id: str
    ) -> tuple[list[dict[str, Any]], dict[str, Any]] | None: ...

    def write_records(
        self,
        batch_id: str,
        process_attempt_id: str,
        records: Sequence[Mapping[str, Any]],
    ) -> Path: ...

    def read_records(
        self, batch_id: str, process_attempt_id: str
    ) -> list[dict[str, Any]]: ...

    def create_success(
        self,
        batch_id: str,
        process_attempt_id: str,
        marker: Mapping[str, Any],
    ) -> None: ...


class LocalJsonAttemptAdapter:
    """Create-only, attempt-scoped JSON staging used by local tests and direct runs."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root).expanduser().resolve()

    def attempt_path(self, batch_id: str, process_attempt_id: str) -> Path:
        return (
            self.root
            / f"batch={_path_segment(batch_id)}"
            / f"attempt={_path_segment(process_attempt_id)}"
        )

    def load_complete(
        self, batch_id: str, process_attempt_id: str
    ) -> tuple[list[dict[str, Any]], dict[str, Any]] | None:
        path = self.attempt_path(batch_id, process_attempt_id)
        records_path = path / "records.json"
        marker_path = path / "_SUCCESS"
        if not path.exists():
            return None
        if not records_path.is_file() or not marker_path.is_file():
            raise StagingConflictError(f"partial attempt staging exists at {path}")
        records = _read_json(records_path, expected_type=list)
        marker = _read_json(marker_path, expected_type=dict)
        return _ordered_records(records), dict(marker)

    def write_records(
        self,
        batch_id: str,
        process_attempt_id: str,
        records: Sequence[Mapping[str, Any]],
    ) -> Path:
        path = self.attempt_path(batch_id, process_attempt_id)
        if path.exists():
            raise StagingConflictError(f"attempt staging already exists at {path}")
        path.mkdir(parents=True)
        content = _canonical(_ordered_records(records))
        records_path = path / "records.json"
        with records_path.open("x", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        return path

    def read_records(
        self, batch_id: str, process_attempt_id: str
    ) -> list[dict[str, Any]]:
        path = self.attempt_path(batch_id, process_attempt_id) / "records.json"
        if not path.is_file():
            raise StagingConflictError(f"attempt records are missing at {path}")
        return _ordered_records(_read_json(path, expected_type=list))

    def create_success(
        self,
        batch_id: str,
        process_attempt_id: str,
        marker: Mapping[str, Any],
    ) -> None:
        marker_path = self.attempt_path(batch_id, process_attempt_id) / "_SUCCESS"
        content = _canonical(dict(marker))
        try:
            with marker_path.open("x", encoding="utf-8") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
        except FileExistsError:
            if marker_path.read_text(encoding="utf-8") != content:
                raise ImmutableConflictError("_SUCCESS marker conflicts with this attempt")


class SparkDeltaAttemptAdapter:
    """Immutable Delta attempt storage used when the Spark harness is selected."""

    def __init__(self, root: Path, spark_session: Any) -> None:
        self.root = Path(root).expanduser().resolve()
        self.spark_session = spark_session

    def attempt_path(self, batch_id: str, process_attempt_id: str) -> Path:
        return LocalJsonAttemptAdapter(self.root).attempt_path(
            batch_id, process_attempt_id
        )

    def load_complete(
        self, batch_id: str, process_attempt_id: str
    ) -> tuple[list[dict[str, Any]], dict[str, Any]] | None:
        path = self.attempt_path(batch_id, process_attempt_id)
        marker_path = path / "_SUCCESS"
        if not path.exists():
            return None
        if not marker_path.is_file() or not (path / "_delta_log").is_dir():
            raise StagingConflictError(f"partial Delta attempt staging exists at {path}")
        rows = self.spark_session.read.format("delta").load(str(path)).collect()
        records = _ordered_records(
            [json.loads(row["record_json"]) for row in rows]
        )
        marker = _read_json(marker_path, expected_type=dict)
        return records, dict(marker)

    def write_records(
        self,
        batch_id: str,
        process_attempt_id: str,
        records: Sequence[Mapping[str, Any]],
    ) -> Path:
        path = self.attempt_path(batch_id, process_attempt_id)
        if path.exists():
            raise StagingConflictError(f"attempt staging already exists at {path}")
        rows = [
            {
                "work_id": str(record["work_id"]),
                "attempt_id": str(record["attempt_id"]),
                "record_json": _canonical(dict(record)),
            }
            for record in records
        ]
        frame = self.spark_session.createDataFrame(rows)
        (
            frame.write.format("delta")
            .mode("errorifexists")
            .option("txnAppId", f"people-counter-sjd:{process_attempt_id}")
            .option("txnVersion", "0")
            .save(str(path))
        )
        return path

    def read_records(
        self, batch_id: str, process_attempt_id: str
    ) -> list[dict[str, Any]]:
        path = self.attempt_path(batch_id, process_attempt_id)
        if not (path / "_delta_log").is_dir():
            raise StagingConflictError(f"attempt Delta records are missing at {path}")
        rows = self.spark_session.read.format("delta").load(str(path)).collect()
        return _ordered_records(
            [json.loads(row["record_json"]) for row in rows]
        )

    def create_success(
        self,
        batch_id: str,
        process_attempt_id: str,
        marker: Mapping[str, Any],
    ) -> None:
        LocalJsonAttemptAdapter(self.root).create_success(
            batch_id, process_attempt_id, marker
        )


class FabricAttemptAdapter:
    """Explicit placeholder until a reviewed Fabric Lakehouse adapter exists."""

    def __init__(self, *_: object, **__: object) -> None:
        raise UnsupportedAttemptStoreError(
            "Fabric process attempt storage is unsupported by sjd_process"
        )


@dataclass(frozen=True)
class LiveBatch:
    status: str
    lease_expires_at: float


class ProcessControl:
    """Process-side fence/heartbeat facade over the authoritative control DB."""

    def __init__(self, store: SQLiteControlStore) -> None:
        self.store = store

    def live_batch(self, envelope: VerifiedEnvelope) -> LiveBatch:
        with sqlite3.connect(self.store.database) as connection:
            connection.row_factory = sqlite3.Row
            batch = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (envelope.batch_id,)
            ).fetchone()
            if batch is None:
                raise KeyError(envelope.batch_id)
            self._assert_rows(connection, envelope, str(batch["status"]))
            return LiveBatch(str(batch["status"]), float(batch["lease_expires_at"]))

    def assert_fence(self, envelope: VerifiedEnvelope) -> None:
        live = self.live_batch(envelope)
        if live.status == "COMMITTED":
            return
        if (
            live.status not in {"LEASED", "SEALED"}
            or live.lease_expires_at <= self.store._now()
        ):
            raise LeaseLostError(f"batch {envelope.batch_id} does not own a live fence")

    def heartbeat(self, envelope: VerifiedEnvelope, extension_seconds: float) -> None:
        extension = _positive_float(extension_seconds, "extension_seconds")
        now = self.store._now()
        with self.store._transaction() as connection:
            batch = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (envelope.batch_id,)
            ).fetchone()
            if (
                batch is None
                or batch["status"] != "LEASED"
                or float(batch["lease_expires_at"]) <= now
            ):
                raise LeaseLostError("heartbeat lost the batch lease")
            self._assert_rows(connection, envelope, "LEASED")
            expires = max(float(batch["lease_expires_at"]), now + extension)
            connection.execute(
                "UPDATE batches SET lease_expires_at = ? WHERE batch_id = ?",
                (expires, envelope.batch_id),
            )
            connection.execute(
                "UPDATE attempts SET lease_expires_at = ? "
                "WHERE batch_id = ? AND status = 'LEASED'",
                (expires, envelope.batch_id),
            )
            for item in envelope.items:
                updated = connection.execute(
                    "UPDATE work SET lease_expires_at = ?, updated_at = ? "
                    "WHERE work_id = ? AND status = 'LEASED' "
                    "AND lease_attempt_id = ? AND fence = ?",
                    (expires, now, item.work_id, item.attempt_id, item.fence),
                )
                if updated.rowcount != 1:
                    raise LeaseLostError(f"heartbeat lost fence for {item.work_id}")

    @staticmethod
    def _assert_rows(
        connection: sqlite3.Connection,
        envelope: VerifiedEnvelope,
        batch_status: str,
    ) -> None:
        rows = connection.execute(
            "SELECT a.attempt_id, a.work_id, a.fence, a.payload_sha256, a.status, "
            "w.status work_status, w.lease_attempt_id, w.fence work_fence, "
            "b.envelope_sha256, b.membership_sha256 "
            "FROM attempts a JOIN work w ON w.work_id = a.work_id "
            "JOIN batches b ON b.batch_id = a.batch_id WHERE a.batch_id = ?",
            (envelope.batch_id,),
        ).fetchall()
        expected = {
            (item.work_id, item.attempt_id, item.fence, item.payload_sha256)
            for item in envelope.items
        }
        observed = {
            (
                str(row["work_id"]),
                str(row["attempt_id"]),
                int(row["fence"]),
                str(row["payload_sha256"]),
            )
            for row in rows
        }
        if observed != expected:
            raise LeaseLostError("authoritative attempt membership changed")
        for row in rows:
            if (
                row["envelope_sha256"] != envelope.envelope_sha256
                or row["membership_sha256"] != envelope.membership_sha256
            ):
                raise BatchValidationError("authoritative envelope identity changed")
            if batch_status in {"LEASED", "SEALED"} and (
                row["work_status"] != "LEASED"
                or row["lease_attempt_id"] != row["attempt_id"]
                or int(row["work_fence"]) != int(row["fence"])
            ):
                raise LeaseLostError(f"attempt {row['attempt_id']} lost its work fence")


class _Heartbeat:
    def __init__(
        self,
        control: ProcessControl,
        envelope: VerifiedEnvelope,
        interval: float,
        extension: float,
    ) -> None:
        self.control = control
        self.envelope = envelope
        self.interval = interval
        self.extension = extension
        self._stop = threading.Event()
        self._error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, name="sjd-heartbeat", daemon=True)

    def __enter__(self) -> _Heartbeat:
        self.control.heartbeat(self.envelope, self.extension)
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        self._thread.join()
        self.raise_if_failed()

    def raise_if_failed(self) -> None:
        if self._error is not None:
            raise LeaseLostError(f"heartbeat failed: {self._error}") from self._error

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                self.control.heartbeat(self.envelope, self.extension)
            except BaseException as error:
                self._error = error
                self._stop.set()


def validate_staged_records(
    records: Sequence[Mapping[str, Any]],
    envelope: VerifiedEnvelope,
    plan: ExecutionPlan,
) -> dict[str, list[dict[str, Any]]]:
    """Strictly verify membership, terminal cardinality, provenance, and hashes."""
    expected = {(item.work_id, item.attempt_id): item for item in envelope.items}
    mapping = {
        (planned.item.work_id, planned.item.attempt_id): planned
        for bucket in plan.buckets
        for planned in bucket.items
    }
    by_identity = {identity: [] for identity in expected}
    logical: set[tuple[str, str, int]] = set()
    terminal = {identity: 0 for identity in expected}
    for raw in records:
        record = dict(raw)
        record_type, sequence, payload = _validate_staged_record_schema(record)
        identity = (
            _text(record.get("work_id"), "work_id"),
            _text(record.get("attempt_id"), "attempt_id"),
        )
        if identity not in expected:
            raise ProcessValidationError(f"unexpected staged identity: {identity!r}")
        item = expected[identity]
        planned = mapping[identity]
        key = (*identity, sequence)
        if key in logical:
            raise ProcessValidationError(f"duplicate staged logical record: {key!r}")
        logical.add(key)
        checks = {
            "batch_id": envelope.batch_id,
            "process_attempt_id": envelope.execution_attempt_id,
            "envelope_sha256": envelope.envelope_sha256,
            "manifest_sha256": envelope.envelope_sha256,
            "membership_sha256": envelope.membership_sha256,
            "fence": item.fence,
            "input_payload_sha256": item.payload_sha256,
            "config_sha256": item.config_sha256,
            "model_identity": _model_identity(item.payload),
            "release_digest": item.release_digest,
            "runtime_key": item.runtime_key,
            "source_video": item.payload.get("source_video"),
            "duration_seconds": item.duration_seconds,
            "planned_cost_seconds": planned.planned_cost_seconds,
            "detector_batch_size": item.payload.get("batch_size", 1),
            "bucket_id": planned.bucket_id,
            "wave_index": planned.wave_index,
            "physical_partition": planned.physical_partition,
            "physical_partition_id": planned.physical_partition,
            "partition_id": planned.physical_partition,
            "planned_concurrency": plan.concurrency.concurrency,
            "peak_rss_bytes": plan.concurrency.peak_rss_bytes,
            "duration_only_fallback": plan.concurrency.duration_only_fallback,
        }
        for name, value in checks.items():
            if record.get(name) != value:
                raise ProcessValidationError(
                    f"staged {name} mismatch for {identity!r}"
                )
        if record.get("record_payload_sha256") != _sha256(payload):
            raise ProcessValidationError(f"record payload hash mismatch for {identity!r}")
        if record_type in TERMINAL_TYPES:
            terminal[identity] += 1
            if (record_type == "error") != (record.get("status") == "FAILED"):
                raise ProcessValidationError(f"terminal status mismatch for {identity!r}")
        by_identity[identity].append(record)
    invalid = sorted(identity for identity, count in terminal.items() if count != 1)
    if invalid:
        raise ProcessValidationError(
            f"expected exactly one terminal record for {invalid!r}"
        )
    return {
        item.work_id: by_identity[(item.work_id, item.attempt_id)]
        for item in envelope.items
    }


def _validate_staged_record_schema(
    record: Mapping[str, Any],
) -> tuple[str, int, dict[str, Any]]:
    """Validate every required staged column before success can be marked."""
    text_fields = (
        "record_type",
        "work_id",
        "attempt_id",
        "source_video",
        "status",
        "payload_json",
        "emitted_at_utc",
        "batch_id",
        "process_attempt_id",
        "config_sha256",
        "release_digest",
        "runtime_key",
        "executor_identity",
        "executor_host",
    )
    sha_fields = (
        "envelope_sha256",
        "manifest_sha256",
        "membership_sha256",
        "input_payload_sha256",
        "record_payload_sha256",
        "model_identity",
    )
    positive_int_fields = (
        "fence",
        "planned_concurrency",
        "detector_batch_size",
        "cpu_threads",
    )
    nonnegative_int_fields = (
        "bucket_id",
        "wave_index",
        "physical_partition",
        "physical_partition_id",
        "stage_id",
        "partition_id",
        "task_attempt_id",
        "task_attempt_number",
        "record_sequence",
        "runtime_loads",
        "runtime_cache_hits",
    )
    for name in text_fields:
        _text(_required_staged_value(record, name), name)
    for name in sha_fields:
        _sha_text(_required_staged_value(record, name), name)
    for name in positive_int_fields:
        _positive_int(_required_staged_value(record, name), name)
    for name in nonnegative_int_fields:
        _nonnegative_int(_required_staged_value(record, name), name)
    for name in ("duration_seconds", "planned_cost_seconds"):
        _positive_float(_required_staged_value(record, name), name)
    _boolean(
        _required_staged_value(record, "duration_only_fallback"),
        "duration_only_fallback",
    )
    _validate_nullable_staged_value(record, "peak_rss_bytes", _positive_int)
    _validate_nullable_staged_value(record, "error_type", _text)
    _validate_nullable_staged_value(record, "error_message", _text)
    _validate_nullable_staged_value(record, "retryable", _boolean)
    _validate_nullable_staged_value(record, "processed_frames", _nonnegative_int)
    _validate_nullable_staged_value(
        record, "processing_seconds", _nonnegative_float
    )
    record_type = _text(record["record_type"], "record_type")
    _validate_staged_record_semantics(record, record_type)
    _validate_executor_provenance(record)
    payload = _staged_payload(record)
    return record_type, int(record["record_sequence"]), payload


def _validate_staged_record_semantics(
    record: Mapping[str, Any],
    record_type: str,
) -> None:
    if record_type not in RECORD_TYPES:
        raise ProcessValidationError(f"unsupported staged record_type: {record_type!r}")
    expected_status = "FAILED" if record_type == "error" else "SUCCEEDED"
    if record["status"] != expected_status:
        raise ProcessValidationError(
            f"staged status does not match record_type {record_type!r}"
        )
    if record_type == "error":
        _text(record["error_type"], "error_type")
        _text(record["error_message"], "error_message")
        _boolean(record["retryable"], "retryable")
    if record_type == "video_result":
        _nonnegative_int(record["processed_frames"], "processed_frames")
        _nonnegative_float(record["processing_seconds"], "processing_seconds")
    if record["detector_batch_size"] not in DETECTOR_BATCH_SIZES:
        raise ProcessValidationError(
            "detector_batch_size must be one of "
            f"{sorted(DETECTOR_BATCH_SIZES)!r}"
        )


def _validate_executor_provenance(record: Mapping[str, Any]) -> None:
    executor_identity = str(record["executor_identity"])
    identity_parts = executor_identity.split("@")
    if (
        len(identity_parts) != 2
        or not identity_parts[0]
        or identity_parts[1] != record["executor_host"]
    ):
        raise ProcessValidationError(
            "executor_identity must end with the staged executor_host"
        )


def _staged_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(str(record["payload_json"]))
    except json.JSONDecodeError as error:
        raise ProcessValidationError("payload_json must be valid JSON") from error
    if not isinstance(payload, dict):
        raise ProcessValidationError("payload_json must contain a JSON object")
    return payload


def _required_staged_value(record: Mapping[str, Any], name: str) -> Any:
    if name not in record:
        raise ProcessValidationError(f"staged record is missing required {name}")
    return record[name]


def _validate_nullable_staged_value(
    record: Mapping[str, Any],
    name: str,
    validator: Callable[[object, str], Any],
) -> None:
    value = _required_staged_value(record, name)
    if value is not None:
        validator(value, name)


def _boolean(value: object, name: str) -> bool:
    if type(value) is not bool:
        raise ProcessValidationError(f"{name} must be a boolean")
    return value


def _nonnegative_float(value: object, name: str) -> float:
    number = _finite(value, name)
    if number < 0:
        raise ProcessValidationError(f"{name} must be non-negative")
    return number


def _success_marker(
    envelope: VerifiedEnvelope,
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    failures = sorted(
        str(record["work_id"])
        for record in records
        if record["record_type"] == "error"
    )
    return {
        "schema_version": 1,
        "batch_id": envelope.batch_id,
        "process_attempt_id": envelope.execution_attempt_id,
        "envelope_sha256": envelope.envelope_sha256,
        "membership_sha256": envelope.membership_sha256,
        "record_count": len(records),
        "records_sha256": _sha256([dict(record) for record in records]),
        "failed_work_ids": failures,
    }


def _verify_marker(
    marker: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    envelope: VerifiedEnvelope,
) -> None:
    expected = _success_marker(envelope, records)
    if dict(marker) != expected:
        raise StagingConflictError("complete marker conflicts with staged records")


@dataclass(frozen=True)
class ProcessResult:
    batch_id: str
    process_attempt_id: str
    staging_path: Path
    record_count: int
    failed_work_ids: tuple[str, ...]
    publication_sequences: tuple[int, ...]
    resumed: bool


CrashHook = Callable[[str], None]


def run_process_batch(
    store: SQLiteControlStore,
    batch_id: str,
    profile: ExecutionProfile,
    mode: Mode,
    harness: ExecutionHarness,
    attempts: AttemptAdapter,
    *,
    peak_rss_bytes: int | None = None,
    clock: Callable[[], float] | None = None,
    crash_hook: CrashHook | None = None,
) -> ProcessResult:
    """Execute, validate, seal, publish, and only then settle one claimed batch."""
    if mode not in {"probe", "sdk"}:
        raise ProcessValidationError(f"unsupported processor mode: {mode!r}")
    envelope_data = store.load_claim_envelope(batch_id)
    with sqlite3.connect(store.database) as connection:
        row = connection.execute(
            "SELECT envelope_sha256 FROM batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
    if row is None:
        raise KeyError(batch_id)
    envelope = verify_envelope(
        envelope_data, batch_id=batch_id, envelope_sha256=str(row[0])
    )
    control = ProcessControl(store)
    live = control.live_batch(envelope)
    plan = plan_duration_lpt(
        envelope.items, profile, peak_rss_bytes=peak_rss_bytes
    )
    if live.status == "COMMITTED":
        complete = attempts.load_complete(batch_id, envelope.execution_attempt_id)
        if complete is None:
            raise StagingConflictError("committed batch is missing complete staging")
        records, marker = complete
        validate_staged_records(records, envelope, plan)
        _verify_marker(marker, records, envelope)
        sequences = store.commit_batch(batch_id)
        return _process_result(envelope, attempts, records, sequences, True)
    if live.status not in {"LEASED", "SEALED"}:
        raise LeaseLostError(f"batch {batch_id} is not processable: {live.status}")
    admit_lease(
        plan,
        profile,
        lease_expires_at=live.lease_expires_at,
        now=(store._now() if clock is None else clock()),
    )
    complete = attempts.load_complete(batch_id, envelope.execution_attempt_id)
    resumed = complete is not None
    if complete is None:
        with _Heartbeat(
            control,
            envelope,
            profile.heartbeat_seconds,
            max(
                profile.heartbeat_seconds * 3,
                plan.projected_makespan_seconds * profile.lease_safety_factor
                + profile.lease_margin_seconds,
            ),
        ) as heartbeat:
            records = harness.execute(envelope, plan, profile, mode)
            heartbeat.raise_if_failed()
            _crash(crash_hook, "after_action")
            validate_staged_records(records, envelope, plan)
            attempts.write_records(batch_id, envelope.execution_attempt_id, records)
            records = attempts.read_records(
                batch_id, envelope.execution_attempt_id
            )
            validate_staged_records(records, envelope, plan)
            _crash(crash_hook, "after_staging")
            control.assert_fence(envelope)
            marker = _success_marker(envelope, records)
            attempts.create_success(batch_id, envelope.execution_attempt_id, marker)
            _crash(crash_hook, "after_marker")
    else:
        records, marker = complete
        validate_staged_records(records, envelope, plan)
        _verify_marker(marker, records, envelope)
    control.assert_fence(envelope)
    grouped = validate_staged_records(records, envelope, plan)
    staging_path = attempts.attempt_path(batch_id, envelope.execution_attempt_id)
    outputs = []
    for item in envelope.items:
        item_records = grouped[item.work_id]
        terminal = next(
            record
            for record in item_records
            if record["record_type"] in TERMINAL_TYPES
        )
        outputs.append(
            {
                "work_id": item.work_id,
                "attempt_id": item.attempt_id,
                "succeeded": terminal["record_type"] == "video_result",
                "output_path": str(staging_path),
                "output_sha256": _sha256(item_records),
                "records": [
                    {
                        "executor_identity": record["executor_identity"],
                        "partition_id": record["partition_id"],
                        "task_attempt_id": record["task_attempt_id"],
                        "record_sequence": record["record_sequence"],
                    }
                    for record in item_records
                ],
            }
        )
    store.seal_batch(
        batch_id,
        outputs,
        envelope_sha256=envelope.envelope_sha256,
        membership_sha256=envelope.membership_sha256,
    )
    _crash(crash_hook, "after_seal")
    control.assert_fence(envelope)
    sequences = store.commit_batch(batch_id)
    _crash(crash_hook, "after_pointer")
    return _process_result(envelope, attempts, records, sequences, resumed)


def _process_result(
    envelope: VerifiedEnvelope,
    attempts: AttemptAdapter,
    records: Sequence[Mapping[str, Any]],
    sequences: tuple[int, ...],
    resumed: bool,
) -> ProcessResult:
    return ProcessResult(
        batch_id=envelope.batch_id,
        process_attempt_id=envelope.execution_attempt_id,
        staging_path=attempts.attempt_path(
            envelope.batch_id, envelope.execution_attempt_id
        ),
        record_count=len(records),
        failed_work_ids=tuple(
            sorted(
                str(record["work_id"])
                for record in records
                if record["record_type"] == "error"
            )
        ),
        publication_sequences=sequences,
        resumed=resumed,
    )


def _crash(hook: CrashHook | None, point: str) -> None:
    if hook is not None:
        hook(point)


def _read_json(path: Path, *, expected_type: type[Any]) -> Any:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise StagingConflictError(f"invalid immutable staging file {path}") from error
    if not isinstance(value, expected_type):
        raise StagingConflictError(f"invalid immutable staging shape in {path}")
    return value


def _ordered_records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized = [dict(record) for record in records]
    return sorted(
        normalized,
        key=lambda record: (
            str(record.get("work_id", "")),
            str(record.get("attempt_id", "")),
            str(record.get("record_type", "")),
            (
                (0, record["record_sequence"])
                if type(record.get("record_sequence")) is int
                else (1, repr(record.get("record_sequence")))
            ),
        ),
    )


def _model_identity(payload: Mapping[str, Any]) -> str:
    return _sha256(
        {
            key: payload.get(key)
            for key in (
                "pipeline",
                "detector_model",
                "model_format",
                "models_dir",
                "device_variant",
            )
        }
    )


def _canonical(value: Any) -> str:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        raise ProcessValidationError("value must be finite JSON data") from error


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProcessValidationError(f"{name} must be a non-empty string")
    return value


def _sha_text(value: object, name: str) -> str:
    text = _text(value, name)
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise ProcessValidationError(
            f"{name} must be 64 lowercase hexadecimal characters"
        )
    return text


def _finite(value: object, name: str) -> float:
    if type(value) not in (int, float) or not math.isfinite(float(value)):
        raise ProcessValidationError(f"{name} must be a finite number")
    return float(value)


def _positive_float(value: object, name: str) -> float:
    number = _finite(value, name)
    if number <= 0:
        raise ProcessValidationError(f"{name} must be greater than zero")
    return number


def _positive_int(value: object, name: str) -> int:
    if type(value) is not int or value < 1:
        raise ProcessValidationError(f"{name} must be a positive integer")
    return value


def _nonnegative_int(value: object, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ProcessValidationError(f"{name} must be a non-negative integer")
    return value


def _path_segment(value: str) -> str:
    text = _text(value, "path segment")
    if text in {".", ".."} or Path(text).name != text or "/" in text or "\\" in text:
        raise ProcessValidationError(f"unsafe path segment: {text!r}")
    return text


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--batch-id", required=True)
    run.add_argument("--profile", required=True)
    run.add_argument("--mode", choices=("probe", "sdk"), required=True)
    run.add_argument(
        "--database",
        type=Path,
        default=Path(
            os.environ.get(
                "PEOPLE_COUNTER_CONTROL_DB",
                ".people-counter/control.sqlite3",
            )
        ),
    )
    run.add_argument("--content-root", type=Path)
    run.add_argument(
        "--staging-root",
        type=Path,
        default=Path(os.environ.get("PEOPLE_COUNTER_STAGING_ROOT", ".people-counter/staging")),
    )
    run.add_argument("--harness", choices=("direct", "spark"), default="spark")
    run.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
    )
    run.add_argument("--peak-rss-mib", type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the local Candidate A process command."""
    arguments = _parser().parse_args(argv)
    profile = resolve_profile(arguments.profile)
    store = SQLiteControlStore(arguments.database, arguments.content_root)
    spark = None
    if arguments.harness == "direct":
        harness: ExecutionHarness = DirectExecutionHarness()
        attempts: AttemptAdapter = LocalJsonAttemptAdapter(arguments.staging_root)
    else:
        from people_counter.local_spark import create_local_spark_session

        spark = create_local_spark_session(
            master=arguments.spark_master,
            app_name=f"people-counter-process-{arguments.batch_id}",
            correlation_id=arguments.batch_id,
        )
        harness = SparkExecutionHarness(spark)
        attempts = SparkDeltaAttemptAdapter(arguments.staging_root, spark)
    try:
        result = run_process_batch(
            store,
            arguments.batch_id,
            profile,
            arguments.mode,
            harness,
            attempts,
            peak_rss_bytes=(
                None
                if arguments.peak_rss_mib is None
                else arguments.peak_rss_mib * MIB
            ),
        )
    finally:
        if spark is not None:
            spark.stop()
    print(_canonical({**asdict(result), "staging_path": str(result.staging_path)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
