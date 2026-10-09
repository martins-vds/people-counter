"""Pure parser for Fabric Spark event logs into typed telemetry records.

Spark (and Fabric's managed Spark runtime) can write an application event
log as newline-delimited JSON, one Spark listener event per line. This
module turns that log into typed, immutable executor/task records plus an
observed task-overlap measurement, entirely offline and without depending
on a live ``SparkContext``. When an expected field is absent from a given
event (for example, because a particular Spark/Fabric runtime build does
not populate it), the corresponding record field is explicitly ``None``
with a recorded reason -- it is never fabricated as zero or omitted
silently.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass


class SparkEventLogError(ValueError):
    """A Spark event log line could not be parsed as a JSON object."""


_REQUIRED_FIELD_MISSING = "missing-required-field"
_ZERO_TIMESTAMP_STAND_IN = "zero-placeholder-timestamp"


@dataclass(frozen=True)
class ExecutorEventRecord:
    """One observed executor add/remove event from the event log."""

    executor_id: str | None
    event_type: str  # "added" or "removed"
    host: str | None
    total_cores: int | None
    timestamp_ms: int | None
    removed_reason: str | None = None
    missing_field_reasons: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if self.event_type not in {"added", "removed"}:
            raise SparkEventLogError(
                f"unsupported executor event type: {self.event_type!r}"
            )


@dataclass(frozen=True)
class TaskEventRecord:
    """One observed task-end event with metrics, or an explicit null reason."""

    stage_id: int | None
    task_id: int | None
    executor_id: str | None
    launch_time_ms: int | None
    finish_time_ms: int | None
    failed: bool
    speculative: bool
    executor_run_time_ms: int | None
    jvm_gc_time_ms: int | None
    memory_bytes_spilled: int | None
    disk_bytes_spilled: int | None
    input_bytes_read: int | None
    output_bytes_written: int | None
    missing_metric_reasons: tuple[tuple[str, str], ...] = ()
    missing_field_reasons: tuple[tuple[str, str], ...] = ()

    @property
    def duration_ms(self) -> int | None:
        if self.launch_time_ms is None or self.finish_time_ms is None:
            return None
        return self.finish_time_ms - self.launch_time_ms


@dataclass(frozen=True)
class TaskOverlapSummary:
    """Observed concurrent-task overlap; never extrapolated."""

    max_concurrent_tasks: int
    total_wall_ms: int
    total_busy_task_ms: int

    @property
    def observed_parallelism(self) -> float:
        """Busy-task-time / wall-time; the actual measured overlap factor."""
        if self.total_wall_ms <= 0:
            return 0.0
        return self.total_busy_task_ms / self.total_wall_ms


@dataclass(frozen=True)
class SparkEventLogSummary:
    """Fully parsed, typed contents of one Spark application event log."""

    executors: tuple[ExecutorEventRecord, ...]
    tasks: tuple[TaskEventRecord, ...]
    unrecognized_event_types: tuple[str, ...]
    task_cpu_values: tuple[int, ...] = ()


def _optional_int(metrics: Mapping[str, object], key: str) -> tuple[int | None, str | None]:
    if key not in metrics:
        return None, f"{key!r} absent from Task Metrics in this event log"
    value = metrics[key]
    if not isinstance(value, int) or isinstance(value, bool):
        return None, f"{key!r} present but not an integer ({value!r})"
    return value, None


def _mapping_or_empty(
    container: Mapping[str, object],
    key: str,
    *,
    event_type: str,
) -> Mapping[str, object]:
    if key not in container or container[key] is None:
        return {}
    value = container[key]
    if not isinstance(value, Mapping):
        raise SparkEventLogError(
            f"{event_type} field {key!r} must be a JSON object when present"
        )
    return value


def _required_int(
    container: Mapping[str, object],
    key: str,
    *,
    event_type: str,
    reasons: list[tuple[str, str]],
) -> int | None:
    if key not in container or container[key] is None:
        reasons.append((key, _REQUIRED_FIELD_MISSING))
        return None
    value = container[key]
    if not isinstance(value, int) or isinstance(value, bool):
        raise SparkEventLogError(
            f"{event_type} field {key!r} must be an integer when present"
        )
    return value


def _required_timestamp_ms(
    container: Mapping[str, object],
    key: str,
    *,
    event_type: str,
    reasons: list[tuple[str, str]],
) -> int | None:
    value = _required_int(container, key, event_type=event_type, reasons=reasons)
    if value is None:
        return None
    if value < 0:
        raise SparkEventLogError(
            f"{event_type} field {key!r} must be a non-negative integer timestamp"
        )
    if value == 0:
        reasons.append((key, _ZERO_TIMESTAMP_STAND_IN))
        return None
    return value


def _required_text(
    container: Mapping[str, object],
    key: str,
    *,
    reasons: list[tuple[str, str]],
) -> str | None:
    if key not in container or container[key] is None:
        reasons.append((key, _REQUIRED_FIELD_MISSING))
        return None
    value = str(container[key]).strip()
    if not value:
        reasons.append((key, _REQUIRED_FIELD_MISSING))
        return None
    return value


def parse_spark_event_log_lines(
    lines: Iterable[str],
) -> SparkEventLogSummary:
    """Parse newline-delimited Spark listener JSON events into typed records.

    Blank lines are skipped. A line that is present but not valid JSON
    raises :class:`SparkEventLogError` -- partial/corrupt event logs must
    fail closed rather than silently dropping events.
    """
    executors: list[ExecutorEventRecord] = []
    tasks: list[TaskEventRecord] = []
    unrecognized: list[str] = []
    task_cpu_values: list[int] = []
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as error:
            raise SparkEventLogError(f"invalid JSON event line: {error}") from error
        if not isinstance(event, Mapping):
            raise SparkEventLogError("event log line did not decode to a JSON object")
        event_type = str(event.get("Event", ""))
        if event_type == "SparkListenerExecutorAdded":
            info = _mapping_or_empty(
                event, "Executor Info", event_type=event_type
            )
            missing_fields: list[tuple[str, str]] = []
            executors.append(
                ExecutorEventRecord(
                    executor_id=_required_text(
                        event,
                        "Executor ID",
                        reasons=missing_fields,
                    ),
                    event_type="added",
                    host=info.get("Host"),
                    total_cores=info.get("Total Cores"),
                    timestamp_ms=_required_timestamp_ms(
                        event,
                        "Timestamp",
                        event_type=event_type,
                        reasons=missing_fields,
                    ),
                    missing_field_reasons=tuple(missing_fields),
                )
            )
        elif event_type == "SparkListenerExecutorRemoved":
            missing_fields = []
            executors.append(
                ExecutorEventRecord(
                    executor_id=_required_text(
                        event,
                        "Executor ID",
                        reasons=missing_fields,
                    ),
                    event_type="removed",
                    host=None,
                    total_cores=None,
                    timestamp_ms=_required_timestamp_ms(
                        event,
                        "Timestamp",
                        event_type=event_type,
                        reasons=missing_fields,
                    ),
                    removed_reason=event.get("Removed Reason"),
                    missing_field_reasons=tuple(missing_fields),
                )
            )
        elif event_type == "SparkListenerTaskEnd":
            info = _mapping_or_empty(event, "Task Info", event_type=event_type)
            metrics = _mapping_or_empty(event, "Task Metrics", event_type=event_type)
            input_metrics = _mapping_or_empty(
                metrics, "Input Metrics", event_type=event_type
            )
            output_metrics = _mapping_or_empty(
                metrics, "Output Metrics", event_type=event_type
            )
            missing_metric_reasons: list[tuple[str, str]] = []
            missing_field_reasons: list[tuple[str, str]] = []

            def _field(container: Mapping[str, object], key: str) -> int | None:
                value, reason = _optional_int(container, key)
                if reason is not None:
                    missing_metric_reasons.append((key, reason))
                return value

            launch_time_ms = _required_timestamp_ms(
                info,
                "Launch Time",
                event_type=event_type,
                reasons=missing_field_reasons,
            )
            finish_time_ms = _required_timestamp_ms(
                info,
                "Finish Time",
                event_type=event_type,
                reasons=missing_field_reasons,
            )
            if (
                launch_time_ms is not None
                and finish_time_ms is not None
                and finish_time_ms < launch_time_ms
            ):
                raise SparkEventLogError(
                    "SparkListenerTaskEnd has negative duration: "
                    f"finish time {finish_time_ms} precedes launch time {launch_time_ms}"
                )

            tasks.append(
                TaskEventRecord(
                    stage_id=_required_int(
                        event,
                        "Stage ID",
                        event_type=event_type,
                        reasons=missing_field_reasons,
                    ),
                    task_id=_required_int(
                        info,
                        "Task ID",
                        event_type=event_type,
                        reasons=missing_field_reasons,
                    ),
                    executor_id=_required_text(
                        info,
                        "Executor ID",
                        reasons=missing_field_reasons,
                    ),
                    launch_time_ms=launch_time_ms,
                    finish_time_ms=finish_time_ms,
                    failed=bool(info.get("Failed", False)),
                    speculative=bool(info.get("Speculative", False)),
                    executor_run_time_ms=_field(metrics, "Executor Run Time"),
                    jvm_gc_time_ms=_field(metrics, "JVM GC Time"),
                    memory_bytes_spilled=_field(metrics, "Memory Bytes Spilled"),
                    disk_bytes_spilled=_field(metrics, "Disk Bytes Spilled"),
                    input_bytes_read=_field(input_metrics, "Bytes Read"),
                    output_bytes_written=_field(output_metrics, "Bytes Written"),
                    missing_metric_reasons=tuple(missing_metric_reasons),
                    missing_field_reasons=tuple(missing_field_reasons),
                )
            )
        elif event_type == "SparkListenerResourceProfileAdded":
            resources = _mapping_or_empty(
                event,
                "Task Resource Requests",
                event_type=event_type,
            )
            cpu = resources.get("cpus")
            if not isinstance(cpu, Mapping):
                raise SparkEventLogError(
                    "resource profile has no task cpus request"
                )
            amount = cpu.get("Amount")
            if (
                not isinstance(amount, (int, float))
                or isinstance(amount, bool)
                or amount <= 0
                or int(amount) != amount
            ):
                raise SparkEventLogError(
                    "task cpus resource amount must be a positive integer"
                )
            task_cpu_values.append(int(amount))
        elif event_type == "SparkListenerEnvironmentUpdate":
            properties = _mapping_or_empty(
                event,
                "Spark Properties",
                event_type=event_type,
            )
            if "spark.task.cpus" in properties:
                try:
                    value = int(str(properties["spark.task.cpus"]))
                except ValueError as error:
                    raise SparkEventLogError(
                        "spark.task.cpus event property is not an integer"
                    ) from error
                if value < 1:
                    raise SparkEventLogError(
                        "spark.task.cpus event property must be positive"
                    )
                task_cpu_values.append(value)
        else:
            unrecognized.append(event_type)
    return SparkEventLogSummary(
        executors=tuple(executors),
        tasks=tuple(tasks),
        unrecognized_event_types=tuple(unrecognized),
        task_cpu_values=tuple(task_cpu_values),
    )


def compute_task_overlap(tasks: Sequence[TaskEventRecord]) -> TaskOverlapSummary:
    """Compute actual observed concurrent-task overlap; never extrapolated.

    Uses a sweep-line over task launch/finish timestamps so the result
    reflects genuinely observed concurrency, never an assumed or perfectly
    scaled value. Speculative-execution duplicates are excluded (this
    project always runs with speculation disabled, but the exclusion keeps
    the measurement correct if a log is ever ingested from elsewhere).
    """
    non_speculative = [task for task in tasks if not task.speculative]
    if not non_speculative:
        return TaskOverlapSummary(
            max_concurrent_tasks=0, total_wall_ms=0, total_busy_task_ms=0
        )
    if any(
        task.launch_time_ms is None or task.finish_time_ms is None
        for task in non_speculative
    ):
        raise SparkEventLogError(
            "cannot compute task overlap with missing required task timestamps"
        )
    events: list[tuple[int, int]] = []
    for task in non_speculative:
        duration_ms = task.duration_ms
        if duration_ms is None:
            raise SparkEventLogError(
                "cannot compute task overlap with missing required task timestamps"
            )
        if duration_ms < 0:
            raise SparkEventLogError(
                f"task {task.task_id!r} has negative duration {duration_ms}"
            )
        events.append((task.launch_time_ms, 1))
        events.append((task.finish_time_ms, -1))
    events.sort(key=lambda item: (item[0], item[1]))
    concurrent = 0
    max_concurrent = 0
    for _, delta in events:
        concurrent += delta
        max_concurrent = max(max_concurrent, concurrent)
    total_wall_ms = max(task.finish_time_ms for task in non_speculative) - min(
        task.launch_time_ms for task in non_speculative
    )
    total_busy_task_ms = sum(task.duration_ms or 0 for task in non_speculative)
    return TaskOverlapSummary(
        max_concurrent_tasks=max_concurrent,
        total_wall_ms=max(total_wall_ms, 0),
        total_busy_task_ms=max(total_busy_task_ms, 0),
    )


def require_complete_event_evidence(
    summary: SparkEventLogSummary,
    *,
    expected_executor_ids: Sequence[str],
    expected_task_cpus: int,
    expected_task_count: int,
) -> TaskOverlapSummary:
    """Fail closed unless one workload has complete resource/task evidence."""
    expected_ids = set(expected_executor_ids)
    if not expected_ids:
        raise SparkEventLogError("expected_executor_ids must not be empty")
    if expected_task_cpus < 1 or expected_task_count < 1:
        raise SparkEventLogError(
            "expected task CPU width and task count must be positive"
        )
    added_ids = {
        event.executor_id
        for event in summary.executors
        if event.event_type == "added" and event.executor_id is not None
    }
    if added_ids != expected_ids:
        raise SparkEventLogError(
            f"event-log executor IDs differ: expected {sorted(expected_ids)!r}, "
            f"observed {sorted(added_ids)!r}"
        )
    if any(event.missing_field_reasons for event in summary.executors):
        raise SparkEventLogError("executor event evidence is incomplete")
    if len(summary.tasks) != expected_task_count:
        raise SparkEventLogError(
            f"event-log task count differs: expected {expected_task_count}, "
            f"observed {len(summary.tasks)}"
        )
    if not summary.task_cpu_values or set(summary.task_cpu_values) != {
        expected_task_cpus
    }:
        raise SparkEventLogError(
            "event-log task CPU evidence is absent or inconsistent"
        )
    for task in summary.tasks:
        if (
            task.missing_field_reasons
            or task.missing_metric_reasons
            or task.executor_id not in expected_ids
            or task.failed
            or task.speculative
        ):
            raise SparkEventLogError(
                f"task event evidence is incomplete or unsafe for task {task.task_id!r}"
            )
    return compute_task_overlap(summary.tasks)
