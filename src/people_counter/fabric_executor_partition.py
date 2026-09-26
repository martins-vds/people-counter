"""Helpers for opt-in Fabric Spark executor-partition video inference."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Iterator, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Protocol, TypedDict

from people_counter.api import (
    PipelineConfig,
    PipelineRuntime,
    line_count_records,
    load_runtime,
    run_with_runtime,
    telemetry_records,
)
from people_counter.config import RFDetrBotsortConfig, RTDetrOsnetConfig
from people_counter.cpu_runtime import (
    ThreadBudget,
    calculate_thread_budget,
    configure_cpu_runtime,
)
from people_counter.models import RunResult


RecordType = Literal["video_result", "telemetry", "line_count", "error"]
RecordStatus = Literal["SUCCEEDED", "FAILED"]


class ExecutorPartitionRecord(TypedDict):
    record_type: RecordType
    work_id: str
    attempt_id: str | None
    source_video: str
    status: RecordStatus
    payload_json: str
    error_type: str | None
    error_message: str | None
    retryable: bool | None
    processed_frames: int | None
    processing_seconds: float | None
    emitted_at_utc: datetime


class RuntimeProcessor(Protocol):
    def run(self, config: PipelineConfig) -> RunResult:
        """Process one video with any executor-local runtime cache."""


ErrorClassifier = Callable[[Exception], tuple[bool, str]]
ConfigBuilder = Callable[[Mapping[str, Any]], PipelineConfig]


class ExecutorRuntimeCache:
    """Executor-local cache for model runtimes keyed by pipeline settings."""

    def __init__(self) -> None:
        self._items: dict[tuple[object, ...], PipelineRuntime] = {}

    def get_or_load(
        self,
        key: tuple[object, ...],
        loader: Callable[[], PipelineRuntime],
    ) -> PipelineRuntime:
        runtime = self._items.get(key)
        if runtime is None:
            runtime = loader()
            self._items[key] = runtime
        return runtime

    def clear(self) -> None:
        self._items.clear()


class SdkRuntimeProcessor:
    """Reuse compatible SDK runtimes within one executor Python process."""

    def __init__(self, cache: ExecutorRuntimeCache | None = None) -> None:
        self._cache = cache or ExecutorRuntimeCache()

    def run(self, config: PipelineConfig) -> RunResult:
        runtime = self._cache.get_or_load(
            _runtime_cache_key(config),
            lambda: load_runtime(config),
        )
        return run_with_runtime(config, runtime)


def _runtime_cache_key(config: PipelineConfig) -> tuple[object, ...]:
    """Return the model-construction settings that define runtime compatibility."""
    if isinstance(config, RTDetrOsnetConfig):
        return (
            "rtdetr-osnet",
            config.device_variant,
            config.device,
            config.detector_model,
            _normalized_path(config.models_dir),
        )
    if isinstance(config, RFDetrBotsortConfig):
        return (
            "rfdetr-botsort",
            config.device_variant,
            config.device,
            config.batch_size,
            config.use_fp16,
            _normalized_path(config.models_dir),
        )
    raise TypeError(
        "config must be RTDetrOsnetConfig or RFDetrBotsortConfig; "
        f"got {type(config).__name__}"
    )


def executor_partition_schema() -> Any:
    """Return the explicit PySpark schema for partition result records."""
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("record_type", T.StringType(), nullable=False),
            T.StructField("work_id", T.StringType(), nullable=False),
            T.StructField("attempt_id", T.StringType(), nullable=True),
            T.StructField("source_video", T.StringType(), nullable=False),
            T.StructField("status", T.StringType(), nullable=False),
            T.StructField("payload_json", T.StringType(), nullable=False),
            T.StructField("error_type", T.StringType(), nullable=True),
            T.StructField("error_message", T.StringType(), nullable=True),
            T.StructField("retryable", T.BooleanType(), nullable=True),
            T.StructField("processed_frames", T.LongType(), nullable=True),
            T.StructField("processing_seconds", T.DoubleType(), nullable=True),
            T.StructField("emitted_at_utc", T.TimestampType(), nullable=False),
        ]
    )


def process_video_partition(
    rows: Iterable[Mapping[str, Any]],
    *,
    config_builder: ConfigBuilder,
    processor: RuntimeProcessor | None = None,
    error_classifier: ErrorClassifier | None = None,
) -> Iterator[ExecutorPartitionRecord]:
    """Process whole videos sequentially and emit compact partition records."""
    runner = processor if processor is not None else SdkRuntimeProcessor()
    for row in rows:
        work = dict(row)
        try:
            config = config_builder(work)
            result = runner.run(config)
        except Exception as error:
            retryable, category = (
                error_classifier(error)
                if error_classifier is not None
                else (True, "RUNTIME")
            )
            yield _record(
                "error",
                work,
                "FAILED",
                {
                    "error_category": category,
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                },
                error_type=type(error).__name__,
                error_message=str(error),
                retryable=retryable,
            )
            continue

        summary = {
            "total_source_frames": result.total_source_frames,
            "total_sampled_frames": result.total_sampled_frames,
            "source_frames_read": result.source_frames_read,
            "effective_sample_fps": result.effective_sample_fps,
            "line_in_count": result.line_in_count,
            "line_out_count": result.line_out_count,
            "ended_early": result.ended_early,
        }
        yield _record(
            "video_result",
            work,
            "SUCCEEDED",
            summary,
            processed_frames=result.processed_frames,
            processing_seconds=result.processing_seconds,
        )
        for item in telemetry_records(result):
            yield _record("telemetry", work, "SUCCEEDED", item)
        for item in line_count_records(result):
            yield _record("line_count", work, "SUCCEEDED", item)


def _record(
    record_type: RecordType,
    work: Mapping[str, Any],
    status: RecordStatus,
    payload: Mapping[str, Any],
    *,
    error_type: str | None = None,
    error_message: str | None = None,
    retryable: bool | None = None,
    processed_frames: int | None = None,
    processing_seconds: float | None = None,
) -> ExecutorPartitionRecord:
    work_id = _required_text(work, "work_id")
    return {
        "record_type": record_type,
        "work_id": work_id,
        "attempt_id": _optional_text(work.get("attempt_id")),
        "source_video": _source_video(work),
        "status": status,
        "payload_json": json.dumps(
            payload,
            default=_json_default,
            separators=(",", ":"),
            sort_keys=True,
        ),
        "error_type": error_type,
        "error_message": error_message,
        "retryable": retryable,
        "processed_frames": processed_frames,
        "processing_seconds": processing_seconds,
        "emitted_at_utc": datetime.now(timezone.utc),
    }


def _required_text(work: Mapping[str, Any], name: str) -> str:
    value = work.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("attempt_id must be null or a non-empty string")
    return value.strip()


def _source_video(work: Mapping[str, Any]) -> str:
    for name in ("source_video", "video", "source_uri"):
        value = work.get(name)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, Path):
            return str(value)
    raise ValueError("source_video, video, or source_uri must identify the video")


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _normalized_path(path: Path | None) -> str | None:
    return str(path.resolve()) if path is not None else None
