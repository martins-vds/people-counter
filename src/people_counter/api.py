"""Stable public API for embedding people-counter in Python applications."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias, TypedDict

from people_counter.config import (
    RFDetrBotsortConfig,
    RTDetrOsnetConfig,
)
from people_counter.models import RunResult
from people_counter.video import format_video_timestamp

if TYPE_CHECKING:
    from people_counter.pipelines.rfdetr_botsort import RFDetrRuntime
    from people_counter.pipelines.rtdetr_osnet import RTDetrRuntime

PipelineConfig: TypeAlias = RTDetrOsnetConfig | RFDetrBotsortConfig
PipelineRuntime: TypeAlias = "RTDetrRuntime | RFDetrRuntime"


class TelemetryRecord(TypedDict):
    person_id: int
    entry_frame: int
    exit_frame: int
    entry_seconds: float
    exit_seconds: float
    entry_timestamp: str
    exit_timestamp: str
    duration_seconds: float


class LineCountRecordDict(TypedDict):
    frame: int
    video_seconds: str
    video_timestamp: str
    frame_in_count: int
    frame_out_count: int
    cumulative_in_count: int
    cumulative_out_count: int
    line_start_x: int
    line_start_y: int
    line_end_x: int
    line_end_y: int


def run(config: PipelineConfig) -> RunResult:
    """Run the pipeline selected by the concrete configuration type."""
    if isinstance(config, RTDetrOsnetConfig):
        from people_counter.pipelines.rtdetr_osnet import run as run_pipeline

        return run_pipeline(config)
    if isinstance(config, RFDetrBotsortConfig):
        from people_counter.pipelines.rfdetr_botsort import run as run_pipeline

        return run_pipeline(config)
    raise TypeError(
        "config must be RTDetrOsnetConfig or RFDetrBotsortConfig; "
        f"got {type(config).__name__}"
    )


def load_runtime(config: PipelineConfig) -> PipelineRuntime:
    """Load a reusable runtime selected by the concrete configuration type."""
    if isinstance(config, RTDetrOsnetConfig):
        from people_counter.pipelines.rtdetr_osnet import load_runtime

        return load_runtime(config)
    if isinstance(config, RFDetrBotsortConfig):
        from people_counter.pipelines.rfdetr_botsort import load_runtime

        return load_runtime(config)
    raise TypeError(
        "config must be RTDetrOsnetConfig or RFDetrBotsortConfig; "
        f"got {type(config).__name__}"
    )


def run_with_runtime(
    config: PipelineConfig,
    runtime: PipelineRuntime,
) -> RunResult:
    """Run one video with a compatible already-loaded runtime."""
    if isinstance(config, RTDetrOsnetConfig):
        from people_counter.pipelines.rtdetr_osnet import (
            RTDetrRuntime,
            run_with_runtime,
        )

        if not isinstance(runtime, RTDetrRuntime):
            raise TypeError("RTDetrOsnetConfig requires an RTDetrRuntime")
        return run_with_runtime(config, runtime)
    if isinstance(config, RFDetrBotsortConfig):
        from people_counter.pipelines.rfdetr_botsort import (
            RFDetrRuntime,
            run_with_runtime,
        )

        if not isinstance(runtime, RFDetrRuntime):
            raise TypeError("RFDetrBotsortConfig requires an RFDetrRuntime")
        return run_with_runtime(config, runtime)
    raise TypeError(
        "config must be RTDetrOsnetConfig or RFDetrBotsortConfig; "
        f"got {type(config).__name__}"
    )


def telemetry_records(result: RunResult) -> list[TelemetryRecord]:
    """Convert person telemetry into typed, DataFrame-ready records."""
    _ensure_initialized(result)
    if result.fps <= 0:
        raise ValueError("RunResult fps must be greater than zero")

    records: list[TelemetryRecord] = []
    for person_id in sorted(result.telemetry):
        telemetry = result.telemetry[person_id]
        entry_seconds = telemetry.entry_frame / result.fps
        exit_seconds = telemetry.last_seen_frame / result.fps
        records.append(
            {
                "person_id": person_id,
                "entry_frame": telemetry.entry_frame,
                "exit_frame": telemetry.last_seen_frame,
                "entry_seconds": entry_seconds,
                "exit_seconds": exit_seconds,
                "entry_timestamp": format_video_timestamp(
                    telemetry.entry_frame,
                    result.fps,
                ),
                "exit_timestamp": format_video_timestamp(
                    telemetry.last_seen_frame,
                    result.fps,
                ),
                "duration_seconds": exit_seconds - entry_seconds,
            }
        )
    return records


def line_count_records(result: RunResult) -> list[LineCountRecordDict]:
    """Convert line counts into typed, DataFrame-ready records."""
    _ensure_initialized(result)
    return [
        {
            "frame": record.frame,
            "video_seconds": record.video_seconds,
            "video_timestamp": record.video_timestamp,
            "frame_in_count": record.frame_in_count,
            "frame_out_count": record.frame_out_count,
            "cumulative_in_count": record.cumulative_in_count,
            "cumulative_out_count": record.cumulative_out_count,
            "line_start_x": record.line_start_x,
            "line_start_y": record.line_start_y,
            "line_end_x": record.line_end_x,
            "line_end_y": record.line_end_y,
        }
        for record in result.line_counts
    ]


def _ensure_initialized(result: RunResult) -> None:
    if not result.initialized:
        raise RuntimeError(
            "RunResult is not initialized; run a pipeline before conversion"
        )
