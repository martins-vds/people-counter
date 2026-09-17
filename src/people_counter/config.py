"""Typed pipeline configuration and sampling computations."""

import math
from dataclasses import dataclass, field
from pathlib import Path
from collections.abc import Callable
from typing import Literal

from people_counter.models import LineCoordinates, RunResult


DETECTOR_FLOOR = 0.1
MAX_DISAPPEARED_SECONDS = 1.0
MIN_CONFIRMATION_FRAMES = 2

DeviceVariant = Literal["cpu", "gpu"]
ProgressCallback = Callable[[RunResult], None]


@dataclass(frozen=True)
class SamplingConfig:
    interval: int
    effective_fps: float
    total_sampled_frames: int


@dataclass(frozen=True)
class CommonPipelineConfig:
    video: Path
    device_variant: DeviceVariant
    device: str
    batch_size: int
    sample_fps: float | None = 3.0
    detection_threshold: float = 0.6
    use_fp16: bool = False
    line: LineCoordinates | None = None
    result: RunResult = field(
        default_factory=RunResult,
        repr=False,
        compare=False,
    )
    progress_callback: ProgressCallback | None = field(
        default=None,
        repr=False,
        compare=False,
    )


@dataclass(frozen=True)
class RTDetrOsnetConfig(CommonPipelineConfig):
    detector_model: Literal["r18", "r50"] = "r18"


@dataclass(frozen=True)
class RFDetrBotsortConfig(CommonPipelineConfig):
    camera_motion_compensation: bool | None = None


def sampling_config(
    source_fps: float,
    requested_sample_fps: float | None,
    total_source_frames: int,
) -> SamplingConfig:
    requested_fps = requested_sample_fps or source_fps
    interval = max(1, round(source_fps / min(requested_fps, source_fps)))
    return SamplingConfig(
        interval=interval,
        effective_fps=source_fps / interval,
        total_sampled_frames=math.ceil(
            max(0, total_source_frames) / interval
        ),
    )


def retention_seconds_for_sample_rate(effective_sample_fps: float) -> float:
    return max(MAX_DISAPPEARED_SECONDS, 2.0 / effective_sample_fps)


def disappeared_frames_for_sample_rate(effective_sample_fps: float) -> int:
    return round(
        retention_seconds_for_sample_rate(effective_sample_fps)
        * effective_sample_fps
    )


def lost_track_buffer_for_sample_rate(effective_sample_fps: float) -> int:
    return math.ceil(
        30 * retention_seconds_for_sample_rate(effective_sample_fps)
    )


def confirmed_entry_frame(
    source_frame_index: int,
    sample_interval: int,
    confirmation_frames: int = MIN_CONFIRMATION_FRAMES,
) -> int:
    return max(
        0,
        source_frame_index - (confirmation_frames - 1) * sample_interval,
    )
