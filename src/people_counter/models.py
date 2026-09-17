"""Typed data models shared by the counting pipelines."""

from dataclasses import dataclass, field

import numpy as np
from numpy.typing import NDArray


BBox = tuple[int, int, int, int]
Centroid = tuple[int, int]
Embedding = NDArray[np.floating]
LineCoordinates = tuple[int, int, int, int]


@dataclass(frozen=True)
class Detection:
    bbox: BBox
    centroid: Centroid
    confidence: float
    embedding: Embedding | None = None


@dataclass(frozen=True)
class LastGeometry:
    centroid: Centroid
    bbox: BBox


@dataclass
class TrackProfile:
    centroid: Centroid
    embedding: Embedding
    age: int
    bbox: BBox
    first_seen_frame: int
    hits: int


@dataclass
class PersonTelemetry:
    entry_frame: int
    last_seen_frame: int
    last_geometry: LastGeometry | None = None


@dataclass(frozen=True)
class LineCountRecord:
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


@dataclass
class RunResult:
    """Mutable result container for one pipeline invocation only."""

    telemetry: dict[int, PersonTelemetry] = field(default_factory=dict)
    line_counts: list[LineCountRecord] = field(default_factory=list)
    started: bool = False
    initialized: bool = False
    fps: float = 0.0
    total_source_frames: int = 0
    total_sampled_frames: int = 0
    source_frames_read: int = 0
    processed_frames: int = 0
    sample_interval: int = 1
    effective_sample_fps: float = 0.0
    processing_seconds: float = 0.0
    ended_early: bool = False
    line_in_count: int = 0
    line_out_count: int = 0
    batch_size: int = 1
    use_fp16: bool = False
    camera_motion_compensation: bool | None = None

    def ensure_unused(self) -> None:
        if self.started or self.initialized:
            raise RuntimeError(
                "RunResult already populated; create a new config for each run"
            )
        self.started = True
