"""Video metadata, sampled frame reading, and timestamp helpers."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Protocol, TypeAlias

import cv2
from numpy.typing import NDArray


Frame: TypeAlias = NDArray[Any]
SampledFrame: TypeAlias = tuple[int, Frame]
FrameBatch: TypeAlias = list[SampledFrame]


class VideoCapture(Protocol):
    def isOpened(self) -> bool: ...
    def read(self) -> tuple[bool, Frame | None]: ...
    def grab(self) -> bool: ...
    def get(self, property_id: int) -> float: ...
    def release(self) -> None: ...


@dataclass
class FrameReadState:
    source_frames_read: int = 0
    ended_early: bool = False


@dataclass(frozen=True)
class VideoMetadata:
    fps: float
    width: int
    height: int
    total_source_frames: int


def read_video_metadata(
    capture: VideoCapture,
    input_path: Path,
) -> VideoMetadata:
    fps = capture.get(cv2.CAP_PROP_FPS)
    width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
    if fps <= 0 or width <= 0 or height <= 0:
        raise RuntimeError(f"Invalid video metadata for input: {input_path}")
    return VideoMetadata(
        fps=fps,
        width=width,
        height=height,
        total_source_frames=int(capture.get(cv2.CAP_PROP_FRAME_COUNT)),
    )


def iter_sampled_frame_batches(
    capture: VideoCapture,
    sample_interval: int,
    batch_size: int,
    expected_source_frames: int,
    read_state: FrameReadState,
) -> Iterator[FrameBatch]:
    batch: FrameBatch = []
    source_frame_index = 0

    while capture.isOpened():
        if source_frame_index % sample_interval == 0:
            success, frame = capture.read()
        else:
            success = capture.grab()
            frame = None
        if not success:
            read_state.ended_early = (
                expected_source_frames > 0
                and source_frame_index < expected_source_frames - 1
            )
            break

        read_state.source_frames_read = source_frame_index + 1
        if frame is not None:
            batch.append((source_frame_index, frame))
            if len(batch) == batch_size:
                yield batch
                batch = []
        source_frame_index += 1

    if batch:
        yield batch


def format_video_timestamp(frame_index: int, fps: float) -> str:
    total_milliseconds = round((frame_index / fps) * 1000)
    hours, remainder = divmod(total_milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, milliseconds = divmod(remainder, 1000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds:03d}"
