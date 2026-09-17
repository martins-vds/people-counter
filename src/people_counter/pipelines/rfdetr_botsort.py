import time
from dataclasses import dataclass
from typing import Any

import cv2
import numpy as np
import supervision as sv
import torch
from people_counter.config import (
    DETECTOR_FLOOR,
    MIN_CONFIRMATION_FRAMES,
    RFDetrBotsortConfig,
    SamplingConfig,
    confirmed_entry_frame,
    lost_track_buffer_for_sample_rate,
    sampling_config,
)
from people_counter.line_counting import (
    BoTSORTCoastingCache,
    coasting_track_detections,
    create_line_zone,
    record_line_counts,
)
from people_counter.models import PersonTelemetry, RunResult
from people_counter.video import (
    FrameBatch,
    FrameReadState,
    VideoCapture,
    VideoMetadata,
    iter_sampled_frame_batches,
    read_video_metadata,
)
from rfdetr import RFDETRLarge
from trackers import BoTSORTTracker


@dataclass(frozen=True)
class RFDetrRuntime:
    model: RFDETRLarge


@dataclass(frozen=True)
class RFDetrRunState:
    config: RFDetrBotsortConfig
    runtime: RFDetrRuntime
    metadata: VideoMetadata
    sampling: SamplingConfig
    tracker: BoTSORTTracker
    lost_track_buffer: int
    line_track_cache: BoTSORTCoastingCache
    line_zone: Any


def load_runtime(config: RFDetrBotsortConfig) -> RFDetrRuntime:
    device = torch.device(config.device)
    model = RFDETRLarge(device=str(device))
    model.inference(
        compile=False,
        batch_size=config.batch_size,
        dtype=torch.float16 if config.use_fp16 else torch.float32,
        inplace=True,
    )
    return RFDetrRuntime(model=model)


def _notify_progress(config: RFDetrBotsortConfig) -> None:
    if config.progress_callback is not None:
        config.progress_callback(config.result)


def initialize_run_state(
    config: RFDetrBotsortConfig,
    runtime: RFDetrRuntime,
    metadata: VideoMetadata,
) -> RFDetrRunState:
    line_zone = create_line_zone(
        config.line,
        metadata.width,
        metadata.height,
    )
    sampling = sampling_config(
        metadata.fps,
        config.sample_fps,
        metadata.total_source_frames,
    )
    enable_cmc = (
        config.camera_motion_compensation
        if config.camera_motion_compensation is not None
        else sampling.interval == 1
    )
    lost_track_buffer = lost_track_buffer_for_sample_rate(
        sampling.effective_fps
    )
    tracker = BoTSORTTracker(
        frame_rate=metadata.fps,
        lost_track_buffer=lost_track_buffer,
        track_activation_threshold=config.detection_threshold,
        high_conf_det_threshold=config.detection_threshold,
        minimum_consecutive_frames=MIN_CONFIRMATION_FRAMES,
        instant_first_frame_activation=False,
        enable_cmc=enable_cmc,
    )
    result = config.result
    result.fps = metadata.fps
    result.total_source_frames = metadata.total_source_frames
    result.total_sampled_frames = sampling.total_sampled_frames
    result.sample_interval = sampling.interval
    result.effective_sample_fps = sampling.effective_fps
    result.batch_size = config.batch_size
    result.use_fp16 = config.use_fp16
    result.camera_motion_compensation = enable_cmc
    result.initialized = True
    _notify_progress(config)
    return RFDetrRunState(
        config=config,
        runtime=runtime,
        metadata=metadata,
        sampling=sampling,
        tracker=tracker,
        lost_track_buffer=lost_track_buffer,
        line_track_cache={},
        line_zone=line_zone,
    )


def _confirmed_people(
    state: RFDetrRunState,
    detections: Any,
    frame: np.ndarray,
    source_frame_index: int,
) -> sv.Detections:
    class_names = detections.data.get("class_name")
    if class_names is None:
        raise RuntimeError("RF-DETR detections did not include class names")
    people = detections[np.asarray(class_names) == "person"]
    tracked_people = state.tracker.update(
        people,
        frame=frame,
        timestamp=source_frame_index / state.metadata.fps,
    )
    if tracked_people.tracker_id is None:
        return sv.Detections.empty()
    return tracked_people[tracked_people.tracker_id >= 0]


def _record_telemetry(
    state: RFDetrRunState,
    confirmed_people: sv.Detections,
    source_frame_index: int,
) -> None:
    if confirmed_people.tracker_id is None:
        return
    for tracker_id in confirmed_people.tracker_id:
        person_id = int(tracker_id)
        record = state.config.result.telemetry.get(person_id)
        if record is None:
            state.config.result.telemetry[person_id] = PersonTelemetry(
                entry_frame=confirmed_entry_frame(
                    source_frame_index,
                    state.sampling.interval,
                ),
                last_seen_frame=source_frame_index,
            )
        else:
            record.last_seen_frame = source_frame_index


def process_frame(
    state: RFDetrRunState,
    source_frame_index: int,
    frame: np.ndarray,
    detections: Any,
) -> None:
    confirmed_people = _confirmed_people(
        state,
        detections,
        frame,
        source_frame_index,
    )
    if state.line_zone is not None:
        record_line_counts(
            state.line_zone,
            coasting_track_detections(
                confirmed_people,
                state.line_track_cache,
                source_frame_index,
                round(
                    (state.lost_track_buffer / 30) * state.metadata.fps
                ),
            ),
            source_frame_index,
            state.metadata.fps,
            state.config.line,
            state.config.result.line_counts,
        )
    _record_telemetry(state, confirmed_people, source_frame_index)


def process_frame_batch(
    state: RFDetrRunState,
    frame_batch: FrameBatch,
) -> None:
    source_frame_indices = [item[0] for item in frame_batch]
    frames = [item[1] for item in frame_batch]
    rgb_frames = [
        cv2.cvtColor(frame, cv2.COLOR_BGR2RGB) for frame in frames
    ]
    detections_batch = state.runtime.model.predict(
        rgb_frames,
        threshold=DETECTOR_FLOOR,
        include_source_image=False,
    )
    if not isinstance(detections_batch, list):
        raise RuntimeError(
            "RF-DETR returned an unexpected result for batched input"
        )
    for source_frame_index, frame, detections in zip(
        source_frame_indices,
        frames,
        detections_batch,
    ):
        process_frame(state, source_frame_index, frame, detections)
    state.config.result.processed_frames += len(frame_batch)
    _notify_progress(state.config)


def finalize_run(
    result: RunResult,
    capture: VideoCapture,
    read_state: FrameReadState,
    processing_started: float,
    line_zone: Any,
) -> None:
    capture.release()
    result.processing_seconds = time.perf_counter() - processing_started
    result.source_frames_read = read_state.source_frames_read
    result.ended_early = read_state.ended_early
    if result.initialized and line_zone is not None:
        result.line_in_count = line_zone.in_count
        result.line_out_count = line_zone.out_count


def run(config: RFDetrBotsortConfig) -> RunResult:
    """Run RF-DETR/BoT-SORT and mutate the result owned by ``config``."""
    config.result.ensure_unused()
    runtime = load_runtime(config)
    capture = cv2.VideoCapture(str(config.video))
    if not capture.isOpened():
        raise RuntimeError(f"Could not open input video: {config.video}")
    try:
        metadata = read_video_metadata(capture, config.video)
    except Exception:
        capture.release()
        raise
    read_state = FrameReadState()
    processing_started = time.perf_counter()
    line_zone = None
    try:
        state = initialize_run_state(config, runtime, metadata)
        line_zone = state.line_zone
        for frame_batch in iter_sampled_frame_batches(
            capture,
            state.sampling.interval,
            config.batch_size,
            metadata.total_source_frames,
            read_state,
        ):
            process_frame_batch(state, frame_batch)
    finally:
        finalize_run(
            config.result,
            capture,
            read_state,
            processing_started,
            line_zone,
        )
    return config.result
