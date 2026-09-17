import time

import cv2
import numpy as np
import supervision as sv
import torch
from people_counter.config import (
    DETECTOR_FLOOR,
    MIN_CONFIRMATION_FRAMES,
    RFDetrBotsortConfig,
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
    FrameReadState,
    iter_sampled_frame_batches,
    read_video_metadata,
)
from rfdetr import RFDETRLarge
from trackers import BoTSORTTracker

def run(config: RFDetrBotsortConfig) -> RunResult:
    """Run RF-DETR/BoT-SORT and mutate the result owned by ``config``."""
    config.result.ensure_unused()
    device = torch.device(config.device)
    model = RFDETRLarge(device=str(device))
    model.inference(
        compile=False,
        batch_size=config.batch_size,
        dtype=torch.float16 if config.use_fp16 else torch.float32,
        inplace=True,
    )

    input_path = config.video
    capture = cv2.VideoCapture(str(input_path))
    if not capture.isOpened():
        raise RuntimeError(f"Could not open input video: {input_path}")

    try:
        metadata = read_video_metadata(capture, input_path)
    except Exception:
        capture.release()
        raise

    result = config.result
    read_state = FrameReadState()
    processing_started = time.perf_counter()
    line_zone = None
    try:
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
        line_track_cache: BoTSORTCoastingCache = {}
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
        result.fps = metadata.fps
        result.total_source_frames = metadata.total_source_frames
        result.total_sampled_frames = sampling.total_sampled_frames
        result.sample_interval = sampling.interval
        result.effective_sample_fps = sampling.effective_fps
        result.batch_size = config.batch_size
        result.use_fp16 = config.use_fp16
        result.camera_motion_compensation = enable_cmc
        result.initialized = True
        if config.progress_callback is not None:
            config.progress_callback(result)

        for frame_batch in iter_sampled_frame_batches(
            capture,
            sampling.interval,
            config.batch_size,
            metadata.total_source_frames,
            read_state,
        ):
            source_frame_indices = [item[0] for item in frame_batch]
            frames = [item[1] for item in frame_batch]
            rgb_frames = [
                cv2.cvtColor(frame, cv2.COLOR_BGR2RGB) for frame in frames
            ]
            detections_batch = model.predict(
                rgb_frames,
                threshold=DETECTOR_FLOOR,
                include_source_image=False,
            )
            if not isinstance(detections_batch, list):
                raise RuntimeError(
                    "RF-DETR returned an unexpected result for batched input"
                )

            for source_frame_index, frame, detections in zip(
                source_frame_indices, frames, detections_batch
            ):
                class_names = detections.data.get("class_name")
                if class_names is None:
                    raise RuntimeError("RF-DETR detections did not include class names")
                people = detections[np.asarray(class_names) == "person"]
                tracked_people = tracker.update(
                    people,
                    frame=frame,
                    timestamp=source_frame_index / metadata.fps,
                )
                if tracked_people.tracker_id is None:
                    confirmed_people = sv.Detections.empty()
                else:
                    confirmed_people = tracked_people[
                        tracked_people.tracker_id >= 0
                    ]

                if line_zone is not None:
                    record_line_counts(
                        line_zone,
                        coasting_track_detections(
                            confirmed_people,
                            line_track_cache,
                            source_frame_index,
                            round(
                                (lost_track_buffer / 30) * metadata.fps
                            ),
                        ),
                        source_frame_index,
                        metadata.fps,
                        config.line,
                        result.line_counts,
                    )
                if confirmed_people.tracker_id is None:
                    continue

                for tracker_id in confirmed_people.tracker_id:
                    person_id = int(tracker_id)
                    record = result.telemetry.setdefault(
                        person_id,
                        PersonTelemetry(
                            entry_frame=confirmed_entry_frame(
                                source_frame_index,
                                sampling.interval,
                            ),
                            last_seen_frame=source_frame_index,
                        ),
                    )
                    record.last_seen_frame = source_frame_index

            result.processed_frames += len(frame_batch)
            if config.progress_callback is not None:
                config.progress_callback(result)
    finally:
        capture.release()
        result.processing_seconds = time.perf_counter() - processing_started
        result.source_frames_read = read_state.source_frames_read
        result.ended_early = read_state.ended_early
        if result.initialized and line_zone is not None:
            result.line_in_count = line_zone.in_count
            result.line_out_count = line_zone.out_count

    return result
