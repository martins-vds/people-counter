import argparse
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import cv2
import numpy as np
import supervision as sv
import torch
from people_counter.common import (
    DETECTOR_FLOOR,
    MIN_CONFIRMATION_FRAMES,
    FrameReadState,
    coasting_track_detections,
    confirmed_entry_frame,
    create_line_zone,
    detection_threshold_value,
    iter_sampled_frame_batches,
    lost_track_buffer_for_sample_rate,
    positive_int,
    record_line_counts,
    sample_fps_value,
    video_file_path,
    write_line_counts,
    write_telemetry,
)
from rfdetr import RFDETRLarge
from trackers import BoTSORTTracker

def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare RF-DETR Large with Roboflow BoT-SORT tracking."
    )
    parser.add_argument(
        "video",
        type=video_file_path,
        help="Path to the input video file.",
    )
    parser.add_argument(
        "--device",
        choices=("cpu", "gpu"),
        required=True,
        help="Run with the matching CPU-only or CUDA-enabled PyTorch variant.",
    )
    parser.add_argument(
        "--sample-fps",
        type=sample_fps_value,
        default=3.0,
        metavar="FPS|all",
        help="Video sampling rate (default: 3; use 'all' for every frame).",
    )
    parser.add_argument(
        "--batch-size",
        type=positive_int,
        help="Detector batch size (default: 4 on GPU, 1 on CPU).",
    )
    parser.add_argument(
        "--detection-threshold",
        type=detection_threshold_value,
        default=0.6,
        help="Confidence required to activate a track (default: 0.6).",
    )
    parser.add_argument(
        "--no-fp16",
        action="store_true",
        help="Disable FP16 detector inference in GPU mode.",
    )
    parser.add_argument(
        "--line",
        type=int,
        nargs=4,
        metavar=("X1", "Y1", "X2", "Y2"),
        help="Directed counting line in source-video pixels.",
    )
    parser.add_argument(
        "--cmc",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Enable camera-motion compensation "
            "(default: enabled only when processing every frame)."
        ),
    )
    return parser.parse_args()


def resolve_device(device_variant):
    if device_variant == "cpu":
        if torch.version.cuda is not None:
            raise RuntimeError(
                "CPU mode requires the CPU-only PyTorch build. "
                "Run with: uv run --extra cpu rfdetr_botsort_counter.py "
                "<video> --device cpu"
            )
        return torch.device("cpu")

    if torch.version.cuda is None:
        raise RuntimeError(
            "GPU mode requires a CUDA-enabled PyTorch build. "
            "Run with: uv run --extra gpu rfdetr_botsort_counter.py "
            "<video> --device gpu"
        )
    if not torch.cuda.is_available():
        raise RuntimeError(
            "GPU mode was requested, but CUDA is unavailable. "
            "Check the NVIDIA driver and GPU access."
        )
    return torch.device("cuda")


def main():
    args = parse_args()
    device = resolve_device(args.device)
    batch_size = args.batch_size or (4 if args.device == "gpu" else 1)
    use_fp16 = args.device == "gpu" and not args.no_fp16

    print(f"Running {args.device.upper()} variant on: {device}")
    print("Loading detector: RF-DETR Large")
    model = RFDETRLarge(device=str(device))
    model.inference(
        compile=False,
        batch_size=batch_size,
        dtype=torch.float16 if use_fp16 else torch.float32,
        inplace=True,
    )

    input_path = args.video
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    telemetry_path = Path(
        f"outputs/{input_path.stem}_telemetry_rfdetr_large_botsort_"
        f"{args.device}_{run_timestamp}.csv"
    )
    line_counts_path = (
        Path(
            f"outputs/{input_path.stem}_line_counts_rfdetr_large_botsort_"
            f"{args.device}_{run_timestamp}.csv"
        )
        if args.line is not None
        else None
    )
    capture = cv2.VideoCapture(str(input_path))
    if not capture.isOpened():
        raise RuntimeError(f"Could not open input video: {input_path}")

    fps = capture.get(cv2.CAP_PROP_FPS)
    frame_width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
    if fps <= 0 or frame_width <= 0 or frame_height <= 0:
        capture.release()
        raise RuntimeError(f"Invalid video metadata for input: {input_path}")

    results_initialized = False
    line_count_records = []
    telemetry = {}
    try:
        line_zone = create_line_zone(args.line, frame_width, frame_height)
        total_source_frames = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
        requested_sample_fps = args.sample_fps or fps
        sample_interval = max(1, round(fps / min(requested_sample_fps, fps)))
        effective_sample_fps = fps / sample_interval
        total_sampled_frames = (
            math.ceil(total_source_frames / sample_interval)
            if total_source_frames > 0
            else 0
        )
        read_state = FrameReadState()
        line_track_cache = {}
        enable_cmc = args.cmc if args.cmc is not None else sample_interval == 1
        lost_track_buffer = lost_track_buffer_for_sample_rate(
            effective_sample_fps
        )
        tracker = BoTSORTTracker(
            frame_rate=fps,
            lost_track_buffer=lost_track_buffer,
            track_activation_threshold=args.detection_threshold,
            high_conf_det_threshold=args.detection_threshold,
            minimum_consecutive_frames=MIN_CONFIRMATION_FRAMES,
            instant_first_frame_activation=False,
            enable_cmc=enable_cmc,
        )
        processed_frames = 0
        processing_started = time.perf_counter()
        telemetry_path.parent.mkdir(parents=True, exist_ok=True)
        results_initialized = True

        print(
            f"Sampling {effective_sample_fps:.2f} FPS (every {sample_interval} "
            f"source frame(s)); detector batch size {batch_size}; FP16 {use_fp16}; "
            f"CMC {enable_cmc}"
        )

        for frame_batch in iter_sampled_frame_batches(
            capture,
            sample_interval,
            batch_size,
            total_source_frames,
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
                    timestamp=source_frame_index / fps,
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
                            round((lost_track_buffer / 30) * fps),
                        ),
                        source_frame_index,
                        fps,
                        args.line,
                        line_count_records,
                    )
                if confirmed_people.tracker_id is None:
                    continue

                for tracker_id in confirmed_people.tracker_id:
                    person_id = int(tracker_id)
                    record = telemetry.setdefault(
                        person_id,
                        {
                            "entry_frame": confirmed_entry_frame(
                                source_frame_index,
                                sample_interval,
                            ),
                            "last_seen_frame": source_frame_index,
                        },
                    )
                    record["last_seen_frame"] = source_frame_index

            processed_frames += len(frame_batch)
            progress_total = (
                f"/{total_sampled_frames}" if total_sampled_frames > 0 else ""
            )
            print(
                f"\rProcessed {processed_frames}{progress_total} sampled frames",
                end="",
                flush=True,
            )
    finally:
        capture.release()
        if results_initialized:
            write_telemetry(telemetry_path, telemetry, fps)
        if results_initialized and line_counts_path is not None:
            write_line_counts(line_counts_path, line_count_records)

    processing_seconds = time.perf_counter() - processing_started
    print()
    if read_state.ended_early:
        print(
            f"Warning: video decoding stopped after "
            f"{read_state.source_frames_read}/{total_source_frames} source frames",
            file=sys.stderr,
        )
    processing_fps = (
        processed_frames / processing_seconds if processing_seconds else 0
    )
    print(
        f"Processing time: {processing_seconds:.1f}s "
        f"({processing_fps:.2f} sampled FPS)"
    )

    print(f"Process ended cleanly. Total distinct individuals: {len(telemetry)}")
    print(f"Person telemetry saved to: {telemetry_path}")
    if line_counts_path is not None:
        print(
            f"Line crossings: {line_zone.in_count} in, {line_zone.out_count} out"
        )
        print(f"Line counts saved to: {line_counts_path}")


if __name__ == "__main__":
    main()
