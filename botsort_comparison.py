import argparse
import csv
import math
import time
from pathlib import Path

import cv2
import torch
from rfdetr import RFDETRLarge
from trackers import BoTSORTTracker

PERSON_CLASS_ID = 1
DETECTOR_FLOOR = 0.1


def video_file_path(value):
    path = Path(value).expanduser()
    if not path.is_file():
        raise argparse.ArgumentTypeError(f"Video file does not exist: {path}")
    return path


def positive_int(value):
    parsed_value = int(value)
    if parsed_value <= 0:
        raise argparse.ArgumentTypeError("Value must be greater than zero")
    return parsed_value


def sample_fps_value(value):
    if value.lower() == "all":
        return None
    parsed_value = float(value)
    if parsed_value <= 0:
        raise argparse.ArgumentTypeError("Sample FPS must be greater than zero")
    return parsed_value


def probability_value(value):
    parsed_value = float(value)
    if not 0 < parsed_value <= 1:
        raise argparse.ArgumentTypeError("Probability must be in the range (0, 1]")
    return parsed_value


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
        type=probability_value,
        default=0.6,
        help="Confidence required to activate a track (default: 0.6).",
    )
    parser.add_argument(
        "--no-fp16",
        action="store_true",
        help="Disable FP16 detector inference in GPU mode.",
    )
    return parser.parse_args()


def resolve_device(device_variant):
    if device_variant == "cpu":
        if torch.version.cuda is not None:
            raise RuntimeError(
                "CPU mode requires the CPU-only PyTorch build. "
                "Run with: uv run --extra cpu botsort_comparison.py "
                "<video> --device cpu"
            )
        return torch.device("cpu")

    if torch.version.cuda is None:
        raise RuntimeError(
            "GPU mode requires a CUDA-enabled PyTorch build. "
            "Run with: uv run --extra gpu botsort_comparison.py "
            "<video> --device gpu"
        )
    if not torch.cuda.is_available():
        raise RuntimeError(
            "GPU mode was requested, but CUDA is unavailable. "
            "Check the NVIDIA driver and GPU access."
        )
    return torch.device("cuda")


def iter_sampled_frame_batches(capture, sample_interval, batch_size):
    batch = []
    source_frame_index = 0

    while capture.isOpened():
        success, frame = capture.read()
        if not success:
            break

        if source_frame_index % sample_interval == 0:
            batch.append((source_frame_index, frame))
            if len(batch) == batch_size:
                yield batch
                batch = []

        source_frame_index += 1

    if batch:
        yield batch


def format_video_timestamp(frame_index, fps):
    total_milliseconds = round((frame_index / fps) * 1000)
    hours, remainder = divmod(total_milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, milliseconds = divmod(remainder, 1000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds:03d}"


def write_telemetry(telemetry_path, telemetry, fps):
    with telemetry_path.open("w", newline="", encoding="utf-8") as telemetry_file:
        fieldnames = [
            "person_id",
            "entry_frame",
            "exit_frame",
            "entry_seconds",
            "exit_seconds",
            "entry_timestamp",
            "exit_timestamp",
            "duration_seconds",
        ]
        writer = csv.DictWriter(telemetry_file, fieldnames=fieldnames)
        writer.writeheader()

        for person_id in sorted(telemetry):
            entry_frame = telemetry[person_id]["entry_frame"]
            exit_frame = telemetry[person_id]["last_seen_frame"]
            entry_seconds = entry_frame / fps
            exit_seconds = exit_frame / fps
            writer.writerow(
                {
                    "person_id": person_id,
                    "entry_frame": entry_frame,
                    "exit_frame": exit_frame,
                    "entry_seconds": f"{entry_seconds:.3f}",
                    "exit_seconds": f"{exit_seconds:.3f}",
                    "entry_timestamp": format_video_timestamp(entry_frame, fps),
                    "exit_timestamp": format_video_timestamp(exit_frame, fps),
                    "duration_seconds": f"{exit_seconds - entry_seconds:.3f}",
                }
            )


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
    telemetry_path = Path(
        f"outputs/{input_path.stem}_telemetry_rfdetr_large_botsort_"
        f"{args.device}.csv"
    )
    capture = cv2.VideoCapture(str(input_path))
    if not capture.isOpened():
        raise RuntimeError(f"Could not open input video: {input_path}")

    fps = capture.get(cv2.CAP_PROP_FPS)
    if fps <= 0:
        capture.release()
        raise RuntimeError(f"Invalid video metadata for input: {input_path}")

    total_source_frames = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
    requested_sample_fps = args.sample_fps or fps
    sample_interval = max(1, round(fps / min(requested_sample_fps, fps)))
    effective_sample_fps = fps / sample_interval
    total_sampled_frames = (
        math.ceil(total_source_frames / sample_interval)
        if total_source_frames > 0
        else 0
    )

    tracker = BoTSORTTracker(
        frame_rate=fps,
        lost_track_buffer=max(1, round(fps)),
        track_activation_threshold=args.detection_threshold,
        high_conf_det_threshold=args.detection_threshold,
        enable_cmc=True,
    )
    telemetry = {}
    processed_frames = 0
    processing_started = time.perf_counter()
    telemetry_path.parent.mkdir(parents=True, exist_ok=True)

    print(
        f"Sampling {effective_sample_fps:.2f} FPS (every {sample_interval} "
        f"source frame(s)); detector batch size {batch_size}; FP16 {use_fp16}"
    )

    try:
        for frame_batch in iter_sampled_frame_batches(
            capture, sample_interval, batch_size
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
                if detections.class_id is None:
                    raise RuntimeError("RF-DETR detections did not include class IDs")

                people = detections[detections.class_id == PERSON_CLASS_ID]
                tracked_people = tracker.update(
                    people,
                    frame=frame,
                    timestamp=source_frame_index / fps,
                )
                if tracked_people.tracker_id is None:
                    continue

                for tracker_id in tracked_people.tracker_id:
                    person_id = int(tracker_id)
                    if person_id < 0:
                        continue
                    record = telemetry.setdefault(
                        person_id,
                        {
                            "entry_frame": source_frame_index,
                            "last_seen_frame": source_frame_index,
                        },
                    )
                    record["last_seen_frame"] = source_frame_index

            processed_frames += len(frame_batch)
            if total_sampled_frames > 0:
                print(
                    f"\rProcessed {processed_frames}/{total_sampled_frames} "
                    f"sampled frames",
                    end="",
                    flush=True,
                )
    finally:
        capture.release()

    processing_seconds = time.perf_counter() - processing_started
    if total_sampled_frames > 0:
        print()
    processing_fps = (
        processed_frames / processing_seconds if processing_seconds else 0
    )
    print(
        f"Processing time: {processing_seconds:.1f}s "
        f"({processing_fps:.2f} sampled FPS)"
    )

    write_telemetry(telemetry_path, telemetry, fps)
    print(f"Process ended cleanly. Total distinct individuals: {len(telemetry)}")
    print(f"Person telemetry saved to: {telemetry_path}")


if __name__ == "__main__":
    main()
