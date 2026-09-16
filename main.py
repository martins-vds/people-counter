import argparse
import csv
import hashlib
import math
import time
from pathlib import Path

import cv2
import numpy as np
import torch
from huggingface_hub import hf_hub_download
from libreyolo.tracking.reid import OSNetEmbedder
from scipy.optimize import linear_sum_assignment
from transformers import AutoImageProcessor, RTDetrV2ForObjectDetection

REID_REPO_ID = "LibreYOLO/LibreReID-osnet"
REID_FILENAME = "osnet_ain_x0_25.pt"
REID_REVISION = "5c7c20e54ccf80c9889a64020748f148ad5f7634"
REID_SHA256 = "ce171fe160b3608f5e4c19489774991419be965b1d6f4bdccc4b4cfd2ef95347"
ACTIVE_MATCH_THRESHOLD = 0.70
REENTRY_MATCH_THRESHOLD = 0.75
MAX_CENTROID_DISTANCE = 150
MAX_DISAPPEARED_SECONDS = 1.0
EMBEDDING_EMA_ALPHA = 0.95
DETECTOR_IMAGE_SIZE = (640, 640)
DETECTOR_MODELS = {
    "r18": "PekingU/rtdetr_v2_r18vd",
    "r50": "PekingU/rtdetr_v2_r50vd",
}


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
    parser = argparse.ArgumentParser(description="Count unique people in a video.")
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
        help="Detector batch size (default: 8 on GPU, 1 on CPU).",
    )
    parser.add_argument(
        "--detector-model",
        choices=tuple(DETECTOR_MODELS),
        default="r18",
        help="RT-DETRv2 backbone (default: r18; r50 is slower and more accurate).",
    )
    parser.add_argument(
        "--detection-threshold",
        type=probability_value,
        default=0.6,
        help="Minimum person detection confidence (default: 0.6).",
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
                "Run with: uv run --extra cpu main.py <video> --device cpu"
            )
        return torch.device("cpu")

    if torch.version.cuda is None:
        raise RuntimeError(
            "GPU mode requires a CUDA-enabled PyTorch build. "
            "Run with: uv run --extra gpu main.py <video> --device gpu"
        )
    if not torch.cuda.is_available():
        raise RuntimeError(
            "GPU mode was requested, but CUDA is unavailable. "
            "Check the NVIDIA driver and GPU access."
        )
    return torch.device("cuda")


def load_reid_embedder(device):
    weights_path = Path(
        hf_hub_download(
            repo_id=REID_REPO_ID,
            filename=REID_FILENAME,
            revision=REID_REVISION,
        )
    )
    weights_digest = hashlib.sha256(weights_path.read_bytes()).hexdigest()
    if weights_digest != REID_SHA256:
        raise RuntimeError(
            f"ReID weights checksum mismatch for {weights_path}: "
            f"expected {REID_SHA256}, got {weights_digest}"
        )
    return OSNetEmbedder(
        variant="osnet_ain_x0_25",
        weights=weights_path,
        device=str(device),
    )


def update_embedding(previous_embedding, current_embedding):
    embedding = (
        EMBEDDING_EMA_ALPHA * previous_embedding
        + (1.0 - EMBEDDING_EMA_ALPHA) * current_embedding
    )
    norm = np.linalg.norm(embedding)
    if norm == 0:
        raise RuntimeError("OSNet produced a zero-norm identity embedding")
    return embedding / norm


def make_track_profile(detection, embedding):
    return {
        "centroid": detection["centroid"],
        "embedding": embedding,
        "age": 0,
        "bbox": detection["bbox"],
    }


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


def extract_person_detections(result, frame, reid_embedder):
    boxes = result["boxes"].cpu().numpy()
    labels = result["labels"].cpu().numpy()
    person_boxes = []

    for box in boxes[labels == 0]:
        x1, y1, x2, y2 = map(int, box)
        x1, y1 = max(0, x1), max(0, y1)
        x2, y2 = min(frame.shape[1], x2), min(frame.shape[0], y2)
        if x2 > x1 and y2 > y1:
            person_boxes.append((x1, y1, x2, y2))

    if not person_boxes:
        return []

    rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    embeddings = reid_embedder(
        rgb_frame, np.asarray(person_boxes, dtype=np.float32)
    )
    detections = []
    for bbox, embedding in zip(person_boxes, embeddings):
        x1, y1, x2, y2 = bbox
        detections.append(
            {
                "bbox": bbox,
                "centroid": ((x1 + x2) // 2, (y1 + y2) // 2),
                "embedding": embedding,
            }
        )
    return detections


def associate_detections(
    detections,
    track_gallery,
    identity_gallery,
    person_telemetry,
    next_track_id,
    source_frame_index,
    max_centroid_distance,
    max_disappeared_frames,
):
    updated_tracks = {}
    matched_detection_indices = set()
    active_track_ids = list(track_gallery)

    if detections and active_track_ids:
        detection_embeddings = np.stack(
            [detection["embedding"] for detection in detections]
        )
        active_embeddings = np.stack(
            [track_gallery[track_id]["embedding"] for track_id in active_track_ids]
        )
        similarities = detection_embeddings @ active_embeddings.T
        costs = 1.0 - similarities
        valid_pairs = np.zeros(costs.shape, dtype=bool)

        for detection_index, detection in enumerate(detections):
            for track_index, track_id in enumerate(active_track_ids):
                profile = track_gallery[track_id]
                spatial_distance = np.hypot(
                    detection["centroid"][0] - profile["centroid"][0],
                    detection["centroid"][1] - profile["centroid"][1],
                )
                valid_pairs[detection_index, track_index] = (
                    spatial_distance < max_centroid_distance
                )

        costs[~valid_pairs] = 1_000_000
        matched_rows, matched_columns = linear_sum_assignment(costs)
        for detection_index, track_index in zip(matched_rows, matched_columns):
            if (
                not valid_pairs[detection_index, track_index]
                or similarities[detection_index, track_index]
                < ACTIVE_MATCH_THRESHOLD
            ):
                continue

            track_id = active_track_ids[track_index]
            detection = detections[detection_index]
            embedding = update_embedding(
                identity_gallery[track_id], detection["embedding"]
            )
            identity_gallery[track_id] = embedding
            updated_tracks[track_id] = make_track_profile(detection, embedding)
            person_telemetry[track_id]["last_seen_frame"] = source_frame_index
            matched_detection_indices.add(detection_index)

    unmatched_detection_indices = [
        index
        for index in range(len(detections))
        if index not in matched_detection_indices
    ]
    inactive_identity_ids = [
        track_id for track_id in identity_gallery if track_id not in track_gallery
    ]

    if unmatched_detection_indices and inactive_identity_ids:
        unmatched_embeddings = np.stack(
            [detections[index]["embedding"] for index in unmatched_detection_indices]
        )
        inactive_embeddings = np.stack(
            [identity_gallery[track_id] for track_id in inactive_identity_ids]
        )
        reentry_similarities = unmatched_embeddings @ inactive_embeddings.T
        reentry_rows, reentry_columns = linear_sum_assignment(
            1.0 - reentry_similarities
        )

        for unmatched_row, inactive_column in zip(reentry_rows, reentry_columns):
            if (
                reentry_similarities[unmatched_row, inactive_column]
                < REENTRY_MATCH_THRESHOLD
            ):
                continue

            detection_index = unmatched_detection_indices[unmatched_row]
            track_id = inactive_identity_ids[inactive_column]
            detection = detections[detection_index]
            embedding = update_embedding(
                identity_gallery[track_id], detection["embedding"]
            )
            identity_gallery[track_id] = embedding
            updated_tracks[track_id] = make_track_profile(detection, embedding)
            person_telemetry[track_id]["last_seen_frame"] = source_frame_index
            matched_detection_indices.add(detection_index)

    for detection_index, detection in enumerate(detections):
        if detection_index in matched_detection_indices:
            continue

        embedding = detection["embedding"]
        identity_gallery[next_track_id] = embedding
        updated_tracks[next_track_id] = make_track_profile(detection, embedding)
        person_telemetry[next_track_id] = {
            "entry_frame": source_frame_index,
            "last_seen_frame": source_frame_index,
        }
        next_track_id += 1

    for track_id, profile in track_gallery.items():
        if track_id not in updated_tracks:
            profile["age"] += 1
            if profile["age"] <= max_disappeared_frames:
                updated_tracks[track_id] = profile

    return updated_tracks, next_track_id


def format_video_timestamp(frame_index, fps):
    total_milliseconds = round((frame_index / fps) * 1000)
    hours, remainder = divmod(total_milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, milliseconds = divmod(remainder, 1000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds:03d}"


# 1. Initialize Permissive RT-DETR Model (Apache 2.0)
args = parse_args()
device = resolve_device(args.device)
print(f"Running {args.device.upper()} variant on: {device}")
if args.device == "gpu":
    torch.backends.cudnn.benchmark = True
reid_embedder = load_reid_embedder(device)
detector_model_id = DETECTOR_MODELS[args.detector_model]
print(f"Loading detector: {detector_model_id}")
processor = AutoImageProcessor.from_pretrained(detector_model_id)
model = RTDetrV2ForObjectDetection.from_pretrained(
    detector_model_id
).to(device)

# 2. OSNet ReID tracker state
track_gallery = {}
identity_gallery = {}
next_track_id = 1
person_telemetry = {}

# 3. Read Video Stream
input_path = args.video
telemetry_path = Path(f"outputs/{input_path.stem}_telemetry_{args.device}.csv")
cap = cv2.VideoCapture(str(input_path))
if not cap.isOpened():
    raise RuntimeError(f"Could not open input video: {input_path}")

fps = cap.get(cv2.CAP_PROP_FPS)
if fps <= 0:
    cap.release()
    raise RuntimeError(f"Invalid video metadata for input: {input_path}")

total_source_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
requested_sample_fps = args.sample_fps or fps
sample_interval = max(1, round(fps / min(requested_sample_fps, fps)))
effective_sample_fps = fps / sample_interval
batch_size = args.batch_size or (8 if args.device == "gpu" else 1)
use_fp16 = args.device == "gpu" and not args.no_fp16
max_disappeared_frames = max(
    1, round(MAX_DISAPPEARED_SECONDS * effective_sample_fps)
)
max_centroid_distance = round(
    MAX_CENTROID_DISTANCE * math.sqrt(sample_interval)
)
total_sampled_frames = (
    math.ceil(total_source_frames / sample_interval)
    if total_source_frames > 0
    else 0
)

print(
    f"Sampling {effective_sample_fps:.2f} FPS (every {sample_interval} source "
    f"frame(s)); detector batch size {batch_size}; FP16 {use_fp16}"
)
telemetry_path.parent.mkdir(parents=True, exist_ok=True)
processed_frames = 0
processing_started = time.perf_counter()

try:
    for frame_batch in iter_sampled_frame_batches(
        cap, sample_interval, batch_size
    ):
        source_frame_indices = [item[0] for item in frame_batch]
        frames = [item[1] for item in frame_batch]
        detector_frames = [
            cv2.cvtColor(
                cv2.resize(frame, DETECTOR_IMAGE_SIZE, interpolation=cv2.INTER_LINEAR),
                cv2.COLOR_BGR2RGB,
            )
            for frame in frames
        ]
        inputs = processor(
            images=detector_frames,
            do_resize=False,
            return_tensors="pt",
        ).to(device)

        with torch.inference_mode(), torch.autocast(
            device_type=device.type,
            dtype=torch.float16,
            enabled=use_fp16,
        ):
            outputs = model(**inputs)

        target_sizes = torch.tensor(
            [frame.shape[:2] for frame in frames], device=device
        )
        results = processor.post_process_object_detection(
            outputs,
            target_sizes=target_sizes,
            threshold=args.detection_threshold,
        )

        for source_frame_index, frame, result in zip(
            source_frame_indices, frames, results
        ):
            detections = extract_person_detections(result, frame, reid_embedder)
            track_gallery, next_track_id = associate_detections(
                detections,
                track_gallery,
                identity_gallery,
                person_telemetry,
                next_track_id,
                source_frame_index,
                max_centroid_distance,
                max_disappeared_frames,
            )

        processed_frames += len(frame_batch)
        if total_sampled_frames > 0:
            print(
                f"\rProcessed {processed_frames}/{total_sampled_frames} "
                f"sampled frames",
                end="",
                flush=True,
            )
finally:
    cap.release()

processing_seconds = time.perf_counter() - processing_started
if total_sampled_frames > 0:
    print()
processing_fps = processed_frames / processing_seconds if processing_seconds else 0
print(
    f"Processing time: {processing_seconds:.1f}s "
    f"({processing_fps:.2f} sampled FPS)"
)

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
    telemetry_writer = csv.DictWriter(telemetry_file, fieldnames=fieldnames)
    telemetry_writer.writeheader()

    for person_id in sorted(person_telemetry):
        entry_frame = person_telemetry[person_id]["entry_frame"]
        exit_frame = person_telemetry[person_id]["last_seen_frame"]
        entry_seconds = entry_frame / fps
        exit_seconds = exit_frame / fps
        telemetry_writer.writerow(
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
print(f"Process ended cleanly. Total distinct individuals: {len(identity_gallery)}")
print(f"Person telemetry saved to: {telemetry_path}")
