import argparse
import csv
import hashlib
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
EMBEDDING_EMA_ALPHA = 0.95


def video_file_path(value):
    path = Path(value).expanduser()
    if not path.is_file():
        raise argparse.ArgumentTypeError(f"Video file does not exist: {path}")
    return path


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
reid_embedder = load_reid_embedder(device)
processor = AutoImageProcessor.from_pretrained("PekingU/rtdetr_v2_r50vd")
model = RTDetrV2ForObjectDetection.from_pretrained(
    "PekingU/rtdetr_v2_r50vd"
).to(device)

# 2. OSNet ReID tracker state
track_gallery = {}
identity_gallery = {}
next_track_id = 1
max_disappeared_frames = 30  # How long to remember someone when they disappear

# Unique person registry tracker
tracked_distinct_people = set()
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

telemetry_path.parent.mkdir(parents=True, exist_ok=True)

frame_index = 0
while cap.isOpened():
    success, frame = cap.read()
    if not success:
        break

    # Object Detection Processing
    rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    inputs = processor(images=rgb_frame, return_tensors="pt").to(device)
    with torch.no_grad():
        outputs = model(**inputs)
    
    # Process Bounding Boxes
    target_sizes = torch.tensor([frame.shape[:2]]).to(device)
    result = processor.post_process_object_detection(
        outputs, target_sizes=target_sizes, threshold=0.4
    )[0]
    
    boxes = result["boxes"].cpu().numpy()
    labels = result["labels"].cpu().numpy()
    
    current_frame_detections = []
    person_boxes = []

    # Filter strictly for "person" (COCO class 0)
    person_mask = labels == 0
    if np.any(person_mask):
        for box in boxes[person_mask]:
            x1, y1, x2, y2 = map(int, box)

            # Prevent cropping errors on edge boundaries
            x1, y1 = max(0, x1), max(0, y1)
            x2, y2 = min(frame.shape[1], x2), min(frame.shape[0], y2)
            if x2 > x1 and y2 > y1:
                person_boxes.append((x1, y1, x2, y2))

    if person_boxes:
        embeddings = reid_embedder(
            rgb_frame, np.asarray(person_boxes, dtype=np.float32)
        )
        for bbox, embedding in zip(person_boxes, embeddings):
            x1, y1, x2, y2 = bbox
            current_frame_detections.append(
                {
                    "bbox": bbox,
                    "centroid": ((x1 + x2) // 2, (y1 + y2) // 2),
                    "embedding": embedding,
                }
            )

    # 4. One-to-one OSNet identity association
    updated_tracks = {}
    matched_detection_indices = set()
    active_track_ids = list(track_gallery)

    if current_frame_detections and active_track_ids:
        detection_embeddings = np.stack(
            [detection["embedding"] for detection in current_frame_detections]
        )
        active_embeddings = np.stack(
            [track_gallery[track_id]["embedding"] for track_id in active_track_ids]
        )
        similarities = detection_embeddings @ active_embeddings.T
        costs = 1.0 - similarities
        valid_pairs = np.zeros(costs.shape, dtype=bool)

        for detection_index, detection in enumerate(current_frame_detections):
            for track_index, track_id in enumerate(active_track_ids):
                profile = track_gallery[track_id]
                spatial_distance = np.hypot(
                    detection["centroid"][0] - profile["centroid"][0],
                    detection["centroid"][1] - profile["centroid"][1],
                )
                valid_pairs[detection_index, track_index] = (
                    spatial_distance < MAX_CENTROID_DISTANCE
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
            detection = current_frame_detections[detection_index]
            embedding = update_embedding(
                identity_gallery[track_id], detection["embedding"]
            )
            identity_gallery[track_id] = embedding
            updated_tracks[track_id] = make_track_profile(detection, embedding)
            person_telemetry[track_id]["last_seen_frame"] = frame_index
            matched_detection_indices.add(detection_index)

    unmatched_detection_indices = [
        index
        for index in range(len(current_frame_detections))
        if index not in matched_detection_indices
    ]
    inactive_identity_ids = [
        track_id for track_id in identity_gallery if track_id not in track_gallery
    ]

    if unmatched_detection_indices and inactive_identity_ids:
        unmatched_embeddings = np.stack(
            [
                current_frame_detections[index]["embedding"]
                for index in unmatched_detection_indices
            ]
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
            detection = current_frame_detections[detection_index]
            embedding = update_embedding(
                identity_gallery[track_id], detection["embedding"]
            )
            identity_gallery[track_id] = embedding
            updated_tracks[track_id] = make_track_profile(detection, embedding)
            person_telemetry[track_id]["last_seen_frame"] = frame_index
            matched_detection_indices.add(detection_index)

    for detection_index, detection in enumerate(current_frame_detections):
        if detection_index in matched_detection_indices:
            continue

        embedding = detection["embedding"]
        identity_gallery[next_track_id] = embedding
        updated_tracks[next_track_id] = make_track_profile(detection, embedding)
        tracked_distinct_people.add(next_track_id)
        person_telemetry[next_track_id] = {
            "entry_frame": frame_index,
            "last_seen_frame": frame_index,
        }
        next_track_id += 1

    # Age unmapped tracks to see if they should be dropped or held in memory
    for tid, profile in track_gallery.items():
        if tid not in updated_tracks:
            profile["age"] += 1
            if profile["age"] <= max_disappeared_frames:
                updated_tracks[tid] = profile

    track_gallery = updated_tracks

    frame_index += 1

cap.release()

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
print(f"Process ended cleanly. Total distinct individuals: {len(tracked_distinct_people)}")
print(f"Person telemetry saved to: {telemetry_path}")
