import argparse
import csv
from pathlib import Path

import cv2
import torch
import numpy as np
from transformers import AutoImageProcessor, RTDetrForObjectDetection


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
processor = AutoImageProcessor.from_pretrained("PekingU/rtdetr_v2_r50vd")
model = RTDetrForObjectDetection.from_pretrained("PekingU/rtdetr_v2_r50vd").to(device)

# 2. Permissive Manual Tracker State (MIT Logic)
# Stores tracking memory: { track_id: {"centroid": (x, y), "color_hist": hist_data, "age": frame_count} }
track_gallery = {}
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
    
    # Filter strictly for "person" (COCO class 0)
    person_mask = labels == 0
    if np.any(person_mask):
        for box in boxes[person_mask]:
            x1, y1, x2, y2 = map(int, box)
            
            # Prevent cropping errors on edge boundaries
            x1, y1 = max(0, x1), max(0, y1)
            x2, y2 = min(frame.shape[1], x2), min(frame.shape[0], y2)
            
            # Compute spatial center point (Centroid)
            cx, cy = int((x1 + x2) / 2), int((y1 + y2) / 2)
            
            # Compute a lightweight permissive ReID marker: Color Histogram of clothes/person
            person_crop = frame[y1:y2, x1:x2]
            if person_crop.size > 0:
                hist = cv2.calcHist([person_crop], [0, 1, 2], None, [8, 8, 8], [0, 256, 0, 256, 0, 256])
                cv2.normalize(hist, hist)
                current_frame_detections.append({"bbox": (x1, y1, x2, y2), "centroid": (cx, cy), "hist": hist})

    # 4. Core ReID Tracker Matching Logic (MIT/BSD-compliant algorithm)
    updated_tracks = {}
    
    for det in current_frame_detections:
        best_match_id = None
        best_score = -1  # High correlation score means highly similar visual profile
        
        # Check against every existing person profile in our memory gallery
        for tid, profile in track_gallery.items():
            # Spatial distance constraint (Are they near where the person was last seen?)
            spatial_dist = np.hypot(det["centroid"][0] - profile["centroid"][0], det["centroid"][1] - profile["centroid"][1])
            
            if spatial_dist < 150: # Distance threshold in pixels
                # Visual appearance verification (ReID via Cosine/Histogram Correlation)
                appearance_score = cv2.compareHist(det["hist"], profile["hist"], cv2.HISTCMP_CORREL)
                
                if appearance_score > best_score and appearance_score > 0.5:
                    best_score = appearance_score
                    best_match_id = tid
        
        if best_match_id is not None:
            # ReID Match Found! Update the person's records and retain their original ID
            updated_tracks[best_match_id] = {"centroid": det["centroid"], "hist": det["hist"], "age": 0, "bbox": det["bbox"]}
            person_telemetry[best_match_id]["last_seen_frame"] = frame_index
        else:
            # Brand New Identity discovered
            updated_tracks[next_track_id] = {"centroid": det["centroid"], "hist": det["hist"], "age": 0, "bbox": det["bbox"]}
            tracked_distinct_people.add(next_track_id)
            person_telemetry[next_track_id] = {
                "entry_frame": frame_index,
                "last_seen_frame": frame_index,
            }
            next_track_id += 1

    # Age unmapped tracks to see if they should be dropped or held in memory
    for tid, profile in list(track_gallery.items()):
        if tid not in updated_tracks:
            profile["age"] += 1
            if profile["age"] <= max_disappeared_frames:
                updated_tracks[tid] = profile # Carry over track memory to keep ReID active

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
print(f"Process ended cleanly. Total distinct individuals: {len(tracked_distinct_people)}")
print(f"Person telemetry saved to: {telemetry_path}")
