import argparse
import hashlib
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import cv2
import numpy as np
import supervision as sv
import torch
from huggingface_hub import hf_hub_download
from libreyolo.tracking.reid import OSNetEmbedder
from people_counter.common import (
    DETECTOR_FLOOR,
    MIN_CONFIRMATION_FRAMES,
    FrameReadState,
    create_line_zone,
    detection_threshold_value,
    iter_sampled_frame_batches,
    positive_int,
    record_line_counts,
    retention_seconds_for_sample_rate,
    sample_fps_value,
    video_file_path,
    write_line_counts,
    write_telemetry,
)
from scipy.optimize import linear_sum_assignment
from transformers import AutoImageProcessor, RTDetrV2ForObjectDetection

REID_REPO_ID = "LibreYOLO/LibreReID-osnet"
REID_FILENAME = "osnet_ain_x0_25.pt"
REID_REVISION = "5c7c20e54ccf80c9889a64020748f148ad5f7634"
REID_SHA256 = "ce171fe160b3608f5e4c19489774991419be965b1d6f4bdccc4b4cfd2ef95347"
ACTIVE_MATCH_THRESHOLD = 0.70
SECONDARY_MATCH_THRESHOLD = 0.50
REENTRY_MATCH_THRESHOLD = 0.75
MAX_REENTRY_SECONDS = 30.0
MAX_CENTROID_SPEED_FRAME_HEIGHTS_PER_SECOND = 0.75
MIN_CENTROID_DISTANCE_BBOX_HEIGHTS = 0.5
PERSON_NMS_IOU_THRESHOLD = 0.7
EMBEDDING_EMA_ALPHA = 0.95
DETECTOR_IMAGE_SIZE = (640, 640)
DETECTOR_MODELS = {
    "r18": "PekingU/rtdetr_v2_r18vd",
    "r50": "PekingU/rtdetr_v2_r50vd",
}


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
        type=detection_threshold_value,
        default=0.6,
        help="Minimum person detection confidence (default: 0.6).",
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
    return parser.parse_args()


def resolve_device(device_variant):
    if device_variant == "cpu":
        if torch.version.cuda is not None:
            raise RuntimeError(
                "CPU mode requires the CPU-only PyTorch build. "
                "Run with: uv run --extra cpu rtdetr_osnet_counter.py "
                "<video> --device cpu"
            )
        return torch.device("cpu")

    if torch.version.cuda is None:
        raise RuntimeError(
            "GPU mode requires a CUDA-enabled PyTorch build. "
            "Run with: uv run --extra gpu rtdetr_osnet_counter.py "
            "<video> --device gpu"
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


def make_track_profile(detection, embedding, first_seen_frame, hits):
    return {
        "centroid": detection["centroid"],
        "embedding": embedding,
        "age": 0,
        "bbox": detection["bbox"],
        "first_seen_frame": first_seen_frame,
        "hits": hits,
    }


def resolve_person_class_id(id2label):
    person_class_ids = [
        int(class_id)
        for class_id, label in id2label.items()
        if str(label).strip().lower() == "person"
    ]
    if len(person_class_ids) != 1:
        raise RuntimeError(
            "Detector label map must contain exactly one 'person' class"
        )
    return person_class_ids[0]


def centroid_distance_limit(
    detection,
    profile,
    max_centroid_displacement,
    max_disappeared_frames,
):
    detection_height = detection["bbox"][3] - detection["bbox"][1]
    profile_height = profile["bbox"][3] - profile["bbox"][1]
    return max(
        max_centroid_displacement
        * min(profile["age"] + 1, max_disappeared_frames + 1),
        MIN_CENTROID_DISTANCE_BBOX_HEIGHTS
        * max(detection_height, profile_height),
    )


def is_near_active_track(detection, track_gallery):
    for profile in track_gallery.values():
        spatial_distance = np.hypot(
            detection["centroid"][0] - profile["centroid"][0],
            detection["centroid"][1] - profile["centroid"][1],
        )
        detection_height = detection["bbox"][3] - detection["bbox"][1]
        profile_height = profile["bbox"][3] - profile["bbox"][1]
        if spatial_distance < max(detection_height, profile_height):
            return True
    return False


def non_max_suppression(detections):
    if not detections:
        return []

    boxes = np.asarray(
        [detection["bbox"] for detection in detections],
        dtype=np.float32,
    )
    scores = np.asarray(
        [detection["confidence"] for detection in detections],
        dtype=np.float32,
    )
    areas = (boxes[:, 2] - boxes[:, 0]) * (boxes[:, 3] - boxes[:, 1])
    order = scores.argsort()[::-1]
    kept_indices = []

    while order.size:
        selected_index = int(order[0])
        kept_indices.append(selected_index)
        if order.size == 1:
            break

        remaining = order[1:]
        intersection_x1 = np.maximum(
            boxes[selected_index, 0],
            boxes[remaining, 0],
        )
        intersection_y1 = np.maximum(
            boxes[selected_index, 1],
            boxes[remaining, 1],
        )
        intersection_x2 = np.minimum(
            boxes[selected_index, 2],
            boxes[remaining, 2],
        )
        intersection_y2 = np.minimum(
            boxes[selected_index, 3],
            boxes[remaining, 3],
        )
        intersection = np.maximum(0, intersection_x2 - intersection_x1) * (
            np.maximum(0, intersection_y2 - intersection_y1)
        )
        union = areas[selected_index] + areas[remaining] - intersection
        iou = np.divide(
            intersection,
            union,
            out=np.zeros_like(intersection),
            where=union > 0,
        )
        order = remaining[iou <= PERSON_NMS_IOU_THRESHOLD]

    return [detections[index] for index in kept_indices]


def extract_person_detections(
    result,
    frame,
    rgb_frame,
    reid_embedder,
    person_class_id,
    track_gallery,
    activation_threshold,
):
    boxes = result["boxes"].cpu().numpy()
    labels = result["labels"].cpu().numpy()
    scores = result["scores"].cpu().numpy()
    candidates = []

    for box, score in zip(
        boxes[labels == person_class_id],
        scores[labels == person_class_id],
    ):
        x1, y1, x2, y2 = map(int, box)
        x1, y1 = max(0, x1), max(0, y1)
        x2, y2 = min(frame.shape[1], x2), min(frame.shape[0], y2)
        if x2 > x1 and y2 > y1:
            candidate = {
                "bbox": (x1, y1, x2, y2),
                "centroid": ((x1 + x2) // 2, (y1 + y2) // 2),
                "confidence": float(score),
            }
            if (
                candidate["confidence"] >= activation_threshold
                or is_near_active_track(candidate, track_gallery)
            ):
                candidates.append(candidate)

    candidates = non_max_suppression(candidates)
    if not candidates:
        return []

    person_boxes = [candidate["bbox"] for candidate in candidates]
    embeddings = reid_embedder(
        rgb_frame, np.asarray(person_boxes, dtype=np.float32)
    )
    detections = []
    for candidate, embedding in zip(candidates, embeddings):
        detections.append({**candidate, "embedding": embedding})
    return detections


def associate_detections(
    detections,
    track_gallery,
    identity_gallery,
    person_telemetry,
    next_track_id,
    next_tentative_id,
    source_frame_index,
    max_centroid_displacement,
    max_disappeared_frames,
    max_reentry_frames,
    sample_interval,
    activation_threshold,
):
    active_confirmed_ids = {
        track_id for track_id in track_gallery if track_id > 0
    }
    expired_identity_ids = [
        track_id
        for track_id in identity_gallery
        if track_id not in active_confirmed_ids
        and source_frame_index
        - person_telemetry[track_id]["last_seen_frame"]
        > max_reentry_frames
    ]
    for track_id in expired_identity_ids:
        del identity_gallery[track_id]

    updated_tracks = {}
    matched_detection_indices = set()
    matched_track_ids = set()
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
                distance_limit = centroid_distance_limit(
                    detection,
                    profile,
                    max_centroid_displacement,
                    max_disappeared_frames,
                )
                valid_pairs[detection_index, track_index] = (
                    spatial_distance < distance_limit
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

            original_track_id = active_track_ids[track_index]
            track_id = original_track_id
            detection = detections[detection_index]
            embedding = update_embedding(
                track_gallery[track_id]["embedding"],
                detection["embedding"],
            )
            previous_profile = track_gallery[track_id]
            hits = previous_profile["hits"] + 1
            updated_profile = make_track_profile(
                detection,
                embedding,
                previous_profile["first_seen_frame"],
                hits,
            )
            if track_id < 0 and hits >= MIN_CONFIRMATION_FRAMES:
                track_id = next_track_id
                next_track_id += 1
                identity_gallery[track_id] = embedding
                person_telemetry[track_id] = {
                    "entry_frame": previous_profile["first_seen_frame"],
                    "last_seen_frame": source_frame_index,
                    "last_centroid": detection["centroid"],
                    "last_bbox": detection["bbox"],
                }
            elif track_id > 0:
                identity_gallery[track_id] = embedding
                person_telemetry[track_id]["last_seen_frame"] = source_frame_index
                person_telemetry[track_id]["last_centroid"] = detection["centroid"]
                person_telemetry[track_id]["last_bbox"] = detection["bbox"]
            updated_tracks[track_id] = updated_profile
            matched_detection_indices.add(detection_index)
            matched_track_ids.add(original_track_id)

    unmatched_detection_indices = [
        index
        for index in range(len(detections))
        if index not in matched_detection_indices
    ]
    secondary_detection_indices = [
        index
        for index in unmatched_detection_indices
        if detections[index]["confidence"] >= activation_threshold
    ]
    secondary_track_ids = [
        track_id
        for track_id in track_gallery
        if track_id not in matched_track_ids
    ]
    if secondary_detection_indices and secondary_track_ids:
        spatial_costs = np.full(
            (len(secondary_detection_indices), len(secondary_track_ids)),
            1_000_000.0,
        )
        valid_spatial_pairs = np.zeros(spatial_costs.shape, dtype=bool)
        for row, detection_index in enumerate(secondary_detection_indices):
            detection = detections[detection_index]
            for column, track_id in enumerate(secondary_track_ids):
                profile = track_gallery[track_id]
                spatial_distance = np.hypot(
                    detection["centroid"][0] - profile["centroid"][0],
                    detection["centroid"][1] - profile["centroid"][1],
                )
                distance_limit = centroid_distance_limit(
                    detection,
                    profile,
                    max_centroid_displacement,
                    max_disappeared_frames,
                )
                if spatial_distance < distance_limit:
                    similarity = float(
                        detection["embedding"]
                        @ profile["embedding"]
                    )
                    if similarity >= SECONDARY_MATCH_THRESHOLD:
                        valid_spatial_pairs[row, column] = True
                        spatial_costs[row, column] = (
                            spatial_distance / distance_limit
                        )

        matched_rows, matched_columns = linear_sum_assignment(spatial_costs)
        for row, column in zip(matched_rows, matched_columns):
            if not valid_spatial_pairs[row, column]:
                continue

            detection_index = secondary_detection_indices[row]
            track_id = secondary_track_ids[column]
            detection = detections[detection_index]
            previous_profile = track_gallery[track_id]
            embedding = previous_profile["embedding"]
            hits = previous_profile["hits"] + 1
            updated_profile = make_track_profile(
                detection,
                embedding,
                previous_profile["first_seen_frame"],
                hits,
            )
            original_track_id = track_id
            if track_id < 0 and hits >= MIN_CONFIRMATION_FRAMES:
                track_id = next_track_id
                next_track_id += 1
                identity_gallery[track_id] = embedding
                person_telemetry[track_id] = {
                    "entry_frame": previous_profile["first_seen_frame"],
                    "last_seen_frame": source_frame_index,
                    "last_centroid": detection["centroid"],
                    "last_bbox": detection["bbox"],
                }
            elif track_id > 0:
                person_telemetry[track_id]["last_seen_frame"] = source_frame_index
                person_telemetry[track_id]["last_centroid"] = detection["centroid"]
                person_telemetry[track_id]["last_bbox"] = detection["bbox"]
            updated_tracks[track_id] = updated_profile
            matched_detection_indices.add(detection_index)
            matched_track_ids.add(original_track_id)

    unmatched_detection_indices = [
        index
        for index in range(len(detections))
        if index not in matched_detection_indices
    ]
    reentry_detection_indices = [
        index
        for index in unmatched_detection_indices
        if detections[index]["confidence"] >= activation_threshold
    ]
    reentry_identity_ids = [
        track_id
        for track_id in identity_gallery
        if track_id not in updated_tracks
        and (
            track_id not in track_gallery
            or track_gallery[track_id]["age"] > 0
        )
    ]

    if reentry_detection_indices and reentry_identity_ids:
        unmatched_embeddings = np.stack(
            [detections[index]["embedding"] for index in reentry_detection_indices]
        )
        reentry_embeddings = np.stack(
            [identity_gallery[track_id] for track_id in reentry_identity_ids]
        )
        reentry_similarities = unmatched_embeddings @ reentry_embeddings.T
        reentry_costs = 1.0 - reentry_similarities
        valid_reentry_pairs = np.zeros(reentry_costs.shape, dtype=bool)
        for row, detection_index in enumerate(reentry_detection_indices):
            detection = detections[detection_index]
            for column, track_id in enumerate(reentry_identity_ids):
                telemetry = person_telemetry[track_id]
                elapsed_sample_periods = max(
                    1.0,
                    (
                        source_frame_index - telemetry["last_seen_frame"]
                    )
                    / sample_interval,
                )
                last_centroid = telemetry["last_centroid"]
                spatial_distance = np.hypot(
                    detection["centroid"][0] - last_centroid[0],
                    detection["centroid"][1] - last_centroid[1],
                )
                last_bbox = telemetry["last_bbox"]
                bbox_height = max(
                    detection["bbox"][3] - detection["bbox"][1],
                    last_bbox[3] - last_bbox[1],
                )
                distance_limit = max(
                    max_centroid_displacement * elapsed_sample_periods,
                    MIN_CENTROID_DISTANCE_BBOX_HEIGHTS * bbox_height,
                )
                valid_reentry_pairs[row, column] = (
                    spatial_distance < distance_limit
                )
        reentry_costs[~valid_reentry_pairs] = 1_000_000
        reentry_rows, reentry_columns = linear_sum_assignment(
            reentry_costs
        )

        for unmatched_row, inactive_column in zip(reentry_rows, reentry_columns):
            if (
                not valid_reentry_pairs[unmatched_row, inactive_column]
                or reentry_similarities[unmatched_row, inactive_column]
                < REENTRY_MATCH_THRESHOLD
            ):
                continue

            detection_index = reentry_detection_indices[unmatched_row]
            track_id = reentry_identity_ids[inactive_column]
            detection = detections[detection_index]
            embedding = update_embedding(
                identity_gallery[track_id], detection["embedding"]
            )
            identity_gallery[track_id] = embedding
            updated_tracks[track_id] = make_track_profile(
                detection,
                embedding,
                person_telemetry[track_id]["entry_frame"],
                MIN_CONFIRMATION_FRAMES,
            )
            person_telemetry[track_id]["last_seen_frame"] = source_frame_index
            person_telemetry[track_id]["last_centroid"] = detection["centroid"]
            person_telemetry[track_id]["last_bbox"] = detection["bbox"]
            matched_detection_indices.add(detection_index)

    for detection_index, detection in enumerate(detections):
        if detection_index in matched_detection_indices:
            continue
        if detection["confidence"] < activation_threshold:
            continue
        if any(
            track_id not in matched_track_ids
            and np.hypot(
                detection["centroid"][0] - profile["centroid"][0],
                detection["centroid"][1] - profile["centroid"][1],
            )
            < centroid_distance_limit(
                detection,
                profile,
                max_centroid_displacement,
                max_disappeared_frames,
            )
            and float(detection["embedding"] @ profile["embedding"])
            >= SECONDARY_MATCH_THRESHOLD
            for track_id, profile in track_gallery.items()
        ):
            continue

        embedding = detection["embedding"]
        updated_tracks[next_tentative_id] = make_track_profile(
            detection,
            embedding,
            source_frame_index,
            1,
        )
        next_tentative_id -= 1

    for track_id, profile in track_gallery.items():
        if track_id not in updated_tracks and track_id not in matched_track_ids:
            profile["age"] += 1
            if track_id > 0 and profile["age"] <= max_disappeared_frames:
                updated_tracks[track_id] = profile

    return updated_tracks, next_track_id, next_tentative_id


def active_track_detections(track_gallery):
    active_tracks = [
        (track_id, profile)
        for track_id, profile in track_gallery.items()
        if track_id > 0
    ]
    if not active_tracks:
        return sv.Detections.empty()

    return sv.Detections(
        xyxy=np.asarray(
            [profile["bbox"] for _, profile in active_tracks],
            dtype=np.float32,
        ),
        class_id=np.zeros(len(active_tracks), dtype=np.int32),
        tracker_id=np.asarray(
            [track_id for track_id, _ in active_tracks],
            dtype=np.int32,
        ),
    )




def main():
    args = parse_args()
    device = resolve_device(args.device)
    print(f"Running {args.device.upper()} variant on: {device}")
    if args.device == "gpu":
        torch.backends.cudnn.benchmark = True

    reid_embedder = load_reid_embedder(device)
    detector_model_id = DETECTOR_MODELS[args.detector_model]
    print(f"Loading detector: {detector_model_id}")
    processor = AutoImageProcessor.from_pretrained(detector_model_id)
    model = RTDetrV2ForObjectDetection.from_pretrained(detector_model_id).to(
        device
    )
    person_class_id = resolve_person_class_id(model.config.id2label)

    track_gallery = {}
    identity_gallery = {}
    next_track_id = 1
    next_tentative_id = -1
    person_telemetry = {}

    input_path = args.video
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    telemetry_path = Path(
        f"outputs/{input_path.stem}_telemetry_{args.device}_{run_timestamp}.csv"
    )
    line_counts_path = (
        Path(
            f"outputs/{input_path.stem}_line_counts_{args.device}_"
            f"{run_timestamp}.csv"
        )
        if args.line is not None
        else None
    )
    cap = cv2.VideoCapture(str(input_path))
    if not cap.isOpened():
        raise RuntimeError(f"Could not open input video: {input_path}")

    fps = cap.get(cv2.CAP_PROP_FPS)
    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    if fps <= 0 or frame_width <= 0 or frame_height <= 0:
        cap.release()
        raise RuntimeError(f"Invalid video metadata for input: {input_path}")

    results_initialized = False
    line_count_records = []
    try:
        line_zone = create_line_zone(args.line, frame_width, frame_height)
        total_source_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        requested_sample_fps = args.sample_fps or fps
        sample_interval = max(1, round(fps / min(requested_sample_fps, fps)))
        effective_sample_fps = fps / sample_interval
        batch_size = args.batch_size or (8 if args.device == "gpu" else 1)
        use_fp16 = args.device == "gpu" and not args.no_fp16
        max_disappeared_frames = max(
            1,
            round(
                retention_seconds_for_sample_rate(effective_sample_fps)
                * effective_sample_fps
            ),
        )
        max_reentry_frames = round(MAX_REENTRY_SECONDS * fps)
        max_centroid_displacement = (
            frame_height
            * MAX_CENTROID_SPEED_FRAME_HEIGHTS_PER_SECOND
            / effective_sample_fps
        )
        total_sampled_frames = (
            math.ceil(total_source_frames / sample_interval)
            if total_source_frames > 0
            else 0
        )
        read_state = FrameReadState()
        telemetry_path.parent.mkdir(parents=True, exist_ok=True)
        results_initialized = True
        processed_frames = 0
        processing_started = time.perf_counter()

        print(
            f"Sampling {effective_sample_fps:.2f} FPS (every {sample_interval} "
            f"source frame(s)); detector batch size {batch_size}; FP16 {use_fp16}"
        )

        for frame_batch in iter_sampled_frame_batches(
            cap,
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
            detector_frames = [
                cv2.resize(
                    rgb_frame,
                    DETECTOR_IMAGE_SIZE,
                    interpolation=cv2.INTER_LINEAR,
                )
                for rgb_frame in rgb_frames
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
                [frame.shape[:2] for frame in frames],
                device=device,
            )
            results = processor.post_process_object_detection(
                outputs,
                target_sizes=target_sizes,
                threshold=DETECTOR_FLOOR,
            )

            for source_frame_index, frame, rgb_frame, result in zip(
                source_frame_indices,
                frames,
                rgb_frames,
                results,
            ):
                detections = extract_person_detections(
                    result,
                    frame,
                    rgb_frame,
                    reid_embedder,
                    person_class_id,
                    track_gallery,
                    args.detection_threshold,
                )
                (
                    track_gallery,
                    next_track_id,
                    next_tentative_id,
                ) = associate_detections(
                    detections,
                    track_gallery,
                    identity_gallery,
                    person_telemetry,
                    next_track_id,
                    next_tentative_id,
                    source_frame_index,
                    max_centroid_displacement,
                    max_disappeared_frames,
                    max_reentry_frames,
                    sample_interval,
                    args.detection_threshold,
                )
                if line_zone is not None:
                    record_line_counts(
                        line_zone,
                        active_track_detections(track_gallery),
                        source_frame_index,
                        fps,
                        args.line,
                        line_count_records,
                    )

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
        cap.release()
        if results_initialized:
            write_telemetry(telemetry_path, person_telemetry, fps)
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
    print(
        f"Process ended cleanly. Total distinct individuals: "
        f"{len(person_telemetry)}"
    )
    print(f"Person telemetry saved to: {telemetry_path}")
    if line_counts_path is not None:
        print(
            f"Line crossings: {line_zone.in_count} in, "
            f"{line_zone.out_count} out"
        )
        print(f"Line counts saved to: {line_counts_path}")


if __name__ == "__main__":
    main()
