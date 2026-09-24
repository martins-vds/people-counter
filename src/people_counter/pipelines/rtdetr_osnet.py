import hashlib
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import cv2
import numpy as np
import torch
from huggingface_hub import hf_hub_download
from libreyolo.tracking.reid import OSNetEmbedder
from people_counter.config import (
    DETECTOR_FLOOR,
    MIN_CONFIRMATION_FRAMES,
    RTDetrOsnetConfig,
    SamplingConfig,
    disappeared_frames_for_sample_rate,
    sampling_config,
)
from people_counter.line_counting import (
    active_track_detections,
    create_line_zone,
    record_line_counts,
)
from people_counter.model_artifacts import (
    OSNET_FILENAME as REID_FILENAME,
    resolve_rtdetr_osnet_artifacts,
)
from people_counter.models import (
    Detection,
    Embedding,
    LastGeometry,
    PersonTelemetry,
    RunResult,
    TrackProfile,
)
from people_counter.video import (
    FrameBatch,
    FrameReadState,
    VideoCapture,
    VideoMetadata,
    iter_sampled_frame_batches,
    read_video_metadata,
)
from scipy.optimize import linear_sum_assignment
from transformers import AutoImageProcessor, RTDetrV2ForObjectDetection

REID_REPO_ID = "LibreYOLO/LibreReID-osnet"
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
INVALID_ASSIGNMENT_COST = 1_000_000.0
DETECTOR_MODELS = {
    "r18": "PekingU/rtdetr_v2_r18vd",
    "r50": "PekingU/rtdetr_v2_r50vd",
}


@dataclass
class RTDetrTrackingState:
    tracks: dict[int, TrackProfile] = field(default_factory=dict)
    identities: dict[int, Embedding] = field(default_factory=dict)
    telemetry: dict[int, PersonTelemetry] = field(default_factory=dict)
    next_track_id: int = 1
    next_tentative_id: int = -1


@dataclass
class AssociationState:
    detections: list[Detection]
    tracking: RTDetrTrackingState
    source_frame_index: int
    max_centroid_displacement: float
    max_disappeared_frames: int
    max_reentry_frames: int
    sample_interval: int
    activation_threshold: float
    updated_tracks: dict[int, TrackProfile] = field(default_factory=dict)
    matched_detection_indices: set[int] = field(default_factory=set)
    matched_track_ids: set[int] = field(default_factory=set)


@dataclass(frozen=True)
class RTDetrRuntime:
    device: torch.device
    reid_embedder: Any
    processor: Any
    model: Any
    person_class_id: int


@dataclass(frozen=True)
class RTDetrRunState:
    config: RTDetrOsnetConfig
    runtime: RTDetrRuntime
    tracking: RTDetrTrackingState
    metadata: VideoMetadata
    sampling: SamplingConfig
    max_disappeared_frames: int
    max_reentry_frames: int
    max_centroid_displacement: float
    line_zone: Any


def detection_embedding(detection: Detection) -> Embedding:
    if detection.embedding is None:
        raise RuntimeError("Detection is missing its identity embedding")
    return detection.embedding


def update_person_telemetry(
    telemetry: PersonTelemetry,
    detection: Detection,
    source_frame_index: int,
) -> None:
    telemetry.last_seen_frame = source_frame_index
    telemetry.last_geometry = LastGeometry(
        centroid=detection.centroid,
        bbox=detection.bbox,
    )


def load_reid_embedder(
    device: torch.device,
    weights_path: Path | None = None,
):
    if weights_path is None:
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


def make_track_profile(
    detection: Detection,
    embedding,
    first_seen_frame: int,
    hits: int,
) -> TrackProfile:
    return TrackProfile(
        centroid=detection.centroid,
        embedding=embedding,
        age=0,
        bbox=detection.bbox,
        first_seen_frame=first_seen_frame,
        hits=hits,
    )


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
    detection: Detection,
    profile: TrackProfile,
    max_centroid_displacement: float,
    max_disappeared_frames: int,
) -> float:
    detection_height = detection.bbox[3] - detection.bbox[1]
    profile_height = profile.bbox[3] - profile.bbox[1]
    return max(
        max_centroid_displacement
        * min(profile.age + 1, max_disappeared_frames + 1),
        MIN_CENTROID_DISTANCE_BBOX_HEIGHTS
        * max(detection_height, profile_height),
    )


def is_near_active_track(
    detection: Detection,
    track_gallery: dict[int, TrackProfile],
) -> bool:
    for profile in track_gallery.values():
        spatial_distance = np.hypot(
            detection.centroid[0] - profile.centroid[0],
            detection.centroid[1] - profile.centroid[1],
        )
        detection_height = detection.bbox[3] - detection.bbox[1]
        profile_height = profile.bbox[3] - profile.bbox[1]
        if spatial_distance < max(detection_height, profile_height):
            return True
    return False


def non_max_suppression(detections: list[Detection]) -> list[Detection]:
    if not detections:
        return []

    boxes = np.asarray([detection.bbox for detection in detections])
    scores = np.asarray([detection.confidence for detection in detections])
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
        iou = intersection / union
        order = remaining[iou <= PERSON_NMS_IOU_THRESHOLD]

    return [detections[index] for index in kept_indices]


def extract_person_detections(
    result,
    frame,
    rgb_frame,
    reid_embedder,
    person_class_id,
    track_gallery: dict[int, TrackProfile],
    activation_threshold: float,
) -> list[Detection]:
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
            candidate = Detection(
                bbox=(x1, y1, x2, y2),
                centroid=((x1 + x2) // 2, (y1 + y2) // 2),
                confidence=float(score),
            )
            if (
                candidate.confidence >= activation_threshold
                or is_near_active_track(candidate, track_gallery)
            ):
                candidates.append(candidate)

    candidates = non_max_suppression(candidates)
    if not candidates:
        return []

    person_boxes = [candidate.bbox for candidate in candidates]
    embeddings = reid_embedder(
        rgb_frame, np.asarray(person_boxes, dtype=np.float32)
    )
    detections = []
    for candidate, embedding in zip(candidates, embeddings):
        detections.append(
            Detection(
                bbox=candidate.bbox,
                centroid=candidate.centroid,
                confidence=candidate.confidence,
                embedding=embedding,
            )
        )
    return detections


def associate_detections(
    detections: list[Detection],
    tracking: RTDetrTrackingState,
    source_frame_index: int,
    max_centroid_displacement: float,
    max_disappeared_frames: int,
    max_reentry_frames: int,
    sample_interval: int,
    activation_threshold: float,
) -> None:
    state = AssociationState(
        detections=detections,
        tracking=tracking,
        source_frame_index=source_frame_index,
        max_centroid_displacement=max_centroid_displacement,
        max_disappeared_frames=max_disappeared_frames,
        max_reentry_frames=max_reentry_frames,
        sample_interval=sample_interval,
        activation_threshold=activation_threshold,
    )
    expire_inactive_identities(state)
    assign_active_appearance_matches(state)
    assign_spatial_secondary_matches(state)
    assign_reentry_matches(state)
    spawn_tentative_tracks(state)
    age_unmatched_tracks(state)
    tracking.tracks = state.updated_tracks


def expire_inactive_identities(state: AssociationState) -> None:
    active_confirmed_ids = {
        track_id for track_id in state.tracking.tracks if track_id > 0
    }
    expired_identity_ids = [
        track_id
        for track_id in state.tracking.identities
        if track_id not in active_confirmed_ids
        and state.source_frame_index
        - state.tracking.telemetry[track_id].last_seen_frame
        > state.max_reentry_frames
    ]
    for track_id in expired_identity_ids:
        del state.tracking.identities[track_id]


def _active_spatially_valid(
    state: AssociationState,
    detection: Detection,
    profile: TrackProfile,
) -> bool:
    spatial_distance = np.hypot(
        detection.centroid[0] - profile.centroid[0],
        detection.centroid[1] - profile.centroid[1],
    )
    return spatial_distance < centroid_distance_limit(
        detection,
        profile,
        state.max_centroid_displacement,
        state.max_disappeared_frames,
    )


def _record_track_match(
    state: AssociationState,
    detection_index: int,
    original_track_id: int,
    embedding: Embedding,
    update_identity: bool,
) -> None:
    detection = state.detections[detection_index]
    previous_profile = state.tracking.tracks[original_track_id]
    hits = previous_profile.hits + 1
    track_id = original_track_id
    if track_id < 0 and hits >= MIN_CONFIRMATION_FRAMES:
        track_id = state.tracking.next_track_id
        state.tracking.next_track_id += 1
        state.tracking.identities[track_id] = embedding
        state.tracking.telemetry[track_id] = PersonTelemetry(
            entry_frame=previous_profile.first_seen_frame,
            last_seen_frame=state.source_frame_index,
            last_geometry=LastGeometry(
                centroid=detection.centroid,
                bbox=detection.bbox,
            ),
        )
    elif track_id > 0:
        if update_identity:
            state.tracking.identities[track_id] = embedding
        update_person_telemetry(
            state.tracking.telemetry[track_id],
            detection,
            state.source_frame_index,
        )
    state.updated_tracks[track_id] = make_track_profile(
        detection,
        embedding,
        previous_profile.first_seen_frame,
        hits,
    )
    state.matched_detection_indices.add(detection_index)
    state.matched_track_ids.add(original_track_id)


def _finite_assignments(costs: np.ndarray) -> list[tuple[int, int]]:
    candidate_rows = [
        index for index, row in enumerate(costs) if np.isfinite(row).any()
    ]
    if not candidate_rows:
        return []
    candidate_columns = [
        index
        for index, column in enumerate(costs.T)
        if np.isfinite(column).any()
    ]
    filtered_costs = costs[np.ix_(candidate_rows, candidate_columns)]
    solver_costs = np.where(
        np.isfinite(filtered_costs),
        filtered_costs,
        INVALID_ASSIGNMENT_COST,
    )
    matched_rows, matched_columns = linear_sum_assignment(solver_costs)
    return [
        (candidate_rows[int(row)], candidate_columns[int(column)])
        for row, column in zip(matched_rows, matched_columns)
        if np.isfinite(filtered_costs[row, column])
    ]


def assign_active_appearance_matches(state: AssociationState) -> None:
    active_track_ids = list(state.tracking.tracks)
    if not state.detections:
        return
    if not active_track_ids:
        return
    detection_embeddings = np.stack(
        [detection_embedding(detection) for detection in state.detections]
    )
    active_embeddings = np.stack(
        [
            state.tracking.tracks[track_id].embedding
            for track_id in active_track_ids
        ]
    )
    similarities = detection_embeddings @ active_embeddings.T
    costs = np.asarray(
        [
            [
                -similarities[detection_index, track_index]
                if _active_spatially_valid(
                    state,
                    detection,
                    state.tracking.tracks[track_id],
                )
                else np.inf
                for track_index, track_id in enumerate(active_track_ids)
            ]
            for detection_index, detection in enumerate(state.detections)
        ],
    )
    for detection_index, track_index in _finite_assignments(costs):
        if similarities[detection_index, track_index] < ACTIVE_MATCH_THRESHOLD:
            continue
        track_id = active_track_ids[track_index]
        embedding = update_embedding(
            state.tracking.tracks[track_id].embedding,
            detection_embeddings[detection_index],
        )
        _record_track_match(
            state,
            int(detection_index),
            track_id,
            embedding,
            update_identity=True,
        )


def _unmatched_activated_detections(state: AssociationState) -> list[int]:
    return [
        index
        for index, detection in enumerate(state.detections)
        if index not in state.matched_detection_indices
        and detection.confidence >= state.activation_threshold
    ]


def _secondary_pair_cost(
    state: AssociationState,
    detection: Detection,
    profile: TrackProfile,
) -> float | None:
    spatial_distance = np.hypot(
        detection.centroid[0] - profile.centroid[0],
        detection.centroid[1] - profile.centroid[1],
    )
    distance_limit = centroid_distance_limit(
        detection,
        profile,
        state.max_centroid_displacement,
        state.max_disappeared_frames,
    )
    if spatial_distance >= distance_limit:
        return None
    similarity = float(detection_embedding(detection) @ profile.embedding)
    if similarity < SECONDARY_MATCH_THRESHOLD:
        return None
    return float(spatial_distance / distance_limit)


def assign_spatial_secondary_matches(state: AssociationState) -> None:
    detection_indices = _unmatched_activated_detections(state)
    track_ids = [
        track_id
        for track_id in state.tracking.tracks
        if track_id not in state.matched_track_ids
    ]
    if not detection_indices:
        return
    if not track_ids:
        return
    costs = np.asarray(
        [
            [
                np.inf
                if (
                    cost := _secondary_pair_cost(
                        state,
                        state.detections[detection_index],
                        state.tracking.tracks[track_id],
                    )
                )
                is None
                else cost
                for track_id in track_ids
            ]
            for detection_index in detection_indices
        ],
    )
    for row, column in _finite_assignments(costs):
        detection_index = detection_indices[row]
        track_id = track_ids[column]
        _record_track_match(
            state,
            detection_index,
            track_id,
            state.tracking.tracks[track_id].embedding,
            update_identity=False,
        )


def _reentry_spatially_valid(
    state: AssociationState,
    detection: Detection,
    telemetry: PersonTelemetry,
) -> bool:
    if telemetry.last_geometry is None:
        raise RuntimeError("Identity is missing its last geometry")
    elapsed_sample_periods = max(
        1.0,
        (
            state.source_frame_index - telemetry.last_seen_frame
        )
        / state.sample_interval,
    )
    last_geometry = telemetry.last_geometry
    spatial_distance = np.hypot(
        detection.centroid[0] - last_geometry.centroid[0],
        detection.centroid[1] - last_geometry.centroid[1],
    )
    bbox_height = max(
        detection.bbox[3] - detection.bbox[1],
        last_geometry.bbox[3] - last_geometry.bbox[1],
    )
    distance_limit = max(
        state.max_centroid_displacement * elapsed_sample_periods,
        MIN_CENTROID_DISTANCE_BBOX_HEIGHTS * bbox_height,
    )
    return spatial_distance < distance_limit


def _reentry_identity_ids(state: AssociationState) -> list[int]:
    return [
        track_id
        for track_id in state.tracking.identities
        if track_id not in state.updated_tracks
        and (
            track_id not in state.tracking.tracks
            or state.tracking.tracks[track_id].age > 0
        )
    ]


def _record_reentry_match(
    state: AssociationState,
    detection_index: int,
    track_id: int,
) -> None:
    detection = state.detections[detection_index]
    embedding = update_embedding(
        state.tracking.identities[track_id],
        detection_embedding(detection),
    )
    state.tracking.identities[track_id] = embedding
    state.updated_tracks[track_id] = make_track_profile(
        detection,
        embedding,
        state.tracking.telemetry[track_id].entry_frame,
        MIN_CONFIRMATION_FRAMES,
    )
    update_person_telemetry(
        state.tracking.telemetry[track_id],
        detection,
        state.source_frame_index,
    )
    state.matched_detection_indices.add(detection_index)


def assign_reentry_matches(state: AssociationState) -> None:
    detection_indices = _unmatched_activated_detections(state)
    identity_ids = _reentry_identity_ids(state)
    if not detection_indices:
        return
    if not identity_ids:
        return
    detection_embeddings = np.stack(
        [
            detection_embedding(state.detections[index])
            for index in detection_indices
        ]
    )
    identity_embeddings = np.stack(
        [state.tracking.identities[track_id] for track_id in identity_ids]
    )
    similarities = detection_embeddings @ identity_embeddings.T
    costs = np.asarray(
        [
            [
                -similarities[row, column]
                if _reentry_spatially_valid(
                    state,
                    state.detections[detection_index],
                    state.tracking.telemetry[track_id],
                )
                else np.inf
                for column, track_id in enumerate(identity_ids)
            ]
            for row, detection_index in enumerate(detection_indices)
        ],
    )
    for detection_index, identity_index in _finite_assignments(costs):
        if similarities[detection_index, identity_index] < REENTRY_MATCH_THRESHOLD:
            continue
        _record_reentry_match(
            state,
            detection_indices[detection_index],
            identity_ids[identity_index],
        )


def _overlaps_unmatched_track(
    state: AssociationState,
    detection: Detection,
) -> bool:
    return any(
        track_id not in state.matched_track_ids
        and _active_spatially_valid(state, detection, profile)
        and float(detection_embedding(detection) @ profile.embedding)
        >= SECONDARY_MATCH_THRESHOLD
        for track_id, profile in state.tracking.tracks.items()
    )


def spawn_tentative_tracks(state: AssociationState) -> None:
    for detection_index in _unmatched_activated_detections(state):
        detection = state.detections[detection_index]
        if _overlaps_unmatched_track(state, detection):
            continue
        state.updated_tracks[
            state.tracking.next_tentative_id
        ] = make_track_profile(
            detection,
            detection_embedding(detection),
            state.source_frame_index,
            1,
        )
        state.tracking.next_tentative_id -= 1


def age_unmatched_tracks(state: AssociationState) -> None:
    for track_id, profile in state.tracking.tracks.items():
        if (
            track_id in state.updated_tracks
            or track_id in state.matched_track_ids
        ):
            continue
        profile.age += 1
        if (
            track_id > 0
            and profile.age <= state.max_disappeared_frames
        ):
            state.updated_tracks[track_id] = profile




def load_runtime(config: RTDetrOsnetConfig) -> RTDetrRuntime:
    device = torch.device(config.device)
    if config.device_variant == "gpu":
        torch.backends.cudnn.benchmark = True
    if config.models_dir is None:
        detector_model: str | Path = DETECTOR_MODELS[config.detector_model]
        reid_embedder = load_reid_embedder(device)
        load_kwargs: dict[str, bool] = {}
    else:
        detector_model, reid_path = resolve_rtdetr_osnet_artifacts(
            config.models_dir,
            config.detector_model,
        )
        reid_embedder = load_reid_embedder(device, reid_path)
        load_kwargs = {"local_files_only": True}
    processor = AutoImageProcessor.from_pretrained(
        detector_model,
        **load_kwargs,
    )
    model = RTDetrV2ForObjectDetection.from_pretrained(
        detector_model,
        **load_kwargs,
    ).to(device)
    return RTDetrRuntime(
        device=device,
        reid_embedder=reid_embedder,
        processor=processor,
        model=model,
        person_class_id=resolve_person_class_id(model.config.id2label),
    )


def _notify_progress(config: RTDetrOsnetConfig) -> None:
    if config.progress_callback is not None:
        config.progress_callback(config.result)


def initialize_run_state(
    config: RTDetrOsnetConfig,
    runtime: RTDetrRuntime,
    tracking: RTDetrTrackingState,
    metadata: VideoMetadata,
) -> RTDetrRunState:
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
    max_disappeared_frames = disappeared_frames_for_sample_rate(
        sampling.effective_fps
    )
    result = config.result
    result.fps = metadata.fps
    result.total_source_frames = metadata.total_source_frames
    result.total_sampled_frames = sampling.total_sampled_frames
    result.sample_interval = sampling.interval
    result.effective_sample_fps = sampling.effective_fps
    result.batch_size = config.batch_size
    result.use_fp16 = config.use_fp16
    result.initialized = True
    _notify_progress(config)
    return RTDetrRunState(
        config=config,
        runtime=runtime,
        tracking=tracking,
        metadata=metadata,
        sampling=sampling,
        max_disappeared_frames=max_disappeared_frames,
        max_reentry_frames=round(MAX_REENTRY_SECONDS * metadata.fps),
        max_centroid_displacement=(
            metadata.height
            * MAX_CENTROID_SPEED_FRAME_HEIGHTS_PER_SECOND
            / sampling.effective_fps
        ),
        line_zone=line_zone,
    )


def predict_detector_results(
    state: RTDetrRunState,
    frames: list[np.ndarray],
    rgb_frames: list[np.ndarray],
) -> Any:
    detector_frames = [
        cv2.resize(
            rgb_frame,
            DETECTOR_IMAGE_SIZE,
            interpolation=cv2.INTER_LINEAR,
        )
        for rgb_frame in rgb_frames
    ]
    inputs = state.runtime.processor(
        images=detector_frames,
        do_resize=False,
        return_tensors="pt",
    ).to(state.runtime.device)
    with torch.inference_mode(), torch.autocast(
        device_type=state.runtime.device.type,
        dtype=torch.float16,
        enabled=state.config.use_fp16,
    ):
        outputs = state.runtime.model(**inputs)
    target_sizes = torch.tensor(
        [frame.shape[:2] for frame in frames],
        device=state.runtime.device,
    )
    return state.runtime.processor.post_process_object_detection(
        outputs,
        target_sizes=target_sizes,
        threshold=DETECTOR_FLOOR,
    )


def process_frame(
    state: RTDetrRunState,
    source_frame_index: int,
    frame: np.ndarray,
    rgb_frame: np.ndarray,
    detector_result: Any,
) -> None:
    detections = extract_person_detections(
        detector_result,
        frame,
        rgb_frame,
        state.runtime.reid_embedder,
        state.runtime.person_class_id,
        state.tracking.tracks,
        state.config.detection_threshold,
    )
    associate_detections(
        detections,
        state.tracking,
        source_frame_index,
        state.max_centroid_displacement,
        state.max_disappeared_frames,
        state.max_reentry_frames,
        state.sampling.interval,
        state.config.detection_threshold,
    )
    if state.line_zone is not None:
        record_line_counts(
            state.line_zone,
            active_track_detections(state.tracking.tracks),
            source_frame_index,
            state.metadata.fps,
            state.config.line,
            state.config.result.line_counts,
        )


def process_frame_batch(
    state: RTDetrRunState,
    frame_batch: FrameBatch,
) -> None:
    source_frame_indices = [item[0] for item in frame_batch]
    frames = [item[1] for item in frame_batch]
    rgb_frames = [
        cv2.cvtColor(frame, cv2.COLOR_BGR2RGB) for frame in frames
    ]
    detector_results = predict_detector_results(state, frames, rgb_frames)
    for source_frame_index, frame, rgb_frame, detector_result in zip(
        source_frame_indices,
        frames,
        rgb_frames,
        detector_results,
    ):
        process_frame(
            state,
            source_frame_index,
            frame,
            rgb_frame,
            detector_result,
        )
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


def run(config: RTDetrOsnetConfig) -> RunResult:
    """Run RT-DETR/OSNet and mutate the result owned by ``config``."""
    config.result.ensure_unused()
    runtime = load_runtime(config)
    tracking = RTDetrTrackingState(telemetry=config.result.telemetry)
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
        state = initialize_run_state(
            config,
            runtime,
            tracking,
            metadata,
        )
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
