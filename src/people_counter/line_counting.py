"""Directed line counting and short-term BoT-SORT geometry retention."""

from dataclasses import dataclass

import numpy as np
import supervision as sv
from numpy.typing import NDArray

from people_counter.models import (
    LineCoordinates,
    LineCountRecord,
    TrackProfile,
)
from people_counter.video import format_video_timestamp


@dataclass
class CoastingTrack:
    bbox: NDArray[np.float32]
    last_seen_frame: int


BoTSORTCoastingCache = dict[int, CoastingTrack]


def create_line_zone(
    line_coordinates: LineCoordinates | None,
    frame_width: int,
    frame_height: int,
) -> sv.LineZone | None:
    if line_coordinates is None:
        return None

    x1, y1, x2, y2 = line_coordinates
    if (x1, y1) == (x2, y2):
        raise ValueError("Counting line start and end coordinates must differ")
    for x, y in ((x1, y1), (x2, y2)):
        if not 0 <= x < frame_width or not 0 <= y < frame_height:
            raise ValueError(
                f"Counting line point ({x}, {y}) is outside the "
                f"{frame_width}x{frame_height} video frame"
            )

    return sv.LineZone(
        start=sv.Point(x=x1, y=y1),
        end=sv.Point(x=x2, y=y2),
        triggering_anchors=(sv.Position.BOTTOM_CENTER,),
    )


def active_track_detections(
    track_gallery: dict[int, TrackProfile],
) -> sv.Detections:
    active_tracks = [
        (track_id, profile)
        for track_id, profile in track_gallery.items()
        if track_id > 0
    ]
    if not active_tracks:
        return sv.Detections.empty()
    return sv.Detections(
        xyxy=np.asarray(
            [profile.bbox for _, profile in active_tracks],
            dtype=np.float32,
        ),
        class_id=np.zeros(len(active_tracks), dtype=np.int32),
        tracker_id=np.asarray(
            [track_id for track_id, _ in active_tracks],
            dtype=np.int32,
        ),
    )


def coasting_track_detections(
    detections: sv.Detections,
    track_cache: BoTSORTCoastingCache,
    source_frame_index: int,
    max_coast_source_frames: int,
) -> sv.Detections:
    if detections.tracker_id is not None:
        for tracker_id, bbox in zip(detections.tracker_id, detections.xyxy):
            track_cache[int(tracker_id)] = CoastingTrack(
                bbox=np.asarray(bbox, dtype=np.float32),
                last_seen_frame=source_frame_index,
            )

    expired_ids = [
        track_id
        for track_id, profile in track_cache.items()
        if source_frame_index - profile.last_seen_frame
        > max_coast_source_frames
    ]
    for track_id in expired_ids:
        del track_cache[track_id]

    if not track_cache:
        return sv.Detections.empty()
    return sv.Detections(
        xyxy=np.stack([profile.bbox for profile in track_cache.values()]),
        class_id=np.zeros(len(track_cache), dtype=np.int32),
        tracker_id=np.asarray(list(track_cache), dtype=np.int32),
    )


def record_line_counts(
    line_zone: sv.LineZone | None,
    detections: sv.Detections,
    source_frame_index: int,
    fps: float,
    line_coordinates: LineCoordinates | None,
    line_count_records: list[LineCountRecord],
) -> None:
    if line_zone is None or line_coordinates is None:
        return

    crossed_in, crossed_out = line_zone.trigger(detections)
    x1, y1, x2, y2 = line_coordinates
    line_count_records.append(
        LineCountRecord(
            frame=source_frame_index,
            video_seconds=f"{source_frame_index / fps:.3f}",
            video_timestamp=format_video_timestamp(source_frame_index, fps),
            frame_in_count=int(crossed_in.sum()),
            frame_out_count=int(crossed_out.sum()),
            cumulative_in_count=line_zone.in_count,
            cumulative_out_count=line_zone.out_count,
            line_start_x=x1,
            line_start_y=y1,
            line_end_x=x2,
            line_end_y=y2,
        )
    )
