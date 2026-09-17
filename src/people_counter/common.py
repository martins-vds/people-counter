import argparse
import csv
import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import supervision as sv


DETECTOR_FLOOR = 0.1
MAX_DISAPPEARED_SECONDS = 1.0
MIN_CONFIRMATION_FRAMES = 2


@dataclass
class FrameReadState:
    source_frames_read: int = 0
    ended_early: bool = False


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


def detection_threshold_value(value):
    parsed_value = float(value)
    if not DETECTOR_FLOOR <= parsed_value <= 1:
        raise argparse.ArgumentTypeError(
            f"Detection threshold must be in the range [{DETECTOR_FLOOR}, 1]"
        )
    return parsed_value


def retention_seconds_for_sample_rate(effective_sample_fps):
    return max(
        MAX_DISAPPEARED_SECONDS,
        2.0 / effective_sample_fps,
    )


def lost_track_buffer_for_sample_rate(effective_sample_fps):
    return math.ceil(
        30 * retention_seconds_for_sample_rate(effective_sample_fps)
    )


def confirmed_entry_frame(
    source_frame_index,
    sample_interval,
    confirmation_frames=MIN_CONFIRMATION_FRAMES,
):
    return max(
        0,
        source_frame_index - (confirmation_frames - 1) * sample_interval,
    )


def coasting_track_detections(
    detections,
    track_cache,
    source_frame_index,
    max_coast_source_frames,
):
    if detections.tracker_id is not None:
        for tracker_id, bbox in zip(detections.tracker_id, detections.xyxy):
            track_cache[int(tracker_id)] = {
                "bbox": np.asarray(bbox, dtype=np.float32),
                "last_seen_frame": source_frame_index,
            }

    expired_ids = [
        track_id
        for track_id, profile in track_cache.items()
        if source_frame_index - profile["last_seen_frame"]
        > max_coast_source_frames
    ]
    for track_id in expired_ids:
        del track_cache[track_id]

    if not track_cache:
        return sv.Detections.empty()
    return sv.Detections(
        xyxy=np.stack(
            [profile["bbox"] for profile in track_cache.values()]
        ),
        class_id=np.zeros(len(track_cache), dtype=np.int32),
        tracker_id=np.asarray(list(track_cache), dtype=np.int32),
    )


def iter_sampled_frame_batches(
    capture,
    sample_interval,
    batch_size,
    expected_source_frames,
    read_state,
):
    batch = []
    source_frame_index = 0

    while capture.isOpened():
        if source_frame_index % sample_interval == 0:
            success, frame = capture.read()
        else:
            success = capture.grab()
            frame = None
        if not success:
            read_state.ended_early = (
                expected_source_frames > 0
                and source_frame_index < expected_source_frames - 1
            )
            break

        read_state.source_frames_read = source_frame_index + 1
        if frame is not None:
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


def create_line_zone(line_coordinates, frame_width, frame_height):
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


def record_line_counts(
    line_zone,
    detections,
    source_frame_index,
    fps,
    line_coordinates,
    line_count_records,
):
    if line_zone is None:
        return

    crossed_in, crossed_out = line_zone.trigger(detections)
    x1, y1, x2, y2 = line_coordinates
    line_count_records.append(
        {
            "frame": source_frame_index,
            "video_seconds": f"{source_frame_index / fps:.3f}",
            "video_timestamp": format_video_timestamp(source_frame_index, fps),
            "frame_in_count": int(crossed_in.sum()),
            "frame_out_count": int(crossed_out.sum()),
            "cumulative_in_count": line_zone.in_count,
            "cumulative_out_count": line_zone.out_count,
            "line_start_x": x1,
            "line_start_y": y1,
            "line_end_x": x2,
            "line_end_y": y2,
        }
    )


def write_line_counts(line_counts_path, line_count_records):
    fieldnames = [
        "frame",
        "video_seconds",
        "video_timestamp",
        "frame_in_count",
        "frame_out_count",
        "cumulative_in_count",
        "cumulative_out_count",
        "line_start_x",
        "line_start_y",
        "line_end_x",
        "line_end_y",
    ]
    with line_counts_path.open("w", newline="", encoding="utf-8") as counts_file:
        writer = csv.DictWriter(counts_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(line_count_records)


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
