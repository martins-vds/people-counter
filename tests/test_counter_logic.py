import csv
import tempfile
import unittest
from pathlib import Path

import numpy as np
import supervision as sv
import torch

from people_counter.common import (
    FrameReadState,
    coasting_track_detections,
    confirmed_entry_frame,
    create_line_zone,
    format_video_timestamp,
    iter_sampled_frame_batches,
    lost_track_buffer_for_sample_rate,
    retention_seconds_for_sample_rate,
    write_line_counts,
    write_telemetry,
)
from rtdetr_osnet_counter import (
    active_track_detections,
    associate_detections,
    extract_person_detections,
    non_max_suppression,
)


def detection(x, confidence=0.9, embedding=(1.0, 0.0)):
    return {
        "bbox": (x, 0, x + 20, 40),
        "centroid": (x + 10, 20),
        "confidence": confidence,
        "embedding": np.asarray(embedding, dtype=np.float32),
    }


class FakeCapture:
    def __init__(self, frames):
        self.frames = list(frames)
        self.index = 0

    def isOpened(self):
        return True

    def read(self):
        if self.index == len(self.frames):
            return False, None
        frame = self.frames[self.index]
        self.index += 1
        return True, frame

    def grab(self):
        if self.index == len(self.frames):
            return False
        self.index += 1
        return True


class RecordingEmbedder:
    def __init__(self):
        self.boxes = None

    def __call__(self, frame, boxes):
        del frame
        self.boxes = boxes
        return np.tile(
            np.asarray([1.0, 0.0], dtype=np.float32),
            (len(boxes), 1),
        )


class AssociationTests(unittest.TestCase):
    def setUp(self):
        self.tracks = {}
        self.identities = {}
        self.telemetry = {}
        self.next_track_id = 1
        self.next_tentative_id = -1

    def associate(self, detections, frame_index, sample_interval=1):
        (
            self.tracks,
            self.next_track_id,
            self.next_tentative_id,
        ) = associate_detections(
            detections,
            self.tracks,
            self.identities,
            self.telemetry,
            self.next_track_id,
            self.next_tentative_id,
            frame_index,
            max_centroid_displacement=50,
            max_disappeared_frames=1,
            max_reentry_frames=10,
            sample_interval=sample_interval,
            activation_threshold=0.6,
        )

    def test_single_frame_detection_is_not_counted(self):
        self.associate([detection(0)], 0)

        self.assertEqual(set(self.tracks), {-1})
        self.assertEqual(self.identities, {})
        self.assertEqual(self.telemetry, {})

        self.associate([], 1)
        self.associate([], 2)

        self.assertEqual(self.tracks, {})
        self.assertEqual(self.telemetry, {})

    def test_second_matching_frame_confirms_identity(self):
        self.associate([detection(0)], 0)
        self.associate([detection(5)], 1)

        self.assertEqual(set(self.tracks), {1})
        self.assertEqual(set(self.identities), {1})
        self.assertEqual(self.telemetry[1]["entry_frame"], 0)
        self.assertEqual(self.telemetry[1]["last_seen_frame"], 1)

    def test_low_confidence_detection_cannot_start_identity(self):
        self.associate([detection(0, confidence=0.2)], 0)

        self.assertEqual(self.tracks, {})
        self.assertEqual(self.telemetry, {})

    def test_low_confidence_detection_can_confirm_existing_track(self):
        self.associate([detection(0)], 0)
        self.associate([detection(5, confidence=0.2)], 1)

        self.assertEqual(set(self.tracks), {1})
        self.assertEqual(set(self.telemetry), {1})

    def test_low_confidence_detection_cannot_revive_identity(self):
        self.associate([detection(0)], 0)
        self.associate([detection(5)], 1)
        self.associate([], 2)
        self.associate([], 3)
        self.associate([detection(500, confidence=0.2)], 4)

        self.assertEqual(self.tracks, {})
        self.assertEqual(self.telemetry[1]["last_seen_frame"], 1)

    def test_coasting_identity_can_reenter_outside_spatial_gate(self):
        self.associate([detection(0)], 0)
        self.associate([detection(5)], 1)
        self.associate([], 2)
        self.associate([], 3)
        self.associate([detection(400)], 10)

        self.assertEqual(set(self.tracks), {1})
        self.assertEqual(self.telemetry[1]["last_seen_frame"], 10)
        self.assertEqual(self.next_track_id, 2)

    def test_hungarian_matching_keeps_distinct_people_separate(self):
        self.associate(
            [
                detection(0, embedding=(1.0, 0.0)),
                detection(200, embedding=(0.0, 1.0)),
            ],
            0,
        )
        self.associate(
            [
                detection(205, embedding=(0.0, 1.0)),
                detection(5, embedding=(1.0, 0.0)),
            ],
            1,
        )

        self.assertEqual(set(self.tracks), {1, 2})
        identity_embeddings = {
            tuple(np.round(embedding, 6))
            for embedding in self.identities.values()
        }
        self.assertEqual(identity_embeddings, {(1.0, 0.0), (0.0, 1.0)})

    def test_expired_embedding_is_pruned_but_telemetry_is_retained(self):
        self.associate([detection(0)], 0)
        self.associate([detection(5)], 1)
        self.associate([], 2)
        self.associate([], 3)
        self.associate([], 20)

        self.assertEqual(self.identities, {})
        self.assertIn(1, self.telemetry)

    def test_appearance_drift_does_not_spawn_over_live_track(self):
        self.associate([detection(100)], 0)
        self.associate([detection(125)], 1)
        drifted_embedding = (0.6, 0.8)
        self.associate([detection(150, embedding=drifted_embedding)], 2)
        self.associate([detection(175, embedding=drifted_embedding)], 3)

        self.assertEqual(set(self.telemetry), {1})
        self.assertEqual(self.telemetry[1]["last_seen_frame"], 3)

    def test_live_identity_cannot_teleport_through_reentry_matching(self):
        self.associate([detection(0)], 0)
        self.associate([detection(5)], 1)
        self.associate([detection(500)], 2)
        self.associate([detection(505)], 3)

        self.assertEqual(self.telemetry[1]["last_seen_frame"], 1)
        self.assertEqual(set(self.telemetry), {1, 2})

    def test_coasting_track_survives_appearance_drift(self):
        self.associate([detection(100)], 0)
        self.associate([detection(125)], 1)
        self.associate([], 2)
        self.associate([detection(150, embedding=(0.6, 0.8))], 3)

        self.assertEqual(set(self.telemetry), {1})
        self.assertEqual(self.telemetry[1]["last_seen_frame"], 3)

    def test_tentative_track_can_confirm_through_secondary_match(self):
        self.associate([detection(100)], 0)
        self.associate([detection(120, embedding=(0.6, 0.8))], 1)

        self.assertEqual(set(self.tracks), {1})
        self.assertEqual(self.telemetry[1]["entry_frame"], 0)

    def test_tentative_track_requires_consecutive_detections(self):
        self.associate([detection(100)], 0)
        self.associate([], 1)
        self.associate([detection(100)], 2)

        self.assertEqual(set(self.tracks), {-2})
        self.assertEqual(self.telemetry, {})

    def test_orthogonal_appearance_is_not_absorbed_by_spatial_match(self):
        self.associate([detection(100)], 0)
        self.associate([detection(105)], 1)
        self.associate([detection(135, embedding=(0.0, 1.0))], 2)

        self.assertEqual(self.telemetry[1]["last_seen_frame"], 1)
        self.assertEqual(set(self.tracks), {1, -2})

    def test_reentry_rejects_implausible_short_gap_motion(self):
        self.associate([detection(0)], 0)
        self.associate([detection(5)], 1)
        self.associate([], 2)
        self.associate([], 3)
        self.associate([detection(900)], 4)

        self.assertEqual(set(self.tracks), {-2})
        self.assertEqual(self.telemetry[1]["last_seen_frame"], 1)
        self.assertEqual(set(self.telemetry), {1})

    def test_reentry_motion_uses_sampled_periods_not_source_frames(self):
        self.associate([detection(0)], 0, sample_interval=10)
        self.associate([detection(5)], 10, sample_interval=10)
        self.associate([], 20, sample_interval=10)
        self.associate([], 30, sample_interval=10)
        self.associate([detection(400)], 40, sample_interval=10)

        self.assertEqual(set(self.tracks), {-2})
        self.assertEqual(self.telemetry[1]["last_seen_frame"], 10)
        self.assertEqual(set(self.telemetry), {1})


class DetectionPreparationTests(unittest.TestCase):
    def test_nms_keeps_highest_scoring_overlap_and_disjoint_box(self):
        detections = [
            detection(0, confidence=0.8),
            detection(1, confidence=0.9),
            detection(100, confidence=0.7),
        ]

        kept = non_max_suppression(detections)

        self.assertEqual(
            [(item["bbox"], item["confidence"]) for item in kept],
            [
                ((1, 0, 21, 40), 0.9),
                ((100, 0, 120, 40), 0.7),
            ],
        )

    def test_far_low_confidence_box_is_not_embedded(self):
        result = {
            "boxes": torch.tensor(
                [[500, 0, 520, 40], [0, 0, 20, 40]],
                dtype=torch.float32,
            ),
            "labels": torch.tensor([0, 0]),
            "scores": torch.tensor([0.2, 0.9]),
        }
        embedder = RecordingEmbedder()
        active_track = {
            1: {
                "bbox": (0, 0, 20, 40),
                "centroid": (10, 20),
                "embedding": np.asarray([1.0, 0.0]),
                "age": 0,
                "first_seen_frame": 0,
                "hits": 2,
            }
        }

        prepared = extract_person_detections(
            result,
            np.zeros((100, 600, 3), dtype=np.uint8),
            np.zeros((100, 600, 3), dtype=np.uint8),
            embedder,
            person_class_id=0,
            track_gallery=active_track,
            activation_threshold=0.6,
        )

        self.assertEqual(len(prepared), 1)
        self.assertEqual(embedder.boxes.tolist(), [[0.0, 0.0, 20.0, 40.0]])


class FrameReaderTests(unittest.TestCase):
    def test_expected_eof_is_not_marked_early(self):
        state = FrameReadState()
        batches = list(
            iter_sampled_frame_batches(
                FakeCapture([0, 1, 2]),
                sample_interval=1,
                batch_size=2,
                expected_source_frames=3,
                read_state=state,
            )
        )

        self.assertEqual(batches, [[(0, 0), (1, 1)], [(2, 2)]])
        self.assertFalse(state.ended_early)

    def test_short_decode_is_marked_early(self):
        state = FrameReadState()
        list(
            iter_sampled_frame_batches(
                FakeCapture([0, 1, 2]),
                sample_interval=1,
                batch_size=2,
                expected_source_frames=5,
                read_state=state,
            )
        )

        self.assertTrue(state.ended_early)
        self.assertEqual(state.source_frames_read, 3)

    def test_skipped_frames_are_grabbed_without_retrieval(self):
        state = FrameReadState()
        batches = list(
            iter_sampled_frame_batches(
                FakeCapture([0, 1, 2, 3, 4]),
                sample_interval=2,
                batch_size=3,
                expected_source_frames=5,
                read_state=state,
            )
        )

        self.assertEqual(batches, [[(0, 0), (2, 2), (4, 4)]])


class LineTrackingTests(unittest.TestCase):
    def test_rtdetr_line_detections_include_coasting_tracks(self):
        tracks = {
            1: {
                "bbox": (0, 0, 20, 40),
                "centroid": (10, 20),
                "embedding": np.asarray([1.0, 0.0]),
                "age": 2,
                "first_seen_frame": 0,
                "hits": 2,
            }
        }

        detections = active_track_detections(tracks)

        self.assertEqual(detections.tracker_id.tolist(), [1])

    def test_botsort_line_cache_preserves_and_expires_stale_bbox(self):
        cache = {}
        current = sv.Detections(
            xyxy=np.asarray([[0, 0, 20, 40]], dtype=np.float32),
            tracker_id=np.asarray([7], dtype=np.int32),
        )
        coasting_track_detections(current, cache, 0, 2)

        coasted = coasting_track_detections(
            sv.Detections.empty(),
            cache,
            2,
            2,
        )
        expired = coasting_track_detections(
            sv.Detections.empty(),
            cache,
            3,
            2,
        )

        self.assertEqual(coasted.tracker_id.tolist(), [7])
        self.assertEqual(len(expired), 0)

    def test_rtdetr_line_crossing_survives_coasting_gap(self):
        line_zone = create_line_zone((50, 0, 50, 99), 100, 100)

        def trigger(x, age):
            tracks = {
                1: {
                    "bbox": (x, 20, x + 20, 80),
                    "centroid": (x + 10, 50),
                    "embedding": np.asarray([1.0, 0.0]),
                    "age": age,
                    "first_seen_frame": 0,
                    "hits": 2,
                }
            }
            line_zone.trigger(active_track_detections(tracks))

        trigger(10, 0)
        trigger(10, 0)
        trigger(10, 1)
        trigger(10, 2)
        trigger(70, 0)
        trigger(70, 0)

        self.assertEqual(line_zone.in_count + line_zone.out_count, 1)

    def test_botsort_line_crossing_survives_coasting_gap(self):
        line_zone = create_line_zone((50, 0, 50, 99), 100, 100)
        cache = {}

        def trigger(frame_index, x=None):
            if x is None:
                current = sv.Detections.empty()
            else:
                current = sv.Detections(
                    xyxy=np.asarray(
                        [[x, 20, x + 20, 80]],
                        dtype=np.float32,
                    ),
                    tracker_id=np.asarray([7], dtype=np.int32),
                )
            line_zone.trigger(
                coasting_track_detections(
                    current,
                    cache,
                    frame_index,
                    max_coast_source_frames=3,
                )
            )

        trigger(0, 10)
        trigger(1, 10)
        trigger(2)
        trigger(3)
        trigger(4, 70)
        trigger(5, 70)

        self.assertEqual(line_zone.in_count + line_zone.out_count, 1)


class ConfigurationTests(unittest.TestCase):
    def test_lost_track_buffer_covers_two_sample_periods(self):
        for effective_fps in (0.5, 0.9, 3.0):
            buffer_frames = lost_track_buffer_for_sample_rate(effective_fps)
            self.assertGreaterEqual(
                buffer_frames / 30,
                2 / effective_fps,
            )

    def test_shared_retention_produces_two_or_more_sampled_frames(self):
        for effective_fps in (0.5, 1.0, 3.0, 30.0):
            retained_sampled_frames = round(
                retention_seconds_for_sample_rate(effective_fps)
                * effective_fps
            )
            self.assertGreaterEqual(retained_sampled_frames, 2)

    def test_confirmed_entry_frame_backdates_confirmation(self):
        self.assertEqual(confirmed_entry_frame(20, 5), 15)
        self.assertEqual(confirmed_entry_frame(3, 5), 0)

    def test_line_zone_rejects_invalid_coordinates(self):
        with self.assertRaises(ValueError):
            create_line_zone((1, 1, 1, 1), 100, 100)
        with self.assertRaises(ValueError):
            create_line_zone((0, 0, 100, 50), 100, 100)


class OutputTests(unittest.TestCase):
    def test_video_timestamp_rounds_to_milliseconds(self):
        self.assertEqual(format_video_timestamp(1, 3), "00:00:00.333")
        self.assertEqual(
            format_video_timestamp(10_800, 3),
            "01:00:00.000",
        )

    def test_telemetry_csv_schema_and_values(self):
        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "telemetry.csv"
            write_telemetry(
                output_path,
                {
                    2: {"entry_frame": 3, "last_seen_frame": 9},
                    1: {"entry_frame": 0, "last_seen_frame": 6},
                },
                fps=3,
            )

            with output_path.open(newline="", encoding="utf-8") as output:
                rows = list(csv.DictReader(output))

        self.assertEqual([row["person_id"] for row in rows], ["1", "2"])
        self.assertEqual(rows[0]["entry_timestamp"], "00:00:00.000")
        self.assertEqual(rows[0]["exit_timestamp"], "00:00:02.000")
        self.assertEqual(rows[0]["duration_seconds"], "2.000")
        self.assertEqual(rows[1]["entry_seconds"], "1.000")

    def test_line_counts_csv_schema_and_values(self):
        records = [
            {
                "frame": 3,
                "video_seconds": "1.000",
                "video_timestamp": "00:00:01.000",
                "frame_in_count": 1,
                "frame_out_count": 0,
                "cumulative_in_count": 2,
                "cumulative_out_count": 1,
                "line_start_x": 0,
                "line_start_y": 50,
                "line_end_x": 99,
                "line_end_y": 50,
            }
        ]
        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "line_counts.csv"
            write_line_counts(output_path, records)
            with output_path.open(newline="", encoding="utf-8") as output:
                rows = list(csv.DictReader(output))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["frame_in_count"], "1")
        self.assertEqual(rows[0]["cumulative_in_count"], "2")
        self.assertEqual(rows[0]["line_end_x"], "99")


if __name__ == "__main__":
    unittest.main()
