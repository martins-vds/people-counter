import unittest

import numpy as np
import supervision as sv

from people_counter.line_counting import (
    active_track_detections,
    coasting_track_detections,
    create_line_zone,
    record_line_counts,
)
from people_counter.models import LineCountRecord, TrackProfile


def track_profile(x: int, age: int) -> TrackProfile:
    return TrackProfile(
        bbox=(x, 20, x + 20, 80),
        centroid=(x + 10, 50),
        embedding=np.asarray([1.0, 0.0]),
        age=age,
        first_seen_frame=0,
        hits=2,
    )


class LineTrackingTests(unittest.TestCase):
    def test_record_line_counts_records_frame_and_cumulative_totals(self):
        line_zone = unittest.mock.MagicMock(in_count=5, out_count=3)
        line_zone.trigger.return_value = (
            np.asarray([True, False, True]),
            np.asarray([False, True, False]),
        )
        detections = sv.Detections.empty()
        records: list[LineCountRecord] = []

        record_line_counts(
            line_zone,
            detections,
            source_frame_index=45,
            fps=30.0,
            line_coordinates=(10, 20, 90, 70),
            line_count_records=records,
        )

        line_zone.trigger.assert_called_once_with(detections)
        self.assertEqual(
            records,
            [
                LineCountRecord(
                    frame=45,
                    video_seconds="1.500",
                    video_timestamp="00:00:01.500",
                    frame_in_count=2,
                    frame_out_count=1,
                    cumulative_in_count=5,
                    cumulative_out_count=3,
                    line_start_x=10,
                    line_start_y=20,
                    line_end_x=90,
                    line_end_y=70,
                )
            ],
        )

    def test_record_line_counts_requires_zone_and_coordinates(self):
        line_zone = unittest.mock.MagicMock()
        records: list[LineCountRecord] = []
        detections = sv.Detections.empty()

        record_line_counts(
            None,
            detections,
            0,
            30.0,
            (0, 0, 1, 1),
            records,
        )
        record_line_counts(
            line_zone,
            detections,
            0,
            30.0,
            None,
            records,
        )

        line_zone.trigger.assert_not_called()
        self.assertEqual(records, [])

    def test_rtdetr_line_detections_include_coasting_tracks(self):
        detections = active_track_detections(
            {
                -1: track_profile(0, 2),
                0: track_profile(20, 2),
                2: track_profile(40, 2),
            }
        )

        self.assertEqual(detections.tracker_id.tolist(), [2])
        self.assertEqual(detections.tracker_id.dtype, np.int32)
        self.assertEqual(detections.class_id.tolist(), [0])
        self.assertEqual(detections.class_id.dtype, np.int32)
        self.assertEqual(detections.xyxy.dtype, np.float32)

    def test_line_zone_accepts_frame_edges_and_preserves_direction(self):
        line_zone = create_line_zone((0, 0, 99, 99), 100, 100)

        self.assertEqual((line_zone.vector.start.x, line_zone.vector.start.y), (0, 0))
        self.assertEqual((line_zone.vector.end.x, line_zone.vector.end.y), (99, 99))
        self.assertEqual(
            line_zone.triggering_anchors,
            [sv.Position.BOTTOM_CENTER],
        )

    def test_line_zone_errors_are_exact_at_invalid_boundaries(self):
        with self.assertRaisesRegex(
            ValueError,
            "^Counting line start and end coordinates must differ$",
        ):
            create_line_zone((1, 1, 1, 1), 100, 100)
        with self.assertRaisesRegex(
            ValueError,
            (
                "^Counting line point \\(50, 100\\) is outside "
                "the 100x100 video frame$"
            ),
        ):
            create_line_zone((0, 0, 50, 100), 100, 100)

    def test_botsort_line_cache_preserves_and_expires_stale_bbox(self):
        cache = {}
        current = sv.Detections(
            xyxy=np.asarray([[0, 0, 20, 40]], dtype=np.float64),
            tracker_id=np.asarray([7], dtype=np.int64),
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
        self.assertEqual(coasted.tracker_id.dtype, np.int32)
        self.assertEqual(coasted.class_id.tolist(), [0])
        self.assertEqual(coasted.class_id.dtype, np.int32)
        self.assertEqual(coasted.xyxy.dtype, np.float32)
        self.assertEqual(len(expired), 0)

    def test_rtdetr_line_crossing_survives_coasting_gap(self):
        line_zone = create_line_zone((50, 0, 50, 99), 100, 100)

        def trigger(x, age):
            line_zone.trigger(
                active_track_detections({1: track_profile(x, age)})
            )

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
