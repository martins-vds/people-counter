import unittest

import numpy as np
import supervision as sv

from people_counter.line_counting import (
    active_track_detections,
    coasting_track_detections,
    create_line_zone,
)
from people_counter.models import TrackProfile


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
    def test_rtdetr_line_detections_include_coasting_tracks(self):
        detections = active_track_detections({1: track_profile(0, 2)})

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
