import unittest

import numpy as np

from people_counter.models import (
    Detection,
    LastGeometry,
    PersonTelemetry,
    TrackProfile,
)
from people_counter.pipelines.rtdetr_osnet import (
    AssociationState,
    RTDetrTrackingState,
    _reentry_spatially_valid,
    centroid_distance_limit,
    detection_embedding,
    is_near_active_track,
    non_max_suppression,
    update_embedding,
    update_person_telemetry,
)


def box_detection(
    bbox=(100, 20, 140, 80),
    confidence=0.9,
    embedding=(1.0, 0.0),
):
    x1, y1, x2, y2 = bbox
    return Detection(
        bbox=bbox,
        centroid=((x1 + x2) // 2, (y1 + y2) // 2),
        confidence=confidence,
        embedding=np.asarray(embedding, dtype=np.float32),
    )


def track_profile(
    bbox=(90, 10, 130, 50),
    age=0,
    embedding=(1.0, 0.0),
):
    x1, y1, x2, y2 = bbox
    return TrackProfile(
        bbox=bbox,
        centroid=((x1 + x2) // 2, (y1 + y2) // 2),
        embedding=np.asarray(embedding, dtype=np.float32),
        age=age,
        first_seen_frame=3,
        hits=2,
    )


def association_state(
    detection,
    tracking=None,
    source_frame_index=20,
    max_centroid_displacement=10.0,
    max_disappeared_frames=2,
    max_reentry_frames=100,
    sample_interval=5,
):
    return AssociationState(
        detections=[detection],
        tracking=tracking or RTDetrTrackingState(),
        source_frame_index=source_frame_index,
        max_centroid_displacement=max_centroid_displacement,
        max_disappeared_frames=max_disappeared_frames,
        max_reentry_frames=max_reentry_frames,
        sample_interval=sample_interval,
        activation_threshold=0.6,
    )


class EmbeddingTests(unittest.TestCase):
    def test_detection_embedding_requires_an_embedding(self):
        embedded = box_detection(embedding=(0.25, 0.75))
        self.assertIs(detection_embedding(embedded), embedded.embedding)

        missing = Detection(
            bbox=(0, 0, 10, 10),
            centroid=(5, 5),
            confidence=0.9,
        )
        with self.assertRaisesRegex(
            RuntimeError,
            "^Detection is missing its identity embedding$",
        ):
            detection_embedding(missing)

    def test_update_embedding_applies_normalized_ema(self):
        updated = update_embedding(
            np.asarray([1.0, 0.0]),
            np.asarray([0.0, 1.0]),
        )
        expected = np.asarray([0.95, 0.05])
        expected /= np.linalg.norm(expected)

        np.testing.assert_allclose(updated, expected)
        self.assertAlmostEqual(float(np.linalg.norm(updated)), 1.0)

    def test_update_embedding_rejects_zero_norm(self):
        with self.assertRaisesRegex(
            RuntimeError,
            "^OSNet produced a zero-norm identity embedding$",
        ):
            update_embedding(np.zeros(2), np.zeros(2))


class GeometryTests(unittest.TestCase):
    def test_centroid_limit_uses_bbox_floor_and_capped_age(self):
        detection = box_detection()
        profile = track_profile(age=2)
        self.assertEqual(
            centroid_distance_limit(
                detection,
                profile,
                max_centroid_displacement=5.0,
                max_disappeared_frames=2,
            ),
            30.0,
        )

        old_profile = track_profile(age=10)
        self.assertEqual(
            centroid_distance_limit(
                detection,
                old_profile,
                max_centroid_displacement=20.0,
                max_disappeared_frames=2,
            ),
            60.0,
        )

    def test_near_active_track_uses_strict_bbox_height_radius(self):
        profile = track_profile(bbox=(100, 20, 140, 80))
        self.assertTrue(
            is_near_active_track(
                box_detection(bbox=(159, 20, 199, 80)),
                {1: profile},
            )
        )
        self.assertFalse(
            is_near_active_track(
                box_detection(bbox=(160, 20, 200, 80)),
                {1: profile},
            )
        )
        self.assertFalse(
            is_near_active_track(
                box_detection(bbox=(300, 100, 340, 160)),
                {1: profile},
            )
        )
        self.assertFalse(is_near_active_track(box_detection(), {}))

    def test_reentry_gate_uses_elapsed_sample_periods_and_strict_limit(self):
        detection = box_detection(bbox=(139, 20, 179, 80))
        telemetry = PersonTelemetry(
            entry_frame=0,
            last_seen_frame=10,
            last_geometry=LastGeometry(
                centroid=(100, 50),
                bbox=(80, 20, 120, 80),
            ),
        )
        state = association_state(
            detection,
            source_frame_index=20,
            max_centroid_displacement=30.0,
            sample_interval=5,
        )

        self.assertTrue(_reentry_spatially_valid(state, detection, telemetry))

        boundary = box_detection(bbox=(140, 20, 180, 80))
        self.assertFalse(
            _reentry_spatially_valid(state, boundary, telemetry)
        )

    def test_reentry_gate_requires_last_geometry(self):
        detection = box_detection()
        telemetry = PersonTelemetry(
            entry_frame=0,
            last_seen_frame=10,
        )
        with self.assertRaisesRegex(
            RuntimeError,
            "^Identity is missing its last geometry$",
        ):
            _reentry_spatially_valid(
                association_state(detection),
                detection,
                telemetry,
            )

    def test_update_person_telemetry_replaces_geometry_and_frame(self):
        telemetry = PersonTelemetry(entry_frame=1, last_seen_frame=2)
        detection = box_detection()

        update_person_telemetry(telemetry, detection, 25)

        self.assertEqual(telemetry.entry_frame, 1)
        self.assertEqual(telemetry.last_seen_frame, 25)
        self.assertEqual(
            telemetry.last_geometry,
            LastGeometry(
                centroid=detection.centroid,
                bbox=detection.bbox,
            ),
        )


class SuppressionTests(unittest.TestCase):
    def test_nms_handles_nonzero_origins_and_multiple_candidates(self):
        detections = [
            box_detection((10, 20, 50, 80), confidence=0.9),
            box_detection((11, 21, 51, 81), confidence=0.8),
            box_detection((60, 25, 90, 75), confidence=0.7),
            box_detection((100, 30, 120, 60), confidence=0.6),
        ]

        kept = non_max_suppression(detections)

        self.assertEqual(
            [(item.bbox, item.confidence) for item in kept],
            [
                ((10, 20, 50, 80), 0.9),
                ((60, 25, 90, 75), 0.7),
                ((100, 30, 120, 60), 0.6),
            ],
        )

    def test_nms_handles_empty_and_single_detection(self):
        detection = box_detection()

        self.assertEqual(non_max_suppression([]), [])
        self.assertEqual(non_max_suppression([detection]), [detection])


if __name__ == "__main__":
    unittest.main()
