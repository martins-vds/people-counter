import unittest

import numpy as np
import torch

from people_counter.models import TrackProfile
from people_counter.pipelines.rtdetr_osnet import (
    RTDetrTrackingState,
    associate_detections,
    extract_person_detections,
    non_max_suppression,
)
from tests.helpers import RecordingEmbedder, detection


class AssociationTests(unittest.TestCase):
    def setUp(self):
        self.state = RTDetrTrackingState()

    def associate(self, detections, frame_index, sample_interval=1):
        associate_detections(
            detections,
            self.state,
            frame_index,
            max_centroid_displacement=50,
            max_disappeared_frames=1,
            max_reentry_frames=10,
            sample_interval=sample_interval,
            activation_threshold=0.6,
        )

    def test_single_frame_detection_is_not_counted(self):
        self.associate([detection(0)], 0)

        self.assertEqual(set(self.state.tracks), {-1})
        self.assertEqual(self.state.identities, {})
        self.assertEqual(self.state.telemetry, {})

        self.associate([], 1)
        self.associate([], 2)

        self.assertEqual(self.state.tracks, {})
        self.assertEqual(self.state.telemetry, {})

    def test_second_matching_frame_confirms_identity(self):
        self.associate([detection(0)], 0)
        self.associate([detection(5)], 1)

        self.assertEqual(set(self.state.tracks), {1})
        self.assertEqual(set(self.state.identities), {1})
        self.assertEqual(self.state.telemetry[1].entry_frame, 0)
        self.assertEqual(self.state.telemetry[1].last_seen_frame, 1)

    def test_low_confidence_detection_cannot_start_identity(self):
        self.associate([detection(0, confidence=0.2)], 0)

        self.assertEqual(self.state.tracks, {})
        self.assertEqual(self.state.telemetry, {})

    def test_low_confidence_detection_can_confirm_existing_track(self):
        self.associate([detection(0)], 0)
        self.associate([detection(5, confidence=0.2)], 1)

        self.assertEqual(set(self.state.tracks), {1})
        self.assertEqual(set(self.state.telemetry), {1})

    def test_low_confidence_detection_cannot_revive_identity(self):
        self.associate([detection(0)], 0)
        self.associate([detection(5)], 1)
        self.associate([], 2)
        self.associate([], 3)
        self.associate([detection(500, confidence=0.2)], 4)

        self.assertEqual(self.state.tracks, {})
        self.assertEqual(self.state.telemetry[1].last_seen_frame, 1)

    def test_coasting_identity_can_reenter_outside_spatial_gate(self):
        self.associate([detection(0)], 0)
        self.associate([detection(5)], 1)
        self.associate([], 2)
        self.associate([], 3)
        self.associate([detection(400)], 10)

        self.assertEqual(set(self.state.tracks), {1})
        self.assertEqual(self.state.telemetry[1].last_seen_frame, 10)
        self.assertEqual(self.state.next_track_id, 2)

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

        self.assertEqual(set(self.state.tracks), {1, 2})
        identity_embeddings = {
            tuple(np.round(embedding, 6))
            for embedding in self.state.identities.values()
        }
        self.assertEqual(identity_embeddings, {(1.0, 0.0), (0.0, 1.0)})

    def test_expired_embedding_is_pruned_but_telemetry_is_retained(self):
        self.associate([detection(0)], 0)
        self.associate([detection(5)], 1)
        self.associate([], 2)
        self.associate([], 3)
        self.associate([], 20)

        self.assertEqual(self.state.identities, {})
        self.assertIn(1, self.state.telemetry)

    def test_appearance_drift_does_not_spawn_over_live_track(self):
        self.associate([detection(100)], 0)
        self.associate([detection(125)], 1)
        drifted_embedding = (0.6, 0.8)
        self.associate([detection(150, embedding=drifted_embedding)], 2)
        self.associate([detection(175, embedding=drifted_embedding)], 3)

        self.assertEqual(set(self.state.telemetry), {1})
        self.assertEqual(self.state.telemetry[1].last_seen_frame, 3)

    def test_live_identity_cannot_teleport_through_reentry_matching(self):
        self.associate([detection(0)], 0)
        self.associate([detection(5)], 1)
        self.associate([detection(500)], 2)
        self.associate([detection(505)], 3)

        self.assertEqual(self.state.telemetry[1].last_seen_frame, 1)
        self.assertEqual(set(self.state.telemetry), {1, 2})

    def test_coasting_track_survives_appearance_drift(self):
        self.associate([detection(100)], 0)
        self.associate([detection(125)], 1)
        self.associate([], 2)
        self.associate([detection(150, embedding=(0.6, 0.8))], 3)

        self.assertEqual(set(self.state.telemetry), {1})
        self.assertEqual(self.state.telemetry[1].last_seen_frame, 3)

    def test_tentative_track_can_confirm_through_secondary_match(self):
        self.associate([detection(100)], 0)
        self.associate([detection(120, embedding=(0.6, 0.8))], 1)

        self.assertEqual(set(self.state.tracks), {1})
        self.assertEqual(self.state.telemetry[1].entry_frame, 0)

    def test_tentative_track_requires_consecutive_detections(self):
        self.associate([detection(100)], 0)
        self.associate([], 1)
        self.associate([detection(100)], 2)

        self.assertEqual(set(self.state.tracks), {-2})
        self.assertEqual(self.state.telemetry, {})

    def test_orthogonal_appearance_is_not_absorbed_by_spatial_match(self):
        self.associate([detection(100)], 0)
        self.associate([detection(105)], 1)
        self.associate([detection(135, embedding=(0.0, 1.0))], 2)

        self.assertEqual(self.state.telemetry[1].last_seen_frame, 1)
        self.assertEqual(set(self.state.tracks), {1, -2})

    def test_reentry_rejects_implausible_short_gap_motion(self):
        self.associate([detection(0)], 0)
        self.associate([detection(5)], 1)
        self.associate([], 2)
        self.associate([], 3)
        self.associate([detection(900)], 4)

        self.assertEqual(set(self.state.tracks), {-2})
        self.assertEqual(self.state.telemetry[1].last_seen_frame, 1)
        self.assertEqual(set(self.state.telemetry), {1})

    def test_reentry_motion_uses_sampled_periods_not_source_frames(self):
        self.associate([detection(0)], 0, sample_interval=10)
        self.associate([detection(5)], 10, sample_interval=10)
        self.associate([], 20, sample_interval=10)
        self.associate([], 30, sample_interval=10)
        self.associate([detection(400)], 40, sample_interval=10)

        self.assertEqual(set(self.state.tracks), {-2})
        self.assertEqual(self.state.telemetry[1].last_seen_frame, 10)
        self.assertEqual(set(self.state.telemetry), {1})


class DetectionPreparationTests(unittest.TestCase):
    def test_nms_keeps_highest_scoring_overlap_and_disjoint_box(self):
        detections = [
            detection(0, confidence=0.8),
            detection(1, confidence=0.9),
            detection(100, confidence=0.7),
        ]

        kept = non_max_suppression(detections)

        self.assertEqual(
            [(item.bbox, item.confidence) for item in kept],
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
            1: TrackProfile(
                bbox=(0, 0, 20, 40),
                centroid=(10, 20),
                embedding=np.asarray([1.0, 0.0]),
                age=0,
                first_seen_frame=0,
                hits=2,
            )
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
