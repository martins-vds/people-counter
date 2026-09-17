import unittest
from unittest.mock import patch

import numpy as np

from people_counter.models import (
    Detection,
    LastGeometry,
    PersonTelemetry,
    TrackProfile,
)
from people_counter.pipelines.rtdetr_osnet import (
    ACTIVE_MATCH_THRESHOLD,
    INVALID_ASSIGNMENT_COST,
    MIN_CONFIRMATION_FRAMES,
    REENTRY_MATCH_THRESHOLD,
    SECONDARY_MATCH_THRESHOLD,
    AssociationState,
    RTDetrTrackingState,
    _active_spatially_valid,
    _finite_assignments,
    _overlaps_unmatched_track,
    _record_reentry_match,
    _record_track_match,
    _reentry_identity_ids,
    _reentry_spatially_valid,
    _secondary_pair_cost,
    _unmatched_activated_detections,
    age_unmatched_tracks,
    assign_active_appearance_matches,
    assign_reentry_matches,
    assign_spatial_secondary_matches,
    centroid_distance_limit,
    detection_embedding,
    expire_inactive_identities,
    is_near_active_track,
    non_max_suppression,
    spawn_tentative_tracks,
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
    first_seen_frame=3,
    hits=2,
):
    x1, y1, x2, y2 = bbox
    return TrackProfile(
        bbox=bbox,
        centroid=((x1 + x2) // 2, (y1 + y2) // 2),
        embedding=np.asarray(embedding, dtype=np.float32),
        age=age,
        first_seen_frame=first_seen_frame,
        hits=hits,
    )


def association_state(
    detections,
    tracking=None,
    source_frame_index=20,
    max_centroid_displacement=10.0,
    max_disappeared_frames=2,
    max_reentry_frames=100,
    sample_interval=5,
):
    if isinstance(detections, Detection):
        detections = [detections]
    return AssociationState(
        detections=list(detections),
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

    def test_centroid_limit_uses_live_age_and_taller_profile_geometry(self):
        self.assertEqual(
            centroid_distance_limit(
                box_detection(bbox=(100, 20, 120, 50)),
                track_profile(bbox=(90, 10, 130, 90), age=1),
                max_centroid_displacement=7.0,
                max_disappeared_frames=4,
            ),
            40.0,
        )
        self.assertEqual(
            centroid_distance_limit(
                box_detection(bbox=(100, 20, 120, 30)),
                track_profile(bbox=(90, 10, 110, 20), age=1),
                max_centroid_displacement=7.0,
                max_disappeared_frames=4,
            ),
            14.0,
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

    def test_near_active_track_uses_larger_height_from_detection_or_track(self):
        self.assertTrue(
            is_near_active_track(
                box_detection(bbox=(145, 20, 165, 70)),
                {1: track_profile(bbox=(100, 30, 120, 50))},
            )
        )
        self.assertTrue(
            is_near_active_track(
                box_detection(bbox=(149, 20, 169, 30)),
                {1: track_profile(bbox=(100, 10, 120, 90))},
            )
        )

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

    def test_reentry_gate_clamps_short_gaps_and_uses_bbox_floor_from_both_geometries(
        self,
    ):
        short_gap_state = association_state(
            box_detection(bbox=(110, 20, 120, 30)),
            source_frame_index=11,
            max_centroid_displacement=10.0,
            sample_interval=10,
        )
        short_gap_telemetry = PersonTelemetry(
            entry_frame=0,
            last_seen_frame=10,
            last_geometry=LastGeometry(
                centroid=(100, 25),
                bbox=(90, 20, 100, 30),
            ),
        )
        self.assertFalse(
            _reentry_spatially_valid(
                short_gap_state,
                short_gap_state.detections[0],
                short_gap_telemetry,
            )
        )

        detection_tall = box_detection(bbox=(110, 20, 130, 80))
        self.assertTrue(
            _reentry_spatially_valid(
                association_state(
                    detection_tall,
                    source_frame_index=20,
                    max_centroid_displacement=1.0,
                    sample_interval=5,
                ),
                detection_tall,
                PersonTelemetry(
                    entry_frame=0,
                    last_seen_frame=10,
                    last_geometry=LastGeometry(
                        centroid=(91, 50),
                        bbox=(80, 20, 100, 30),
                    ),
                ),
            )
        )

        detection_short = box_detection(bbox=(110, 45, 130, 55))
        self.assertTrue(
            _reentry_spatially_valid(
                association_state(
                    detection_short,
                    source_frame_index=20,
                    max_centroid_displacement=1.0,
                    sample_interval=5,
                ),
                detection_short,
                PersonTelemetry(
                    entry_frame=0,
                    last_seen_frame=10,
                    last_geometry=LastGeometry(
                        centroid=(91, 50),
                        bbox=(80, 20, 102, 80),
                    ),
                ),
            )
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


class AssociationPrimitiveTests(unittest.TestCase):
    def test_finite_assignments_handles_non_bijective_candidate_graph(self):
        costs = np.asarray(
            [
                [0.1, np.inf, np.inf],
                [0.2, np.inf, np.inf],
                [np.inf, 0.3, 0.4],
            ]
        )

        assignments = _finite_assignments(costs)

        self.assertEqual(len(assignments), 2)
        assigned_rows = {row for row, _ in assignments}
        self.assertEqual(len(assigned_rows & {0, 1}), 1)
        self.assertIn(2, assigned_rows)
        self.assertTrue(
            all(np.isfinite(costs[row, column]) for row, column in assignments)
        )
        self.assertEqual(
            len({column for _, column in assignments}),
            len(assignments),
        )

    def test_finite_assignments_filters_distinct_row_and_column_indices(self):
        costs = np.full((4, 5), np.inf)
        costs[0, 1] = 0.1
        costs[2, 4] = 0.2

        def solve(solver_costs):
            np.testing.assert_allclose(
                solver_costs,
                np.asarray(
                    [
                        [0.1, INVALID_ASSIGNMENT_COST],
                        [INVALID_ASSIGNMENT_COST, 0.2],
                    ]
                ),
            )
            return np.asarray([0, 1]), np.asarray([0, 1])

        with patch(
            "people_counter.pipelines.rtdetr_osnet.linear_sum_assignment",
            side_effect=solve,
        ):
            assignments = _finite_assignments(costs)

        self.assertEqual(assignments, [(0, 1), (2, 4)])

    def test_expire_inactive_identities_keeps_active_confirmed_ids_and_uses_strict_gap(
        self,
    ):
        tracking = RTDetrTrackingState(
            tracks={
                1: track_profile(),
                -1: track_profile(
                    bbox=(150, 10, 170, 30),
                    hits=1,
                ),
            },
            identities={
                1: np.asarray([1.0, 0.0], dtype=np.float32),
                2: np.asarray([0.0, 1.0], dtype=np.float32),
            },
            telemetry={
                1: PersonTelemetry(entry_frame=0, last_seen_frame=0),
                2: PersonTelemetry(entry_frame=1, last_seen_frame=5),
            },
        )

        expire_inactive_identities(
            association_state(
                [],
                tracking=tracking,
                source_frame_index=15,
                max_reentry_frames=10,
            )
        )
        self.assertEqual(set(tracking.identities), {1, 2})

        expire_inactive_identities(
            association_state(
                [],
                tracking=tracking,
                source_frame_index=16,
                max_reentry_frames=10,
            )
        )
        self.assertEqual(set(tracking.identities), {1})

    def test_expire_inactive_identities_prunes_corrupted_track_zero_identity(
        self,
    ):
        tracking = RTDetrTrackingState(
            tracks={0: track_profile()},
            identities={0: np.asarray([1.0, 0.0], dtype=np.float32)},
            telemetry={0: PersonTelemetry(entry_frame=0, last_seen_frame=5)},
        )

        expire_inactive_identities(
            association_state(
                [],
                tracking=tracking,
                source_frame_index=16,
                max_reentry_frames=10,
            )
        )

        self.assertEqual(tracking.identities, {})

    def test_active_spatial_gate_uses_coordinate_deltas_and_strict_boundary(self):
        profile = track_profile(bbox=(90, 5, 110, 15))
        state = association_state(
            [],
            max_centroid_displacement=10.0,
            max_disappeared_frames=2,
        )

        self.assertTrue(
            _active_spatially_valid(
                state,
                box_detection(bbox=(96, 9, 116, 19)),
                profile,
            )
        )
        self.assertFalse(
            _active_spatially_valid(
                state,
                box_detection(bbox=(96, 13, 116, 23)),
                profile,
            )
        )

    def test_record_track_match_confirms_tentative_track_exactly_once(self):
        embedding = np.asarray([0.2, 0.8], dtype=np.float32)
        detection = box_detection(
            bbox=(105, 20, 145, 80),
            embedding=(0.2, 0.8),
        )
        tracking = RTDetrTrackingState(
            tracks={
                -1: track_profile(
                    first_seen_frame=7,
                    hits=1,
                )
            }
        )
        state = association_state(
            detection,
            tracking=tracking,
            source_frame_index=11,
        )

        _record_track_match(
            state,
            detection_index=0,
            original_track_id=-1,
            embedding=embedding,
            update_identity=False,
        )

        self.assertEqual(set(state.updated_tracks), {1})
        self.assertEqual(state.updated_tracks[1].hits, MIN_CONFIRMATION_FRAMES)
        self.assertEqual(state.updated_tracks[1].first_seen_frame, 7)
        np.testing.assert_array_equal(state.updated_tracks[1].embedding, embedding)
        np.testing.assert_array_equal(tracking.identities[1], embedding)
        self.assertEqual(
            tracking.telemetry[1],
            PersonTelemetry(
                entry_frame=7,
                last_seen_frame=11,
                last_geometry=LastGeometry(
                    centroid=detection.centroid,
                    bbox=detection.bbox,
                ),
            ),
        )
        self.assertEqual(state.matched_detection_indices, {0})
        self.assertEqual(state.matched_track_ids, {-1})

    def test_record_track_match_updates_confirmed_identity_and_telemetry(self):
        new_embedding = np.asarray([0.3, 0.7], dtype=np.float32)
        detection = box_detection(
            bbox=(110, 20, 150, 80),
            embedding=(0.3, 0.7),
        )
        tracking = RTDetrTrackingState(
            tracks={4: track_profile(first_seen_frame=3, hits=2)},
            identities={4: np.asarray([1.0, 0.0], dtype=np.float32)},
            telemetry={
                4: PersonTelemetry(
                    entry_frame=3,
                    last_seen_frame=6,
                    last_geometry=LastGeometry(
                        centroid=(110, 30),
                        bbox=(90, 10, 130, 50),
                    ),
                )
            },
        )
        state = association_state(
            detection,
            tracking=tracking,
            source_frame_index=12,
        )

        _record_track_match(
            state,
            detection_index=0,
            original_track_id=4,
            embedding=new_embedding,
            update_identity=True,
        )

        self.assertEqual(state.updated_tracks[4].hits, 3)
        self.assertEqual(state.updated_tracks[4].first_seen_frame, 3)
        np.testing.assert_array_equal(state.updated_tracks[4].embedding, new_embedding)
        np.testing.assert_array_equal(tracking.identities[4], new_embedding)
        self.assertEqual(
            tracking.telemetry[4],
            PersonTelemetry(
                entry_frame=3,
                last_seen_frame=12,
                last_geometry=LastGeometry(
                    centroid=detection.centroid,
                    bbox=detection.bbox,
                ),
            ),
        )
        self.assertEqual(state.matched_detection_indices, {0})
        self.assertEqual(state.matched_track_ids, {4})

    def test_record_track_match_keeps_corrupted_track_zero_out_of_confirmed_flow(
        self,
    ):
        embedding = np.asarray([0.4, 0.6], dtype=np.float32)
        detection = box_detection(
            bbox=(110, 20, 150, 80),
            embedding=(0.4, 0.6),
        )
        tracking = RTDetrTrackingState(
            tracks={0: track_profile(first_seen_frame=3, hits=1)}
        )
        state = association_state(
            detection,
            tracking=tracking,
            source_frame_index=12,
        )

        _record_track_match(
            state,
            detection_index=0,
            original_track_id=0,
            embedding=embedding,
            update_identity=False,
        )

        self.assertEqual(set(state.updated_tracks), {0})
        self.assertEqual(state.updated_tracks[0].hits, MIN_CONFIRMATION_FRAMES)
        self.assertEqual(state.updated_tracks[0].first_seen_frame, 3)
        np.testing.assert_array_equal(state.updated_tracks[0].embedding, embedding)
        self.assertEqual(tracking.next_track_id, 1)
        self.assertEqual(tracking.identities, {})
        self.assertEqual(tracking.telemetry, {})
        self.assertEqual(state.matched_detection_indices, {0})
        self.assertEqual(state.matched_track_ids, {0})

    def test_unmatched_activated_detections_include_threshold_hits(self):
        state = association_state(
            [
                box_detection(confidence=0.6),
                box_detection(bbox=(150, 20, 190, 80), confidence=0.59),
            ]
        )

        self.assertEqual(_unmatched_activated_detections(state), [0])

    def test_secondary_pair_cost_uses_strict_distance_and_threshold_similarity(
        self,
    ):
        profile = track_profile(bbox=(90, 5, 110, 15))
        state = association_state(
            [],
            max_centroid_displacement=10.0,
            max_disappeared_frames=2,
        )

        self.assertEqual(
            _secondary_pair_cost(
                state,
                box_detection(
                    bbox=(93, 9, 113, 19),
                    embedding=(SECONDARY_MATCH_THRESHOLD, 0.0),
                ),
                profile,
            ),
            0.5,
        )
        self.assertIsNone(
            _secondary_pair_cost(
                state,
                box_detection(
                    bbox=(96, 13, 116, 23),
                    embedding=(1.0, 0.0),
                ),
                profile,
            )
        )

    def test_assign_active_appearance_matches_prefers_high_similarity_pairs(
        self,
    ):
        tracking = RTDetrTrackingState(
            tracks={
                1: track_profile(bbox=(90, 10, 130, 50), embedding=(1.0, 0.0)),
                2: track_profile(
                    bbox=(100, 20, 140, 60),
                    embedding=(0.0, 1.0),
                ),
            },
            identities={
                1: np.asarray([1.0, 0.0], dtype=np.float32),
                2: np.asarray([0.0, 1.0], dtype=np.float32),
            },
            telemetry={
                1: PersonTelemetry(entry_frame=0, last_seen_frame=0),
                2: PersonTelemetry(entry_frame=1, last_seen_frame=1),
            },
        )
        detections = [
            box_detection(
                bbox=(92, 12, 132, 52),
                embedding=(0.2, 0.8),
            ),
            box_detection(
                bbox=(102, 22, 142, 62),
                embedding=(0.9, 0.1),
            ),
        ]
        state = association_state(
            detections,
            tracking=tracking,
            source_frame_index=10,
            max_centroid_displacement=500.0,
        )

        assign_active_appearance_matches(state)

        self.assertEqual(set(state.updated_tracks), {1, 2})
        self.assertEqual(state.updated_tracks[1].centroid, detections[1].centroid)
        self.assertEqual(state.updated_tracks[2].centroid, detections[0].centroid)
        self.assertEqual(state.matched_detection_indices, {0, 1})
        self.assertEqual(state.matched_track_ids, {1, 2})

    def test_assign_active_appearance_matches_updates_threshold_identities_and_skips_invalid_pairs(
        self,
    ):
        original_identity_1 = np.asarray([1.0, 0.0], dtype=np.float32)
        original_identity_2 = np.asarray([0.0, 1.0], dtype=np.float32)
        tracking = RTDetrTrackingState(
            tracks={
                1: track_profile(bbox=(90, 10, 130, 50), embedding=(1.0, 0.0)),
                2: track_profile(
                    bbox=(290, 10, 330, 50),
                    embedding=(0.0, 1.0),
                ),
            },
            identities={
                1: original_identity_1.copy(),
                2: original_identity_2.copy(),
            },
            telemetry={
                1: PersonTelemetry(entry_frame=0, last_seen_frame=0),
                2: PersonTelemetry(entry_frame=1, last_seen_frame=1),
            },
        )
        detections = [
            box_detection(
                bbox=(95, 10, 135, 50),
                embedding=(ACTIVE_MATCH_THRESHOLD, 1.0),
            ),
            box_detection(
                bbox=(295, 10, 335, 50),
                embedding=(1.0, ACTIVE_MATCH_THRESHOLD),
            ),
        ]
        state = association_state(
            detections,
            tracking=tracking,
            source_frame_index=10,
            max_centroid_displacement=30.0,
        )

        assign_active_appearance_matches(state)

        self.assertEqual(set(state.updated_tracks), {1, 2})
        self.assertEqual(state.updated_tracks[1].centroid, detections[0].centroid)
        self.assertEqual(state.updated_tracks[2].centroid, detections[1].centroid)
        self.assertFalse(
            np.allclose(tracking.identities[1], original_identity_1)
        )
        self.assertFalse(
            np.allclose(tracking.identities[2], original_identity_2)
        )

    def test_assign_active_appearance_matches_continues_after_invalid_rows(self):
        tracking = RTDetrTrackingState(
            tracks={
                1: track_profile(bbox=(90, 10, 130, 50), embedding=(1.0, 0.0)),
                2: track_profile(
                    bbox=(290, 10, 330, 50),
                    embedding=(0.0, 1.0),
                ),
            },
            identities={
                1: np.asarray([1.0, 0.0], dtype=np.float32),
                2: np.asarray([0.0, 1.0], dtype=np.float32),
            },
            telemetry={
                1: PersonTelemetry(entry_frame=0, last_seen_frame=0),
                2: PersonTelemetry(entry_frame=1, last_seen_frame=1),
            },
        )
        detections = [
            box_detection(
                bbox=(600, 10, 640, 50),
                embedding=(0.0, 1.0),
            ),
            box_detection(
                bbox=(295, 10, 335, 50),
                embedding=(0.1, 0.9),
            ),
        ]
        state = association_state(
            detections,
            tracking=tracking,
            source_frame_index=10,
            max_centroid_displacement=30.0,
        )

        assign_active_appearance_matches(state)

        self.assertEqual(set(state.updated_tracks), {2})
        self.assertEqual(state.updated_tracks[2].centroid, detections[1].centroid)
        self.assertEqual(state.matched_detection_indices, {1})
        self.assertEqual(state.matched_track_ids, {2})

    def test_assign_active_appearance_matches_continues_after_below_threshold_rows(
        self,
    ):
        tracking = RTDetrTrackingState(
            tracks={
                1: track_profile(bbox=(90, 10, 130, 50), embedding=(1.0, 0.0)),
                2: track_profile(
                    bbox=(290, 10, 330, 50),
                    embedding=(0.0, 1.0),
                ),
            },
            identities={
                1: np.asarray([1.0, 0.0], dtype=np.float32),
                2: np.asarray([0.0, 1.0], dtype=np.float32),
            },
            telemetry={
                1: PersonTelemetry(entry_frame=0, last_seen_frame=0),
                2: PersonTelemetry(entry_frame=1, last_seen_frame=1),
            },
        )
        detections = [
            box_detection(
                bbox=(95, 10, 135, 50),
                embedding=(ACTIVE_MATCH_THRESHOLD - 0.01, 0.0),
            ),
            box_detection(
                bbox=(295, 10, 335, 50),
                embedding=(0.1, 0.9),
            ),
        ]
        state = association_state(
            detections,
            tracking=tracking,
            source_frame_index=10,
            max_centroid_displacement=30.0,
        )

        assign_active_appearance_matches(state)

        self.assertEqual(set(state.updated_tracks), {2})
        self.assertEqual(state.updated_tracks[2].centroid, detections[1].centroid)
        self.assertEqual(state.matched_detection_indices, {1})
        self.assertEqual(state.matched_track_ids, {2})

    def test_assign_active_appearance_matches_filters_solver_inputs_and_skips_invalid_pairs(
        self,
    ):
        tracking = RTDetrTrackingState(
            tracks={
                1: track_profile(
                    bbox=(0, 0, 20, 40),
                    embedding=(1.0, 0.0),
                ),
                2: track_profile(
                    bbox=(100, 0, 120, 40),
                    embedding=(1.0, 0.0),
                ),
                3: track_profile(
                    bbox=(200, 0, 220, 40),
                    embedding=(0.0, 1.0),
                ),
            },
            identities={
                1: np.asarray([1.0, 0.0], dtype=np.float32),
                2: np.asarray([1.0, 0.0], dtype=np.float32),
                3: np.asarray([0.0, 1.0], dtype=np.float32),
            },
            telemetry={
                1: PersonTelemetry(entry_frame=0, last_seen_frame=0),
                2: PersonTelemetry(entry_frame=1, last_seen_frame=1),
                3: PersonTelemetry(entry_frame=2, last_seen_frame=2),
            },
        )
        detections = [
            box_detection(bbox=(300, 0, 320, 40), embedding=(0.0, 1.0)),
            box_detection(bbox=(400, 0, 420, 40), embedding=(1.0, 0.0)),
            box_detection(bbox=(500, 0, 520, 40), embedding=(1.0, 0.0)),
        ]
        state = association_state(
            detections,
            tracking=tracking,
            source_frame_index=10,
            max_centroid_displacement=30.0,
        )

        valid_pairs = {
            (detections[0].centroid, tracking.tracks[3].centroid),
            (detections[2].centroid, tracking.tracks[2].centroid),
        }

        def assert_costs(costs):
            np.testing.assert_allclose(
                costs,
                np.asarray(
                    [
                        [INVALID_ASSIGNMENT_COST, -1.0],
                        [-1.0, INVALID_ASSIGNMENT_COST],
                    ]
                ),
            )
            return np.asarray([0, 1]), np.asarray([0, 0])

        with (
            patch(
                "people_counter.pipelines.rtdetr_osnet._active_spatially_valid",
                side_effect=lambda actual_state, detection, profile: (
                    actual_state is state
                    and (detection.centroid, profile.centroid)
                    in valid_pairs
                ),
            ),
            patch(
                "people_counter.pipelines.rtdetr_osnet.linear_sum_assignment",
                side_effect=assert_costs,
            ),
        ):
            assign_active_appearance_matches(state)

        self.assertEqual(set(state.updated_tracks), {2})
        self.assertEqual(state.updated_tracks[2].centroid, detections[2].centroid)
        self.assertEqual(state.matched_detection_indices, {2})
        self.assertEqual(state.matched_track_ids, {2})

    def test_assign_spatial_secondary_matches_returns_when_no_track_candidates(
        self,
    ):
        tracking = RTDetrTrackingState(
            tracks={1: track_profile()},
        )
        state = association_state(
            [box_detection()],
            tracking=tracking,
        )
        state.matched_track_ids.add(1)

        assign_spatial_secondary_matches(state)

        self.assertEqual(state.updated_tracks, {})
        self.assertEqual(state.matched_detection_indices, set())

    def test_assign_spatial_secondary_matches_continues_past_invalid_rows_and_preserves_identity(
        self,
    ):
        preserved_identity = np.asarray([0.0, 1.0], dtype=np.float32)
        tracking = RTDetrTrackingState(
            tracks={
                1: track_profile(bbox=(90, 10, 130, 50), embedding=(1.0, 0.0)),
                2: track_profile(
                    bbox=(290, 10, 330, 50),
                    embedding=(1.0, 0.0),
                ),
            },
            identities={
                1: np.asarray([1.0, 0.0], dtype=np.float32),
                2: preserved_identity.copy(),
            },
            telemetry={
                1: PersonTelemetry(entry_frame=0, last_seen_frame=0),
                2: PersonTelemetry(entry_frame=1, last_seen_frame=1),
            },
        )
        detections = [
            box_detection(
                bbox=(600, 10, 640, 50),
                embedding=(1.0, 0.0),
            ),
            box_detection(
                bbox=(295, 10, 335, 50),
                embedding=(0.8, 0.0),
            ),
        ]
        state = association_state(
            detections,
            tracking=tracking,
            source_frame_index=10,
            max_centroid_displacement=30.0,
        )

        assign_spatial_secondary_matches(state)

        self.assertEqual(set(state.updated_tracks), {2})
        self.assertEqual(state.updated_tracks[2].centroid, detections[1].centroid)
        np.testing.assert_array_equal(tracking.identities[2], preserved_identity)
        self.assertEqual(state.matched_detection_indices, {1})
        self.assertEqual(state.matched_track_ids, {2})

    def test_assign_spatial_secondary_matches_marks_identity_update_false(
        self,
    ):
        tracking = RTDetrTrackingState(
            tracks={2: track_profile(bbox=(290, 10, 330, 50))},
        )
        state = association_state(
            [box_detection(bbox=(295, 10, 335, 50), embedding=(0.8, 0.0))],
            tracking=tracking,
            source_frame_index=10,
            max_centroid_displacement=30.0,
        )

        with patch(
            "people_counter.pipelines.rtdetr_osnet._record_track_match"
        ) as record_match:
            assign_spatial_secondary_matches(state)

        record_match.assert_called_once()
        self.assertIs(record_match.call_args.kwargs["update_identity"], False)

    def test_assign_spatial_secondary_matches_filters_solver_inputs_and_skips_invalid_pairs(
        self,
    ):
        tracking = RTDetrTrackingState(
            tracks={
                1: track_profile(
                    bbox=(0, 0, 20, 40),
                    embedding=(1.0, 0.0),
                ),
                2: track_profile(
                    bbox=(100, 0, 120, 40),
                    embedding=(1.0, 0.0),
                ),
                3: track_profile(
                    bbox=(200, 0, 220, 40),
                    embedding=(0.0, 1.0),
                ),
            },
            identities={
                1: np.asarray([1.0, 0.0], dtype=np.float32),
                2: np.asarray([1.0, 0.0], dtype=np.float32),
                3: np.asarray([0.0, 1.0], dtype=np.float32),
            },
            telemetry={
                1: PersonTelemetry(entry_frame=0, last_seen_frame=0),
                2: PersonTelemetry(entry_frame=1, last_seen_frame=1),
                3: PersonTelemetry(entry_frame=2, last_seen_frame=2),
            },
        )
        detections = [
            box_detection(bbox=(300, 0, 320, 40), embedding=(0.0, 1.0)),
            box_detection(bbox=(400, 0, 420, 40), embedding=(1.0, 0.0)),
            box_detection(bbox=(500, 0, 520, 40), embedding=(1.0, 0.0)),
        ]
        state = association_state(
            detections,
            tracking=tracking,
            source_frame_index=10,
            max_centroid_displacement=30.0,
        )
        pair_costs = {
            (detections[0].centroid, tracking.tracks[3].centroid): 0.2,
            (detections[2].centroid, tracking.tracks[2].centroid): 0.25,
        }

        def assert_costs(costs):
            np.testing.assert_allclose(
                costs,
                np.asarray(
                    [
                        [INVALID_ASSIGNMENT_COST, 0.2],
                        [0.25, INVALID_ASSIGNMENT_COST],
                    ]
                ),
            )
            return np.asarray([0, 1]), np.asarray([0, 0])

        with (
            patch(
                "people_counter.pipelines.rtdetr_osnet._secondary_pair_cost",
                side_effect=lambda actual_state, detection, profile: (
                    pair_costs.get((detection.centroid, profile.centroid))
                    if actual_state is state
                    else None
                ),
            ),
            patch(
                "people_counter.pipelines.rtdetr_osnet.linear_sum_assignment",
                side_effect=assert_costs,
            ),
        ):
            assign_spatial_secondary_matches(state)

        self.assertEqual(set(state.updated_tracks), {2})
        self.assertEqual(state.updated_tracks[2].centroid, detections[2].centroid)
        self.assertEqual(state.matched_detection_indices, {2})
        self.assertEqual(state.matched_track_ids, {2})

    def test_reentry_identity_ids_require_coasted_or_absent_tracks(self):
        tracking = RTDetrTrackingState(
            tracks={
                2: track_profile(age=0),
                3: track_profile(age=1),
            },
            identities={
                1: np.asarray([1.0, 0.0], dtype=np.float32),
                2: np.asarray([0.0, 1.0], dtype=np.float32),
                3: np.asarray([0.5, 0.5], dtype=np.float32),
                4: np.asarray([0.2, 0.8], dtype=np.float32),
            },
        )
        state = association_state([], tracking=tracking)
        state.updated_tracks[1] = track_profile()

        self.assertEqual(set(_reentry_identity_ids(state)), {3, 4})

    def test_record_reentry_match_refreshes_identity_track_and_telemetry(self):
        detection = box_detection(
            bbox=(110, 20, 150, 80),
            embedding=(0.0, 1.0),
        )
        tracking = RTDetrTrackingState(
            identities={1: np.asarray([1.0, 0.0], dtype=np.float32)},
            telemetry={
                1: PersonTelemetry(
                    entry_frame=7,
                    last_seen_frame=9,
                    last_geometry=LastGeometry(
                        centroid=(90, 30),
                        bbox=(80, 10, 120, 50),
                    ),
                )
            },
        )
        state = association_state(
            detection,
            tracking=tracking,
            source_frame_index=13,
        )

        _record_reentry_match(state, detection_index=0, track_id=1)

        expected_embedding = update_embedding(
            np.asarray([1.0, 0.0], dtype=np.float32),
            detection.embedding,
        )
        np.testing.assert_allclose(tracking.identities[1], expected_embedding)
        np.testing.assert_allclose(
            state.updated_tracks[1].embedding,
            expected_embedding,
        )
        self.assertEqual(state.updated_tracks[1].first_seen_frame, 7)
        self.assertEqual(state.updated_tracks[1].hits, MIN_CONFIRMATION_FRAMES)
        self.assertEqual(
            tracking.telemetry[1],
            PersonTelemetry(
                entry_frame=7,
                last_seen_frame=13,
                last_geometry=LastGeometry(
                    centroid=detection.centroid,
                    bbox=detection.bbox,
                ),
            ),
        )
        self.assertEqual(state.matched_detection_indices, {0})

    def test_assign_reentry_matches_prefers_high_similarity_identities(self):
        tracking = RTDetrTrackingState(
            identities={
                1: np.asarray([1.0, 0.0], dtype=np.float32),
                2: np.asarray([0.0, 1.0], dtype=np.float32),
            },
            telemetry={
                1: PersonTelemetry(
                    entry_frame=0,
                    last_seen_frame=5,
                    last_geometry=LastGeometry(
                        centroid=(100, 30),
                        bbox=(90, 10, 130, 50),
                    ),
                ),
                2: PersonTelemetry(
                    entry_frame=1,
                    last_seen_frame=6,
                    last_geometry=LastGeometry(
                        centroid=(110, 40),
                        bbox=(100, 20, 140, 60),
                    ),
                ),
            },
        )
        detections = [
            box_detection(
                bbox=(96, 12, 136, 52),
                embedding=(0.2, 0.8),
            ),
            box_detection(
                bbox=(106, 22, 146, 62),
                embedding=(0.9, 0.1),
            ),
        ]
        state = association_state(
            detections,
            tracking=tracking,
            source_frame_index=12,
            max_centroid_displacement=500.0,
        )

        assign_reentry_matches(state)

        self.assertEqual(set(state.updated_tracks), {1, 2})
        self.assertEqual(state.updated_tracks[1].centroid, detections[1].centroid)
        self.assertEqual(state.updated_tracks[2].centroid, detections[0].centroid)
        self.assertEqual(state.matched_detection_indices, {0, 1})

    def test_assign_reentry_matches_continues_after_invalid_rows_and_accepts_threshold(
        self,
    ):
        tracking = RTDetrTrackingState(
            identities={
                1: np.asarray([1.0, 0.0], dtype=np.float32),
                2: np.asarray([0.0, 1.0], dtype=np.float32),
            },
            telemetry={
                1: PersonTelemetry(
                    entry_frame=0,
                    last_seen_frame=5,
                    last_geometry=LastGeometry(
                        centroid=(300, 30),
                        bbox=(290, 10, 330, 50),
                    ),
                ),
                2: PersonTelemetry(
                    entry_frame=1,
                    last_seen_frame=5,
                    last_geometry=LastGeometry(
                        centroid=(500, 30),
                        bbox=(490, 10, 530, 50),
                    ),
                ),
            },
        )
        detections = [
            box_detection(
                bbox=(50, 10, 90, 50),
                embedding=(1.0, 0.0),
            ),
            box_detection(
                bbox=(295, 10, 335, 50),
                embedding=(REENTRY_MATCH_THRESHOLD, 0.0),
            ),
        ]
        state = association_state(
            detections,
            tracking=tracking,
            source_frame_index=12,
            max_centroid_displacement=30.0,
        )

        assign_reentry_matches(state)

        self.assertEqual(set(state.updated_tracks), {1})
        self.assertEqual(state.updated_tracks[1].centroid, detections[1].centroid)
        self.assertEqual(state.matched_detection_indices, {1})

    def test_assign_reentry_matches_continues_after_below_threshold_rows(self):
        tracking = RTDetrTrackingState(
            identities={
                1: np.asarray([1.0, 0.0], dtype=np.float32),
                2: np.asarray([0.0, 1.0], dtype=np.float32),
            },
            telemetry={
                1: PersonTelemetry(
                    entry_frame=0,
                    last_seen_frame=5,
                    last_geometry=LastGeometry(
                        centroid=(100, 30),
                        bbox=(90, 10, 130, 50),
                    ),
                ),
                2: PersonTelemetry(
                    entry_frame=1,
                    last_seen_frame=5,
                    last_geometry=LastGeometry(
                        centroid=(300, 30),
                        bbox=(290, 10, 330, 50),
                    ),
                ),
            },
        )
        detections = [
            box_detection(
                bbox=(95, 10, 135, 50),
                embedding=(REENTRY_MATCH_THRESHOLD - 0.01, 0.0),
            ),
            box_detection(
                bbox=(295, 10, 335, 50),
                embedding=(0.0, 0.9),
            ),
        ]
        state = association_state(
            detections,
            tracking=tracking,
            source_frame_index=12,
            max_centroid_displacement=30.0,
        )

        assign_reentry_matches(state)

        self.assertEqual(set(state.updated_tracks), {2})
        self.assertEqual(state.updated_tracks[2].centroid, detections[1].centroid)
        self.assertEqual(state.matched_detection_indices, {1})

    def test_assign_reentry_matches_filters_solver_inputs_and_skips_invalid_pairs(
        self,
    ):
        tracking = RTDetrTrackingState(
            identities={
                1: np.asarray([1.0, 0.0], dtype=np.float32),
                2: np.asarray([1.0, 0.0], dtype=np.float32),
                3: np.asarray([0.0, 1.0], dtype=np.float32),
            },
            telemetry={
                1: PersonTelemetry(
                    entry_frame=0,
                    last_seen_frame=5,
                    last_geometry=LastGeometry(
                        centroid=(10, 20),
                        bbox=(0, 0, 20, 40),
                    ),
                ),
                2: PersonTelemetry(
                    entry_frame=1,
                    last_seen_frame=5,
                    last_geometry=LastGeometry(
                        centroid=(110, 20),
                        bbox=(100, 0, 120, 40),
                    ),
                ),
                3: PersonTelemetry(
                    entry_frame=2,
                    last_seen_frame=5,
                    last_geometry=LastGeometry(
                        centroid=(210, 20),
                        bbox=(200, 0, 220, 40),
                    ),
                ),
            },
        )
        detections = [
            box_detection(bbox=(300, 0, 320, 40), embedding=(0.0, 1.0)),
            box_detection(bbox=(400, 0, 420, 40), embedding=(1.0, 0.0)),
            box_detection(bbox=(500, 0, 520, 40), embedding=(1.0, 0.0)),
        ]
        state = association_state(
            detections,
            tracking=tracking,
            source_frame_index=12,
            max_centroid_displacement=30.0,
        )
        valid_pairs = {
            (detections[0].centroid, tracking.telemetry[3].entry_frame),
            (detections[2].centroid, tracking.telemetry[2].entry_frame),
        }

        def assert_costs(costs):
            np.testing.assert_allclose(
                costs,
                np.asarray(
                    [
                        [INVALID_ASSIGNMENT_COST, -1.0],
                        [-1.0, INVALID_ASSIGNMENT_COST],
                    ]
                ),
            )
            return np.asarray([0, 1]), np.asarray([0, 0])

        with (
            patch(
                "people_counter.pipelines.rtdetr_osnet._reentry_spatially_valid",
                side_effect=lambda actual_state, detection, telemetry: (
                    actual_state is state
                    and (detection.centroid, telemetry.entry_frame)
                    in valid_pairs
                ),
            ),
            patch(
                "people_counter.pipelines.rtdetr_osnet.linear_sum_assignment",
                side_effect=assert_costs,
            ),
        ):
            assign_reentry_matches(state)

        self.assertEqual(set(state.updated_tracks), {2})
        self.assertEqual(state.updated_tracks[2].centroid, detections[2].centroid)
        self.assertEqual(state.matched_detection_indices, {2})

    def test_overlaps_unmatched_track_ignores_matched_tracks_and_keeps_boundary_similarity(
        self,
    ):
        state = association_state(
            [],
            tracking=RTDetrTrackingState(
                tracks={
                    1: track_profile(bbox=(400, 10, 440, 50)),
                    2: track_profile(bbox=(90, 10, 130, 50)),
                }
            ),
            max_centroid_displacement=30.0,
        )
        state.matched_track_ids.add(1)

        self.assertTrue(
            _overlaps_unmatched_track(
                state,
                box_detection(
                    bbox=(95, 10, 135, 50),
                    embedding=(SECONDARY_MATCH_THRESHOLD, 0.0),
                ),
            )
        )

    def test_spawn_tentative_tracks_continues_after_overlap_and_starts_with_one_hit(
        self,
    ):
        tracking = RTDetrTrackingState(
            tracks={1: track_profile(bbox=(90, 10, 130, 50))},
        )
        detections = [
            box_detection(bbox=(95, 10, 135, 50)),
            box_detection(bbox=(295, 10, 335, 50)),
        ]
        state = association_state(
            detections,
            tracking=tracking,
            source_frame_index=12,
            max_centroid_displacement=30.0,
        )

        spawn_tentative_tracks(state)

        self.assertEqual(set(state.updated_tracks), {-1})
        self.assertEqual(state.updated_tracks[-1].centroid, detections[1].centroid)
        self.assertEqual(state.updated_tracks[-1].hits, 1)
        self.assertEqual(state.tracking.next_tentative_id, -2)

    def test_age_unmatched_tracks_skips_updated_ids_and_keeps_processing_later_tracks(
        self,
    ):
        updated_track = track_profile(age=0)
        tracking = RTDetrTrackingState(
            tracks={
                1: track_profile(age=0),
                2: track_profile(age=1),
            }
        )
        state = association_state(
            [],
            tracking=tracking,
            max_disappeared_frames=2,
        )
        state.updated_tracks[1] = updated_track

        age_unmatched_tracks(state)

        self.assertIs(state.updated_tracks[1], updated_track)
        self.assertEqual(state.updated_tracks[2].age, 2)

    def test_age_unmatched_tracks_drops_corrupted_track_zero_after_aging(self):
        zero_track = track_profile(age=0)
        tracking = RTDetrTrackingState(tracks={0: zero_track})
        state = association_state(
            [],
            tracking=tracking,
            max_disappeared_frames=2,
        )

        age_unmatched_tracks(state)

        self.assertEqual(zero_track.age, 1)
        self.assertEqual(state.updated_tracks, {})


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

    def test_nms_uses_box_geometry_and_keeps_exact_threshold_iou(self):
        detections = [
            box_detection((100, 20, 117, 30), confidence=0.95),
            box_detection((103, 20, 120, 30), confidence=0.85),
            box_detection((102, 20, 119, 30), confidence=0.75),
            box_detection((150, 20, 167, 30), confidence=0.65),
        ]

        kept = non_max_suppression(detections)

        self.assertEqual(
            [item.bbox for item in kept],
            [
                (100, 20, 117, 30),
                (103, 20, 120, 30),
                (150, 20, 167, 30),
            ],
        )

    def test_nms_does_not_invent_overlap_when_boxes_are_separated_by_one_pixel(
        self,
    ):
        detections = [
            box_detection((0, 0, 1, 1), confidence=0.95),
            box_detection((2, 0, 3, 1), confidence=0.85),
            box_detection((0, 2, 1, 3), confidence=0.75),
        ]

        kept = non_max_suppression(detections)

        self.assertEqual(
            [item.bbox for item in kept],
            [
                (0, 0, 1, 1),
                (2, 0, 3, 1),
                (0, 2, 1, 3),
            ],
        )

    def test_nms_suppresses_identical_unit_boxes(self):
        detections = [
            box_detection((0, 0, 1, 1), confidence=0.95),
            box_detection((0, 0, 1, 1), confidence=0.85),
        ]

        kept = non_max_suppression(detections)

        self.assertEqual(
            [item.bbox for item in kept],
            [(0, 0, 1, 1)],
        )


if __name__ == "__main__":
    unittest.main()
