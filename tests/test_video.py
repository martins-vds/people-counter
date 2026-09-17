import unittest
from pathlib import Path
from unittest.mock import MagicMock

import cv2

from people_counter.video import (
    FrameReadState,
    format_video_timestamp,
    iter_sampled_frame_batches,
    read_video_metadata,
)
from tests.helpers import FakeCapture


class FrameReaderTests(unittest.TestCase):
    def test_video_metadata_preserves_valid_values(self):
        capture = MagicMock()
        values = {
            cv2.CAP_PROP_FPS: 1.0,
            cv2.CAP_PROP_FRAME_WIDTH: 1,
            cv2.CAP_PROP_FRAME_HEIGHT: 1,
            cv2.CAP_PROP_FRAME_COUNT: 7,
        }
        capture.get.side_effect = values.__getitem__

        metadata = read_video_metadata(capture, Path("tiny.mp4"))

        self.assertEqual(metadata.fps, 1.0)
        self.assertEqual(metadata.width, 1)
        self.assertEqual(metadata.height, 1)
        self.assertEqual(metadata.total_source_frames, 7)

    def test_video_metadata_rejects_each_nonpositive_dimension(self):
        for invalid_property in (
            cv2.CAP_PROP_FPS,
            cv2.CAP_PROP_FRAME_WIDTH,
            cv2.CAP_PROP_FRAME_HEIGHT,
        ):
            values = {
                cv2.CAP_PROP_FPS: 30.0,
                cv2.CAP_PROP_FRAME_WIDTH: 100,
                cv2.CAP_PROP_FRAME_HEIGHT: 80,
                cv2.CAP_PROP_FRAME_COUNT: 7,
            }
            values[invalid_property] = 0
            capture = MagicMock()
            capture.get.side_effect = values.__getitem__
            with self.subTest(property=invalid_property):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "^Invalid video metadata for input: invalid.mp4$",
                ):
                    read_video_metadata(capture, Path("invalid.mp4"))

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

    def test_unknown_expected_length_does_not_report_early_end(self):
        state = FrameReadState()
        list(
            iter_sampled_frame_batches(
                FakeCapture([0]),
                sample_interval=1,
                batch_size=1,
                expected_source_frames=0,
                read_state=state,
            )
        )

        self.assertFalse(state.ended_early)

    def test_empty_expected_video_and_last_expected_frame_are_not_early(self):
        empty_state = FrameReadState()
        list(
            iter_sampled_frame_batches(
                FakeCapture([]),
                sample_interval=1,
                batch_size=1,
                expected_source_frames=1,
                read_state=empty_state,
            )
        )
        last_frame_state = FrameReadState()
        list(
            iter_sampled_frame_batches(
                FakeCapture([0]),
                sample_interval=1,
                batch_size=1,
                expected_source_frames=2,
                read_state=last_frame_state,
            )
        )

        self.assertFalse(empty_state.ended_early)
        self.assertFalse(last_frame_state.ended_early)

    def test_timestamp_handles_minute_boundary_exactly(self):
        self.assertEqual(
            format_video_timestamp(1_800, 30.0),
            "00:01:00.000",
        )
