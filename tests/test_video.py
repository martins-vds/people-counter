import unittest

from people_counter.video import FrameReadState, iter_sampled_frame_batches
from tests.helpers import FakeCapture


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
