import unittest

from people_counter.config import (
    confirmed_entry_frame,
    disappeared_frames_for_sample_rate,
    lost_track_buffer_for_sample_rate,
    retention_seconds_for_sample_rate,
    sampling_config,
)
from people_counter.line_counting import create_line_zone


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

    def test_retention_and_disappearance_values_are_exact(self):
        self.assertEqual(retention_seconds_for_sample_rate(0.5), 4.0)
        self.assertEqual(retention_seconds_for_sample_rate(3.0), 1.0)
        self.assertEqual(disappeared_frames_for_sample_rate(0.5), 2)
        self.assertEqual(disappeared_frames_for_sample_rate(3.0), 3)

    def test_confirmed_entry_frame_backdates_confirmation(self):
        self.assertEqual(confirmed_entry_frame(20, 5), 15)
        self.assertEqual(confirmed_entry_frame(3, 5), 0)

    def test_sampling_all_frames(self):
        sampling = sampling_config(30.0, None, 61)

        self.assertEqual(sampling.interval, 1)
        self.assertEqual(sampling.effective_fps, 30.0)
        self.assertEqual(sampling.total_sampled_frames, 61)

    def test_sampling_caps_request_at_source_rate(self):
        sampling = sampling_config(24.0, 60.0, 48)

        self.assertEqual(sampling.interval, 1)
        self.assertEqual(sampling.effective_fps, 24.0)
        self.assertEqual(sampling.total_sampled_frames, 48)

    def test_sampling_below_source_rate(self):
        sampling = sampling_config(30.0, 3.0, 61)

        self.assertEqual(sampling.interval, 10)
        self.assertEqual(sampling.effective_fps, 3.0)
        self.assertEqual(sampling.total_sampled_frames, 7)

    def test_sampling_unknown_or_empty_video_has_no_estimated_frames(self):
        self.assertEqual(
            sampling_config(30.0, 3.0, 0).total_sampled_frames,
            0,
        )
        self.assertEqual(
            sampling_config(30.0, 3.0, -1).total_sampled_frames,
            0,
        )
        self.assertEqual(
            sampling_config(30.0, 3.0, -11).total_sampled_frames,
            0,
        )
        self.assertEqual(
            sampling_config(30.0, 3.0, 1).total_sampled_frames,
            1,
        )

    def test_line_zone_rejects_invalid_coordinates(self):
        with self.assertRaises(ValueError):
            create_line_zone((1, 1, 1, 1), 100, 100)
        with self.assertRaises(ValueError):
            create_line_zone((0, 0, 100, 50), 100, 100)
