import unittest
from pathlib import Path
from typing import cast
from unittest.mock import patch

from people_counter import (
    PipelineConfig,
    RFDetrBotsortConfig,
    RTDetrOsnetConfig,
    RunResult,
    line_count_records,
    run,
    telemetry_records,
)
from people_counter.models import LineCountRecord, PersonTelemetry


class PublicSdkTests(unittest.TestCase):
    def test_run_dispatches_rtdetr_config(self):
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
        )
        expected = RunResult(initialized=True)

        with patch(
            "people_counter.pipelines.rtdetr_osnet.run",
            return_value=expected,
        ) as pipeline_run:
            result = run(config)

        self.assertIs(result, expected)
        pipeline_run.assert_called_once_with(config)

    def test_run_dispatches_botsort_config(self):
        config = RFDetrBotsortConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
        )
        expected = RunResult(initialized=True)

        with patch(
            "people_counter.pipelines.rfdetr_botsort.run",
            return_value=expected,
        ) as pipeline_run:
            result = run(config)

        self.assertIs(result, expected)
        pipeline_run.assert_called_once_with(config)

    def test_run_rejects_unknown_config_type(self):
        config = cast(PipelineConfig, object())

        with self.assertRaisesRegex(
            TypeError,
            (
                "^config must be RTDetrOsnetConfig or "
                "RFDetrBotsortConfig; got object$"
            ),
        ):
            run(config)

    def test_telemetry_records_are_sorted_and_dataframe_ready(self):
        result = RunResult(
            initialized=True,
            fps=4.0,
            telemetry={
                2: PersonTelemetry(entry_frame=4, last_seen_frame=12),
                1: PersonTelemetry(entry_frame=0, last_seen_frame=6),
            },
        )

        records = telemetry_records(result)

        self.assertEqual(
            records,
            [
                {
                    "person_id": 1,
                    "entry_frame": 0,
                    "exit_frame": 6,
                    "entry_seconds": 0.0,
                    "exit_seconds": 1.5,
                    "entry_timestamp": "00:00:00.000",
                    "exit_timestamp": "00:00:01.500",
                    "duration_seconds": 1.5,
                },
                {
                    "person_id": 2,
                    "entry_frame": 4,
                    "exit_frame": 12,
                    "entry_seconds": 1.0,
                    "exit_seconds": 3.0,
                    "entry_timestamp": "00:00:01.000",
                    "exit_timestamp": "00:00:03.000",
                    "duration_seconds": 2.0,
                },
            ],
        )

    def test_line_count_records_copy_all_public_fields(self):
        result = RunResult(
            initialized=True,
            line_counts=[
                LineCountRecord(
                    frame=3,
                    video_seconds="1.000",
                    video_timestamp="00:00:01.000",
                    frame_in_count=1,
                    frame_out_count=0,
                    cumulative_in_count=2,
                    cumulative_out_count=1,
                    line_start_x=0,
                    line_start_y=50,
                    line_end_x=99,
                    line_end_y=50,
                )
            ],
        )

        records = line_count_records(result)

        self.assertEqual(
            records,
            [
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
            ],
        )

    def test_record_converters_reject_uninitialized_results(self):
        for converter in (telemetry_records, line_count_records):
            with self.subTest(converter=converter):
                with self.assertRaisesRegex(
                    RuntimeError,
                    (
                        "^RunResult is not initialized; "
                        "run a pipeline before conversion$"
                    ),
                ):
                    converter(RunResult())

    def test_telemetry_records_reject_nonpositive_fps(self):
        with self.assertRaisesRegex(
            ValueError,
            "^RunResult fps must be greater than zero$",
        ):
            telemetry_records(RunResult(initialized=True, fps=0.0))

    def test_telemetry_records_accept_one_frame_per_second(self):
        result = RunResult(
            initialized=True,
            fps=1.0,
            telemetry={1: PersonTelemetry(0, 1)},
        )

        self.assertEqual(
            telemetry_records(result)[0]["duration_seconds"],
            1.0,
        )


if __name__ == "__main__":
    unittest.main()
