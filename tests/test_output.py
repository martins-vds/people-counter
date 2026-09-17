import csv
import tempfile
import unittest
from pathlib import Path

from people_counter.models import LineCountRecord, PersonTelemetry, RunResult
from people_counter.output import (
    OutputPaths,
    generate_output_paths,
    write_line_counts,
    write_run_result,
    write_telemetry,
)
from people_counter.video import format_video_timestamp


class OutputTests(unittest.TestCase):
    def test_output_paths_match_pipeline_filename_conventions(self):
        video = Path("samples/example.mp4")

        rtdetr = generate_output_paths(
            video,
            "",
            "cpu",
            True,
            Path("results"),
            "20260101T000000000000Z",
        )
        botsort = generate_output_paths(
            video,
            "rfdetr_large_botsort",
            "gpu",
            False,
            Path("results"),
            "20260101T000000000000Z",
        )

        self.assertEqual(
            rtdetr.telemetry,
            Path("results/example_telemetry_cpu_20260101T000000000000Z.csv"),
        )
        self.assertEqual(
            rtdetr.line_counts,
            Path(
                "results/example_line_counts_cpu_"
                "20260101T000000000000Z.csv"
            ),
        )
        self.assertEqual(
            botsort.telemetry,
            Path(
                "results/example_telemetry_rfdetr_large_botsort_gpu_"
                "20260101T000000000000Z.csv"
            ),
        )
        self.assertIsNone(botsort.line_counts)

    def test_uninitialized_run_result_is_not_written(self):
        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "telemetry.csv"
            write_run_result(
                OutputPaths(telemetry=output_path, line_counts=None),
                RunResult(),
            )

            self.assertFalse(output_path.exists())

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
                    2: PersonTelemetry(entry_frame=3, last_seen_frame=9),
                    1: PersonTelemetry(entry_frame=0, last_seen_frame=6),
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
