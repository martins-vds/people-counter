import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from people_counter.cli import (
    _execute,
    _print_progress,
    build_parser,
    main,
)
from people_counter.config import RTDetrOsnetConfig
from people_counter.models import PersonTelemetry, RunResult
from people_counter.output import OutputPaths


class CliTests(unittest.TestCase):
    def test_parser_accepts_each_pipeline_specific_option(self):
        with tempfile.NamedTemporaryFile(suffix=".mp4") as video:
            rtdetr = build_parser().parse_args(
                [
                    "rtdetr-osnet",
                    video.name,
                    "--device",
                    "cpu",
                    "--detector-model",
                    "r50",
                ]
            )
            botsort = build_parser().parse_args(
                [
                    "rfdetr-botsort",
                    video.name,
                    "--device",
                    "cpu",
                    "--no-cmc",
                ]
            )

        self.assertEqual(rtdetr.detector_model, "r50")
        self.assertFalse(botsort.cmc)

    def test_main_builds_rtdetr_config_with_cpu_defaults(self):
        captured = {}

        def run(config):
            captured["config"] = config
            config.result.initialized = True
            config.result.fps = 30.0
            config.result.effective_sample_fps = 3.0
            return config.result

        with (
            tempfile.TemporaryDirectory() as directory,
            tempfile.NamedTemporaryFile(suffix=".mp4") as video,
            patch("people_counter.cli.resolve_device", return_value="cpu"),
            patch("people_counter.cli._load_rtdetr_runner", return_value=run),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            exit_code = main(
                [
                    "rtdetr-osnet",
                    video.name,
                    "--device",
                    "cpu",
                    "--output-dir",
                    directory,
                ]
            )

        config = captured["config"]
        self.assertEqual(exit_code, 0)
        self.assertIsInstance(config, RTDetrOsnetConfig)
        self.assertEqual(config.batch_size, 1)
        self.assertFalse(config.use_fp16)
        self.assertIsNotNone(config.progress_callback)

    def test_main_builds_botsort_config_with_gpu_defaults(self):
        captured = {}

        def run(config):
            captured["config"] = config
            config.result.initialized = True
            config.result.fps = 30.0
            config.result.effective_sample_fps = 3.0
            return config.result

        with (
            tempfile.TemporaryDirectory() as directory,
            tempfile.NamedTemporaryFile(suffix=".mp4") as video,
            patch("people_counter.cli.resolve_device", return_value="cuda"),
            patch("people_counter.cli._load_botsort_runner", return_value=run),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            exit_code = main(
                [
                    "rfdetr-botsort",
                    video.name,
                    "--device",
                    "gpu",
                    "--cmc",
                    "--output-dir",
                    directory,
                ]
            )
            telemetry_files = list(Path(directory).glob("*_telemetry_*.csv"))

        config = captured["config"]
        self.assertEqual(exit_code, 0)
        self.assertEqual(config.batch_size, 4)
        self.assertTrue(config.use_fp16)
        self.assertTrue(config.camera_motion_compensation)
        self.assertEqual(len(telemetry_files), 1)
        self.assertIn(
            "rfdetr_large_botsort_gpu",
            telemetry_files[0].name,
        )

    def test_invalid_output_directory_fails_before_runner_load(self):
        with (
            tempfile.NamedTemporaryFile(suffix=".mp4") as video,
            tempfile.NamedTemporaryFile() as output_file,
            patch("people_counter.cli._load_rtdetr_runner") as load_runner,
            contextlib.redirect_stderr(io.StringIO()),
        ):
            with self.assertRaises(SystemExit):
                main(
                    [
                        "rtdetr-osnet",
                        video.name,
                        "--device",
                        "cpu",
                        "--output-dir",
                        output_file.name,
                    ]
                )

        load_runner.assert_not_called()

    def test_execute_persists_partial_result_and_reraises(self):
        result = RunResult()
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
            result=result,
        )

        def run(failing_config):
            failing_config.result.initialized = True
            failing_config.result.fps = 3.0
            failing_config.result.telemetry[1] = PersonTelemetry(0, 3)
            raise KeyboardInterrupt

        with tempfile.TemporaryDirectory() as directory:
            telemetry_path = Path(directory) / "partial.csv"
            stderr = io.StringIO()
            with (
                self.assertRaises(KeyboardInterrupt),
                contextlib.redirect_stderr(stderr),
            ):
                _execute(config, run, OutputPaths(telemetry_path, None))

            self.assertTrue(telemetry_path.is_file())
            self.assertIn("person_id", telemetry_path.read_text())
            self.assertIn("Partial person telemetry saved", stderr.getvalue())

    def test_progress_reports_initial_settings_and_batch_count(self):
        result = RunResult(
            initialized=True,
            effective_sample_fps=3.0,
            sample_interval=10,
            total_sampled_frames=12,
            batch_size=4,
            use_fp16=True,
        )
        output = io.StringIO()

        with contextlib.redirect_stdout(output):
            _print_progress(result)
            result.processed_frames = 4
            _print_progress(result)

        text = output.getvalue()
        self.assertIn("Sampling 3.00 FPS", text)
        self.assertIn("Processed 4/12 sampled frames", text)


if __name__ == "__main__":
    unittest.main()
