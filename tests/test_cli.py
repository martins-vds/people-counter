import argparse
import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from people_counter.cli import (
    PIPELINES,
    _common_parser,
    _legacy_main,
    _execute,
    _print_progress,
    _print_summary,
    build_parser,
    detection_threshold_value,
    main,
    positive_int,
    resolve_device,
    rfdetr_botsort_main,
    rtdetr_osnet_main,
    sample_fps_value,
    video_file_path,
)
from people_counter.config import RTDetrOsnetConfig
from people_counter.models import PersonTelemetry, RunResult
from people_counter.output import OutputPaths


class CliTests(unittest.TestCase):
    def test_cli_value_parsers_accept_valid_values(self):
        self.assertEqual(positive_int("1"), 1)
        self.assertEqual(positive_int("4"), 4)
        self.assertIsNone(sample_fps_value("ALL"))
        self.assertEqual(sample_fps_value("1"), 1.0)
        self.assertEqual(sample_fps_value("2.5"), 2.5)
        self.assertEqual(detection_threshold_value("0.1"), 0.1)
        self.assertEqual(detection_threshold_value("1"), 1.0)

    def test_cli_value_parsers_reject_out_of_range_values(self):
        invalid_values = (
            (positive_int, "0", "^Value must be greater than zero$"),
            (positive_int, "-1", "^Value must be greater than zero$"),
            (
                sample_fps_value,
                "0",
                "^Sample FPS must be greater than zero$",
            ),
            (
                detection_threshold_value,
                "0.09",
                "^Detection threshold must be in the range \\[0.1, 1\\]$",
            ),
            (
                detection_threshold_value,
                "1.01",
                "^Detection threshold must be in the range \\[0.1, 1\\]$",
            ),
        )
        for parser, value, message in invalid_values:
            with self.subTest(parser=parser.__name__, value=value):
                with self.assertRaisesRegex(
                    argparse.ArgumentTypeError,
                    message,
                ):
                    parser(value)

    def test_resolve_device_accepts_matching_torch_variants(self):
        with patch("torch.version.cuda", None):
            self.assertEqual(resolve_device("cpu"), "cpu")
        with (
            patch("torch.version.cuda", "12.8"),
            patch("torch.cuda.is_available", return_value=True),
        ):
            self.assertEqual(resolve_device("gpu"), "cuda")

    def test_resolve_device_rejects_mismatched_or_unavailable_variants(self):
        with (
            patch("torch.version.cuda", "12.8"),
            self.assertRaisesRegex(
                RuntimeError,
                (
                    "^CPU mode requires the CPU-only PyTorch build\\. "
                    "Run with: uv run --extra cpu people-counter "
                    "<subcommand> <video> --device cpu$"
                ),
            ),
        ):
            resolve_device("cpu")
        with (
            patch("torch.version.cuda", None),
            self.assertRaisesRegex(
                RuntimeError,
                (
                    "^GPU mode requires a CUDA-enabled PyTorch build\\. "
                    "Run with: uv run --extra gpu people-counter "
                    "<subcommand> <video> --device gpu$"
                ),
            ),
        ):
            resolve_device("gpu")
        with (
            patch("torch.version.cuda", "12.8"),
            patch("torch.cuda.is_available", return_value=False),
            self.assertRaisesRegex(
                RuntimeError,
                (
                    "^GPU mode was requested, but CUDA is unavailable\\. "
                    "Check the NVIDIA driver and GPU access\\.$"
                ),
            ),
        ):
            resolve_device("gpu")

    def test_legacy_main_prepends_subcommand_and_exits(self):
        with (
            patch("people_counter.cli.sys.argv", ["legacy", "video.mp4"]),
            patch("people_counter.cli.main", return_value=7) as unified_main,
            self.assertRaises(SystemExit) as raised,
        ):
            _legacy_main("rtdetr-osnet")

        self.assertEqual(raised.exception.code, 7)
        unified_main.assert_called_once_with(
            ["rtdetr-osnet", "video.mp4"]
        )

    def test_legacy_entry_points_select_their_pipeline(self):
        with patch(
            "people_counter.cli._legacy_main",
            side_effect=SystemExit,
        ) as legacy_main:
            with self.assertRaises(SystemExit):
                rtdetr_osnet_main()
            legacy_main.assert_called_once_with("rtdetr-osnet")

            legacy_main.reset_mock()
            with self.assertRaises(SystemExit):
                rfdetr_botsort_main()
            legacy_main.assert_called_once_with("rfdetr-botsort")

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

    def test_parser_contract_exposes_expected_commands_and_options(self):
        parser = build_parser()
        self.assertEqual(parser.prog, "people-counter")
        self.assertEqual(
            parser.description,
            "Count and track people in a video.",
        )
        subparsers_action = next(
            action
            for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        )
        self.assertTrue(subparsers_action.required)
        self.assertEqual(subparsers_action.dest, "pipeline")
        self.assertEqual(
            subparsers_action.metavar,
            "{rtdetr-osnet,rfdetr-botsort}",
        )
        self.assertEqual(
            set(subparsers_action.choices),
            set(PIPELINES),
        )
        command_help = {
            action.dest: action.help
            for action in subparsers_action._choices_actions
        }
        self.assertEqual(
            command_help,
            {
                "rtdetr-osnet": (
                    "RT-DETRv2 detection with OSNet identity tracking."
                ),
                "rfdetr-botsort": (
                    "RF-DETR Large detection with BoT-SORT tracking."
                ),
            },
        )

        common_actions = {
            action.dest: action for action in _common_parser()._actions
        }
        self.assertEqual(
            set(common_actions),
            {
                "video",
                "device",
                "sample_fps",
                "batch_size",
                "detection_threshold",
                "no_fp16",
                "line",
                "output_dir",
            },
        )
        self.assertEqual(common_actions["video"].type, video_file_path)
        self.assertEqual(
            common_actions["video"].help,
            "Path to the input video file.",
        )
        self.assertEqual(common_actions["device"].choices, ("cpu", "gpu"))
        self.assertTrue(common_actions["device"].required)
        self.assertEqual(
            common_actions["device"].help,
            "Use the matching CPU-only or CUDA-enabled PyTorch variant.",
        )
        self.assertEqual(common_actions["sample_fps"].type, sample_fps_value)
        self.assertEqual(common_actions["sample_fps"].default, 3.0)
        self.assertEqual(common_actions["sample_fps"].metavar, "FPS|all")
        self.assertEqual(
            common_actions["sample_fps"].help,
            "Video sampling rate (default: 3; use 'all' for every frame).",
        )
        self.assertEqual(common_actions["batch_size"].type, positive_int)
        self.assertIsNone(common_actions["batch_size"].default)
        self.assertEqual(
            common_actions["batch_size"].help,
            "Detector batch size (pipeline and device specific by default).",
        )
        self.assertEqual(
            common_actions["detection_threshold"].type,
            detection_threshold_value,
        )
        self.assertEqual(
            common_actions["detection_threshold"].default,
            0.6,
        )
        self.assertEqual(
            common_actions["detection_threshold"].help,
            "Confidence required to activate a track (default: 0.6).",
        )
        self.assertIsInstance(
            common_actions["no_fp16"],
            argparse._StoreTrueAction,
        )
        self.assertEqual(
            common_actions["no_fp16"].help,
            "Disable FP16 detector inference in GPU mode.",
        )
        self.assertEqual(common_actions["line"].type, int)
        self.assertEqual(common_actions["line"].nargs, 4)
        self.assertEqual(
            common_actions["line"].metavar,
            ("X1", "Y1", "X2", "Y2"),
        )
        self.assertEqual(
            common_actions["line"].help,
            "Directed counting line in source-video pixels.",
        )
        self.assertIs(common_actions["output_dir"].type, Path)
        self.assertEqual(
            common_actions["output_dir"].default,
            Path("outputs"),
        )
        self.assertEqual(
            common_actions["output_dir"].help,
            "Directory for generated CSV files (default: outputs).",
        )

        rtdetr_actions = {
            action.dest: action
            for action in subparsers_action.choices[
                "rtdetr-osnet"
            ]._actions
        }
        self.assertEqual(
            rtdetr_actions["detector_model"].choices,
            ("r18", "r50"),
        )
        self.assertEqual(rtdetr_actions["detector_model"].default, "r18")
        self.assertEqual(
            rtdetr_actions["detector_model"].help,
            "RT-DETRv2 backbone (default: r18).",
        )
        botsort_actions = {
            action.dest: action
            for action in subparsers_action.choices[
                "rfdetr-botsort"
            ]._actions
        }
        self.assertEqual(
            botsort_actions["cmc"].help,
            (
                "Camera-motion compensation "
                "(default: on only for all frames)."
            ),
        )
        defaults = build_parser().parse_args(
            [
                "rfdetr-botsort",
                __file__,
                "--device",
                "cpu",
            ]
        )
        self.assertIsNone(defaults.cmc)

    def test_common_parser_explicitly_disables_parent_help(self):
        parser_type = MagicMock(side_effect=argparse.ArgumentParser)
        argparse_module = MagicMock(ArgumentParser=parser_type)
        with patch("people_counter.cli.argparse", argparse_module):
            _common_parser()

        parser_type.assert_called_once_with(add_help=False)

    def test_subcommand_help_preserves_user_facing_descriptions(self):
        help_text = build_parser().format_help()
        self.assertIn("Count and track people in a video.", help_text)
        self.assertIn(
            "RT-DETRv2 detection with OSNet identity tracking.",
            help_text,
        )
        self.assertIn(
            "RF-DETR Large detection with BoT-SORT tracking.",
            help_text,
        )
        with self.assertRaises(SystemExit):
            with contextlib.redirect_stdout(io.StringIO()) as output:
                build_parser().parse_args(["rtdetr-osnet", "--help"])
        rtdetr_help = " ".join(output.getvalue().split())
        for expected in (
            "Path to the input video file.",
            "Use the matching CPU-only or CUDA-enabled PyTorch variant.",
            "Video sampling rate (default: 3; use 'all' for every frame).",
            "Detector batch size (pipeline and device specific by default).",
            "Confidence required to activate a track (default: 0.6).",
            "Disable FP16 detector inference in GPU mode.",
            "Directed counting line in source-video pixels.",
            "Directory for generated CSV files (default: outputs).",
            "RT-DETRv2 backbone (default: r18).",
        ):
            self.assertIn(expected, rtdetr_help)

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

    def test_main_propagates_explicit_common_options(self):
        captured = {}

        def run(config):
            captured["config"] = config
            config.result.initialized = True
            config.result.fps = 30.0
            return config.result

        with (
            tempfile.TemporaryDirectory() as directory,
            tempfile.NamedTemporaryFile(suffix=".mp4") as video,
            patch(
                "people_counter.cli.resolve_device",
                return_value="cpu",
            ) as resolve,
            patch(
                "people_counter.cli._load_rtdetr_runner",
                return_value=run,
            ),
            contextlib.redirect_stdout(io.StringIO()) as output,
        ):
            output_dir = Path(directory, "nested", "outputs")
            exit_code = main(
                [
                    "rtdetr-osnet",
                    video.name,
                    "--device",
                    "cpu",
                    "--sample-fps",
                    "all",
                    "--batch-size",
                    "3",
                    "--detection-threshold",
                    "0.75",
                    "--no-fp16",
                    "--line",
                    "1",
                    "2",
                    "3",
                    "4",
                    "--output-dir",
                    str(output_dir),
                    "--detector-model",
                    "r50",
                ]
            )
            line_files = list(output_dir.glob("*_line_counts_*.csv"))

        config = captured["config"]
        self.assertEqual(exit_code, 0)
        resolve.assert_called_once_with("cpu")
        self.assertEqual(config.device, "cpu")
        self.assertEqual(config.device_variant, "cpu")
        self.assertEqual(config.batch_size, 3)
        self.assertIsNone(config.sample_fps)
        self.assertEqual(config.detection_threshold, 0.75)
        self.assertFalse(config.use_fp16)
        self.assertEqual(config.line, (1, 2, 3, 4))
        self.assertEqual(config.detector_model, "r50")
        self.assertEqual(len(line_files), 1)
        self.assertIn("Running CPU variant on: cpu", output.getvalue())
        self.assertIn("Loading detector: RT-DETRv2 (R50)", output.getvalue())

    def test_invalid_output_directory_fails_before_runner_load(self):
        with (
            tempfile.NamedTemporaryFile(suffix=".mp4") as video,
            tempfile.NamedTemporaryFile() as output_file,
            patch("people_counter.cli._load_rtdetr_runner") as load_runner,
            contextlib.redirect_stderr(io.StringIO()) as stderr,
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
        self.assertIn(
            "Could not create output directory:",
            stderr.getvalue(),
        )

    def test_video_file_path_error_is_exact(self):
        missing = Path("definitely-missing-video.mp4")
        with self.assertRaisesRegex(
            argparse.ArgumentTypeError,
            f"^Video file does not exist: {missing}$",
        ):
            video_file_path(str(missing))

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
            line_path = Path(directory) / "partial_line.csv"
            stderr = io.StringIO()
            with (
                self.assertRaises(KeyboardInterrupt),
                contextlib.redirect_stderr(stderr),
            ):
                _execute(
                    config,
                    run,
                    OutputPaths(telemetry_path, line_path),
                )

            self.assertTrue(telemetry_path.is_file())
            self.assertTrue(line_path.is_file())
            self.assertIn("person_id", telemetry_path.read_text())
            self.assertEqual(
                stderr.getvalue(),
                (
                    f"Partial person telemetry saved to: {telemetry_path}\n"
                    f"Partial line counts saved to: {line_path}\n"
                ),
            )

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

    def test_progress_reports_cmc_and_unknown_total_exactly(self):
        result = RunResult(
            initialized=True,
            effective_sample_fps=30.0,
            sample_interval=1,
            total_sampled_frames=0,
            batch_size=2,
            use_fp16=False,
            camera_motion_compensation=False,
        )
        output = io.StringIO()

        with contextlib.redirect_stdout(output):
            _print_progress(result)
            result.processed_frames = 2
            _print_progress(result)

        self.assertEqual(
            output.getvalue(),
            (
                "Sampling 30.00 FPS (every 1 source frame(s)); "
                "detector batch size 2; FP16 False\n"
                "Camera-motion compensation: False\n"
                "\rProcessed 2 sampled frames"
            ),
        )

    def test_progress_includes_single_known_frame_total(self):
        result = RunResult(
            processed_frames=1,
            total_sampled_frames=1,
        )
        with contextlib.redirect_stdout(io.StringIO()) as output:
            _print_progress(result)

        self.assertEqual(
            output.getvalue(),
            "\rProcessed 1/1 sampled frames",
        )
        with patch("builtins.print") as print_output:
            _print_progress(result)
        print_output.assert_called_once_with(
            "\rProcessed 1/1 sampled frames",
            end="",
            flush=True,
        )

    def test_summary_reports_warning_rates_and_line_outputs(self):
        result = RunResult(
            initialized=True,
            total_source_frames=100,
            source_frames_read=80,
            processed_frames=20,
            processing_seconds=4.0,
            ended_early=True,
            telemetry={1: PersonTelemetry(0, 10)},
            line_in_count=3,
            line_out_count=2,
        )
        paths = OutputPaths(
            Path("telemetry.csv"),
            Path("line.csv"),
        )
        stdout = io.StringIO()
        stderr = io.StringIO()

        with (
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            _print_summary(result, paths)

        self.assertEqual(
            stderr.getvalue(),
            (
                "Warning: video decoding stopped after 80/100 "
                "source frames\n"
            ),
        )
        self.assertEqual(
            stdout.getvalue(),
            (
                "\n"
                "Processing time: 4.0s (5.00 sampled FPS)\n"
                "Process ended cleanly. Total distinct individuals: 1\n"
                "Person telemetry saved to: telemetry.csv\n"
                "Line crossings: 3 in, 2 out\n"
                "Line counts saved to: line.csv\n"
            ),
        )

    def test_summary_zero_frame_run_has_no_leading_blank_and_zero_rate(self):
        result = RunResult(processing_seconds=0.0)
        with contextlib.redirect_stdout(io.StringIO()) as output:
            _print_summary(
                result,
                OutputPaths(Path("telemetry.csv"), None),
            )

        self.assertEqual(
            output.getvalue(),
            (
                "Processing time: 0.0s (0.00 sampled FPS)\n"
                "Process ended cleanly. Total distinct individuals: 0\n"
                "Person telemetry saved to: telemetry.csv\n"
            ),
        )

    def test_summary_single_frame_run_starts_with_blank_line(self):
        result = RunResult(
            processed_frames=1,
            processing_seconds=1.0,
        )
        with contextlib.redirect_stdout(io.StringIO()) as output:
            _print_summary(
                result,
                OutputPaths(Path("telemetry.csv"), None),
            )

        self.assertTrue(output.getvalue().startswith("\nProcessing time:"))
    def test_execute_adds_partial_output_failure_note(self):
        result = RunResult(initialized=True, fps=30.0)
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
            result=result,
        )
        processing_error = RuntimeError("processing failed")

        def fail(_):
            raise processing_error

        with contextlib.redirect_stderr(io.StringIO()) as stderr:
            with (
                patch(
                    "people_counter.cli.write_run_result",
                    side_effect=OSError("disk full"),
                ),
                self.assertRaises(RuntimeError) as raised,
            ):
                _execute(
                    config,
                    fail,
                    OutputPaths(Path("telemetry.csv"), Path("line.csv")),
                )

        self.assertIs(raised.exception, processing_error)
        self.assertEqual(
            raised.exception.__notes__,
            ["Additionally failed to persist partial results: disk full"],
        )
        self.assertEqual(stderr.getvalue(), "")

    def test_execute_reports_partial_output_failure_without_add_note(self):
        result = RunResult(initialized=True, fps=30.0)
        config = RTDetrOsnetConfig(
            video=Path("video.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
            result=result,
        )

        def fail(_):
            raise RuntimeError("processing failed")

        stderr = io.StringIO()
        with (
            patch(
                "people_counter.cli.write_run_result",
                side_effect=OSError("disk full"),
            ),
            patch(
                "people_counter.cli.hasattr",
                return_value=False,
                create=True,
            ),
            contextlib.redirect_stderr(stderr),
            self.assertRaisesRegex(RuntimeError, "processing failed"),
        ):
            _execute(
                config,
                fail,
                OutputPaths(Path("telemetry.csv"), None),
            )

        self.assertEqual(
            stderr.getvalue(),
            (
                "Additionally failed to persist partial results: "
                "disk full\n"
            ),
        )


if __name__ == "__main__":
    unittest.main()
