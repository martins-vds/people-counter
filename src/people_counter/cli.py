"""Unified command-line interface for all people-counting pipelines."""

import argparse
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn, TypeVar

from people_counter.config import (
    DETECTOR_FLOOR,
    RFDetrBotsortConfig,
    RTDetrOsnetConfig,
    CommonPipelineConfig,
)
from people_counter.models import RunResult
from people_counter.output import (
    OutputPaths,
    generate_output_paths,
    write_run_result,
)

ConfigT = TypeVar("ConfigT", bound=CommonPipelineConfig)
Runner = Callable[[Any], RunResult]


@dataclass(frozen=True)
class PipelineSpec:
    gpu_batch_size: int
    output_suffix: str
    detector_label: Callable[[argparse.Namespace], str]
    build_config: Callable[
        [argparse.Namespace, dict[str, Any]],
        CommonPipelineConfig,
    ]
    load_runner: Callable[[], Runner]


def _load_rtdetr_runner() -> Runner:
    from people_counter.pipelines.rtdetr_osnet import run

    return run


def _load_botsort_runner() -> Runner:
    from people_counter.pipelines.rfdetr_botsort import run

    return run


def _build_rtdetr_config(
    args: argparse.Namespace,
    common_kwargs: dict[str, Any],
) -> CommonPipelineConfig:
    return RTDetrOsnetConfig(
        **common_kwargs,
        detector_model=args.detector_model,
    )


def _build_botsort_config(
    args: argparse.Namespace,
    common_kwargs: dict[str, Any],
) -> CommonPipelineConfig:
    return RFDetrBotsortConfig(
        **common_kwargs,
        camera_motion_compensation=args.cmc,
    )


PIPELINES = {
    "rtdetr-osnet": PipelineSpec(
        gpu_batch_size=8,
        output_suffix="",
        detector_label=lambda args: (
            f"RT-DETRv2 ({args.detector_model.upper()})"
        ),
        build_config=_build_rtdetr_config,
        load_runner=lambda: _load_rtdetr_runner(),
    ),
    "rfdetr-botsort": PipelineSpec(
        gpu_batch_size=4,
        output_suffix="rfdetr_large_botsort",
        detector_label=lambda _: "RF-DETR Large",
        build_config=_build_botsort_config,
        load_runner=lambda: _load_botsort_runner(),
    ),
}


def video_file_path(value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_file():
        raise argparse.ArgumentTypeError(f"Video file does not exist: {path}")
    return path


def positive_int(value: str) -> int:
    parsed_value = int(value)
    if parsed_value <= 0:
        raise argparse.ArgumentTypeError("Value must be greater than zero")
    return parsed_value


def sample_fps_value(value: str) -> float | None:
    if value.lower() == "all":
        return None
    parsed_value = float(value)
    if parsed_value <= 0:
        raise argparse.ArgumentTypeError("Sample FPS must be greater than zero")
    return parsed_value


def detection_threshold_value(value: str) -> float:
    parsed_value = float(value)
    if not DETECTOR_FLOOR <= parsed_value <= 1:
        raise argparse.ArgumentTypeError(
            f"Detection threshold must be in the range [{DETECTOR_FLOOR}, 1]"
        )
    return parsed_value


def resolve_device(device_variant: str) -> str:
    import torch

    if device_variant == "cpu":
        if torch.version.cuda is not None:
            raise RuntimeError(
                "CPU mode requires the CPU-only PyTorch build. "
                "Run with: uv run --extra cpu people-counter "
                "<subcommand> <video> --device cpu"
            )
        return "cpu"

    if torch.version.cuda is None:
        raise RuntimeError(
            "GPU mode requires a CUDA-enabled PyTorch build. "
            "Run with: uv run --extra gpu people-counter "
            "<subcommand> <video> --device gpu"
        )
    if not torch.cuda.is_available():
        raise RuntimeError(
            "GPU mode was requested, but CUDA is unavailable. "
            "Check the NVIDIA driver and GPU access."
        )
    return "cuda"


def _common_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "video",
        type=video_file_path,
        help="Path to the input video file.",
    )
    parser.add_argument(
        "--device",
        choices=("cpu", "gpu"),
        required=True,
        help="Use the matching CPU-only or CUDA-enabled PyTorch variant.",
    )
    parser.add_argument(
        "--sample-fps",
        type=sample_fps_value,
        default=3.0,
        metavar="FPS|all",
        help="Video sampling rate (default: 3; use 'all' for every frame).",
    )
    parser.add_argument(
        "--batch-size",
        type=positive_int,
        help="Detector batch size (pipeline and device specific by default).",
    )
    parser.add_argument(
        "--detection-threshold",
        type=detection_threshold_value,
        default=0.6,
        help="Confidence required to activate a track (default: 0.6).",
    )
    parser.add_argument(
        "--no-fp16",
        action="store_true",
        help="Disable FP16 detector inference in GPU mode.",
    )
    parser.add_argument(
        "--line",
        type=int,
        nargs=4,
        metavar=("X1", "Y1", "X2", "Y2"),
        help="Directed counting line in source-video pixels.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs"),
        help="Directory for generated CSV files (default: outputs).",
    )
    return parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="people-counter",
        description="Count and track people in a video.",
    )
    subparsers = parser.add_subparsers(
        dest="pipeline",
        required=True,
        metavar="{rtdetr-osnet,rfdetr-botsort}",
    )
    common = _common_parser()

    rtdetr = subparsers.add_parser(
        "rtdetr-osnet",
        parents=[common],
        help="RT-DETRv2 detection with OSNet identity tracking.",
    )
    rtdetr.add_argument(
        "--detector-model",
        choices=("r18", "r50"),
        default="r18",
        help="RT-DETRv2 backbone (default: r18).",
    )

    botsort = subparsers.add_parser(
        "rfdetr-botsort",
        parents=[common],
        help="RF-DETR Large detection with BoT-SORT tracking.",
    )
    botsort.add_argument(
        "--cmc",
        action=argparse.BooleanOptionalAction,
        help="Camera-motion compensation (default: on only for all frames).",
    )
    return parser


def _print_progress(result: RunResult) -> None:
    if result.processed_frames == 0:
        print(
            f"Sampling {result.effective_sample_fps:.2f} FPS "
            f"(every {result.sample_interval} source frame(s)); "
            f"detector batch size {result.batch_size}; FP16 {result.use_fp16}"
        )
        if result.camera_motion_compensation is not None:
            print(
                "Camera-motion compensation: "
                f"{result.camera_motion_compensation}"
            )
        return

    progress_total = (
        f"/{result.total_sampled_frames}"
        if result.total_sampled_frames > 0
        else ""
    )
    print(
        f"\rProcessed {result.processed_frames}{progress_total} sampled frames",
        end="",
        flush=True,
    )


def _print_summary(result: RunResult, paths: OutputPaths) -> None:
    if result.processed_frames > 0:
        print()
    if result.ended_early:
        print(
            "Warning: video decoding stopped after "
            f"{result.source_frames_read}/{result.total_source_frames} "
            "source frames",
            file=sys.stderr,
        )
    processing_fps = (
        result.processed_frames / result.processing_seconds
        if result.processing_seconds
        else 0.0
    )
    print(
        f"Processing time: {result.processing_seconds:.1f}s "
        f"({processing_fps:.2f} sampled FPS)"
    )
    print(
        "Process ended cleanly. Total distinct individuals: "
        f"{len(result.telemetry)}"
    )
    print(f"Person telemetry saved to: {paths.telemetry}")
    if paths.line_counts is not None:
        print(
            f"Line crossings: {result.line_in_count} in, "
            f"{result.line_out_count} out"
        )
        print(f"Line counts saved to: {paths.line_counts}")


def _execute(
    config: ConfigT,
    runner: Callable[[ConfigT], RunResult],
    paths: OutputPaths,
) -> RunResult:
    try:
        result = runner(config)
    except BaseException as processing_error:
        try:
            write_run_result(paths, config.result)
            if config.result.initialized:
                print(
                    f"Partial person telemetry saved to: {paths.telemetry}",
                    file=sys.stderr,
                )
                if paths.line_counts is not None:
                    print(
                        f"Partial line counts saved to: {paths.line_counts}",
                        file=sys.stderr,
                    )
        except Exception as output_error:
            note = (
                "Additionally failed to persist partial results: "
                f"{output_error}"
            )
            if hasattr(processing_error, "add_note"):
                processing_error.add_note(note)
            else:
                print(note, file=sys.stderr)
        raise
    write_run_result(paths, result)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        parser.error(f"Could not create output directory: {error}")

    spec = PIPELINES[args.pipeline]
    device = resolve_device(args.device)
    batch_size = args.batch_size or (
        spec.gpu_batch_size if args.device == "gpu" else 1
    )
    use_fp16 = args.device == "gpu" and not args.no_fp16
    result = RunResult()
    common_config_kwargs: dict[str, Any] = {
        "video": args.video,
        "device_variant": args.device,
        "device": device,
        "batch_size": batch_size,
        "sample_fps": args.sample_fps,
        "detection_threshold": args.detection_threshold,
        "use_fp16": use_fp16,
        "line": tuple(args.line) if args.line is not None else None,
        "result": result,
        "progress_callback": _print_progress,
    }

    config = spec.build_config(args, common_config_kwargs)
    paths = generate_output_paths(
        args.video,
        spec.output_suffix,
        args.device,
        args.line is not None,
        args.output_dir,
    )
    print(f"Running {args.device.upper()} variant on: {device}")
    print(f"Loading detector: {spec.detector_label(args)}")
    result = _execute(config, spec.load_runner(), paths)

    _print_summary(result, paths)
    return 0


def _legacy_main(subcommand: str) -> NoReturn:
    raise SystemExit(main([subcommand, *sys.argv[1:]]))


def rtdetr_osnet_main() -> NoReturn:
    _legacy_main("rtdetr-osnet")


def rfdetr_botsort_main() -> NoReturn:
    _legacy_main("rfdetr-botsort")


if __name__ == "__main__":
    raise SystemExit(main())
