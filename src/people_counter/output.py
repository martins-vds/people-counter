"""Output path generation and CSV serialization."""

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from people_counter.models import (
    LineCountRecord,
    PersonTelemetry,
    RunResult,
)
from people_counter.video import format_video_timestamp


@dataclass(frozen=True)
class OutputPaths:
    telemetry: Path
    line_counts: Path | None


def generate_output_paths(
    video: Path,
    pipeline_suffix: str,
    device_variant: str,
    include_line_counts: bool,
    output_directory: Path = Path("outputs"),
    run_timestamp: str | None = None,
) -> OutputPaths:
    timestamp = run_timestamp or datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )
    suffix = f"{pipeline_suffix}_" if pipeline_suffix else ""
    return OutputPaths(
        telemetry=output_directory
        / (
            f"{video.stem}_telemetry_{suffix}"
            f"{device_variant}_{timestamp}.csv"
        ),
        line_counts=(
            output_directory
            / (
                f"{video.stem}_line_counts_{suffix}"
                f"{device_variant}_{timestamp}.csv"
            )
            if include_line_counts
            else None
        ),
    )


def write_telemetry(
    telemetry_path: Path,
    telemetry: dict[int, PersonTelemetry],
    fps: float,
) -> None:
    telemetry_path.parent.mkdir(parents=True, exist_ok=True)
    with telemetry_path.open("w", newline="", encoding="utf-8") as output:
        fieldnames = [
            "person_id",
            "entry_frame",
            "exit_frame",
            "entry_seconds",
            "exit_seconds",
            "entry_timestamp",
            "exit_timestamp",
            "duration_seconds",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for person_id in sorted(telemetry):
            record = telemetry[person_id]
            entry_seconds = record.entry_frame / fps
            exit_seconds = record.last_seen_frame / fps
            writer.writerow(
                {
                    "person_id": person_id,
                    "entry_frame": record.entry_frame,
                    "exit_frame": record.last_seen_frame,
                    "entry_seconds": f"{entry_seconds:.3f}",
                    "exit_seconds": f"{exit_seconds:.3f}",
                    "entry_timestamp": format_video_timestamp(
                        record.entry_frame, fps
                    ),
                    "exit_timestamp": format_video_timestamp(
                        record.last_seen_frame, fps
                    ),
                    "duration_seconds": (
                        f"{exit_seconds - entry_seconds:.3f}"
                    ),
                }
            )


def write_line_counts(
    line_counts_path: Path,
    line_count_records: list[LineCountRecord],
) -> None:
    line_counts_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(LineCountRecord.__dataclass_fields__)
    with line_counts_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(asdict(record) for record in line_count_records)


def write_run_result(paths: OutputPaths, result: RunResult) -> None:
    if not result.initialized:
        return
    write_telemetry(paths.telemetry, result.telemetry, result.fps)
    if paths.line_counts is not None:
        write_line_counts(paths.line_counts, result.line_counts)
