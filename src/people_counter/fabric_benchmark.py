"""Pure contracts and evaluation logic for the Candidate A capacity benchmark."""

from __future__ import annotations

import hashlib
import json
import math
import random
import re
import statistics
import subprocess
import uuid
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


WORKSPACE_ID = "c31ee864-230d-4005-8fd5-7c7130ebf774"
LAKEHOUSE_ID = "883cff91-eaa8-40be-870f-6e9716303cb2"
ENVIRONMENT_ID = "3e580f48-9ff7-4bc6-af2e-a59158029ada"
TABLE_PREFIX = "pc_ca_benchmark_v1_"
FILES_ROOT = "Files/_benchmark/people-counter/candidate-a/v1/"
FABRIC_RUNTIME = "2.0"
MEASUREMENT_SECONDS = 6 * 60 * 60
THROUGHPUT_TARGET_X = 416.67
CAPACITY_HEADROOM = 1.20
REQUIRED_CONCURRENT_WORK = 5
BOOTSTRAP_SEED = 20_261_004
MIN_BOOTSTRAP_SAMPLES = 8
_SHA256 = re.compile(r"[0-9a-f]{64}")


class BenchmarkValidationError(ValueError):
    """A benchmark contract or identity is invalid."""


class IdentityMismatchError(BenchmarkValidationError):
    """Observed immutable identity differs from its registered identity."""


class InsufficientSamplesError(BenchmarkValidationError):
    """There are too few interval observations for a confidence bound."""


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def object_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _require_sha256(name: str, value: str) -> str:
    if _SHA256.fullmatch(value) is None:
        raise BenchmarkValidationError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _require_finite_positive(name: str, value: float) -> float:
    if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
        raise BenchmarkValidationError(f"{name} must be finite and positive")
    return value


def _safe_relative_path(relative: str) -> str:
    parts = relative.split("/")
    if (
        not relative
        or relative.startswith("/")
        or "\\" in relative
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise BenchmarkValidationError(f"unsafe benchmark relative path {relative!r}")
    return relative


@dataclass(frozen=True)
class CpuInferenceProfile:
    """Immutable identity for one semantically labelled CPU experiment."""

    profile_id: str
    model_format: str
    detector_input_pixels: int
    detector_batch_size: int
    sample_fps: float
    intra_op_threads: int
    inter_op_threads: int
    opencv_threads: int
    spark_task_cpus: int
    videos_per_partition: int
    required_output_fps: float = 1.0
    artifact_sha256: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if re.fullmatch(r"[a-z0-9][a-z0-9-]*", self.profile_id) is None:
            raise BenchmarkValidationError("CPU profile_id is not canonical")
        if self.model_format not in {"pytorch", "onnx"}:
            raise BenchmarkValidationError("CPU profile model_format is unsupported")
        for name in (
            "detector_input_pixels",
            "detector_batch_size",
            "intra_op_threads",
            "inter_op_threads",
            "opencv_threads",
            "spark_task_cpus",
            "videos_per_partition",
        ):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise BenchmarkValidationError(f"{name} must be a positive integer")
        _require_finite_positive("sample_fps", self.sample_fps)
        _require_finite_positive("required_output_fps", self.required_output_fps)
        if self.sample_fps < self.required_output_fps:
            raise BenchmarkValidationError(
                "CPU profile silently reduces the required temporal sampling"
            )
        paths = [path for path, _ in self.artifact_sha256]
        if len(paths) != len(set(paths)):
            raise BenchmarkValidationError("CPU profile artifact paths must be unique")
        for path, digest in self.artifact_sha256:
            _safe_relative_path(path)
            _require_sha256("artifact_sha256", digest)
        if self.model_format == "onnx" and not self.artifact_sha256:
            raise BenchmarkValidationError(
                "ONNX CPU profiles require immutable artifact hashes"
            )

    @property
    def equivalence_label(self) -> str:
        if math.isclose(
            self.sample_fps,
            self.required_output_fps,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            return f"required-{self.required_output_fps:g}fps-equivalent"
        return (
            f"higher-sampling-{self.sample_fps:g}fps-"
            f"not-cost-equivalent-to-{self.required_output_fps:g}fps"
        )

    @property
    def sha256(self) -> str:
        return object_sha256(asdict(self))


@dataclass(frozen=True)
class ConcurrentWorkObservation:
    """One successful work result from a real concurrent pilot."""

    work_id: str
    executor_identity: str
    source_seconds: float
    processing_seconds: float
    startup_seconds: float = 0.0

    def __post_init__(self) -> None:
        if not self.work_id or not self.executor_identity:
            raise BenchmarkValidationError(
                "concurrent observation identities are required"
            )
        _require_finite_positive("source_seconds", self.source_seconds)
        _require_finite_positive("processing_seconds", self.processing_seconds)
        if (
            isinstance(self.startup_seconds, bool)
            or not math.isfinite(self.startup_seconds)
            or self.startup_seconds < 0
        ):
            raise BenchmarkValidationError(
                "startup_seconds must be finite and nonnegative"
            )


@dataclass(frozen=True)
class ConcurrentPilotMeasurement:
    """Measured five-work aggregate; never extrapolates missing workers."""

    observations: tuple[ConcurrentWorkObservation, ...]
    wall_seconds: float
    physical_source_diversity: int

    def __post_init__(self) -> None:
        _require_finite_positive("wall_seconds", self.wall_seconds)
        if len(self.observations) != REQUIRED_CONCURRENT_WORK:
            raise BenchmarkValidationError(
                "pilot requires exactly five completed concurrent work items"
            )
        work_ids = {item.work_id for item in self.observations}
        if len(work_ids) != REQUIRED_CONCURRENT_WORK:
            raise BenchmarkValidationError(
                "pilot work IDs must be unique"
            )
        if (
            type(self.physical_source_diversity) is not int
            or self.physical_source_diversity < 1
            or self.physical_source_diversity > len(self.observations)
        ):
            raise BenchmarkValidationError(
                "physical_source_diversity is invalid"
            )

    @property
    def actual_executor_count(self) -> int:
        return len({item.executor_identity for item in self.observations})

    @property
    def aggregate_throughput_x(self) -> float:
        return (
            sum(item.source_seconds for item in self.observations)
            / self.wall_seconds
        )

    @property
    def mean_per_work_throughput_x(self) -> float:
        return statistics.fmean(
            item.source_seconds / item.processing_seconds
            for item in self.observations
        )


def required_f64_equivalent_capacities(
    measured_aggregate_throughput_x: float,
    *,
    target_throughput_x: float = THROUGHPUT_TARGET_X,
    headroom: float = CAPACITY_HEADROOM,
) -> int:
    """Return whole measured F64-equivalent capacities required at headroom."""

    measured = _require_finite_positive(
        "measured_aggregate_throughput_x",
        measured_aggregate_throughput_x,
    )
    target = _require_finite_positive("target_throughput_x", target_throughput_x)
    margin = _require_finite_positive("headroom", headroom)
    if margin < 1:
        raise BenchmarkValidationError("headroom must be at least one")
    return math.ceil(target * margin / measured)


@dataclass(frozen=True)
class FabricBenchmarkConfig:
    """Fixed, isolated Fabric namespace for the approval benchmark."""

    workspace_id: str = WORKSPACE_ID
    lakehouse_id: str = LAKEHOUSE_ID
    environment_id: str = ENVIRONMENT_ID
    table_prefix: str = TABLE_PREFIX
    files_root: str = FILES_ROOT
    runtime: str = FABRIC_RUNTIME

    def __post_init__(self) -> None:
        expected = (
            ("workspace_id", WORKSPACE_ID),
            ("lakehouse_id", LAKEHOUSE_ID),
            ("environment_id", ENVIRONMENT_ID),
            ("table_prefix", TABLE_PREFIX),
            ("files_root", FILES_ROOT),
            ("runtime", FABRIC_RUNTIME),
        )
        mismatches = {
            name: (getattr(self, name), required)
            for name, required in expected
            if getattr(self, name) != required
        }
        if mismatches:
            raise BenchmarkValidationError(
                f"benchmark configuration is fixed; mismatches={mismatches!r}"
            )
        for value in (self.workspace_id, self.lakehouse_id, self.environment_id):
            uuid.UUID(value)

    def table(self, suffix: str) -> str:
        if re.fullmatch(r"[a-z0-9_]+", suffix) is None:
            raise BenchmarkValidationError(f"invalid benchmark table suffix {suffix!r}")
        return f"{self.table_prefix}{suffix}"

    def file_path(self, relative: str) -> str:
        return f"{self.files_root}{_safe_relative_path(relative)}"

    def abfss_path(self, path: str) -> str:
        if (
            not path.startswith(self.files_root)
            or "\\" in path
            or "://" in path
            or ".." in path.split("/")
        ):
            raise BenchmarkValidationError(
                f"path must remain under the fixed benchmark root: {path!r}"
            )
        return (
            f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{self.lakehouse_id}/{path}"
        )


@dataclass(frozen=True)
class MediaProbe:
    """Immutable identity and probe result for one physical source object."""

    source_id: str
    source_path: str
    source_sha256: str
    byte_size: int
    frame_count: int
    fps_numerator: int
    fps_denominator: int
    duration_seconds: float
    codec: str
    width: int
    height: int
    camera_id: str

    def __post_init__(self) -> None:
        if not self.source_id or not self.source_id.isascii():
            raise BenchmarkValidationError("source_id must be non-empty ASCII")
        if not self.source_path.startswith("Files/"):
            raise BenchmarkValidationError("source_path must be an immutable Files path")
        if "\\" in self.source_path or ".." in self.source_path.split("/"):
            raise BenchmarkValidationError("source_path is unsafe")
        _require_sha256("source_sha256", self.source_sha256)
        for name in ("byte_size", "frame_count", "fps_numerator", "fps_denominator"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise BenchmarkValidationError(f"{name} must be a positive integer")
        for name in ("width", "height"):
            if getattr(self, name) <= 0:
                raise BenchmarkValidationError(f"{name} must be positive")
        _require_finite_positive("duration_seconds", self.duration_seconds)
        if not self.codec or not self.camera_id:
            raise BenchmarkValidationError("codec and camera_id are required")
        frame_duration = self.fps_denominator / self.fps_numerator
        derived = self.frame_count * frame_duration
        if abs(derived - self.duration_seconds) > frame_duration + 1e-9:
            raise BenchmarkValidationError(
                "duration_seconds differs from frame_count/fps by more than one frame"
            )

    @property
    def fps(self) -> float:
        return self.fps_numerator / self.fps_denominator

    @property
    def identity_sha256(self) -> str:
        return object_sha256(asdict(self))

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MediaProbe":
        return cls(**{name: value[name] for name in cls.__dataclass_fields__})


def reject_probe_mismatch(expected: MediaProbe, observed: MediaProbe) -> None:
    """Reject any probe, content, path, or source identity drift."""

    differences = {
        name: (getattr(expected, name), getattr(observed, name))
        for name in expected.__dataclass_fields__
        if getattr(expected, name) != getattr(observed, name)
    }
    if differences:
        raise IdentityMismatchError(f"immutable media probe mismatch: {differences!r}")


@dataclass(frozen=True)
class MediaProbeObservation:
    """One independent decoder's immutable media observations."""

    frame_count: int
    fps: float
    duration_seconds: float
    codec: str
    width: int
    height: int

    def __post_init__(self) -> None:
        for name in ("frame_count", "width", "height"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise BenchmarkValidationError(f"{name} must be a positive integer")
        _require_finite_positive("fps", self.fps)
        _require_finite_positive("duration_seconds", self.duration_seconds)
        if not self.codec:
            raise BenchmarkValidationError("codec is required")


def verified_media_probe(
    *,
    source_id: str,
    source_path: str,
    source_sha256: str,
    byte_size: int,
    camera_id: str,
    opencv: MediaProbeObservation,
    ffprobe: MediaProbeObservation,
) -> MediaProbe:
    """Cross-check OpenCV and ffprobe observations before creating a manifest row."""

    frame_tolerance = max(1, math.ceil(max(opencv.fps, ffprobe.fps)))
    duration_tolerance = max(1 / opencv.fps, 1 / ffprobe.fps)
    if abs(opencv.frame_count - ffprobe.frame_count) > frame_tolerance:
        raise IdentityMismatchError("OpenCV and ffprobe frame counts disagree")
    if not math.isclose(opencv.fps, ffprobe.fps, rel_tol=1e-4, abs_tol=1e-4):
        raise IdentityMismatchError("OpenCV and ffprobe FPS disagree")
    if abs(opencv.duration_seconds - ffprobe.duration_seconds) > duration_tolerance:
        raise IdentityMismatchError("OpenCV and ffprobe durations disagree")
    if (opencv.width, opencv.height) != (ffprobe.width, ffprobe.height):
        raise IdentityMismatchError("OpenCV and ffprobe dimensions disagree")
    fps_denominator = 1_000_000
    fps_numerator = round(ffprobe.fps * fps_denominator)
    divisor = math.gcd(fps_numerator, fps_denominator)
    fps_numerator //= divisor
    fps_denominator //= divisor
    duration = ffprobe.frame_count * fps_denominator / fps_numerator
    return MediaProbe(
        source_id=source_id,
        source_path=source_path,
        source_sha256=source_sha256,
        byte_size=byte_size,
        frame_count=ffprobe.frame_count,
        fps_numerator=fps_numerator,
        fps_denominator=fps_denominator,
        duration_seconds=duration,
        codec=ffprobe.codec,
        width=ffprobe.width,
        height=ffprobe.height,
        camera_id=camera_id,
    )


def probe_media_file(
    local_path: str | Path,
    *,
    source_id: str,
    source_path: str,
    camera_id: str,
) -> MediaProbe:
    """Probe a local immutable copy with both OpenCV and ffprobe."""

    path = Path(local_path)
    if not path.is_file() or path.is_symlink():
        raise BenchmarkValidationError("media probe input must be a regular file")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)

    try:
        import cv2
    except ImportError as error:  # pragma: no cover - project dependency
        raise BenchmarkValidationError("OpenCV is required for media probing") from error
    capture = cv2.VideoCapture(str(path))
    try:
        if not capture.isOpened():
            raise BenchmarkValidationError("OpenCV could not open media")
        cv_frames = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
        cv_fps = float(capture.get(cv2.CAP_PROP_FPS))
        if cv_frames <= 0 or not math.isfinite(cv_fps) or cv_fps <= 0:
            raise BenchmarkValidationError(
                "OpenCV returned invalid frame count or FPS"
            )
        opencv = MediaProbeObservation(
            frame_count=cv_frames,
            fps=cv_fps,
            duration_seconds=cv_frames / cv_fps,
            codec="opencv",
            width=int(capture.get(cv2.CAP_PROP_FRAME_WIDTH)),
            height=int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT)),
        )
    finally:
        capture.release()

    command = (
        "ffprobe",
        "-v",
        "error",
        "-count_frames",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=codec_name,width,height,avg_frame_rate,nb_read_frames:format=duration",
        "-of",
        "json",
        str(path),
    )
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
        payload = json.loads(completed.stdout)
        stream = payload["streams"][0]
        numerator, denominator = (
            int(value) for value in str(stream["avg_frame_rate"]).split("/", 1)
        )
        ffprobe = MediaProbeObservation(
            frame_count=int(stream["nb_read_frames"]),
            fps=numerator / denominator,
            duration_seconds=float(payload["format"]["duration"]),
            codec=str(stream["codec_name"]),
            width=int(stream["width"]),
            height=int(stream["height"]),
        )
    except (
        FileNotFoundError,
        subprocess.CalledProcessError,
        KeyError,
        IndexError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        raise BenchmarkValidationError("ffprobe media verification failed") from error
    return verified_media_probe(
        source_id=source_id,
        source_path=source_path,
        source_sha256=digest.hexdigest(),
        byte_size=path.stat().st_size,
        camera_id=camera_id,
        opencv=opencv,
        ffprobe=ffprobe,
    )


@dataclass(frozen=True)
class ArtifactIdentity:
    artifact_id: str
    path: str
    sha256: str

    def __post_init__(self) -> None:
        if not self.artifact_id or not self.path:
            raise BenchmarkValidationError("artifact_id and path are required")
        _require_sha256("artifact sha256", self.sha256)

    def to_dict(self) -> dict[str, str]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ArtifactIdentity":
        return cls(
            artifact_id=str(value["artifact_id"]),
            path=str(value["path"]),
            sha256=str(value["sha256"]),
        )


@dataclass(frozen=True)
class WorkloadItem:
    logical_work_id: str
    source_id: str
    repetition_index: int

    def __post_init__(self) -> None:
        if re.fullmatch(r"pcbm-[0-9a-f]{32}", self.logical_work_id) is None:
            raise BenchmarkValidationError("invalid logical_work_id")
        if not self.source_id or self.repetition_index < 0:
            raise BenchmarkValidationError("invalid logical workload reference")

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class WorkloadStats:
    logical_items: int
    unique_sources: int
    unique_cameras: int
    unique_codecs: int
    physical_bytes: int
    logical_source_hours: float
    unique_source_hours: float
    target_source_hours: float
    target_overshoot_hours: float
    minimum_repetitions: int
    maximum_repetitions: int
    repetition_counts: tuple[tuple[str, int], ...]

    def to_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["repetition_counts"] = dict(self.repetition_counts)
        return value


def _logical_work_id(
    workload_identity: Mapping[str, object], source_id: str, repetition_index: int
) -> str:
    digest = object_sha256(
        {
            "workload_identity": workload_identity,
            "source_id": source_id,
            "repetition_index": repetition_index,
        }
    )
    return f"pcbm-{digest[:32]}"


@dataclass(frozen=True)
class WorkloadManifest:
    """Self-validating logical workload; media bytes remain single-copy."""

    schema_version: str
    target_source_hours: float
    sources: tuple[MediaProbe, ...]
    model: ArtifactIdentity
    config: ArtifactIdentity
    release: ArtifactIdentity
    items: tuple[WorkloadItem, ...]

    def __post_init__(self) -> None:
        if self.schema_version != "pc-ca-benchmark-workload-v1":
            raise BenchmarkValidationError("unsupported workload schema")
        _require_finite_positive("target_source_hours", self.target_source_hours)
        if not self.sources or not self.items:
            raise BenchmarkValidationError("workload sources and items are required")
        source_ids = [source.source_id for source in self.sources]
        if len(source_ids) != len(set(source_ids)):
            raise BenchmarkValidationError("source_id values must be unique")
        if tuple(source_ids) != tuple(sorted(source_ids)):
            raise BenchmarkValidationError("sources must be sorted by source_id")
        identity = self.workload_identity
        expected_indices: dict[str, int] = {source_id: 0 for source_id in source_ids}
        seen_work_ids: set[str] = set()
        source_map = {source.source_id: source for source in self.sources}
        logical_seconds = 0.0
        for item in self.items:
            if item.source_id not in source_map:
                raise BenchmarkValidationError("work item references an unknown source")
            if item.repetition_index != expected_indices[item.source_id]:
                raise BenchmarkValidationError(
                    "repetition indexes must be contiguous per source"
                )
            expected_indices[item.source_id] += 1
            expected_id = _logical_work_id(
                identity, item.source_id, item.repetition_index
            )
            if item.logical_work_id != expected_id:
                raise IdentityMismatchError("logical_work_id does not match identity")
            if item.logical_work_id in seen_work_ids:
                raise BenchmarkValidationError("logical_work_id values must be unique")
            seen_work_ids.add(item.logical_work_id)
            logical_seconds += source_map[item.source_id].duration_seconds
        if logical_seconds + 1e-9 < self.target_source_hours * 3600:
            raise BenchmarkValidationError("logical workload does not reach target size")

    @property
    def workload_identity(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "target_source_hours": self.target_source_hours,
            "source_identity_sha256": tuple(
                source.identity_sha256 for source in self.sources
            ),
            "model": self.model.to_dict(),
            "config": self.config.to_dict(),
            "release": self.release.to_dict(),
        }

    @property
    def sha256(self) -> str:
        return object_sha256(self.to_dict())

    @property
    def stats(self) -> WorkloadStats:
        repetitions = {source.source_id: 0 for source in self.sources}
        source_map = {source.source_id: source for source in self.sources}
        logical_seconds = 0.0
        for item in self.items:
            repetitions[item.source_id] += 1
            logical_seconds += source_map[item.source_id].duration_seconds
        counts = tuple(sorted(repetitions.items()))
        return WorkloadStats(
            logical_items=len(self.items),
            unique_sources=len(self.sources),
            unique_cameras=len({source.camera_id for source in self.sources}),
            unique_codecs=len({source.codec for source in self.sources}),
            physical_bytes=sum(source.byte_size for source in self.sources),
            logical_source_hours=logical_seconds / 3600,
            unique_source_hours=sum(
                source.duration_seconds for source in self.sources
            )
            / 3600,
            target_source_hours=self.target_source_hours,
            target_overshoot_hours=(
                logical_seconds / 3600 - self.target_source_hours
            ),
            minimum_repetitions=min(repetitions.values()),
            maximum_repetitions=max(repetitions.values()),
            repetition_counts=counts,
        )

    def source_for(self, logical_work_id: str) -> MediaProbe:
        work_sources = {
            item.logical_work_id: item.source_id for item in self.items
        }
        source_id = work_sources.get(logical_work_id)
        if source_id is None:
            raise BenchmarkValidationError(f"unknown logical work {logical_work_id!r}")
        sources = {source.source_id: source for source in self.sources}
        return sources[source_id]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "target_source_hours": self.target_source_hours,
            "sources": [source.to_dict() for source in self.sources],
            "model": self.model.to_dict(),
            "config": self.config.to_dict(),
            "release": self.release.to_dict(),
            "items": [item.to_dict() for item in self.items],
            "storage_mode": "logical-references-only",
        }

    def to_json(self) -> str:
        return canonical_json_bytes(self.to_dict()).decode("utf-8")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkloadManifest":
        if value.get("storage_mode") != "logical-references-only":
            raise BenchmarkValidationError("workload must not contain repeated media bytes")
        return cls(
            schema_version=str(value["schema_version"]),
            target_source_hours=float(value["target_source_hours"]),
            sources=tuple(MediaProbe.from_dict(item) for item in value["sources"]),
            model=ArtifactIdentity.from_dict(value["model"]),
            config=ArtifactIdentity.from_dict(value["config"]),
            release=ArtifactIdentity.from_dict(value["release"]),
            items=tuple(WorkloadItem(**item) for item in value["items"]),
        )

    @classmethod
    def from_json(cls, value: str | bytes) -> "WorkloadManifest":
        parsed = json.loads(value)
        if not isinstance(parsed, Mapping):
            raise BenchmarkValidationError("workload JSON must be an object")
        return cls.from_dict(parsed)


def build_workload_manifest(
    sources: Iterable[MediaProbe],
    *,
    model: ArtifactIdentity,
    config: ArtifactIdentity,
    release: ArtifactIdentity,
    target_source_hours: float,
) -> WorkloadManifest:
    """Size a deterministic round-robin workload using logical references."""

    target = _require_finite_positive("target_source_hours", target_source_hours)
    ordered = tuple(sorted(sources, key=lambda source: source.source_id))
    if not ordered:
        raise BenchmarkValidationError("at least one source is required")
    identity = {
        "schema_version": "pc-ca-benchmark-workload-v1",
        "target_source_hours": target,
        "source_identity_sha256": tuple(source.identity_sha256 for source in ordered),
        "model": model.to_dict(),
        "config": config.to_dict(),
        "release": release.to_dict(),
    }
    repetitions = {source.source_id: 0 for source in ordered}
    items: list[WorkloadItem] = []
    logical_seconds = 0.0
    target_seconds = target * 3600
    while logical_seconds + 1e-9 < target_seconds:
        for source in ordered:
            index = repetitions[source.source_id]
            items.append(
                WorkloadItem(
                    logical_work_id=_logical_work_id(
                        identity, source.source_id, index
                    ),
                    source_id=source.source_id,
                    repetition_index=index,
                )
            )
            repetitions[source.source_id] += 1
            logical_seconds += source.duration_seconds
            if logical_seconds + 1e-9 >= target_seconds:
                break
    return WorkloadManifest(
        schema_version="pc-ca-benchmark-workload-v1",
        target_source_hours=target,
        sources=ordered,
        model=model,
        config=config,
        release=release,
        items=tuple(items),
    )


@dataclass(frozen=True)
class MeasurementConfig:
    warmup_seconds: int = 15 * 60
    measurement_seconds: int = MEASUREMENT_SECONDS
    drain_seconds: int = 30 * 60
    interval_seconds: int = 5 * 60
    bootstrap_resamples: int = 2_000
    bootstrap_seed: int = BOOTSTRAP_SEED
    bootstrap_block_length: int | None = None
    require_fresh_application: bool = True
    forbid_stitching: bool = True

    def __post_init__(self) -> None:
        if self.measurement_seconds != MEASUREMENT_SECONDS:
            raise BenchmarkValidationError("measurement window must be exactly six hours")
        for name in (
            "warmup_seconds",
            "drain_seconds",
            "interval_seconds",
            "bootstrap_resamples",
        ):
            if getattr(self, name) <= 0:
                raise BenchmarkValidationError(f"{name} must be positive")
        if self.measurement_seconds % self.interval_seconds:
            raise BenchmarkValidationError(
                "interval_seconds must divide the six-hour window exactly"
            )
        samples = self.measurement_seconds // self.interval_seconds
        if samples < MIN_BOOTSTRAP_SAMPLES:
            raise InsufficientSamplesError(
                f"measurement configuration provides only {samples} intervals"
            )
        if self.bootstrap_resamples < 100:
            raise InsufficientSamplesError("at least 100 bootstrap resamples are required")
        if self.bootstrap_block_length is not None and not (
            1 <= self.bootstrap_block_length <= samples // 2
        ):
            raise BenchmarkValidationError("invalid bootstrap_block_length")
        if not self.require_fresh_application or not self.forbid_stitching:
            raise BenchmarkValidationError(
                "approval measurement requires restart and forbids stitching"
            )

    @property
    def sha256(self) -> str:
        return object_sha256(asdict(self))


@dataclass(frozen=True)
class MeasurementRun:
    run_id: str
    spark_application_id: str
    spark_session_id: str
    restart_token: str
    workload_sha256: str
    model_sha256: str
    config_sha256: str
    release_sha256: str
    definition_sha256: str
    measurement_config_sha256: str
    warmup_started_at: float
    measured_started_at: float
    measured_ended_at: float
    drain_ended_at: float
    segment_hashes: tuple[str, ...]

    def __post_init__(self) -> None:
        for name in (
            "run_id",
            "spark_application_id",
            "spark_session_id",
            "restart_token",
        ):
            if not getattr(self, name):
                raise BenchmarkValidationError(f"{name} is required")
        for name in (
            "workload_sha256",
            "model_sha256",
            "config_sha256",
            "release_sha256",
            "definition_sha256",
            "measurement_config_sha256",
        ):
            _require_sha256(name, getattr(self, name))
        if len(self.segment_hashes) != 1:
            raise BenchmarkValidationError(
                "measurement cannot stitch multiple run segments"
            )
        _require_sha256("segment_hash", self.segment_hashes[0])
        for name in (
            "warmup_started_at",
            "measured_started_at",
            "measured_ended_at",
            "drain_ended_at",
        ):
            if not math.isfinite(getattr(self, name)):
                raise BenchmarkValidationError(f"{name} must be finite")

    @property
    def identity_sha256(self) -> str:
        return object_sha256(asdict(self))


def measurement_segment_sha256(
    *,
    run_id: str,
    spark_application_id: str,
    spark_session_id: str,
    restart_token: str,
    measured_started_at: float,
    measured_ended_at: float,
) -> str:
    return object_sha256(
        {
            "run_id": run_id,
            "spark_application_id": spark_application_id,
            "spark_session_id": spark_session_id,
            "restart_token": restart_token,
            "measured_started_at": measured_started_at,
            "measured_ended_at": measured_ended_at,
        }
    )


def validate_measurement_run(
    run: MeasurementRun,
    measurement: MeasurementConfig,
    manifest: WorkloadManifest,
    *,
    expected_definition_sha256: str,
    prior_application_id: str,
) -> None:
    """Validate warm-up, exact window, drain, restart, and all no-stitch hashes."""

    expected = {
        "workload_sha256": manifest.sha256,
        "model_sha256": manifest.model.sha256,
        "config_sha256": manifest.config.sha256,
        "release_sha256": manifest.release.sha256,
        "definition_sha256": _require_sha256(
            "expected_definition_sha256", expected_definition_sha256
        ),
        "measurement_config_sha256": measurement.sha256,
    }
    mismatches = {
        name: (getattr(run, name), value)
        for name, value in expected.items()
        if getattr(run, name) != value
    }
    if mismatches:
        raise IdentityMismatchError(f"measurement identity mismatch: {mismatches!r}")
    if run.spark_application_id == prior_application_id:
        raise BenchmarkValidationError(
            "benchmark setting requires a fresh Spark application restart"
        )
    warmup = run.measured_started_at - run.warmup_started_at
    elapsed = run.measured_ended_at - run.measured_started_at
    drain = run.drain_ended_at - run.measured_ended_at
    if warmup < measurement.warmup_seconds:
        raise BenchmarkValidationError("warm-up interval is too short")
    if elapsed != measurement.measurement_seconds:
        raise BenchmarkValidationError("measured interval is not exactly six hours")
    if drain < 0 or drain > measurement.drain_seconds:
        raise BenchmarkValidationError("drain interval is invalid")
    expected_segment = measurement_segment_sha256(
        run_id=run.run_id,
        spark_application_id=run.spark_application_id,
        spark_session_id=run.spark_session_id,
        restart_token=run.restart_token,
        measured_started_at=run.measured_started_at,
        measured_ended_at=run.measured_ended_at,
    )
    if run.segment_hashes != (expected_segment,):
        raise IdentityMismatchError("measurement segment hash mismatch")


@dataclass(frozen=True)
class WorkAttemptTelemetry:
    logical_work_id: str
    source_id: str
    attempt_id: str
    started_at: float
    completed_at: float
    status: str
    committed: bool
    processed_frames: int
    partial_output_visible: bool = False
    error_category: str | None = None

    def __post_init__(self) -> None:
        if not all((self.logical_work_id, self.source_id, self.attempt_id)):
            raise BenchmarkValidationError("attempt identifiers are required")
        if self.status not in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            raise BenchmarkValidationError("invalid attempt status")
        if (
            not math.isfinite(self.started_at)
            or not math.isfinite(self.completed_at)
            or self.completed_at < self.started_at
        ):
            raise BenchmarkValidationError("invalid attempt timestamps")
        if self.processed_frames < 0:
            raise BenchmarkValidationError("processed_frames cannot be negative")
        if self.committed and self.status != "SUCCEEDED":
            raise BenchmarkValidationError("only a successful attempt can be committed")


class TelemetryStage(str, Enum):
    WORK = "work"
    ATTEMPT = "attempt"
    BATCH = "batch"
    PARTITION = "partition"
    EXECUTOR = "executor"
    MODEL_CACHE = "model_cache"
    DECODE = "decode"
    INFERENCE = "inference"
    STAGE = "stage"
    COMMIT = "commit"
    RECONCILE = "reconcile"
    PUBLICATION = "publication"
    GOLD_LAG = "gold_lag"


@dataclass(frozen=True)
class TelemetryEvent:
    stage: TelemetryStage
    recorded_at: float
    duration_seconds: float
    logical_work_id: str | None = None
    attempt_id: str | None = None
    batch_id: str | None = None
    partition_id: str | None = None
    executor_id: str | None = None
    succeeded: bool = True
    retry: bool = False
    lease_lost: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.stage, TelemetryStage):
            raise BenchmarkValidationError("telemetry stage is not allowlisted")
        if not math.isfinite(self.recorded_at):
            raise BenchmarkValidationError("telemetry timestamp must be finite")
        if not math.isfinite(self.duration_seconds) or self.duration_seconds < 0:
            raise BenchmarkValidationError(
                "telemetry duration must be finite and nonnegative"
            )


@dataclass(frozen=True)
class TelemetryAggregate:
    stage_counts: tuple[tuple[str, int], ...]
    stage_duration_seconds: tuple[tuple[str, float], ...]
    retries: int
    failures: int
    lease_losses: int

    def to_dict(self) -> dict[str, object]:
        return {
            "stage_counts": dict(self.stage_counts),
            "stage_duration_seconds": dict(self.stage_duration_seconds),
            "retries": self.retries,
            "failures": self.failures,
            "lease_losses": self.lease_losses,
        }


def aggregate_telemetry(events: Iterable[TelemetryEvent]) -> TelemetryAggregate:
    counts = {stage.value: 0 for stage in TelemetryStage}
    durations = {stage.value: 0.0 for stage in TelemetryStage}
    retries = failures = lease_losses = 0
    for event in events:
        if not isinstance(event, TelemetryEvent):
            raise BenchmarkValidationError(
                "telemetry contains a non-TelemetryEvent value"
            )
        counts[event.stage.value] += 1
        durations[event.stage.value] += event.duration_seconds
        retries += int(event.retry)
        failures += int(not event.succeeded)
        lease_losses += int(event.lease_lost)
    return TelemetryAggregate(
        tuple(sorted(counts.items())),
        tuple(sorted(durations.items())),
        retries,
        failures,
        lease_losses,
    )


@dataclass(frozen=True)
class IntervalTelemetry:
    index: int
    started_at: float
    ended_at: float
    committed_logical_items: int
    committed_source_seconds: float
    throughput_x: float


@dataclass(frozen=True)
class ResourceTelemetry:
    planned_concurrency: int
    peak_concurrency: int
    work_available_for_peak: bool
    longest_partition_seconds: float
    median_partition_seconds: float
    inference_wall_seconds: float
    idle_tail_seconds: float
    first_hour_p95_rss_bytes: int
    final_hour_p95_rss_bytes: int
    homogeneous_groups: int
    model_loads: int
    model_cache_hits: int


@dataclass(frozen=True)
class ReliabilityTelemetry:
    correctness_failures: int
    executor_ooms: int
    python_worker_crashes: int
    missing_terminal_results: int
    visible_partial_outputs: int
    queue_failures: int
    publication_failures: int
    resource: ResourceTelemetry
    committed_pointer_read_failures: int = 0
    unsealed_committed_pointers: int = 0
    duplicate_logical_identities: int = 0
    duplicate_publications: int = 0
    stale_fences: int = 0
    missing_outputs: int = 0
    uncommitted_gold_visible: int = 0
    critical_reconciliation_findings: int = 0
    retry_attempts: int = 0
    failed_attempts: int = 0
    attempted_logical_items: int = 0
    max_retry_rate: float = 0.05
    max_failure_rate: float = 0.01
    missing_telemetry_stages: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in (
            "correctness_failures",
            "executor_ooms",
            "python_worker_crashes",
            "missing_terminal_results",
            "visible_partial_outputs",
            "queue_failures",
            "publication_failures",
            "committed_pointer_read_failures",
            "unsealed_committed_pointers",
            "duplicate_logical_identities",
            "duplicate_publications",
            "stale_fences",
            "missing_outputs",
            "uncommitted_gold_visible",
            "critical_reconciliation_findings",
            "retry_attempts",
            "failed_attempts",
            "attempted_logical_items",
        ):
            if getattr(self, name) < 0:
                raise BenchmarkValidationError(f"{name} cannot be negative")
        for name in ("max_retry_rate", "max_failure_rate"):
            value = getattr(self, name)
            if not math.isfinite(value) or not 0 <= value <= 1:
                raise BenchmarkValidationError(f"{name} must be between zero and one")
        object.__setattr__(
            self, "missing_telemetry_stages", tuple(self.missing_telemetry_stages)
        )


@dataclass(frozen=True)
class GateResult:
    name: str
    passed: bool
    observed: object
    requirement: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def evaluate_reliability_gates(value: ReliabilityTelemetry) -> tuple[GateResult, ...]:
    failures = (
        value.correctness_failures
        + value.executor_ooms
        + value.python_worker_crashes
        + value.missing_terminal_results
        + value.visible_partial_outputs
        + value.queue_failures
        + value.publication_failures
    )
    resource = value.resource
    imbalance = (
        None
        if resource.median_partition_seconds <= 0
        else resource.longest_partition_seconds / resource.median_partition_seconds
    )
    idle_ratio = (
        None
        if resource.inference_wall_seconds <= 0
        else resource.idle_tail_seconds / resource.inference_wall_seconds
    )
    concurrency_ratio = (
        None
        if not resource.work_available_for_peak
        or resource.planned_concurrency <= 0
        else resource.peak_concurrency / resource.planned_concurrency
    )
    rss_growth = (
        None
        if resource.first_hour_p95_rss_bytes <= 0
        else (
            resource.final_hour_p95_rss_bytes
            / resource.first_hour_p95_rss_bytes
            - 1
        )
    )
    cache_expected = resource.homogeneous_groups * 3
    pointer_failures = (
        value.committed_pointer_read_failures
        + value.unsealed_committed_pointers
    )
    identity_failures = (
        value.duplicate_logical_identities + value.duplicate_publications
    )
    visibility_failures = (
        value.stale_fences
        + value.missing_outputs
        + value.uncommitted_gold_visible
        + value.critical_reconciliation_findings
    )
    retry_rate = (
        (0.0 if value.retry_attempts == 0 else None)
        if value.attempted_logical_items <= 0
        else value.retry_attempts / value.attempted_logical_items
    )
    failure_rate = (
        (0.0 if value.failed_attempts == 0 else None)
        if value.attempted_logical_items <= 0
        else value.failed_attempts / value.attempted_logical_items
    )
    return (
        GateResult("zero-failures", failures == 0, failures, "== 0"),
        GateResult(
            "committed-pointer-readable-and-sealed",
            pointer_failures == 0,
            pointer_failures,
            "== 0",
        ),
        GateResult(
            "unique-logical-identity-and-publication",
            identity_failures == 0,
            identity_failures,
            "== 0",
        ),
        GateResult(
            "gold-and-reconciliation-visibility",
            visibility_failures == 0,
            visibility_failures,
            "== 0",
        ),
        GateResult(
            "retry-threshold",
            retry_rate is not None and retry_rate <= value.max_retry_rate,
            retry_rate,
            f"<= {value.max_retry_rate}",
        ),
        GateResult(
            "failure-threshold",
            failure_rate is not None and failure_rate <= value.max_failure_rate,
            failure_rate,
            f"<= {value.max_failure_rate}",
        ),
        GateResult(
            "observability-complete",
            not value.missing_telemetry_stages,
            list(value.missing_telemetry_stages),
            "all declared telemetry stages present",
        ),
        GateResult(
            "partition-balance",
            imbalance is not None and imbalance <= 1.30,
            imbalance,
            "<= 1.30",
        ),
        GateResult(
            "idle-tail",
            idle_ratio is not None and idle_ratio <= 0.20,
            idle_ratio,
            "<= 0.20",
        ),
        GateResult(
            "observed-concurrency",
            not resource.work_available_for_peak
            or (
                concurrency_ratio is not None
                and concurrency_ratio >= 0.90
            ),
            concurrency_ratio,
            ">= 0.90 while sufficient work remains",
        ),
        GateResult(
            "model-cache",
            resource.model_loads == resource.homogeneous_groups
            and resource.model_cache_hits >= cache_expected,
            {
                "groups": resource.homogeneous_groups,
                "loads": resource.model_loads,
                "hits": resource.model_cache_hits,
            },
            "one load and at least three hits per homogeneous group",
        ),
        GateResult(
            "rss-stability",
            rss_growth is not None and rss_growth <= 0.10,
            rss_growth,
            "<= 0.10",
        ),
    )


def lag_one_autocorrelation(values: Sequence[float]) -> float:
    if len(values) < 2:
        raise InsufficientSamplesError("at least two samples are required")
    mean = statistics.fmean(values)
    denominator = sum((value - mean) ** 2 for value in values)
    if denominator == 0:
        return 0.0
    numerator = sum(
        (values[index] - mean) * (values[index - 1] - mean)
        for index in range(1, len(values))
    )
    return max(-1.0, min(1.0, numerator / denominator))


def select_block_length(values: Sequence[float]) -> int:
    if len(values) < MIN_BOOTSTRAP_SAMPLES:
        raise InsufficientSamplesError(
            f"need at least {MIN_BOOTSTRAP_SAMPLES} interval samples"
        )
    rho = max(0.0, min(0.95, lag_one_autocorrelation(values)))
    inflation = ((1 + rho) / (1 - rho)) ** (2 / 3)
    selected = math.ceil(len(values) ** (1 / 3) * inflation)
    return max(1, min(selected, len(values) // 2))


def moving_block_bootstrap_lcb(
    values: Sequence[float],
    *,
    seed: int = BOOTSTRAP_SEED,
    block_length: int | None = None,
    resamples: int = 2_000,
    confidence: float = 0.95,
) -> tuple[float, int]:
    """Return a deterministic circular moving-block bootstrap mean LCB."""

    samples = tuple(float(value) for value in values)
    if len(samples) < MIN_BOOTSTRAP_SAMPLES:
        raise InsufficientSamplesError(
            f"need at least {MIN_BOOTSTRAP_SAMPLES} interval samples"
        )
    if any(not math.isfinite(value) or value < 0 for value in samples):
        raise BenchmarkValidationError("throughput samples must be finite and nonnegative")
    if resamples < 100:
        raise InsufficientSamplesError("at least 100 bootstrap resamples are required")
    if not 0.5 < confidence < 1:
        raise BenchmarkValidationError("confidence must be between 0.5 and 1")
    selected = select_block_length(samples) if block_length is None else block_length
    if not 1 <= selected <= len(samples) // 2:
        raise BenchmarkValidationError("block_length is outside the valid range")
    randomizer = random.Random(seed)
    block_count = math.ceil(len(samples) / selected)
    means: list[float] = []
    for _ in range(resamples):
        resampled: list[float] = []
        for _ in range(block_count):
            start = randomizer.randrange(len(samples))
            resampled.extend(
                samples[(start + offset) % len(samples)]
                for offset in range(selected)
            )
        means.append(statistics.fmean(resampled[: len(samples)]))
    means.sort()
    index = max(0, math.ceil((1 - confidence) * resamples) - 1)
    return means[index], selected


@dataclass(frozen=True)
class StatisticsReport:
    measured_seconds: float
    committed_logical_items: int
    retry_attempts: int
    logical_successful_source_hours: float
    unique_successful_source_hours: float
    aggregate_throughput_x: float
    interval_throughput_x: tuple[float, ...]
    lag_one_autocorrelation: float
    bootstrap_seed: int
    bootstrap_resamples: int
    bootstrap_block_length: int
    throughput_lcb_95_x: float

    def to_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["interval_throughput_x"] = list(self.interval_throughput_x)
        return value


def _dedupe_committed_attempts(
    attempts: Sequence[WorkAttemptTelemetry],
    manifest: WorkloadManifest,
) -> tuple[tuple[WorkAttemptTelemetry, ...], int]:
    known = {item.logical_work_id: item.source_id for item in manifest.items}
    by_work: dict[str, list[WorkAttemptTelemetry]] = {}
    seen_attempts: set[str] = set()
    for attempt in attempts:
        if attempt.attempt_id in seen_attempts:
            raise BenchmarkValidationError("attempt_id values must be unique")
        seen_attempts.add(attempt.attempt_id)
        if known.get(attempt.logical_work_id) != attempt.source_id:
            raise IdentityMismatchError("attempt work/source identity mismatch")
        by_work.setdefault(attempt.logical_work_id, []).append(attempt)
    committed: list[WorkAttemptTelemetry] = []
    for logical_work_id, work_attempts in by_work.items():
        winners = [
            attempt
            for attempt in work_attempts
            if attempt.committed and attempt.status == "SUCCEEDED"
        ]
        if len(winners) > 1:
            raise BenchmarkValidationError(
                f"multiple committed attempts for {logical_work_id}"
            )
        committed.extend(winners)
    retries = sum(max(0, len(work_attempts) - 1) for work_attempts in by_work.values())
    return tuple(committed), retries


def calculate_statistics(
    manifest: WorkloadManifest,
    run: MeasurementRun,
    measurement: MeasurementConfig,
    attempts: Sequence[WorkAttemptTelemetry],
) -> tuple[StatisticsReport, tuple[IntervalTelemetry, ...]]:
    committed, retries = _dedupe_committed_attempts(attempts, manifest)
    measured = [
        attempt
        for attempt in committed
        if run.measured_started_at
        <= attempt.completed_at
        < run.measured_ended_at
    ]
    source_by_id = {source.source_id: source for source in manifest.sources}
    logical_seconds = sum(
        source_by_id[attempt.source_id].duration_seconds for attempt in measured
    )
    unique_source_ids = {attempt.source_id for attempt in measured}
    unique_seconds = sum(
        source_by_id[source_id].duration_seconds for source_id in unique_source_ids
    )
    interval_count = measurement.measurement_seconds // measurement.interval_seconds
    interval_seconds = [0.0] * interval_count
    interval_items = [0] * interval_count
    for attempt in measured:
        index = int(
            (attempt.completed_at - run.measured_started_at)
            // measurement.interval_seconds
        )
        interval_seconds[index] += source_by_id[attempt.source_id].duration_seconds
        interval_items[index] += 1
    throughput = tuple(
        source_seconds / measurement.interval_seconds
        for source_seconds in interval_seconds
    )
    lcb, block = moving_block_bootstrap_lcb(
        throughput,
        seed=measurement.bootstrap_seed,
        block_length=measurement.bootstrap_block_length,
        resamples=measurement.bootstrap_resamples,
    )
    intervals = tuple(
        IntervalTelemetry(
            index=index,
            started_at=run.measured_started_at
            + index * measurement.interval_seconds,
            ended_at=run.measured_started_at
            + (index + 1) * measurement.interval_seconds,
            committed_logical_items=interval_items[index],
            committed_source_seconds=interval_seconds[index],
            throughput_x=throughput[index],
        )
        for index in range(interval_count)
    )
    return (
        StatisticsReport(
            measured_seconds=measurement.measurement_seconds,
            committed_logical_items=len(measured),
            retry_attempts=retries,
            logical_successful_source_hours=logical_seconds / 3600,
            unique_successful_source_hours=unique_seconds / 3600,
            aggregate_throughput_x=logical_seconds / measurement.measurement_seconds,
            interval_throughput_x=throughput,
            lag_one_autocorrelation=lag_one_autocorrelation(throughput),
            bootstrap_seed=measurement.bootstrap_seed,
            bootstrap_resamples=measurement.bootstrap_resamples,
            bootstrap_block_length=block,
            throughput_lcb_95_x=lcb,
        ),
        intervals,
    )


@dataclass(frozen=True)
class CostUsage:
    capacity_units: float
    capacity_elapsed_hours: float
    average_storage_gb: float
    storage_retention_hours: float
    egress_gb: float
    executor_runtime_hours: float = 0.0
    executor_core_hours: float = 0.0
    startup_hours: float = 0.0
    steady_state_hours: float = 0.0
    drain_hours: float = 0.0
    successful_source_hours: float = 0.0
    storage_read_bytes: int = 0
    storage_write_bytes: int = 0
    storage_transactions: int = 0

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise BenchmarkValidationError(f"{name} must be finite and nonnegative")


@dataclass(frozen=True)
class CostRates:
    capacity_usd_per_cu_hour: float | None = None
    storage_usd_per_gb_month: float | None = None
    egress_usd_per_gb: float | None = None
    currency: str = "USD"
    source: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "capacity_usd_per_cu_hour",
            "storage_usd_per_gb_month",
            "egress_usd_per_gb",
        ):
            value = getattr(self, name)
            if value is not None and (not math.isfinite(value) or value < 0):
                raise BenchmarkValidationError(f"{name} must be nonnegative or unknown")
        if self.currency != "USD":
            raise BenchmarkValidationError("v1 cost accounting supports USD only")


@dataclass(frozen=True)
class CostReport:
    capacity_unit_hours: float
    storage_gb_months: float
    egress_gb: float
    component_cost_usd: tuple[tuple[str, float | None], ...]
    known_subtotal_usd: float
    total_cost_usd: float | None
    unknown_rates: tuple[str, ...]
    formulas: tuple[tuple[str, str], ...]
    rate_source: str | None
    executor_runtime_hours: float
    executor_core_hours: float
    phase_hours: tuple[tuple[str, float], ...]
    storage_counters: tuple[tuple[str, int], ...]
    cost_per_1000_source_hours_usd: float | None
    projected_200000_source_hours_usd: float | None

    def to_dict(self) -> dict[str, object]:
        return {
            "usage": {
                "capacity_unit_hours": self.capacity_unit_hours,
                "storage_gb_months": self.storage_gb_months,
                "egress_gb": self.egress_gb,
            },
            "component_cost_usd": dict(self.component_cost_usd),
            "known_subtotal_usd": self.known_subtotal_usd,
            "total_cost_usd": self.total_cost_usd,
            "unknown_rates": list(self.unknown_rates),
            "formulas": dict(self.formulas),
            "rate_source": self.rate_source,
            "executor_runtime_hours": self.executor_runtime_hours,
            "executor_core_hours": self.executor_core_hours,
            "phase_hours": dict(self.phase_hours),
            "storage_counters": dict(self.storage_counters),
            "cost_per_1000_source_hours_usd": self.cost_per_1000_source_hours_usd,
            "projected_200000_source_hours_usd": (
                self.projected_200000_source_hours_usd
            ),
        }


def calculate_cost(usage: CostUsage, rates: CostRates) -> CostReport:
    cu_hours = usage.capacity_units * usage.capacity_elapsed_hours
    gb_months = usage.average_storage_gb * usage.storage_retention_hours / (
        24 * 365.25 / 12
    )
    quantities = {
        "capacity": (cu_hours, rates.capacity_usd_per_cu_hour),
        "storage": (gb_months, rates.storage_usd_per_gb_month),
        "egress": (usage.egress_gb, rates.egress_usd_per_gb),
    }
    components = tuple(
        (name, None if rate is None else quantity * rate)
        for name, (quantity, rate) in quantities.items()
    )
    unknown = tuple(
        name
        for name, (quantity, rate) in quantities.items()
        if quantity > 0 and rate is None
    )
    known = sum(value for _, value in components if value is not None)
    total = None if unknown else known
    normalized = (
        None
        if total is None or usage.successful_source_hours <= 0
        else total * 1_000 / usage.successful_source_hours
    )
    return CostReport(
        capacity_unit_hours=cu_hours,
        storage_gb_months=gb_months,
        egress_gb=usage.egress_gb,
        component_cost_usd=components,
        known_subtotal_usd=known,
        total_cost_usd=total,
        unknown_rates=unknown,
        formulas=(
            ("capacity", "capacity_units * capacity_elapsed_hours * usd_per_cu_hour"),
            (
                "storage",
                "average_storage_gb * retention_hours / 730.5 * usd_per_gb_month",
            ),
            ("egress", "egress_gb * usd_per_gb"),
        ),
        rate_source=rates.source,
        executor_runtime_hours=usage.executor_runtime_hours,
        executor_core_hours=usage.executor_core_hours,
        phase_hours=(
            ("drain", usage.drain_hours),
            ("startup", usage.startup_hours),
            ("steady_state", usage.steady_state_hours),
        ),
        storage_counters=(
            ("read_bytes", usage.storage_read_bytes),
            ("transactions", usage.storage_transactions),
            ("write_bytes", usage.storage_write_bytes),
        ),
        cost_per_1000_source_hours_usd=normalized,
        projected_200000_source_hours_usd=(
            None if normalized is None else normalized * 200
        ),
    )


@dataclass(frozen=True)
class BenchmarkReport:
    schema_version: str
    status: str
    manifest_sha256: str
    measurement_run_sha256: str
    workload: Mapping[str, object]
    measurement: Mapping[str, object]
    statistics: Mapping[str, object]
    reliability: Mapping[str, object]
    cost: Mapping[str, object]
    gates: tuple[GateResult, ...]

    def __post_init__(self) -> None:
        if self.schema_version != "pc-ca-benchmark-report-v1":
            raise BenchmarkValidationError("unsupported report schema")
        expected = "PASS" if all(gate.passed for gate in self.gates) else "FAIL"
        if self.status != expected:
            raise BenchmarkValidationError("report status disagrees with gates")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "manifest_sha256": self.manifest_sha256,
            "measurement_run_sha256": self.measurement_run_sha256,
            "workload": dict(self.workload),
            "measurement": dict(self.measurement),
            "statistics": dict(self.statistics),
            "reliability": dict(self.reliability),
            "cost": dict(self.cost),
            "gates": [gate.to_dict() for gate in self.gates],
        }

    def to_json(self) -> str:
        return canonical_json_bytes(self.to_dict()).decode("utf-8")


def build_benchmark_report(
    manifest: WorkloadManifest,
    run: MeasurementRun,
    measurement: MeasurementConfig,
    attempts: Sequence[WorkAttemptTelemetry],
    reliability: ReliabilityTelemetry,
    usage: CostUsage,
    rates: CostRates,
    *,
    expected_definition_sha256: str,
    prior_application_id: str,
) -> BenchmarkReport:
    validate_measurement_run(
        run,
        measurement,
        manifest,
        expected_definition_sha256=expected_definition_sha256,
        prior_application_id=prior_application_id,
    )
    stats, _ = calculate_statistics(manifest, run, measurement, attempts)
    gates = (
        GateResult(
            "throughput-lcb",
            stats.throughput_lcb_95_x >= THROUGHPUT_TARGET_X,
            stats.throughput_lcb_95_x,
            f">= {THROUGHPUT_TARGET_X}",
        ),
        *evaluate_reliability_gates(reliability),
    )
    cost = calculate_cost(usage, rates)
    return BenchmarkReport(
        schema_version="pc-ca-benchmark-report-v1",
        status="PASS" if all(gate.passed for gate in gates) else "FAIL",
        manifest_sha256=manifest.sha256,
        measurement_run_sha256=run.identity_sha256,
        workload=manifest.stats.to_dict(),
        measurement={
            "warmup_seconds": measurement.warmup_seconds,
            "measured_seconds": measurement.measurement_seconds,
            "drain_seconds": run.drain_ended_at - run.measured_ended_at,
            "measurement_config_sha256": measurement.sha256,
            "segment_hashes": list(run.segment_hashes),
            "spark_application_id": run.spark_application_id,
            "spark_session_id": run.spark_session_id,
            "restart_token": run.restart_token,
        },
        statistics=stats.to_dict(),
        reliability={
            **asdict(reliability),
            "gates_passed": all(
                gate.passed for gate in evaluate_reliability_gates(reliability)
            ),
        },
        cost=cost.to_dict(),
        gates=gates,
    )
