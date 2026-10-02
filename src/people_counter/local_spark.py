"""Manifest-driven local Spark execution with immutable Delta staging."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import os
import platform
import socket
import subprocess
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from people_counter.config import RFDetrBotsortConfig, RTDetrOsnetConfig
from people_counter.fabric_executor_partition import (
    ExecutorPartitionRecord,
    SdkRuntimeProcessor,
    process_video_partition,
)
from people_counter.models import RunResult


ProcessorMode = Literal["probe", "sdk"]
MANIFEST_VERSION = 1
MAX_VIDEOS_PER_PARTITION = 2


class ManifestValidationError(ValueError):
    """The immutable claimed-work manifest is malformed or changed."""


class StagingValidationError(RuntimeError):
    """Attempt-scoped staging is incomplete or internally inconsistent."""


@dataclass(frozen=True)
class BatchStaging:
    batch_id: str
    batch_attempt_id: str
    manifest_sha256: str
    path: Path
    record_count: int
    executor_identities: tuple[str, ...]
    release_digests: tuple[str, ...]
    failed_work_ids: tuple[str, ...]


class ProbeRuntimeProcessor:
    """Deterministic no-model processor used by the approved smoke path."""

    def __init__(self, delay_seconds: float) -> None:
        self._delay_seconds = delay_seconds

    def run(self, config: RTDetrOsnetConfig | RFDetrBotsortConfig) -> RunResult:
        if self._delay_seconds:
            time.sleep(self._delay_seconds)
        result = config.result
        result.started = True
        result.initialized = True
        result.fps = 30.0
        result.total_source_frames = 1
        result.total_sampled_frames = 1
        result.source_frames_read = 1
        result.processed_frames = 1
        result.effective_sample_fps = 1.0
        result.processing_seconds = self._delay_seconds
        return result


def read_claimed_manifest(
    path: Path,
    *,
    expected_sha256: str | None = None,
) -> tuple[dict[str, Any], str]:
    content = path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    if expected_sha256 is not None and digest != expected_sha256:
        raise ManifestValidationError(
            f"manifest SHA-256 mismatch: expected {expected_sha256}, got {digest}"
        )
    try:
        manifest = json.loads(content)
    except json.JSONDecodeError as error:
        raise ManifestValidationError(f"manifest is not valid JSON: {error}") from error
    if not isinstance(manifest, dict):
        raise ManifestValidationError("manifest root must be an object")
    _validate_manifest(manifest)
    return manifest, digest


def process_claimed_batch(
    spark_session: Any,
    manifest_path: Path,
    staging_root: Path,
    *,
    expected_manifest_sha256: str | None = None,
    processor_mode: ProcessorMode = "probe",
    minimum_executor_identities: int = 1,
) -> BatchStaging:
    """Distribute whole videos and write one immutable attempt-scoped Delta table."""
    if processor_mode not in {"probe", "sdk"}:
        raise ValueError(f"unsupported processor mode: {processor_mode!r}")
    if minimum_executor_identities < 1:
        raise ValueError("minimum_executor_identities must be at least one")
    manifest, manifest_sha256 = read_claimed_manifest(
        manifest_path,
        expected_sha256=expected_manifest_sha256,
    )
    items = manifest["items"]
    destination = attempt_staging_path(
        staging_root,
        manifest["batch_id"],
        manifest["batch_attempt_id"],
    )
    expected_staging_path = Path(
        _required_text(manifest, "expected_staging_path")
    ).expanduser().resolve()
    if expected_staging_path != destination:
        raise ManifestValidationError(
            "manifest expected_staging_path does not match its batch attempt"
        )
    staged_items = [
        {
            **item,
            "batch_id": manifest["batch_id"],
            "batch_attempt_id": manifest["batch_attempt_id"],
            "manifest_sha256": manifest_sha256,
            "expected_staging_path": str(destination),
        }
        for item in items
    ]
    partition_count = _bounded_partition_count(
        len(items),
        minimum_executor_identities,
    )
    rows = spark_session.sparkContext.parallelize(
        staged_items,
        partition_count,
    ).mapPartitions(lambda partition: _process_partition(partition, processor_mode))
    dataframe = spark_session.createDataFrame(rows, schema=local_staging_schema())
    (
        dataframe.write.format("delta")
        .mode("errorifexists")
        .option("txnAppId", f"people-counter-local:{manifest_sha256}")
        .option("txnVersion", "0")
        .save(str(destination))
    )
    return validate_delta_staging(
        spark_session,
        destination,
        items,
        batch_id=manifest["batch_id"],
        batch_attempt_id=manifest["batch_attempt_id"],
        manifest_sha256=manifest_sha256,
        minimum_executor_identities=minimum_executor_identities,
    )


def validate_delta_staging(
    spark_session: Any,
    path: Path,
    items: Sequence[Mapping[str, Any]],
    *,
    batch_id: str,
    batch_attempt_id: str,
    manifest_sha256: str,
    minimum_executor_identities: int = 1,
) -> BatchStaging:
    rows = (
        spark_session.read.format("delta")
        .load(str(path))
        .select(
            "batch_id",
            "batch_attempt_id",
            "manifest_sha256",
            "expected_staging_path",
            "work_id",
            "attempt_id",
            "record_type",
            "status",
            "executor_id",
            "executor_host",
            "executor_identity",
            "release_digest",
        )
        .collect()
    )
    expected = {
        _required_text(item, "work_id"): _required_text(item, "attempt_id")
        for item in items
    }
    if len(expected) != len(items):
        raise StagingValidationError("manifest contains duplicate work identities")
    expected_path = str(path.expanduser().resolve())
    terminal_count = {work_id: 0 for work_id in expected}
    failures: set[str] = set()
    identities: set[str] = set()
    release_digests: set[str] = set()
    for row in rows:
        work_id = _staged_text(row, "work_id")
        if work_id not in expected:
            raise StagingValidationError(f"unexpected staged work_id: {work_id!r}")
        _verify_staged_identity(
            row,
            expected_attempt_id=expected[work_id],
            batch_id=batch_id,
            batch_attempt_id=batch_attempt_id,
            manifest_sha256=manifest_sha256,
            expected_path=expected_path,
        )
        record_type = _staged_text(row, "record_type")
        if record_type in {"video_result", "error"}:
            terminal_count[work_id] += 1
            status = _staged_text(row, "status")
            if record_type == "error":
                if status != "FAILED":
                    raise StagingValidationError(
                        f"error terminal for {work_id!r} must be FAILED"
                    )
                failures.add(work_id)
            elif status != "SUCCEEDED":
                raise StagingValidationError(
                    f"video_result terminal for {work_id!r} must be SUCCEEDED"
                )
            identities.add(_verified_executor_identity(row))
            release_digests.add(_staged_text(row, "release_digest"))
    invalid = sorted(key for key, count in terminal_count.items() if count != 1)
    if invalid:
        raise StagingValidationError(
            f"expected exactly one terminal record for work: {invalid!r}"
        )
    if len(identities) < minimum_executor_identities:
        raise StagingValidationError(
            f"expected at least {minimum_executor_identities} executor identities, "
            f"observed {sorted(identities)!r}"
        )
    return BatchStaging(
        batch_id=batch_id,
        batch_attempt_id=batch_attempt_id,
        manifest_sha256=manifest_sha256,
        path=path,
        record_count=len(rows),
        executor_identities=tuple(sorted(identities)),
        release_digests=tuple(sorted(release_digests)),
        failed_work_ids=tuple(sorted(failures)),
    )


def local_staging_schema() -> Any:
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("batch_id", T.StringType(), nullable=False),
            T.StructField("batch_attempt_id", T.StringType(), nullable=False),
            T.StructField("manifest_sha256", T.StringType(), nullable=False),
            T.StructField("expected_staging_path", T.StringType(), nullable=False),
            T.StructField("record_type", T.StringType(), nullable=False),
            T.StructField("work_id", T.StringType(), nullable=False),
            T.StructField("attempt_id", T.StringType(), nullable=False),
            T.StructField("source_video", T.StringType(), nullable=False),
            T.StructField("status", T.StringType(), nullable=False),
            T.StructField("payload_json", T.StringType(), nullable=False),
            T.StructField("error_type", T.StringType(), nullable=True),
            T.StructField("error_message", T.StringType(), nullable=True),
            T.StructField("retryable", T.BooleanType(), nullable=True),
            T.StructField("processed_frames", T.LongType(), nullable=True),
            T.StructField("processing_seconds", T.DoubleType(), nullable=True),
            T.StructField("emitted_at_utc", T.TimestampType(), nullable=False),
            T.StructField("executor_id", T.StringType(), nullable=False),
            T.StructField("executor_host", T.StringType(), nullable=False),
            T.StructField("executor_identity", T.StringType(), nullable=False),
            T.StructField("python_version", T.StringType(), nullable=False),
            T.StructField("release_digest", T.StringType(), nullable=False),
        ]
    )


def create_local_spark_session(
    *,
    master: str,
    app_name: str,
    correlation_id: str,
) -> Any:
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config(
            "spark.driver.host",
            os.environ.get("SPARK_DRIVER_HOST", socket.gethostname()),
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.peopleCounter.correlationId", correlation_id)
        .getOrCreate()
    )


def runtime_identity(spark_session: Any) -> dict[str, str]:
    jvm = spark_session.sparkContext._jvm
    try:
        scala_version = str(jvm.scala.util.Properties.versionNumberString())
    except Exception:
        scala_version = "unavailable"
    try:
        java_version = str(jvm.java.lang.System.getProperty("java.version"))
    except Exception:
        java_version = _java_version()
    try:
        delta_version = importlib.metadata.version("delta-spark")
    except importlib.metadata.PackageNotFoundError:
        delta_version = "unavailable"
    return {
        "python": platform.python_version(),
        "java": java_version,
        "spark": str(spark_session.version),
        "scala": scala_version,
        "delta": delta_version,
        "image_digest": os.environ.get("PEOPLE_COUNTER_IMAGE_DIGEST", "unknown"),
        "release_digest": os.environ.get(
            "PEOPLE_COUNTER_RELEASE_DIGEST",
            "development",
        ),
    }


def _process_partition(
    rows: Iterable[Mapping[str, Any]],
    processor_mode: ProcessorMode,
) -> Iterator[dict[str, Any]]:
    host = socket.gethostname()
    executor_id = os.environ.get("SPARK_EXECUTOR_ID", host)
    identity = f"{executor_id}@{host}"
    materialized = [dict(row) for row in rows]
    if not materialized:
        return
    delay = max(float(row.get("probe_delay_seconds", 0)) for row in materialized)
    processor = (
        ProbeRuntimeProcessor(delay)
        if processor_mode == "probe"
        else SdkRuntimeProcessor()
    )
    for record in process_video_partition(
        materialized,
        config_builder=_config_from_work,
        processor=processor,
    ):
        yield {
            **record,
            "batch_id": _required_text(materialized[0], "batch_id"),
            "batch_attempt_id": _required_text(
                materialized[0],
                "batch_attempt_id",
            ),
            "manifest_sha256": _required_text(
                materialized[0],
                "manifest_sha256",
            ),
            "expected_staging_path": _required_text(
                materialized[0],
                "expected_staging_path",
            ),
            "executor_id": executor_id,
            "executor_host": host,
            "executor_identity": identity,
            "python_version": platform.python_version(),
            "release_digest": os.environ.get(
                "PEOPLE_COUNTER_RELEASE_DIGEST",
                "development",
            ),
        }


def _config_from_work(
    work: Mapping[str, Any],
) -> RTDetrOsnetConfig | RFDetrBotsortConfig:
    video = _verified_video(work)
    common: dict[str, Any] = {
        "video": video,
        "device_variant": "cpu",
        "device": "cpu",
        "batch_size": _positive_int(work.get("batch_size", 1), "batch_size"),
        "sample_fps": _optional_positive_float(work.get("sample_fps", 3.0)),
        "detection_threshold": float(work.get("detection_threshold", 0.6)),
        "use_fp16": False,
        "model_format": work.get("model_format", "pytorch"),
        "models_dir": (
            Path(work["models_dir"]) if work.get("models_dir") is not None else None
        ),
        "line": (
            tuple(work["line"]) if work.get("line") is not None else None
        ),
    }
    pipeline = work.get("pipeline", "rtdetr-osnet")
    if pipeline == "rtdetr-osnet":
        return RTDetrOsnetConfig(
            **common,
            detector_model=work.get("detector_model", "r18"),
        )
    if pipeline == "rfdetr-botsort":
        return RFDetrBotsortConfig(
            **common,
            camera_motion_compensation=work.get(
                "camera_motion_compensation"
            ),
        )
    raise ValueError(f"unsupported pipeline: {pipeline!r}")


def _validate_manifest(manifest: Mapping[str, Any]) -> None:
    if manifest.get("schema_version") != MANIFEST_VERSION:
        raise ManifestValidationError(
            f"schema_version must equal {MANIFEST_VERSION}"
        )
    _required_text(manifest, "batch_id")
    _required_text(manifest, "batch_attempt_id")
    _required_text(manifest, "correlation_id")
    _required_text(manifest, "expected_staging_path")
    items = manifest.get("items")
    if not isinstance(items, list) or not items:
        raise ManifestValidationError("items must be a non-empty list")
    identities: set[tuple[str, str]] = set()
    for item in items:
        if not isinstance(item, dict):
            raise ManifestValidationError("every item must be an object")
        identity = (
            _required_text(item, "work_id"),
            _required_text(item, "attempt_id"),
        )
        _required_text(item, "source_video")
        if identity in identities:
            raise ManifestValidationError(f"duplicate work attempt: {identity!r}")
        identities.add(identity)
        _require_primitive_tree(item)


def _require_primitive_tree(value: Any) -> None:
    if value is None or isinstance(value, (bool, int, float, str)):
        return
    if isinstance(value, list):
        for item in value:
            _require_primitive_tree(item)
        return
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        for item in value.values():
            _require_primitive_tree(item)
        return
    raise ManifestValidationError(
        f"manifest values must be JSON primitives, got {type(value).__name__}"
    )


def attempt_staging_path(root: Path, batch_id: str, attempt_id: str) -> Path:
    resolved_root = root.expanduser().resolve()
    relative = Path(_path_segment(batch_id)) / _path_segment(attempt_id)
    destination = (resolved_root / relative).resolve(strict=False)
    if resolved_root not in destination.parents:
        raise ValueError("staging path escapes its configured root")
    return destination


_attempt_staging_path = attempt_staging_path


def _bounded_partition_count(
    item_count: int,
    minimum_executor_identities: int,
) -> int:
    if item_count < 1:
        raise ValueError("item_count must be positive")
    if minimum_executor_identities < 1:
        raise ValueError("minimum_executor_identities must be at least one")
    if minimum_executor_identities > item_count:
        raise ValueError(
            "minimum_executor_identities cannot exceed the item count"
        )
    reuse_partition_count = math.ceil(item_count / MAX_VIDEOS_PER_PARTITION)
    return max(reuse_partition_count, minimum_executor_identities)


def _staged_text(row: Mapping[str, Any], name: str) -> str:
    value = row[name]
    if not isinstance(value, str) or not value.strip():
        raise StagingValidationError(
            f"staged {name} must be a non-empty string"
        )
    return value.strip()


def _verify_staged_identity(
    row: Mapping[str, Any],
    *,
    expected_attempt_id: str,
    batch_id: str,
    batch_attempt_id: str,
    manifest_sha256: str,
    expected_path: str,
) -> None:
    expected_values = {
        "attempt_id": expected_attempt_id,
        "batch_id": batch_id,
        "batch_attempt_id": batch_attempt_id,
        "manifest_sha256": manifest_sha256,
        "expected_staging_path": expected_path,
    }
    for name, expected in expected_values.items():
        actual = _staged_text(row, name)
        if actual != expected:
            raise StagingValidationError(
                f"staged {name} mismatch: expected {expected!r}, got {actual!r}"
            )


def _verified_executor_identity(row: Mapping[str, Any]) -> str:
    executor_id = _staged_text(row, "executor_id")
    executor_host = _staged_text(row, "executor_host")
    identity = _staged_text(row, "executor_identity")
    expected = f"{executor_id}@{executor_host}"
    if identity != expected:
        raise StagingValidationError(
            f"executor_identity mismatch: expected {expected!r}, got {identity!r}"
        )
    return identity


def _path_segment(value: str) -> str:
    text = value.strip()
    if not text or text in {".", ".."} or "/" in text or "\\" in text:
        raise ValueError(f"unsafe staging path segment: {value!r}")
    return text


def _required_text(value: Mapping[str, Any], name: str) -> str:
    candidate = value.get(name)
    if not isinstance(candidate, str) or not candidate.strip():
        raise ManifestValidationError(f"{name} must be a non-empty string")
    return candidate.strip()


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _optional_positive_float(value: object) -> float | None:
    if value is None:
        return None
    result = float(value)
    if result <= 0:
        raise ValueError("sample_fps must be greater than zero or null")
    return result


def _verified_video(work: Mapping[str, Any]) -> Path:
    video = Path(_required_text(work, "source_video"))
    expected = work.get("source_sha256")
    if expected is None:
        return video
    if not isinstance(expected, str) or len(expected) != 64:
        raise ValueError("source_sha256 must be a SHA-256 digest")
    digest = hashlib.sha256()
    with video.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    actual = digest.hexdigest()
    if actual != expected:
        raise ValueError(
            f"source SHA-256 mismatch for {video}: expected {expected}, got {actual}"
        )
    return video


def _java_version() -> str:
    try:
        completed = subprocess.run(
            ["java", "-version"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"
    output = completed.stderr or completed.stdout
    return output.splitlines()[0] if output else "unavailable"
