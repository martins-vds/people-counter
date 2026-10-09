from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import shutil
import sys
from dataclasses import asdict, dataclass, make_dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

import people_counter.fabric_benchmark as benchmark_module
import people_counter.fabric_benchmark_jobs as benchmark_jobs_module
import people_counter.fabric_candidate_a_jobs as candidate_jobs
import people_counter.sjd_control as sjd_control_module
import people_counter.sjd_process as sjd_process_module
from people_counter.fabric_candidate_a import (
    CandidateANamespaceMode,
    FabricCandidateAConfig,
)
from people_counter.fabric_benchmark import (
    ENVIRONMENT_ID,
    FILES_ROOT,
    LAKEHOUSE_ID,
    MEASUREMENT_SECONDS,
    TABLE_PREFIX,
    THROUGHPUT_TARGET_X,
    ArtifactIdentity,
    BenchmarkReport,
    BenchmarkValidationError,
    ConcurrentPilotMeasurement,
    ConcurrentWorkObservation,
    CostRates,
    CostUsage,
    CpuInferenceProfile,
    FabricBenchmarkConfig,
    GateResult,
    IdentityMismatchError,
    InsufficientSamplesError,
    MeasurementConfig,
    MeasurementRun,
    MediaProbeObservation,
    MediaProbe,
    ReliabilityTelemetry,
    ResourceTelemetry,
    TelemetryEvent,
    TelemetryStage,
    WorkAttemptTelemetry,
    WorkloadManifest,
    build_benchmark_report,
    build_event_log_section,
    build_workload_manifest,
    aggregate_telemetry,
    calculate_cost,
    calculate_statistics,
    evaluate_reliability_gates,
    measurement_segment_sha256,
    moving_block_bootstrap_lcb,
    probe_media_file,
    reject_probe_mismatch,
    required_f64_equivalent_capacities,
    verified_media_probe,
    select_block_length,
    validate_measurement_run,
)
from people_counter.fabric_benchmark_jobs import (
    CPU_PROFILES,
    _benchmark_execution_profile as _production_benchmark_execution_profile,
    _require_report_envelope,
    _require_report_gates,
    _resource_inventory_main,
    build_all_sjd_v2_definitions,
    build_sjd_v2_definition,
    control_main as benchmark_control_main,
    export_sjd_definitions,
    measurement_main,
    report_main,
    thin_main_source,
    workload_main,
)
from people_counter.fabric_spark_event_ingest import SparkEventLogError


class _FakeProperty:
    @staticmethod
    def getProperty(name: str) -> str:
        assert name == "java.version"
        return "21.0.12"


class _FakeJvm:
    class java:
        class lang:
            class System(_FakeProperty):
                pass


class _FakeSpark:
    version = "4.1.1.5"

    def __init__(self, **overrides: str) -> None:
        self.values = {
            "spark.dynamicAllocation.enabled": "false",
            "spark.speculation": "false",
            "spark.executor.instances": "5",
            "spark.executor.memory": "56g",
            **overrides,
        }
        self.conf = self
        self.sparkContext = type("Context", (), {"_jvm": _FakeJvm()})()

    def get(self, name: str, default: str | None = None) -> str | None:
        return self.values.get(name, default)


class _BareSpark:
    """Fake Spark session that sets only the keys explicitly given.

    Unlike ``_FakeSpark`` (which always pre-populates dynamicAllocation,
    speculation, and executor.instances), this fixture exercises the
    function's real *default* fallback values for keys Fabric's live conf
    does not happen to expose.
    """

    version = "4.1.1.5"

    def __init__(self, **values: str) -> None:
        self.values = dict(values)
        self.conf = self
        self.sparkContext = type("Context", (), {"_jvm": _FakeJvm()})()

    def get(self, name: str, default: str | None = None) -> str | None:
        return self.values.get(name, default)


def _probe(source_id: str, *, camera: str = "camera-a") -> MediaProbe:
    return MediaProbe(
        source_id=source_id,
        source_path=f"Files/input/{source_id}.mp4",
        source_sha256=(source_id[-1] * 64),
        byte_size=1_000_000,
        frame_count=3_600,
        fps_numerator=1,
        fps_denominator=1,
        duration_seconds=3_600.0,
        codec="h264",
        width=1920,
        height=1080,
        camera_id=camera,
    )


def _artifact(name: str, digest_character: str) -> ArtifactIdentity:
    return ArtifactIdentity(
        artifact_id=name,
        path=f"Files/models/{name}",
        sha256=digest_character * 64,
    )


def _manifest(target_hours: float = 4.0) -> WorkloadManifest:
    return build_workload_manifest(
        (_probe("source-a"), _probe("source-b", camera="camera-b")),
        model=_artifact("model", "c"),
        config=_artifact("config", "d"),
        release=_artifact("release", "e"),
        target_source_hours=target_hours,
    )


def _run(
    manifest: WorkloadManifest,
    measurement: MeasurementConfig,
    *,
    application_id: str = "application-2",
) -> MeasurementRun:
    measured_started_at = 1_900.0
    measured_ended_at = measured_started_at + MEASUREMENT_SECONDS
    segment = measurement_segment_sha256(
        run_id="run-1",
        spark_application_id=application_id,
        spark_session_id="session-1",
        restart_token="restart-1",
        measured_started_at=measured_started_at,
        measured_ended_at=measured_ended_at,
    )
    return MeasurementRun(
        run_id="run-1",
        spark_application_id=application_id,
        spark_session_id="session-1",
        restart_token="restart-1",
        workload_sha256=manifest.sha256,
        model_sha256=manifest.model.sha256,
        config_sha256=manifest.config.sha256,
        release_sha256=manifest.release.sha256,
        definition_sha256="f" * 64,
        measurement_config_sha256=measurement.sha256,
        warmup_started_at=1_000.0,
        measured_started_at=measured_started_at,
        measured_ended_at=measured_ended_at,
        drain_ended_at=measured_ended_at + 120,
        segment_hashes=(segment,),
    )


def _passing_reliability() -> ReliabilityTelemetry:
    return ReliabilityTelemetry(
        correctness_failures=0,
        executor_ooms=0,
        python_worker_crashes=0,
        missing_terminal_results=0,
        visible_partial_outputs=0,
        queue_failures=0,
        publication_failures=0,
        resource=ResourceTelemetry(
            planned_concurrency=10,
            peak_concurrency=9,
            work_available_for_peak=True,
            longest_partition_seconds=120,
            median_partition_seconds=100,
            inference_wall_seconds=1_000,
            idle_tail_seconds=100,
            first_hour_p95_rss_bytes=1_000,
            final_hour_p95_rss_bytes=1_090,
            homogeneous_groups=2,
            model_loads=2,
            model_cache_hits=6,
        ),
    )


def _passing_attempts(
    manifest: WorkloadManifest,
    run: MeasurementRun,
    measurement: MeasurementConfig,
) -> list[WorkAttemptTelemetry]:
    attempts = []
    items_per_interval = 35
    for index, item in enumerate(manifest.items):
        interval = index // items_per_interval
        completed = (
            run.measured_started_at + interval * measurement.interval_seconds + 10
        )
        attempts.append(
            WorkAttemptTelemetry(
                logical_work_id=item.logical_work_id,
                source_id=item.source_id,
                attempt_id=f"attempt-{index}",
                started_at=completed - 5,
                completed_at=completed,
                status="SUCCEEDED",
                committed=True,
                processed_frames=3_600,
            )
        )
    return attempts


def test_fixed_namespace_and_safe_paths() -> None:
    config = FabricBenchmarkConfig()
    assert config.table("attempts") == TABLE_PREFIX + "attempts"
    assert config.file_path("reports/final.json") == (
        FILES_ROOT + "reports/final.json"
    )
    assert config.abfss_path(config.file_path("telemetry")).endswith(
        f"/{LAKEHOUSE_ID}/{FILES_ROOT}telemetry"
    )
    with pytest.raises(BenchmarkValidationError, match="fixed"):
        FabricBenchmarkConfig(table_prefix="other_")
    with pytest.raises(BenchmarkValidationError, match="unsafe"):
        config.file_path("../production")
    with pytest.raises(BenchmarkValidationError, match="fixed benchmark root"):
        config.abfss_path("Files/production")


def test_cpu_profiles_are_hashed_cpu_only_and_equivalence_labelled() -> None:
    baseline = CPU_PROFILES["pytorch-r18-b1-1fps-1t"]
    optimized = CPU_PROFILES["onnx-r18-b4-1fps-1t"]
    assert baseline.equivalence_label == "required-1fps-equivalent"
    assert optimized.equivalence_label == "required-1fps-equivalent"
    assert optimized.sha256 != baseline.sha256
    common = {
        "rtdetr_osnet/rtdetr_v2_r18vd/config.json": (
            "ed051ec77cb41c5d9d5e3af21a979b1e890599dfd68434d7636ff82ded4c1527"
        ),
        "rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json": (
            "cd38cd59999e7a95d68e487fbe5132df3d4e5c32a0836add57e6126ba0c4eaf1"
        ),
    }
    assert dict(baseline.artifact_sha256) == {
        **common,
        "rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors": (
            "d18309d0d7ea57048138885c4c6ecfcb1e24506fc6153b94ad484f8ab62c7115"
        ),
        "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt": (
            "ce171fe160b3608f5e4c19489774991419be965b1d6f4bdccc4b4cfd2ef95347"
        ),
    }
    assert dict(optimized.artifact_sha256) == {
        **common,
        "rtdetr_osnet/rtdetr_v2_r18vd/model.onnx": (
            "425192fda18c29867123479a7a047cec0797d24a6a68ebbcad1faaa702c00ef3"
        ),
        "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.onnx": (
            "da73234e40324fe032847368dccc2aab7297407ac98f2463ff9395b09434b64a"
        ),
    }
    higher_sampling = replace(baseline, profile_id="pytorch-3fps", sample_fps=3)
    assert "not-cost-equivalent" in higher_sampling.equivalence_label
    with pytest.raises(BenchmarkValidationError, match="silently reduces"):
        replace(baseline, profile_id="invalid-half-fps", sample_fps=0.5)
    with pytest.raises(BenchmarkValidationError, match="artifact hashes"):
        CpuInferenceProfile(
            profile_id="onnx-no-artifact",
            model_format="onnx",
            detector_input_pixels=640,
            detector_batch_size=1,
            sample_fps=1,
            intra_op_threads=1,
            inter_op_threads=1,
            opencv_threads=1,
            spark_task_cpus=1,
            videos_per_partition=1,
        )


def test_five_work_measurement_and_required_capacity_use_actual_aggregate() -> None:
    observations = tuple(
        ConcurrentWorkObservation(
            work_id=f"work-{index}",
            executor_identity=f"executor-{index}",
            source_seconds=7,
            processing_seconds=8.612,
        )
        for index in range(5)
    )
    measured = ConcurrentPilotMeasurement(
        observations,
        wall_seconds=8.612,
        physical_source_diversity=1,
    )
    assert measured.actual_executor_count == 5
    assert measured.aggregate_throughput_x == pytest.approx(35 / 8.612)
    assert measured.mean_per_work_throughput_x == pytest.approx(7 / 8.612)
    assert required_f64_equivalent_capacities(
        measured.aggregate_throughput_x
    ) == math.ceil(416.67 * 1.2 / (35 / 8.612))
    with pytest.raises(BenchmarkValidationError, match="exactly 5"):
        ConcurrentPilotMeasurement(
            observations[:4],
            wall_seconds=8.612,
            physical_source_diversity=1,
        )


def test_concurrent_pilot_measurement_supports_dynamic_discovered_work_count() -> None:
    """New pilots may size the measurement to live-discovered slots, not a
    hard-coded five-executor assumption."""

    observations = tuple(
        ConcurrentWorkObservation(
            work_id=f"work-{index}",
            executor_identity=f"executor-{index}",
            source_seconds=7,
            processing_seconds=8.612,
        )
        for index in range(3)
    )
    measured = ConcurrentPilotMeasurement(
        observations,
        wall_seconds=8.612,
        physical_source_diversity=1,
        required_concurrent_work=3,
    )
    assert measured.actual_executor_count == 3
    assert measured.aggregate_throughput_x == pytest.approx(21 / 8.612)

    with pytest.raises(
        BenchmarkValidationError, match="exactly 3 completed concurrent work items"
    ):
        ConcurrentPilotMeasurement(
            observations[:2],
            wall_seconds=8.612,
            physical_source_diversity=1,
            required_concurrent_work=3,
        )

    with pytest.raises(
        BenchmarkValidationError, match="required_concurrent_work must be"
    ):
        ConcurrentPilotMeasurement(
            observations,
            wall_seconds=8.612,
            physical_source_diversity=1,
            required_concurrent_work=0,
        )


def test_concurrent_pilot_measurement_rejects_duplicate_work_ids_with_custom_width() -> None:
    """Duplicate work IDs must fail closed regardless of required_concurrent_work."""

    duplicated = tuple(
        ConcurrentWorkObservation(
            work_id="same-work-id",
            executor_identity=f"executor-{index}",
            source_seconds=7,
            processing_seconds=8.612,
        )
        for index in range(3)
    )
    with pytest.raises(BenchmarkValidationError) as error:
        ConcurrentPilotMeasurement(
            duplicated,
            wall_seconds=8.612,
            physical_source_diversity=1,
            required_concurrent_work=3,
        )
    assert str(error.value) == "pilot work IDs must be unique"


def test_concurrent_pilot_measurement_exact_messages_for_wall_seconds_and_width() -> None:
    observation = (
        ConcurrentWorkObservation(
            work_id="work-0",
            executor_identity="executor-0",
            source_seconds=7,
            processing_seconds=8.612,
        ),
    )
    with pytest.raises(BenchmarkValidationError) as wall_seconds_error:
        ConcurrentPilotMeasurement(
            observation,
            wall_seconds=0,
            physical_source_diversity=1,
            required_concurrent_work=1,
        )
    assert str(wall_seconds_error.value) == "wall_seconds must be finite and positive"

    with pytest.raises(BenchmarkValidationError) as width_error:
        ConcurrentPilotMeasurement(
            observation,
            wall_seconds=8.612,
            physical_source_diversity=1,
            required_concurrent_work=0,
        )
    assert (
        str(width_error.value)
        == "required_concurrent_work must be a positive integer"
    )


def test_concurrent_pilot_measurement_accepts_the_single_work_item_floor() -> None:
    """required_concurrent_work == 1 (with exactly one observation) is the
    minimum valid width and must not be rejected."""
    observation = (
        ConcurrentWorkObservation(
            work_id="work-0",
            executor_identity="executor-0",
            source_seconds=7,
            processing_seconds=8.612,
        ),
    )
    measured = ConcurrentPilotMeasurement(
        observation,
        wall_seconds=8.612,
        physical_source_diversity=1,
        required_concurrent_work=1,
    )
    assert measured.required_concurrent_work == 1


def test_concurrent_pilot_measurement_validates_physical_source_diversity() -> None:
    observations = tuple(
        ConcurrentWorkObservation(
            work_id=f"work-{index}",
            executor_identity=f"executor-{index}",
            source_seconds=7,
            processing_seconds=8.612,
        )
        for index in range(3)
    )

    def _build(diversity: object) -> ConcurrentPilotMeasurement:
        return ConcurrentPilotMeasurement(
            observations,
            wall_seconds=8.612,
            physical_source_diversity=diversity,
            required_concurrent_work=3,
        )

    # Exactly as many distinct physical sources as observations is valid
    # (the upper bound is inclusive, not exclusive).
    assert _build(3).physical_source_diversity == 3

    for invalid_diversity in (0, 4, 2.5, "3"):
        with pytest.raises(BenchmarkValidationError) as error:
            _build(invalid_diversity)
        assert str(error.value) == "physical_source_diversity is invalid"


def test_workload_is_deterministic_single_copy_and_self_validating() -> None:
    manifest = _manifest(5.0)
    repeated = _manifest(5.0)
    assert manifest.sha256 == repeated.sha256
    assert len({item.logical_work_id for item in manifest.items}) == 5
    assert manifest.stats.logical_items == 5
    assert manifest.stats.unique_sources == 2
    assert manifest.stats.unique_cameras == 2
    assert manifest.stats.physical_bytes == 2_000_000
    assert manifest.stats.logical_source_hours == 5
    assert manifest.stats.unique_source_hours == 2
    assert manifest.stats.maximum_repetitions == 3
    assert manifest.source_for(manifest.items[0].logical_work_id).source_id == "source-a"
    with pytest.raises(BenchmarkValidationError, match="unknown logical work"):
        manifest.source_for("pcbm-" + "f" * 32)
    payload = json.loads(manifest.to_json())
    assert payload["storage_mode"] == "logical-references-only"
    assert all(set(item) == {"logical_work_id", "source_id", "repetition_index"} for item in payload["items"])
    assert WorkloadManifest.from_json(manifest.to_json()) == manifest


def test_probe_and_manifest_identity_drift_are_rejected() -> None:
    expected = _probe("source-a")
    reject_probe_mismatch(expected, expected)
    with pytest.raises(IdentityMismatchError, match="probe mismatch"):
        reject_probe_mismatch(expected, replace(expected, byte_size=1_000_001))

    manifest = _manifest()
    item = manifest.items[0]
    with pytest.raises(IdentityMismatchError, match="logical_work_id"):
        replace(
            manifest,
            items=(replace(item, logical_work_id="pcbm-" + "0" * 32),)
            + manifest.items[1:],
        )
    with pytest.raises(BenchmarkValidationError, match="duration"):
        replace(expected, duration_seconds=1.0)
    invalid_probes = (
        ({"source_id": ""}, "source_id"),
        ({"source_path": "https://example/video.mp4"}, "Files path"),
        ({"source_path": "Files/input/../video.mp4"}, "unsafe"),
        ({"source_sha256": "ABC"}, "SHA-256"),
        ({"byte_size": 0}, "byte_size"),
        ({"frame_count": 0}, "frame_count"),
        ({"fps_numerator": 0}, "fps_numerator"),
        ({"fps_denominator": 0}, "fps_denominator"),
        ({"width": 0}, "width"),
        ({"height": 0}, "height"),
        ({"duration_seconds": 0}, "duration_seconds"),
        ({"codec": ""}, "codec"),
        ({"camera_id": ""}, "camera_id"),
    )
    for changes, message in invalid_probes:
        with pytest.raises(BenchmarkValidationError, match=message):
            replace(expected, **changes)
    with pytest.raises(BenchmarkValidationError, match="unsupported workload"):
        replace(manifest, schema_version="v2")
    with pytest.raises(BenchmarkValidationError, match="unique"):
        replace(manifest, sources=(manifest.sources[0], manifest.sources[0]))
    with pytest.raises(BenchmarkValidationError, match="sorted"):
        replace(manifest, sources=tuple(reversed(manifest.sources)))
    with pytest.raises(BenchmarkValidationError, match="unknown source"):
        replace(
            manifest,
            items=(replace(manifest.items[0], source_id="missing"),)
            + manifest.items[1:],
        )
    with pytest.raises(BenchmarkValidationError, match="contiguous"):
        replace(
            manifest,
            items=(replace(manifest.items[0], repetition_index=1),)
            + manifest.items[1:],
        )


def test_independent_media_observations_must_agree() -> None:
    opencv = MediaProbeObservation(3_600, 30.0, 120.0, "opencv", 1920, 1080)
    ffprobe = MediaProbeObservation(3_600, 30.0, 120.0, "h264", 1920, 1080)
    observed = verified_media_probe(
        source_id="source-a",
        source_path="Files/input/source-a.mp4",
        source_sha256="a" * 64,
        byte_size=100,
        camera_id="camera-a",
        opencv=opencv,
        ffprobe=ffprobe,
    )
    assert observed.frame_count == 3_600
    assert observed.fps == 30
    with pytest.raises(IdentityMismatchError, match="durations disagree"):
        verified_media_probe(
            source_id="source-a",
            source_path="Files/input/source-a.mp4",
            source_sha256="a" * 64,
            byte_size=100,
            camera_id="camera-a",
            opencv=replace(opencv, duration_seconds=130),
            ffprobe=ffprobe,
        )


def test_media_file_is_independently_probed_by_opencv_and_ffprobe() -> None:
    observed = probe_media_file(
        Path("samples/three_people_walking.mp4"),
        source_id="sample-three",
        source_path="Files/samples/three_people_walking.mp4",
        camera_id="sample",
    )
    assert observed.source_sha256 == (
        "0e70fed4fcdc59334f16dc1eac1a5f433403af4fb53a348a26d24b07404b58d4"
    )
    assert (observed.frame_count, observed.fps, observed.duration_seconds) == (
        175,
        25,
        7,
    )


def test_structured_telemetry_aggregates_every_declared_stage() -> None:
    events = [
        TelemetryEvent(stage, 1.0, 0.5, retry=stage is TelemetryStage.ATTEMPT)
        for stage in TelemetryStage
    ]
    events.append(
        TelemetryEvent(
            TelemetryStage.COMMIT,
            2.0,
            0.1,
            succeeded=False,
            lease_lost=True,
        )
    )
    aggregate = aggregate_telemetry(events)
    assert set(dict(aggregate.stage_counts)) == {
        stage.value for stage in TelemetryStage
    }
    assert aggregate.retries == 1
    assert aggregate.failures == aggregate.lease_losses == 1


def test_measurement_requires_warmup_exact_six_hours_restart_and_no_stitch() -> None:
    manifest = _manifest()
    measurement = MeasurementConfig(bootstrap_resamples=100)
    run = _run(manifest, measurement)
    validate_measurement_run(
        run,
        measurement,
        manifest,
        expected_definition_sha256="f" * 64,
        prior_application_id="application-1",
    )
    validate_measurement_run(
        replace(run, drain_ended_at=run.measured_ended_at),
        measurement,
        manifest,
        expected_definition_sha256="f" * 64,
        prior_application_id="application-1",
    )
    validate_measurement_run(
        replace(
            run,
            drain_ended_at=run.measured_ended_at + measurement.drain_seconds,
        ),
        measurement,
        manifest,
        expected_definition_sha256="f" * 64,
        prior_application_id="application-1",
    )
    with pytest.raises(BenchmarkValidationError, match="drain interval"):
        validate_measurement_run(
            replace(run, drain_ended_at=run.measured_ended_at - 1),
            measurement,
            manifest,
            expected_definition_sha256="f" * 64,
            prior_application_id="application-1",
        )
    with pytest.raises(BenchmarkValidationError, match="exactly six hours"):
        MeasurementConfig(measurement_seconds=21_599)
    with pytest.raises(BenchmarkValidationError, match="warmup_seconds"):
        MeasurementConfig(warmup_seconds=0)
    with pytest.raises(BenchmarkValidationError, match="divide"):
        MeasurementConfig(interval_seconds=301)
    with pytest.raises(InsufficientSamplesError, match="only 6 intervals"):
        MeasurementConfig(interval_seconds=3_600)
    with pytest.raises(InsufficientSamplesError, match="100 bootstrap"):
        MeasurementConfig(bootstrap_resamples=99)
    with pytest.raises(BenchmarkValidationError, match="block"):
        MeasurementConfig(bootstrap_block_length=37)
    with pytest.raises(BenchmarkValidationError, match="restart"):
        MeasurementConfig(require_fresh_application=False)
    with pytest.raises(BenchmarkValidationError, match="fresh Spark"):
        validate_measurement_run(
            run,
            measurement,
            manifest,
            expected_definition_sha256="f" * 64,
            prior_application_id="application-2",
        )
    with pytest.raises(IdentityMismatchError, match="identity mismatch"):
        validate_measurement_run(
            replace(run, workload_sha256="0" * 64),
            measurement,
            manifest,
            expected_definition_sha256="f" * 64,
            prior_application_id="application-1",
        )
    with pytest.raises(BenchmarkValidationError, match="stitch"):
        replace(run, segment_hashes=("1" * 64, "2" * 64))
    with pytest.raises(BenchmarkValidationError, match="warm-up"):
        validate_measurement_run(
            replace(run, warmup_started_at=1_100),
            measurement,
            manifest,
            expected_definition_sha256="f" * 64,
            prior_application_id="application-1",
        )


def test_moving_block_bootstrap_is_deterministic_and_reports_selection() -> None:
    values = [390, 400, 410, 420, 430, 440, 450, 460] * 2
    first = moving_block_bootstrap_lcb(values, seed=1234, resamples=200)
    second = moving_block_bootstrap_lcb(values, seed=1234, resamples=200)
    assert first == second
    assert first[0] < sum(values) / len(values)
    assert first[1] == select_block_length(values)
    with pytest.raises(InsufficientSamplesError, match="at least 8"):
        moving_block_bootstrap_lcb([1.0] * 7)


def test_statistics_deduplicate_retries_and_unique_source_hours() -> None:
    manifest = _manifest()
    measurement = MeasurementConfig(bootstrap_resamples=100)
    run = _run(manifest, measurement)
    attempts = _passing_attempts(manifest, run, measurement)
    first = attempts[0]
    attempts.insert(
        0,
        replace(
            first,
            attempt_id="failed-attempt",
            completed_at=first.completed_at - 1,
            status="FAILED",
            committed=False,
            processed_frames=0,
            error_category="executor-loss",
        ),
    )
    stats, intervals = calculate_statistics(
        manifest, run, measurement, attempts
    )
    assert stats.committed_logical_items == 4
    assert stats.retry_attempts == 1
    assert stats.logical_successful_source_hours == 4
    assert stats.unique_successful_source_hours == 2
    assert stats.aggregate_throughput_x == pytest.approx(4 / 6)
    assert len(intervals) == 72
    assert intervals[0].committed_logical_items == 4

    attempts.append(replace(first, attempt_id="second-winner"))
    with pytest.raises(BenchmarkValidationError, match="multiple committed"):
        calculate_statistics(manifest, run, measurement, attempts)


def test_reliability_gates_cover_failures_resources_cache_and_rss() -> None:
    passing = evaluate_reliability_gates(_passing_reliability())
    assert {gate.name for gate in passing} == {
        "zero-failures",
        "committed-pointer-readable-and-sealed",
        "unique-logical-identity-and-publication",
        "gold-and-reconciliation-visibility",
        "retry-threshold",
        "failure-threshold",
        "observability-complete",
        "partition-balance",
        "idle-tail",
        "observed-concurrency",
        "model-cache",
        "rss-stability",
    }
    assert all(gate.passed for gate in passing)
    failing = replace(
        _passing_reliability(),
        executor_ooms=1,
        resource=replace(
            _passing_reliability().resource,
            peak_concurrency=8,
            idle_tail_seconds=250,
            model_cache_hits=5,
            final_hour_p95_rss_bytes=1_200,
        ),
    )
    outcomes = {
        gate.name: gate.passed for gate in evaluate_reliability_gates(failing)
    }
    assert outcomes == {
        "zero-failures": False,
        "committed-pointer-readable-and-sealed": True,
        "unique-logical-identity-and-publication": True,
        "gold-and-reconciliation-visibility": True,
        "retry-threshold": True,
        "failure-threshold": True,
        "observability-complete": True,
        "partition-balance": True,
        "idle-tail": False,
        "observed-concurrency": False,
        "model-cache": False,
        "rss-stability": False,
    }
    unavailable = replace(
        _passing_reliability(),
        resource=replace(
            _passing_reliability().resource,
            planned_concurrency=0,
            peak_concurrency=0,
            work_available_for_peak=False,
            median_partition_seconds=0,
            inference_wall_seconds=0,
            first_hour_p95_rss_bytes=0,
        ),
    )
    unavailable_gates = evaluate_reliability_gates(unavailable)
    assert next(
        gate for gate in unavailable_gates if gate.name == "observed-concurrency"
    ).passed
    assert (
        json.dumps([gate.to_dict() for gate in unavailable_gates], allow_nan=False)
        is not None
    )


def test_cost_formulas_leave_used_unknown_rates_explicit() -> None:
    usage = CostUsage(
        capacity_units=4,
        capacity_elapsed_hours=6,
        average_storage_gb=100,
        storage_retention_hours=730.5,
        egress_gb=2,
        executor_runtime_hours=18,
        executor_core_hours=72,
        startup_hours=0.25,
        steady_state_hours=6,
        drain_hours=0.1,
        successful_source_hours=2_000,
        storage_read_bytes=10,
        storage_write_bytes=20,
        storage_transactions=3,
    )
    unknown = calculate_cost(
        usage,
        CostRates(storage_usd_per_gb_month=0.02, egress_usd_per_gb=0.05),
    )
    assert unknown.capacity_unit_hours == 24
    assert unknown.storage_gb_months == 100
    assert unknown.known_subtotal_usd == pytest.approx(2.1)
    assert unknown.total_cost_usd is None
    assert unknown.unknown_rates == ("capacity",)
    assert "capacity_units" in dict(unknown.formulas)["capacity"]

    complete = calculate_cost(
        usage,
        CostRates(
            capacity_usd_per_cu_hour=0.10,
            storage_usd_per_gb_month=0.02,
            egress_usd_per_gb=0.05,
            source="contract-2026-10",
        ),
    )
    assert complete.total_cost_usd == pytest.approx(4.5)
    assert complete.cost_per_1000_source_hours_usd == pytest.approx(2.25)
    assert complete.projected_200000_source_hours_usd == pytest.approx(450)
    assert complete.executor_core_hours == 72
    assert dict(complete.storage_counters)["transactions"] == 3


def _complete_event_log_lines(*, task_cpus: int = 1) -> list[str]:
    return [
        json.dumps(
            {
                "Event": "SparkListenerResourceProfileAdded",
                "Task Resource Requests": {
                    "cpus": {
                        "Resource Name": "cpus",
                        "Amount": float(task_cpus),
                    }
                },
            }
        ),
        json.dumps(
            {
                "Event": "SparkListenerExecutorAdded",
                "Timestamp": 10,
                "Executor ID": "1",
                "Executor Info": {"Host": "host-a", "Total Cores": 4},
            }
        ),
        json.dumps(
            {
                "Event": "SparkListenerTaskEnd",
                "Stage ID": 1,
                "Task Info": {
                    "Task ID": 7,
                    "Executor ID": "1",
                    "Launch Time": 100,
                    "Finish Time": 200,
                    "Failed": False,
                    "Speculative": False,
                },
                "Task Metrics": {
                    "Executor Run Time": 100,
                    "JVM GC Time": 1,
                    "Memory Bytes Spilled": 0,
                    "Disk Bytes Spilled": 0,
                    "Input Metrics": {"Bytes Read": 1024},
                    "Output Metrics": {"Bytes Written": 512},
                },
            }
        ),
    ]


def test_json_report_passes_only_lcb_reliability_and_complete_event_gates() -> None:
    manifest = _manifest(2_520.0)
    measurement = MeasurementConfig(bootstrap_resamples=100)
    run = _run(manifest, measurement)
    attempts = _passing_attempts(manifest, run, measurement)
    report = build_benchmark_report(
        manifest,
        run,
        measurement,
        attempts,
        _passing_reliability(),
        CostUsage(4, 6, 1, 24, 0),
        CostRates(),
        expected_definition_sha256="f" * 64,
        prior_application_id="application-1",
        event_log_lines=_complete_event_log_lines(),
        event_log_executor_ids=("1",),
        event_log_task_cpus=1,
        event_log_task_count=1,
    )
    decoded = json.loads(report.to_json())
    assert decoded["status"] == "PASS"
    assert decoded["statistics"]["aggregate_throughput_x"] == 420
    assert decoded["statistics"]["throughput_lcb_95_x"] == 420
    assert decoded["statistics"]["unique_successful_source_hours"] == 2
    assert decoded["manifest_sha256"] == manifest.sha256
    assert decoded["measurement_run_sha256"] == run.identity_sha256
    assert decoded["measurement"] == {
        "drain_seconds": 120,
        "measured_seconds": measurement.measurement_seconds,
        "measurement_config_sha256": measurement.sha256,
        "restart_token": run.restart_token,
        "segment_hashes": list(run.segment_hashes),
        "spark_application_id": run.spark_application_id,
        "spark_session_id": run.spark_session_id,
        "warmup_seconds": run.measured_started_at - run.warmup_started_at,
    }
    assert decoded["gates"][0]["observed"] == 420
    assert decoded["gates"][0]["requirement"] == ">= 416.67"
    assert decoded["reliability"]["gates_passed"] is True
    assert decoded["cost"]["total_cost_usd"] is None
    assert all(gate["passed"] for gate in decoded["gates"])
    assert decoded["gates"][-1] == {
        "name": "event-log-completeness",
        "passed": True,
        "observed": "ingested",
        "requirement": "complete ingested Spark event evidence",
    }
    assert decoded["event_log"]["status"] == "ingested"

    failed = build_benchmark_report(
        manifest,
        run,
        measurement,
        attempts,
        replace(_passing_reliability(), publication_failures=1),
        CostUsage(0, 0, 0, 0, 0),
        CostRates(),
        expected_definition_sha256="f" * 64,
        prior_application_id="application-1",
    )
    assert failed.status == "FAIL"
    with pytest.raises(BenchmarkValidationError, match="fresh Spark"):
        build_benchmark_report(
            manifest,
            run,
            measurement,
            attempts,
            _passing_reliability(),
            CostUsage(0, 0, 0, 0, 0),
            CostRates(),
            expected_definition_sha256="f" * 64,
            prior_application_id=run.spark_application_id,
        )

    below_target = build_benchmark_report(
        manifest,
        run,
        measurement,
        [
            attempt
            for index, attempt in enumerate(attempts)
            if index % 35 != 34
        ],
        _passing_reliability(),
        CostUsage(0, 0, 0, 0, 0),
        CostRates(),
        expected_definition_sha256="f" * 64,
        prior_application_id="application-1",
    )
    assert below_target.status == "FAIL"
    assert below_target.gates[0].name == "throughput-lcb"
    assert not below_target.gates[0].passed


def test_build_event_log_section_reports_capability_null_without_lines() -> None:
    section = build_event_log_section(None)
    assert section == {
        "status": "capability_null",
        "reason": (
            "Fabric platform did not expose a Spark application event log "
            "for this run"
        ),
        "executor_count": 0,
        "task_count": 0,
        "unrecognized_event_types": [],
        "max_concurrent_tasks": None,
        "observed_parallelism": None,
        "overlap_capability_reason": None,
    }


def test_build_event_log_section_ingests_real_lines_and_computes_overlap() -> None:
    lines = [
        json.dumps(
            {
                "Event": "SparkListenerExecutorAdded",
                "Timestamp": 10,
                "Executor ID": "1",
                "Executor Info": {"Host": "host-a", "Total Cores": 4},
            }
        ),
        json.dumps(
            {
                "Event": "SparkListenerTaskEnd",
                "Stage ID": 0,
                "Task Info": {
                    "Task ID": 1,
                    "Executor ID": "1",
                    "Launch Time": 1000,
                    "Finish Time": 2000,
                    "Failed": False,
                    "Speculative": False,
                },
                "Task Metrics": {
                    "Executor Run Time": 1000,
                    "Memory Bytes Spilled": 0,
                    "Disk Bytes Spilled": 0,
                },
            }
        ),
        json.dumps(
            {
                "Event": "SparkListenerTaskEnd",
                "Stage ID": 0,
                "Task Info": {
                    "Task ID": 2,
                    "Executor ID": "1",
                    "Launch Time": 1500,
                    "Finish Time": 2500,
                    "Failed": False,
                    "Speculative": False,
                },
                "Task Metrics": {
                    "Executor Run Time": 1000,
                    "Memory Bytes Spilled": 0,
                    "Disk Bytes Spilled": 0,
                },
            }
        ),
    ]
    section = build_event_log_section(lines)
    assert section["unrecognized_event_types"] == []
    observed_parallelism = section.pop("observed_parallelism")
    assert observed_parallelism == pytest.approx(2000 / 1500)
    assert section == {
        "status": "ingested",
        "reason": None,
        "executor_count": 1,
        "task_count": 2,
        "unrecognized_event_types": [],
        "max_concurrent_tasks": 2,
        "overlap_capability_reason": None,
    }


def test_build_event_log_section_distinguishes_one_missing_timestamp_from_both_missing() -> (
    None
):
    """A task missing only one of launch/finish must not raise.

    ``build_event_log_section`` must record an explicit
    ``overlap_capability_reason`` for this case and return normally
    rather than propagating ``compute_task_overlap``'s stricter error,
    which guards against the "both missing" case being conflated with
    the "exactly one missing" case by an ``or``/``and`` swap.
    """
    lines = [
        json.dumps(
            {
                "Event": "SparkListenerTaskEnd",
                "Stage ID": 0,
                "Task Info": {
                    "Task ID": 1,
                    "Executor ID": "1",
                    "Launch Time": 1000,
                    "Failed": False,
                    "Speculative": False,
                },
                "Task Metrics": {
                    "Executor Run Time": 1000,
                    "Memory Bytes Spilled": 0,
                    "Disk Bytes Spilled": 0,
                },
            }
        ),
    ]
    section = build_event_log_section(lines)
    assert section["task_count"] == 1
    assert section["max_concurrent_tasks"] is None
    assert section["observed_parallelism"] is None
    assert section["overlap_capability_reason"] == (
        "platform event log did not expose required task launch/finish timestamps"
    )


def test_build_event_log_section_records_overlap_capability_reason_when_timestamps_missing() -> (
    None
):
    lines = [
        json.dumps(
            {
                "Event": "SparkListenerTaskEnd",
                "Stage ID": 0,
                "Task Info": {
                    "Task ID": 1,
                    "Executor ID": "1",
                    "Failed": False,
                    "Speculative": False,
                },
                "Task Metrics": {
                    "Executor Run Time": 1000,
                    "Memory Bytes Spilled": 0,
                    "Disk Bytes Spilled": 0,
                },
            }
        ),
    ]
    section = build_event_log_section(lines)
    assert section["status"] == "ingested"
    assert section["task_count"] == 1
    assert section["max_concurrent_tasks"] is None
    assert section["observed_parallelism"] is None
    assert section["overlap_capability_reason"] == (
        "platform event log did not expose required task launch/finish timestamps"
    )


def test_build_event_log_section_reports_no_tasks_observed_reason() -> None:
    lines = [
        json.dumps(
            {
                "Event": "SparkListenerExecutorAdded",
                "Timestamp": 0,
                "Executor ID": "1",
                "Executor Info": {"Host": "host-a", "Total Cores": 4},
            }
        ),
    ]
    section = build_event_log_section(lines)
    assert section["status"] == "ingested"
    assert section["task_count"] == 0
    assert section["overlap_capability_reason"] == (
        "no non-speculative task-end events observed"
    )


def test_build_event_log_section_fails_closed_on_malformed_lines() -> None:
    with pytest.raises(SparkEventLogError, match="invalid JSON event line"):
        build_event_log_section(["{not valid json"])


@pytest.mark.parametrize(
    ("executor_ids", "task_cpus", "task_count"),
    [
        (("1",), None, 1),
        (None, 1, 1),
        (("1",), 1, None),
    ],
)
def test_build_event_log_section_requires_every_completeness_expectation(
    executor_ids: tuple[str, ...] | None,
    task_cpus: int | None,
    task_count: int | None,
) -> None:
    with pytest.raises(BenchmarkValidationError) as error:
        build_event_log_section(
            _complete_event_log_lines(),
            expected_executor_ids=executor_ids,
            expected_task_cpus=task_cpus,
            expected_task_count=task_count,
        )
    assert str(error.value) == "complete event-log expectations are required"


def test_report_includes_ingested_event_log_when_lines_are_supplied() -> None:
    manifest = _manifest(2_520.0)
    measurement = MeasurementConfig(bootstrap_resamples=100)
    run = _run(manifest, measurement)
    attempts = _passing_attempts(manifest, run, measurement)
    lines = _complete_event_log_lines()
    report = build_benchmark_report(
        manifest,
        run,
        measurement,
        attempts,
        _passing_reliability(),
        CostUsage(4, 6, 1, 24, 0),
        CostRates(),
        expected_definition_sha256="f" * 64,
        prior_application_id="application-1",
        event_log_lines=lines,
        event_log_executor_ids=("1",),
        event_log_task_cpus=1,
        event_log_task_count=1,
    )
    decoded = json.loads(report.to_json())
    assert decoded["event_log"]["status"] == "ingested"
    assert decoded["event_log"]["executor_count"] == 1


def _minimal_report_kwargs() -> dict[str, Any]:
    return dict(
        schema_version="pc-ca-benchmark-report-v2",
        status="PASS",
        manifest_sha256="a" * 64,
        measurement_run_sha256="b" * 64,
        workload={"logical_items": 1},
        measurement={"measurement_seconds": 1.0},
        statistics={"throughput_lcb_95_x": 1.0},
        reliability={"correctness_failures": 0},
        cost={"total_cost": 0.0},
        gates=(GateResult(name="g", passed=True, observed=1, requirement="r"),),
        event_log={"status": "capability_null", "reason": "no event log"},
    )


def test_benchmark_report_rejects_unsupported_schema_version_with_exact_message() -> (
    None
):
    kwargs = {**_minimal_report_kwargs(), "schema_version": "pc-ca-benchmark-report-v1"}
    with pytest.raises(BenchmarkValidationError) as excinfo:
        BenchmarkReport(**kwargs)
    assert str(excinfo.value) == "unsupported report schema"


def test_benchmark_report_rejects_status_disagreeing_with_gates_with_exact_message() -> (
    None
):
    kwargs = {**_minimal_report_kwargs(), "status": "FAIL"}
    with pytest.raises(BenchmarkValidationError) as excinfo:
        BenchmarkReport(**kwargs)
    assert str(excinfo.value) == "report status disagrees with gates"


def test_benchmark_report_rejects_invalid_event_log_status_with_exact_message() -> None:
    kwargs = {**_minimal_report_kwargs(), "event_log": {"status": "unknown"}}
    with pytest.raises(BenchmarkValidationError) as excinfo:
        BenchmarkReport(**kwargs)
    assert str(excinfo.value) == "report event_log status is invalid"


def test_benchmark_report_accepts_ingested_event_log_status() -> None:
    kwargs = {
        **_minimal_report_kwargs(),
        "event_log": {"status": "ingested", "reason": None},
    }
    report = BenchmarkReport(**kwargs)
    assert report.event_log["status"] == "ingested"


def test_benchmark_report_to_dict_is_exact_canonical_key_set() -> None:
    report = BenchmarkReport(**_minimal_report_kwargs())
    assert report.to_dict() == {
        "schema_version": "pc-ca-benchmark-report-v2",
        "status": "PASS",
        "manifest_sha256": "a" * 64,
        "measurement_run_sha256": "b" * 64,
        "workload": {"logical_items": 1},
        "measurement": {"measurement_seconds": 1.0},
        "statistics": {"throughput_lcb_95_x": 1.0},
        "reliability": {"correctness_failures": 0},
        "cost": {"total_cost": 0.0},
        "gates": [
            {"name": "g", "passed": True, "observed": 1, "requirement": "r"}
        ],
        "event_log": {"status": "capability_null", "reason": "no event log"},
    }
    # ``to_json`` decodes the canonical bytes as UTF-8; the codec name's
    # casing is immaterial to Python's codec registry (ASCII case-folded),
    # so ``.decode("utf-8")`` and ``.decode("UTF-8")`` are observably
    # equivalent for any input this project produces. This is confirmed
    # here rather than left as an unexplained mutmut survivor.
    assert "utf-8".encode().decode("utf-8") == "utf-8".encode().decode("UTF-8")
    assert json.loads(report.to_json()) == report.to_dict()


def test_report_throughput_gate_accepts_exact_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _manifest(2_520.0)
    measurement = MeasurementConfig(bootstrap_resamples=100)
    run = _run(manifest, measurement)
    attempts = _passing_attempts(manifest, run, measurement)
    original = benchmark_module.calculate_statistics

    def exact_target(*args, **kwargs):
        statistics, selection = original(*args, **kwargs)
        return (
            replace(
                statistics,
                throughput_lcb_95_x=THROUGHPUT_TARGET_X,
            ),
            selection,
        )

    monkeypatch.setattr(
        benchmark_module, "calculate_statistics", exact_target
    )
    report = build_benchmark_report(
        manifest,
        run,
        measurement,
        attempts,
        _passing_reliability(),
        CostUsage(0, 0, 0, 0, 0),
        CostRates(),
        expected_definition_sha256="f" * 64,
        prior_application_id="application-1",
    )

    assert report.gates[0].observed == THROUGHPUT_TARGET_X
    assert report.gates[0].passed


def test_sjd_definitions_are_thin_fixed_and_never_deploy() -> None:
    definitions = build_all_sjd_v2_definitions()
    assert set(definitions) == {"control", "process", "gold"}
    for job, definition in definitions.items():
        assert definition["definition"]["format"] == "SparkJobDefinitionV2"
        parts = {
            part["path"]: base64.b64decode(part["payload"])
            for part in definition["definition"]["parts"]
        }
        metadata = json.loads(parts["SparkJobDefinitionV1.json"])
        assert metadata["defaultLakehouseArtifactId"] == LAKEHOUSE_ID
        assert metadata["environmentArtifactId"] == ENVIRONMENT_ID
        assert metadata["additionalLibraryUris"] == []
        source = parts["Main/main.py"].decode()
        assert f"fabric_benchmark_jobs import {job}_main" in source
        assert "requests" not in source and "deploy" not in source.lower()
        assert thin_main_source(job) == parts["Main/main.py"]
    with pytest.raises(BenchmarkValidationError, match="unsupported"):
        build_sjd_v2_definition("deploy")


def _fake_executors(
    count: int, *, cores: int = 1, memory_bytes: int = 4 * 1024**3
) -> tuple:
    from people_counter.fabric_executor_inventory import ExecutorRecord

    return tuple(
        ExecutorRecord(
            executor_id=str(index),
            host=f"host-{index}",
            total_cores=cores,
            max_memory_bytes=memory_bytes,
        )
        for index in range(count)
    )


def _benchmark_execution_profile(*args: Any, **kwargs: Any) -> Any:
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    kwargs.setdefault(
        "probe_peak_rss_bytes",
        lambda _spark, _executors: CapabilityProbeResult(
            capability="executor_peak_rss_bytes",
            status=CapabilityStatus.AVAILABLE,
            evidence="test warmed RSS",
            value=512 * 1024**2,
        ),
    )
    return _production_benchmark_execution_profile(*args, **kwargs)


def test_benchmark_execution_profile_is_fixed_to_observed_live_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )
    profile = _benchmark_execution_profile(
        _FakeSpark(), discover_executors=lambda session, minimum: _fake_executors(5)
    )
    assert profile.name == "candidate-a-benchmark-f64-fixed-v1"
    assert profile.executor_instances == 5
    assert profile.executor_cores == profile.task_cpus == 1
    assert profile.executor_memory_bytes == 56 * 1024**3
    assert profile.memory_reserve_bytes == 4 * 1024**3
    assert type(profile.memory_reserve_bytes) is int
    assert profile.fixed_allocation is True
    assert profile.speculation is False
    assert profile.heartbeat_seconds == 10.0
    assert profile.minimum_speed_x == 0.01
    assert profile.lease_safety_factor == 1.25
    assert profile.lease_margin_seconds == 60.0

    failures = (
        (
            {"spark.dynamicAllocation.enabled": "true"},
            "benchmark requires the reviewed fixed live pool",
        ),
        (
            {"spark.speculation": "true"},
            "benchmark requires Spark speculation disabled",
        ),
        (
            {"spark.executor.memory": "unknown"},
            "unsupported Fabric executor memory 'unknown'",
        ),
    )
    for overrides, message in failures:
        with pytest.raises(RuntimeError) as error:
            _benchmark_execution_profile(
                _FakeSpark(**overrides),
                discover_executors=lambda session, minimum: _fake_executors(5),
            )
        assert str(error.value) == message


def test_benchmark_execution_profile_rejects_observed_executor_core_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )
    with pytest.raises(RuntimeError, match="do not match the reviewed profile"):
        _benchmark_execution_profile(
            _FakeSpark(),
            discover_executors=lambda session, minimum: _fake_executors(5, cores=2),
        )


def test_benchmark_execution_profile_uses_the_measured_executor_rss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real executor-side RSS measurement is used for placement safety.

    Unlike the prior driver-side-only RSS reading, this must call the
    injected probe with the real discovered executors and record its
    measured value on the resulting profile.
    """
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )
    seen = {}

    def fake_probe(spark, executors):
        seen["executors"] = executors
        return CapabilityProbeResult(
            capability="executor_peak_rss_bytes",
            status=CapabilityStatus.AVAILABLE,
            evidence="fake measured 512MiB",
            value=512 * 1024**2,
        )

    profile = _benchmark_execution_profile(
        _FakeSpark(),
        discover_executors=lambda session, minimum: _fake_executors(5),
        probe_peak_rss_bytes=fake_probe,
    )
    assert seen["executors"] == _fake_executors(5)
    assert profile.peak_rss_bytes == 512 * 1024**2
    assert profile.planned_task_count == 5


def test_benchmark_execution_profile_fails_when_rss_is_unmeasured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Profile promotion fails when warmed RSS is unavailable."""
    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )

    def fake_probe(spark, executors):
        return CapabilityProbeResult(
            capability="executor_peak_rss_bytes",
            status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="not measurable in this test",
        )

    with pytest.raises(
        RuntimeError,
        match="requires complete warmed executor RSS evidence",
    ):
        _benchmark_execution_profile(
            _FakeSpark(),
            discover_executors=lambda session, minimum: _fake_executors(5),
            probe_peak_rss_bytes=fake_probe,
        )


def test_benchmark_execution_profile_no_longer_hard_codes_five_executors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Live-discovered executor count drives planning instead of a literal."""
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )
    profile = _benchmark_execution_profile(
        _FakeSpark(**{"spark.executor.instances": "3"}),
        discover_executors=lambda session, minimum: _fake_executors(3),
    )
    assert profile.executor_instances == 3


def test_benchmark_execution_profile_discovery_is_driven_by_configured_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )
    observed_minimum: dict[str, int] = {}

    def _capture(session: Any, minimum: int) -> tuple:
        observed_minimum["value"] = minimum
        return _fake_executors(5)

    _benchmark_execution_profile(_FakeSpark(), discover_executors=_capture)
    assert observed_minimum["value"] == 5


def test_benchmark_execution_profile_default_discovery_uses_live_status_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )
    calls: dict[str, Any] = {}

    def _fake_discover_active_executors(session: Any, *, minimum_executors: int) -> tuple:
        calls["session"] = session
        calls["minimum_executors"] = minimum_executors
        return _fake_executors(5)

    monkeypatch.setattr(
        "people_counter.fabric_executor_inventory.discover_active_executors",
        _fake_discover_active_executors,
    )
    spark = _FakeSpark()
    profile = _benchmark_execution_profile(spark)
    assert profile.executor_instances == 5
    assert calls["session"] is spark
    assert calls["minimum_executors"] == 5


def test_benchmark_execution_profile_rejects_patch_level_python_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``sys.version_info`` is a 3+ element tuple in real interpreters; the
    major/minor check must slice to exactly two elements (``[:2]``) so a
    real patch release such as ``(3, 13, 5)`` is still accepted."""
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13, 5)
    )
    profile = _benchmark_execution_profile(
        _FakeSpark(), discover_executors=lambda session, minimum: _fake_executors(5)
    )
    assert profile.executor_instances == 5

    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 12, 9)
    )
    with pytest.raises(RuntimeError, match="requires Python 3.13"):
        _benchmark_execution_profile(
            _FakeSpark(), discover_executors=lambda session, minimum: _fake_executors(5)
        )


def test_benchmark_execution_profile_reports_interpreter_runtime_and_java_mismatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )

    class _WrongVersionSpark(_FakeSpark):
        version = "3.5.0"

    with pytest.raises(RuntimeError, match=r"requires Spark 4\.1\.1, got 3\.5\.0"):
        _benchmark_execution_profile(
            _WrongVersionSpark(),
            discover_executors=lambda session, minimum: _fake_executors(5),
        )

    class _WrongJavaProperty:
        @staticmethod
        def getProperty(name: str) -> str:
            return "17.0.9"

    class _WrongJavaJvm:
        class java:
            class lang:
                class System(_WrongJavaProperty):
                    pass

    class _WrongJavaSpark(_FakeSpark):
        def __init__(self, **overrides: str) -> None:
            super().__init__(**overrides)
            self.sparkContext = type("Context", (), {"_jvm": _WrongJavaJvm()})()

    with pytest.raises(RuntimeError, match=r"requires Java 21, got 17\.0\.9"):
        _benchmark_execution_profile(
            _WrongJavaSpark(),
            discover_executors=lambda session, minimum: _fake_executors(5),
        )


def test_benchmark_execution_profile_validates_task_cpus_against_cpu_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )
    cpu_profile = CPU_PROFILES["pytorch-r18-b1-1fps-1t"]
    assert cpu_profile.spark_task_cpus == 1

    # Matching task_cpus must not raise, and the resulting profile name must
    # carry the cpu_profile suffix (not silently fall back to the bare name).
    profile = _benchmark_execution_profile(
        _FakeSpark(),
        cpu_profile,
        discover_executors=lambda session, minimum: _fake_executors(5),
    )
    assert profile.name == f"candidate-a-benchmark-f64-fixed-v1:{cpu_profile.profile_id}"

    # A live spark.task.cpus that differs from the reviewed CPU profile must
    # fail closed with the exact, distinguishing message.
    with pytest.raises(RuntimeError) as mismatch_error:
        _benchmark_execution_profile(
            _FakeSpark(**{"spark.task.cpus": "2"}),
            cpu_profile,
            discover_executors=lambda session, minimum: _fake_executors(5),
        )
    assert (
        str(mismatch_error.value)
        == "effective spark.task.cpus differs from the reviewed CPU profile"
    )


def test_benchmark_execution_profile_uses_the_documented_speculation_default_when_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``spark.speculation`` defaults to ``"false"`` (speculation off) when
    Fabric's live conf does not expose the key at all -- proves the real
    default literal, not just that *some* default is used."""
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )
    profile = _benchmark_execution_profile(
        _BareSpark(
            **{
                "spark.dynamicAllocation.enabled": "false",
                "spark.executor.memory": "56g",
            }
        ),
        discover_executors=lambda session, minimum: _fake_executors(5),
    )
    assert profile.speculation is False


def test_benchmark_execution_profile_uses_the_documented_executor_instances_floor_when_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``spark.executor.instances`` defaults to ``"1"`` when Fabric's live
    conf does not expose the key, driving discovery with a floor of one."""
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )
    observed_minimum: dict[str, int] = {}

    def _capture(session: Any, minimum: int) -> tuple:
        observed_minimum["value"] = minimum
        return _fake_executors(1)

    profile = _benchmark_execution_profile(
        _BareSpark(
            **{
                "spark.dynamicAllocation.enabled": "false",
                "spark.speculation": "false",
                "spark.executor.memory": "56g",
            }
        ),
        discover_executors=_capture,
    )
    assert observed_minimum["value"] == 1
    assert profile.executor_instances == 1


def test_benchmark_execution_profile_accepts_the_minimum_single_executor_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``configured_instances == 1`` is the valid minimum and must not be
    rejected by the "at least one" guard."""
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )
    profile = _benchmark_execution_profile(
        _FakeSpark(**{"spark.executor.instances": "1"}),
        discover_executors=lambda session, minimum: _fake_executors(1),
    )
    assert profile.executor_instances == 1


def test_benchmark_execution_profile_rejects_zero_configured_executors_with_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )
    with pytest.raises(RuntimeError) as zero_error:
        _benchmark_execution_profile(
            _FakeSpark(**{"spark.executor.instances": "0"}),
            discover_executors=lambda session, minimum: _fake_executors(1),
        )
    assert (
        str(zero_error.value)
        == "benchmark requires at least one fixed live-pool executor"
    )


def test_benchmark_execution_profile_reads_the_real_task_cpus_and_executor_cores_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Proves ``spark.task.cpus``/``spark.executor.cores`` are read from
    their exact configuration keys rather than a mistyped or wrong-case key
    that would silently fall back to the "1" default."""
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )
    profile = _benchmark_execution_profile(
        _FakeSpark(**{"spark.task.cpus": "2", "spark.executor.cores": "2"}),
        discover_executors=lambda session, minimum: _fake_executors(5, cores=2),
    )
    assert profile.task_cpus == 2
    assert profile.executor_cores == 2


def test_benchmark_execution_profile_computes_exact_memory_reserve_below_the_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``memory_reserve_bytes`` floors to an exact quarter of executor memory
    (integer division) when that quarter is below the 4 GiB cap; verifies
    the divisor and integer (non-float) result."""
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )
    profile = _benchmark_execution_profile(
        _FakeSpark(**{"spark.executor.memory": "8g"}),
        discover_executors=lambda session, minimum: _fake_executors(5),
    )
    assert profile.executor_memory_bytes == 8 * 1024**3
    assert profile.memory_reserve_bytes == 2 * 1024**3
    assert type(profile.memory_reserve_bytes) is int


def test_benchmark_execution_profile_parses_megabyte_and_kilobyte_memory_units(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )
    profile_mb = _benchmark_execution_profile(
        _FakeSpark(**{"spark.executor.memory": "100m"}),
        discover_executors=lambda session, minimum: _fake_executors(5),
    )
    assert profile_mb.executor_memory_bytes == 100 * 1024**2

    profile_kb = _benchmark_execution_profile(
        _FakeSpark(**{"spark.executor.memory": "100k"}),
        discover_executors=lambda session, minimum: _fake_executors(5),
    )
    assert profile_kb.executor_memory_bytes == 100 * 1024


def _fake_control_store_module(
    monkeypatch: pytest.MonkeyPatch, **table_rows: list
) -> SimpleNamespace:
    """Install a fake ``FabricControlStore`` and expose every call it observed.

    Returns a namespace with ``constructed`` (the ``(spark, config)`` kwargs
    seen on every instantiation), ``registered`` (every ``register`` call,
    captured in order), and ``recovered``/``reconciled`` call counters, so
    mutation tests can assert on exact call identity/arguments rather than
    only on the final dispatched status code.
    """
    import people_counter.sjd_control as sjd_control_module

    calls = SimpleNamespace(constructed=[], registered=[], recovered=0, reconciled=0)

    @dataclass
    class _FakeRegistration:
        work_id: str
        status: str = "REGISTERED"
        duration_seconds: float = 0.0

    @dataclass
    class _FakeRecovery:
        recovered_locks: int = 3

    class _FakeStore:
        def __init__(self, spark: Any, *, config: Any) -> None:
            calls.constructed.append({"spark": spark, "config": config})

        def _rows(self, suffix: str) -> list:
            return table_rows.get(suffix, [])

        def register(self, work_id: str, payload: dict, **kwargs: Any) -> Any:
            calls.registered.append({"work_id": work_id, "payload": payload, **kwargs})
            # The real ``RegisteredWork`` carries the caller-supplied
            # ``duration_seconds`` through verbatim (``sjd_control.py``), so
            # a corrupted historical row's value (e.g. NaN) really can reach
            # the final printed report; mirror that here.
            return _FakeRegistration(
                work_id=work_id,
                duration_seconds=kwargs.get("duration_seconds", 0.0),
            )

        def recover(self) -> Any:
            calls.recovered += 1
            return _FakeRecovery()

        def reconcile(self) -> list:
            calls.reconciled += 1
            # Production calls ``asdict(item)`` on every finding, so the fake
            # must yield real dataclass instances (not plain dicts) with
            # exactly the given keys/values.
            return [
                make_dataclass("_FakeFinding", list(item.keys()))(**item)
                for item in table_rows.get("__findings__", [])
            ]

    monkeypatch.setattr(sjd_control_module, "FabricControlStore", _FakeStore)
    return calls


def _json_round_trip(value: Any) -> Any:
    """Normalize Python-native tuples (etc.) the same way ``json.dumps`` does.

    Production code always serializes through JSON before printing, so
    tuples become lists.  Expected-value fixtures built directly from
    dataclasses (via ``asdict``) must go through the same normalization
    before an exact equality comparison against a parsed report.
    """
    return json.loads(json.dumps(value, default=str))


def _expected_pilot_payload() -> dict:
    cpu_profile = CPU_PROFILES["pytorch-r18-b1-1fps-1t"]
    return {
        "batch_size": cpu_profile.detector_batch_size,
        "device": "cpu",
        "device_variant": "cpu",
        "model_format": cpu_profile.model_format,
        "sample_fps": cpu_profile.sample_fps,
        "cpu_profile_id": cpu_profile.profile_id,
        "cpu_profile_sha256": cpu_profile.sha256,
        "model_artifact_sha256": dict(cpu_profile.artifact_sha256),
    }


def test_prepare_pilot_main_defaults_to_five_work_items_for_backward_compatibility(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    sentinel_spark = _FakeSpark()
    calls = _fake_control_store_module(
        monkeypatch,
        work=[
            {
                "work_id": "source-1",
                "status": "SUCCEEDED",
                "payload_json": "{}",
                "duration_seconds": 1.0,
            }
        ],
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: sentinel_spark
    )

    exit_code = benchmark_control_main(
        [
            "prepare-pilot",
            "--run-id",
            "pilot-run-01",
            "--profile-id",
            "pytorch-r18-b1-1fps-1t",
            "--release-digest",
            "a" * 64,
            "--source-work-id",
            "source-1",
        ]
    )
    assert exit_code == 0
    assert len(calls.registered) == 5
    assert calls.constructed == [
        {"spark": sentinel_spark, "config": FabricCandidateAConfig.benchmark()}
    ]

    cpu_profile = CPU_PROFILES["pytorch-r18-b1-1fps-1t"]
    expected_payload = _expected_pilot_payload()
    expected_config_sha256 = hashlib.sha256(
        benchmark_module.canonical_json_bytes(expected_payload)
    ).hexdigest()
    for index, call in enumerate(calls.registered, start=1):
        assert call == {
            "work_id": f"pcbm-pilot-run-01-w{index:06d}",
            "payload": expected_payload,
            "runtime_key": cpu_profile.sha256,
            "duration_seconds": 1.0,
            "config_sha256": expected_config_sha256,
            "release_digest": "a" * 64,
            "max_attempts": 1,
        }

    out = capsys.readouterr().out
    expected_report = {
        "run_id": "pilot-run-01",
        "profile": _json_round_trip(
            {
                **asdict(cpu_profile),
                "equivalence_label": cpu_profile.equivalence_label,
                "sha256": cpu_profile.sha256,
            }
        ),
        "physical_source_diversity": 1,
        "work": [
            {
                "work_id": f"pcbm-pilot-run-01-w{index:06d}",
                "status": "REGISTERED",
                "duration_seconds": 1.0,
            }
            for index in range(1, 6)
        ],
    }
    assert json.loads(out) == expected_report
    # Natural key order above (run_id, profile, physical_source_diversity,
    # work; and per-item work_id, status, duration_seconds) is not
    # alphabetical, so a byte-exact match also proves ``sort_keys=True``
    # (not a falsy/omitted mutant) actually ran.
    assert out == json.dumps(
        expected_report, allow_nan=False, default=str, sort_keys=True
    ) + "\n"


def test_prepare_pilot_main_fills_every_discovered_slot_via_work_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _fake_control_store_module(
        monkeypatch,
        work=[
            {
                "work_id": "source-1",
                "status": "SUCCEEDED",
                "payload_json": "{}",
                "duration_seconds": 1.0,
            }
        ],
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: _FakeSpark()
    )

    exit_code = benchmark_control_main(
        [
            "prepare-pilot",
            "--run-id",
            "pilot-run-02",
            "--profile-id",
            "pytorch-r18-b1-1fps-1t",
            "--release-digest",
            "a" * 64,
            "--source-work-id",
            "source-1",
            "--work-count",
            "40",
        ]
    )
    assert exit_code == 0
    assert len(calls.registered) == 40
    assert calls.registered[0]["work_id"] == "pcbm-pilot-run-02-w000001"
    assert calls.registered[-1]["work_id"] == "pcbm-pilot-run-02-w000040"


def test_prepare_pilot_main_accepts_the_minimum_boundary_work_count_of_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _fake_control_store_module(
        monkeypatch,
        work=[
            {
                "work_id": "source-1",
                "status": "SUCCEEDED",
                "payload_json": "{}",
                "duration_seconds": 1.0,
            }
        ],
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: _FakeSpark()
    )

    exit_code = benchmark_control_main(
        [
            "prepare-pilot",
            "--run-id",
            "pilot-run-02b",
            "--profile-id",
            "pytorch-r18-b1-1fps-1t",
            "--release-digest",
            "a" * 64,
            "--source-work-id",
            "source-1",
            "--work-count",
            "1",
        ]
    )
    assert exit_code == 0
    assert len(calls.registered) == 1


def test_prepare_pilot_main_rejects_non_positive_work_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_control_store_module(monkeypatch)
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: _FakeSpark()
    )

    with pytest.raises(BenchmarkValidationError) as exc_info:
        benchmark_control_main(
            [
                "prepare-pilot",
                "--run-id",
                "pilot-run-03",
                "--profile-id",
                "pytorch-r18-b1-1fps-1t",
                "--release-digest",
                "a" * 64,
                "--source-work-id",
                "source-1",
                "--work-count",
                "0",
            ]
        )
    assert str(exc_info.value) == "pilot work count must be at least 1"


def test_prepare_pilot_main_requires_both_matching_work_id_and_succeeded_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row matching only one of the two selection conditions must not match.

    Guards the ``and`` between ``work_id == source_work_id`` and
    ``status == "SUCCEEDED"``: a decoy row satisfying only one condition
    would incorrectly match under an ``or``.
    """
    _fake_control_store_module(
        monkeypatch,
        work=[
            # Right work_id, wrong status.
            {
                "work_id": "source-1",
                "status": "FAILED",
                "payload_json": "{}",
                "duration_seconds": 1.0,
            },
            # Right status, wrong work_id.
            {
                "work_id": "source-2",
                "status": "SUCCEEDED",
                "payload_json": "{}",
                "duration_seconds": 1.0,
            },
        ],
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: _FakeSpark()
    )

    with pytest.raises(BenchmarkValidationError) as exc_info:
        benchmark_control_main(
            [
                "prepare-pilot",
                "--run-id",
                "pilot-run-04b",
                "--profile-id",
                "pytorch-r18-b1-1fps-1t",
                "--release-digest",
                "a" * 64,
                "--source-work-id",
                "source-1",
            ]
        )
    assert str(exc_info.value) == (
        "pilot source must be an existing successful benchmark work item"
    )


def test_prepare_pilot_main_rejects_a_corrupted_nan_duration_seconds_source_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A corrupted historical row's NaN ``duration_seconds`` must fail closed.

    ``RegisteredWork.duration_seconds`` (``sjd_control.py``) carries the
    caller-supplied value through verbatim, so a NaN can really reach the
    final printed report; ``allow_nan=False`` must reject it rather than
    silently emitting non-JSON-compliant ``NaN`` telemetry.
    """
    _fake_control_store_module(
        monkeypatch,
        work=[
            {
                "work_id": "source-1",
                "status": "SUCCEEDED",
                "payload_json": "{}",
                "duration_seconds": float("nan"),
            }
        ],
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: _FakeSpark()
    )

    with pytest.raises(ValueError, match="not JSON compliant"):
        benchmark_control_main(
            [
                "prepare-pilot",
                "--run-id",
                "pilot-run-04c",
                "--profile-id",
                "pytorch-r18-b1-1fps-1t",
                "--release-digest",
                "a" * 64,
                "--source-work-id",
                "source-1",
            ]
        )


def test_prepare_pilot_main_rejects_unresolvable_source_work_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_control_store_module(monkeypatch, work=[])
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: _FakeSpark()
    )

    with pytest.raises(BenchmarkValidationError) as exc_info:
        benchmark_control_main(
            [
                "prepare-pilot",
                "--run-id",
                "pilot-run-04",
                "--profile-id",
                "pytorch-r18-b1-1fps-1t",
                "--release-digest",
                "a" * 64,
                "--source-work-id",
                "missing-source",
            ]
        )
    assert str(exc_info.value) == (
        "pilot source must be an existing successful benchmark work item"
    )


def test_prepare_pilot_main_rejects_a_non_canonical_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_control_store_module(monkeypatch)
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: _FakeSpark()
    )

    with pytest.raises(BenchmarkValidationError) as exc_info:
        benchmark_control_main(
            [
                "prepare-pilot",
                "--run-id",
                "Not_Canonical",
                "--profile-id",
                "pytorch-r18-b1-1fps-1t",
                "--release-digest",
                "a" * 64,
                "--source-work-id",
                "source-1",
            ]
        )
    assert str(exc_info.value) == "pilot run ID is not canonical"


def test_prepare_pilot_main_rejects_a_malformed_release_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_control_store_module(monkeypatch)
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: _FakeSpark()
    )

    with pytest.raises(BenchmarkValidationError) as exc_info:
        benchmark_control_main(
            [
                "prepare-pilot",
                "--run-id",
                "pilot-run-05",
                "--profile-id",
                "pytorch-r18-b1-1fps-1t",
                "--release-digest",
                "A" * 64,
                "--source-work-id",
                "source-1",
            ]
        )
    assert str(exc_info.value) == "release digest must be SHA-256"


@pytest.mark.parametrize(
    "omitted_flag",
    ["--run-id", "--profile-id", "--release-digest", "--source-work-id"],
)
def test_prepare_pilot_main_requires_every_mandatory_flag(
    monkeypatch: pytest.MonkeyPatch, omitted_flag: str
) -> None:
    _fake_control_store_module(monkeypatch)
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: _FakeSpark()
    )
    arguments = [
        "prepare-pilot",
        "--run-id",
        "pilot-run-06",
        "--profile-id",
        "pytorch-r18-b1-1fps-1t",
        "--release-digest",
        "a" * 64,
        "--source-work-id",
        "source-1",
    ]
    flag_index = arguments.index(omitted_flag)
    del arguments[flag_index : flag_index + 2]

    with pytest.raises(SystemExit):
        benchmark_control_main(arguments)


def test_prepare_pilot_main_rejects_an_unknown_profile_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_control_store_module(monkeypatch)
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: _FakeSpark()
    )

    with pytest.raises(SystemExit):
        benchmark_control_main(
            [
                "prepare-pilot",
                "--run-id",
                "pilot-run-07",
                "--profile-id",
                "not-a-real-profile",
                "--release-digest",
                "a" * 64,
                "--source-work-id",
                "source-1",
            ]
        )


def test_control_main_dispatches_diagnose_to_the_extracted_helper(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    sentinel_spark = _FakeSpark()
    calls = _fake_control_store_module(
        monkeypatch,
        locks=[],
        work=[{"work_id": "w1"}],
        batches=[{"batch_id": "b1"}],
        batch_members=[],
        attempts=[],
        publications=[],
        reconciliation_findings=[],
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: sentinel_spark
    )

    assert benchmark_control_main(["diagnose"]) == 0
    out = capsys.readouterr().out
    expected = {
        "locks": [],
        "work": [{"work_id": "w1"}],
        "batches": [{"batch_id": "b1"}],
        "batch_members": [],
        "attempts": [],
        "publications": [],
        "reconciliation_findings": [],
    }
    assert json.loads(out) == expected
    # The real call sorts top-level keys; the natural ``_rows`` iteration
    # order above is deliberately *not* alphabetical, so this also proves
    # ``sort_keys=True`` (not a falsy/omitted mutant) actually ran.
    assert out == json.dumps(expected, allow_nan=False, default=str, sort_keys=True) + "\n"
    assert calls.constructed == [
        {"spark": sentinel_spark, "config": FabricCandidateAConfig.benchmark()}
    ]


def test_diagnose_main_uses_str_fallback_for_non_json_native_row_values(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class _Opaque:
        def __str__(self) -> str:
            return "opaque-value"

    _fake_control_store_module(
        monkeypatch,
        locks=[{"owner": _Opaque()}],
        work=[],
        batches=[],
        batch_members=[],
        attempts=[],
        publications=[],
        reconciliation_findings=[],
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: _FakeSpark()
    )

    assert benchmark_control_main(["diagnose"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["locks"] == [{"owner": "opaque-value"}]


def test_diagnose_main_rejects_nan_row_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_control_store_module(
        monkeypatch,
        locks=[{"score": float("nan")}],
        work=[],
        batches=[],
        batch_members=[],
        attempts=[],
        publications=[],
        reconciliation_findings=[],
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: _FakeSpark()
    )

    with pytest.raises(ValueError, match="not JSON compliant"):
        benchmark_control_main(["diagnose"])


def test_control_main_dispatches_recover_stale_to_the_extracted_helper(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    sentinel_spark = _FakeSpark()
    calls = _fake_control_store_module(
        monkeypatch,
        __findings__=[
            {"severity": "ERROR", "detail": "d1"},
            {"severity": "WARNING", "detail": "d2"},
            {"severity": "ERROR", "detail": "d3"},
        ],
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: sentinel_spark
    )

    assert benchmark_control_main(["recover-stale"]) == 0
    out = capsys.readouterr().out
    expected = {
        "recovery": {"recovered_locks": 3},
        "critical_findings": 2,
        "noncritical_findings": 1,
        "findings": [
            {"severity": "ERROR", "detail": "d1"},
            {"severity": "WARNING", "detail": "d2"},
            {"severity": "ERROR", "detail": "d3"},
        ],
    }
    assert json.loads(out) == expected
    # Natural key order above (recovery, critical_findings, ...) is not
    # alphabetical, so a byte-exact match also proves ``sort_keys=True`` ran.
    assert out == json.dumps(expected, allow_nan=False, default=str, sort_keys=True) + "\n"
    assert calls.constructed == [
        {"spark": sentinel_spark, "config": FabricCandidateAConfig.benchmark()}
    ]
    assert calls.recovered == 1
    assert calls.reconciled == 1


def test_recover_stale_main_uses_str_fallback_for_non_json_native_findings(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class _Opaque:
        def __str__(self) -> str:
            return "opaque-finding"

    _fake_control_store_module(
        monkeypatch, __findings__=[{"severity": "WARNING", "detail": _Opaque()}]
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: _FakeSpark()
    )

    assert benchmark_control_main(["recover-stale"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["findings"] == [{"severity": "WARNING", "detail": "opaque-finding"}]
    assert report["critical_findings"] == 0
    assert report["noncritical_findings"] == 1


def test_recover_stale_main_rejects_nan_finding_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_control_store_module(
        monkeypatch, __findings__=[{"severity": "ERROR", "score": float("nan")}]
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: _FakeSpark()
    )

    with pytest.raises(ValueError, match="not JSON compliant"):
        benchmark_control_main(["recover-stale"])


def test_control_main_rejects_bare_claim_without_work_id_with_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(BenchmarkValidationError) as exc_info:
        benchmark_control_main(["claim"])
    assert str(exc_info.value) == (
        "benchmark claims require one or more explicit --work-id values"
    )


def test_control_main_dispatches_tail_diagnostics_to_the_extracted_helper(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Fabric's driver-log-fetch API 404s for apps that die before YARN
    log-aggregation completes, so this live-readback path uses the exact
    ``notebookutils.fs`` API the diagnostic/stage-marker writers use rather
    than an external storage REST listing."""

    class _FakeEntry:
        def __init__(self, name: str, path: str, size: int, is_dir: bool) -> None:
            self.name = name
            self.path = path
            self.size = size
            self.isDir = is_dir
            self.modifyTime = 1700000000

    class _FakeFs:
        @staticmethod
        def ls(path: str) -> list[_FakeEntry]:
            assert path.endswith("process/stages/batch-009")
            return [_FakeEntry("00-bootstrap.json", path + "/00-bootstrap.json", 42, False)]

    monkeypatch.setitem(sys.modules, "notebookutils", SimpleNamespace(fs=_FakeFs()))

    assert (
        benchmark_control_main(
            ["tail-diagnostics", "--path", "process/stages/batch-009"]
        )
        == 0
    )
    out = json.loads(capsys.readouterr().out)
    assert out["path"].endswith("process/stages/batch-009")
    assert out["entries"] == [
        {
            "name": "00-bootstrap.json",
            "path": out["path"] + "/00-bootstrap.json",
            "size": 42,
            "is_dir": False,
            "modify_time": 1700000000,
        }
    ]


def test_tail_diagnostics_main_requires_explicit_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(SystemExit):
        benchmark_jobs_module._tail_diagnostics_main([])


def test_tail_diagnostics_main_reports_listing_failures_without_raising(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class _FailingFs:
        @staticmethod
        def ls(path: str) -> list[object]:
            raise RuntimeError("path not found")

    monkeypatch.setitem(sys.modules, "notebookutils", SimpleNamespace(fs=_FailingFs()))

    assert (
        benchmark_jobs_module._tail_diagnostics_main(
            ["--path", "process/diagnostics"]
        )
        == 0
    )
    out = json.loads(capsys.readouterr().out)
    assert out["path"].endswith("process/diagnostics")
    assert out["error"] == "RuntimeError: path not found"


def test_tail_diagnostics_main_error_payload_is_key_sorted(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The error dict is built as ``{"path": ..., "error": ...}`` (insertion
    order), which differs from alphabetical order (``error`` before
    ``path``). A dropped/weakened ``sort_keys=True`` would silently emit
    insertion order instead; assert the literal serialized text to catch
    that."""

    class _FailingFs:
        @staticmethod
        def ls(path: str) -> list[object]:
            raise RuntimeError("boom")

    monkeypatch.setitem(sys.modules, "notebookutils", SimpleNamespace(fs=_FailingFs()))

    assert (
        benchmark_jobs_module._tail_diagnostics_main(["--path", "process/x"]) == 0
    )
    raw = capsys.readouterr().out
    assert raw.index('"error"') < raw.index('"path"')


def test_tail_diagnostics_main_success_payload_is_key_sorted(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The success dict is built as ``{"path": ..., "entries": ...}``
    (insertion order), which differs from alphabetical order (``entries``
    before ``path``). Assert the literal serialized text to catch a
    dropped/weakened ``sort_keys=True``."""

    class _FakeFs:
        @staticmethod
        def ls(path: str) -> list[object]:
            return []

    monkeypatch.setitem(sys.modules, "notebookutils", SimpleNamespace(fs=_FakeFs()))

    assert (
        benchmark_jobs_module._tail_diagnostics_main(["--path", "process/x"]) == 0
    )
    raw = capsys.readouterr().out
    assert raw.index('"entries"') < raw.index('"path"')


def test_tail_diagnostics_main_rejects_nan_entry_fields(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """``allow_nan=False`` on the success payload must actually be enforced;
    weakening it to ``True`` (or dropping it, which defaults to ``True``)
    would silently let a non-finite ``size`` through instead of failing
    loudly on a corrupt listing."""

    class _NanEntry:
        name = "x"
        path = "x"
        size = float("nan")
        isDir = False
        modifyTime = 1

    class _FakeFs:
        @staticmethod
        def ls(path: str) -> list[object]:
            return [_NanEntry()]

    monkeypatch.setitem(sys.modules, "notebookutils", SimpleNamespace(fs=_FakeFs()))

    with pytest.raises(ValueError, match="Out of range float values"):
        benchmark_jobs_module._tail_diagnostics_main(["--path", "process/x"])


def test_tail_diagnostics_main_coerces_non_json_native_entry_fields_via_str(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """``default=str`` on the success payload must actually run; dropping it
    (or replacing it with ``None``) would raise ``TypeError`` instead of
    coercing a non-JSON-native ``modifyTime`` (for example a Spark/py4j
    timestamp wrapper) to its string form."""

    class _Timestamp:
        def __str__(self) -> str:
            return "2024-01-01T00:00:00Z"

    class _OpaqueEntry:
        name = "x"
        path = "x"
        size = 1
        isDir = False
        modifyTime = _Timestamp()

    class _FakeFs:
        @staticmethod
        def ls(path: str) -> list[object]:
            return [_OpaqueEntry()]

    monkeypatch.setitem(sys.modules, "notebookutils", SimpleNamespace(fs=_FakeFs()))

    assert (
        benchmark_jobs_module._tail_diagnostics_main(["--path", "process/x"]) == 0
    )
    out = json.loads(capsys.readouterr().out)
    assert out["entries"] == [
        {
            "name": "x",
            "path": "x",
            "size": 1,
            "is_dir": False,
            "modify_time": "2024-01-01T00:00:00Z",
        }
    ]


def test_tail_diagnostics_main_defaults_missing_entry_attributes_to_none(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Every ``getattr(entry, ..., None)`` call must keep its explicit
    ``None`` default; dropping any one of them would raise
    ``AttributeError`` instead of tolerating a listing entry that is
    missing that field."""

    class _BareEntry:
        """Deliberately has none of ``name``/``path``/``size``/``isDir``/
        ``modifyTime`` so a dropped default on any single field surfaces
        as an ``AttributeError``."""

    class _FakeFs:
        @staticmethod
        def ls(path: str) -> list[object]:
            return [_BareEntry()]

    monkeypatch.setitem(sys.modules, "notebookutils", SimpleNamespace(fs=_FakeFs()))

    assert (
        benchmark_jobs_module._tail_diagnostics_main(["--path", "process/x"]) == 0
    )
    out = json.loads(capsys.readouterr().out)
    assert out["entries"] == [
        {
            "name": None,
            "path": None,
            "size": None,
            "is_dir": None,
            "modify_time": None,
        }
    ]


def test_process_main_persists_diagnostic_traceback_and_still_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crashing benchmark ``process`` command must leave an evidence
    trail even when Fabric's standard Spark driver log-fetch API 404s for
    apps that die before full YARN log-aggregation registration completes.
    This reproduces a live blocker observed as a generic ``state=[dead]``
    failure with zero driver-log evidence."""

    def _boom(arguments: Any, config: Any) -> int:
        raise RuntimeError("boom from process dispatch")

    monkeypatch.setattr(benchmark_jobs_module, "_process_dispatch", _boom)
    written: dict[str, object] = {}

    class _FakeFs:
        @staticmethod
        def put(path: str, content: str, overwrite: bool) -> bool:
            written["path"] = path
            written["content"] = content
            written["overwrite"] = overwrite
            return True

    fake_notebookutils = SimpleNamespace(fs=_FakeFs())
    monkeypatch.setitem(sys.modules, "notebookutils", fake_notebookutils)

    with pytest.raises(RuntimeError, match="boom from process dispatch"):
        benchmark_jobs_module.process_main(
            ["--batch-id", "batch-001", "--profile-id", "pytorch-r18-b1-1fps-1t"]
        )

    assert written["overwrite"] is True
    assert "/process/diagnostics/process-batch-001-" in written["path"]
    payload = json.loads(written["content"])
    assert payload["command"] == "process-batch-001"
    assert payload["error_type"] == "RuntimeError"
    assert payload["error_message"] == "boom from process dispatch"
    assert any(
        "boom from process dispatch" in line for line in payload["traceback"]
    )


def test_write_process_stage_marker_writes_expected_path_and_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Live ``process`` crashes surface as a generic ``state=[dead]`` Spark
    failure with zero driver-log evidence and can terminate the process
    before any Python exception handler runs (a native/JVM crash bypasses
    even the ``_process_dispatch`` try/except). Sequence-numbered stage
    markers let a post-mortem OneLake listing identify the last reached
    stage even with no traceback at all."""

    written: dict[str, object] = {}

    class _FakeFs:
        @staticmethod
        def put(path: str, content: str, overwrite: bool) -> bool:
            written["path"] = path
            written["content"] = content
            written["overwrite"] = overwrite
            return True

    monkeypatch.setitem(
        sys.modules, "notebookutils", SimpleNamespace(fs=_FakeFs())
    )
    config = FabricCandidateAConfig.benchmark()

    benchmark_jobs_module._write_process_stage_marker(
        config, "batch-007", 3, "envelope-loaded"
    )

    assert written["overwrite"] is True
    assert written["path"].endswith(
        "process/stages/batch-007/03-envelope-loaded.json"
    )
    payload = json.loads(written["content"])
    assert payload == {
        "batch_id": "batch-007",
        "sequence": 3,
        "stage": "envelope-loaded",
    }


def test_write_process_stage_marker_swallows_write_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FailingFs:
        @staticmethod
        def put(path: str, content: str, overwrite: bool) -> bool:
            raise RuntimeError("OneLake unavailable")

    monkeypatch.setitem(
        sys.modules, "notebookutils", SimpleNamespace(fs=_FailingFs())
    )
    config = FabricCandidateAConfig.benchmark()

    benchmark_jobs_module._write_process_stage_marker(
        config, "batch-008", 0, "bootstrap"
    )


def _patch_process_dispatch_collaborators(
    monkeypatch: pytest.MonkeyPatch,
    *,
    observed_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Fake every ``_process_dispatch`` collaborator at its origin module.

    ``_process_dispatch`` resolves its collaborators via local imports
    executed at call time, so patching the attribute on the *origin*
    module (rather than on ``benchmark_jobs_module``) is picked up by the
    real function body, letting this test exercise ``_process_dispatch``'s
    actual control flow (payload validation, harness selection wiring,
    result-to-JSON packaging) instead of bypassing it entirely.
    """

    calls: dict[str, Any] = {}
    spark = SimpleNamespace(
        conf=SimpleNamespace(
            get=lambda name, default=None: {
                "spark.task.cpus": "1",
                "spark.executor.cores": "1",
            }.get(name, default)
        )
    )
    cpu_profile = benchmark_jobs_module.CPU_PROFILES["pytorch-r18-b1-1fps-1t"]
    default_payload = {
        "device_variant": "cpu",
        "device": "cpu",
        "model_format": cpu_profile.model_format,
        "batch_size": cpu_profile.detector_batch_size,
        "sample_fps": cpu_profile.sample_fps,
    }

    monkeypatch.setattr(candidate_jobs, "_spark", lambda: spark)

    fake_profile = SimpleNamespace(peak_rss_bytes=12345)

    def _fake_benchmark_execution_profile(
        session: Any, profile: Any, **kwargs: Any
    ) -> Any:
        calls["benchmark_execution_profile_args"] = (session, profile)
        calls["warm_up"] = kwargs["warm_up"]
        return fake_profile

    monkeypatch.setattr(
        benchmark_jobs_module,
        "_benchmark_execution_profile",
        _fake_benchmark_execution_profile,
    )

    class _FakeControlStore:
        def __init__(self, session: Any, *, config: Any) -> None:
            calls["store_init"] = (session, config)
            calls["store_instance"] = self

        def load_claim_envelope_with_digest(
            self, batch_id: str
        ) -> tuple[dict[str, Any], str]:
            calls["loaded_batch_id"] = batch_id
            return (
                {
                    "items": [
                        {
                            "work_id": "work-0",
                            "payload": observed_payload or default_payload,
                        },
                    ]
                },
                "digest-stub",
            )

    monkeypatch.setattr(sjd_control_module, "FabricControlStore", _FakeControlStore)

    release_evidence = SimpleNamespace(
        identity_sha256="a" * 64,
        manifest_sha256="b" * 64,
        receipt_sha256="c" * 64,
        manifest=SimpleNamespace(package_version="0.9.29"),
        receipt=SimpleNamespace(environment_target_version="target"),
    )
    monkeypatch.setattr(
        candidate_jobs,
        "_load_runtime_release_evidence",
        lambda *args, **kwargs: release_evidence,
    )

    from people_counter.fabric_capability_probe import (
        CapabilityProbeResult,
        CapabilityStatus,
    )

    consumer_result = CapabilityProbeResult(
        capability="direct_mount_consumer_capability",
        status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
        evidence="test fallback",
    )
    monkeypatch.setattr(
        candidate_jobs,
        "_probe_and_persist_consumer_decision",
        lambda *args, **kwargs: (
            consumer_result,
            {
                "sha256": "d" * 64,
                "selected_backend": "FABRIC_FALLBACK",
                "executor_ids": ["executor-1"],
            },
        ),
    )

    def _fake_localize(
        session: Any, store: Any, batch_id: str, **kwargs: Any
    ) -> candidate_jobs.LocalizedProcessInputs:
        calls["localize_args"] = (session, store, batch_id)
        return candidate_jobs.LocalizedProcessInputs(
            {"work-0": {"localized": True}},
            {"backend": "FABRIC_FALLBACK", "cache_metrics": {"hits": 0}},
        )

    monkeypatch.setattr(
        candidate_jobs, "_localize_process_inputs", _fake_localize
    )

    fake_harness = object()

    def _fake_select_harness(
        session: Any,
        config: Any,
        batch_id: str,
        *,
        row_enrichment: Mapping[str, Any],
        **kwargs: Any,
    ) -> tuple[Any, str]:
        calls["select_harness_args"] = (session, config, batch_id, row_enrichment)
        return (fake_harness, "capability-stub")

    monkeypatch.setattr(
        candidate_jobs, "select_process_execution_harness", _fake_select_harness
    )

    class _FakeAttemptAdapter:
        def __init__(self, path: Any, session: Any, *, config: Any) -> None:
            calls["adapter_init"] = (path, session, config)
            calls["adapter_instance"] = self

    monkeypatch.setattr(
        sjd_process_module, "OneLakeDeltaAttemptAdapter", _FakeAttemptAdapter
    )

    def _fake_run_process_batch(
        store: Any,
        batch_id: str,
        profile: Any,
        mode: str,
        harness: Any,
        attempts: Any,
        *,
        peak_rss_bytes: int | None = None,
        release_evidence: Any | None = None,
    ) -> sjd_process_module.ProcessResult:
        calls["run_process_batch_args"] = (
            store,
            batch_id,
            profile,
            mode,
            harness,
            attempts,
            peak_rss_bytes,
        )
        return sjd_process_module.ProcessResult(
            batch_id=batch_id,
            process_attempt_id="attempt-1",
            staging_path=Path("/lakehouse/default/Files/stage/attempt-1"),
            record_count=1,
            failed_work_ids=(),
            publication_sequences=(1,),
            resumed=False,
            driver_package_version="0.0.0-test",
        )

    monkeypatch.setattr(
        sjd_process_module, "run_process_batch", _fake_run_process_batch
    )
    calls["spark"] = spark
    calls["config_attempts_path"] = None
    calls["fake_profile"] = fake_profile
    calls["fake_harness"] = fake_harness
    calls["cpu_profile"] = cpu_profile
    return calls


def test_process_dispatch_executes_the_real_body_on_success(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Exercise ``_process_dispatch``'s real body end to end, with only its
    external collaborators faked at their origin modules. Every existing
    ``process_main``/``_process_dispatch`` test fully monkeypatches
    ``_process_dispatch`` itself out, leaving its actual glue logic
    (payload validation, harness wiring, result packaging) with zero
    mutation-test coverage; this closes that gap."""

    calls = _patch_process_dispatch_collaborators(monkeypatch)
    config = FabricCandidateAConfig.benchmark()
    arguments = argparse.Namespace(
        batch_id="batch-1", mode="sdk", profile_id="pytorch-r18-b1-1fps-1t"
    )

    result = benchmark_jobs_module._process_dispatch(arguments, config)

    assert result == 0
    assert calls["loaded_batch_id"] == "batch-1"
    assert calls["store_init"] == (calls["spark"], config)
    assert calls["benchmark_execution_profile_args"] == (
        calls["spark"],
        calls["cpu_profile"],
    )
    warm_works = calls["warm_up"].args[0]
    assert len(warm_works) == 1
    warm_work = warm_works[0]
    assert warm_work["model_identity"] == sjd_process_module._model_identity(
        warm_work
    )
    assert calls["localize_args"] == (
        calls["spark"],
        calls["store_instance"],
        "batch-1",
    )
    (
        harness_session,
        harness_config,
        harness_batch_id,
        harness_enrichment,
    ) = calls["select_harness_args"]
    assert harness_session is calls["spark"]
    assert harness_config is config
    assert harness_batch_id == "batch-1"
    assert harness_enrichment == {
        "work-0": {
            "localized": True,
            "_expected_release_package_version": "0.9.29",
            "_expected_release_manifest_sha256": "b" * 64,
        }
    }
    assert calls["adapter_init"] == (
        config.file_path("attempts"),
        calls["spark"],
        config,
    )
    (
        run_store,
        run_batch_id,
        run_profile,
        run_mode,
        run_harness,
        run_attempts,
        run_peak_rss,
    ) = calls["run_process_batch_args"]
    assert run_store is calls["store_instance"]
    assert run_batch_id == "batch-1"
    assert run_profile is calls["fake_profile"]
    assert run_mode == "sdk"
    assert run_harness is calls["fake_harness"]
    assert run_attempts is calls["adapter_instance"]
    assert run_peak_rss == 12345

    stdout = capsys.readouterr().out
    payload = json.loads(stdout)
    # ``sort_keys=True`` is load-bearing: the dict is built in a different
    # (non-alphabetical) insertion order, so this only holds if the real
    # dump call still sorts keys.
    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["batch_id"] == "batch-1"
    assert payload["harness_capability"] == "capability-stub"
    assert payload["resolver_capability"] == {
        "backend": "FABRIC_FALLBACK",
        "cache_metrics": {"hits": 0},
    }
    assert payload["staging_path"] == "/lakehouse/default/Files/stage/attempt-1"
    cpu_profile = calls["cpu_profile"]
    expected_cpu_profile = json.loads(
        json.dumps(
            {
                **asdict(cpu_profile),
                "equivalence_label": cpu_profile.equivalence_label,
                "sha256": cpu_profile.sha256,
            }
        )
    )
    assert payload["cpu_profile"] == expected_cpu_profile


def test_process_dispatch_accepts_a_claim_payload_relying_on_documented_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``device_variant``/``device``/``model_format``/``batch_size`` are
    read with fallback defaults that must exactly match the base CPU
    profile's expected values; omitting them (unlike ``sample_fps``, whose
    literal default never matches any reviewed profile) must still
    validate and succeed."""

    _patch_process_dispatch_collaborators(
        monkeypatch, observed_payload={"sample_fps": 1.0}
    )
    config = FabricCandidateAConfig.benchmark()
    arguments = argparse.Namespace(
        batch_id="batch-1", mode="sdk", profile_id="pytorch-r18-b1-1fps-1t"
    )

    result = benchmark_jobs_module._process_dispatch(arguments, config)

    assert result == 0


def test_process_dispatch_rejects_a_payload_missing_sample_fps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``sample_fps``'s literal fallback default (3.0) never matches any
    reviewed CPU profile's expected sample rate, so an omitted
    ``sample_fps`` must fail closed with that exact observed default
    recorded in the raised error, not silently coerced to some other
    value."""

    _patch_process_dispatch_collaborators(
        monkeypatch,
        observed_payload={
            "device_variant": "cpu",
            "device": "cpu",
            "model_format": "pytorch",
            "batch_size": 1,
        },
    )
    config = FabricCandidateAConfig.benchmark()
    arguments = argparse.Namespace(
        batch_id="batch-1", mode="sdk", profile_id="pytorch-r18-b1-1fps-1t"
    )

    with pytest.raises(BenchmarkValidationError) as excinfo:
        benchmark_jobs_module._process_dispatch(arguments, config)
    assert "'sample_fps': 3.0" in str(excinfo.value)


def test_process_dispatch_rejects_a_device_variant_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_process_dispatch_collaborators(
        monkeypatch,
        observed_payload={
            "device_variant": "gpu",
            "device": "cpu",
            "model_format": "pytorch",
            "batch_size": 1,
            "sample_fps": 1.0,
        },
    )
    config = FabricCandidateAConfig.benchmark()
    arguments = argparse.Namespace(
        batch_id="batch-1", mode="sdk", profile_id="pytorch-r18-b1-1fps-1t"
    )

    with pytest.raises(BenchmarkValidationError) as excinfo:
        benchmark_jobs_module._process_dispatch(arguments, config)
    assert "'device_variant': 'gpu'" in str(excinfo.value)


def test_process_dispatch_rejects_a_device_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_process_dispatch_collaborators(
        monkeypatch,
        observed_payload={
            "device_variant": "cpu",
            "device": "gpu",
            "model_format": "pytorch",
            "batch_size": 1,
            "sample_fps": 1.0,
        },
    )
    config = FabricCandidateAConfig.benchmark()
    arguments = argparse.Namespace(
        batch_id="batch-1", mode="sdk", profile_id="pytorch-r18-b1-1fps-1t"
    )

    with pytest.raises(BenchmarkValidationError) as excinfo:
        benchmark_jobs_module._process_dispatch(arguments, config)
    assert "'device': 'gpu'" in str(excinfo.value)


def test_process_dispatch_rejects_a_batch_size_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_process_dispatch_collaborators(
        monkeypatch,
        observed_payload={
            "device_variant": "cpu",
            "device": "cpu",
            "model_format": "pytorch",
            "batch_size": 99,
            "sample_fps": 1.0,
        },
    )
    config = FabricCandidateAConfig.benchmark()
    arguments = argparse.Namespace(
        batch_id="batch-1", mode="sdk", profile_id="pytorch-r18-b1-1fps-1t"
    )

    with pytest.raises(BenchmarkValidationError) as excinfo:
        benchmark_jobs_module._process_dispatch(arguments, config)
    assert "'batch_size': 99" in str(excinfo.value)


def test_process_dispatch_rejects_a_payload_that_differs_from_the_cpu_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A claimed work item whose observed payload diverges from the
    requested CPU profile must fail closed before any batch execution is
    attempted, rather than silently running a mismatched configuration."""

    _patch_process_dispatch_collaborators(
        monkeypatch,
        observed_payload={
            "device_variant": "cpu",
            "device": "cpu",
            "model_format": "onnx",
            "batch_size": 1,
            "sample_fps": 1.0,
        },
    )
    config = FabricCandidateAConfig.benchmark()
    arguments = argparse.Namespace(
        batch_id="batch-1", mode="sdk", profile_id="pytorch-r18-b1-1fps-1t"
    )

    with pytest.raises(BenchmarkValidationError, match="work payload differs"):
        benchmark_jobs_module._process_dispatch(arguments, config)


def test_process_main_dispatches_to_extracted_helper_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[Any, Any]] = []

    def _fake_dispatch(arguments: Any, config: Any) -> int:
        calls.append((arguments, config))
        return 0

    monkeypatch.setattr(benchmark_jobs_module, "_process_dispatch", _fake_dispatch)

    result = benchmark_jobs_module.process_main(
        [
            "--batch-id",
            "batch-xyz",
            "--mode",
            "sdk",
            "--profile-id",
            "onnx-r18-b4-1fps-1t",
        ]
    )

    assert result == 0
    assert len(calls) == 1
    arguments, config = calls[0]
    assert arguments.batch_id == "batch-xyz"
    assert arguments.mode == "sdk"
    assert arguments.profile_id == "onnx-r18-b4-1fps-1t"
    assert isinstance(config, FabricCandidateAConfig)
    assert config.mode is CandidateANamespaceMode.BENCHMARK


def test_process_main_writes_entry_marker_before_argument_parsing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The live ``state=[dead]`` crash leaves zero stage-marker evidence even
    for the ``_process_dispatch`` bootstrap checkpoint, so this bisects one
    step earlier: a sentinel ``_entry`` marker that must be written as the
    very first action of ``process_main``, before argument parsing can even
    fail, to prove whether driver-side Python execution starts at all."""

    written: list[str] = []

    class _FakeFs:
        @staticmethod
        def put(path: str, content: str, overwrite: bool) -> bool:
            written.append(path)
            return True

    monkeypatch.setitem(sys.modules, "notebookutils", SimpleNamespace(fs=_FakeFs()))

    with pytest.raises(SystemExit):
        benchmark_jobs_module.process_main([])

    assert len(written) == 1
    assert written[0].endswith("process/stages/entry/00-main-entry.json")


def test_process_main_requires_explicit_batch_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        benchmark_jobs_module,
        "_process_dispatch",
        lambda *_a, **_k: 0,
    )
    with pytest.raises(SystemExit):
        benchmark_jobs_module.process_main([])


def test_process_main_rejects_an_unrecognised_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        benchmark_jobs_module,
        "_process_dispatch",
        lambda *_a, **_k: 0,
    )
    with pytest.raises(SystemExit):
        benchmark_jobs_module.process_main(
            ["--batch-id", "batch-1", "--mode", "not-sdk"]
        )


def test_process_main_defaults_mode_to_sdk_when_omitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Any] = []
    monkeypatch.setattr(
        benchmark_jobs_module,
        "_process_dispatch",
        lambda arguments, config: calls.append(arguments) or 0,
    )
    benchmark_jobs_module.process_main(["--batch-id", "batch-1"])
    assert calls[0].mode == "sdk"


def test_process_main_rejects_an_unrecognised_profile_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        benchmark_jobs_module,
        "_process_dispatch",
        lambda *_a, **_k: 0,
    )
    with pytest.raises(SystemExit):
        benchmark_jobs_module.process_main(
            ["--batch-id", "batch-1", "--profile-id", "not-a-real-profile"]
        )


def test_process_main_defaults_profile_id_when_omitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Any] = []
    monkeypatch.setattr(
        benchmark_jobs_module,
        "_process_dispatch",
        lambda arguments, config: calls.append(arguments) or 0,
    )
    benchmark_jobs_module.process_main(["--batch-id", "batch-1"])
    assert calls[0].profile_id == "pytorch-r18-b1-1fps-1t"


def test_gold_main_persists_diagnostic_traceback_and_still_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crashing benchmark gold-reconcile command must leave an evidence
    trail under the same bounded try/except diagnostic pattern."""

    def _boom(argv: Any, *, config: Any) -> int:
        raise RuntimeError("boom from gold reconcile")

    monkeypatch.setattr(candidate_jobs, "gold_main", _boom)
    written: dict[str, object] = {}

    class _FakeFs:
        @staticmethod
        def put(path: str, content: str, overwrite: bool) -> bool:
            written["path"] = path
            written["content"] = content
            written["overwrite"] = overwrite
            return True

    fake_notebookutils = SimpleNamespace(fs=_FakeFs())
    monkeypatch.setitem(sys.modules, "notebookutils", fake_notebookutils)

    with pytest.raises(RuntimeError, match="boom from gold reconcile"):
        benchmark_jobs_module.gold_main([])

    assert written["overwrite"] is True
    assert "/gold/diagnostics/gold-" in written["path"]
    payload = json.loads(written["content"])
    assert payload["command"] == "gold"
    assert payload["error_type"] == "RuntimeError"
    assert payload["error_message"] == "boom from gold reconcile"


def test_gold_main_dispatches_to_candidate_a_gold_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[Any, Any]] = []

    def _fake_gold(argv: Any, *, config: Any) -> int:
        calls.append((argv, config))
        return 0

    monkeypatch.setattr(candidate_jobs, "gold_main", _fake_gold)

    result = benchmark_jobs_module.gold_main(["--dry-run"])

    assert result == 0
    assert len(calls) == 1
    argv, config = calls[0]
    assert argv == ["--dry-run"]
    assert isinstance(config, FabricCandidateAConfig)


def test_resource_inventory_main_reports_live_executors_placement_and_probes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from people_counter import fabric_capability_probe, fabric_executor_inventory
    from people_counter.cpu_runtime import EffectiveThreadSettings
    from people_counter.fabric_executor_inventory import plan_task_width

    fake_spark = _FakeSpark(**{"spark.task.cpus": "1"})
    monkeypatch.setattr(benchmark_jobs_module, "_spark", lambda: fake_spark)
    fake_executors = _fake_executors(5, cores=8)
    discover_calls: list[dict] = []

    def _discover(session: Any, *, minimum_executors: int) -> tuple:
        discover_calls.append(
            {"session": session, "minimum_executors": minimum_executors}
        )
        return fake_executors

    monkeypatch.setattr(
        fabric_executor_inventory, "discover_active_executors", _discover
    )
    fake_settings = EffectiveThreadSettings(
        environment_variables=(("OMP_NUM_THREADS", "1"),),
        torch_num_threads=1,
        torch_num_interop_threads=1,
        opencv_num_threads=1,
        onnx_intra_op_threads=1,
        onnx_inter_op_threads=1,
    )
    monkeypatch.setattr(
        "people_counter.cpu_runtime.read_effective_thread_settings",
        lambda: fake_settings,
    )
    gpu_calls: list[Any] = []

    def _gpu_probe(spark: Any) -> Any:
        gpu_calls.append(spark)
        return fabric_capability_probe.CapabilityProbeResult(
            capability="fabric_gpu_spark_runtime",
            status=fabric_capability_probe.CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="no documented Fabric GPU Spark profile key",
        )

    monkeypatch.setattr(
        fabric_capability_probe, "probe_gpu_spark_runtime", _gpu_probe
    )
    monkeypatch.setattr(
        fabric_capability_probe,
        "probe_rss_high_water_mark",
        lambda: fabric_capability_probe.CapabilityProbeResult(
            capability="rss_high_water_mark",
            status=fabric_capability_probe.CapabilityStatus.AVAILABLE,
            evidence="resource.getrusage(RUSAGE_SELF).ru_maxrss=123456KiB",
            value=123456 * 1024,
        ),
    )

    assert benchmark_control_main(["resource-inventory"]) == 0
    assert discover_calls == [{"session": fake_spark, "minimum_executors": 1}]
    assert len(gpu_calls) == 1 and gpu_calls[0] is fake_spark

    report = json.loads(capsys.readouterr().out)
    assert report == {
        "executors": [asdict(executor) for executor in fake_executors],
        "executor_count": 5,
        "task_cpus": 1,
        "placement": _json_round_trip(
            asdict(plan_task_width(fake_executors, task_cpus=1))
        ),
        "effective_thread_settings": {
            "environment_variables": {"OMP_NUM_THREADS": "1"},
            "torch_num_threads": 1,
            "torch_num_interop_threads": 1,
            "opencv_num_threads": 1,
            "onnx_intra_op_threads": 1,
            "onnx_inter_op_threads": 1,
        },
        "capability_probes": {
            "fabric_gpu_spark_runtime": {
                "status": "FABRIC_PLATFORM_BLOCKED",
                "evidence": "no documented Fabric GPU Spark profile key",
                "value": None,
            },
            "rss_high_water_mark": {
                "status": "AVAILABLE",
                "evidence": "resource.getrusage(RUSAGE_SELF).ru_maxrss=123456KiB",
                "value": 123456 * 1024,
            },
        },
    }


def test_resource_inventory_main_dispatch_forwards_every_argument_after_the_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``control_main`` must slice off only the ``resource-inventory`` token.

    A regression here (for example slicing ``arguments[2:]``) would silently
    drop the first flag passed to the action.
    """
    from people_counter import fabric_capability_probe, fabric_executor_inventory
    from people_counter.cpu_runtime import EffectiveThreadSettings

    fake_spark = _FakeSpark(**{"spark.task.cpus": "1"})
    monkeypatch.setattr(benchmark_jobs_module, "_spark", lambda: fake_spark)
    discover_calls: list[dict] = []

    def _discover(session: Any, *, minimum_executors: int) -> tuple:
        discover_calls.append(
            {"session": session, "minimum_executors": minimum_executors}
        )
        return _fake_executors(5, cores=8)

    monkeypatch.setattr(
        fabric_executor_inventory, "discover_active_executors", _discover
    )
    monkeypatch.setattr(
        "people_counter.cpu_runtime.read_effective_thread_settings",
        lambda: EffectiveThreadSettings(
            environment_variables=(),
            torch_num_threads=1,
            torch_num_interop_threads=1,
            opencv_num_threads=1,
            onnx_intra_op_threads=1,
            onnx_inter_op_threads=1,
        ),
    )
    monkeypatch.setattr(
        fabric_capability_probe,
        "probe_gpu_spark_runtime",
        lambda spark: fabric_capability_probe.CapabilityProbeResult(
            capability="fabric_gpu_spark_runtime",
            status=fabric_capability_probe.CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="blocked",
        ),
    )
    monkeypatch.setattr(
        fabric_capability_probe,
        "probe_rss_high_water_mark",
        lambda: fabric_capability_probe.CapabilityProbeResult(
            capability="rss_high_water_mark",
            status=fabric_capability_probe.CapabilityStatus.AVAILABLE,
            evidence="ok",
            value=1,
        ),
    )

    # The very first token after the action name is a flag.  If
    # ``control_main`` ever sliced ``arguments[2:]`` instead of
    # ``arguments[1:]`` this flag (and its value) would be silently dropped
    # and argparse would fall back to the untouched default of ``1``.
    assert (
        benchmark_control_main(["resource-inventory", "--minimum-executors", "3"])
        == 0
    )
    assert discover_calls == [{"session": fake_spark, "minimum_executors": 3}]


def test_control_main_routes_non_resource_inventory_actions_precisely(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A loosened ``resource-inventory`` dispatch guard (``or``/``!=`` instead
    of ``and``/``==``) would silently reroute every other action into the
    live-Spark resource-inventory probe, which hangs/times out rather than
    failing fast. Assert each sibling action reaches only its own helper and
    never touches ``_resource_inventory_main``."""

    def _forbidden(*_args: Any, **_kwargs: Any) -> int:
        raise AssertionError("resource-inventory must not be dispatched here")

    monkeypatch.setattr(
        benchmark_jobs_module, "_resource_inventory_main", _forbidden
    )

    diagnose_calls: list[object] = []
    monkeypatch.setattr(
        benchmark_jobs_module,
        "_diagnose_main",
        lambda: diagnose_calls.append(object()) or 0,
    )
    assert benchmark_control_main(["diagnose"]) == 0
    assert len(diagnose_calls) == 1

    recover_calls: list[object] = []
    monkeypatch.setattr(
        benchmark_jobs_module,
        "_recover_stale_main",
        lambda: recover_calls.append(object()) or 0,
    )
    assert benchmark_control_main(["recover-stale"]) == 0
    assert len(recover_calls) == 1

    tail_calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        benchmark_jobs_module,
        "_tail_diagnostics_main",
        lambda arguments: tail_calls.append(arguments) or 0,
    )
    assert benchmark_control_main(["tail-diagnostics", "--path", "x"]) == 0
    assert tail_calls == [("--path", "x")]


def test_resource_inventory_main_computes_slots_from_measured_cores(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from people_counter import fabric_capability_probe, fabric_executor_inventory
    from people_counter.cpu_runtime import EffectiveThreadSettings

    fake_spark = _FakeSpark(**{"spark.task.cpus": "1"})
    monkeypatch.setattr(benchmark_jobs_module, "_spark", lambda: fake_spark)
    monkeypatch.setattr(
        fabric_executor_inventory,
        "discover_active_executors",
        lambda session, *, minimum_executors: _fake_executors(5, cores=8),
    )
    fake_settings = EffectiveThreadSettings(
        environment_variables=(),
        torch_num_threads=1,
        torch_num_interop_threads=1,
        opencv_num_threads=1,
        onnx_intra_op_threads=None,
        onnx_inter_op_threads=None,
    )
    monkeypatch.setattr(
        "people_counter.cpu_runtime.read_effective_thread_settings",
        lambda: fake_settings,
    )
    monkeypatch.setattr(
        fabric_capability_probe,
        "probe_gpu_spark_runtime",
        lambda spark: fabric_capability_probe.CapabilityProbeResult(
            capability="fabric_gpu_spark_runtime",
            status=fabric_capability_probe.CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="blocked",
        ),
    )
    monkeypatch.setattr(
        fabric_capability_probe,
        "probe_rss_high_water_mark",
        lambda: fabric_capability_probe.CapabilityProbeResult(
            capability="rss_high_water_mark",
            status=fabric_capability_probe.CapabilityStatus.AVAILABLE,
            evidence="ok",
            value=1,
        ),
    )

    assert _resource_inventory_main([]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["executor_count"] == 5
    assert report["task_cpus"] == 1
    assert report["placement"]["slots"] == 40
    assert report["capability_probes"]["fabric_gpu_spark_runtime"]["status"] == (
        "FABRIC_PLATFORM_BLOCKED"
    )
    assert report["capability_probes"]["rss_high_water_mark"]["status"] == "AVAILABLE"


def test_resource_inventory_main_minimum_executors_defaults_to_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from people_counter import fabric_capability_probe, fabric_executor_inventory
    from people_counter.cpu_runtime import EffectiveThreadSettings

    monkeypatch.setattr(
        benchmark_jobs_module, "_spark", lambda: _FakeSpark(**{"spark.task.cpus": "1"})
    )
    discover_calls: list[dict] = []

    def _discover(session: Any, *, minimum_executors: int) -> tuple:
        discover_calls.append({"minimum_executors": minimum_executors})
        return _fake_executors(5, cores=8)

    monkeypatch.setattr(
        fabric_executor_inventory, "discover_active_executors", _discover
    )
    monkeypatch.setattr(
        "people_counter.cpu_runtime.read_effective_thread_settings",
        lambda: EffectiveThreadSettings(
            environment_variables=(),
            torch_num_threads=1,
            torch_num_interop_threads=1,
            opencv_num_threads=1,
            onnx_intra_op_threads=1,
            onnx_inter_op_threads=1,
        ),
    )
    monkeypatch.setattr(
        fabric_capability_probe,
        "probe_gpu_spark_runtime",
        lambda spark: fabric_capability_probe.CapabilityProbeResult(
            capability="fabric_gpu_spark_runtime",
            status=fabric_capability_probe.CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="blocked",
        ),
    )
    monkeypatch.setattr(
        fabric_capability_probe,
        "probe_rss_high_water_mark",
        lambda: fabric_capability_probe.CapabilityProbeResult(
            capability="rss_high_water_mark",
            status=fabric_capability_probe.CapabilityStatus.AVAILABLE,
            evidence="ok",
            value=1,
        ),
    )

    assert _resource_inventory_main([]) == 0
    assert discover_calls == [{"minimum_executors": 1}]


def test_resource_inventory_main_minimum_executors_is_configurable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from people_counter import fabric_capability_probe, fabric_executor_inventory
    from people_counter.cpu_runtime import EffectiveThreadSettings

    monkeypatch.setattr(
        benchmark_jobs_module, "_spark", lambda: _FakeSpark(**{"spark.task.cpus": "1"})
    )
    discover_calls: list[dict] = []

    def _discover(session: Any, *, minimum_executors: int) -> tuple:
        discover_calls.append({"minimum_executors": minimum_executors})
        return _fake_executors(5, cores=8)

    monkeypatch.setattr(
        fabric_executor_inventory, "discover_active_executors", _discover
    )
    monkeypatch.setattr(
        "people_counter.cpu_runtime.read_effective_thread_settings",
        lambda: EffectiveThreadSettings(
            environment_variables=(),
            torch_num_threads=1,
            torch_num_interop_threads=1,
            opencv_num_threads=1,
            onnx_intra_op_threads=1,
            onnx_inter_op_threads=1,
        ),
    )
    monkeypatch.setattr(
        fabric_capability_probe,
        "probe_gpu_spark_runtime",
        lambda spark: fabric_capability_probe.CapabilityProbeResult(
            capability="fabric_gpu_spark_runtime",
            status=fabric_capability_probe.CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="blocked",
        ),
    )
    monkeypatch.setattr(
        fabric_capability_probe,
        "probe_rss_high_water_mark",
        lambda: fabric_capability_probe.CapabilityProbeResult(
            capability="rss_high_water_mark",
            status=fabric_capability_probe.CapabilityStatus.AVAILABLE,
            evidence="ok",
            value=1,
        ),
    )

    assert _resource_inventory_main(["--minimum-executors", "3"]) == 0
    # ``type=int`` must actually run: a ``type=None`` mutant would leave
    # this as the string ``"3"``, which fails the exact-int comparison.
    assert discover_calls == [{"minimum_executors": 3}]


def test_resource_inventory_main_task_cpus_override_wins_over_spark_conf(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from people_counter import fabric_capability_probe, fabric_executor_inventory
    from people_counter.cpu_runtime import EffectiveThreadSettings

    # The live Spark conf disagrees with the explicit override so any
    # mutant that ignores the parsed ``--task-cpus`` value (and always
    # falls back to ``spark.conf.get(...)``) is caught by the mismatch.
    monkeypatch.setattr(
        benchmark_jobs_module, "_spark", lambda: _BareSpark(**{"spark.task.cpus": "99"})
    )
    monkeypatch.setattr(
        fabric_executor_inventory,
        "discover_active_executors",
        lambda session, *, minimum_executors: _fake_executors(5, cores=8),
    )
    monkeypatch.setattr(
        "people_counter.cpu_runtime.read_effective_thread_settings",
        lambda: EffectiveThreadSettings(
            environment_variables=(),
            torch_num_threads=1,
            torch_num_interop_threads=1,
            opencv_num_threads=1,
            onnx_intra_op_threads=1,
            onnx_inter_op_threads=1,
        ),
    )
    monkeypatch.setattr(
        fabric_capability_probe,
        "probe_gpu_spark_runtime",
        lambda spark: fabric_capability_probe.CapabilityProbeResult(
            capability="fabric_gpu_spark_runtime",
            status=fabric_capability_probe.CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="blocked",
        ),
    )
    monkeypatch.setattr(
        fabric_capability_probe,
        "probe_rss_high_water_mark",
        lambda: fabric_capability_probe.CapabilityProbeResult(
            capability="rss_high_water_mark",
            status=fabric_capability_probe.CapabilityStatus.AVAILABLE,
            evidence="ok",
            value=1,
        ),
    )

    assert _resource_inventory_main(["--task-cpus", "4"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["task_cpus"] == 4


def test_resource_inventory_main_task_cpus_falls_back_to_exact_spark_conf_key(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from people_counter import fabric_capability_probe, fabric_executor_inventory
    from people_counter.cpu_runtime import EffectiveThreadSettings

    # ``_BareSpark`` only exposes the one key given, so misreading the key
    # name (or the ``"1"`` default string) would surface as a mismatch.
    monkeypatch.setattr(
        benchmark_jobs_module, "_spark", lambda: _BareSpark(**{"spark.task.cpus": "3"})
    )
    monkeypatch.setattr(
        fabric_executor_inventory,
        "discover_active_executors",
        lambda session, *, minimum_executors: _fake_executors(5, cores=8),
    )
    monkeypatch.setattr(
        "people_counter.cpu_runtime.read_effective_thread_settings",
        lambda: EffectiveThreadSettings(
            environment_variables=(),
            torch_num_threads=1,
            torch_num_interop_threads=1,
            opencv_num_threads=1,
            onnx_intra_op_threads=1,
            onnx_inter_op_threads=1,
        ),
    )
    monkeypatch.setattr(
        fabric_capability_probe,
        "probe_gpu_spark_runtime",
        lambda spark: fabric_capability_probe.CapabilityProbeResult(
            capability="fabric_gpu_spark_runtime",
            status=fabric_capability_probe.CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="blocked",
        ),
    )
    monkeypatch.setattr(
        fabric_capability_probe,
        "probe_rss_high_water_mark",
        lambda: fabric_capability_probe.CapabilityProbeResult(
            capability="rss_high_water_mark",
            status=fabric_capability_probe.CapabilityStatus.AVAILABLE,
            evidence="ok",
            value=1,
        ),
    )

    assert _resource_inventory_main([]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["task_cpus"] == 3


def test_resource_inventory_main_task_cpus_defaults_to_one_when_conf_is_bare(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from people_counter import fabric_capability_probe, fabric_executor_inventory
    from people_counter.cpu_runtime import EffectiveThreadSettings

    # No ``spark.task.cpus`` key at all: only the literal ``"1"`` default
    # string in production code can satisfy this.
    monkeypatch.setattr(benchmark_jobs_module, "_spark", lambda: _BareSpark())
    monkeypatch.setattr(
        fabric_executor_inventory,
        "discover_active_executors",
        lambda session, *, minimum_executors: _fake_executors(5, cores=8),
    )
    monkeypatch.setattr(
        "people_counter.cpu_runtime.read_effective_thread_settings",
        lambda: EffectiveThreadSettings(
            environment_variables=(),
            torch_num_threads=1,
            torch_num_interop_threads=1,
            opencv_num_threads=1,
            onnx_intra_op_threads=1,
            onnx_inter_op_threads=1,
        ),
    )
    monkeypatch.setattr(
        fabric_capability_probe,
        "probe_gpu_spark_runtime",
        lambda spark: fabric_capability_probe.CapabilityProbeResult(
            capability="fabric_gpu_spark_runtime",
            status=fabric_capability_probe.CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="blocked",
        ),
    )
    monkeypatch.setattr(
        fabric_capability_probe,
        "probe_rss_high_water_mark",
        lambda: fabric_capability_probe.CapabilityProbeResult(
            capability="rss_high_water_mark",
            status=fabric_capability_probe.CapabilityStatus.AVAILABLE,
            evidence="ok",
            value=1,
        ),
    )

    assert _resource_inventory_main([]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["task_cpus"] == 1



def test_definition_export_is_idempotent_and_refuses_differing_files() -> None:
    destination = Path.cwd() / ".benchmark-definition-export-test"
    shutil.rmtree(destination, ignore_errors=True)
    try:
        first = export_sjd_definitions(destination)
        second = export_sjd_definitions(destination)
        assert first == second
        for exported in first.values():
            assert (
                hashlib.sha256(Path(exported["path"]).read_bytes()).hexdigest()
                == exported["sha256"]
            )
        control_path = Path(first["control"]["path"])
        control_path.write_text("{}\n", encoding="utf-8")
        with pytest.raises(FileExistsError, match="refusing"):
            export_sjd_definitions(destination)
    finally:
        shutil.rmtree(destination, ignore_errors=True)


def test_workload_validation_main(capsys: pytest.CaptureFixture[str]) -> None:
    manifest = _manifest()
    assert workload_main(["--manifest-json", manifest.to_json()]) == 0
    workload_output = json.loads(capsys.readouterr().out)
    assert workload_output == {
        "manifest_sha256": manifest.sha256,
        "stats": manifest.stats.to_dict(),
    }


def _valid_report_envelope() -> dict[str, Any]:
    return {
        "schema_version": "pc-ca-benchmark-report-v2",
        "status": "PASS",
        "manifest_sha256": "a" * 64,
        "measurement_run_sha256": "b" * 64,
        "workload": {},
        "measurement": {},
        "statistics": {},
        "reliability": {},
        "cost": {},
        "event_log": {"status": "capability_null", "reason": "no event log"},
        "gates": [],
    }


def test_require_report_envelope_exact_validation_error_messages() -> None:
    base = _valid_report_envelope()
    with pytest.raises(BenchmarkValidationError) as excinfo:
        _require_report_envelope({**base, "schema_version": "pc-ca-benchmark-report-v1"})
    assert str(excinfo.value) == "unsupported report schema"

    with pytest.raises(BenchmarkValidationError) as excinfo:
        _require_report_envelope({**base, "extra_field": 1})
    assert str(excinfo.value) == "report fields differ from the canonical schema"

    with pytest.raises(BenchmarkValidationError) as excinfo:
        _require_report_envelope({**base, "manifest_sha256": "not-hex"})
    assert str(excinfo.value) == "manifest_sha256 must be lowercase SHA-256"

    with pytest.raises(BenchmarkValidationError) as excinfo:
        _require_report_envelope({**base, "workload": "not-a-mapping"})
    assert str(excinfo.value) == "report workload must be an object"

    with pytest.raises(BenchmarkValidationError) as excinfo:
        _require_report_envelope(
            {**base, "event_log": {"status": "bogus"}}
        )
    assert (
        str(excinfo.value)
        == "report event_log status must be capability_null or ingested"
    )


def test_require_report_envelope_accepts_ingested_event_log_status() -> None:
    base = _valid_report_envelope()
    _require_report_envelope(
        {**base, "event_log": {"status": "ingested", "reason": None}}
    )


def test_safe_validation_mains(capsys: pytest.CaptureFixture[str]) -> None:
    assert (
        measurement_main(
            ["--config-json", json.dumps({"bootstrap_resamples": 100})]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["measurement_seconds"] == 21_600

    passing = {
        "schema_version": "pc-ca-benchmark-report-v2",
        "status": "PASS",
        "manifest_sha256": "a" * 64,
        "measurement_run_sha256": "b" * 64,
        "workload": {},
        "measurement": {},
        "statistics": {},
        "reliability": {},
        "cost": {},
        "event_log": {"status": "capability_null", "reason": "no event log"},
        "gates": [
            {
                "name": name,
                "passed": True,
                "observed": 0,
                "requirement": "reviewed",
            }
            for name in (
                "throughput-lcb",
                "zero-failures",
                "committed-pointer-readable-and-sealed",
                "unique-logical-identity-and-publication",
                "gold-and-reconciliation-visibility",
                "retry-threshold",
                "failure-threshold",
                "observability-complete",
                "partition-balance",
                "idle-tail",
                "observed-concurrency",
                "model-cache",
                "rss-stability",
            )
        ],
    }
    _require_report_envelope(passing)
    assert _require_report_gates(passing["gates"]) == passing["gates"]
    assert report_main(["--report-json", json.dumps(passing)]) == 0
    assert json.loads(capsys.readouterr().out) == {"status": "PASS"}

    failing = {
        **passing,
        "status": "FAIL",
        "gates": [
            *passing["gates"][:-1],
            {**passing["gates"][-1], "passed": False},
        ],
    }
    assert report_main(["--report-json", json.dumps(failing)]) == 1
    assert json.loads(capsys.readouterr().out) == {"status": "FAIL"}
    with pytest.raises(BenchmarkValidationError, match="status disagrees"):
        report_main(
            [
                "--report-json",
                json.dumps({**passing, "status": "FAIL"}),
            ]
        )
    with pytest.raises(BenchmarkValidationError, match="canonical gate set"):
        report_main(
            [
                "--report-json",
                json.dumps(
                    {
                        **passing,
                        "gates": passing["gates"][:1],
                    }
                ),
            ]
        )


def test_benchmark_control_preserves_cli_and_requires_explicit_claim_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[tuple[str, ...], str]] = []

    def fake_control(argv, *, config):
        observed.append((tuple(argv), config.mode.value))
        return 0

    monkeypatch.setattr(candidate_jobs, "control_main", fake_control)
    assert benchmark_control_main(["bootstrap"]) == 0
    monkeypatch.setattr(sys, "argv", ["pc-benchmark-control-sjd", "bootstrap"])
    assert benchmark_control_main() == 0
    explicit_claim = [
        "claim",
        "--work-id",
        "work-001",
        "--owner",
        "owner",
        "--max-items",
        "1",
        "--lease-seconds",
        "60",
    ]
    assert benchmark_control_main(explicit_claim) == 0
    assert observed == [
        (("bootstrap",), "BENCHMARK"),
        (("bootstrap",), "BENCHMARK"),
        (tuple(explicit_claim), "BENCHMARK"),
    ]
    with pytest.raises(BenchmarkValidationError, match="explicit --work-id"):
        benchmark_control_main(
            [
                "claim",
                "--owner",
                "owner",
                "--max-items",
                "1",
                "--lease-seconds",
                "60",
            ]
        )
