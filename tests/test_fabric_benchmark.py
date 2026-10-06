from __future__ import annotations

import base64
import hashlib
import json
import math
import shutil
import sys
from dataclasses import replace
from pathlib import Path

import pytest

import people_counter.fabric_benchmark as benchmark_module
import people_counter.fabric_candidate_a_jobs as candidate_jobs
from people_counter.fabric_benchmark import (
    ENVIRONMENT_ID,
    FILES_ROOT,
    LAKEHOUSE_ID,
    MEASUREMENT_SECONDS,
    TABLE_PREFIX,
    THROUGHPUT_TARGET_X,
    ArtifactIdentity,
    BenchmarkValidationError,
    ConcurrentPilotMeasurement,
    ConcurrentWorkObservation,
    CostRates,
    CostUsage,
    CpuInferenceProfile,
    FabricBenchmarkConfig,
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
    _benchmark_execution_profile,
    _require_report_envelope,
    _require_report_gates,
    build_all_sjd_v2_definitions,
    build_sjd_v2_definition,
    control_main as benchmark_control_main,
    export_sjd_definitions,
    measurement_main,
    report_main,
    thin_main_source,
    workload_main,
)


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
    assert dict(optimized.artifact_sha256) == {
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
    with pytest.raises(BenchmarkValidationError, match="exactly five"):
        ConcurrentPilotMeasurement(
            observations[:4],
            wall_seconds=8.612,
            physical_source_diversity=1,
        )


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


def test_json_report_passes_only_lcb_and_all_reliability_gates() -> None:
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


def test_benchmark_execution_profile_is_fixed_to_observed_live_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "people_counter.fabric_benchmark_jobs.sys.version_info", (3, 13)
    )
    profile = _benchmark_execution_profile(_FakeSpark())
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
        ({"spark.dynamicAllocation.enabled": "true"}, "fixed live pool"),
        ({"spark.speculation": "true"}, "speculation disabled"),
        ({"spark.executor.instances": "4"}, "exactly five"),
        ({"spark.executor.memory": "unknown"}, "executor memory"),
    )
    for overrides, message in failures:
        with pytest.raises(RuntimeError, match=message):
            _benchmark_execution_profile(_FakeSpark(**overrides))


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


def test_safe_validation_mains(capsys: pytest.CaptureFixture[str]) -> None:
    assert (
        measurement_main(
            ["--config-json", json.dumps({"bootstrap_resamples": 100})]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["measurement_seconds"] == 21_600

    passing = {
        "schema_version": "pc-ca-benchmark-report-v1",
        "status": "PASS",
        "manifest_sha256": "a" * 64,
        "measurement_run_sha256": "b" * 64,
        "workload": {},
        "measurement": {},
        "statistics": {},
        "reliability": {},
        "cost": {},
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
