from dataclasses import replace

import pytest

from people_counter.fabric_benchmark import (
    ArtifactIdentity,
    BenchmarkValidationError,
    ConcurrentWorkObservation,
    CostRates,
    CostUsage,
    CpuInferenceProfile,
    InsufficientSamplesError,
    MeasurementConfig,
    MediaProbeObservation,
    TelemetryEvent,
    TelemetryStage,
    WorkAttemptTelemetry,
    WorkloadManifest,
    aggregate_telemetry,
    moving_block_bootstrap_lcb,
    required_f64_equivalent_capacities,
    verified_media_probe,
)
from tests.test_fabric_benchmark import _manifest, _passing_reliability, _probe, _run


@pytest.mark.parametrize("changes,message", [
    ({"profile_id": "INVALID"}, "not canonical"),
    ({"model_format": "unsupported"}, "unsupported"),
    ({"detector_batch_size": True}, "positive integer"),
    ({"intra_op_threads": 0}, "positive integer"),
    ({"sample_fps": 0.5}, "temporal sampling"),
    ({"artifact_sha256": (("model", "a" * 64), ("model", "b" * 64))}, "unique"),
    ({"artifact_sha256": (("../model", "a" * 64),)}, "unsafe"),
    ({"model_format": "onnx"}, "immutable artifact hashes"),
])
def test_cpu_profile_rejects_unreproducible_settings(changes, message):
    profile = CpuInferenceProfile("test", "pytorch", 640, 1, 1.0, 1, 1, 1, 1, 1)
    with pytest.raises(BenchmarkValidationError, match=message):
        replace(profile, **changes)


@pytest.mark.parametrize("changes,message", [
    ({"source_id": ""}, "non-empty ASCII"),
    ({"source_path": "relative.mp4"}, "immutable Files path"),
    ({"source_path": "Files/../outside"}, "unsafe"),
    ({"byte_size": True}, "positive integer"),
    ({"fps_denominator": 0}, "positive integer"),
    ({"width": 0}, "positive"),
    ({"camera_id": ""}, "required"),
    ({"duration_seconds": 3602}, "more than one frame"),
])
def test_media_probe_rejects_invalid_source_contract(changes, message):
    with pytest.raises(BenchmarkValidationError, match=message):
        replace(_probe("source-a"), **changes)


@pytest.mark.parametrize("changes,message", [
    ({"frame_count": 0}, "positive integer"), ({"codec": ""}, "codec is required"),
    ({"fps": float("inf")}, "finite"), ({"duration_seconds": 0}, "positive"),
])
def test_independent_probe_rejects_invalid_observations(changes, message):
    observation = MediaProbeObservation(300, 30, 10, "h264", 320, 180)
    with pytest.raises(BenchmarkValidationError, match=message):
        replace(observation, **changes)


@pytest.mark.parametrize("changes,message", [
    ({"fps": 31}, "FPS disagree"),
    ({"width": 640}, "dimensions disagree"),
    ({"duration_seconds": 12}, "durations disagree"),
])
def test_independent_decoders_must_agree_on_timing_and_geometry(changes, message):
    observation = MediaProbeObservation(300, 30, 10, "h264", 320, 180)
    with pytest.raises(BenchmarkValidationError, match=message):
        verified_media_probe(
            source_id="source", source_path="Files/source.mp4", source_sha256="a" * 64,
            byte_size=100, camera_id="camera", opencv=observation,
            ffprobe=replace(observation, **changes),
        )


@pytest.mark.parametrize("changes,message", [
    ({"schema_version": "old"}, "unsupported workload schema"),
    ({"sources": ()}, "sources and items are required"),
    ({"sources": (_probe("source-a"), _probe("source-a"))}, "must be unique"),
    ({"sources": (_probe("source-b"), _probe("source-a"))}, "must be sorted"),
    ({"items": ()}, "sources and items are required"),
])
def test_workload_rejects_ambiguous_source_inventory(changes, message):
    with pytest.raises(BenchmarkValidationError, match=message):
        replace(_manifest(), **changes)


def test_workload_rejects_unknown_sources_gaps_and_incomplete_work():
    manifest = _manifest()
    for item, message in (
        (replace(manifest.items[0], source_id="unknown"), "unknown source"),
        (replace(manifest.items[0], repetition_index=1), "contiguous"),
    ):
        with pytest.raises(BenchmarkValidationError, match=message):
            replace(manifest, items=(item,))
    with pytest.raises(BenchmarkValidationError, match="target size"):
        replace(manifest, items=manifest.items[:1])
    with pytest.raises(BenchmarkValidationError, match="unknown logical work"):
        manifest.source_for("unknown")
    with pytest.raises(BenchmarkValidationError, match="must be an object"):
        WorkloadManifest.from_json("[]")


@pytest.mark.parametrize("changes,message", [
    ({"interval_seconds": 7}, "divide"),
    ({"interval_seconds": 3600}, "only 6 intervals"),
    ({"warmup_seconds": 0}, "positive"),
    ({"bootstrap_resamples": 99}, "at least 100"),
    ({"bootstrap_block_length": 0}, "invalid bootstrap_block_length"),
    ({"require_fresh_application": False}, "requires restart|requires|approval measurement"),
])
def test_measurement_configuration_rejects_invalid_sampling(changes, message):
    with pytest.raises((BenchmarkValidationError, InsufficientSamplesError), match=message):
        MeasurementConfig(**changes)


def test_measurement_run_requires_identity_and_finite_timing():
    manifest = _manifest()
    run = _run(manifest, MeasurementConfig())
    with pytest.raises(BenchmarkValidationError, match="run_id is required"):
        replace(run, run_id="")
    with pytest.raises(BenchmarkValidationError, match="must be finite"):
        replace(run, measured_started_at=float("nan"))


@pytest.mark.parametrize("changes,message", [
    ({"logical_work_id": ""}, "identifiers"),
    ({"status": "RUNNING"}, "status"),
    ({"completed_at": -1}, "timestamps"),
    ({"processed_frames": -1}, "negative"),
    ({"status": "FAILED", "committed": True}, "only a successful"),
])
def test_attempt_telemetry_rejects_invalid_terminal_results(changes, message):
    attempt = WorkAttemptTelemetry("work", "source", "attempt", 0, 1, "SUCCEEDED", True, 1)
    with pytest.raises(BenchmarkValidationError, match=message):
        replace(attempt, **changes)


@pytest.mark.parametrize("changes,message", [
    ({"stage": "inference"}, "allowlisted"),
    ({"recorded_at": float("inf")}, "timestamp"),
    ({"duration_seconds": -1}, "duration"),
])
def test_telemetry_event_rejects_unusable_measurements(changes, message):
    with pytest.raises(BenchmarkValidationError, match=message):
        replace(TelemetryEvent(TelemetryStage.INFERENCE, 1, 1), **changes)


@pytest.mark.parametrize("changes,message", [
    ({"correctness_failures": -1}, "negative"),
    ({"max_retry_rate": 2}, "between zero and one"),
    ({"max_failure_rate": float("nan")}, "between zero and one"),
])
def test_reliability_thresholds_reject_invalid_values(changes, message):
    with pytest.raises(BenchmarkValidationError, match=message):
        replace(_passing_reliability(), **changes)


@pytest.mark.parametrize("changes,error,message", [
    ({"resamples": 99}, InsufficientSamplesError, "at least 100"),
    ({"confidence": 0.5}, BenchmarkValidationError, "confidence"),
    ({"block_length": 0}, BenchmarkValidationError, "block_length"),
])
def test_bootstrap_rejects_invalid_estimation_settings(changes, error, message):
    with pytest.raises(error, match=message):
        moving_block_bootstrap_lcb([1.0] * 72, **changes)


def test_statistical_and_cost_inputs_fail_explicitly():
    with pytest.raises(BenchmarkValidationError, match="finite and nonnegative"):
        moving_block_bootstrap_lcb([float("nan")] * 72)
    with pytest.raises(BenchmarkValidationError, match="non-TelemetryEvent"):
        aggregate_telemetry([{}])
    with pytest.raises(BenchmarkValidationError, match="at least one"):
        required_f64_equivalent_capacities(1, headroom=0.9)
    with pytest.raises(BenchmarkValidationError, match="required"):
        ConcurrentWorkObservation("", "executor", 1, 1)
    with pytest.raises(BenchmarkValidationError, match="startup_seconds"):
        ConcurrentWorkObservation("work", "executor", 1, 1, startup_seconds=-1)
    with pytest.raises(BenchmarkValidationError, match="required"):
        ArtifactIdentity("", "path", "a" * 64)
    with pytest.raises(BenchmarkValidationError, match="finite and nonnegative"):
        CostUsage(1, -1, 0, 0, 0)
    with pytest.raises(BenchmarkValidationError, match="nonnegative or unknown"):
        CostRates(capacity_usd_per_cu_hour=-1)
    with pytest.raises(BenchmarkValidationError, match="USD only"):
        CostRates(currency="EUR")
