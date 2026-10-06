"""Safe local generation and validation entry points for benchmark SJD artifacts."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from people_counter.fabric_benchmark import (
    ENVIRONMENT_ID,
    LAKEHOUSE_ID,
    BenchmarkValidationError,
    CpuInferenceProfile,
    MeasurementConfig,
    WorkloadManifest,
    canonical_json_bytes,
)
from people_counter.fabric_candidate_a import FabricCandidateAConfig


_JOB_MODULES = {
    "control": "people_counter.fabric_benchmark_jobs.control_main",
    "process": "people_counter.fabric_benchmark_jobs.process_main",
    "gold": "people_counter.fabric_benchmark_jobs.gold_main",
}
_REPORT_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "manifest_sha256",
        "measurement_run_sha256",
        "workload",
        "measurement",
        "statistics",
        "reliability",
        "cost",
        "gates",
    }
)
_REPORT_GATE_NAMES = (
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
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_ONNX_ARTIFACTS = (
    (
        "rtdetr_osnet/rtdetr_v2_r18vd/model.onnx",
        "425192fda18c29867123479a7a047cec0797d24a6a68ebbcad1faaa702c00ef3",
    ),
    (
        "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.onnx",
        "da73234e40324fe032847368dccc2aab7297407ac98f2463ff9395b09434b64a",
    ),
)
CPU_PROFILES = {
    "pytorch-r18-b1-1fps-1t": CpuInferenceProfile(
        profile_id="pytorch-r18-b1-1fps-1t",
        model_format="pytorch",
        detector_input_pixels=640,
        detector_batch_size=1,
        sample_fps=1.0,
        intra_op_threads=1,
        inter_op_threads=1,
        opencv_threads=1,
        spark_task_cpus=1,
        videos_per_partition=1,
    ),
    "pytorch-r18-b2-1fps-1t": CpuInferenceProfile(
        profile_id="pytorch-r18-b2-1fps-1t",
        model_format="pytorch",
        detector_input_pixels=640,
        detector_batch_size=2,
        sample_fps=1.0,
        intra_op_threads=1,
        inter_op_threads=1,
        opencv_threads=1,
        spark_task_cpus=1,
        videos_per_partition=1,
    ),
    "onnx-r18-b4-1fps-1t": CpuInferenceProfile(
        profile_id="onnx-r18-b4-1fps-1t",
        model_format="onnx",
        detector_input_pixels=640,
        detector_batch_size=4,
        sample_fps=1.0,
        intra_op_threads=1,
        inter_op_threads=1,
        opencv_threads=1,
        spark_task_cpus=1,
        videos_per_partition=2,
        artifact_sha256=_ONNX_ARTIFACTS,
    ),
}


def _encode_part(path: str, content: bytes) -> dict[str, str]:
    if (
        not path
        or path.startswith("/")
        or "\\" in path
        or any(part in {"", ".", ".."} for part in path.split("/"))
    ):
        raise BenchmarkValidationError(f"unsafe definition part path {path!r}")
    return {
        "path": path,
        "payload": base64.b64encode(content).decode("ascii"),
        "payloadType": "InlineBase64",
    }


def thin_main_source(job: str) -> bytes:
    """Return an installed-wheel-only SJD main with no deployment behavior."""

    try:
        target = _JOB_MODULES[job]
    except KeyError as error:
        raise BenchmarkValidationError(
            f"unsupported benchmark job {job!r}"
        ) from error
    module, function = target.rsplit(".", 1)
    return (
        '"""Generated Candidate A benchmark SJD entry point."""\n\n'
        f"from {module} import {function} as main\n\n"
        'if __name__ == "__main__":\n'
        "    raise SystemExit(main())\n"
    ).encode("utf-8")


def build_sjd_v2_definition(
    job: str,
    *,
    command_line_arguments: str = "",
) -> dict[str, object]:
    """Build one deterministic definition; this function never deploys it."""

    if "\x00" in command_line_arguments:
        raise BenchmarkValidationError("command_line_arguments contains NUL")
    source = thin_main_source(job)
    metadata = {
        "additionalLakehouseIds": [],
        "additionalLibraryUris": [],
        "commandLineArguments": command_line_arguments,
        "defaultLakehouseArtifactId": LAKEHOUSE_ID,
        "environmentArtifactId": ENVIRONMENT_ID,
        "executableFile": "main.py",
        "language": "Python",
        "mainClass": "",
        "retryPolicy": None,
    }
    metadata_bytes = canonical_json_bytes(metadata)
    return {
        "definition": {
            "format": "SparkJobDefinitionV2",
            "parts": [
                _encode_part("Main/main.py", source),
                _encode_part("SparkJobDefinitionV1.json", metadata_bytes),
            ],
        }
    }


def build_all_sjd_v2_definitions(
    command_line_arguments: Mapping[str, str] | None = None,
) -> dict[str, dict[str, object]]:
    arguments = dict(command_line_arguments or {})
    unknown = set(arguments) - set(_JOB_MODULES)
    if unknown:
        raise BenchmarkValidationError(f"unknown benchmark jobs: {sorted(unknown)!r}")
    return {
        job: build_sjd_v2_definition(
            job, command_line_arguments=arguments.get(job, "")
        )
        for job in _JOB_MODULES
    }


def export_sjd_definitions(
    destination: str | Path,
    *,
    command_line_arguments: Mapping[str, str] | None = None,
) -> dict[str, dict[str, str]]:
    """Export deterministic JSON files locally, refusing destructive overwrite."""

    root = Path(destination)
    if root.absolute().resolve(strict=False) != root.absolute():
        raise BenchmarkValidationError("export destination cannot traverse symlinks")
    if root.exists() and (not root.is_dir() or root.is_symlink()):
        raise BenchmarkValidationError("export destination must be a real directory")
    root.mkdir(parents=True, exist_ok=True)
    exported: dict[str, dict[str, str]] = {}
    for job, definition in build_all_sjd_v2_definitions(
        command_line_arguments
    ).items():
        content = canonical_json_bytes(definition) + b"\n"
        path = root / f"{job}.SparkJobDefinitionV2.json"
        if path.is_symlink():
            raise BenchmarkValidationError("refusing to overwrite a symlink")
        if path.exists() and path.read_bytes() != content:
            raise FileExistsError(f"refusing to replace differing export {path}")
        if not path.exists():
            path.write_bytes(content)
        exported[job] = {
            "path": str(path),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
    return exported


def _single_json_argument(
    argv: Sequence[str] | None, *, program: str, argument: str
) -> Mapping[str, Any]:
    parser = argparse.ArgumentParser(prog=program)
    parser.add_argument(argument, required=True)
    parsed = parser.parse_args(argv)
    value = json.loads(getattr(parsed, argument.removeprefix("--").replace("-", "_")))
    if not isinstance(value, Mapping):
        raise BenchmarkValidationError(f"{argument} must decode to a JSON object")
    return value


def workload_main(argv: Sequence[str] | None = None) -> int:
    """Validate and summarize a workload supplied as an inline JSON argument."""

    value = _single_json_argument(
        argv, program="pc-ca-benchmark-workload", argument="--manifest-json"
    )
    manifest = WorkloadManifest.from_dict(value)
    print(
        canonical_json_bytes(
            {
                "manifest_sha256": manifest.sha256,
                "stats": manifest.stats.to_dict(),
            }
        ).decode("utf-8")
    )
    return 0


def measurement_main(argv: Sequence[str] | None = None) -> int:
    """Validate the immutable six-hour measurement configuration."""

    value = _single_json_argument(
        argv, program="pc-ca-benchmark-measurement", argument="--config-json"
    )
    allowed = set(MeasurementConfig.__dataclass_fields__)
    unknown = set(value) - allowed
    if unknown:
        raise BenchmarkValidationError(
            f"unknown measurement configuration fields: {sorted(unknown)!r}"
        )
    config = MeasurementConfig(**value)
    print(
        canonical_json_bytes(
            {
                "measurement_config_sha256": config.sha256,
                "measurement_seconds": config.measurement_seconds,
            }
        ).decode("utf-8")
    )
    return 0


def _require_report_envelope(value: Mapping[str, Any]) -> None:
    if value.get("schema_version") != "pc-ca-benchmark-report-v1":
        raise BenchmarkValidationError("unsupported report schema")
    if set(value) != _REPORT_FIELDS:
        raise BenchmarkValidationError("report fields differ from the canonical schema")
    for name in ("manifest_sha256", "measurement_run_sha256"):
        if _HEX64.fullmatch(str(value.get(name))) is None:
            raise BenchmarkValidationError(f"{name} must be lowercase SHA-256")
    for name in ("workload", "measurement", "statistics", "reliability", "cost"):
        if not isinstance(value.get(name), Mapping):
            raise BenchmarkValidationError(f"report {name} must be an object")


def _require_report_gates(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        raise BenchmarkValidationError("report gates must be a list")
    gates = value
    names = tuple(
        gate.get("name") if isinstance(gate, Mapping) else None
        for gate in gates
    )
    if names != _REPORT_GATE_NAMES:
        raise BenchmarkValidationError(
            "report gates must be the exact ordered canonical gate set"
        )
    if any(
        set(gate) != {"name", "passed", "observed", "requirement"}
        or type(gate["passed"]) is not bool
        for gate in gates
    ):
        raise BenchmarkValidationError("report gate shape is invalid")
    return gates


def report_main(argv: Sequence[str] | None = None) -> int:
    """Validate a generated report envelope and return PASS/FAIL as a process code."""

    value = _single_json_argument(
        argv, program="pc-ca-benchmark-report", argument="--report-json"
    )
    _require_report_envelope(value)
    gates = _require_report_gates(value.get("gates"))
    computed = (
        "PASS"
        if all(isinstance(gate, Mapping) and gate.get("passed") is True for gate in gates)
        else "FAIL"
    )
    if value.get("status") != computed:
        raise BenchmarkValidationError("report status disagrees with gates")
    print(canonical_json_bytes({"status": computed}).decode("utf-8"))
    return 0 if computed == "PASS" else 1


def control_main(argv: Sequence[str] | None = None) -> int:
    """Run explicit benchmark control arguments in the fixed namespace."""

    from people_counter.fabric_candidate_a_jobs import control_main as candidate_main
    from people_counter.fabric_candidate_a_jobs import _spark
    from people_counter.sjd_control import FabricControlStore

    arguments = tuple(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == "prepare-pilot":
        parser = argparse.ArgumentParser(prog="pc-ca-benchmark-prepare-pilot")
        parser.add_argument("prepare-pilot")
        parser.add_argument("--run-id", required=True)
        parser.add_argument("--profile-id", choices=tuple(CPU_PROFILES), required=True)
        parser.add_argument("--release-digest", required=True)
        parser.add_argument("--source-work-id", required=True)
        parsed = parser.parse_args(arguments)
        if re.fullmatch(r"[a-z0-9][a-z0-9-]{7,63}", parsed.run_id) is None:
            raise BenchmarkValidationError("pilot run ID is not canonical")
        if _HEX64.fullmatch(parsed.release_digest) is None:
            raise BenchmarkValidationError("release digest must be SHA-256")
        store = FabricControlStore(
            _spark(), config=FabricCandidateAConfig.benchmark()
        )
        source = next(
            (
                row
                for row in store._rows("work")
                if row["work_id"] == parsed.source_work_id
                and row["status"] == "SUCCEEDED"
            ),
            None,
        )
        if source is None:
            raise BenchmarkValidationError(
                "pilot source must be an existing successful benchmark work item"
            )
        cpu_profile = CPU_PROFILES[parsed.profile_id]
        payload = json.loads(source["payload_json"])
        payload.update(
            {
                "batch_size": cpu_profile.detector_batch_size,
                "device": "cpu",
                "device_variant": "cpu",
                "model_format": cpu_profile.model_format,
                "sample_fps": cpu_profile.sample_fps,
                "cpu_profile_id": cpu_profile.profile_id,
                "cpu_profile_sha256": cpu_profile.sha256,
                "model_artifact_sha256": dict(cpu_profile.artifact_sha256),
            }
        )
        payload_sha256 = hashlib.sha256(
            canonical_json_bytes(payload)
        ).hexdigest()
        registered = []
        for index in range(1, 6):
            work_id = f"pcbm-{parsed.run_id}-w{index:06d}"
            registered.append(
                asdict(
                    store.register(
                        work_id,
                        payload,
                        runtime_key=cpu_profile.sha256,
                        duration_seconds=float(source["duration_seconds"]),
                        config_sha256=payload_sha256,
                        release_digest=parsed.release_digest,
                        max_attempts=1,
                    )
                )
            )
        print(
            json.dumps(
                {
                    "run_id": parsed.run_id,
                    "profile": {
                        **asdict(cpu_profile),
                        "equivalence_label": cpu_profile.equivalence_label,
                        "sha256": cpu_profile.sha256,
                    },
                    "physical_source_diversity": 1,
                    "work": registered,
                },
                allow_nan=False,
                default=str,
                sort_keys=True,
            )
        )
        return 0
    if arguments == ("diagnose",):
        store = FabricControlStore(
            _spark(), config=FabricCandidateAConfig.benchmark()
        )
        rows = {
            suffix: store._rows(suffix)
            for suffix in (
                "locks",
                "work",
                "batches",
                "batch_members",
                "attempts",
                "publications",
                "reconciliation_findings",
            )
        }
        print(json.dumps(rows, allow_nan=False, default=str, sort_keys=True))
        return 0
    if arguments == ("recover-stale",):
        store = FabricControlStore(
            _spark(), config=FabricCandidateAConfig.benchmark()
        )
        recovery = asdict(store.recover())
        findings = [asdict(item) for item in store.reconcile()]
        payload = {
            "recovery": recovery,
            "critical_findings": sum(
                item["severity"] == "ERROR" for item in findings
            ),
            "noncritical_findings": sum(
                item["severity"] != "ERROR" for item in findings
            ),
            "findings": findings,
        }
        print(json.dumps(payload, allow_nan=False, default=str, sort_keys=True))
        return 0
    if "claim" in arguments and "--work-id" not in arguments:
        raise BenchmarkValidationError(
            "benchmark claims require one or more explicit --work-id values"
        )
    return candidate_main(arguments, config=FabricCandidateAConfig.benchmark())


def _benchmark_execution_profile(
    spark: Any,
    cpu_profile: CpuInferenceProfile | None = None,
) -> Any:
    """Validate and describe the reviewed fixed F64 benchmark pool."""

    from people_counter.sjd_process import GIB, ExecutionProfile

    if sys.version_info[:2] != (3, 13):
        raise RuntimeError(f"benchmark requires Python 3.13, got {sys.version}")
    if not str(spark.version).startswith("4.1.1"):
        raise RuntimeError(f"benchmark requires Spark 4.1.1, got {spark.version}")
    java = str(
        spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
    )
    if not java.startswith("21"):
        raise RuntimeError(f"benchmark requires Java 21, got {java}")
    if str(spark.conf.get("spark.dynamicAllocation.enabled", "")).lower() != "false":
        raise RuntimeError("benchmark requires the reviewed fixed live pool")
    if str(spark.conf.get("spark.speculation", "false")).lower() != "false":
        raise RuntimeError("benchmark requires Spark speculation disabled")
    configured_instances = int(spark.conf.get("spark.executor.instances", "5"))
    if configured_instances != 5:
        raise RuntimeError(
            "benchmark requires exactly five fixed live-pool executors"
        )
    memory = str(spark.conf.get("spark.executor.memory")).lower()
    match = re.fullmatch(r"(\d+)([gmk])", memory)
    if match is None:
        raise RuntimeError(f"unsupported Fabric executor memory {memory!r}")
    scale = {"k": 1024, "m": 1024**2, "g": GIB}[match.group(2)]
    memory_bytes = int(match.group(1)) * scale
    task_cpus = int(spark.conf.get("spark.task.cpus", "1"))
    if cpu_profile is not None and task_cpus != cpu_profile.spark_task_cpus:
        raise RuntimeError(
            "effective spark.task.cpus differs from the reviewed CPU profile"
        )
    return ExecutionProfile(
        name=(
            "candidate-a-benchmark-f64-fixed-v1"
            if cpu_profile is None
            else f"candidate-a-benchmark-f64-fixed-v1:{cpu_profile.profile_id}"
        ),
        executor_instances=5,
        executor_cores=1,
        executor_memory_bytes=memory_bytes,
        task_cpus=task_cpus,
        fixed_allocation=True,
        speculation=False,
        memory_reserve_bytes=min(4 * GIB, memory_bytes // 4),
        heartbeat_seconds=10.0,
        minimum_speed_x=0.01,
        lease_safety_factor=1.25,
        lease_margin_seconds=60.0,
    )


def process_main(argv: Sequence[str] | None = None) -> int:
    """Run one explicit benchmark batch on the fixed five-executor live pool."""

    from people_counter.fabric_candidate_a_jobs import (
        _localize_process_inputs,
        _spark,
    )
    from people_counter.sjd_control import FabricControlStore
    from people_counter.sjd_process import (
        OneLakeDeltaAttemptAdapter,
        SparkExecutionHarness,
        run_process_batch,
    )

    parser = argparse.ArgumentParser(prog="pc-benchmark-process-sjd")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--mode", choices=("sdk",), default="sdk")
    parser.add_argument(
        "--profile-id",
        choices=tuple(CPU_PROFILES),
        default="pytorch-r18-b1-1fps-1t",
    )
    arguments = parser.parse_args(argv)
    spark = _spark()
    cpu_profile = CPU_PROFILES[arguments.profile_id]
    profile = _benchmark_execution_profile(spark, cpu_profile)
    config = FabricCandidateAConfig.benchmark()
    config.require_write_enabled()
    store = FabricControlStore(spark, config=config)
    envelope, _ = store.load_claim_envelope_with_digest(arguments.batch_id)
    for item in envelope["items"]:
        payload = item["payload"]
        observed = {
            "device_variant": payload.get("device_variant", "cpu"),
            "device": payload.get("device", "cpu"),
            "model_format": payload.get("model_format", "pytorch"),
            "batch_size": payload.get("batch_size", 1),
            "sample_fps": float(payload.get("sample_fps", 3.0)),
        }
        expected = {
            "device_variant": "cpu",
            "device": "cpu",
            "model_format": cpu_profile.model_format,
            "batch_size": cpu_profile.detector_batch_size,
            "sample_fps": cpu_profile.sample_fps,
        }
        if observed != expected:
            raise BenchmarkValidationError(
                f"work payload differs from CPU profile: "
                f"observed={observed!r}, expected={expected!r}"
            )
    enrichment = _localize_process_inputs(spark, store, arguments.batch_id)
    result = run_process_batch(
        store,
        arguments.batch_id,
        profile,
        arguments.mode,
        SparkExecutionHarness(
            spark,
            verify_settings=False,
            row_enrichment=enrichment,
        ),
        OneLakeDeltaAttemptAdapter(
            config.file_path("attempts"),
            spark,
            config=config,
        ),
    )
    payload = asdict(result)
    payload["staging_path"] = str(payload["staging_path"])
    payload["cpu_profile"] = {
        **asdict(cpu_profile),
        "equivalence_label": cpu_profile.equivalence_label,
        "sha256": cpu_profile.sha256,
    }
    print(json.dumps(payload, allow_nan=False, sort_keys=True))
    return 0


def gold_main(argv: Sequence[str] | None = None) -> int:
    """Run benchmark-only gold; production views and pointers are unreachable."""

    from people_counter.fabric_candidate_a_jobs import gold_main as candidate_main

    return candidate_main(argv, config=FabricCandidateAConfig.benchmark())
