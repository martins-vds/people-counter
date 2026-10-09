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
from typing import TYPE_CHECKING, Any, Callable, Mapping, Sequence

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

if TYPE_CHECKING:
    from people_counter.fabric_executor_inventory import ExecutorRecord


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
        "event_log",
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
_COMMON_RTDETR_ARTIFACTS = (
    (
        "rtdetr_osnet/rtdetr_v2_r18vd/config.json",
        "ed051ec77cb41c5d9d5e3af21a979b1e890599dfd68434d7636ff82ded4c1527",
    ),
    (
        "rtdetr_osnet/rtdetr_v2_r18vd/preprocessor_config.json",
        "cd38cd59999e7a95d68e487fbe5132df3d4e5c32a0836add57e6126ba0c4eaf1",
    ),
)
_PYTORCH_ARTIFACTS = _COMMON_RTDETR_ARTIFACTS + (
    (
        "rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors",
        "d18309d0d7ea57048138885c4c6ecfcb1e24506fc6153b94ad484f8ab62c7115",
    ),
    (
        "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt",
        "ce171fe160b3608f5e4c19489774991419be965b1d6f4bdccc4b4cfd2ef95347",
    ),
)
_ONNX_ARTIFACTS = _COMMON_RTDETR_ARTIFACTS + (
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
        artifact_sha256=_PYTORCH_ARTIFACTS,
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
        artifact_sha256=_PYTORCH_ARTIFACTS,
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
    if value.get("schema_version") != "pc-ca-benchmark-report-v2":
        raise BenchmarkValidationError("unsupported report schema")
    if set(value) != _REPORT_FIELDS:
        raise BenchmarkValidationError("report fields differ from the canonical schema")
    for name in ("manifest_sha256", "measurement_run_sha256"):
        if _HEX64.fullmatch(str(value.get(name))) is None:
            raise BenchmarkValidationError(f"{name} must be lowercase SHA-256")
    for name in ("workload", "measurement", "statistics", "reliability", "cost", "event_log"):
        if not isinstance(value.get(name), Mapping):
            raise BenchmarkValidationError(f"report {name} must be an object")
    if value["event_log"].get("status") not in ("capability_null", "ingested"):
        raise BenchmarkValidationError(
            "report event_log status must be capability_null or ingested"
        )


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


def _resource_inventory_main(argv: Sequence[str]) -> int:
    """Live Step 8.1 resource inventory and slot-capability probe.

    Discovers the actually-registered Spark executors (identity/cores/
    memory) after a registration-stability window, computes the slot plan
    from those measured resources (never a hard-coded worker count), reads
    back effective native thread settings, and probes Fabric-native GPU
    Spark runtime availability. Every field is either a live measurement or
    an explicit ``FABRIC_PLATFORM_BLOCKED`` capability-probe result with
    exact evidence; nothing here is extrapolated or assumed.
    """
    from people_counter.cpu_runtime import read_effective_thread_settings
    from people_counter.fabric_capability_probe import (
        probe_gpu_spark_runtime,
        probe_rss_high_water_mark,
    )
    from people_counter.fabric_executor_inventory import (
        discover_active_executors,
        plan_task_width,
    )

    parser = argparse.ArgumentParser(prog="pc-ca-benchmark-resource-inventory")
    parser.add_argument("--minimum-executors", type=int, default=1)
    parser.add_argument("--task-cpus", type=int, default=None)
    parsed = parser.parse_args(argv)

    spark = _spark()
    task_cpus = parsed.task_cpus
    if task_cpus is None:
        task_cpus = int(spark.conf.get("spark.task.cpus", "1"))
    executors = discover_active_executors(
        spark, minimum_executors=parsed.minimum_executors
    )
    placement = plan_task_width(executors, task_cpus=task_cpus)
    thread_settings = read_effective_thread_settings()
    gpu_probe = probe_gpu_spark_runtime(spark)
    rss_probe = probe_rss_high_water_mark()

    def _probe_dict(result: Any) -> dict[str, Any]:
        return {
            "status": result.status.value,
            "evidence": result.evidence,
            "value": result.value,
        }

    report = {
        "executors": [asdict(executor) for executor in executors],
        "executor_count": len(executors),
        "task_cpus": task_cpus,
        "placement": asdict(placement),
        "effective_thread_settings": {
            "environment_variables": dict(thread_settings.environment_variables),
            "torch_num_threads": thread_settings.torch_num_threads,
            "torch_num_interop_threads": thread_settings.torch_num_interop_threads,
            "opencv_num_threads": thread_settings.opencv_num_threads,
            "onnx_intra_op_threads": thread_settings.onnx_intra_op_threads,
            "onnx_inter_op_threads": thread_settings.onnx_inter_op_threads,
        },
        "capability_probes": {
            "fabric_gpu_spark_runtime": _probe_dict(gpu_probe),
            "rss_high_water_mark": _probe_dict(rss_probe),
        },
    }
    print(canonical_json_bytes(report).decode("utf-8"))
    return 0


def _spark() -> Any:
    from people_counter.fabric_candidate_a_jobs import _spark as _live_spark

    return _live_spark()


def _prepare_pilot_main(arguments: tuple[str, ...]) -> int:
    """Register pilot benchmark work, sized to fill discovered Spark slots.

    ``--work-count`` defaults to ``5`` to preserve the original hard-coded
    pilot size, but callers running the live Step 8 resource-inventory probe
    should pass the actual discovered slot count (or a multiple of it, for
    multi-video-per-partition amortization) so every slot is exercised.
    """
    from people_counter.fabric_candidate_a_jobs import _spark
    from people_counter.sjd_control import FabricControlStore

    parser = argparse.ArgumentParser(prog="pc-ca-benchmark-prepare-pilot")
    parser.add_argument("prepare-pilot")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--profile-id", choices=tuple(CPU_PROFILES), required=True)
    parser.add_argument("--release-digest", required=True)
    parser.add_argument("--source-work-id", required=True)
    parser.add_argument("--work-count", type=int, default=5)
    parsed = parser.parse_args(arguments)
    if re.fullmatch(r"[a-z0-9][a-z0-9-]{7,63}", parsed.run_id) is None:
        raise BenchmarkValidationError("pilot run ID is not canonical")
    if _HEX64.fullmatch(parsed.release_digest) is None:
        raise BenchmarkValidationError("release digest must be SHA-256")
    if parsed.work_count < 1:
        raise BenchmarkValidationError("pilot work count must be at least 1")
    store = FabricControlStore(_spark(), config=FabricCandidateAConfig.benchmark())
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
    payload_sha256 = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
    registered = []
    for index in range(1, parsed.work_count + 1):
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


def _diagnose_main() -> int:
    """Dump every control-store table as a single JSON diagnostics payload.

    Pure extraction of the pre-existing ``diagnose`` branch body with no
    logic change, isolated so ``control_main`` stays a thin, fully-covered
    dispatcher.
    """
    from people_counter.fabric_candidate_a_jobs import _spark
    from people_counter.sjd_control import FabricControlStore

    store = FabricControlStore(_spark(), config=FabricCandidateAConfig.benchmark())
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


def _recover_stale_main() -> int:
    """Run control-store recovery and reconciliation and report findings.

    Pure extraction of the pre-existing ``recover-stale`` branch body with no
    logic change, isolated so ``control_main`` stays a thin, fully-covered
    dispatcher.
    """
    from people_counter.fabric_candidate_a_jobs import _spark
    from people_counter.sjd_control import FabricControlStore

    store = FabricControlStore(_spark(), config=FabricCandidateAConfig.benchmark())
    recovery = asdict(store.recover())
    findings = [asdict(item) for item in store.reconcile()]
    payload = {
        "recovery": recovery,
        "critical_findings": sum(item["severity"] == "ERROR" for item in findings),
        "noncritical_findings": sum(
            item["severity"] != "ERROR" for item in findings
        ),
        "findings": findings,
    }
    print(json.dumps(payload, allow_nan=False, default=str, sort_keys=True))
    return 0


def _tail_diagnostics_main(arguments: Sequence[str]) -> int:
    """List an under-investigation relative OneLake path via ``notebookutils``.

    This exists because Fabric's standard Spark driver-log-fetch API 404s
    for apps that die before YARN log-aggregation completes, so a crashed
    ``process``/``gold`` job can leave zero evidence reachable through the
    job-instance/Livy-session REST surface. It reuses the exact
    ``notebookutils.fs`` API the diagnostic/stage-marker writers use (rather
    than an external storage REST listing, whose directory-scoping has
    proven unreliable from outside the Fabric runtime), so a readback here
    reflects what the write path actually produced.
    """

    parser = argparse.ArgumentParser(prog="pc-ca-benchmark-tail-diagnostics")
    parser.add_argument("--path", required=True)
    parsed = parser.parse_args(arguments)
    import notebookutils

    config = FabricCandidateAConfig.benchmark()
    target = config.file_path(parsed.path)
    try:
        entries = notebookutils.fs.ls(target)
    except Exception as error:  # pragma: no cover - live readback only
        print(
            json.dumps(
                {"path": target, "error": f"{type(error).__name__}: {error}"},
                allow_nan=False,
                sort_keys=True,
            )
        )
        return 0
    payload = {
        "path": target,
        "entries": [
            {
                "name": getattr(entry, "name", None),
                "path": getattr(entry, "path", None),
                "size": getattr(entry, "size", None),
                "is_dir": getattr(entry, "isDir", None),
                "modify_time": getattr(entry, "modifyTime", None),
            }
            for entry in entries
        ],
    }
    print(json.dumps(payload, allow_nan=False, default=str, sort_keys=True))
    return 0


def control_main(argv: Sequence[str] | None = None) -> int:
    """Run explicit benchmark control arguments in the fixed namespace."""

    from people_counter.fabric_candidate_a_jobs import control_main as candidate_main

    arguments = tuple(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == "prepare-pilot":
        return _prepare_pilot_main(arguments)
    if arguments and arguments[0] == "resource-inventory":
        return _resource_inventory_main(arguments[1:])
    if arguments and arguments[0] == "tail-diagnostics":
        return _tail_diagnostics_main(arguments[1:])
    if arguments == ("diagnose",):
        return _diagnose_main()
    if arguments == ("recover-stale",):
        return _recover_stale_main()
    if "claim" in arguments and "--work-id" not in arguments:
        raise BenchmarkValidationError(
            "benchmark claims require one or more explicit --work-id values"
        )
    return candidate_main(arguments, config=FabricCandidateAConfig.benchmark())


def _benchmark_execution_profile(
    spark: Any,
    cpu_profile: CpuInferenceProfile | None = None,
    *,
    warm_up: Callable[[], None] | None = None,
    discover_executors: Callable[[Any, int], Sequence["ExecutorRecord"]] | None = None,
    probe_peak_rss_bytes: Callable[[Any, Sequence["ExecutorRecord"]], Any] | None = None,
) -> Any:
    """Validate and describe the reviewed fixed F64 benchmark pool.

    Executor count is proven from the live Spark status store after a
    registration-stability window rather than assumed to be a hard-coded
    literal; ``spark.executor.instances`` is read only as the configured
    floor that discovery must stably reach. Executor cores, memory, and
    ``spark.task.cpus`` still must match the reviewed single-core-per-
    executor benchmark profile, and mismatched observed resources fail
    closed via ``build_profile_from_inventory``.

    Before admitting concurrency, this attempts a real warm-executor RSS
    measurement taken inside the discovered executors' own tasks
    (:func:`people_counter.fabric_capability_probe.probe_executor_peak_rss_bytes`)
    rather than leaving the advertised RSS cap absent; if Fabric cannot
    expose that measurement, the total physical task count is capped at one
    per executor (``operator_cap=len(executors)``) instead of silently
    admitting unmeasured concurrency.
    """

    from people_counter.fabric_capability_probe import (
        CapabilityStatus,
        probe_executor_peak_rss_bytes,
    )
    from people_counter.fabric_executor_inventory import discover_active_executors
    from people_counter.sjd_process import GIB, build_profile_from_inventory

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
    configured_instances = int(spark.conf.get("spark.executor.instances", "1"))
    if configured_instances < 1:
        raise RuntimeError("benchmark requires at least one fixed live-pool executor")
    expected_executor_cores = int(spark.conf.get("spark.executor.cores", "1"))
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
    discover = discover_executors or (
        lambda session, minimum: discover_active_executors(
            session, minimum_executors=minimum
        )
    )
    executors = tuple(discover(spark, configured_instances))
    if probe_peak_rss_bytes is None:
        rss_result = probe_executor_peak_rss_bytes(
            spark, executors, warm_up=warm_up
        )
    else:
        rss_result = probe_peak_rss_bytes(spark, executors)
    if rss_result.status is not CapabilityStatus.AVAILABLE:
        raise RuntimeError(
            "benchmark profile promotion requires complete warmed executor "
            f"RSS evidence: {rss_result.evidence}"
        )
    peak_rss_bytes = rss_result.value
    return build_profile_from_inventory(
        (
            "candidate-a-benchmark-f64-fixed-v1"
            if cpu_profile is None
            else f"candidate-a-benchmark-f64-fixed-v1:{cpu_profile.profile_id}"
        ),
        executors,
        task_cpus=task_cpus,
        expected_executor_cores=expected_executor_cores,
        executor_memory_bytes=memory_bytes,
        memory_reserve_bytes=min(4 * GIB, memory_bytes // 4),
        heartbeat_seconds=10.0,
        minimum_speed_x=0.01,
        lease_safety_factor=1.25,
        lease_margin_seconds=60.0,
        peak_rss_bytes=peak_rss_bytes,
        rss_headroom_fraction=0.20,
    )


def process_main(argv: Sequence[str] | None = None) -> int:
    """Run one explicit benchmark batch on the fixed five-executor live pool.

    The body is dispatched through a bounded try/except so an uncaught crash
    (observed live as a generic ``state=[dead]`` Spark failure with zero
    driver-log evidence, because Fabric's log-fetch API 404s for apps that
    die before YARN log-aggregation completes) still leaves a diagnosable
    OneLake traceback, mirroring the proven control-dispatch pattern.
    """
    from people_counter.fabric_candidate_a_jobs import _write_control_diagnostic

    entry_config = FabricCandidateAConfig.benchmark()
    _write_process_stage_marker(entry_config, "entry", 0, "main-entry")

    parser = argparse.ArgumentParser(prog="pc-benchmark-process-sjd")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--mode", choices=("sdk",), default="sdk")
    parser.add_argument(
        "--profile-id",
        choices=tuple(CPU_PROFILES),
        default="pytorch-r18-b1-1fps-1t",
    )
    parser.add_argument("--release-manifest-path", default="")
    parser.add_argument("--release-manifest-sha256", default="")
    parser.add_argument("--release-receipt-path", default="")
    parser.add_argument("--release-receipt-sha256", default="")
    arguments = parser.parse_args(argv)
    config = FabricCandidateAConfig.benchmark()
    try:
        return _process_dispatch(arguments, config)
    except Exception as error:
        _write_control_diagnostic(
            config, f"process-{arguments.batch_id}", error, prefix="process"
        )
        raise


def _write_process_stage_marker(
    config: FabricCandidateAConfig, batch_id: str, sequence: int, stage: str
) -> None:
    """Best-effort bisection checkpoint for the live ``state=[dead]`` crash.

    Fabric's driver-log-fetch API 404s for apps that die before YARN log
    aggregation completes, and a genuine native/JVM crash (segfault, OOM
    kill) can terminate the process before any Python exception handler
    (including the ``_process_dispatch`` try/except) ever runs. Writing a
    sequence-numbered marker immediately before each risky step lets a
    post-mortem OneLake listing identify the last stage reached even with
    zero traceback, mirroring the proven migration-tool stage-marker
    pattern. Any failure here is swallowed; it must never affect dispatch.
    """
    try:
        import notebookutils

        path = config.file_path(f"process/stages/{batch_id}/{sequence:02d}-{stage}.json")
        payload = json.dumps(
            {"batch_id": batch_id, "sequence": sequence, "stage": stage},
            allow_nan=False,
            sort_keys=True,
        )
        notebookutils.fs.put(path, payload, True)
    except Exception:  # pragma: no cover - diagnostics must never mask errors
        return


def _process_dispatch(
    arguments: argparse.Namespace, config: FabricCandidateAConfig
) -> int:
    """Pure extraction of ``process_main``'s body for a thin, covered wrapper."""

    from people_counter.fabric_candidate_a_jobs import (
        _load_runtime_release_evidence,
        _bind_consumer_probe_to_executor_inventory,
        _localize_process_inputs,
        _probe_and_persist_consumer_decision,
        _spark,
        select_process_execution_harness,
    )
    from people_counter.sjd_control import FabricControlStore
    from people_counter.sjd_process import OneLakeDeltaAttemptAdapter, run_process_batch
    from people_counter.fabric_candidate_a_control import (
        NotebookUtilsOneLakeFiles,
    )

    batch_id = arguments.batch_id
    _write_process_stage_marker(config, batch_id, 0, "bootstrap")
    spark = _spark()
    _write_process_stage_marker(config, batch_id, 1, "spark-ready")
    cpu_profile = CPU_PROFILES[arguments.profile_id]
    config.require_write_enabled()
    store = FabricControlStore(spark, config=config)
    envelope, _ = store.load_claim_envelope_with_digest(batch_id)
    _write_process_stage_marker(config, batch_id, 2, "envelope-loaded")
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
    release_evidence = _load_runtime_release_evidence(
        NotebookUtilsOneLakeFiles(),
        manifest_path=getattr(arguments, "release_manifest_path", ""),
        manifest_sha256=getattr(arguments, "release_manifest_sha256", ""),
        receipt_path=getattr(arguments, "release_receipt_path", ""),
        receipt_sha256=getattr(arguments, "release_receipt_sha256", ""),
    )
    consumer_result, consumer_decision = _probe_and_persist_consumer_decision(
        spark,
        config,
        batch_id,
        envelope,
    )
    fixed_probe = _bind_consumer_probe_to_executor_inventory(
        consumer_result,
        consumer_decision["executor_ids"],
    )
    _write_process_stage_marker(config, batch_id, 3, "payload-validated")
    enrichment, resolver_capability = _localize_process_inputs(
        spark,
        store,
        batch_id,
        probe_mounted_path=fixed_probe,
    )
    _write_process_stage_marker(config, batch_id, 4, "inputs-localized")
    import functools
    from people_counter.fabric_candidate_a_jobs import _executor_warm_works
    from people_counter.sjd_process import warm_executor_for_works

    task_cpus = int(spark.conf.get("spark.task.cpus", "1"))
    executor_cores = int(spark.conf.get("spark.executor.cores", "1"))
    warm_works = _executor_warm_works(
        envelope,
        enrichment,
        executor_cores=executor_cores,
        task_cpus=task_cpus,
        package_version=release_evidence.manifest.package_version,
        manifest_sha256=release_evidence.manifest_sha256,
    )
    for values in enrichment.values():
        values["_expected_release_package_version"] = (
            release_evidence.manifest.package_version
        )
        values["_expected_release_manifest_sha256"] = (
            release_evidence.manifest_sha256
        )
    profile = _benchmark_execution_profile(
        spark,
        cpu_profile,
        warm_up=functools.partial(warm_executor_for_works, warm_works),
    )
    _write_process_stage_marker(config, batch_id, 5, "profile-built")
    harness, harness_capability = select_process_execution_harness(
        spark,
        config,
        batch_id,
        row_enrichment=enrichment,
        probe_mounted_path=fixed_probe,
    )
    result = run_process_batch(
        store,
        batch_id,
        profile,
        arguments.mode,
        harness,
        OneLakeDeltaAttemptAdapter(
            config.file_path("attempts"),
            spark,
            config=config,
        ),
        peak_rss_bytes=profile.peak_rss_bytes,
        release_evidence=release_evidence,
    )
    _write_process_stage_marker(config, batch_id, 6, "batch-completed")
    payload = asdict(result)
    payload["staging_path"] = str(payload["staging_path"])
    payload["harness_capability"] = harness_capability
    payload["resolver_capability"] = resolver_capability
    payload["consumer_probe_decision"] = consumer_decision
    payload["release_evidence"] = {
        "identity_sha256": release_evidence.identity_sha256,
        "manifest_sha256": release_evidence.manifest_sha256,
        "receipt_sha256": release_evidence.receipt_sha256,
        "environment_target_version": (
            release_evidence.receipt.environment_target_version
        ),
    }
    payload["cpu_profile"] = {
        **asdict(cpu_profile),
        "equivalence_label": cpu_profile.equivalence_label,
        "sha256": cpu_profile.sha256,
    }
    print(json.dumps(payload, allow_nan=False, sort_keys=True))
    return 0


def gold_main(argv: Sequence[str] | None = None) -> int:
    """Run benchmark-only gold; production views and pointers are unreachable.

    Wrapped in the same bounded try/except diagnostic-capture pattern as
    ``process_main`` and ``fabric_candidate_a_jobs.control_main`` so an
    uncaught gold-reconcile crash still leaves an OneLake traceback.
    """

    from people_counter.fabric_candidate_a_jobs import _write_control_diagnostic
    from people_counter.fabric_candidate_a_jobs import gold_main as candidate_main

    config = FabricCandidateAConfig.benchmark()
    try:
        return candidate_main(argv, config=config)
    except Exception as error:
        _write_control_diagnostic(config, "gold", error, prefix="gold")
        raise
