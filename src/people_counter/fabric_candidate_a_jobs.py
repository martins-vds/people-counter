"""Installed-wheel entry points for Candidate A Fabric Spark jobs."""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import re
import sys
from dataclasses import asdict
from typing import Any, Sequence

from people_counter.fabric_candidate_a import (
    LAKEHOUSE_ID,
    WORKSPACE_ID,
    FabricCandidateAConfig,
)


def _spark() -> Any:
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is None:
        session = SparkSession.builder.getOrCreate()
    return session


def _fabric_process_profile(spark: Any) -> Any:
    """Derive one whole-video task from the reviewed Runtime 2.0 live pool."""
    from people_counter.sjd_process import ExecutionProfile

    if sys.version_info[:2] != (3, 13):
        raise RuntimeError(f"Candidate A requires Python 3.13, got {sys.version}")
    if not str(spark.version).startswith("4.1.1"):
        raise RuntimeError(f"Candidate A requires Spark 4.1.1, got {spark.version}")
    java = str(
        spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
    )
    if not java.startswith("21"):
        raise RuntimeError(f"Candidate A requires Java 21, got {java}")
    if str(spark.conf.get("spark.dynamicAllocation.enabled", "")).lower() != "true":
        raise RuntimeError("Candidate A requires the reviewed dynamic live pool")
    if str(spark.conf.get("spark.speculation", "false")).lower() != "false":
        raise RuntimeError("Candidate A requires Spark speculation disabled")
    live_cores = int(spark.conf.get("spark.executor.cores"))
    if live_cores < 1:
        raise RuntimeError("Candidate A requires at least one executor core")
    memory = str(spark.conf.get("spark.executor.memory")).lower()
    match = re.fullmatch(r"(\d+)([gmk])", memory)
    if match is None:
        raise RuntimeError(f"unsupported Fabric executor memory {memory!r}")
    scale = {"k": 1024, "m": 1024**2, "g": 1024**3}[match.group(2)]
    memory_bytes = int(match.group(1)) * scale
    return ExecutionProfile(
        name="candidate-a-fabric-runtime2-v1",
        executor_instances=1,
        executor_cores=1,
        executor_memory_bytes=memory_bytes,
        task_cpus=1,
        fixed_allocation=False,
        speculation=False,
        memory_reserve_bytes=min(4 * 1024**3, memory_bytes // 4),
        heartbeat_seconds=10.0,
        minimum_speed_x=0.05,
        lease_safety_factor=1.25,
        lease_margin_seconds=60.0,
    )


def _localize_process_inputs(
    spark: Any,
    store: Any,
    batch_id: str,
) -> dict[str, dict[str, Any]]:
    """Distribute immutable OneLake video/model files to executors."""
    from pyspark import SparkFiles

    envelope, _ = store.load_claim_envelope_with_digest(batch_id)
    enrichment: dict[str, dict[str, Any]] = {}
    for item in envelope["items"]:
        payload = item["payload"]
        source = str(payload["source_video"])
        mount_prefix = "/lakehouse/default/"
        if not source.startswith(mount_prefix):
            raise ValueError("Candidate A source_video must use the default Lakehouse")
        relative = source[len(mount_prefix) :]
        spark.sparkContext.addFile(
            f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
            f"{LAKEHOUSE_ID}/{relative}"
        )
        source_name = source.rsplit("/", 1)[-1]
        localized_source = SparkFiles.get(source_name)
        source_sha = hashlib.sha256(open(localized_source, "rb").read()).hexdigest()
        if source_sha != payload.get("source_sha256"):
            raise ValueError("localized video digest differs from registered input")

        detector = str(payload.get("detector_model", "r18"))
        model_format = str(payload.get("model_format", "pytorch"))
        if payload.get("pipeline", "rtdetr-osnet") != "rtdetr-osnet":
            raise ValueError("Candidate A v1 localizer supports RT-DETR/OSNet only")
        detector_dir = (
            "rtdetr_v2_r18vd" if detector == "r18" else "rtdetr_v2_r50vd"
        )
        detector_file = (
            "model.safetensors" if model_format == "pytorch" else "model.onnx"
        )
        reid_file = (
            "osnet_ain_x0_25.pt"
            if model_format == "pytorch"
            else "osnet_ain_x0_25.onnx"
        )
        model_paths = (
            f"rtdetr_osnet/{detector_dir}/config.json",
            f"rtdetr_osnet/{detector_dir}/preprocessor_config.json",
            f"rtdetr_osnet/{detector_dir}/{detector_file}",
            f"rtdetr_osnet/libre_reid_osnet/{reid_file}",
        )
        localized_models: dict[str, dict[str, str]] = {}
        for model_path in model_paths:
            spark.sparkContext.addFile(
                f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
                f"{LAKEHOUSE_ID}/Files/models/{model_path}"
            )
            name = model_path.rsplit("/", 1)[-1]
            localized = SparkFiles.get(name)
            localized_models[model_path] = {
                "localized_name": name,
                "sha256": hashlib.sha256(open(localized, "rb").read()).hexdigest(),
            }
        enrichment[str(item["work_id"])] = {
            "spark_localized_video_name": source_name,
            "spark_localized_models": localized_models,
        }
    return enrichment


def _control_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pc-fabric-control-sjd")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("bootstrap")
    register = commands.add_parser("register")
    register.add_argument("--work-id", required=True)
    payload = register.add_mutually_exclusive_group(required=True)
    payload.add_argument("--payload-json")
    payload.add_argument("--payload-base64")
    register.add_argument("--runtime-key", required=True)
    register.add_argument("--duration-seconds", required=True, type=float)
    register.add_argument("--config-sha256", required=True)
    register.add_argument("--release-digest", required=True)
    register.add_argument("--max-attempts", type=int, default=3)
    claim = commands.add_parser("claim")
    claim.add_argument("--owner", required=True)
    claim.add_argument("--work-id", action="append")
    claim.add_argument("--max-items", required=True, type=int)
    claim.add_argument("--lease-seconds", required=True, type=float)
    claim.add_argument("--minimum-speed-x", type=float, default=1.0)
    claim.add_argument("--safety-factor", type=float, default=1.25)
    claim.add_argument("--margin-seconds", type=float, default=30.0)
    claim.add_argument("--minimum-items", type=int, default=1)
    replay = commands.add_parser("replay")
    replay.add_argument("--work-id", required=True)
    replay.add_argument("--operator", required=True)
    replay.add_argument("--reason", required=True)
    replay.add_argument("--additional-attempts", type=int, default=1)
    recover = commands.add_parser("recover")
    recover.add_argument("--now", type=float)
    clear_lock = commands.add_parser("clear-stale-lock")
    clear_lock.add_argument("--expected-owner-id", required=True)
    commands.add_parser("reconcile")
    return parser


def _clear_stale_lock(spark: Any, expected_owner_id: str) -> dict[str, Any]:
    """Manually clear one investigated canary writer token by exact CAS."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions

    table = FabricCandidateAConfig().table("locks")
    rows = spark.table(table).select(
        "lock_name", "owner_id", "acquired_at"
    ).limit(2).collect()
    if (
        len(rows) != 1
        or rows[0]["lock_name"] != "global"
        or rows[0]["owner_id"] != expected_owner_id
        or rows[0]["acquired_at"] is None
    ):
        raise RuntimeError(
            "refusing stale-lock clear: exact owner/readback precondition failed"
        )
    acquired_at = rows[0]["acquired_at"]
    DeltaTable.forName(spark, table).update(
        condition=(
            (functions.col("lock_name") == "global")
            & (functions.col("owner_id") == expected_owner_id)
        ),
        set={
            "owner_id": functions.lit(None).cast("string"),
            "acquired_at": functions.lit(None).cast("timestamp"),
        },
    )
    spark.catalog.refreshTable(table)
    observed = spark.table(table).select(
        "lock_name", "owner_id", "acquired_at"
    ).limit(2).collect()
    if (
        len(observed) != 1
        or observed[0]["lock_name"] != "global"
        or observed[0]["owner_id"] is not None
        or observed[0]["acquired_at"] is not None
    ):
        raise RuntimeError("stale-lock clear exact readback failed")
    return {
        "cleared_owner_id": expected_owner_id,
        "acquired_at": str(acquired_at),
    }


def control_main(argv: Sequence[str] | None = None) -> int:
    """Run bounded control maintenance against the fixed canary tables."""
    arguments = _control_parser().parse_args(argv)
    from people_counter.sjd_control import FabricControlStore

    store = FabricControlStore(_spark(), config=FabricCandidateAConfig())
    if arguments.command == "bootstrap":
        result: object = {"bootstrapped": True}
    elif arguments.command == "register":
        encoded = arguments.payload_json
        if arguments.payload_base64 is not None:
            try:
                encoded = base64.urlsafe_b64decode(
                    arguments.payload_base64.encode("ascii")
                ).decode("utf-8")
            except (UnicodeError, ValueError, binascii.Error) as error:
                raise ValueError("--payload-base64 must be canonical UTF-8 JSON") from error
        payload = json.loads(encoded)
        if not isinstance(payload, dict):
            raise ValueError("--payload-json must decode to an object")
        result = asdict(
            store.register(
                arguments.work_id,
                payload,
                runtime_key=arguments.runtime_key,
                duration_seconds=arguments.duration_seconds,
                config_sha256=arguments.config_sha256,
                release_digest=arguments.release_digest,
                max_attempts=arguments.max_attempts,
            )
        )
    elif arguments.command == "claim":
        claimed = store.claim(
            arguments.owner,
            max_items=arguments.max_items,
            lease_seconds=arguments.lease_seconds,
            minimum_speed_x=arguments.minimum_speed_x,
            safety_factor=arguments.safety_factor,
            margin_seconds=arguments.margin_seconds,
            minimum_items=arguments.minimum_items,
            allowed_work_ids=arguments.work_id,
        )
        result = None if claimed is None else asdict(claimed)
    elif arguments.command == "replay":
        result = asdict(
            store.replay(
                arguments.work_id,
                operator=arguments.operator,
                reason=arguments.reason,
                additional_attempts=arguments.additional_attempts,
            )
        )
    elif arguments.command == "recover":
        result = asdict(store.recover(now=arguments.now))
    elif arguments.command == "clear-stale-lock":
        result = _clear_stale_lock(_spark(), arguments.expected_owner_id)
    else:
        result = [asdict(item) for item in store.reconcile()]
    print(json.dumps(result, allow_nan=False, default=str, sort_keys=True))
    return 0


def _process_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pc-fabric-process-sjd")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--mode", choices=("probe", "sdk"), default="sdk")
    parser.add_argument("--peak-rss-mib", type=int)
    return parser


def process_main(argv: Sequence[str] | None = None) -> int:
    """Run one claimed batch using only fixed Candidate A Fabric backends."""
    arguments = _process_parser().parse_args(argv)
    from people_counter.sjd_control import FabricControlStore
    from people_counter.sjd_process import (
        MIB,
        OneLakeDeltaAttemptAdapter,
        SparkExecutionHarness,
        run_process_batch,
    )

    spark = _spark()
    config = FabricCandidateAConfig()
    profile = _fabric_process_profile(spark)
    store = FabricControlStore(spark, config=config)
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
            config.file_path("attempts"), spark
        ),
        peak_rss_bytes=(
            None
            if arguments.peak_rss_mib is None
            else arguments.peak_rss_mib * MIB
        ),
    )
    payload = asdict(result)
    payload["staging_path"] = str(payload["staging_path"])
    print(json.dumps(payload, allow_nan=False, sort_keys=True))
    return 0


def _gold_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pc-fabric-gold-sjd")
    parser.add_argument(
        "mode",
        choices=("plan", "validate", "run"),
    )
    parser.add_argument("--lookback-hours", type=int, default=48)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--full-rebuild", action="store_true")
    return parser


def gold_main(argv: Sequence[str] | None = None) -> int:
    """Build gold through committed pointers and typed Delta tables."""
    arguments = _gold_parser().parse_args(argv)
    from people_counter.fabric_candidate_a_gold import (
        FabricCommittedSource,
        FabricGoldJob,
        FabricGoldState,
        FabricGoldStoreImpl,
    )
    from people_counter.sjd_process import OneLakeDeltaAttemptAdapter

    spark = _spark()
    config = FabricCandidateAConfig()
    source = FabricCommittedSource(
        spark,
        OneLakeDeltaAttemptAdapter(
            config.file_path("attempts"), spark
        ),
        config=config,
    )
    job = FabricGoldJob(
        source,
        FabricGoldStoreImpl(spark, config=config),
        FabricGoldState(spark, config=config),
    )
    if arguments.mode == "plan":
        result: object = job.plan(
            lookback_hours=arguments.lookback_hours,
            full_rebuild=arguments.full_rebuild,
            reset=arguments.force,
        ).to_dict()
    elif arguments.mode == "validate":
        result = job.validate()
    else:
        result = job.run(
            lookback_hours=arguments.lookback_hours,
            force=arguments.force,
            full_rebuild=arguments.full_rebuild,
        )
    print(json.dumps(result, allow_nan=False, sort_keys=True))
    return 0
