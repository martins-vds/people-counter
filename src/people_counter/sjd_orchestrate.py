"""End-to-end local orchestrator for the Candidate A Spark job suite."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Sequence

from people_counter.sjd_control import SQLiteControlStore
from people_counter.sjd_gold import LocalGoldJob, PathDeltaGoldStore
from people_counter.sjd_process import (
    DirectExecutionHarness,
    LocalJsonAttemptAdapter,
    SparkDeltaAttemptAdapter,
    SparkExecutionHarness,
    resolve_profile,
    run_process_batch,
)


@dataclass(frozen=True)
class LocalPipelinePaths:
    """Resolve all environment-specific local paths from one typed root."""

    root: Path

    @classmethod
    def resolve(cls, root: Path) -> LocalPipelinePaths:
        return cls(Path(root).expanduser().resolve())

    @property
    def control_database(self) -> Path:
        return self.root / "control" / "control.sqlite3"

    @property
    def content_root(self) -> Path:
        return self.root / "content"

    @property
    def staging_root(self) -> Path:
        return self.root / "attempts"

    @property
    def gold_root(self) -> Path:
        return self.root / "gold"


@dataclass(frozen=True)
class LocalPipelineConfig:
    paths: LocalPipelinePaths
    mode: Literal["probe", "sdk"]
    profile: str = "local-two-workers"
    harness: Literal["direct", "spark"] = "direct"
    max_items: int = 2
    lease_seconds: float = 600.0
    spark_master: str = "spark://spark-master:7077"
    acknowledge_refresh: bool = True


@dataclass(frozen=True)
class _ExecutionRuntime:
    spark: Any | None
    harness: Any
    attempts: Any


def run_local_pipeline(
    config: LocalPipelineConfig,
    *,
    manifest: Path | None = None,
    fixture_count: int = 0,
) -> dict[str, Any]:
    """Run bootstrap through gold and local semantic-refresh acknowledgement."""
    if (manifest is None) == (fixture_count == 0):
        raise ValueError("provide exactly one of manifest or a positive fixture_count")
    if fixture_count < 0 or fixture_count > 100:
        raise ValueError("fixture_count must be between 0 and 100")
    if config.max_items < 1 or config.max_items > 100:
        raise ValueError("max_items must be between 1 and 100")
    if config.mode == "sdk" and fixture_count:
        raise ValueError("SDK mode requires an explicit immutable manifest")

    paths = config.paths
    store = SQLiteControlStore(paths.control_database, paths.content_root)
    if manifest is not None:
        registered = store.register_manifest(manifest)
    else:
        registered = tuple(
            store.register(
                f"probe-{index:04d}",
                _probe_payload(index),
                runtime_key="probe:rtdetr-osnet:cpu",
                duration_seconds=1.0,
                config_sha256=_digest("probe-config-v1"),
                release_digest=os.environ.get(
                    "PEOPLE_COUNTER_RELEASE_DIGEST", "development"
                ),
                max_attempts=3,
            )
            for index in range(fixture_count)
        )
    runtime = _open_execution_runtime(config)
    try:
        requested_ids = {work.work_id for work in registered}
        process_results = _run_requested_batches(
            config,
            store,
            requested_ids,
            runtime,
        )
        process = process_results[-1]
        batch_id = process.batch_id
        executor_identities = sorted(
            {
                identity
                for item in process_results
                for identity in _executor_identities(
                    runtime.attempts, item.batch_id, item.process_attempt_id
                )
            }
        )
        findings = store.reconcile()
        gold, pending, acknowledged = _run_gold(config, runtime.spark)
    finally:
        if runtime.spark is not None:
            runtime.spark.stop()
    return {
        "paths": {
            "root": str(paths.root),
            "control_database": str(paths.control_database),
            "staging_root": str(paths.staging_root),
            "gold_root": str(paths.gold_root),
        },
        "registered_work_ids": [work.work_id for work in registered],
        "batch_id": batch_id,
        "process": {
            **asdict(process),
            "staging_path": str(process.staging_path),
        },
        "processes": [
            {**asdict(item), "staging_path": str(item.staging_path)}
            for item in process_results
        ],
        "executor_identities": executor_identities,
        "reconciliation_findings": [asdict(item) for item in findings],
        "gold": gold,
        "refresh": {
            "pending_before_ack": [int(item["outbox_id"]) for item in pending],
            "acknowledged": acknowledged,
        },
        "status": store.status(),
    }


def _open_execution_runtime(config: LocalPipelineConfig) -> _ExecutionRuntime:
    if config.harness == "direct":
        return _ExecutionRuntime(
            None,
            DirectExecutionHarness(),
            LocalJsonAttemptAdapter(config.paths.staging_root),
        )
    from people_counter.local_spark import create_local_spark_session

    spark = create_local_spark_session(
        master=config.spark_master,
        app_name="people-counter-candidate-a-pipeline",
        correlation_id="pc-local-orchestrate",
    )
    return _ExecutionRuntime(
        spark,
        SparkExecutionHarness(spark),
        SparkDeltaAttemptAdapter(config.paths.staging_root, spark),
    )


def _run_requested_batches(
    config: LocalPipelineConfig,
    store: SQLiteControlStore,
    requested_ids: set[str],
    runtime: _ExecutionRuntime,
) -> list[Any]:
    profile = resolve_profile(config.profile)
    results = []
    while remaining_ids := {
        work_id
        for work_id in requested_ids
        if store.get_work(work_id).status == "READY"
    }:
        batch = store.claim(
            "pc-local-orchestrate",
            max_items=min(config.max_items, len(remaining_ids)),
            minimum_items=1,
            lease_seconds=config.lease_seconds,
            allowed_work_ids=remaining_ids,
            process_profile=profile,
        )
        if batch is None:
            raise RuntimeError("ready registered work could not form a homogeneous claim")
        claimed_ids = {item.work_id for item in batch.items}
        if not claimed_ids or not claimed_ids <= remaining_ids:
            raise RuntimeError("control store returned work outside the requested run scope")
        results.append(
            run_process_batch(
                store,
                batch.batch_id,
                profile,
                config.mode,
                runtime.harness,
                runtime.attempts,
            )
        )
    if results:
        return results
    results = [
        run_process_batch(
            store,
            batch_id,
            profile,
            config.mode,
            runtime.harness,
            runtime.attempts,
        )
        for batch_id in _committed_batches(config.paths.control_database, requested_ids)
    ]
    if not results:
        raise RuntimeError(
            "registered work is not ready and has no committed batch to resume"
        )
    return results


def _run_gold(
    config: LocalPipelineConfig,
    spark: Any | None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[int]]:
    paths = config.paths
    job = LocalGoldJob(
        paths.control_database,
        paths.gold_root,
        store=PathDeltaGoldStore(paths.gold_root, spark) if spark is not None else None,
    )
    gold = job.run()
    pending = job.state.pending_refreshes()
    acknowledged = [
        outbox_id
        for item in pending
        if (outbox_id := int(item["outbox_id"]))
        and config.acknowledge_refresh
        and job.state.acknowledge_refresh(outbox_id, actor="pc-local-orchestrate")
    ]
    return gold, pending, acknowledged


def _committed_batches(database: Path, work_ids: set[str]) -> list[str]:
    if not work_ids:
        return []
    placeholders = ",".join("?" for _ in work_ids)
    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            f"""
            SELECT DISTINCT b.batch_id, b.committed_at
            FROM batches b
            JOIN batch_members m ON m.batch_id = b.batch_id
            WHERE b.status = 'COMMITTED'
              AND m.work_id IN ({placeholders})
            ORDER BY b.committed_at, b.batch_id
            """,
            tuple(sorted(work_ids)),
        ).fetchall()
    return [str(row[0]) for row in rows]


def _executor_identities(
    attempts: LocalJsonAttemptAdapter | SparkDeltaAttemptAdapter,
    batch_id: str,
    process_attempt_id: str,
) -> list[str]:
    records = attempts.read_records(batch_id, process_attempt_id)
    return sorted({str(record["executor_identity"]) for record in records})


def _probe_payload(index: int) -> dict[str, Any]:
    return {
        "source_video": f"/probe/fixture-{index:04d}.mp4",
        "pipeline": "rtdetr-osnet",
        "batch_size": 1,
        "probe_delay_seconds": 0.01,
        "captured_at_utc": "2026-01-01T00:00:00Z",
        "camera_id": f"probe-camera-{index % 2}",
        "location_id": "local-probe",
        "camera_timezone": "UTC",
        "asset_id": f"probe-asset-{index:04d}",
        "asset_version": "v1",
    }


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    pipeline = commands.add_parser("pipeline")
    pipeline.add_argument(
        "--root",
        type=Path,
        default=Path(
            os.environ.get("PEOPLE_COUNTER_PIPELINE_ROOT", ".people-counter/pipeline")
        ),
    )
    source = pipeline.add_mutually_exclusive_group(required=True)
    source.add_argument("--manifest", type=Path)
    source.add_argument("--fixture-count", type=int)
    pipeline.add_argument("--mode", choices=("probe", "sdk"), default="probe")
    pipeline.add_argument("--profile", default="local-two-workers")
    pipeline.add_argument("--harness", choices=("direct", "spark"), default="direct")
    pipeline.add_argument("--max-items", type=int, default=2)
    pipeline.add_argument("--lease-seconds", type=float, default=600.0)
    pipeline.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
    )
    pipeline.add_argument("--no-ack-refresh", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    config = LocalPipelineConfig(
        LocalPipelinePaths.resolve(arguments.root),
        arguments.mode,
        arguments.profile,
        arguments.harness,
        arguments.max_items,
        arguments.lease_seconds,
        arguments.spark_master,
        not arguments.no_ack_refresh,
    )
    result = run_local_pipeline(
        config,
        manifest=arguments.manifest,
        fixture_count=arguments.fixture_count or 0,
    )
    print(json.dumps(result, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
