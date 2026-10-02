"""Command-line control plane for the local Spark development slice."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import socket
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from people_counter.local_control import run_claimed_batch
from people_counter.local_queue import SQLiteQueue, SerializedQueueActor
from people_counter.local_spark import (
    create_local_spark_session,
    process_claimed_batch,
    runtime_identity,
)
from people_counter.local_storage import ContentAddressedStore


class JsonFormatter(logging.Formatter):
    """Emit stable one-event-per-line JSON logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": getattr(record, "event", record.getMessage()),
        }
        for name in (
            "batch_id",
            "correlation_id",
            "manifest_sha256",
            "record_count",
            "runtime_identity",
        ):
            value = getattr(record, name, None)
            if value is not None:
                payload[name] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def configure_json_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="people-counter-local",
        description="Control the local SQLite-to-Spark development slice.",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=Path(
            os.environ.get(
                "PEOPLE_COUNTER_QUEUE_DB",
                "/var/lib/people-counter/control/queue.sqlite3",
            )
        ),
    )
    parser.add_argument(
        "--content-root",
        type=Path,
        default=Path(
            os.environ.get(
                "PEOPLE_COUNTER_CONTENT_ROOT",
                "/data/output/local-content",
            )
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="Create or validate the queue schema.")

    status = subparsers.add_parser("status", help="Print queue state as JSON.")
    status.add_argument(
        "--state",
        choices=("ready", "locked", "completed", "dead"),
    )

    recover = subparsers.add_parser(
        "recover",
        help="Recover expired locks and print the recovered count.",
    )
    recover.add_argument("--now", type=float)

    seed = subparsers.add_parser("seed", help="Enqueue immutable local video work.")
    seed.add_argument("--video", type=Path, required=True)
    seed.add_argument("--idempotency-prefix", required=True)
    seed.add_argument("--copies", type=_positive_int, default=1)
    seed.add_argument(
        "--pipeline",
        choices=("rtdetr-osnet", "rfdetr-botsort"),
        default="rtdetr-osnet",
    )
    seed.add_argument("--models-dir", type=Path)
    seed.add_argument("--probe-delay-seconds", type=float, default=2.0)
    seed.add_argument("--max-delivery-count", type=_positive_int, default=5)

    submit = subparsers.add_parser(
        "submit",
        help="Claim one batch and run it as one Spark application.",
    )
    submit.add_argument("--max-items", type=_positive_int, default=2)
    submit.add_argument("--lock-seconds", type=float, default=120)
    submit.add_argument("--heartbeat-seconds", type=float, default=30)
    submit.add_argument(
        "--mode",
        choices=("probe", "sdk"),
        default="probe",
    )
    submit.add_argument("--minimum-workers", type=_positive_int, default=1)
    submit.add_argument(
        "--master",
        default=os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
    )
    submit.add_argument(
        "--staging-root",
        type=Path,
        default=Path("/data/output/delta-staging"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    configure_json_logging()
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "init":
        SQLiteQueue(args.database)
        _print_json({"initialized": True, "database": str(args.database)})
        return 0
    if args.command == "status":
        queue = SQLiteQueue(args.database)
        _print_json(
            {
                "messages": [
                    {
                        "message_id": message.message_id,
                        "idempotency_key": message.idempotency_key,
                        "state": message.state,
                        "delivery_count": message.delivery_count,
                        "max_delivery_count": message.max_delivery_count,
                        "available_at": message.available_at,
                        "locked_until": message.locked_until,
                        "dead_letter_reason": message.dead_letter_reason,
                    }
                    for message in queue.list(state=args.state)
                ]
            }
        )
        return 0
    if args.command == "recover":
        recovered = SQLiteQueue(args.database).recover_expired(now=args.now)
        _print_json({"recovered": recovered})
        return 0
    if args.command == "seed":
        return _seed(args, parser)
    if args.command == "submit":
        if args.minimum_workers > args.max_items:
            parser.error("--minimum-workers cannot exceed --max-items")
        return _submit(args)
    parser.error(f"unsupported command: {args.command}")


def _seed(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    video = args.video.expanduser().resolve()
    if not video.is_file():
        parser.error(f"video does not exist: {video}")
    if args.probe_delay_seconds < 0 or args.probe_delay_seconds > 60:
        parser.error("--probe-delay-seconds must be between 0 and 60")
    queue = SQLiteQueue(args.database)
    source_sha256 = _file_sha256(video)
    message_ids = []
    for index in range(args.copies):
        payload = {
            "source_video": str(video),
            "source_sha256": source_sha256,
            "pipeline": args.pipeline,
            "models_dir": (
                str(args.models_dir.expanduser().resolve())
                if args.models_dir is not None
                else None
            ),
            "probe_delay_seconds": args.probe_delay_seconds,
        }
        message_ids.append(
            queue.enqueue(
                f"{args.idempotency_prefix}:{index}",
                payload,
                max_delivery_count=args.max_delivery_count,
            )
        )
    _print_json(
        {
            "message_ids": message_ids,
            "source_sha256": source_sha256,
        }
    )
    return 0


def _submit(args: argparse.Namespace) -> int:
    if args.minimum_workers > args.max_items:
        raise ValueError("minimum_workers cannot exceed max_items")
    logger = logging.getLogger(__name__)
    owner = f"{socket.gethostname()}:{os.getpid()}"
    store = ContentAddressedStore(args.content_root)
    spark_holder: dict[str, Any] = {}

    def runner(
        manifest_path: Path,
        manifest_sha256: str,
        correlation_id: str,
    ):
        spark = create_local_spark_session(
            master=args.master,
            app_name=f"people-counter-local-{correlation_id}",
            correlation_id=correlation_id,
        )
        spark_holder["spark"] = spark
        logger.info(
            "local_runtime_identity",
            extra={
                "event": "local_runtime_identity",
                "correlation_id": correlation_id,
                "runtime_identity": runtime_identity(spark),
            },
        )
        return process_claimed_batch(
            spark,
            manifest_path,
            args.staging_root,
            expected_manifest_sha256=manifest_sha256,
            processor_mode=args.mode,
            minimum_executor_identities=args.minimum_workers,
        )

    try:
        with SerializedQueueActor(args.database) as actor:
            result = run_claimed_batch(
                actor,
                store,
                runner,
                owner=owner,
                max_items=args.max_items,
                minimum_items=args.minimum_workers,
                lock_seconds=args.lock_seconds,
                heartbeat_seconds=args.heartbeat_seconds,
                staging_root=args.staging_root,
            )
    finally:
        spark = spark_holder.get("spark")
        if spark is not None:
            spark.stop()
    output: dict[str, Any] = {
        "batch_id": result.batch_id,
        "correlation_id": result.correlation_id,
        "claimed_count": result.claimed_count,
        "manifest_sha256": (
            result.manifest.sha256 if result.manifest is not None else None
        ),
    }
    if result.staging is not None:
        output.update(
            {
                "staging_path": str(result.staging.path),
                "record_count": result.staging.record_count,
                "executor_identities": result.staging.executor_identities,
                "release_digests": result.staging.release_digests,
                "failed_work_ids": result.staging.failed_work_ids,
            }
        )
    _print_json(output)
    return 0 if result.staging is None or not result.staging.failed_work_ids else 2


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def _print_json(value: Mapping[str, Any]) -> None:
    print(json.dumps(value, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main())
