"""Durable worker commands applied by the Lakehouse's exclusive metadata writer.

Spark and Delta are imported only at the storage boundary. A receipt is the
acknowledgement, not the inbox append. Failed storage operations deliberately
escape the writer callback so its durable lock remains held for safe recovery.
"""

from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from threading import Lock
from typing import Any
from uuid import uuid4


class LeaseLostError(RuntimeError):
    """The authoritative writer rejected a worker's ownership or state."""


class _Rejected(Exception):
    pass


_ACTIVE = ("LEASED", "STAGING", "RUNNING", "WRITING")
_KINDS = {
    "claim_execution",
    "heartbeat",
    "attempt_update",
    "commit",
    "failure",
    "release",
}
_RECEIPT_TIMEOUT_SECONDS = 600
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_TEXT_FIELDS = {
    "pipeline_run_id", "activity_run_id", "fabric_job_instance_id", "sdk_version",
    "bundle_manifest_sha256", "input_sha256", "error_category", "error_type",
    "error_message",
}
_TIME_FIELDS = {
    "staging_started_at", "inference_started_at", "writing_started_at",
    "completed_at", "last_heartbeat_at",
}
_INT_FIELDS = {
    "source_size_bytes", "total_source_frames", "processed_frames",
    "distinct_people", "line_in_count", "line_out_count",
}
_FLOAT_FIELDS = {
    "source_duration_seconds", "source_fps", "effective_sample_fps",
    "processing_seconds",
}
_UPDATE_FIELDS = _TEXT_FIELDS | _TIME_FIELDS | _INT_FIELDS | _FLOAT_FIELDS | {
    "status", "retryable",
}
_FAILURE_FIELDS = {
    "error_category", "error_type", "error_message", "retryable", "lease_lost",
    "processed_frames", "processing_seconds",
}
_LEASE_CLEAR = {
    "lease_owner_attempt_id": None, "lease_dispatcher_id": None,
    "lease_acquired_at": None, "lease_expires_at": None,
}
_EVENT_IDENTITY_SCHEMA = {
    "event_id": "string", "work_id": "string", "attempt_id": "string",
    "worker_execution_id": "string", "sequence": "bigint",
}
_REQUIRED_SCHEMAS = {
    "worker_events": {
        **_EVENT_IDENTITY_SCHEMA, "capture_date": "date", "event_kind": "string",
        "payload_json": "string", "created_at": "timestamp",
    },
    "worker_event_receipts": {
        **_EVENT_IDENTITY_SCHEMA, "outcome": "string", "message": "string",
        "applied_at": "timestamp",
    },
    "video_work": {
        "work_id": "string", "capture_date": "date", "config_sha256": "string",
        "status": "string", "attempt_count": "int", "max_attempts": "int",
        "lease_owner_attempt_id": "string", "lease_dispatcher_id": "string",
        "lease_acquired_at": "timestamp", "lease_expires_at": "timestamp",
        "last_heartbeat_at": "timestamp", "committed_attempt_id": "string",
        "completed_at": "timestamp", "not_before_at": "timestamp",
        "queue_entered_at": "timestamp", "last_error_category": "string",
        "last_error_type": "string", "last_error_message": "string",
    },
    "video_attempts": {
        **dict.fromkeys(_TEXT_FIELDS, "string"),
        **dict.fromkeys(_TIME_FIELDS, "timestamp"),
        **dict.fromkeys(_INT_FIELDS, "bigint"),
        **dict.fromkeys(_FLOAT_FIELDS, "double"),
        "work_id": "string", "attempt_id": "string", "worker_execution_id": "string",
        "capture_date": "date", "dispatcher_id": "string", "config_sha256": "string",
        "status": "string", "claimed_at": "timestamp", "retryable": "boolean",
    },
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp(value: Any) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime):
        raise ValueError("Expected an ISO datetime or datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _positive_int(value: Any, name: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _tables(table_prefix: str, database: str) -> dict[str, str]:
    if not isinstance(table_prefix, str) or not _IDENTIFIER.fullmatch(table_prefix):
        raise ValueError("table_prefix must be a SQL identifier")
    if not isinstance(database, str) or (database and not _IDENTIFIER.fullmatch(database)):
        raise ValueError("database must be empty or a SQL identifier")
    prefix = f"{database}." if database else ""
    return {
        suffix: f"{prefix}{table_prefix}_{suffix}"
        for suffix in ("video_work", "video_attempts", "worker_events", "worker_event_receipts")
    }


def _table_schema(spark_session: Any, name: str, suffix: str) -> Any:
    if not spark_session.catalog.tableExists(name):
        raise RuntimeError(f"Required Lakehouse table {name} is missing; run offline bootstrap first")
    metadata = spark_session.catalog.getTable(name)
    if metadata.isTemporary or metadata.tableType not in ("MANAGED", "EXTERNAL"):
        raise RuntimeError(f"Lakehouse table {name} must be a permanent bootstrap table, not a view")
    schema = spark_session.table(name).schema
    fields = schema.fields
    if len({column.name.casefold() for column in fields}) != len(fields):
        raise RuntimeError(f"Lakehouse table {name} has ambiguous column names; repair it offline")
    actual = {column.name: column.dataType.simpleString() for column in fields}
    for column, expected in _REQUIRED_SCHEMAS[suffix].items():
        if actual.get(column) != expected:
            raise RuntimeError(
                f"Lakehouse table {name} requires {column} {expected}; "
                f"found {actual.get(column, 'missing')}. Run offline bootstrap/migration first"
            )
    return schema


def _validate_tables(spark_session: Any, tables: dict[str, str]) -> dict[str, Any]:
    return {
        suffix: _table_schema(spark_session, name, suffix)
        for suffix, name in tables.items()
    }


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Unsupported event value: {type(value).__name__}")


def _validate_updates(updates: Any) -> None:
    if not isinstance(updates, dict) or not updates.keys() <= _UPDATE_FIELDS:
        raise ValueError("updates contains unsupported attempt columns")
    for name, value in updates.items():
        if value is None:
            if name == "status":
                raise ValueError("status cannot be null")
            continue
        if name in _TIME_FIELDS:
            _timestamp(value)
        elif name in _INT_FIELDS:
            if type(value) is not int or not 0 <= value <= 2**63 - 1:
                raise ValueError(f"{name} must be a non-negative signed 64-bit integer")
        elif name in _FLOAT_FIELDS:
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be a finite non-negative number")
        elif name == "retryable":
            if type(value) is not bool:
                raise ValueError("retryable must be boolean or null")
        elif not isinstance(value, str):
            raise ValueError(f"{name} must be a string or null")


def _validate_payload(kind: str, payload: Any) -> None:
    if not isinstance(kind, str) or kind not in _KINDS:
        raise ValueError("Unsupported worker event kind")
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dictionary")
    expected = {
        "claim_execution": set(), "commit": set(), "attempt_update": {"updates"},
        "heartbeat": {"status", "updates"}, "failure": _FAILURE_FIELDS,
        "release": {"reason"},
    }[kind]
    if payload.keys() != expected:
        raise ValueError(f"Unexpected payload fields for {kind}")
    if kind in {"heartbeat", "attempt_update"}:
        _validate_updates(payload["updates"])
        if any(payload["updates"].get(key) is not None for key in (
            "retryable", "error_category", "error_type", "error_message",
        )):
            raise ValueError("Only failure events may set error metadata")
    if kind == "heartbeat":
        if payload["status"] not in _ACTIVE:
            raise ValueError("heartbeat status must be active")
        if payload["updates"].get("status", payload["status"]) != payload["status"]:
            raise ValueError("heartbeat and attempt status must agree")
        if "completed_at" in payload["updates"]:
            raise ValueError("heartbeat cannot complete an attempt")
    if kind == "failure":
        for name in ("error_category", "error_type"):
            _text(payload[name], name)
        if not isinstance(payload["error_message"], str):
            raise ValueError("error_message must be a string")
        for name in ("retryable", "lease_lost"):
            if type(payload[name]) is not bool:
                raise ValueError(f"{name} must be boolean")
        _validate_updates({key: payload[key] for key in ("processed_frames", "processing_seconds")})
    if kind == "release":
        _text(payload["reason"], "reason")


@dataclass(frozen=True)
class _Command:
    event_id: str
    work_id: str
    attempt_id: str
    worker_execution_id: str
    capture_date: date
    sequence: int
    kind: str
    payload: dict[str, Any]
    created_at: datetime
    lease_minutes: int

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> _Command:
        envelope = json.loads(row["payload_json"])
        if not isinstance(envelope, dict) or envelope.keys() != {"payload", "lease_minutes"}:
            raise ValueError("Invalid worker event envelope")
        _validate_payload(row["event_kind"], envelope["payload"])
        if type(row["capture_date"]) is not date:
            raise ValueError("capture_date must be a date")
        return cls(
            *(_text(row[name], name) for name in (
                "event_id", "work_id", "attempt_id", "worker_execution_id",
            )),
            row["capture_date"], _positive_int(row["sequence"], "sequence"),
            row["event_kind"], envelope["payload"], _timestamp(row["created_at"]),
            _positive_int(envelope["lease_minutes"], "lease_minutes"),
        )


@dataclass
class _Plan:
    work: dict[str, Any] = field(default_factory=dict)
    attempt: dict[str, Any] = field(default_factory=dict)
    outcome: str = "APPLIED"
    message: str = "Applied"


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise _Rejected(message)


def _matches(row: dict[str, Any], updates: dict[str, Any]) -> bool:
    for key, value in updates.items():
        actual = row[key]
        if isinstance(value, datetime) and actual is not None:
            actual = _timestamp(actual)
        if actual != value:
            return False
    return True


def _identity(command: _Command, work: dict[str, Any], attempt: dict[str, Any]) -> None:
    _require(
        work["work_id"] == attempt["work_id"] == command.work_id
        and attempt["attempt_id"] == command.attempt_id
        and work["capture_date"] == attempt["capture_date"] == command.capture_date,
        "Work/attempt keys or capture_date do not match",
    )
    _require(
        bool(work["config_sha256"]) and work["config_sha256"] == attempt["config_sha256"],
        "Work/attempt configuration does not match",
    )


def _owner(command: _Command, work: dict[str, Any], attempt: dict[str, Any]) -> None:
    _require(work["committed_attempt_id"] is None, "Work is already committed")
    _require(work["lease_owner_attempt_id"] == command.attempt_id, "Attempt does not own the work lease")
    _require(work["lease_dispatcher_id"] == attempt["dispatcher_id"], "Lease dispatcher does not match")
    _require(work["status"] in _ACTIVE, "Work is not active")


def _live(command: _Command, work: dict[str, Any], now: datetime) -> None:
    expires = work["lease_expires_at"]
    _require(
        expires is not None and _timestamp(expires) > max(now, command.created_at),
        "Work lease has expired",
    )
    acquired = work["lease_acquired_at"]
    _require(acquired is not None and command.created_at >= _timestamp(acquired), "Event predates this lease")
    _require(command.created_at <= now, "Event timestamp is in the future")


def _transition(current: str, target: str) -> None:
    _require(current in _ACTIVE and target in _ACTIVE, "Attempt is not active")
    distance = _ACTIVE.index(target) - _ACTIVE.index(current)
    _require(distance in (0, 1), f"Invalid transition {current} -> {target}")


def _attempt_updates(command: _Command) -> dict[str, Any]:
    updates = dict(command.payload["updates"])
    for name in _TIME_FIELDS & updates.keys():
        if updates[name] is not None:
            updates[name] = _timestamp(updates[name])
    if "last_heartbeat_at" in updates:
        updates["last_heartbeat_at"] = command.created_at
    if updates.get("status") == "SUCCEEDED":
        updates["completed_at"] = command.created_at
    return updates


def _heartbeat(command: _Command, work: dict[str, Any], attempt: dict[str, Any], now: datetime) -> _Plan:
    status = command.payload["status"]
    updates = _attempt_updates(command)
    updates.update(status=status, last_heartbeat_at=command.created_at)
    work_updates = {
        "status": status, "last_heartbeat_at": command.created_at,
        "lease_expires_at": command.created_at + timedelta(minutes=command.lease_minutes),
    }
    if work["lease_expires_at"] is not None:
        work_updates["lease_expires_at"] = max(work_updates["lease_expires_at"], _timestamp(work["lease_expires_at"]))
    # A fully applied command remains acknowledged even if recovery is delayed.
    if _matches(work, work_updates) and _matches(attempt, updates):
        return _Plan(message="Heartbeat already applied")
    _live(command, work, now)
    _transition(work["status"], status)
    _transition(attempt["status"], status)
    previous = work["last_heartbeat_at"]
    _require(previous is None or command.created_at >= _timestamp(previous), "Heartbeat is older than current state")
    _require(work_updates["lease_expires_at"] > now, "Heartbeat renewal has already expired")
    return _Plan(work=work_updates, attempt=updates)


def _update(command: _Command, work: dict[str, Any], attempt: dict[str, Any], now: datetime) -> _Plan:
    updates = _attempt_updates(command)
    if attempt["status"] == "SUCCEEDED":
        _require(
            work["status"] == "WRITING" and updates.get("status") == "SUCCEEDED"
            and _matches(attempt, updates) and _timestamp(attempt["completed_at"]) == command.created_at,
            "Completed attempt cannot be changed",
        )
        return _Plan(message="Attempt completion already applied")
    _live(command, work, now)
    _require(attempt["status"] in _ACTIVE and attempt["status"] == work["status"], "Attempt/work state differs")
    target = updates.get("status", attempt["status"])
    _require(
        target == attempt["status"] or (target == "SUCCEEDED" and work["status"] == "WRITING"),
        "Attempt state transitions require heartbeat; success requires WRITING",
    )
    _require(
        "completed_at" not in updates or target == "SUCCEEDED",
        "Only a succeeded attempt may have completed_at",
    )
    return _Plan(attempt=updates)


def _commit(command: _Command, work: dict[str, Any], attempt: dict[str, Any], now: datetime) -> _Plan:
    _require(attempt["status"] == "SUCCEEDED", "Commit requires a succeeded attempt")
    if work["status"] == "SUCCEEDED" and work["committed_attempt_id"] == command.attempt_id:
        return _Plan(message="Attempt already committed")
    _owner(command, work, attempt)
    _live(command, work, now)
    _require(work["status"] == "WRITING", "Commit requires WRITING work")
    return _Plan(work={
        **_LEASE_CLEAR, "status": "SUCCEEDED", "committed_attempt_id": command.attempt_id,
        "completed_at": command.created_at, "not_before_at": None,
        "last_error_category": None, "last_error_type": None, "last_error_message": None,
    })


def _failure(command: _Command, work: dict[str, Any], attempt: dict[str, Any], now: datetime) -> _Plan:
    payload = command.payload
    exhausted = work["attempt_count"] >= work["max_attempts"]
    status = "DEAD_LETTERED" if exhausted else ("RETRY_WAIT" if payload["retryable"] else "TERMINAL_FAILED")
    attempt_status = "LEASE_LOST" if status == "RETRY_WAIT" and payload["lease_lost"] else status
    updates = {key: value for key, value in payload.items() if key != "lease_lost"}
    updates.update(status=attempt_status, completed_at=command.created_at)
    delay = min(60, 2 ** min(6, max(0, work["attempt_count"] - 1)))
    work_updates = {
        **_LEASE_CLEAR, "status": status,
        "not_before_at": command.created_at + timedelta(minutes=delay) if status == "RETRY_WAIT" else None,
        "last_error_category": payload["error_category"],
        "last_error_type": payload["error_type"],
        "last_error_message": payload["error_message"],
    }
    if status == "RETRY_WAIT":
        work_updates["queue_entered_at"] = command.created_at
    _require(work["committed_attempt_id"] is None, "Committed work cannot fail")
    if _matches(work, work_updates) and _matches(attempt, updates):
        return _Plan(message="Failure already applied")
    _owner(command, work, attempt)
    _require(command.created_at <= now, "Event timestamp is in the future")
    acquired = work["lease_acquired_at"]
    _require(acquired is not None and command.created_at >= _timestamp(acquired), "Event predates this lease")
    _require(attempt["status"] in _ACTIVE or _matches(attempt, updates), "Completed attempt cannot fail")
    return _Plan(work=work_updates, attempt=updates)


def _release(command: _Command, work: dict[str, Any], attempt: dict[str, Any], now: datetime) -> _Plan:
    reason = command.payload["reason"]
    attempt_updates = {
        "status": "RELEASED",
        "completed_at": command.created_at,
        "retryable": True,
        "error_category": "WORKER_BUDGET",
        "error_type": "WorkerLifetimeReached",
        "error_message": reason,
    }
    work_updates = {
        **_LEASE_CLEAR,
        "status": "RETRY_WAIT",
        "attempt_count": max(0, work["attempt_count"] - 1),
        "not_before_at": command.created_at,
        "last_error_category": None,
        "last_error_type": None,
        "last_error_message": None,
    }
    attempt_applied = _matches(attempt, attempt_updates)
    if attempt_applied and (
        work["status"] == "RETRY_WAIT"
        and work["lease_owner_attempt_id"] is None
        and work["lease_dispatcher_id"] is None
        and work["lease_acquired_at"] is None
        and work["lease_expires_at"] is None
        and work["not_before_at"] is not None
        and _timestamp(work["not_before_at"]) == command.created_at
    ):
        return _Plan(message="Release already applied")
    _owner(command, work, attempt)
    if not attempt_applied:
        _live(command, work, now)
    else:
        _require(command.created_at <= now, "Event timestamp is in the future")
        acquired = work["lease_acquired_at"]
        _require(acquired is not None and command.created_at >= _timestamp(acquired), "Event predates this lease")
    _require(work["status"] == "LEASED", "Release requires LEASED work")
    _require(
        attempt["status"] == "LEASED" or attempt_applied,
        "Release requires a LEASED or already released attempt",
    )
    _require(attempt["inference_started_at"] is None, "Started attempts cannot be released")
    return _Plan(work=work_updates, attempt=attempt_updates)


def _plan(command: _Command, work: dict[str, Any], attempt: dict[str, Any], now: datetime) -> _Plan:
    _identity(command, work, attempt)
    if command.kind == "claim_execution":
        _owner(command, work, attempt)
        _live(command, work, now)
        _require(work["status"] == attempt["status"] == "LEASED", "Claim requires LEASED work and attempt")
        _require(attempt["worker_execution_id"] in (None, command.worker_execution_id), "Another execution owns the attempt")
        return _Plan(attempt={"worker_execution_id": command.worker_execution_id})
    _require(attempt["worker_execution_id"] == command.worker_execution_id, "Another execution owns the attempt")
    if command.kind == "commit":
        return _commit(command, work, attempt, now)
    if command.kind == "failure":
        return _failure(command, work, attempt, now)
    if command.kind == "release":
        return _release(command, work, attempt, now)
    _owner(command, work, attempt)
    if command.kind == "heartbeat":
        return _heartbeat(command, work, attempt, now)
    return _update(command, work, attempt, now)


class _DeltaStore:
    def __init__(self, spark_session: Any, tables: dict[str, str]) -> None:
        _validate_tables(spark_session, tables)

        from delta.tables import DeltaTable
        from pyspark.sql import functions as F

        self.spark = spark_session
        self.tables = tables
        self.delta = DeltaTable
        self.f = F

    def pending(self, limit: int) -> list[dict[str, Any]]:
        return [
            row.asDict(recursive=True)
            for row in (
                self.spark.table(self.tables["worker_events"])
                .join(self.spark.table(self.tables["worker_event_receipts"]).select("event_id"), "event_id", "leftanti")
                .orderBy("created_at", "worker_execution_id", "sequence", "event_id")
                .limit(limit).collect()
            )
        ]

    def unique(self, table: str, key: str, value: str) -> dict[str, Any]:
        rows = self.spark.table(self.tables[table]).where(self.f.col(key) == value).limit(2).collect()
        _require(len(rows) == 1, f"Expected exactly one {table} row for {value}; found {len(rows)}")
        return rows[0].asDict(recursive=True)

    def receipt(self, event_id: str) -> dict[str, Any] | None:
        return _read_receipt(self.spark, self.tables["worker_event_receipts"], event_id)

    def highest_sequence(self, row: dict[str, Any]) -> int:
        frame = self.spark.table(self.tables["worker_event_receipts"])
        for key in ("work_id", "attempt_id", "worker_execution_id"):
            frame = frame.where(self.f.col(key) == row[key])
        result = frame.agg(self.f.max("sequence").alias("sequence")).collect()[0]["sequence"]
        return 0 if result is None else result

    def update(self, table: str, before: dict[str, Any], updates: dict[str, Any]) -> None:
        if not updates:
            return
        name = self.tables[table]
        schema = _table_schema(self.spark, name, table)
        keys = ["work_id", "capture_date", "config_sha256", "status"]
        if table == "video_work":
            keys += ["lease_owner_attempt_id", "lease_dispatcher_id", "lease_acquired_at",
                     "lease_expires_at", "committed_attempt_id", "attempt_count", "max_attempts"]
            identity = "work_id"
        else:
            keys += ["attempt_id", "worker_execution_id", "dispatcher_id"]
            identity = "attempt_id"
        condition = self.f.lit(True)
        for key in keys:
            condition = condition & self.f.col(key).eqNullSafe(self.f.lit(before[key]))
        live_write = table == "video_work" and updates.get("status") in (*_ACTIVE, "SUCCEEDED")
        if live_write:
            condition = condition & (self.f.col("lease_expires_at") > self.f.current_timestamp())
            if updates.get("lease_expires_at") is not None:
                condition = condition & (self.f.lit(updates["lease_expires_at"]) > self.f.current_timestamp())
        self.delta.forName(self.spark, name).update(
            condition=condition,
            set={key: self.f.lit(value).cast(schema[key].dataType) for key, value in updates.items()},
        )
        # A condition mismatch or a lossy cast is not a successful acknowledgement.
        after = self.unique(table, identity, before[identity])
        if not _matches(after, updates):
            if live_write and (
                after["lease_expires_at"] is None or _timestamp(after["lease_expires_at"]) <= _utc_now()
            ):
                raise RuntimeError("Lease expired while applying the event; reconcile partial writes before recovery")
            raise RuntimeError(f"{name} update did not persist the requested values")

    def append_receipts(self, receipts: list[dict[str, Any]]) -> None:
        name = self.tables["worker_event_receipts"]
        schema = _table_schema(self.spark, name, "worker_event_receipts")
        self.spark.createDataFrame(receipts, schema).write.format("delta").mode("append").saveAsTable(name)


def _read_receipt(spark_session: Any, table: str, event_id: str) -> dict[str, Any] | None:
    from pyspark.sql import functions as F

    rows = spark_session.table(table).where(F.col("event_id") == event_id).limit(2).collect()
    if len(rows) > 1:
        raise RuntimeError(f"Duplicate receipts for event {event_id}")
    return rows[0].asDict(recursive=True) if rows else None


def _apply_event(store: _DeltaStore, row: dict[str, Any]) -> dict[str, Any]:
    try:
        command = _Command.from_row(row)
    except (ValueError, TypeError) as error:
        plan = _Plan(outcome="REJECTED", message=f"Invalid event: {error}")
    else:
        try:
            if command.sequence <= store.highest_sequence(row):
                raise _Rejected("Stale event sequence; a later command was already acknowledged")
            work = store.unique("video_work", "work_id", command.work_id)
            attempt = store.unique("video_attempts", "attempt_id", command.attempt_id)
            plan = _plan(command, work, attempt, _utc_now())
        except _Rejected as error:
            plan = _Plan(outcome="REJECTED", message=str(error))
        else:
            # Attempt first: failure replay can finish releasing work after a crash.
            # Each command's receipt is persisted before another for the same work.
            store.update("video_attempts", attempt, plan.attempt)
            store.update("video_work", work, plan.work)
    return {
        **{key: row[key] for key in ("event_id", "work_id", "attempt_id", "worker_execution_id", "sequence")},
        "outcome": plan.outcome, "message": plan.message, "applied_at": _utc_now(),
    }


def _drain(store: _DeltaStore, limit: int) -> dict[str, int]:
    result = {"processed": 0, "applied": 0, "rejected": 0}
    batch: list[dict[str, Any]] = []
    works: set[str] = set()
    attempts: set[str] = set()
    seen: set[str] = set()
    for row in store.pending(limit):
        if row["event_id"] in seen or store.receipt(row["event_id"]) is not None:
            continue
        seen.add(row["event_id"])
        if row["work_id"] in works or row["attempt_id"] in attempts:
            store.append_receipts(batch)
            batch, works, attempts = [], set(), set()
        receipt = _apply_event(store, row)
        batch.append(receipt)
        works.add(row["work_id"])
        attempts.add(row["attempt_id"])
        result["processed"] += 1
        result[receipt["outcome"].lower()] += 1
    if batch:
        store.append_receipts(batch)
    return result


def process_worker_events(
    spark_session: Any, writer: Any, *, table_prefix: str, database: str = "", limit: int = 1000,
) -> dict[str, int]:
    """Drain one ordered inbox batch inside exactly one exclusive writer run.

    Receipts for independent work are batched. Before processing another command
    for the same work or attempt, its predecessors' receipts are made durable.
    Call this before entering a watchdog's own writer callback, never inside it.
    """
    tables = _tables(table_prefix, database)
    _positive_int(limit, "limit")
    return writer.run(lambda: _drain(_DeltaStore(spark_session, tables), limit))


class WorkerEventClient:
    """Synchronous, per-execution command producer with durable acknowledgements."""

    def __init__(
        self, spark_session: Any, writer: Any, *, table_prefix: str, database: str = "",
        work_id: str, attempt_id: str, worker_execution_id: str,
        capture_date: date, lease_minutes: int,
    ) -> None:
        self._tables = _tables(table_prefix, database)
        self._identity = {
            "work_id": _text(work_id, "work_id"), "attempt_id": _text(attempt_id, "attempt_id"),
            "worker_execution_id": _text(worker_execution_id, "worker_execution_id"),
        }
        if type(capture_date) is not date:
            raise ValueError("capture_date must be a date")
        self._lease_minutes = _positive_int(lease_minutes, "lease_minutes")
        self._capture_date = capture_date
        self._spark, self._writer = spark_session, writer
        self._prefix, self._database = table_prefix, database
        self._sequence = 0
        self._last_created_at = datetime.min.replace(tzinfo=timezone.utc)
        self._lock = Lock()

    def submit(self, kind: str, payload: dict) -> dict:
        """Append once, then await a receipt with a 600-second wait budget.

        The deadline is checked between synchronous drain operations, not by
        interrupting a writer holding the durable lock. Timeout does not cancel
        or resubmit the durable event.
        """
        _validate_payload(kind, payload)
        payload_json = json.dumps(
            {"payload": payload, "lease_minutes": self._lease_minutes},
            default=_json_default, allow_nan=False, sort_keys=True,
        )
        with self._lock:
            schemas = _validate_tables(self._spark, self._tables)
            self._sequence += 1
            event_id = str(uuid4())
            created_at = max(_utc_now(), self._last_created_at)
            self._last_created_at = created_at
            event = {
                **self._identity, "event_id": event_id, "capture_date": self._capture_date,
                "sequence": self._sequence, "event_kind": kind,
                "payload_json": payload_json, "created_at": created_at,
            }
            table = self._tables["worker_events"]
            (
                self._spark.createDataFrame([event], schemas["worker_events"])
                .write.format("delta").mode("append")
                .option("txnAppId", event_id).option("txnVersion", 0).saveAsTable(table)
            )
            deadline = time.monotonic() + _RECEIPT_TIMEOUT_SECONDS
            while True:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"Timed out waiting for receipt for durable event {event_id}; "
                        "the event remains queued for processing"
                    )
                result = process_worker_events(
                    self._spark, self._writer, table_prefix=self._prefix, database=self._database,
                )
                receipt = _read_receipt(self._spark, self._tables["worker_event_receipts"], event_id)
                if receipt is not None:
                    if receipt["outcome"] == "REJECTED":
                        raise LeaseLostError(receipt["message"])
                    if receipt["outcome"] != "APPLIED":
                        raise RuntimeError(f"Unknown worker receipt outcome: {receipt['outcome']}")
                    return receipt
                if result["processed"] == 0:
                    time.sleep(0.1)
