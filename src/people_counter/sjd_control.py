"""Serialized local control plane for Candidate A Spark Job Definitions.

The module deliberately has no Spark dependency.  A control process creates a
durable claim envelope and passes only its batch id, path, and digest to an SJD.
SQLite is authoritative for mutable state; the envelope itself is immutable,
content-addressed JSON.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import time
from collections.abc import Callable, Collection, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from people_counter.local_storage import ContentAddressedStore


SCHEMA_VERSION = 1
CLAIM_ENVELOPE_VERSION = 1
_ACTIVE_ATTEMPTS = frozenset({"LEASED", "SEALED"})
_SHA256_LENGTH = 64


class ControlError(RuntimeError):
    """Base class for local SJD control failures."""


class ImmutableConflictError(ControlError):
    """An immutable identity was reused with different content."""


class LeaseBudgetError(ControlError):
    """A claim cannot safely finish within its configured lease."""


class LeaseLostError(ControlError):
    """An operation no longer owns the authoritative work fence."""


class BatchValidationError(ControlError):
    """A batch artifact does not exactly match its authoritative claim."""


class UnsupportedControlStoreError(ControlError):
    """The requested control-store implementation is intentionally unavailable."""


@dataclass(frozen=True)
class RegisteredWork:
    work_id: str
    status: str
    payload_sha256: str
    runtime_key: str
    duration_seconds: float
    attempt_count: int
    max_attempts: int
    original_max_attempts: int
    committed_attempt_id: str | None
    publication_sequence: int | None
    last_replay_id: str | None


@dataclass(frozen=True)
class ClaimedWork:
    work_id: str
    attempt_id: str
    fence: int
    payload_sha256: str


@dataclass(frozen=True)
class ClaimedBatch:
    batch_id: str
    envelope_path: str | Path
    envelope_sha256: str
    runtime_key: str
    lease_expires_at: float
    items: tuple[ClaimedWork, ...]


@dataclass(frozen=True)
class RecoveryReport:
    recovered: int
    retried: int
    dead: int


@dataclass(frozen=True)
class ReconciliationFinding:
    finding_id: str
    finding_type: str
    severity: str
    entity_key: str
    details: dict[str, Any]
    first_seen_at: float
    last_seen_at: float


@dataclass(frozen=True)
class ReplayRequest:
    replay_id: str
    work_id: str
    operator: str
    reason: str
    generation: int
    requested_at: float


@dataclass(frozen=True)
class ControlBatchState:
    status: str
    lease_expires_at: float


class ControlStore(Protocol):
    """Storage boundary used by a Candidate A control entry point."""

    def register(
        self,
        work_id: str,
        payload: Mapping[str, Any],
        *,
        runtime_key: str,
        duration_seconds: float,
        config_sha256: str,
        release_digest: str,
        max_attempts: int = 3,
        available_at: float | None = None,
    ) -> RegisteredWork: ...

    def claim(
        self,
        owner: str,
        *,
        max_items: int,
        lease_seconds: float,
        minimum_speed_x: float | None = None,
        safety_factor: float | None = None,
        margin_seconds: float | None = None,
        minimum_items: int = 1,
        allowed_work_ids: Collection[str] | None = None,
        process_profile: object | None = None,
        peak_rss_bytes: int | None = None,
    ) -> ClaimedBatch | None: ...

    def recover(self, *, now: float | None = None) -> RecoveryReport: ...

    def reconcile(self, *, now: float | None = None) -> list[ReconciliationFinding]: ...

    def replay(
        self,
        work_id: str,
        *,
        operator: str,
        reason: str,
        additional_attempts: int = 1,
    ) -> ReplayRequest: ...


class ProcessControlStore(Protocol):
    """Control operations required by the backend-neutral process orchestrator."""

    def load_claim_envelope_with_digest(
        self, batch_id: str
    ) -> tuple[dict[str, Any], str]: ...

    def process_batch_state(self, envelope: Any) -> ControlBatchState: ...

    def assert_process_fence(self, envelope: Any) -> None: ...

    def heartbeat_process(self, envelope: Any, extension_seconds: float) -> None: ...

    def seal_batch(
        self,
        batch_id: str,
        outputs: Sequence[Mapping[str, Any]],
        *,
        envelope_sha256: str,
        membership_sha256: str,
    ) -> None: ...

    def commit_batch(self, batch_id: str) -> tuple[int, ...]: ...

    def now(self) -> float: ...


class FabricControlStore:
    """Lazy facade for the Delta-backed Fabric implementation."""

    def __init__(self, spark_session: Any | None = None, **kwargs: Any) -> None:
        if spark_session is None:
            raise UnsupportedControlStoreError(
                "FabricControlStore requires an explicit Fabric Spark session"
            )
        from people_counter.fabric_candidate_a_control import (
            FabricControlStoreImpl,
        )

        self._implementation = FabricControlStoreImpl(
            spark_session, **kwargs
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._implementation, name)


class OneLakeEnvelopeWriter:
    """Lazy public facade for immutable Candidate A claim envelopes."""

    def __init__(self, root: str, files: Any | None = None) -> None:
        from people_counter.fabric_candidate_a_control import (
            OneLakeEnvelopeWriter as Implementation,
        )

        self._implementation = Implementation(root, files)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._implementation, name)


class SQLiteControlStore:
    """Authoritative single-database Candidate A control store."""

    def __init__(
        self,
        database: Path,
        content_root: Path | None = None,
        *,
        clock: Callable[[], float] = time.time,
        id_factory: Callable[[], str] | None = None,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        if not 1 <= busy_timeout_ms <= 60_000:
            raise ValueError("busy_timeout_ms must be between 1 and 60000")
        self.database = Path(database).expanduser().resolve()
        self.database.parent.mkdir(parents=True, exist_ok=True)
        root = content_root or self.database.with_suffix(".content")
        self.content = ContentAddressedStore(Path(root))
        self._clock = clock
        self._id_factory = id_factory or (lambda: str(uuid4()))
        self._busy_timeout_ms = busy_timeout_ms
        self.bootstrap()

    def bootstrap(self) -> None:
        """Create or validate the versioned local control schema."""
        with self._connect() as connection:
            current = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if current not in (0, SCHEMA_VERSION):
                raise ControlError(
                    f"unsupported control schema version {current}; "
                    f"expected {SCHEMA_VERSION}"
                )
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS control_metadata (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    schema_version INTEGER NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS work (
                    work_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    runtime_key TEXT NOT NULL,
                    duration_seconds REAL NOT NULL CHECK (duration_seconds > 0),
                    config_sha256 TEXT NOT NULL,
                    release_digest TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN ('READY', 'LEASED', 'SUCCEEDED', 'DEAD')
                    ),
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
                    original_max_attempts INTEGER NOT NULL
                        CHECK (original_max_attempts > 0),
                    fence INTEGER NOT NULL DEFAULT 0,
                    available_at REAL NOT NULL,
                    lease_owner TEXT,
                    lease_attempt_id TEXT,
                    lease_expires_at REAL,
                    committed_attempt_id TEXT,
                    publication_sequence INTEGER,
                    replay_generation INTEGER NOT NULL DEFAULT 0,
                    last_replay_id TEXT,
                    last_error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    CHECK (
                        (status = 'LEASED' AND lease_owner IS NOT NULL
                            AND lease_attempt_id IS NOT NULL
                            AND lease_expires_at IS NOT NULL)
                        OR
                        (status != 'LEASED' AND lease_owner IS NULL
                            AND lease_attempt_id IS NULL
                            AND lease_expires_at IS NULL)
                    )
                );
                CREATE INDEX IF NOT EXISTS work_claim_idx
                ON work(status, available_at, runtime_key, created_at, work_id);
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    runtime_key TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN ('LEASED', 'SEALED', 'COMMITTED', 'EXPIRED')
                    ),
                    lease_expires_at REAL NOT NULL,
                    item_count INTEGER NOT NULL CHECK (item_count > 0),
                    membership_sha256 TEXT NOT NULL,
                    envelope_version INTEGER NOT NULL,
                    envelope_path TEXT NOT NULL,
                    envelope_sha256 TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    sealed_at REAL,
                    committed_at REAL
                );
                CREATE TABLE IF NOT EXISTS attempts (
                    attempt_id TEXT PRIMARY KEY,
                    work_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    fence INTEGER NOT NULL CHECK (fence > 0),
                    status TEXT NOT NULL CHECK (
                        status IN (
                            'LEASED', 'SEALED', 'SUCCEEDED', 'FAILED', 'EXPIRED'
                        )
                    ),
                    lease_expires_at REAL NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    output_path TEXT,
                    output_sha256 TEXT,
                    terminal_succeeded INTEGER CHECK (
                        terminal_succeeded IS NULL
                        OR terminal_succeeded IN (0, 1)
                    ),
                    sealed_at REAL,
                    recovery_outcome TEXT,
                    created_at REAL NOT NULL,
                    UNIQUE(work_id, fence),
                    FOREIGN KEY(work_id) REFERENCES work(work_id),
                    FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
                );
                CREATE INDEX IF NOT EXISTS attempts_batch_idx
                ON attempts(batch_id, work_id);
                CREATE TABLE IF NOT EXISTS batch_members (
                    batch_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    work_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL UNIQUE,
                    fence INTEGER NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    PRIMARY KEY(batch_id, ordinal),
                    UNIQUE(batch_id, work_id),
                    FOREIGN KEY(batch_id) REFERENCES batches(batch_id),
                    FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id)
                );
                CREATE TABLE IF NOT EXISTS output_records (
                    output_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    attempt_id TEXT NOT NULL,
                    work_id TEXT NOT NULL,
                    output_path TEXT NOT NULL,
                    output_sha256 TEXT NOT NULL,
                    executor_identity TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    task_attempt_id INTEGER NOT NULL,
                    record_sequence INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    UNIQUE(work_id, attempt_id, record_sequence)
                );
                CREATE INDEX IF NOT EXISTS output_attempt_idx
                ON output_records(attempt_id, work_id);
                CREATE TABLE IF NOT EXISTS publications (
                    publication_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    work_id TEXT NOT NULL UNIQUE,
                    attempt_id TEXT NOT NULL UNIQUE,
                    batch_id TEXT NOT NULL,
                    output_path TEXT NOT NULL,
                    output_sha256 TEXT NOT NULL,
                    published_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS reconciliation_findings (
                    finding_id TEXT PRIMARY KEY,
                    finding_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    entity_key TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    first_seen_at REAL NOT NULL,
                    last_seen_at REAL NOT NULL,
                    resolved_at REAL
                );
                CREATE INDEX IF NOT EXISTS findings_open_idx
                ON reconciliation_findings(resolved_at, finding_type);
                CREATE TABLE IF NOT EXISTS replay_requests (
                    replay_id TEXT PRIMARY KEY,
                    work_id TEXT NOT NULL,
                    operator TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    requested_at REAL NOT NULL,
                    from_status TEXT NOT NULL,
                    additional_attempts INTEGER NOT NULL,
                    UNIQUE(work_id, generation),
                    FOREIGN KEY(work_id) REFERENCES work(work_id)
                );
                """
            )
            work_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(work)").fetchall()
            }
            if "original_max_attempts" not in work_columns:
                connection.execute(
                    "ALTER TABLE work ADD COLUMN original_max_attempts INTEGER"
                )
                connection.execute(
                    """
                    UPDATE work SET original_max_attempts = max_attempts
                    WHERE original_max_attempts IS NULL
                    """
                )
            duplicate_output = connection.execute(
                """
                SELECT 1 FROM output_records
                GROUP BY work_id, attempt_id, record_sequence
                HAVING COUNT(*) > 1 LIMIT 1
                """
            ).fetchone()
            if duplicate_output is not None:
                raise ControlError(
                    "output_records contains duplicate immutable identities"
                )
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS output_identity_idx
                ON output_records(work_id, attempt_id, record_sequence)
                """
            )
            now = self._now()
            connection.execute(
                """
                INSERT OR IGNORE INTO control_metadata
                    (singleton, schema_version, created_at)
                VALUES (1, ?, ?)
                """,
                (SCHEMA_VERSION, now),
            )
            metadata = connection.execute(
                "SELECT schema_version FROM control_metadata WHERE singleton = 1"
            ).fetchone()
            if metadata is None or int(metadata[0]) != SCHEMA_VERSION:
                raise ControlError("control metadata conflicts with this schema")
            connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    def register(
        self,
        work_id: str,
        payload: Mapping[str, Any],
        *,
        runtime_key: str,
        duration_seconds: float,
        config_sha256: str,
        release_digest: str,
        max_attempts: int = 3,
        available_at: float | None = None,
    ) -> RegisteredWork:
        identity = _required_text(work_id, "work_id")
        runtime = _required_text(runtime_key, "runtime_key")
        config = _required_text(config_sha256, "config_sha256")
        release = _required_text(release_digest, "release_digest")
        duration = _positive_finite(duration_seconds, "duration_seconds")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 100:
            raise ValueError("max_attempts must be between 1 and 100")
        if not isinstance(payload, Mapping):
            raise ValueError("payload must be a mapping")
        payload_json = _canonical_json(dict(payload))
        payload_sha256 = _sha256_text(payload_json)
        now = self._now()
        ready_at = now if available_at is None else _finite(available_at, "available_at")
        immutable = (
            payload_sha256,
            runtime,
            duration,
            config,
            release,
            max_attempts,
        )
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM work WHERE work_id = ?", (identity,)
            ).fetchone()
            if row is not None:
                existing = (
                    row["payload_sha256"],
                    row["runtime_key"],
                    float(row["duration_seconds"]),
                    row["config_sha256"],
                    row["release_digest"],
                    int(row["original_max_attempts"]),
                )
                if existing != immutable:
                    raise ImmutableConflictError(
                        f"work_id {identity!r} has different immutable content"
                    )
                return self._registered(row)
            connection.execute(
                """
                INSERT INTO work (
                    work_id, payload_json, payload_sha256, runtime_key,
                    duration_seconds, config_sha256, release_digest, status,
                    max_attempts, original_max_attempts, available_at,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'READY', ?, ?, ?, ?, ?)
                """,
                (
                    identity,
                    payload_json,
                    payload_sha256,
                    runtime,
                    duration,
                    config,
                    release,
                    max_attempts,
                    max_attempts,
                    ready_at,
                    now,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM work WHERE work_id = ?", (identity,)
            ).fetchone()
            assert row is not None
            return self._registered(row)

    def register_manifest(self, path: Path) -> tuple[RegisteredWork, ...]:
        """Register all immutable requests in a JSON or JSONL manifest."""
        return tuple(
            self.register(
                request["work_id"],
                request["payload"],
                runtime_key=request["runtime_key"],
                duration_seconds=request["duration_seconds"],
                config_sha256=request["config_sha256"],
                release_digest=request["release_digest"],
                max_attempts=request["max_attempts"],
                available_at=request["available_at"],
            )
            for request in read_registration_manifest(path)
        )

    def get_work(self, work_id: str) -> RegisteredWork:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM work WHERE work_id = ?",
                (_required_text(work_id, "work_id"),),
            ).fetchone()
        if row is None:
            raise KeyError(work_id)
        return self._registered(row)

    def claim(
        self,
        owner: str,
        *,
        max_items: int,
        lease_seconds: float,
        minimum_speed_x: float | None = None,
        safety_factor: float | None = None,
        margin_seconds: float | None = None,
        minimum_items: int = 1,
        allowed_work_ids: Collection[str] | None = None,
        process_profile: object | None = None,
        peak_rss_bytes: int | None = None,
    ) -> ClaimedBatch | None:
        owner_id = _required_text(owner, "owner")
        if type(max_items) is not int or not 1 <= max_items <= 100:
            raise ValueError("max_items must be between 1 and 100")
        if (
            type(minimum_items) is not int
            or minimum_items < 1
            or minimum_items > max_items
        ):
            raise ValueError("minimum_items must be between 1 and max_items")
        lease = _positive_finite(lease_seconds, "lease_seconds")
        speed, factor, margin, profile_workers, profile_name = (
            _claim_admission_settings(
                process_profile,
                minimum_speed_x=minimum_speed_x,
                safety_factor=safety_factor,
                margin_seconds=margin_seconds,
                peak_rss_bytes=peak_rss_bytes,
            )
        )
        if factor < 1:
            raise ValueError("safety_factor must be at least 1")
        scope = _allowed_work_scope(allowed_work_ids)
        now = self._now()
        with self._transaction() as connection:
            scope_join = ""
            if scope is not None:
                if not scope:
                    return None
                connection.execute(
                    "CREATE TEMP TABLE claim_scope (work_id TEXT PRIMARY KEY)"
                )
                connection.executemany(
                    "INSERT INTO claim_scope (work_id) VALUES (?)",
                    ((work_id,) for work_id in scope),
                )
                scope_join = "JOIN claim_scope s ON s.work_id = w.work_id"
            runtime_row = connection.execute(
                f"""
                SELECT w.runtime_key, MIN(w.created_at) AS first_created
                FROM work w
                {scope_join}
                WHERE w.status = 'READY' AND w.available_at <= ?
                GROUP BY w.runtime_key
                HAVING COUNT(*) >= ?
                ORDER BY first_created, runtime_key
                LIMIT 1
                """,
                (now, minimum_items),
            ).fetchone()
            if runtime_row is None:
                return None
            rows = connection.execute(
                f"""
                SELECT w.* FROM work w
                {scope_join}
                WHERE w.status = 'READY' AND w.available_at <= ?
                    AND w.runtime_key = ?
                ORDER BY w.created_at, w.work_id
                LIMIT ?
                """,
                (now, runtime_row["runtime_key"], max_items),
            ).fetchall()
            if not rows or len(rows) < minimum_items:
                return None
            rows, worker_count, makespan = _largest_admissible_claim_prefix(
                rows,
                minimum_items=minimum_items,
                profile_workers=profile_workers,
                minimum_speed_x=speed,
                safety_factor=factor,
                margin_seconds=margin,
                lease_seconds=lease,
            )
            batch_id = self._new_id()
            expires = now + lease
            claimed: list[ClaimedWork] = []
            for row in rows:
                attempt_id = self._new_id()
                fence = int(row["fence"]) + 1
                claimed.append(
                    ClaimedWork(
                        str(row["work_id"]),
                        attempt_id,
                        fence,
                        str(row["payload_sha256"]),
                    )
                )
            membership = _membership_sha256(claimed)
            envelope = {
                "schema_version": CLAIM_ENVELOPE_VERSION,
                "batch_id": batch_id,
                "owner": owner_id,
                "runtime_key": str(runtime_row["runtime_key"]),
                "claimed_at": now,
                "lease_expires_at": expires,
                "membership_sha256": membership,
                "admission": {
                    "process_profile": profile_name,
                    "worker_count": worker_count,
                    "projected_makespan_seconds": makespan,
                    "minimum_speed_x": speed,
                    "safety_factor": factor,
                    "margin_seconds": margin,
                },
                "items": [
                    {
                        "ordinal": ordinal,
                        "work_id": item.work_id,
                        "attempt_id": item.attempt_id,
                        "fence": item.fence,
                        "payload_sha256": item.payload_sha256,
                        "payload": json.loads(rows[ordinal]["payload_json"]),
                        "config_sha256": rows[ordinal]["config_sha256"],
                        "release_digest": rows[ordinal]["release_digest"],
                        "duration_seconds": rows[ordinal]["duration_seconds"],
                    }
                    for ordinal, item in enumerate(claimed)
                ],
            }
            stored = self.content.put_json(envelope)
            connection.execute(
                """
                INSERT INTO batches (
                    batch_id, owner, runtime_key, status, lease_expires_at,
                    item_count, membership_sha256, envelope_version,
                    envelope_path, envelope_sha256, created_at
                ) VALUES (?, ?, ?, 'LEASED', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    owner_id,
                    runtime_row["runtime_key"],
                    expires,
                    len(claimed),
                    membership,
                    CLAIM_ENVELOPE_VERSION,
                    str(stored.path),
                    stored.sha256,
                    now,
                ),
            )
            for ordinal, (row, item) in enumerate(zip(rows, claimed, strict=True)):
                updated = connection.execute(
                    """
                    UPDATE work
                    SET status = 'LEASED', attempt_count = attempt_count + 1,
                        fence = ?, lease_owner = ?, lease_attempt_id = ?,
                        lease_expires_at = ?, updated_at = ?
                    WHERE work_id = ? AND status = 'READY' AND fence = ?
                    """,
                    (
                        item.fence,
                        owner_id,
                        item.attempt_id,
                        expires,
                        now,
                        item.work_id,
                        item.fence - 1,
                    ),
                )
                if updated.rowcount != 1:
                    raise LeaseLostError(
                        f"claim cardinality changed for work {item.work_id}"
                    )
                connection.execute(
                    """
                    INSERT INTO attempts (
                        attempt_id, work_id, batch_id, fence, status,
                        lease_expires_at, payload_sha256, created_at
                    ) VALUES (?, ?, ?, ?, 'LEASED', ?, ?, ?)
                    """,
                    (
                        item.attempt_id,
                        item.work_id,
                        batch_id,
                        item.fence,
                        expires,
                        item.payload_sha256,
                        now,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO batch_members (
                        batch_id, ordinal, work_id, attempt_id, fence,
                        payload_sha256
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        ordinal,
                        item.work_id,
                        item.attempt_id,
                        item.fence,
                        item.payload_sha256,
                    ),
                )
            return ClaimedBatch(
                batch_id,
                stored.path,
                stored.sha256,
                str(runtime_row["runtime_key"]),
                expires,
                tuple(claimed),
            )

    def load_claim_envelope(
        self,
        batch_id: str,
        *,
        envelope_path: Path | None = None,
        envelope_sha256: str | None = None,
    ) -> dict[str, Any]:
        """Read and verify all three pieces of the SJD claim reference."""
        identity = _required_text(batch_id, "batch_id")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (identity,)
            ).fetchone()
        if row is None:
            raise KeyError(batch_id)
        expected_path = Path(row["envelope_path"]).resolve()
        supplied_path = (
            expected_path if envelope_path is None else Path(envelope_path).resolve()
        )
        supplied_digest = envelope_sha256 or str(row["envelope_sha256"])
        if supplied_path != expected_path or supplied_digest != row["envelope_sha256"]:
            raise BatchValidationError("batch id/path/digest claim reference mismatch")
        try:
            content = supplied_path.read_bytes()
        except FileNotFoundError as error:
            raise BatchValidationError("claim envelope is missing") from error
        actual = hashlib.sha256(content).hexdigest()
        if actual != supplied_digest:
            raise BatchValidationError(
                f"claim envelope digest mismatch: expected {supplied_digest}, got {actual}"
            )
        try:
            envelope = json.loads(content)
        except json.JSONDecodeError as error:
            raise BatchValidationError("claim envelope is not valid JSON") from error
        if (
            envelope.get("schema_version") != CLAIM_ENVELOPE_VERSION
            or envelope.get("batch_id") != identity
        ):
            raise BatchValidationError("claim envelope version or batch mismatch")
        return envelope

    def load_claim_envelope_with_digest(
        self, batch_id: str
    ) -> tuple[dict[str, Any], str]:
        identity = _required_text(batch_id, "batch_id")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT envelope_sha256 FROM batches WHERE batch_id = ?",
                (identity,),
            ).fetchone()
        if row is None:
            raise KeyError(batch_id)
        digest = str(row["envelope_sha256"])
        return (
            self.load_claim_envelope(identity, envelope_sha256=digest),
            digest,
        )

    def now(self) -> float:
        """Return the backend clock used for lease decisions."""
        return self._now()

    def process_batch_state(self, envelope: Any) -> ControlBatchState:
        with self._connect() as connection:
            batch = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?",
                (envelope.batch_id,),
            ).fetchone()
            if batch is None:
                raise KeyError(envelope.batch_id)
            self._assert_process_rows(
                connection, envelope, str(batch["status"])
            )
            return ControlBatchState(
                str(batch["status"]), float(batch["lease_expires_at"])
            )

    def assert_process_fence(self, envelope: Any) -> None:
        live = self.process_batch_state(envelope)
        if live.status == "COMMITTED":
            return
        if (
            live.status not in {"LEASED", "SEALED"}
            or live.lease_expires_at <= self._now()
        ):
            raise LeaseLostError(
                f"batch {envelope.batch_id} does not own a live fence"
            )

    def heartbeat_process(
        self, envelope: Any, extension_seconds: float
    ) -> None:
        extension = _positive_finite(extension_seconds, "extension_seconds")
        now = self._now()
        with self._transaction() as connection:
            batch = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?",
                (envelope.batch_id,),
            ).fetchone()
            if (
                batch is None
                or batch["status"] != "LEASED"
                or float(batch["lease_expires_at"]) <= now
            ):
                raise LeaseLostError("heartbeat lost the batch lease")
            self._assert_process_rows(connection, envelope, "LEASED")
            expires = max(float(batch["lease_expires_at"]), now + extension)
            connection.execute(
                "UPDATE batches SET lease_expires_at = ? WHERE batch_id = ?",
                (expires, envelope.batch_id),
            )
            connection.execute(
                "UPDATE attempts SET lease_expires_at = ? "
                "WHERE batch_id = ? AND status = 'LEASED'",
                (expires, envelope.batch_id),
            )
            for item in envelope.items:
                updated = connection.execute(
                    "UPDATE work SET lease_expires_at = ?, updated_at = ? "
                    "WHERE work_id = ? AND status = 'LEASED' "
                    "AND lease_attempt_id = ? AND fence = ?",
                    (
                        expires,
                        now,
                        item.work_id,
                        item.attempt_id,
                        item.fence,
                    ),
                )
                if updated.rowcount != 1:
                    raise LeaseLostError(
                        f"heartbeat lost fence for {item.work_id}"
                    )

    @staticmethod
    def _assert_process_rows(
        connection: sqlite3.Connection,
        envelope: Any,
        batch_status: str,
    ) -> None:
        rows = connection.execute(
            "SELECT a.attempt_id, a.work_id, a.fence, a.payload_sha256, "
            "a.status, w.status work_status, w.lease_attempt_id, "
            "w.fence work_fence, b.envelope_sha256, b.membership_sha256 "
            "FROM attempts a JOIN work w ON w.work_id = a.work_id "
            "JOIN batches b ON b.batch_id = a.batch_id "
            "WHERE a.batch_id = ?",
            (envelope.batch_id,),
        ).fetchall()
        expected = {
            (item.work_id, item.attempt_id, item.fence, item.payload_sha256)
            for item in envelope.items
        }
        observed = {
            (
                str(row["work_id"]),
                str(row["attempt_id"]),
                int(row["fence"]),
                str(row["payload_sha256"]),
            )
            for row in rows
        }
        if observed != expected:
            raise LeaseLostError("authoritative attempt membership changed")
        for row in rows:
            if (
                row["envelope_sha256"] != envelope.envelope_sha256
                or row["membership_sha256"] != envelope.membership_sha256
            ):
                raise BatchValidationError(
                    "authoritative envelope identity changed"
                )
            if batch_status in {"LEASED", "SEALED"} and (
                row["work_status"] != "LEASED"
                or row["lease_attempt_id"] != row["attempt_id"]
                or int(row["work_fence"]) != int(row["fence"])
            ):
                raise LeaseLostError(
                    f"attempt {row['attempt_id']} lost its work fence"
                )

    def seal_batch(
        self,
        batch_id: str,
        outputs: Sequence[Mapping[str, Any]],
        *,
        envelope_sha256: str,
        membership_sha256: str,
    ) -> None:
        """Seal an exact terminal output set while the lease fence is live."""
        identity = _required_text(batch_id, "batch_id")
        digest = _validated_sha256(envelope_sha256, "envelope_sha256")
        membership = _validated_sha256(membership_sha256, "membership_sha256")
        if not isinstance(outputs, Sequence) or isinstance(outputs, (str, bytes)):
            raise ValueError("outputs must be a sequence")
        now = self._now()
        with self._transaction() as connection:
            batch = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (identity,)
            ).fetchone()
            if batch is None:
                raise KeyError(batch_id)
            if batch["status"] == "SEALED":
                self._verify_existing_seal(connection, batch, outputs, digest, membership)
                return
            self._validate_live_seal(batch, identity, digest, membership, now)
            members = connection.execute(
                """
                SELECT * FROM batch_members
                WHERE batch_id = ? ORDER BY ordinal
                """,
                (identity,),
            ).fetchall()
            normalized = self._normalize_outputs(outputs)
            expected = {(row["work_id"], row["attempt_id"]) for row in members}
            observed = {(item["work_id"], item["attempt_id"]) for item in normalized}
            if len(normalized) != len(members) or observed != expected:
                raise BatchValidationError(
                    "sealed output cardinality/membership differs from the claim"
                )
            for item in normalized:
                self._seal_item(connection, identity, item, now)
            connection.execute(
                "UPDATE batches SET status = 'SEALED', sealed_at = ? WHERE batch_id = ?",
                (now, identity),
            )

    @staticmethod
    def _validate_live_seal(
        batch: sqlite3.Row,
        batch_id: str,
        envelope_sha256: str,
        membership_sha256: str,
        now: float,
    ) -> None:
        if batch["status"] != "LEASED" or float(batch["lease_expires_at"]) <= now:
            raise LeaseLostError(f"batch {batch_id} no longer owns a live lease")
        if (
            envelope_sha256 != batch["envelope_sha256"]
            or membership_sha256 != batch["membership_sha256"]
        ):
            raise BatchValidationError("envelope or membership hash mismatch")

    @staticmethod
    def _seal_item(
        connection: sqlite3.Connection,
        batch_id: str,
        item: Mapping[str, Any],
        now: float,
    ) -> None:
        attempt = connection.execute(
            """
            SELECT a.*, w.lease_attempt_id, w.fence AS work_fence,
                   w.status AS work_status
            FROM attempts a JOIN work w ON w.work_id = a.work_id
            WHERE a.attempt_id = ?
            """,
            (item["attempt_id"],),
        ).fetchone()
        valid = (
            attempt is not None
            and attempt["batch_id"] == batch_id
            and attempt["status"] == "LEASED"
            and attempt["work_status"] == "LEASED"
            and attempt["lease_attempt_id"] == item["attempt_id"]
            and int(attempt["fence"]) == int(attempt["work_fence"])
        )
        if not valid:
            raise LeaseLostError(f"attempt {item['attempt_id']} lost its work fence")
        connection.execute(
            """
            UPDATE attempts SET status = 'SEALED', output_path = ?,
                output_sha256 = ?, terminal_succeeded = ?, sealed_at = ?
            WHERE attempt_id = ? AND status = 'LEASED'
            """,
            (
                item["output_path"],
                item["output_sha256"],
                int(item["succeeded"]),
                now,
                item["attempt_id"],
            ),
        )
        connection.executemany(
            """
            INSERT INTO output_records (
                attempt_id, work_id, output_path, output_sha256,
                executor_identity, partition_id, task_attempt_id,
                record_sequence, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    item["attempt_id"],
                    item["work_id"],
                    item["output_path"],
                    item["output_sha256"],
                    record["executor_identity"],
                    record["partition_id"],
                    record["task_attempt_id"],
                    record["record_sequence"],
                    now,
                )
                for record in item["records"]
            ),
        )

    def commit_batch(self, batch_id: str) -> tuple[int, ...]:
        """Atomically publish every sealed attempt and advance visible pointers."""
        identity = _required_text(batch_id, "batch_id")
        now = self._now()
        with self._transaction() as connection:
            batch = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (identity,)
            ).fetchone()
            if batch is None:
                raise KeyError(batch_id)
            if batch["status"] == "COMMITTED":
                return tuple(
                    int(row[0])
                    for row in connection.execute(
                        """
                        SELECT publication_sequence FROM publications
                        WHERE batch_id = ? ORDER BY publication_sequence
                        """,
                        (identity,),
                    ).fetchall()
                )
            if batch["status"] != "SEALED" or float(batch["lease_expires_at"]) <= now:
                raise LeaseLostError("only a live, sealed batch can be committed")
            rows = connection.execute(
                """
                SELECT a.*, w.status AS work_status,
                       w.lease_attempt_id, w.fence AS work_fence,
                       w.committed_attempt_id, w.attempt_count,
                       w.max_attempts
                FROM attempts a JOIN work w ON w.work_id = a.work_id
                WHERE a.batch_id = ? ORDER BY a.work_id
                """,
                (identity,),
            ).fetchall()
            if len(rows) != int(batch["item_count"]):
                raise BatchValidationError("attempt cardinality differs from batch")
            sequences: list[int] = []
            for row in rows:
                sequence = self._commit_attempt(connection, identity, row, now)
                if sequence is not None:
                    sequences.append(sequence)
            connection.execute(
                """
                UPDATE batches SET status = 'COMMITTED', committed_at = ?
                WHERE batch_id = ? AND status = 'SEALED'
                """,
                (now, identity),
            )
            return tuple(sequences)

    @staticmethod
    def _commit_attempt(
        connection: sqlite3.Connection,
        batch_id: str,
        row: sqlite3.Row,
        now: float,
    ) -> int | None:
        valid = (
            row["status"] == "SEALED"
            and row["work_status"] == "LEASED"
            and row["lease_attempt_id"] == row["attempt_id"]
            and int(row["work_fence"]) == int(row["fence"])
            and row["committed_attempt_id"] is None
            and row["output_path"] is not None
            and row["output_sha256"] is not None
        )
        if not valid:
            raise LeaseLostError(
                f"attempt {row['attempt_id']} cannot advance its pointer"
            )
        if not bool(row["terminal_succeeded"]):
            SQLiteControlStore._commit_failed_attempt(connection, row, now)
            return None
        cursor = connection.execute(
            """
            INSERT INTO publications (
                work_id, attempt_id, batch_id, output_path,
                output_sha256, published_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                row["work_id"],
                row["attempt_id"],
                batch_id,
                row["output_path"],
                row["output_sha256"],
                now,
            ),
        )
        sequence = int(cursor.lastrowid)
        updated = connection.execute(
            """
            UPDATE work SET status = 'SUCCEEDED',
                committed_attempt_id = ?, publication_sequence = ?,
                lease_owner = NULL, lease_attempt_id = NULL,
                lease_expires_at = NULL, updated_at = ?
            WHERE work_id = ? AND status = 'LEASED'
                AND lease_attempt_id = ? AND fence = ?
            """,
            (
                row["attempt_id"],
                sequence,
                now,
                row["work_id"],
                row["attempt_id"],
                row["fence"],
            ),
        )
        if updated.rowcount != 1:
            raise LeaseLostError(f"publication fence lost for {row['work_id']}")
        connection.execute(
            "UPDATE attempts SET status = 'SUCCEEDED' WHERE attempt_id = ?",
            (row["attempt_id"],),
        )
        return sequence

    @staticmethod
    def _commit_failed_attempt(
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        now: float,
    ) -> None:
        outcome = (
            "DEAD"
            if int(row["attempt_count"]) >= int(row["max_attempts"])
            else "READY"
        )
        updated = connection.execute(
            """
            UPDATE work SET status = ?, available_at = ?,
                lease_owner = NULL, lease_attempt_id = NULL,
                lease_expires_at = NULL,
                last_error = 'processing failed', updated_at = ?
            WHERE work_id = ? AND status = 'LEASED'
                AND lease_attempt_id = ? AND fence = ?
            """,
            (
                outcome,
                now,
                now,
                row["work_id"],
                row["attempt_id"],
                row["fence"],
            ),
        )
        if updated.rowcount != 1:
            raise LeaseLostError(f"failed-attempt fence lost for {row['work_id']}")
        connection.execute(
            "UPDATE attempts SET status = 'FAILED' WHERE attempt_id = ?",
            (row["attempt_id"],),
        )

    def recover(self, *, now: float | None = None) -> RecoveryReport:
        """Fence expired batches and move each work item to retry or dead."""
        timestamp = self._now() if now is None else _finite(now, "now")
        retried = 0
        dead = 0
        with self._transaction() as connection:
            rows = connection.execute(
                """
                SELECT * FROM work
                WHERE status = 'LEASED' AND lease_expires_at <= ?
                ORDER BY work_id
                """,
                (timestamp,),
            ).fetchall()
            for row in rows:
                outcome = (
                    "DEAD"
                    if int(row["attempt_count"]) >= int(row["max_attempts"])
                    else "READY"
                )
                if outcome == "DEAD":
                    dead += 1
                else:
                    retried += 1
                attempt_id = str(row["lease_attempt_id"])
                connection.execute(
                    """
                    UPDATE attempts
                    SET status = 'EXPIRED', recovery_outcome = ?
                    WHERE attempt_id = ? AND status IN ('LEASED', 'SEALED')
                    """,
                    (outcome, attempt_id),
                )
                updated = connection.execute(
                    """
                    UPDATE work SET status = ?, available_at = ?,
                        lease_owner = NULL, lease_attempt_id = NULL,
                        lease_expires_at = NULL,
                        last_error = 'lease expired', updated_at = ?
                    WHERE work_id = ? AND status = 'LEASED'
                        AND lease_attempt_id = ? AND fence = ?
                    """,
                    (
                        outcome,
                        timestamp,
                        timestamp,
                        row["work_id"],
                        attempt_id,
                        row["fence"],
                    ),
                )
                if updated.rowcount != 1:
                    raise LeaseLostError(
                        f"recovery fence changed for work {row['work_id']}"
                    )
            connection.execute(
                """
                UPDATE batches SET status = 'EXPIRED'
                WHERE status IN ('LEASED', 'SEALED') AND lease_expires_at <= ?
                """,
                (timestamp,),
            )
        return RecoveryReport(len(rows), retried, dead)

    def replay(
        self,
        work_id: str,
        *,
        operator: str,
        reason: str,
        additional_attempts: int = 1,
    ) -> ReplayRequest:
        """Requeue dead work with a stable, operator-attributed replay identity."""
        identity = _required_text(work_id, "work_id")
        actor = _required_text(operator, "operator")
        explanation = _required_text(reason, "reason")
        if type(additional_attempts) is not int or not 1 <= additional_attempts <= 100:
            raise ValueError("additional_attempts must be between 1 and 100")
        now = self._now()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM work WHERE work_id = ?", (identity,)
            ).fetchone()
            if row is None:
                raise KeyError(work_id)
            if row["status"] != "DEAD":
                previous = self._matching_last_replay(
                    connection, row, actor, explanation, additional_attempts
                )
                if previous is not None:
                    return previous
                raise ControlError(
                    f"work {identity} is not replay-eligible; status={row['status']}"
                )
            if row["committed_attempt_id"] is not None:
                raise ControlError("committed work cannot be replayed")
            generation = int(row["replay_generation"]) + 1
            replay_id = "replay-" + hashlib.sha256(
                _canonical_json(
                    {
                        "version": 1,
                        "work_id": identity,
                        "generation": generation,
                        "operator": actor,
                        "reason": explanation,
                        "additional_attempts": additional_attempts,
                    }
                ).encode("utf-8")
            ).hexdigest()
            connection.execute(
                """
                INSERT INTO replay_requests (
                    replay_id, work_id, operator, reason, generation,
                    requested_at, from_status, additional_attempts
                ) VALUES (?, ?, ?, ?, ?, ?, 'DEAD', ?)
                """,
                (
                    replay_id,
                    identity,
                    actor,
                    explanation,
                    generation,
                    now,
                    additional_attempts,
                ),
            )
            connection.execute(
                """
                UPDATE work SET status = 'READY',
                    max_attempts = attempt_count + ?,
                    available_at = ?, replay_generation = ?,
                    last_replay_id = ?, last_error = NULL, updated_at = ?
                WHERE work_id = ? AND status = 'DEAD'
                """,
                (
                    additional_attempts,
                    now,
                    generation,
                    replay_id,
                    now,
                    identity,
                ),
            )
            return ReplayRequest(
                replay_id,
                identity,
                actor,
                explanation,
                generation,
                now,
            )

    def reconcile(
        self, *, now: float | None = None
    ) -> list[ReconciliationFinding]:
        """Persist the current integrity findings under stable identities."""
        timestamp = self._now() if now is None else _finite(now, "now")
        detected: dict[str, tuple[str, str, str, dict[str, Any]]] = {}
        with self._transaction() as connection:
            self._detect_relational_findings(connection, timestamp, detected)
            self._detect_batch_findings(connection, detected)
            current_ids: list[str] = []
            findings: list[ReconciliationFinding] = []
            for _, (kind, severity, entity, details) in sorted(detected.items()):
                finding_id = _finding_id(kind, entity)
                current_ids.append(finding_id)
                details_json = _canonical_json(details)
                connection.execute(
                    """
                    INSERT INTO reconciliation_findings (
                        finding_id, finding_type, severity, entity_key,
                        details_json, first_seen_at, last_seen_at, resolved_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                    ON CONFLICT(finding_id) DO UPDATE SET
                        severity = excluded.severity,
                        details_json = excluded.details_json,
                        last_seen_at = excluded.last_seen_at,
                        resolved_at = NULL
                    """,
                    (
                        finding_id,
                        kind,
                        severity,
                        entity,
                        details_json,
                        timestamp,
                        timestamp,
                    ),
                )
            if current_ids:
                placeholders = ",".join("?" for _ in current_ids)
                connection.execute(
                    f"""
                    UPDATE reconciliation_findings SET resolved_at = ?
                    WHERE resolved_at IS NULL
                      AND finding_id NOT IN ({placeholders})
                    """,
                    (timestamp, *current_ids),
                )
            else:
                connection.execute(
                    """
                    UPDATE reconciliation_findings SET resolved_at = ?
                    WHERE resolved_at IS NULL
                    """,
                    (timestamp,),
                )
            rows = connection.execute(
                """
                SELECT * FROM reconciliation_findings
                WHERE resolved_at IS NULL
                ORDER BY severity, finding_type, entity_key
                """
            ).fetchall()
            for row in rows:
                findings.append(
                    ReconciliationFinding(
                        row["finding_id"],
                        row["finding_type"],
                        row["severity"],
                        row["entity_key"],
                        json.loads(row["details_json"]),
                        float(row["first_seen_at"]),
                        float(row["last_seen_at"]),
                    )
                )
            return findings

    def status(self) -> dict[str, Any]:
        with self._connect() as connection:
            work = {
                row["status"]: int(row["count"])
                for row in connection.execute(
                    "SELECT status, COUNT(*) count FROM work GROUP BY status"
                )
            }
            batches = {
                row["status"]: int(row["count"])
                for row in connection.execute(
                    "SELECT status, COUNT(*) count FROM batches GROUP BY status"
                )
            }
            open_findings = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM reconciliation_findings
                    WHERE resolved_at IS NULL
                    """
                ).fetchone()[0]
            )
            last_publication = connection.execute(
                "SELECT MAX(publication_sequence) FROM publications"
            ).fetchone()[0]
        return {
            "schema_version": SCHEMA_VERSION,
            "work": work,
            "batches": batches,
            "open_findings": open_findings,
            "last_publication_sequence": last_publication,
        }

    def _detect_relational_findings(
        self,
        connection: sqlite3.Connection,
        now: float,
        detected: dict[str, tuple[str, str, str, dict[str, Any]]],
    ) -> None:
        def add(kind: str, severity: str, entity: str, details: dict[str, Any]) -> None:
            detected[_finding_id(kind, entity)] = (kind, severity, entity, details)

        invalid = connection.execute(
            """
            SELECT w.work_id, w.committed_attempt_id, a.work_id pointed_work,
                   a.status attempt_status, p.attempt_id publication_attempt,
                   w.publication_sequence, p.publication_sequence actual_sequence
            FROM work w
            LEFT JOIN attempts a ON a.attempt_id = w.committed_attempt_id
            LEFT JOIN publications p
                ON p.work_id = w.work_id
               AND p.attempt_id = w.committed_attempt_id
            WHERE w.committed_attempt_id IS NOT NULL
              AND (a.attempt_id IS NULL OR a.work_id != w.work_id
                   OR a.status != 'SUCCEEDED' OR p.attempt_id IS NULL
                   OR w.publication_sequence != p.publication_sequence)
            """
        ).fetchall()
        for row in invalid:
            add(
                "INVALID_COMMITTED_POINTER",
                "ERROR",
                str(row["work_id"]),
                {"committed_attempt_id": row["committed_attempt_id"]},
            )
        missing = connection.execute(
            """
            SELECT DISTINCT w.work_id, w.status
            FROM work w
            LEFT JOIN publications p ON p.work_id = w.work_id
            LEFT JOIN attempts a
                ON a.work_id = w.work_id AND a.status = 'SUCCEEDED'
            WHERE w.committed_attempt_id IS NULL
              AND (w.status = 'SUCCEEDED' OR p.attempt_id IS NOT NULL
                   OR a.attempt_id IS NOT NULL)
            """
        ).fetchall()
        for row in missing:
            add(
                "MISSING_COMMITTED_POINTER",
                "ERROR",
                str(row["work_id"]),
                {"status": row["status"]},
            )
        sealed = connection.execute(
            """
            SELECT a.attempt_id, a.work_id, a.batch_id
            FROM attempts a
            LEFT JOIN publications p ON p.attempt_id = a.attempt_id
            WHERE a.status = 'SEALED' AND p.attempt_id IS NULL
            """
        ).fetchall()
        for row in sealed:
            add(
                "SEALED_UNCOMMITTED_ATTEMPT",
                "WARN",
                str(row["attempt_id"]),
                {"work_id": row["work_id"], "batch_id": row["batch_id"]},
            )
        orphan = connection.execute(
            """
            SELECT o.output_id, o.attempt_id, o.work_id, o.output_path
            FROM output_records o
            LEFT JOIN attempts a ON a.attempt_id = o.attempt_id
            WHERE a.attempt_id IS NULL OR a.work_id != o.work_id
            """
        ).fetchall()
        for row in orphan:
            add(
                "ORPHAN_OUTPUT",
                "ERROR",
                str(row["output_id"]),
                {
                    "attempt_id": row["attempt_id"],
                    "work_id": row["work_id"],
                    "output_path": row["output_path"],
                },
            )
        expired = connection.execute(
            """
            SELECT work_id, lease_attempt_id, lease_expires_at FROM work
            WHERE status = 'LEASED' AND lease_expires_at <= ?
            """,
            (now,),
        ).fetchall()
        for row in expired:
            add(
                "EXPIRED_LEASE",
                "ERROR",
                str(row["work_id"]),
                {
                    "attempt_id": row["lease_attempt_id"],
                    "lease_expires_at": row["lease_expires_at"],
                },
            )
        duplicates = connection.execute(
            """
            SELECT work_id, attempt_id, record_sequence, COUNT(*) count
            FROM output_records
            GROUP BY work_id, attempt_id, record_sequence
            HAVING COUNT(*) > 1
            """
        ).fetchall()
        for row in duplicates:
            entity = (
                f"{row['work_id']}:{row['attempt_id']}:"
                f"{row['record_sequence']}"
            )
            add(
                "DUPLICATE_OUTPUT_IDENTITY",
                "ERROR",
                entity,
                {"count": int(row["count"])},
            )

    def _detect_batch_findings(
        self,
        connection: sqlite3.Connection,
        detected: dict[str, tuple[str, str, str, dict[str, Any]]],
    ) -> None:
        def add(kind: str, entity: str, details: dict[str, Any]) -> None:
            detected[_finding_id(kind, entity)] = (kind, "ERROR", entity, details)

        batches = connection.execute("SELECT * FROM batches").fetchall()
        for batch in batches:
            members = connection.execute(
                """
                SELECT work_id, attempt_id, fence, payload_sha256
                FROM batch_members WHERE batch_id = ? ORDER BY ordinal
                """,
                (batch["batch_id"],),
            ).fetchall()
            calculated = _membership_sha256(
                [
                    ClaimedWork(
                        str(row["work_id"]),
                        str(row["attempt_id"]),
                        int(row["fence"]),
                        str(row["payload_sha256"]),
                    )
                    for row in members
                ]
            )
            if (
                len(members) != int(batch["item_count"])
                or calculated != batch["membership_sha256"]
            ):
                add(
                    "MEMBERSHIP_HASH_MISMATCH",
                    str(batch["batch_id"]),
                    {
                        "expected_count": batch["item_count"],
                        "actual_count": len(members),
                        "expected_sha256": batch["membership_sha256"],
                        "actual_sha256": calculated,
                    },
                )
            attempt_mismatches = connection.execute(
                """
                SELECT a.attempt_id
                FROM attempts a
                LEFT JOIN batch_members m ON m.attempt_id = a.attempt_id
                WHERE a.batch_id = ?
                  AND (m.attempt_id IS NULL OR m.work_id != a.work_id
                       OR m.fence != a.fence
                       OR m.payload_sha256 != a.payload_sha256)
                """,
                (batch["batch_id"],),
            ).fetchall()
            if attempt_mismatches:
                add(
                    "MEMBERSHIP_HASH_MISMATCH",
                    str(batch["batch_id"]),
                    {
                        "reason": "attempt rows differ from batch membership",
                        "attempt_ids": [
                            str(row["attempt_id"]) for row in attempt_mismatches
                        ],
                    },
                )
            path = Path(batch["envelope_path"])
            try:
                content = path.read_bytes()
                actual_digest = hashlib.sha256(content).hexdigest()
            except OSError:
                actual_digest = None
            if actual_digest != batch["envelope_sha256"]:
                add(
                    "ENVELOPE_HASH_MISMATCH",
                    str(batch["batch_id"]),
                    {
                        "expected_sha256": batch["envelope_sha256"],
                        "actual_sha256": actual_digest,
                        "path": str(path),
                    },
                )
                continue
            try:
                envelope = json.loads(content)
                envelope_items = envelope["items"]
                envelope_membership = envelope["membership_sha256"]
            except (json.JSONDecodeError, KeyError, TypeError):
                envelope_items = None
                envelope_membership = None
            if (
                not isinstance(envelope_items, list)
                or len(envelope_items) != int(batch["item_count"])
                or envelope_membership != batch["membership_sha256"]
            ):
                add(
                    "MEMBERSHIP_HASH_MISMATCH",
                    str(batch["batch_id"]),
                    {"reason": "claim envelope membership differs from SQLite"},
                )

    def _verify_existing_seal(
        self,
        connection: sqlite3.Connection,
        batch: sqlite3.Row,
        outputs: Sequence[Mapping[str, Any]],
        digest: str,
        membership: str,
    ) -> None:
        if digest != batch["envelope_sha256"] or membership != batch["membership_sha256"]:
            raise ImmutableConflictError("sealed batch hashes are immutable")
        normalized = self._normalize_outputs(outputs)
        existing = connection.execute(
            """
            SELECT work_id, attempt_id, output_path, output_sha256,
                   terminal_succeeded
            FROM attempts WHERE batch_id = ? ORDER BY work_id
            """,
            (batch["batch_id"],),
        ).fetchall()
        requested = sorted(
            (
                item["work_id"],
                item["attempt_id"],
                item["output_path"],
                item["output_sha256"],
                int(item["succeeded"]),
            )
            for item in normalized
        )
        stored = sorted(tuple(row) for row in existing)
        if requested != stored:
            raise ImmutableConflictError("sealed batch output is immutable")
        existing_records = connection.execute(
            """
            SELECT work_id, attempt_id, executor_identity, partition_id,
                   task_attempt_id, record_sequence
            FROM output_records WHERE attempt_id IN (
                SELECT attempt_id FROM attempts WHERE batch_id = ?
            )
            ORDER BY work_id, attempt_id, record_sequence
            """,
            (batch["batch_id"],),
        ).fetchall()
        requested_records = sorted(
            (
                item["work_id"],
                item["attempt_id"],
                record["executor_identity"],
                record["partition_id"],
                record["task_attempt_id"],
                record["record_sequence"],
            )
            for item in normalized
            for record in item["records"]
        )
        if requested_records != sorted(tuple(row) for row in existing_records):
            raise ImmutableConflictError("sealed batch provenance is immutable")

    @staticmethod
    def _normalize_outputs(
        outputs: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        identities: set[tuple[str, str]] = set()
        for raw in outputs:
            if not isinstance(raw, Mapping):
                raise ValueError("every output must be a mapping")
            work_id = _required_text(raw.get("work_id"), "output work_id")
            attempt_id = _required_text(raw.get("attempt_id"), "output attempt_id")
            identity = (work_id, attempt_id)
            if identity in identities:
                raise BatchValidationError(f"duplicate output identity {identity!r}")
            identities.add(identity)
            records_value = raw.get("records")
            if "records" not in raw or records_value is None:
                raise BatchValidationError(
                    "each output must explicitly contain records"
                )
            if not isinstance(records_value, Sequence) or isinstance(
                records_value, (str, bytes)
            ):
                raise BatchValidationError("output records must be a sequence")
            records: list[dict[str, Any]] = []
            record_sequences: set[int] = set()
            for record in records_value:
                if not isinstance(record, Mapping):
                    raise BatchValidationError(
                        "every output record must be a mapping"
                    )
                values = {
                    "executor_identity": _required_text(
                        record.get("executor_identity"), "executor_identity"
                    ),
                    "partition_id": _nonnegative_int(
                        record.get("partition_id"), "partition_id"
                    ),
                    "task_attempt_id": _nonnegative_int(
                        record.get("task_attempt_id"), "task_attempt_id"
                    ),
                    "record_sequence": _nonnegative_int(
                        record.get("record_sequence"), "record_sequence"
                    ),
                }
                if values["record_sequence"] in record_sequences:
                    raise BatchValidationError(
                        "duplicate output record identity "
                        f"{(work_id, attempt_id, values['record_sequence'])!r}"
                    )
                record_sequences.add(values["record_sequence"])
                records.append(values)
            if not records:
                raise BatchValidationError("each attempt requires a terminal record")
            normalized.append(
                {
                    "work_id": work_id,
                    "attempt_id": attempt_id,
                    "output_path": _required_text(
                        raw.get("output_path"), "output_path"
                    ),
                    "output_sha256": _validated_sha256(
                        raw.get("output_sha256"), "output_sha256"
                    ),
                    "succeeded": _boolean(
                        raw.get("succeeded", True), "output succeeded"
                    ),
                    "records": records,
                }
            )
        return normalized

    def _matching_last_replay(
        self,
        connection: sqlite3.Connection,
        work: sqlite3.Row,
        operator: str,
        reason: str,
        additional_attempts: int,
    ) -> ReplayRequest | None:
        replay_id = work["last_replay_id"]
        if replay_id is None or work["status"] != "READY":
            return None
        row = connection.execute(
            "SELECT * FROM replay_requests WHERE replay_id = ?", (replay_id,)
        ).fetchone()
        if (
            row is None
            or row["operator"] != operator
            or row["reason"] != reason
            or int(row["additional_attempts"]) != additional_attempts
            or int(work["attempt_count"]) + additional_attempts
            != int(work["max_attempts"])
        ):
            return None
        return ReplayRequest(
            row["replay_id"],
            row["work_id"],
            row["operator"],
            row["reason"],
            int(row["generation"]),
            float(row["requested_at"]),
        )

    @staticmethod
    def _registered(row: sqlite3.Row) -> RegisteredWork:
        return RegisteredWork(
            str(row["work_id"]),
            str(row["status"]),
            str(row["payload_sha256"]),
            str(row["runtime_key"]),
            float(row["duration_seconds"]),
            int(row["attempt_count"]),
            int(row["max_attempts"]),
            int(row["original_max_attempts"]),
            row["committed_attempt_id"],
            row["publication_sequence"],
            row["last_replay_id"],
        )

    def _new_id(self) -> str:
        return _required_text(self._id_factory(), "generated id")

    def _now(self) -> float:
        return _finite(self._clock(), "clock value")

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database,
            timeout=self._busy_timeout_ms / 1000,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    @contextmanager
    def _transaction(self):
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()


LocalControlStore = SQLiteControlStore


def _allowed_work_scope(
    allowed_work_ids: Collection[str] | None,
) -> tuple[str, ...] | None:
    if allowed_work_ids is None:
        return None
    if isinstance(allowed_work_ids, (str, bytes)) or not isinstance(
        allowed_work_ids, Collection
    ):
        raise ValueError("allowed_work_ids must be a collection")
    return tuple(
        sorted({_required_text(work_id, "allowed work_id") for work_id in allowed_work_ids})
    )


def _claim_admission_settings(
    process_profile: object | None,
    *,
    minimum_speed_x: float | None,
    safety_factor: float | None,
    margin_seconds: float | None,
    peak_rss_bytes: int | None,
) -> tuple[float, float, float, int, str]:
    if process_profile is None:
        if (
            minimum_speed_x is None
            or safety_factor is None
            or margin_seconds is None
        ):
            raise ValueError(
                "process_profile or all legacy lease admission values are required"
            )
        if peak_rss_bytes is not None:
            raise ValueError("peak_rss_bytes requires process_profile")
        return (
            _positive_finite(minimum_speed_x, "minimum_speed_x"),
            _positive_finite(safety_factor, "safety_factor"),
            _positive_finite(margin_seconds, "margin_seconds"),
            1,
            "legacy-single-worker",
        )

    profile_speed = _positive_finite(
        getattr(process_profile, "minimum_speed_x", None),
        "process_profile.minimum_speed_x",
    )
    profile_factor = _positive_finite(
        getattr(process_profile, "lease_safety_factor", None),
        "process_profile.lease_safety_factor",
    )
    profile_margin = _positive_finite(
        getattr(process_profile, "lease_margin_seconds", None),
        "process_profile.lease_margin_seconds",
    )
    profile_instances = _positive_int(
        getattr(process_profile, "executor_instances", None),
        "process_profile.executor_instances",
    )
    profile_cores = _positive_int(
        getattr(process_profile, "executor_cores", None),
        "process_profile.executor_cores",
    )
    task_cpus = _positive_int(
        getattr(process_profile, "task_cpus", None),
        "process_profile.task_cpus",
    )
    cpu_limit = profile_instances * profile_cores // task_cpus
    if cpu_limit < 1:
        raise ValueError("process_profile has no schedulable workers")
    worker_limit = min(profile_instances, cpu_limit)
    if peak_rss_bytes is not None:
        peak = _positive_int(peak_rss_bytes, "peak_rss_bytes")
        memory = _positive_int(
            getattr(process_profile, "executor_memory_bytes", None),
            "process_profile.executor_memory_bytes",
        )
        reserve = _nonnegative_int(
            getattr(process_profile, "memory_reserve_bytes", None),
            "process_profile.memory_reserve_bytes",
        )
        usable = memory - reserve
        if peak > usable:
            raise LeaseBudgetError(
                "measured peak RSS does not fit in one configured executor"
            )
        worker_limit = min(
            worker_limit,
            profile_instances * (usable // peak),
        )
    supplied = (
        (minimum_speed_x, profile_speed, "minimum_speed_x"),
        (safety_factor, profile_factor, "safety_factor"),
        (margin_seconds, profile_margin, "margin_seconds"),
    )
    for value, expected, name in supplied:
        if value is not None and _finite(value, name) != expected:
            raise ValueError(f"{name} conflicts with process_profile")
    profile_name = _required_text(
        getattr(process_profile, "name", None), "process_profile.name"
    )
    return profile_speed, profile_factor, profile_margin, worker_limit, profile_name


def _lpt_makespan_seconds(rows: Sequence[sqlite3.Row], worker_count: int) -> float:
    workers = _positive_int(worker_count, "worker_count")
    costs = [0.0] * workers
    for row in sorted(
        rows,
        key=lambda candidate: (
            -float(candidate["duration_seconds"]),
            str(candidate["work_id"]),
        ),
    ):
        target = min(range(workers), key=lambda index: (costs[index], index))
        costs[target] += float(row["duration_seconds"])
    return max(costs)


def _largest_admissible_claim_prefix(
    rows: Sequence[sqlite3.Row],
    *,
    minimum_items: int,
    profile_workers: int,
    minimum_speed_x: float,
    safety_factor: float,
    margin_seconds: float,
    lease_seconds: float,
) -> tuple[tuple[sqlite3.Row, ...], int, float]:
    """Return the largest queue-ordered prefix that safely fits the lease."""

    def projection(
        prefix: Sequence[sqlite3.Row],
    ) -> tuple[int, float, float]:
        workers = min(len(prefix), profile_workers)
        makespan = _lpt_makespan_seconds(prefix, workers)
        projected = makespan / minimum_speed_x * safety_factor
        return workers, makespan, projected

    minimum_prefix = tuple(rows[:minimum_items])
    minimum_workers, minimum_makespan, minimum_projected = projection(
        minimum_prefix
    )
    if minimum_projected + margin_seconds >= lease_seconds:
        raise LeaseBudgetError(
            "projected LPT makespan does not fit inside the lease budget: "
            f"workers={minimum_workers} makespan={minimum_makespan:g}s "
            f"projected={minimum_projected:g}s margin={margin_seconds:g}s "
            f"lease={lease_seconds:g}s"
        )
    for count in range(len(rows), minimum_items, -1):
        prefix = tuple(rows[:count])
        workers, makespan, projected = projection(prefix)
        if projected + margin_seconds < lease_seconds:
            return prefix, workers, makespan
    return minimum_prefix, minimum_workers, minimum_makespan


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        raise ValueError("value must be finite JSON data") from error


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _membership_sha256(items: Sequence[ClaimedWork]) -> str:
    return _sha256_text(
        _canonical_json(
            [
                {
                    "work_id": item.work_id,
                    "attempt_id": item.attempt_id,
                    "fence": item.fence,
                    "payload_sha256": item.payload_sha256,
                }
                for item in items
            ]
        )
    )


def _finding_id(kind: str, entity: str) -> str:
    return "finding-" + _sha256_text(
        _canonical_json({"version": 1, "type": kind, "entity": entity})
    )


def _required_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _finite(value: object, name: str) -> float:
    if type(value) not in (int, float) or not math.isfinite(float(value)):
        raise ValueError(f"{name} must be a finite number")
    return float(value)


def _positive_finite(value: object, name: str) -> float:
    result = _finite(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return result


def _nonnegative_int(value: object, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _positive_int(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _boolean(value: object, name: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{name} must be a boolean")
    return value


def _validated_sha256(value: object, name: str) -> str:
    text = _required_text(value, name)
    if len(text) != _SHA256_LENGTH or any(
        character not in "0123456789abcdef" for character in text
    ):
        raise ValueError(f"{name} must be 64 lowercase hexadecimal characters")
    return text


def _json_print(value: Any) -> None:
    print(_canonical_json(value))


def read_registration_manifest(path: Path) -> list[dict[str, Any]]:
    """Read a nonempty JSON array/object or JSONL registration manifest."""
    resolved = Path(path).expanduser().resolve()
    try:
        text = resolved.read_text(encoding="utf-8")
    except OSError as error:
        raise ValueError(f"cannot read registration manifest {resolved}") from error
    try:
        decoded: Any = json.loads(text)
    except json.JSONDecodeError:
        decoded = []
        for line_number, line in enumerate(text.splitlines(), 1):
            if not line.strip():
                continue
            try:
                decoded.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"invalid JSONL at {resolved}:{line_number}"
                ) from error
    if isinstance(decoded, Mapping):
        decoded = decoded.get("items", [decoded])
    if not isinstance(decoded, list) or not decoded:
        raise ValueError("registration manifest must contain at least one item")
    requests: list[dict[str, Any]] = []
    for index, raw in enumerate(decoded):
        if not isinstance(raw, Mapping):
            raise ValueError(f"registration item {index} must be an object")
        payload = raw.get("payload")
        if not isinstance(payload, Mapping):
            raise ValueError(f"registration item {index} payload must be an object")
        max_attempts = raw.get("max_attempts", 3)
        if type(max_attempts) is not int or not 1 <= max_attempts <= 100:
            raise ValueError("max_attempts must be between 1 and 100")
        available_at = raw.get("available_at")
        requests.append(
            {
                "work_id": _required_text(raw.get("work_id"), "work_id"),
                "payload": dict(payload),
                "runtime_key": _required_text(
                    raw.get("runtime_key"), "runtime_key"
                ),
                "duration_seconds": _positive_finite(
                    raw.get("duration_seconds"), "duration_seconds"
                ),
                "config_sha256": _required_text(
                    raw.get("config_sha256"), "config_sha256"
                ),
                "release_digest": _required_text(
                    raw.get("release_digest"), "release_digest"
                ),
                "max_attempts": max_attempts,
                "available_at": (
                    None
                    if available_at is None
                    else _finite(available_at, "available_at")
                ),
            }
        )
    return requests


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--content-root", type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("bootstrap")

    register = commands.add_parser("register")
    register.add_argument("--manifest", type=Path)
    register.add_argument("--work-id")
    register.add_argument("--payload-json")
    register.add_argument("--runtime-key")
    register.add_argument("--duration-seconds", type=float)
    register.add_argument("--config-sha256")
    register.add_argument("--release-digest")
    register.add_argument("--max-attempts", type=int, default=3)

    claim = commands.add_parser("claim")
    claim.add_argument("--owner", required=True)
    claim.add_argument("--max-items", type=int, required=True)
    claim.add_argument("--minimum-items", type=int, default=1)
    claim.add_argument("--lease-seconds", type=float, required=True)
    claim.add_argument("--minimum-speed-x", type=float, required=True)
    claim.add_argument("--safety-factor", type=float, default=1.0)
    claim.add_argument("--margin-seconds", type=float, required=True)

    commands.add_parser("recover")
    commands.add_parser("reconcile")

    replay = commands.add_parser("replay")
    replay.add_argument("--work-id", required=True)
    replay.add_argument("--operator", required=True)
    replay.add_argument("--reason", required=True)
    replay.add_argument("--additional-attempts", type=int, default=1)
    commands.add_parser("status")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the local Candidate A control command."""
    arguments = _parser().parse_args(argv)
    store = SQLiteControlStore(arguments.database, arguments.content_root)
    if arguments.command == "bootstrap":
        _json_print({"schema_version": SCHEMA_VERSION, "database": str(store.database)})
    elif arguments.command == "register":
        if arguments.manifest is not None:
            supplied = (
                arguments.work_id,
                arguments.payload_json,
                arguments.runtime_key,
                arguments.duration_seconds,
                arguments.config_sha256,
                arguments.release_digest,
            )
            if any(value is not None for value in supplied):
                raise ValueError(
                    "--manifest cannot be combined with single-item fields"
                )
            _json_print(
                [asdict(work) for work in store.register_manifest(arguments.manifest)]
            )
        else:
            required = {
                "--work-id": arguments.work_id,
                "--payload-json": arguments.payload_json,
                "--runtime-key": arguments.runtime_key,
                "--duration-seconds": arguments.duration_seconds,
                "--config-sha256": arguments.config_sha256,
                "--release-digest": arguments.release_digest,
            }
            missing = [name for name, value in required.items() if value is None]
            if missing:
                raise ValueError(
                    "single-item registration requires " + ", ".join(missing)
                )
            payload = json.loads(arguments.payload_json)
            work = store.register(
                arguments.work_id,
                payload,
                runtime_key=arguments.runtime_key,
                duration_seconds=arguments.duration_seconds,
                config_sha256=arguments.config_sha256,
                release_digest=arguments.release_digest,
                max_attempts=arguments.max_attempts,
            )
            _json_print(asdict(work))
    elif arguments.command == "claim":
        batch = store.claim(
            arguments.owner,
            max_items=arguments.max_items,
            minimum_items=arguments.minimum_items,
            lease_seconds=arguments.lease_seconds,
            minimum_speed_x=arguments.minimum_speed_x,
            safety_factor=arguments.safety_factor,
            margin_seconds=arguments.margin_seconds,
        )
        _json_print(
            None
            if batch is None
            else {
                **asdict(batch),
                "envelope_path": str(batch.envelope_path),
                "items": [asdict(item) for item in batch.items],
            }
        )
    elif arguments.command == "recover":
        _json_print(asdict(store.recover()))
    elif arguments.command == "reconcile":
        _json_print([asdict(finding) for finding in store.reconcile()])
    elif arguments.command == "replay":
        request = store.replay(
            arguments.work_id,
            operator=arguments.operator,
            reason=arguments.reason,
            additional_attempts=arguments.additional_attempts,
        )
        _json_print(asdict(request))
    elif arguments.command == "status":
        _json_print(store.status())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
