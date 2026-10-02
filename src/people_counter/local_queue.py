"""Durable SQLite queue with bounded PeekLock-style delivery semantics."""

from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Literal, TypeVar
from uuid import uuid4


QueueState = Literal["ready", "locked", "completed", "dead"]
FinalizeAction = Literal["complete", "abandon", "dead"]
_T = TypeVar("_T")


class LocalQueueError(RuntimeError):
    """Base error for local queue operations."""


class QueueConflictError(LocalQueueError):
    """An idempotency key or immutable message conflicts with existing data."""


class MessageNotFoundError(LocalQueueError):
    """The requested message does not exist."""


class LeaseLostError(LocalQueueError):
    """The lock token is absent, expired, or no longer owns the message."""


class ActorClosedError(LocalQueueError):
    """The serialized SQLite actor is closing or has stopped."""


class ActorDiedError(LocalQueueError):
    """The serialized SQLite actor terminated unexpectedly."""


@dataclass(frozen=True)
class ClaimedMessage:
    message_id: str
    idempotency_key: str
    payload: dict[str, Any]
    lock_token: str
    locked_until: float
    delivery_count: int
    max_delivery_count: int


@dataclass(frozen=True)
class QueueMessage:
    message_id: str
    idempotency_key: str
    state: QueueState
    delivery_count: int
    max_delivery_count: int
    available_at: float
    locked_until: float | None
    dead_letter_reason: str | None


class SQLiteQueue:
    """SQLite-backed queue intended exclusively for the local driver."""

    def __init__(self, database: Path, *, busy_timeout_ms: int = 5_000) -> None:
        if busy_timeout_ms < 1 or busy_timeout_ms > 60_000:
            raise ValueError("busy_timeout_ms must be between 1 and 60000")
        self.database = database.expanduser().resolve()
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self._busy_timeout_ms = busy_timeout_ms
        self.initialize()

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS queue_messages (
                    message_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (
                        state IN ('ready', 'locked', 'completed', 'dead')
                    ),
                    delivery_count INTEGER NOT NULL DEFAULT 0,
                    max_delivery_count INTEGER NOT NULL,
                    available_at REAL NOT NULL,
                    lock_token TEXT UNIQUE,
                    locked_by TEXT,
                    locked_until REAL,
                    dead_letter_reason TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    CHECK (
                        (state = 'locked' AND lock_token IS NOT NULL
                            AND locked_until IS NOT NULL)
                        OR
                        (state != 'locked' AND lock_token IS NULL
                            AND locked_until IS NULL)
                    )
                );
                CREATE INDEX IF NOT EXISTS queue_ready_idx
                ON queue_messages(state, available_at, created_at);
                """
            )

    def enqueue(
        self,
        idempotency_key: str,
        payload: Mapping[str, Any],
        *,
        max_delivery_count: int = 5,
        available_at: float | None = None,
    ) -> str:
        key = self._required_text(idempotency_key, "idempotency_key")
        if max_delivery_count < 1 or max_delivery_count > 100:
            raise ValueError("max_delivery_count must be between 1 and 100")
        payload_json = json.dumps(
            dict(payload),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        import hashlib

        payload_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        now = time.time()
        message_id = str(uuid4())
        with self._transaction() as connection:
            existing = connection.execute(
                """
                SELECT message_id, payload_sha256, max_delivery_count
                FROM queue_messages WHERE idempotency_key = ?
                """,
                (key,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["payload_sha256"] != payload_sha256
                    or existing["max_delivery_count"] != max_delivery_count
                ):
                    raise QueueConflictError(
                        f"idempotency key {key!r} has different immutable content"
                    )
                return str(existing["message_id"])
            connection.execute(
                """
                INSERT INTO queue_messages (
                    message_id, idempotency_key, payload_json, payload_sha256,
                    state, delivery_count, max_delivery_count, available_at,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'ready', 0, ?, ?, ?, ?)
                """,
                (
                    message_id,
                    key,
                    payload_json,
                    payload_sha256,
                    max_delivery_count,
                    now if available_at is None else float(available_at),
                    now,
                    now,
                ),
            )
        return message_id

    def claim(
        self,
        limit: int,
        *,
        owner: str,
        lock_seconds: float,
        minimum_count: int = 1,
        now: float | None = None,
    ) -> list[ClaimedMessage]:
        if limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100")
        if minimum_count < 1 or minimum_count > limit:
            raise ValueError("minimum_count must be between 1 and limit")
        owner_id = self._required_text(owner, "owner")
        lease_seconds = self._bounded_seconds(lock_seconds, "lock_seconds")
        timestamp = time.time() if now is None else float(now)
        claims: list[ClaimedMessage] = []
        with self._transaction() as connection:
            self._recover_expired(connection, timestamp)
            rows = connection.execute(
                """
                SELECT * FROM queue_messages
                WHERE state = 'ready' AND available_at <= ?
                ORDER BY created_at, message_id
                LIMIT ?
                """,
                (timestamp, limit),
            ).fetchall()
            if len(rows) < minimum_count:
                return []
            for row in rows:
                token = str(uuid4())
                locked_until = timestamp + lease_seconds
                delivery_count = int(row["delivery_count"]) + 1
                connection.execute(
                    """
                    UPDATE queue_messages
                    SET state = 'locked', delivery_count = ?, lock_token = ?,
                        locked_by = ?, locked_until = ?, updated_at = ?
                    WHERE message_id = ? AND state = 'ready'
                    """,
                    (
                        delivery_count,
                        token,
                        owner_id,
                        locked_until,
                        timestamp,
                        row["message_id"],
                    ),
                )
                claims.append(
                    ClaimedMessage(
                        message_id=str(row["message_id"]),
                        idempotency_key=str(row["idempotency_key"]),
                        payload=json.loads(row["payload_json"]),
                        lock_token=token,
                        locked_until=locked_until,
                        delivery_count=delivery_count,
                        max_delivery_count=int(row["max_delivery_count"]),
                    )
                )
        return claims

    def renew(
        self,
        lock_token: str,
        *,
        lock_seconds: float,
        now: float | None = None,
    ) -> float:
        token = self._required_text(lock_token, "lock_token")
        lease_seconds = self._bounded_seconds(lock_seconds, "lock_seconds")
        timestamp = time.time() if now is None else float(now)
        locked_until = timestamp + lease_seconds
        with self._transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE queue_messages
                SET locked_until = ?, updated_at = ?
                WHERE lock_token = ? AND state = 'locked' AND locked_until > ?
                """,
                (locked_until, timestamp, token, timestamp),
            )
            if cursor.rowcount != 1:
                raise LeaseLostError(f"lock token is not live: {token}")
        return locked_until

    def renew_many(
        self,
        lock_tokens: Sequence[str],
        *,
        lock_seconds: float,
        now: float | None = None,
    ) -> float:
        if not lock_tokens:
            raise ValueError("lock_tokens must not be empty")
        tokens = [self._required_text(token, "lock_token") for token in lock_tokens]
        if len(tokens) != len(set(tokens)):
            raise ValueError("lock_tokens must be unique")
        lease_seconds = self._bounded_seconds(lock_seconds, "lock_seconds")
        timestamp = time.time() if now is None else float(now)
        locked_until = timestamp + lease_seconds
        with self._transaction() as connection:
            for token in tokens:
                cursor = connection.execute(
                    """
                    UPDATE queue_messages
                    SET locked_until = ?, updated_at = ?
                    WHERE lock_token = ? AND state = 'locked' AND locked_until > ?
                    """,
                    (locked_until, timestamp, token, timestamp),
                )
                if cursor.rowcount != 1:
                    raise LeaseLostError(f"lock token is not live: {token}")
        return locked_until

    def complete(self, lock_token: str, *, now: float | None = None) -> None:
        self.finalize([(lock_token, "complete", None)], now=now)

    def abandon(
        self,
        lock_token: str,
        *,
        delay_seconds: float = 0,
        reason: str | None = None,
        now: float | None = None,
    ) -> None:
        delay = self._nonnegative_seconds(delay_seconds, "delay_seconds")
        self.finalize(
            [(lock_token, "abandon", reason)],
            delay_seconds=delay,
            now=now,
        )

    def dead_letter(
        self,
        lock_token: str,
        *,
        reason: str,
        now: float | None = None,
    ) -> None:
        self.finalize([(lock_token, "dead", reason)], now=now)

    def finalize(
        self,
        outcomes: Sequence[tuple[str, FinalizeAction, str | None]],
        *,
        delay_seconds: float = 0,
        now: float | None = None,
    ) -> None:
        if not outcomes:
            return
        delay = self._nonnegative_seconds(delay_seconds, "delay_seconds")
        timestamp = time.time() if now is None else float(now)
        with self._transaction() as connection:
            for raw_token, action, reason in outcomes:
                token = self._required_text(raw_token, "lock_token")
                row = connection.execute(
                    """
                    SELECT delivery_count, max_delivery_count
                    FROM queue_messages
                    WHERE lock_token = ? AND state = 'locked'
                        AND locked_until > ?
                    """,
                    (token, timestamp),
                ).fetchone()
                if row is None:
                    raise LeaseLostError(f"lock token is not live: {token}")
                state, dead_reason, available_at = self._final_state(
                    action,
                    reason,
                    int(row["delivery_count"]),
                    int(row["max_delivery_count"]),
                    timestamp,
                    delay,
                )
                connection.execute(
                    """
                    UPDATE queue_messages
                    SET state = ?, available_at = ?, lock_token = NULL,
                        locked_by = NULL, locked_until = NULL,
                        dead_letter_reason = ?, updated_at = ?
                    WHERE lock_token = ?
                    """,
                    (state, available_at, dead_reason, timestamp, token),
                )

    def recover_expired(self, *, now: float | None = None) -> int:
        timestamp = time.time() if now is None else float(now)
        with self._transaction() as connection:
            return self._recover_expired(connection, timestamp)

    def get(self, message_id: str) -> QueueMessage:
        identifier = self._required_text(message_id, "message_id")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM queue_messages WHERE message_id = ?",
                (identifier,),
            ).fetchone()
        if row is None:
            raise MessageNotFoundError(identifier)
        return self._message(row)

    def list(self, *, state: QueueState | None = None) -> list[QueueMessage]:
        if state is not None and state not in {"ready", "locked", "completed", "dead"}:
            raise ValueError(f"invalid queue state: {state!r}")
        query = "SELECT * FROM queue_messages"
        parameters: tuple[object, ...] = ()
        if state is not None:
            query += " WHERE state = ?"
            parameters = (state,)
        query += " ORDER BY created_at, message_id"
        with self._connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [self._message(row) for row in rows]

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database,
            timeout=self._busy_timeout_ms / 1000,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _transaction(self):
        queue = self

        class Transaction:
            def __enter__(self) -> sqlite3.Connection:
                self.connection = queue._connect()
                self.connection.execute("BEGIN IMMEDIATE")
                return self.connection

            def __exit__(self, error_type, error, traceback) -> bool:
                try:
                    if error_type is None:
                        self.connection.commit()
                    else:
                        self.connection.rollback()
                finally:
                    self.connection.close()
                return False

        return Transaction()

    @staticmethod
    def _recover_expired(connection: sqlite3.Connection, now: float) -> int:
        cursor = connection.execute(
            """
            UPDATE queue_messages
            SET state = CASE
                    WHEN delivery_count >= max_delivery_count THEN 'dead'
                    ELSE 'ready'
                END,
                available_at = ?,
                lock_token = NULL,
                locked_by = NULL,
                locked_until = NULL,
                dead_letter_reason = CASE
                    WHEN delivery_count >= max_delivery_count
                    THEN 'maximum delivery count reached after lease expiry'
                    ELSE dead_letter_reason
                END,
                updated_at = ?
            WHERE state = 'locked' AND locked_until <= ?
            """,
            (now, now, now),
        )
        return cursor.rowcount

    @staticmethod
    def _final_state(
        action: FinalizeAction,
        reason: str | None,
        delivery_count: int,
        max_delivery_count: int,
        now: float,
        delay_seconds: float,
    ) -> tuple[QueueState, str | None, float]:
        if action == "complete":
            return "completed", None, now
        if action == "dead":
            if not isinstance(reason, str) or not reason.strip():
                raise ValueError("dead-letter reason must be non-empty")
            return "dead", reason.strip(), now
        if action != "abandon":
            raise ValueError(f"invalid finalize action: {action!r}")
        if delivery_count >= max_delivery_count:
            return "dead", reason or "maximum delivery count reached", now
        return "ready", reason, now + delay_seconds

    @staticmethod
    def _message(row: sqlite3.Row) -> QueueMessage:
        return QueueMessage(
            message_id=str(row["message_id"]),
            idempotency_key=str(row["idempotency_key"]),
            state=row["state"],
            delivery_count=int(row["delivery_count"]),
            max_delivery_count=int(row["max_delivery_count"]),
            available_at=float(row["available_at"]),
            locked_until=(
                float(row["locked_until"]) if row["locked_until"] is not None else None
            ),
            dead_letter_reason=row["dead_letter_reason"],
        )

    @staticmethod
    def _required_text(value: object, name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be a non-empty string")
        return value.strip()

    @staticmethod
    def _bounded_seconds(value: float, name: str) -> float:
        seconds = float(value)
        if not math.isfinite(seconds) or seconds < 1 or seconds > 86_400:
            raise ValueError(f"{name} must be finite and between 1 and 86400")
        return seconds

    @staticmethod
    def _nonnegative_seconds(value: float, name: str) -> float:
        seconds = float(value)
        if not math.isfinite(seconds) or seconds < 0 or seconds > 86_400:
            raise ValueError(f"{name} must be finite and between 0 and 86400")
        return seconds


class SerializedQueueActor:
    """Run every SQLite operation on one dedicated control thread."""

    def __init__(
        self,
        database: Path,
        *,
        busy_timeout_ms: int = 5_000,
        startup_timeout_seconds: float = 10,
        call_timeout_seconds: float = 30,
        close_timeout_seconds: float = 10,
    ) -> None:
        self._startup_timeout_seconds = self._positive_timeout(
            startup_timeout_seconds,
            "startup_timeout_seconds",
        )
        self._call_timeout_seconds = self._positive_timeout(
            call_timeout_seconds,
            "call_timeout_seconds",
        )
        self._close_timeout_seconds = self._positive_timeout(
            close_timeout_seconds,
            "close_timeout_seconds",
        )
        self._requests: Queue[
            tuple[Future[Any], Callable[[SQLiteQueue], Any]] | None
        ] = Queue()
        self._database = database
        self._busy_timeout_ms = busy_timeout_ms
        self._startup: Future[None] = Future()
        self._state_lock = threading.Lock()
        self._state: Literal["starting", "running", "closing", "stopped"] = (
            "starting"
        )
        self._fatal_error: BaseException | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="sqlite-control-actor",
            daemon=True,
        )
        self._thread.start()
        try:
            self._startup.result(timeout=self._startup_timeout_seconds)
        except FutureTimeoutError as error:
            self._request_stop()
            self._thread.join(timeout=self._close_timeout_seconds)
            raise TimeoutError("SQLite control actor startup timed out") from error
        except BaseException:
            self._thread.join(timeout=self._close_timeout_seconds)
            raise

    def call(self, operation: Callable[[SQLiteQueue], _T]) -> _T:
        future: Future[_T] = Future()
        with self._state_lock:
            if self._state != "running":
                self._raise_not_running()
            self._requests.put((future, operation))
        try:
            return future.result(timeout=self._call_timeout_seconds)
        except FutureTimeoutError as error:
            future.cancel()
            raise TimeoutError("SQLite control actor operation timed out") from error

    def close(self) -> None:
        self._request_stop()
        self._thread.join(timeout=self._close_timeout_seconds)
        if self._thread.is_alive():
            raise TimeoutError("SQLite control actor shutdown timed out")

    def __enter__(self) -> SerializedQueueActor:
        return self

    def __exit__(self, error_type, error, traceback) -> None:
        self.close()

    def _run(self) -> None:
        try:
            queue = SQLiteQueue(
                self._database,
                busy_timeout_ms=self._busy_timeout_ms,
            )
            with self._state_lock:
                if self._state == "starting":
                    self._state = "running"
            self._startup.set_result(None)
            while True:
                request = self._requests.get()
                if request is None:
                    break
                future, operation = request
                with self._state_lock:
                    running = self._state == "running"
                if not running:
                    future.set_exception(
                        ActorClosedError("SQLite control actor is closing")
                    )
                    continue
                if not future.set_running_or_notify_cancel():
                    continue
                try:
                    future.set_result(operation(queue))
                except Exception as error:
                    future.set_exception(error)
                except BaseException as error:
                    failure = ActorDiedError(
                        "SQLite control actor terminated unexpectedly"
                    )
                    failure.__cause__ = error
                    self._fatal_error = failure
                    future.set_exception(failure)
                    raise
        except BaseException as error:
            if not self._startup.done():
                self._startup.set_exception(error)
                failure: BaseException = error
            else:
                failure = self._fatal_error or ActorDiedError(
                    "SQLite control actor terminated unexpectedly"
                )
                if failure is not error and failure.__cause__ is None:
                    failure.__cause__ = error
            self._stop_with_error(failure)
        else:
            self._stop_with_error(
                ActorClosedError("SQLite control actor is closed")
            )

    @property
    def call_timeout_seconds(self) -> float:
        return self._call_timeout_seconds

    def _request_stop(self) -> None:
        with self._state_lock:
            if self._state == "stopped":
                return
            self._state = "closing"
            self._fail_pending(
                ActorClosedError("SQLite control actor is closing")
            )
            self._requests.put(None)

    def _stop_with_error(self, error: BaseException) -> None:
        with self._state_lock:
            self._fatal_error = error
            self._state = "stopped"
            self._fail_pending(error)

    def _fail_pending(self, error: BaseException) -> None:
        while True:
            try:
                request = self._requests.get_nowait()
            except Empty:
                return
            if request is None:
                continue
            future, _ = request
            if not future.done():
                future.set_exception(error)

    def _raise_not_running(self) -> None:
        if isinstance(self._fatal_error, ActorClosedError):
            raise ActorClosedError("SQLite control actor is not running")
        if self._fatal_error is not None:
            raise ActorDiedError("SQLite control actor is not running") from (
                self._fatal_error
            )
        raise ActorClosedError("SQLite control actor is not running")

    @staticmethod
    def _positive_timeout(value: float, name: str) -> float:
        timeout = float(value)
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError(f"{name} must be finite and positive")
        return timeout
