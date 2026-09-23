"""Lakehouse-only serialization for cooperating Fabric control-table writers.

Bootstrap must create the lock table with ``lock_name string, owner_id string,
acquired_at timestamp`` and exactly one row: ``('global', NULL, NULL)``.
All writers must use the same lock table. There is deliberately no lease expiry:
a stopped or failed writer must be confirmed unable to commit before an operator
clears its owner token. An exception never triggers an automatic unlock.
"""

from __future__ import annotations

import logging
import math
import random
import time
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar
from uuid import uuid4

if TYPE_CHECKING:
    from delta.tables import DeltaTable
    from pyspark.sql import Column, DataFrame, SparkSession


_LOG = logging.getLogger(__name__)
_ACTIVE_WRITER: ContextVar[bool] = ContextVar("fabric_control_active", default=False)
_T = TypeVar("_T")
_OCC_NAMES = (
    "ConcurrentAppendException",
    "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException",
    "ConcurrentTransactionException",
    "ConcurrentWriteException",
    "MetadataChangedException",
    "ProtocolChangedException",
)
_OCC_ERROR_CLASSES = frozenset(
    {
        "DELTA_CONCURRENT_APPEND",
        "DELTA_CONCURRENT_DELETE_READ",
        "DELTA_CONCURRENT_DELETE_DELETE",
        "DELTA_CONCURRENT_TRANSACTION",
        "DELTA_CONCURRENT_WRITE",
        "DELTA_METADATA_CHANGED",
        "DELTA_PROTOCOL_CHANGED",
    }
)
_OCC_JAVA_CLASSES = frozenset(
    f"{package}.{name}"
    for package in ("io.delta.exceptions", "org.apache.spark.sql.delta")
    for name in _OCC_NAMES
)


class ControlLockError(RuntimeError):
    """The pre-seeded lock is invalid or ownership cannot be verified."""


def _delta_table(spark_session: SparkSession, table_name: str) -> DeltaTable:
    from delta.tables import DeltaTable

    return DeltaTable.forName(spark_session, table_name)


def _spark_functions():
    from pyspark.sql import functions

    return functions


def _acquisition_error_types() -> tuple[type[Exception], ...]:
    from delta import exceptions
    from py4j.protocol import Py4JJavaError
    from pyspark.errors import PySparkException

    return tuple(getattr(exceptions, name) for name in _OCC_NAMES) + (
        Py4JJavaError,
        PySparkException,
    )


def _is_delta_occ(error: Exception) -> bool:
    """Recognize structured Delta OCC errors, never message substrings."""
    if (
        type(error).__module__ == "delta.exceptions"
        and type(error).__name__ in _OCC_NAMES
    ):
        return True
    get_error_class = getattr(error, "getErrorClass", None)
    if get_error_class is not None and get_error_class() in _OCC_ERROR_CLASSES:
        return True
    java_error = getattr(error, "java_exception", None)
    return (
        java_error is not None
        and java_error.getClass().getName() in _OCC_JAVA_CLASSES
    )


@dataclass(frozen=True)
class _LockState:
    owner_id: str | None
    acquired_at: object


class ControlWriter:
    """Serialize synchronous, eagerly completed operations using a Delta CAS.

    ``run`` returns the callback's result. The callback must finish all Spark
    actions before returning; returning a lazy DataFrame is not a protected
    write. Failed or ambiguous actions retain ownership for manual recovery.
    Nested ``run`` calls, including facade actions inside ``run``, are rejected.
    """

    def __init__(
        self,
        spark_session: SparkSession,
        lock_table: str,
        *,
        timeout_seconds: float = 600,
    ) -> None:
        if not lock_table.strip():
            raise ValueError("lock_table must be a nonempty table name")
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be finite and greater than zero")
        self._spark = spark_session
        self._lock_table = lock_table
        self._timeout_seconds = timeout_seconds
        self.tables = _SerializedTables(self)

    def _read_lock(self) -> _LockState:
        self._spark.catalog.refreshTable(self._lock_table)
        rows = (
            self._spark.table(self._lock_table)
            .select("lock_name", "owner_id", "acquired_at")
            .limit(2)
            .collect()
        )
        if len(rows) != 1 or rows[0].lock_name != "global":
            raise ControlLockError(
                f"Lock table {self._lock_table!r} must contain exactly one "
                "pre-seeded row with lock_name='global'; workers never seed it"
            )
        return _LockState(rows[0].owner_id, rows[0].acquired_at)

    def _claim(self, owner_id: str) -> None:
        functions = _spark_functions()
        _delta_table(self._spark, self._lock_table).update(
            condition=(functions.col("lock_name") == "global")
            & functions.col("owner_id").isNull(),
            set={
                "owner_id": functions.lit(owner_id),
                "acquired_at": functions.current_timestamp(),
            },
        )

    def _acquire(self, owner_id: str) -> None:
        deadline = time.monotonic() + self._timeout_seconds
        retryable_types = _acquisition_error_types()
        state = self._read_lock()
        while time.monotonic() < deadline:
            if state.owner_id is None:
                try:
                    self._claim(owner_id)
                except retryable_types as error:
                    if not _is_delta_occ(error):
                        raise
                state = self._read_lock()
            if state.owner_id == owner_id:
                return
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(random.uniform(0.05, 0.25), remaining))
            state = self._read_lock()
        raise TimeoutError(
            f"Timed out acquiring {self._lock_table!r}; "
            f"attempted owner_id={owner_id}, observed owner_id={state.owner_id!r}, "
            f"acquired_at={state.acquired_at!r}. No automatic takeover; confirm "
            "the owning writer cannot commit before manually clearing its token."
        )

    def _release(self, owner_id: str) -> None:
        state = self._read_lock()
        if state.owner_id != owner_id or state.acquired_at is None:
            raise ControlLockError(
                f"Cannot release {self._lock_table!r}: expected owner_id={owner_id}, "
                f"observed owner_id={state.owner_id!r}, "
                f"acquired_at={state.acquired_at!r}; manual investigation required"
            )
        functions = _spark_functions()
        _delta_table(self._spark, self._lock_table).update(
            condition=(functions.col("lock_name") == "global")
            & (functions.col("owner_id") == owner_id),
            set={
                "owner_id": functions.lit(None).cast("string"),
                "acquired_at": functions.lit(None).cast("timestamp"),
            },
        )
        state = self._read_lock()
        # A successor can acquire after our CAS commits but before this read.
        if state.owner_id == owner_id or (
            (state.owner_id is None) != (state.acquired_at is None)
        ):
            raise ControlLockError(
                f"Release of {self._lock_table!r} could not be verified: "
                f"owner_id={owner_id}, observed owner_id={state.owner_id!r}, "
                f"acquired_at={state.acquired_at!r}; manual investigation required"
            )

    def run(self, operation: Callable[[], _T]) -> _T:
        if _ACTIVE_WRITER.get():
            raise RuntimeError(
                "Nested ControlWriter.run is not allowed; use real DeltaTable "
                "actions inside a run callback, not writer.tables actions"
            )
        # This context guard rejects nesting; only the durable Delta CAS locks.
        owner_id = str(uuid4())
        context_token = _ACTIVE_WRITER.set(True)
        completed = False
        released = False
        try:
            self._acquire(owner_id)
            result = operation()
            completed = True
            self._release(owner_id)
            released = True
            return result
        finally:
            _ACTIVE_WRITER.reset(context_token)
            if not released:
                _LOG.error(
                    "Control writer did not finish cleanly: lock_table=%s "
                    "owner_id=%s callback_completed=%s. Ownership may be retained; "
                    "confirm this writer cannot commit before manual recovery.",
                    self._lock_table,
                    owner_id,
                    completed,
                )


@dataclass(frozen=True)
class _Step:
    method: str
    args: tuple[object, ...]
    kwargs: dict[str, object]


class _SerializedTables:
    def __init__(self, writer: ControlWriter) -> None:
        self._writer = writer

    def forName(
        self, spark_session: SparkSession, table_name: str
    ) -> _SerializedDeltaTable:
        return _SerializedDeltaTable(self._writer, spark_session, table_name)


class _SerializedDeltaTable:
    """Record a builder recipe, never an unlocked DeltaTable or merge builder."""

    def __init__(
        self,
        writer: ControlWriter,
        spark_session: SparkSession,
        table_name: str,
        steps: tuple[_Step, ...] = (),
    ) -> None:
        self._writer = writer
        self._spark = spark_session
        self._table_name = table_name
        self._steps = steps

    def _append(self, method: str, *args: object, **kwargs: object) -> _SerializedDeltaTable:
        return _SerializedDeltaTable(
            self._writer,
            self._spark,
            self._table_name,
            (*self._steps, _Step(method, args, kwargs)),
        )

    def _action(self, method: str, **kwargs: object) -> None:
        def perform() -> None:
            builder = _delta_table(self._spark, self._table_name)
            for step in self._steps:
                builder = getattr(builder, step.method)(*step.args, **step.kwargs)
            getattr(builder, method)(**kwargs)

        self._writer.run(perform)

    def alias(self, aliasName: str) -> _SerializedDeltaTable:
        return self._append("alias", aliasName)

    def merge(self, source: DataFrame, condition: Column | str) -> _SerializedDeltaTable:
        return self._append("merge", source, condition)

    def whenMatchedUpdate(
        self,
        condition: Column | str | None = None,
        set: dict[str, Column | str] | None = None,
    ) -> _SerializedDeltaTable:
        return self._append("whenMatchedUpdate", condition=condition, set=set)

    def whenMatchedUpdateAll(
        self, condition: Column | str | None = None
    ) -> _SerializedDeltaTable:
        return self._append("whenMatchedUpdateAll", condition=condition)

    def whenNotMatchedInsertAll(
        self, condition: Column | str | None = None
    ) -> _SerializedDeltaTable:
        return self._append("whenNotMatchedInsertAll", condition=condition)

    def whenNotMatchedInsert(
        self,
        condition: Column | str | None = None,
        values: dict[str, Column | str] | None = None,
    ) -> _SerializedDeltaTable:
        return self._append("whenNotMatchedInsert", condition=condition, values=values)

    def whenMatchedDelete(
        self, condition: Column | str | None = None
    ) -> _SerializedDeltaTable:
        return self._append("whenMatchedDelete", condition=condition)

    def whenNotMatchedBySourceUpdate(
        self,
        condition: Column | str | None = None,
        set: dict[str, Column | str] | None = None,
    ) -> _SerializedDeltaTable:
        return self._append("whenNotMatchedBySourceUpdate", condition=condition, set=set)

    def whenNotMatchedBySourceDelete(
        self, condition: Column | str | None = None
    ) -> _SerializedDeltaTable:
        return self._append("whenNotMatchedBySourceDelete", condition=condition)

    def execute(self) -> None:
        self._action("execute")

    def update(
        self,
        condition: Column | str | None = None,
        set: dict[str, Column | str] | None = None,
    ) -> None:
        self._action("update", condition=condition, set=set)

    def delete(self, condition: Column | str | None = None) -> None:
        self._action("delete", condition=condition)
