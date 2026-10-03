"""Local Candidate A gold builder with committed-pointer visibility.

The local implementation deliberately uses JSON tables and SQLite so planning,
aggregation, checkpointing, and refresh signalling can be tested without
loading Spark.  ``replace_delta_path_partition`` is the path-based Delta seam
for a Spark host; Fabric catalog integration remains an explicit unsupported
boundary.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import statistics
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


FACT_TABLES = (
    "gold_flow_minute",
    "gold_flow_hour",
    "gold_video",
    "gold_operations_hour",
)
DIMENSION_TABLES = (
    "gold_dim_date",
    "gold_dim_time",
    "gold_dim_camera",
    "gold_dim_location",
    "gold_dim_video",
    "gold_dim_model_config",
)
_FAILED_ATTEMPT_STATUSES = frozenset(
    {
        "FAILED",
        "RETRY_WAIT",
        "TERMINAL_FAILED",
        "DEAD_LETTERED",
        "LEASE_LOST",
        "EXPIRED",
    }
)
_DEFERRED_ATTEMPT_STATUSES = frozenset({"RELEASED"})


class GoldError(RuntimeError):
    """Base error for local gold processing."""


class GoldSourceError(GoldError):
    """The committed source pointer or immutable output is invalid."""


class GoldValidationError(GoldError):
    """Gold facts and dimensions fail a required integrity rule."""


class UnsupportedGoldBackendError(GoldError):
    """The requested gold backend is intentionally unavailable."""


@dataclass(frozen=True)
class SourceCheckpoint:
    publication_sequence: int
    versions: dict[str, str]

    @property
    def key(self) -> str:
        return _sha256_json(
            {
                "publication_sequence": self.publication_sequence,
                "versions": self.versions,
            }
        )


@dataclass(frozen=True)
class GoldPlan:
    planned_at: str
    cutoff: str
    lookback_hours: int
    items: tuple[str, ...]
    source: SourceCheckpoint
    flow_source: SourceCheckpoint | None = None
    operations_source: SourceCheckpoint | None = None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["partition_count"] = len(self.items)
        result["items"] = [{"partition_date": item} for item in self.items]
        return result


_SPARK_FIELDS: dict[str, tuple[tuple[str, str, bool], ...]] = {
    "gold_flow_minute": (
        ("minute_utc", "timestamp", False),
        ("time_key", "integer", False),
        ("camera_id", "string", False),
        ("location_id", "string", False),
        ("entries", "long", False),
        ("exits", "long", False),
        ("net_flow", "long", False),
        ("source_videos", "long", False),
        ("refreshed_at", "timestamp", False),
        ("flow_date", "date", False),
    ),
    "gold_flow_hour": (
        ("hour_utc", "timestamp", False),
        ("time_key", "integer", False),
        ("camera_id", "string", False),
        ("location_id", "string", False),
        ("entries", "long", False),
        ("exits", "long", False),
        ("net_flow", "long", False),
        ("source_videos", "long", False),
        ("refreshed_at", "timestamp", False),
        ("flow_date", "date", False),
    ),
    "gold_video": (
        ("work_id", "string", False),
        ("captured_at_utc", "timestamp", False),
        ("time_key", "integer", False),
        ("camera_id", "string", False),
        ("location_id", "string", False),
        ("config_sha256", "string", False),
        ("video_duration_seconds", "double", True),
        ("processing_seconds", "double", True),
        ("speed_x_realtime", "double", True),
        ("distinct_people", "long", True),
        ("line_in_count", "long", True),
        ("line_out_count", "long", True),
        ("completed_at", "timestamp", False),
        ("capture_date", "date", False),
    ),
    "gold_operations_hour": (
        ("hour_utc", "timestamp", False),
        ("time_key", "integer", False),
        ("queued", "long", False),
        ("started", "long", False),
        ("succeeded", "long", False),
        ("failed", "long", False),
        ("deferred", "long", False),
        ("video_hours_completed", "double", False),
        ("average_processing_seconds", "double", True),
        ("p95_processing_seconds", "double", True),
        ("refreshed_at", "timestamp", False),
        ("operation_date", "date", False),
    ),
    "gold_dim_date": (
        ("date_key", "date", False),
        ("calendar_year", "integer", False),
        ("calendar_quarter", "integer", False),
        ("calendar_month", "integer", False),
        ("month_name", "string", False),
        ("month_short_name", "string", False),
        ("year_month", "string", False),
        ("day_of_month", "integer", False),
        ("iso_day_of_week", "integer", False),
        ("iso_week_year", "integer", False),
        ("iso_week_of_year", "integer", False),
        ("iso_year_week", "string", False),
        ("day_name", "string", False),
        ("is_weekend", "boolean", False),
        ("refreshed_at", "timestamp", False),
    ),
    "gold_dim_time": (
        ("time_key", "integer", False),
        ("hour_24", "integer", False),
        ("minute_of_hour", "integer", False),
        ("time_label", "string", False),
        ("hour_label", "string", False),
        ("day_part", "string", False),
        ("refreshed_at", "timestamp", False),
    ),
    "gold_dim_camera": (
        ("camera_id", "string", False),
        ("location_id", "string", False),
        ("camera_timezone", "string", False),
        ("first_capture_utc", "timestamp", False),
        ("last_capture_utc", "timestamp", False),
        ("video_count", "long", False),
        ("refreshed_at", "timestamp", False),
    ),
    "gold_dim_location": (
        ("location_id", "string", False),
        ("first_capture_utc", "timestamp", False),
        ("last_capture_utc", "timestamp", False),
        ("camera_count", "long", False),
        ("video_count", "long", False),
        ("refreshed_at", "timestamp", False),
    ),
    "gold_dim_video": (
        ("work_id", "string", False),
        ("asset_id", "string", False),
        ("asset_version", "string", False),
        ("camera_id", "string", False),
        ("location_id", "string", False),
        ("captured_at_utc", "timestamp", False),
        ("capture_date", "date", False),
        ("time_key", "integer", False),
        ("camera_timezone", "string", False),
        ("config_sha256", "string", False),
        ("refreshed_at", "timestamp", False),
    ),
    "gold_dim_model_config": (
        ("config_sha256", "string", False),
        ("config_json", "string", False),
        ("pipeline", "string", True),
        ("device_variant", "string", True),
        ("device", "string", True),
        ("batch_size", "integer", True),
        ("sample_fps", "double", True),
        ("detection_threshold", "double", True),
        ("use_fp16", "boolean", True),
        ("detector_model", "string", True),
        ("camera_motion_compensation", "boolean", True),
        ("counting_line_json", "string", True),
        ("first_capture_utc", "timestamp", False),
        ("last_capture_utc", "timestamp", False),
        ("video_count", "long", False),
        ("refreshed_at", "timestamp", False),
    ),
}


@dataclass(frozen=True)
class CommittedOutput:
    work_id: str
    attempt_id: str
    publication_sequence: int
    published_at: datetime
    work: dict[str, Any]
    attempt: dict[str, Any]
    run: dict[str, Any]
    line_counts: tuple[dict[str, Any], ...]


class FabricGoldStore:
    """Fail-closed placeholder for a reviewed Fabric catalog implementation."""

    def __init__(self, *_: object, **__: object) -> None:
        raise UnsupportedGoldBackendError(
            "Fabric gold writes are unsupported here; use the reviewed Fabric "
            "Lakehouse implementation or the path-Delta helper"
        )


class LocalJsonGoldStore:
    """Small atomic JSON-table adapter used by the local SJD."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def read_table(self, name: str) -> list[dict[str, Any]]:
        payload = self._read_payload(name)
        rows = payload.get("rows", [])
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise GoldValidationError(f"{name} is not a JSON row table")
        return [dict(row) for row in rows]

    def version(self, name: str) -> int:
        return int(self._read_payload(name).get("version", 0))

    def versions(self, names: Iterable[str]) -> dict[str, int]:
        return {name: self.version(name) for name in names}

    def replace_partition(
        self,
        name: str,
        partition_column: str,
        partition_value: str,
        rows: Sequence[Mapping[str, Any]],
    ) -> int:
        normalized = [_jsonable_row(row) for row in rows]
        invalid = [
            row.get(partition_column)
            for row in normalized
            if row.get(partition_column) != partition_value
        ]
        if invalid:
            raise ValueError(
                f"{name} replacement contains rows outside "
                f"{partition_column}={partition_value}"
            )
        retained = [
            row
            for row in self.read_table(name)
            if row.get(partition_column) != partition_value
        ]
        self._write(name, retained + normalized)
        return len(normalized)

    def replace_table(
        self, name: str, rows: Sequence[Mapping[str, Any]]
    ) -> int:
        normalized = [_jsonable_row(row) for row in rows]
        self._write(name, normalized)
        return len(normalized)

    def _path(self, name: str) -> Path:
        if not name or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789_" for character in name):
            raise ValueError(f"invalid local table name {name!r}")
        return self.root / f"{name}.json"

    def _read_payload(self, name: str) -> dict[str, Any]:
        path = self._path(name)
        if not path.exists():
            return {"version": 0, "rows": []}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise GoldValidationError(f"cannot read local table {path}") from error
        if not isinstance(payload, dict):
            raise GoldValidationError(f"{path} must contain a JSON object")
        return payload

    def _write(self, name: str, rows: Sequence[Mapping[str, Any]]) -> None:
        path = self._path(name)
        version = self.version(name) + 1
        pending = path.with_name(f".{path.name}.{uuid4().hex}.pending")
        payload = {"version": version, "rows": list(rows)}
        try:
            with pending.open("x", encoding="utf-8") as handle:
                json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(pending, path)
        finally:
            pending.unlink(missing_ok=True)


class PathDeltaGoldStore:
    """Path-only Delta gold adapter with explicit schemas and no metastore."""

    def __init__(self, root: Path, spark_session: Any) -> None:
        self.root = Path(root).expanduser().resolve()
        self.spark = spark_session

    def read_table(self, name: str) -> list[dict[str, Any]]:
        path = self._path(name)
        if not (path / "_delta_log").is_dir():
            return []
        return [
            _jsonable_spark_row(row.asDict(recursive=True))
            for row in self.spark.read.format("delta")
            .load(str(path))
            .collect()
        ]

    def version(self, name: str) -> int:
        path = self._path(name)
        if not (path / "_delta_log").is_dir():
            return 0
        from delta.tables import DeltaTable

        row = DeltaTable.forPath(self.spark, str(path)).history(1).select(
            "version"
        ).first()
        return int(row["version"]) + 1

    def versions(self, names: Iterable[str]) -> dict[str, int]:
        return {name: self.version(name) for name in names}

    def replace_partition(
        self,
        name: str,
        partition_column: str,
        partition_value: str,
        rows: Sequence[Mapping[str, Any]],
    ) -> int:
        normalized = [_jsonable_row(row) for row in rows]
        if any(
            row.get(partition_column) != partition_value for row in normalized
        ):
            raise ValueError(
                f"{name} replacement contains rows outside "
                f"{partition_column}={partition_value}"
            )
        path = self._path(name)
        predicate = (
            f"{partition_column} = DATE '{_date_string(partition_value)}'"
        )
        if normalized:
            frame = self.spark.createDataFrame(
                [_spark_row(name, row) for row in normalized],
                schema=_spark_schema(name),
            )
            writer = (
                frame.write.format("delta")
                .mode("overwrite")
                .option("replaceWhere", predicate)
            )
            if not (path / "_delta_log").is_dir():
                writer = writer.partitionBy(partition_column)
            writer.save(str(path))
        elif (path / "_delta_log").is_dir():
            from delta.tables import DeltaTable

            DeltaTable.forPath(self.spark, str(path)).delete(predicate)
        return len(normalized)

    def replace_table(
        self, name: str, rows: Sequence[Mapping[str, Any]]
    ) -> int:
        normalized = [_jsonable_row(row) for row in rows]
        self._replace(name, normalized)
        return len(normalized)

    def _replace(self, name: str, rows: Sequence[Mapping[str, Any]]) -> None:
        path = self._path(name)
        if not rows:
            if (path / "_delta_log").is_dir():
                from delta.tables import DeltaTable

                DeltaTable.forPath(self.spark, str(path)).delete()
            return
        frame = self.spark.createDataFrame(
            [_spark_row(name, row) for row in rows],
            schema=_spark_schema(name),
        )
        (
            frame.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(str(path))
        )

    def _path(self, name: str) -> Path:
        if not name or any(
            character not in "abcdefghijklmnopqrstuvwxyz0123456789_"
            for character in name
        ):
            raise ValueError(f"invalid local table name {name!r}")
        return self.root / name


class SQLiteGoldState:
    """Additive gold checkpoint and semantic-refresh state in the control DB."""

    def __init__(
        self,
        database: Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.database = Path(database).expanduser().resolve()
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._bootstrap()

    def checkpoint(self, stage: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM gold_checkpoints WHERE stage = ?", (stage,)
            ).fetchone()
        return dict(row) if row is not None else None

    def save_checkpoint(
        self,
        stage: str,
        source: SourceCheckpoint,
        target_versions: Mapping[str, int],
    ) -> None:
        with self._connect() as connection:
            self._save_checkpoint(
                connection, stage, source, target_versions, _iso(self._clock())
            )

    def enqueue_refresh(
        self,
        reason: str,
        source: SourceCheckpoint,
        target_versions: Mapping[str, int],
    ) -> int:
        payload = {
            "reason": reason,
            "source_key": source.key,
            "publication_sequence": source.publication_sequence,
            "target_versions": dict(target_versions),
        }
        dedupe_key = _sha256_json(payload)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO semantic_refresh_outbox (
                    dedupe_key, reason, payload_json, created_at
                ) VALUES (?, ?, ?, ?)
                """,
                (dedupe_key, reason, _canonical_json(payload), _iso(self._clock())),
            )
            row = connection.execute(
                "SELECT outbox_id FROM semantic_refresh_outbox WHERE dedupe_key = ?",
                (dedupe_key,),
            ).fetchone()
            assert row is not None
            return int(row[0])

    def save_checkpoints_and_enqueue(
        self,
        checkpoints: Mapping[
            str, tuple[SourceCheckpoint, Mapping[str, int]]
        ],
        reason: str | None,
        refresh_source: SourceCheckpoint,
        refresh_target_versions: Mapping[str, int],
    ) -> int | None:
        """Commit checkpoint advancement and its outbox signal together."""
        now = _iso(self._clock())
        with self._connect() as connection:
            for stage, (source, target_versions) in checkpoints.items():
                self._save_checkpoint(
                    connection, stage, source, target_versions, now
                )
            if reason is None:
                return None
            payload = {
                "reason": reason,
                "source_key": refresh_source.key,
                "publication_sequence": refresh_source.publication_sequence,
                "target_versions": dict(refresh_target_versions),
            }
            dedupe_key = _sha256_json(payload)
            connection.execute(
                """
                INSERT OR IGNORE INTO semantic_refresh_outbox (
                    dedupe_key, reason, payload_json, created_at
                ) VALUES (?, ?, ?, ?)
                """,
                (dedupe_key, reason, _canonical_json(payload), now),
            )
            row = connection.execute(
                """
                SELECT outbox_id FROM semantic_refresh_outbox
                WHERE dedupe_key = ?
                """,
                (dedupe_key,),
            ).fetchone()
            assert row is not None
            return int(row[0])

    def pending_refreshes(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM semantic_refresh_outbox
                WHERE acked_at IS NULL ORDER BY outbox_id
                """
            ).fetchall()
        return [dict(row) | {"payload": json.loads(row["payload_json"])} for row in rows]

    def acknowledge_refresh(
        self,
        outbox_id: int,
        actor: str = "local",
        *,
        expected_dedupe_key: str | None = None,
    ) -> bool:
        if not actor.strip():
            raise ValueError("actor is required")
        with self._connect() as connection:
            updated = connection.execute(
                """
                UPDATE semantic_refresh_outbox
                SET acked_at = ?, acked_by = ?
                WHERE outbox_id = ? AND acked_at IS NULL
                  AND (? IS NULL OR dedupe_key = ?)
                """,
                (
                    _iso(self._clock()),
                    actor.strip(),
                    outbox_id,
                    expected_dedupe_key,
                    expected_dedupe_key,
                ),
            )
        return updated.rowcount == 1

    @staticmethod
    def _save_checkpoint(
        connection: sqlite3.Connection,
        stage: str,
        source: SourceCheckpoint,
        target_versions: Mapping[str, int],
        completed_at: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO gold_checkpoints (
                stage, source_key, publication_sequence,
                source_versions_json, target_versions_json, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(stage) DO UPDATE SET
                source_key = excluded.source_key,
                publication_sequence = excluded.publication_sequence,
                source_versions_json = excluded.source_versions_json,
                target_versions_json = excluded.target_versions_json,
                completed_at = excluded.completed_at
            """,
            (
                stage,
                source.key,
                source.publication_sequence,
                _canonical_json(source.versions),
                _canonical_json(dict(target_versions)),
                completed_at,
            ),
        )

    def _bootstrap(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS gold_checkpoints (
                    stage TEXT PRIMARY KEY,
                    source_key TEXT NOT NULL,
                    publication_sequence INTEGER NOT NULL,
                    source_versions_json TEXT NOT NULL,
                    target_versions_json TEXT NOT NULL,
                    completed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS semantic_refresh_outbox (
                    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dedupe_key TEXT NOT NULL UNIQUE,
                    reason TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    acked_at TEXT,
                    acked_by TEXT
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database)
        connection.row_factory = sqlite3.Row
        return connection


class LocalGoldJob:
    """Plan, build, and validate local Candidate A gold tables."""

    def __init__(
        self,
        control_database: Path,
        gold_root: Path,
        *,
        clock: Callable[[], datetime] | None = None,
        verify_output_hash: bool = True,
        store: LocalJsonGoldStore | PathDeltaGoldStore | None = None,
    ) -> None:
        self.database = Path(control_database).expanduser().resolve()
        if not self.database.exists():
            raise FileNotFoundError(self.database)
        self.store = store or LocalJsonGoldStore(gold_root)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.state = SQLiteGoldState(self.database, clock=self._clock)
        self.verify_output_hash = verify_output_hash

    def source_checkpoint(self) -> SourceCheckpoint:
        """Return the committed-pointer source domain for flow and video."""
        versions: dict[str, str] = {}
        publication_sequence = 0
        with self._connect() as connection:
            required = {"work", "attempts", "publications"}
            if all(_table_exists(connection, table) for table in required):
                visible = {
                    "work": """
                        SELECT w.work_id, w.payload_json, w.payload_sha256,
                               w.duration_seconds, w.config_sha256,
                               w.release_digest, w.committed_attempt_id,
                               w.publication_sequence
                        FROM work w JOIN publications p
                          ON p.work_id = w.work_id
                         AND p.attempt_id = w.committed_attempt_id
                        ORDER BY w.work_id
                    """,
                    "attempts": """
                        SELECT a.attempt_id, a.work_id, a.output_path,
                               a.output_sha256, a.terminal_succeeded
                        FROM attempts a
                        JOIN work w ON w.work_id = a.work_id
                        JOIN publications p
                          ON p.work_id = w.work_id
                         AND p.attempt_id = w.committed_attempt_id
                         AND p.attempt_id = a.attempt_id
                        ORDER BY a.attempt_id
                    """,
                    "publications": """
                        SELECT p.* FROM publications p
                        JOIN work w
                          ON w.work_id = p.work_id
                         AND w.committed_attempt_id = p.attempt_id
                        ORDER BY p.publication_sequence
                    """,
                }
                for table, query in visible.items():
                    versions[table] = _sha256_json(
                        [dict(row) for row in connection.execute(query).fetchall()]
                    )
                row = connection.execute(
                    """
                    SELECT COALESCE(MAX(p.publication_sequence), 0)
                    FROM publications p JOIN work w
                      ON w.work_id = p.work_id
                     AND w.committed_attempt_id = p.attempt_id
                    """
                ).fetchone()
                publication_sequence = int(row[0])
            else:
                versions = {table: _sha256_json([]) for table in required}
        return SourceCheckpoint(publication_sequence, versions)

    def operations_source_checkpoint(self) -> SourceCheckpoint:
        """Return the mutable control domain used by operations facts."""
        tables = ("work", "attempts", "batches", "publications")
        versions: dict[str, str] = {}
        publication_sequence = 0
        with self._connect() as connection:
            for table in tables:
                if not _table_exists(connection, table):
                    versions[table] = _sha256_json([])
                    continue
                rows = connection.execute(
                    f"SELECT * FROM {_quoted_identifier(table)} "
                    f"ORDER BY rowid"
                ).fetchall()
                versions[table] = _sha256_json([dict(row) for row in rows])
            if _table_exists(connection, "publications"):
                row = connection.execute(
                    "SELECT COALESCE(MAX(publication_sequence), 0) "
                    "FROM publications"
                ).fetchone()
                publication_sequence = int(row[0])
        return SourceCheckpoint(publication_sequence, versions)

    @staticmethod
    def _fact_source(
        flow: SourceCheckpoint, operations: SourceCheckpoint
    ) -> SourceCheckpoint:
        return SourceCheckpoint(
            flow.publication_sequence,
            {
                **{f"flow:{key}": value for key, value in flow.versions.items()},
                **{
                    f"operations:{key}": value
                    for key, value in operations.versions.items()
                },
            },
        )

    def committed_outputs(self) -> list[CommittedOutput]:
        with self._connect() as connection:
            required = {"work", "attempts", "publications"}
            missing = sorted(
                table for table in required if not _table_exists(connection, table)
            )
            if missing:
                raise GoldSourceError(
                    "control database lacks required tables: " + ", ".join(missing)
                )
            work_columns = _columns(connection, "work")
            if "committed_attempt_id" not in work_columns:
                raise GoldSourceError("work.committed_attempt_id is required")
            work_rows = connection.execute(
                """
                SELECT * FROM work
                WHERE committed_attempt_id IS NOT NULL
                ORDER BY work_id
                """
            ).fetchall()
            committed: list[CommittedOutput] = []
            for work_row in work_rows:
                work = dict(work_row)
                attempt_id = str(work["committed_attempt_id"])
                publication = connection.execute(
                    """
                    SELECT * FROM publications
                    WHERE work_id = ? AND attempt_id = ?
                    """,
                    (work["work_id"], attempt_id),
                ).fetchone()
                attempt = connection.execute(
                    "SELECT * FROM attempts WHERE attempt_id = ? AND work_id = ?",
                    (attempt_id, work["work_id"]),
                ).fetchone()
                if publication is None or attempt is None:
                    raise GoldSourceError(
                        f"committed pointer for {work['work_id']} has no matching "
                        "publication and attempt"
                    )
                publication_data = dict(publication)
                attempt_data = dict(attempt)
                _validate_pointer_metadata(work, attempt_data, publication_data)
                document = _load_output_document(
                    Path(str(publication_data["output_path"])),
                    str(publication_data.get("output_sha256") or ""),
                    verify_hash=self.verify_output_hash,
                    work_id=str(work["work_id"]),
                    attempt_id=attempt_id,
                )
                run, line_counts = _split_output_document(
                    document, str(work["work_id"]), attempt_id
                )
                published_at = _first_datetime(
                    publication_data,
                    ("published_at", "created_at"),
                    required=True,
                )
                committed.append(
                    CommittedOutput(
                        str(work["work_id"]),
                        attempt_id,
                        int(publication_data["publication_sequence"]),
                        published_at,
                        _merge_payload(work),
                        attempt_data,
                        run,
                        tuple(line_counts),
                    )
                )
        return committed

    def plan(
        self,
        *,
        lookback_hours: int = 48,
        now: datetime | None = None,
        full_rebuild: bool = False,
        reset: bool = False,
    ) -> GoldPlan:
        if type(lookback_hours) is not int or lookback_hours < 1:
            raise ValueError("lookback_hours must be at least 1")
        planned_at = _utc(now or self._clock())
        cutoff = planned_at - timedelta(hours=lookback_hours)
        flow_source = self.source_checkpoint()
        operations_source = self.operations_source_checkpoint()
        source = self._fact_source(flow_source, operations_source)
        previous_flow = self.state.checkpoint("facts:flow")
        previous_operations = self.state.checkpoint("facts:operations")
        all_history = (
            full_rebuild
            or reset
            or previous_flow is None
            or previous_operations is None
        )
        outputs = self.committed_outputs()
        control = self._control_rows()
        dates: set[str] = set()
        if all_history:
            for output in outputs:
                dates.update(_output_dates(output))
            dates.update(_control_dates(control))
        else:
            prior_sequence = int(previous_flow["publication_sequence"])
            flow_dates: set[str] = set()
            for output in outputs:
                if (
                    output.publication_sequence > prior_sequence
                    or output.published_at >= cutoff
                ):
                    flow_dates.update(_output_dates(output))
            prior_completed = _required_datetime(
                previous_operations["completed_at"]
            )
            control_cutoff = min(
                cutoff,
                prior_completed - timedelta(hours=lookback_hours),
            )
            operation_dates = _control_dates(
                control, cutoff=control_cutoff
            )
            if (
                previous_flow["source_key"] != flow_source.key
                and not any(
                    output.publication_sequence > prior_sequence
                    for output in outputs
                )
            ):
                for output in outputs:
                    flow_dates.update(_output_dates(output))
            if (
                previous_operations["source_key"] != operations_source.key
                and not operation_dates
            ):
                operation_dates.update(_control_dates(control))
            dates.update(flow_dates)
            dates.update(operation_dates)
        return GoldPlan(
            planned_at=_iso(planned_at),
            cutoff=_iso(cutoff),
            lookback_hours=lookback_hours,
            items=tuple(sorted(dates)),
            source=source,
            flow_source=flow_source,
            operations_source=operations_source,
        )

    def build_facts(
        self,
        *,
        dates: Sequence[str] | None = None,
        lookback_hours: int = 48,
        force: bool = False,
        full_rebuild: bool = False,
    ) -> dict[str, Any]:
        flow_source = self.source_checkpoint()
        operations_source = self.operations_source_checkpoint()
        source = self._fact_source(flow_source, operations_source)
        previous = self.state.checkpoint("facts")
        automatic = dates is None
        current_versions = self.store.versions(FACT_TABLES)
        targets_current = _checkpoint_targets_match(
            previous, current_versions
        )
        if (
            automatic
            and not force
            and not full_rebuild
            and previous
            and previous["source_key"] == source.key
            and targets_current
        ):
            return {
                "stage": "facts",
                "skipped": True,
                "source_key": source.key,
                "partition_count": 0,
            }
        selected = (
            self.plan(
                lookback_hours=lookback_hours,
                full_rebuild=full_rebuild or not targets_current,
                reset=force,
            ).items
            if dates is None
            else tuple(sorted({_date_string(value) for value in dates}))
        )
        committed = self.committed_outputs()
        control = self._control_rows()
        now = _iso(self._clock())
        totals = {name: 0 for name in FACT_TABLES}
        for partition_date in selected:
            rows = _fact_rows_for_date(
                committed,
                control,
                partition_date,
                now,
            )
            for table, partition_column in (
                ("gold_flow_minute", "flow_date"),
                ("gold_flow_hour", "flow_date"),
                ("gold_video", "capture_date"),
                ("gold_operations_hour", "operation_date"),
            ):
                totals[table] += self.store.replace_partition(
                    table,
                    partition_column,
                    partition_date,
                    rows[table],
                )
        target_versions = self.store.versions(FACT_TABLES)
        checkpoints = {
            f"facts:partition:{partition_date}": (source, target_versions)
            for partition_date in selected
        }
        complete_current = False
        if automatic:
            latest_flow = self.source_checkpoint()
            latest_operations = self.operations_source_checkpoint()
            complete_current = (
                latest_flow.key == flow_source.key
                and latest_operations.key == operations_source.key
            )
            if complete_current:
                checkpoints |= {
                    "facts": (source, target_versions),
                    "facts:flow": (flow_source, target_versions),
                    "facts:operations": (
                        operations_source,
                        target_versions,
                    ),
                }
        outbox_id = self.state.save_checkpoints_and_enqueue(
            checkpoints,
            "gold facts changed" if selected else None,
            source,
            target_versions,
        )
        return {
            "stage": "facts",
            "skipped": False,
            "source_key": source.key,
            "partition_count": len(selected),
            "dates": list(selected),
            "rows": totals,
            "target_versions": target_versions,
            "outbox_id": outbox_id,
            "complete_checkpoint_advanced": complete_current,
        }

    def build_dimensions(
        self,
        *,
        full_rebuild: bool = False,
        force: bool = False,
    ) -> dict[str, Any]:
        source = self.source_checkpoint()
        fact_versions = self.store.versions(FACT_TABLES)
        dimension_source = SourceCheckpoint(
            source.publication_sequence,
            source.versions
            | {f"target:{name}": str(version) for name, version in fact_versions.items()},
        )
        previous = self.state.checkpoint("dimensions")
        current_dimension_versions = self.store.versions(DIMENSION_TABLES)
        if (
            not full_rebuild
            and not force
            and previous
            and previous["source_key"] == dimension_source.key
            and _checkpoint_targets_match(
                previous, current_dimension_versions
            )
        ):
            return {
                "stage": "dimensions",
                "skipped": True,
                "source_key": dimension_source.key,
            }
        rows = _dimension_rows(
            self.committed_outputs(),
            {name: self.store.read_table(name) for name in FACT_TABLES},
            _iso(self._clock()),
        )
        counts = {
            table: self.store.replace_table(table, rows[table])
            for table in DIMENSION_TABLES
        }
        target_versions = self.store.versions(DIMENSION_TABLES)
        outbox_id = self.state.save_checkpoints_and_enqueue(
            {"dimensions": (dimension_source, target_versions)},
            "gold dimensions changed",
            dimension_source,
            target_versions,
        )
        return {
            "stage": "dimensions",
            "skipped": False,
            "source_key": dimension_source.key,
            "rows": counts,
            "target_versions": target_versions,
            "outbox_id": outbox_id,
        }

    def validate(self) -> dict[str, Any]:
        tables = {
            name: self.store.read_table(name)
            for name in FACT_TABLES + DIMENSION_TABLES
        }
        rules = _validate_tables(tables)
        return {
            "valid": True,
            "rules_checked": rules,
            "table_rows": {name: len(rows) for name, rows in tables.items()},
        }

    def run(
        self,
        *,
        lookback_hours: int = 48,
        force: bool = False,
        full_rebuild: bool = False,
    ) -> dict[str, Any]:
        facts = self.build_facts(
            lookback_hours=lookback_hours,
            force=force,
            full_rebuild=full_rebuild,
        )
        dimensions = self.build_dimensions(
            full_rebuild=full_rebuild, force=force
        )
        validation = self.validate()
        return {
            "facts": facts,
            "dimensions": dimensions,
            "validation": validation,
            "pending_refreshes": len(self.state.pending_refreshes()),
        }

    def _control_rows(self) -> dict[str, list[dict[str, Any]]]:
        with self._connect() as connection:
            return {
                "work": [
                    dict(row)
                    for row in connection.execute(
                        "SELECT * FROM work ORDER BY work_id"
                    ).fetchall()
                ],
                "attempts": [
                    dict(row)
                    for row in connection.execute(
                        """
                        SELECT a.*,
                               b.sealed_at AS batch_sealed_at,
                               b.committed_at AS batch_completed_at,
                               b.lease_expires_at AS batch_expired_at,
                               w.updated_at AS work_updated_at,
                               p.published_at AS published_at
                        FROM attempts a
                        JOIN batches b ON b.batch_id = a.batch_id
                        JOIN work w ON w.work_id = a.work_id
                        LEFT JOIN publications p
                          ON p.attempt_id = a.attempt_id
                        ORDER BY a.attempt_id
                        """
                    ).fetchall()
                ],
            }

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database)
        connection.row_factory = sqlite3.Row
        return connection


def replace_delta_path_partition(
    spark: Any,
    path: str | Path,
    source: Any,
    partition_column: str,
    partition_value: str,
) -> int:
    """Replace one path-Delta partition, including deletion for an empty source."""
    try:
        from delta.tables import DeltaTable
    except ImportError as error:
        raise UnsupportedGoldBackendError(
            "delta-spark is required for path-Delta writes"
        ) from error
    output_path = str(Path(path))
    predicate = f"{partition_column} = DATE '{_date_string(partition_value)}'"
    row_count = int(source.count())
    if row_count:
        (
            source.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", predicate)
            .save(output_path)
        )
    elif DeltaTable.isDeltaTable(spark, output_path):
        DeltaTable.forPath(spark, output_path).delete(predicate)
    return row_count


def _fact_rows_for_date(
    committed: Sequence[CommittedOutput],
    control: Mapping[str, Sequence[Mapping[str, Any]]],
    partition_date: str,
    refreshed_at: str,
) -> dict[str, list[dict[str, Any]]]:
    minute_groups: dict[tuple[str, int, str, str], dict[str, Any]] = {}
    hour_groups: dict[tuple[str, int, str, str], dict[str, Any]] = {}
    videos: list[dict[str, Any]] = []
    for output in committed:
        metadata = output.work | output.run
        captured = _required_datetime(metadata.get("captured_at_utc"))
        camera = str(_required_value(metadata, "camera_id"))
        location = str(_required_value(metadata, "location_id"))
        for line in output.line_counts:
            observed = _line_observed_at(line, captured)
            if observed.date().isoformat() != partition_date:
                continue
            entries = _optional_int(
                line.get("frame_in_count", line.get("entries", 0))
            )
            exits = _optional_int(
                line.get("frame_out_count", line.get("exits", 0))
            )
            entries = 0 if entries is None else entries
            exits = 0 if exits is None else exits
            line_camera = str(line.get("camera_id") or camera)
            line_location = str(line.get("location_id") or location)
            minute = observed.replace(second=0, microsecond=0)
            hour = minute.replace(minute=0)
            _add_flow_group(
                minute_groups,
                (_iso(minute), minute.hour * 60 + minute.minute, line_camera, line_location),
                output.work_id,
                entries,
                exits,
            )
            _add_flow_group(
                hour_groups,
                (_iso(hour), hour.hour * 60, line_camera, line_location),
                output.work_id,
                entries,
                exits,
            )
        if captured.date().isoformat() == partition_date:
            duration = _optional_float(
                metadata.get("duration_seconds", metadata.get("video_duration_seconds"))
            )
            processing = _optional_float(
                metadata.get("processing_seconds", output.attempt.get("processing_seconds"))
            )
            videos.append(
                {
                    "work_id": output.work_id,
                    "captured_at_utc": _iso(captured),
                    "time_key": captured.hour * 60 + captured.minute,
                    "camera_id": camera,
                    "location_id": location,
                    "config_sha256": str(_required_value(metadata, "config_sha256")),
                    "video_duration_seconds": duration,
                    "processing_seconds": processing,
                    "speed_x_realtime": (
                        duration / processing
                        if duration is not None
                        and processing is not None
                        and duration > 0
                        and processing > 0
                        else None
                    ),
                    "distinct_people": _optional_int(metadata.get("distinct_people")),
                    "line_in_count": _optional_int(metadata.get("line_in_count")),
                    "line_out_count": _optional_int(metadata.get("line_out_count")),
                    "completed_at": _iso(
                        _first_datetime(
                            metadata | output.attempt,
                            ("completed_at", "sealed_at", "published_at"),
                            required=False,
                        )
                        or output.published_at
                    ),
                    "capture_date": partition_date,
                }
            )
    return {
        "gold_flow_minute": _flow_rows(
            minute_groups, "minute_utc", partition_date, refreshed_at
        ),
        "gold_flow_hour": _flow_rows(
            hour_groups, "hour_utc", partition_date, refreshed_at
        ),
        "gold_video": sorted(videos, key=lambda row: row["work_id"]),
        "gold_operations_hour": _operations_rows(
            control, committed, partition_date, refreshed_at
        ),
    }


def _add_flow_group(
    groups: dict[tuple[str, int, str, str], dict[str, Any]],
    key: tuple[str, int, str, str],
    work_id: str,
    entries: int,
    exits: int,
) -> None:
    group = groups.setdefault(key, {"entries": 0, "exits": 0, "work_ids": set()})
    group["entries"] += entries
    group["exits"] += exits
    group["work_ids"].add(work_id)


def _line_observed_at(
    line: Mapping[str, Any],
    captured_at: datetime,
) -> datetime:
    absolute = line.get("observed_at_utc")
    if absolute is not None:
        return _required_datetime(absolute)
    relative = line.get("video_seconds")
    if isinstance(relative, bool):
        raise GoldSourceError(
            "line count video_seconds must be finite and non-negative"
        )
    try:
        seconds = float(relative)
    except (TypeError, ValueError) as error:
        raise GoldSourceError(
            "line count requires observed_at_utc or numeric video_seconds"
        ) from error
    if not math.isfinite(seconds) or seconds < 0:
        raise GoldSourceError("line count video_seconds must be finite and non-negative")
    return _utc(captured_at) + timedelta(seconds=seconds)


def _flow_rows(
    groups: Mapping[tuple[str, int, str, str], Mapping[str, Any]],
    timestamp_column: str,
    partition_date: str,
    refreshed_at: str,
) -> list[dict[str, Any]]:
    rows = []
    for (timestamp, time_key, camera, location), values in sorted(groups.items()):
        entries = int(values["entries"])
        exits = int(values["exits"])
        rows.append(
            {
                timestamp_column: timestamp,
                "time_key": time_key,
                "camera_id": camera,
                "location_id": location,
                "entries": entries,
                "exits": exits,
                "net_flow": entries - exits,
                "source_videos": len(values["work_ids"]),
                "refreshed_at": refreshed_at,
                "flow_date": partition_date,
            }
        )
    return rows


def _operations_rows(
    control: Mapping[str, Sequence[Mapping[str, Any]]],
    committed: Sequence[CommittedOutput],
    partition_date: str,
    refreshed_at: str,
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "queued": 0,
            "started": 0,
            "succeeded": 0,
            "failed": 0,
            "deferred": 0,
            "video_hours_completed": 0.0,
            "processing": [],
        }
    )
    committed_by_attempt = {item.attempt_id: item for item in committed}
    for work in control.get("work", ()):
        timestamp = _first_datetime(
            work,
            ("queued_at", "queue_entered_at", "created_at"),
            required=False,
        )
        if timestamp and timestamp.date().isoformat() == partition_date:
            groups[_hour(timestamp)]["queued"] += 1
    for attempt in control.get("attempts", ()):
        claimed = _first_datetime(attempt, ("claimed_at", "created_at"), required=False)
        if claimed and claimed.date().isoformat() == partition_date:
            groups[_hour(claimed)]["started"] += 1
        status = str(attempt.get("status") or "")
        attempt_id = str(attempt.get("attempt_id") or "")
        output = committed_by_attempt.get(attempt_id)
        if output is not None and status == "SUCCEEDED":
            completed = _first_datetime(
                attempt,
                ("published_at", "batch_completed_at", "completed_at"),
                required=False,
            )
        else:
            if status == "EXPIRED":
                failure_names = ("batch_expired_at", "work_updated_at")
            elif status in _FAILED_ATTEMPT_STATUSES:
                failure_names = (
                    "failed_at",
                    "batch_completed_at",
                    "work_updated_at",
                )
            else:
                failure_names = ()
            completed = _first_datetime(
                attempt,
                failure_names
                + (
                    "completed_at",
                    "batch_completed_at",
                    "sealed_at",
                    "batch_sealed_at",
                ),
                required=False,
            )
        if not completed or completed.date().isoformat() != partition_date:
            continue
        group = groups[_hour(completed)]
        if output is not None and status == "SUCCEEDED":
            group["succeeded"] += 1
            duration = _optional_float(
                output.run.get(
                    "duration_seconds", attempt.get("source_duration_seconds")
                )
            )
            group["video_hours_completed"] += (duration or 0.0) / 3600.0
        elif status in _FAILED_ATTEMPT_STATUSES:
            group["failed"] += 1
        elif status in _DEFERRED_ATTEMPT_STATUSES:
            group["deferred"] += 1
        processing = _optional_float(
            attempt.get("processing_seconds")
            if attempt.get("processing_seconds") is not None
            else output.run.get("processing_seconds") if output else None
        )
        if processing is not None:
            group["processing"].append(processing)
    rows = []
    for hour, values in sorted(groups.items()):
        processing_values = values.pop("processing")
        timestamp = _required_datetime(hour)
        rows.append(
            {
                "hour_utc": hour,
                "time_key": timestamp.hour * 60,
                **values,
                "average_processing_seconds": (
                    statistics.fmean(processing_values)
                    if processing_values
                    else None
                ),
                "p95_processing_seconds": _percentile(processing_values, 0.95),
                "refreshed_at": refreshed_at,
                "operation_date": partition_date,
            }
        )
    return rows


def _dimension_rows(
    committed: Sequence[CommittedOutput],
    facts: Mapping[str, Sequence[Mapping[str, Any]]],
    refreshed_at: str,
) -> dict[str, list[dict[str, Any]]]:
    metadata = [item.work | item.run | {"work_id": item.work_id} for item in committed]
    cameras: dict[str, list[dict[str, Any]]] = defaultdict(list)
    locations: dict[str, list[dict[str, Any]]] = defaultdict(list)
    configs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    videos: list[dict[str, Any]] = []
    for row in metadata:
        captured = _required_datetime(row.get("captured_at_utc"))
        camera = str(_required_value(row, "camera_id"))
        location = str(_required_value(row, "location_id"))
        config_hash = str(_required_value(row, "config_sha256"))
        cameras[camera].append(row)
        locations[location].append(row)
        configs[config_hash].append(row)
        videos.append(
            {
                "work_id": str(row["work_id"]),
                "asset_id": str(row.get("asset_id") or row["work_id"]),
                "asset_version": str(row.get("asset_version") or "1"),
                "camera_id": camera,
                "location_id": location,
                "captured_at_utc": _iso(captured),
                "capture_date": captured.date().isoformat(),
                "time_key": captured.hour * 60 + captured.minute,
                "camera_timezone": str(row.get("camera_timezone") or "UTC"),
                "config_sha256": config_hash,
                "refreshed_at": refreshed_at,
            }
        )
    dim_camera = []
    for camera, rows in sorted(cameras.items()):
        location_values = {str(row["location_id"]) for row in rows}
        timezone_values = {str(row.get("camera_timezone") or "UTC") for row in rows}
        if len(location_values) != 1 or len(timezone_values) != 1:
            raise GoldValidationError(
                f"camera {camera} must map to one location and timezone"
            )
        camera_timezone = next(iter(timezone_values))
        try:
            ZoneInfo(camera_timezone)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise GoldSourceError(
                f"camera {camera} has invalid IANA timezone "
                f"{camera_timezone!r}"
            ) from error
        captures = [_required_datetime(row["captured_at_utc"]) for row in rows]
        dim_camera.append(
            {
                "camera_id": camera,
                "location_id": next(iter(location_values)),
                "camera_timezone": camera_timezone,
                "first_capture_utc": _iso(min(captures)),
                "last_capture_utc": _iso(max(captures)),
                "video_count": len({str(row["work_id"]) for row in rows}),
                "refreshed_at": refreshed_at,
            }
        )
    dim_location = []
    for location, rows in sorted(locations.items()):
        captures = [_required_datetime(row["captured_at_utc"]) for row in rows]
        dim_location.append(
            {
                "location_id": location,
                "first_capture_utc": _iso(min(captures)),
                "last_capture_utc": _iso(max(captures)),
                "camera_count": len({str(row["camera_id"]) for row in rows}),
                "video_count": len({str(row["work_id"]) for row in rows}),
                "refreshed_at": refreshed_at,
            }
        )
    dim_config = []
    for config_hash, rows in sorted(configs.items()):
        values = {_config_json(row.get("config_json", {})) for row in rows}
        if len(values) != 1:
            raise GoldValidationError(
                f"config_sha256 {config_hash} maps to multiple configurations"
            )
        raw = next(iter(values))
        parsed = json.loads(raw)
        captures = [_required_datetime(row["captured_at_utc"]) for row in rows]
        dim_config.append(
            {
                "config_sha256": config_hash,
                "config_json": raw,
                "pipeline": parsed.get("pipeline"),
                "device_variant": parsed.get("device_variant"),
                "device": parsed.get("device"),
                "batch_size": parsed.get("batch_size"),
                "sample_fps": parsed.get("sample_fps"),
                "detection_threshold": parsed.get("detection_threshold"),
                "use_fp16": parsed.get("use_fp16"),
                "detector_model": parsed.get("detector_model"),
                "camera_motion_compensation": parsed.get(
                    "camera_motion_compensation"
                ),
                "counting_line_json": _canonical_json(parsed.get("line"))
                if parsed.get("line") is not None
                else None,
                "first_capture_utc": _iso(min(captures)),
                "last_capture_utc": _iso(max(captures)),
                "video_count": len({str(row["work_id"]) for row in rows}),
                "refreshed_at": refreshed_at,
            }
        )
    fact_dates = sorted(
        {
            str(row[column])
            for table, column in (
                ("gold_flow_minute", "flow_date"),
                ("gold_flow_hour", "flow_date"),
                ("gold_video", "capture_date"),
                ("gold_operations_hour", "operation_date"),
            )
            for row in facts[table]
            if row.get(column)
        }
    )
    dates = _date_range(fact_dates, _required_datetime(refreshed_at).date())
    return {
        "gold_dim_date": [_date_dimension_row(value, refreshed_at) for value in dates],
        "gold_dim_time": [
            {
                "time_key": minute,
                "hour_24": minute // 60,
                "minute_of_hour": minute % 60,
                "time_label": f"{minute // 60:02d}:{minute % 60:02d}",
                "hour_label": f"{minute // 60:02d}:00",
                "day_part": _day_part(minute // 60),
                "refreshed_at": refreshed_at,
            }
            for minute in range(1440)
        ],
        "gold_dim_camera": dim_camera,
        "gold_dim_location": dim_location,
        "gold_dim_video": sorted(videos, key=lambda row: row["work_id"]),
        "gold_dim_model_config": dim_config,
    }


def _validate_tables(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> int:
    primary_keys = {
        "gold_dim_date": "date_key",
        "gold_dim_time": "time_key",
        "gold_dim_camera": "camera_id",
        "gold_dim_location": "location_id",
        "gold_dim_video": "work_id",
        "gold_dim_model_config": "config_sha256",
    }
    for table, key in primary_keys.items():
        values = [row.get(key) for row in tables[table]]
        if any(value is None for value in values) or len(values) != len(set(values)):
            raise GoldValidationError(f"{table}.{key} must be non-null and unique")
    relationships = (
        ("gold_flow_minute", "flow_date", "gold_dim_date", "date_key"),
        ("gold_flow_hour", "flow_date", "gold_dim_date", "date_key"),
        ("gold_video", "capture_date", "gold_dim_date", "date_key"),
        ("gold_operations_hour", "operation_date", "gold_dim_date", "date_key"),
        ("gold_flow_minute", "time_key", "gold_dim_time", "time_key"),
        ("gold_flow_hour", "time_key", "gold_dim_time", "time_key"),
        ("gold_video", "time_key", "gold_dim_time", "time_key"),
        ("gold_operations_hour", "time_key", "gold_dim_time", "time_key"),
        ("gold_flow_minute", "camera_id", "gold_dim_camera", "camera_id"),
        ("gold_flow_hour", "camera_id", "gold_dim_camera", "camera_id"),
        ("gold_video", "camera_id", "gold_dim_camera", "camera_id"),
        ("gold_flow_minute", "location_id", "gold_dim_location", "location_id"),
        ("gold_flow_hour", "location_id", "gold_dim_location", "location_id"),
        ("gold_video", "location_id", "gold_dim_location", "location_id"),
        ("gold_video", "config_sha256", "gold_dim_model_config", "config_sha256"),
        ("gold_video", "work_id", "gold_dim_video", "work_id"),
    )
    for fact_table, fact_key, dimension_table, dimension_key in relationships:
        dimension_values = {row.get(dimension_key) for row in tables[dimension_table]}
        missing = {
            row.get(fact_key)
            for row in tables[fact_table]
            if row.get(fact_key) not in dimension_values
        }
        if missing:
            raise GoldValidationError(
                f"{fact_table}.{fact_key} has unresolved {dimension_table} keys: "
                f"{sorted(missing, key=str)[:10]}"
            )
    return len(primary_keys) + len(relationships)


def _load_output_document(
    path: Path,
    expected_sha256: str,
    *,
    verify_hash: bool,
    work_id: str | None = None,
    attempt_id: str | None = None,
) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if resolved.is_dir():
        delta_log = resolved / "_delta_log"
        if delta_log.is_dir():
            return _load_delta_output_document(
                resolved,
                expected_sha256,
                verify_hash=verify_hash,
                work_id=work_id,
                attempt_id=attempt_id,
            )
        candidates = [
            resolved / name
            for name in ("output.json", "result.json", "records.json")
            if (resolved / name).is_file()
        ]
        if len(candidates) != 1:
            raise GoldSourceError(
                f"{resolved} must contain exactly one supported output JSON file"
            )
        resolved = candidates[0]
    if not resolved.is_file():
        raise GoldSourceError(f"committed output does not exist: {resolved}")
    content = resolved.read_bytes()
    try:
        payload = json.loads(content)
    except json.JSONDecodeError as error:
        raise GoldSourceError(f"committed output is not valid JSON: {resolved}") from error
    if isinstance(payload, list):
        if work_id is None or attempt_id is None:
            raise GoldSourceError(
                "attempt-scoped records require a committed work/attempt identity"
            )
        matching = [
            row
            for row in payload
            if isinstance(row, dict)
            and str(row.get("work_id")) == work_id
            and str(row.get("attempt_id")) == attempt_id
        ]
        actual_sha256 = _sha256_json(
            sorted(
                matching,
                key=lambda row: (
                    str(row.get("work_id", "")),
                    str(row.get("attempt_id", "")),
                    str(row.get("record_type", "")),
                    int(row.get("record_sequence", -1)),
                ),
            )
        )
        if verify_hash and expected_sha256 and actual_sha256 != expected_sha256:
            raise GoldSourceError(
                f"committed output digest mismatch for {work_id}/{attempt_id}: "
                f"expected {expected_sha256}, observed {actual_sha256}"
            )
        return _records_output_document(matching, work_id, attempt_id)
    actual_sha256 = hashlib.sha256(content).hexdigest()
    if verify_hash and expected_sha256 and actual_sha256 != expected_sha256:
        raise GoldSourceError(
            f"committed output digest mismatch for {resolved}: "
            f"expected {expected_sha256}, observed {actual_sha256}"
        )
    if not isinstance(payload, dict):
        raise GoldSourceError(f"committed output must be a JSON object: {resolved}")
    return payload


def _load_delta_output_document(
    path: Path,
    expected_sha256: str,
    *,
    verify_hash: bool,
    work_id: str | None,
    attempt_id: str | None,
) -> dict[str, Any]:
    identity = _required_delta_identity(work_id, attempt_id)
    spark, owns_session = _delta_read_session(identity[0])
    try:
        records = _read_delta_records(spark, path, *identity)
    finally:
        if owns_session:
            spark.stop()
    _verify_records_digest(
        records,
        expected_sha256,
        verify_hash=verify_hash,
        identity=identity,
        source="Delta",
    )
    return _records_output_document(records, *identity)


def _required_delta_identity(
    work_id: str | None,
    attempt_id: str | None,
) -> tuple[str, str]:
    if work_id is None or attempt_id is None:
        raise GoldSourceError(
            "attempt-scoped Delta output requires a work/attempt identity"
        )
    return work_id, attempt_id


def _delta_read_session(work_id: str) -> tuple[Any, bool]:
    try:
        from pyspark.sql import SparkSession
    except ImportError as error:
        raise GoldSourceError(
            "reading committed Delta output requires people-counter[local-spark]"
        ) from error
    active = SparkSession.getActiveSession()
    if active is not None:
        return active, False
    from people_counter.local_spark import create_local_spark_session

    return (
        create_local_spark_session(
            master=os.environ.get("SPARK_MASTER_URL", "local[*]"),
            app_name="people-counter-gold-read",
            correlation_id=f"gold-{work_id}",
        ),
        True,
    )


def _read_delta_records(
    spark: Any,
    path: Path,
    work_id: str,
    attempt_id: str,
) -> list[dict[str, Any]]:
    rows = (
        spark.read.format("delta")
        .load(str(path))
        .where(f"work_id = {_sql_literal(work_id)}")
        .where(f"attempt_id = {_sql_literal(attempt_id)}")
        .select("record_json")
        .collect()
    )
    records = []
    for row in rows:
        try:
            decoded = json.loads(str(row["record_json"]))
        except (json.JSONDecodeError, TypeError) as error:
            raise GoldSourceError("committed Delta record_json is invalid") from error
        if not isinstance(decoded, dict):
            raise GoldSourceError("committed Delta record_json must be an object")
        records.append(decoded)
    return records


def _verify_records_digest(
    records: Sequence[Mapping[str, Any]],
    expected_sha256: str,
    *,
    verify_hash: bool,
    identity: tuple[str, str],
    source: str,
) -> None:
    ordered = sorted(
        records,
        key=lambda row: (
            str(row.get("work_id", "")),
            str(row.get("attempt_id", "")),
            str(row.get("record_type", "")),
            int(row.get("record_sequence", -1)),
        ),
    )
    actual_sha256 = _sha256_json(ordered)
    if verify_hash and expected_sha256 and actual_sha256 != expected_sha256:
        raise GoldSourceError(
            f"committed {source} digest mismatch for {identity[0]}/{identity[1]}: "
            f"expected {expected_sha256}, observed {actual_sha256}"
        )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _records_output_document(
    records: Sequence[Mapping[str, Any]],
    work_id: str,
    attempt_id: str,
) -> dict[str, Any]:
    terminals = [
        row
        for row in records
        if row.get("record_type") in {"video_result", "error"}
    ]
    if len(terminals) != 1 or terminals[0].get("record_type") != "video_result":
        raise GoldSourceError(
            f"committed output for {work_id}/{attempt_id} lacks one success terminal"
        )
    terminal = terminals[0]
    try:
        run_payload = json.loads(str(terminal.get("payload_json", "{}")))
    except json.JSONDecodeError as error:
        raise GoldSourceError("video_result payload_json is invalid") from error
    if not isinstance(run_payload, dict):
        raise GoldSourceError("video_result payload_json must be an object")
    run = {
        **run_payload,
        "work_id": work_id,
        "attempt_id": attempt_id,
        "processing_seconds": terminal.get("processing_seconds"),
        "processed_frames": terminal.get("processed_frames"),
        "status": terminal.get("status"),
    }
    lines: list[dict[str, Any]] = []
    for row in records:
        if row.get("record_type") != "line_count":
            continue
        try:
            value = json.loads(str(row.get("payload_json", "{}")))
        except json.JSONDecodeError as error:
            raise GoldSourceError("line_count payload_json is invalid") from error
        if not isinstance(value, dict):
            raise GoldSourceError("line_count payload_json must be an object")
        lines.append({**value, "work_id": work_id, "attempt_id": attempt_id})
    return {"run": run, "line_counts": lines}


def _split_output_document(
    document: Mapping[str, Any], work_id: str, attempt_id: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    raw_run: Any = document.get("run")
    if raw_run is None:
        runs = document.get("runs", [])
        if isinstance(runs, list):
            matching = [
                row
                for row in runs
                if isinstance(row, dict)
                and str(row.get("work_id", work_id)) == work_id
                and str(row.get("attempt_id", attempt_id)) == attempt_id
            ]
            raw_run = matching[0] if len(matching) == 1 else None
    if raw_run is None and "captured_at_utc" in document:
        raw_run = document
    if not isinstance(raw_run, dict):
        raise GoldSourceError(
            f"output for {work_id}/{attempt_id} has no unique run record"
        )
    for name, expected in (("work_id", work_id), ("attempt_id", attempt_id)):
        actual = raw_run.get(name)
        if actual is not None and str(actual) != expected:
            raise GoldSourceError(
                f"output {name} {actual!r} does not match committed pointer {expected!r}"
            )
    raw_lines = document.get("line_counts", document.get("lines", []))
    if not isinstance(raw_lines, list) or not all(
        isinstance(line, dict) for line in raw_lines
    ):
        raise GoldSourceError("line_counts must be a list of JSON objects")
    lines = [
        dict(line)
        for line in raw_lines
        if str(line.get("work_id", work_id)) == work_id
        and str(line.get("attempt_id", attempt_id)) == attempt_id
    ]
    return dict(raw_run), lines


def _output_dates(output: CommittedOutput) -> set[str]:
    captured = _required_datetime(
        _coalesce(output.run, output.work, names=("captured_at_utc",))
    )
    return {
        captured.date().isoformat(),
        *(
            _line_observed_at(line, captured).date().isoformat()
            for line in output.line_counts
        ),
    }


def _control_dates(
    control: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    cutoff: datetime | None = None,
) -> set[str]:
    dates: set[str] = set()
    for table, candidates in (
        (
            "work",
            (
                "queued_at",
                "queue_entered_at",
                "created_at",
                "available_at",
                "updated_at",
            ),
        ),
        (
            "attempts",
            (
                "claimed_at",
                "created_at",
                "sealed_at",
                "batch_sealed_at",
                "failed_at",
                "completed_at",
                "batch_completed_at",
                "batch_expired_at",
                "work_updated_at",
                "published_at",
            ),
        ),
    ):
        for row in control.get(table, ()):
            for column in candidates:
                if (
                    column == "batch_expired_at"
                    and str(row.get("status") or "") != "EXPIRED"
                ):
                    continue
                timestamp = _optional_datetime(row.get(column))
                if timestamp is not None and (
                    cutoff is None or timestamp >= cutoff
                ):
                    dates.add(timestamp.date().isoformat())
    return dates


def _validate_pointer_metadata(
    work: Mapping[str, Any],
    attempt: Mapping[str, Any],
    publication: Mapping[str, Any],
) -> None:
    if str(attempt.get("work_id")) != str(work.get("work_id")):
        raise GoldSourceError("attempt belongs to a different work item")
    if attempt.get("output_path") not in (None, publication.get("output_path")):
        raise GoldSourceError("attempt and publication output paths differ")
    if attempt.get("output_sha256") not in (
        None,
        publication.get("output_sha256"),
    ):
        raise GoldSourceError("attempt and publication output digests differ")
    work_sequence = work.get("publication_sequence")
    if work_sequence is not None and int(work_sequence) != int(
        publication["publication_sequence"]
    ):
        raise GoldSourceError("work and publication sequence differ")


def _merge_payload(work: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(work)
    raw = work.get("payload_json")
    if isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as error:
            raise GoldSourceError("work.payload_json is invalid") from error
        if not isinstance(payload, dict):
            raise GoldSourceError("work.payload_json must be a JSON object")
        merged = payload | merged
    return merged


def _date_dimension_row(value: date, refreshed_at: str) -> dict[str, Any]:
    iso_year, iso_week, iso_day = value.isocalendar()
    return {
        "date_key": value.isoformat(),
        "calendar_year": value.year,
        "calendar_quarter": (value.month - 1) // 3 + 1,
        "calendar_month": value.month,
        "month_name": value.strftime("%B"),
        "month_short_name": value.strftime("%b"),
        "year_month": value.strftime("%Y-%m"),
        "day_of_month": value.day,
        "iso_day_of_week": iso_day,
        "iso_week_year": iso_year,
        "iso_week_of_year": iso_week,
        "iso_year_week": f"{iso_year}-W{iso_week:02d}",
        "day_name": value.strftime("%A"),
        "is_weekend": iso_day in (6, 7),
        "refreshed_at": refreshed_at,
    }


def _date_range(values: Sequence[str], fallback: date) -> list[date]:
    if not values:
        return [fallback]
    start = date.fromisoformat(values[0])
    end = date.fromisoformat(values[-1])
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def _day_part(hour: int) -> str:
    if hour < 6:
        return "Night"
    if hour < 12:
        return "Morning"
    if hour < 18:
        return "Afternoon"
    return "Evening"


def _percentile(values: Sequence[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * quantile) - 1)
    return ordered[index]


def _hour(value: datetime) -> str:
    return _iso(value.replace(minute=0, second=0, microsecond=0))


def _coalesce(
    *mappings: Mapping[str, Any], names: Sequence[str]
) -> Any:
    for name in names:
        for mapping in mappings:
            value = mapping.get(name)
            if value is not None:
                return value
    return None


def _required_value(mapping: Mapping[str, Any], name: str) -> Any:
    value = mapping.get(name)
    if value is None or value == "":
        raise GoldSourceError(f"{name} is required in committed output metadata")
    return value


def _config_json(value: Any) -> str:
    if value is None or value == "":
        return "{}"
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as error:
            raise GoldSourceError("config_json is invalid") from error
    elif isinstance(value, dict):
        parsed = value
    else:
        raise GoldSourceError("config_json must be an object or JSON string")
    if not isinstance(parsed, dict):
        raise GoldSourceError("config_json must contain an object")
    return _canonical_json(parsed)


def _first_datetime(
    mapping: Mapping[str, Any],
    names: Sequence[str],
    *,
    required: bool,
) -> datetime | None:
    for name in names:
        if mapping.get(name) is not None:
            return _required_datetime(mapping[name])
    if required:
        raise GoldSourceError(f"one of {', '.join(names)} is required")
    return None


def _required_datetime(value: Any) -> datetime:
    result = _optional_datetime(value)
    if result is None:
        raise GoldSourceError(f"invalid UTC timestamp {value!r}")
    return result


def _optional_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return None
        return _utc(value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        try:
            return datetime.fromtimestamp(numeric, timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        normalized = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                return None
            return _utc(parsed)
        except ValueError:
            return None
    return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _date_string(value: Any) -> str:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    try:
        return date.fromisoformat(str(value)).isoformat()
    except ValueError as error:
        raise ValueError(f"invalid date {value!r}; expected YYYY-MM-DD") from error


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise GoldSourceError(f"metric must be numeric, not {value!r}")
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise GoldSourceError(f"metric must be numeric, not {value!r}") from error
    if not math.isfinite(result) or result < 0:
        raise GoldSourceError(
            f"metric must be finite and non-negative, not {value!r}"
        )
    return result


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise GoldSourceError(f"metric must be an integer, not {value!r}")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as error:
        raise GoldSourceError(f"metric must be an integer, not {value!r}") from error
    if (
        not math.isfinite(numeric)
        or numeric < 0
        or not numeric.is_integer()
    ):
        raise GoldSourceError(
            f"metric must be a finite non-negative integer, not {value!r}"
        )
    return int(numeric)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _jsonable_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return json.loads(_canonical_json(dict(row)))


def _checkpoint_targets_match(
    checkpoint: Mapping[str, Any] | None,
    current: Mapping[str, int],
) -> bool:
    if checkpoint is None:
        return False
    try:
        saved = json.loads(str(checkpoint["target_versions_json"]))
    except (KeyError, TypeError, json.JSONDecodeError):
        return False
    return saved == dict(current)


def _spark_schema(name: str) -> Any:
    try:
        from pyspark.sql.types import (
            BooleanType,
            DateType,
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )
    except ImportError as error:
        raise UnsupportedGoldBackendError(
            "pyspark is required for path-Delta writes"
        ) from error
    constructors = {
        "boolean": BooleanType,
        "date": DateType,
        "double": DoubleType,
        "integer": IntegerType,
        "long": LongType,
        "string": StringType,
        "timestamp": TimestampType,
    }
    try:
        fields = _SPARK_FIELDS[name]
    except KeyError as error:
        raise ValueError(f"no Spark schema for gold table {name!r}") from error
    return StructType(
        [
            StructField(field, constructors[kind](), nullable)
            for field, kind, nullable in fields
        ]
    )


def _spark_row(name: str, row: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field, kind, _nullable in _SPARK_FIELDS[name]:
        value = row.get(field)
        if value is not None and kind == "timestamp":
            value = _required_datetime(value)
        elif value is not None and kind == "date":
            value = date.fromisoformat(_date_string(value))
        result[field] = value
    return result


def _jsonable_spark_row(row: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            result[key] = _iso(value)
        elif isinstance(value, date):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return _jsonable_row(result)


def _table_exists(connection: sqlite3.Connection, name: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
        ).fetchone()
        is not None
    )


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute(
            f"PRAGMA table_info({_quoted_identifier(table)})"
        ).fetchall()
    }


def _quoted_identifier(value: str) -> str:
    if not value or not value.replace("_", "").isalnum():
        raise ValueError(f"invalid SQLite identifier {value!r}")
    return f'"{value}"'


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m people_counter.sjd_gold",
        description="Build local Candidate A gold facts and dimensions.",
    )
    parser.add_argument(
        "mode",
        choices=("plan", "build-facts", "build-dimensions", "validate", "run"),
    )
    parser.add_argument("--control-db", type=Path, required=True)
    parser.add_argument("--gold-root", type=Path, required=True)
    parser.add_argument("--backend", choices=("delta", "json"), default="delta")
    parser.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER_URL", "local[*]"),
    )
    parser.add_argument("--lookback-hours", type=int, default=48)
    parser.add_argument(
        "--date",
        action="append",
        dest="dates",
        help="Explicit YYYY-MM-DD fact partition; repeat as needed.",
    )
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--full-rebuild", action="store_true")
    parser.add_argument(
        "--no-verify-output-hash",
        action="store_true",
        help="Only for recovery of legacy local output pointers.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _build_parser().parse_args(argv)
    spark = None
    try:
        if arguments.backend == "delta":
            from people_counter.local_spark import create_local_spark_session

            spark = create_local_spark_session(
                master=arguments.spark_master,
                app_name=f"people-counter-gold-{arguments.mode}",
                correlation_id=f"gold-{arguments.mode}",
            )
            backend = PathDeltaGoldStore(arguments.gold_root, spark)
        else:
            backend = LocalJsonGoldStore(arguments.gold_root)
        job = LocalGoldJob(
            arguments.control_db,
            arguments.gold_root,
            verify_output_hash=not arguments.no_verify_output_hash,
            store=backend,
        )
        if arguments.mode == "plan":
            result = job.plan(
                lookback_hours=arguments.lookback_hours,
                full_rebuild=arguments.full_rebuild,
                reset=arguments.force,
            ).to_dict()
        elif arguments.mode == "build-facts":
            result = job.build_facts(
                dates=arguments.dates,
                lookback_hours=arguments.lookback_hours,
                force=arguments.force,
                full_rebuild=arguments.full_rebuild,
            )
        elif arguments.mode == "build-dimensions":
            result = job.build_dimensions(
                full_rebuild=arguments.full_rebuild,
                force=arguments.force,
            )
        elif arguments.mode == "validate":
            result = job.validate()
        else:
            result = job.run(
                lookback_hours=arguments.lookback_hours,
                force=arguments.force,
                full_rebuild=arguments.full_rebuild,
            )
    finally:
        if spark is not None:
            spark.stop()
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
