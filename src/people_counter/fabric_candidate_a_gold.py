"""Fabric source, typed gold store, and checkpoint/outbox state."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Callable

from people_counter.fabric_candidate_a import FabricCandidateAConfig
from people_counter.fabric_control import ControlWriter
from people_counter.sjd_gold import (
    CommittedOutput,
    GoldSourceError,
    GoldValidationError,
    LocalGoldJob,
    SourceCheckpoint,
    _jsonable_row,
    _jsonable_spark_row,
    _records_output_document,
    _required_datetime,
    _spark_row,
    _spark_schema,
    _split_output_document,
)


_STATE_SCHEMAS = {
    "gold_checkpoints": (
        "stage string, source_key string, publication_sequence long, "
        "source_versions_json string, target_versions_json string, "
        "completed_at timestamp"
    ),
    "semantic_refresh_outbox": (
        "outbox_id long, dedupe_key string, reason string, payload_json string, "
        "created_at timestamp, acked_at timestamp, acked_by string"
    ),
}


class FabricCommittedSource:
    """Read gold inputs only through authoritative committed pointers."""

    def __init__(
        self,
        spark_session: Any,
        attempt_adapter: Any,
        *,
        config: FabricCandidateAConfig | None = None,
    ) -> None:
        self.spark = spark_session
        self.attempts = attempt_adapter
        self.config = config or FabricCandidateAConfig()

    def checkpoint(self) -> SourceCheckpoint:
        rows = self._visible_rows()
        sequence = max(
            (int(row["publication"]["publication_sequence"]) for row in rows),
            default=0,
        )
        versions = {
            name: _sha256(
                [
                    row[name]
                    for row in rows
                ]
            )
            for name in ("work", "attempt", "publication")
        }
        return SourceCheckpoint(sequence, versions)

    def committed_outputs(self) -> list[CommittedOutput]:
        result: list[CommittedOutput] = []
        for pointer in self._visible_rows():
            work = pointer["work"]
            attempt = pointer["attempt"]
            publication = pointer["publication"]
            path = str(publication["output_path"])
            process_attempt = _path_value(path, "attempt")
            batch_id = _path_value(path, "batch")
            records = self.attempts.read_records(batch_id, process_attempt)
            selected = [
                record
                for record in records
                if record.get("work_id") == work["work_id"]
                and record.get("attempt_id") == attempt["attempt_id"]
            ]
            if not selected:
                raise GoldSourceError(
                    f"committed output for {work['work_id']} has no records"
                )
            actual = _sha256(selected)
            if actual != publication["output_sha256"]:
                raise GoldSourceError(
                    f"committed output digest mismatch for {work['work_id']}"
                )
            document = _records_output_document(
                selected, str(work["work_id"]), str(attempt["attempt_id"])
            )
            run, line_counts = _split_output_document(
                document, str(work["work_id"]), str(attempt["attempt_id"])
            )
            payload = json.loads(str(work["payload_json"]))
            if not isinstance(payload, dict):
                raise GoldSourceError("committed work payload must be an object")
            published_at = _required_datetime(publication["published_at"])
            result.append(
                CommittedOutput(
                    str(work["work_id"]),
                    str(attempt["attempt_id"]),
                    int(publication["publication_sequence"]),
                    published_at,
                    dict(work) | payload,
                    dict(attempt),
                    run,
                    tuple(line_counts),
                )
            )
        return result

    def _visible_rows(self) -> list[dict[str, dict[str, Any]]]:
        work = self._rows("work")
        attempts = {
            row["attempt_id"]: row for row in self._rows("attempts")
        }
        publications = {
            (row["work_id"], row["attempt_id"]): row
            for row in self._rows("publications")
        }
        visible: list[dict[str, dict[str, Any]]] = []
        for row in work:
            attempt_id = row.get("committed_attempt_id")
            if attempt_id is None:
                continue
            attempt = attempts.get(attempt_id)
            publication = publications.get((row["work_id"], attempt_id))
            if (
                attempt is None
                or publication is None
                or attempt["work_id"] != row["work_id"]
                or attempt["status"] != "SUCCEEDED"
                or row["status"] != "SUCCEEDED"
                or row["publication_sequence"]
                != publication["publication_sequence"]
                or publication["output_path"] != attempt["output_path"]
                or publication["output_sha256"] != attempt["output_sha256"]
            ):
                raise GoldSourceError(
                    f"invalid committed pointer for {row['work_id']}"
                )
            visible.append(
                {
                    "work": row,
                    "attempt": attempt,
                    "publication": publication,
                }
            )
        return sorted(
            visible,
            key=lambda item: int(
                item["publication"]["publication_sequence"]
            ),
        )

    def _rows(self, suffix: str) -> list[dict[str, Any]]:
        name = self.config.table(suffix)
        self.spark.catalog.refreshTable(name)
        return [
            _normalize_spark_row(row.asDict(recursive=True))
            for row in self.spark.table(name).collect()
        ]

    def control_rows(self) -> dict[str, list[dict[str, Any]]]:
        work = self._rows("work")
        batches = {
            row["batch_id"]: row for row in self._rows("batches")
        }
        publications = {
            row["attempt_id"]: row for row in self._rows("publications")
        }
        attempts: list[dict[str, Any]] = []
        for row in self._rows("attempts"):
            batch = batches.get(row["batch_id"], {})
            publication = publications.get(row["attempt_id"], {})
            attempts.append(
                dict(row)
                | {
                    "batch_sealed_at": batch.get("sealed_at"),
                    "batch_completed_at": batch.get("committed_at"),
                    "batch_expired_at": batch.get("lease_expires_at"),
                    "work_updated_at": next(
                        (
                            item.get("updated_at")
                            for item in work
                            if item["work_id"] == row["work_id"]
                        ),
                        None,
                    ),
                    "published_at": publication.get("published_at"),
                }
            )
        return {"work": work, "attempts": attempts}

    def operations_checkpoint(self) -> SourceCheckpoint:
        names = ("work", "attempts", "batches", "publications")
        rows = {
            name: sorted(
                self._rows(name),
                key=lambda row: json.dumps(
                    row,
                    allow_nan=False,
                    default=str,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            )
            for name in names
        }
        sequence = max(
            (
                int(row["publication_sequence"])
                for row in rows["publications"]
            ),
            default=0,
        )
        return SourceCheckpoint(
            sequence,
            {name: _sha256(rows[name]) for name in names},
        )


class FabricGoldStoreImpl:
    """Typed Delta catalog tables with exact partition-replace readback."""

    def __init__(
        self,
        spark_session: Any,
        *,
        config: FabricCandidateAConfig | None = None,
    ) -> None:
        self.spark = spark_session
        self.config = config or FabricCandidateAConfig()
        self.writer = ControlWriter(
            spark_session, self.config.table("locks")
        )

    def read_table(self, name: str) -> list[dict[str, Any]]:
        table = self._table(name)
        if not self.spark.catalog.tableExists(table):
            return []
        self.spark.catalog.refreshTable(table)
        return [
            _jsonable_spark_row(row.asDict(recursive=True))
            for row in self.spark.table(table).collect()
        ]

    def version(self, name: str) -> int:
        table = self._table(name)
        if not self.spark.catalog.tableExists(table):
            return 0
        from delta.tables import DeltaTable

        row = DeltaTable.forName(self.spark, table).history(1).select(
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
            row.get(partition_column) != partition_value
            for row in normalized
        ):
            raise ValueError(
                f"{name} replacement contains rows outside "
                f"{partition_column}={partition_value}"
            )

        def operation() -> int:
            table = self._table(name)
            frame = self.spark.createDataFrame(
                [_spark_row(name, row) for row in normalized],
                schema=_spark_schema(name),
            )
            predicate = (
                f"{partition_column} = DATE "
                f"'{_date_partition(partition_value)}'"
            )
            if normalized:
                writer = (
                    frame.write.format("delta")
                    .mode("overwrite")
                    .option("replaceWhere", predicate)
                )
                if not self.spark.catalog.tableExists(table):
                    writer = writer.partitionBy(partition_column)
                writer.saveAsTable(table)
            elif self.spark.catalog.tableExists(table):
                from delta.tables import DeltaTable

                DeltaTable.forName(self.spark, table).delete(predicate)
            observed = [
                row
                for row in self.read_table(name)
                if row.get(partition_column) == partition_value
            ]
            if _canonical_rows(observed) != _canonical_rows(normalized):
                raise GoldValidationError(
                    f"{name} partition exact readback failed"
                )
            return len(normalized)

        return self.writer.run(operation)

    def replace_table(
        self, name: str, rows: Sequence[Mapping[str, Any]]
    ) -> int:
        normalized = [_jsonable_row(row) for row in rows]

        def operation() -> int:
            frame = self.spark.createDataFrame(
                [_spark_row(name, row) for row in normalized],
                schema=_spark_schema(name),
            )
            frame.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "false"
            ).saveAsTable(self._table(name))
            if _canonical_rows(self.read_table(name)) != _canonical_rows(
                normalized
            ):
                raise GoldValidationError(
                    f"{name} exact readback/cardinality failed"
                )
            return len(normalized)

        return self.writer.run(operation)

    def _table(self, name: str) -> str:
        if not name.startswith("gold_"):
            raise ValueError(f"not a Candidate A gold table: {name!r}")
        return self.config.table(name)


class FabricGoldState:
    """Delta checkpoint/outbox state serialized with the control writer."""

    def __init__(
        self,
        spark_session: Any,
        *,
        config: FabricCandidateAConfig | None = None,
        clock: Callable[[], datetime] | None = None,
        auto_bootstrap: bool = True,
    ) -> None:
        self.spark = spark_session
        self.config = config or FabricCandidateAConfig()
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.writer = ControlWriter(
            spark_session, self.config.table("locks")
        )
        if auto_bootstrap:
            self.bootstrap()

    def bootstrap(self) -> None:
        for suffix, schema in _STATE_SCHEMAS.items():
            table = self.config.table(suffix)
            if not self.spark.catalog.tableExists(table):
                self.spark.createDataFrame([], schema=schema).write.format(
                    "delta"
                ).mode("errorifexists").saveAsTable(table)

    def checkpoint(self, stage: str) -> dict[str, Any] | None:
        matches = [
            row for row in self._rows("gold_checkpoints")
            if row["stage"] == stage
        ]
        if len(matches) > 1:
            raise GoldValidationError("duplicate gold checkpoint stage")
        return matches[0] if matches else None

    def save_checkpoint(
        self,
        stage: str,
        source: SourceCheckpoint,
        target_versions: Mapping[str, int],
    ) -> None:
        self.save_checkpoints_and_enqueue(
            {stage: (source, target_versions)},
            None,
            source,
            target_versions,
        )

    def enqueue_refresh(
        self,
        reason: str,
        source: SourceCheckpoint,
        target_versions: Mapping[str, int],
    ) -> int:
        result = self.save_checkpoints_and_enqueue(
            {},
            reason,
            source,
            target_versions,
        )
        assert result is not None
        return result

    def save_checkpoints_and_enqueue(
        self,
        checkpoints: Mapping[
            str, tuple[SourceCheckpoint, Mapping[str, int]]
        ],
        reason: str | None,
        refresh_source: SourceCheckpoint,
        refresh_target_versions: Mapping[str, int],
    ) -> int | None:
        def operation() -> int | None:
            now = _spark_timestamp(self._clock())
            checkpoint_rows = self._rows("gold_checkpoints")
            by_stage = {row["stage"]: row for row in checkpoint_rows}
            for stage, (source, versions) in checkpoints.items():
                by_stage[stage] = {
                    "stage": stage,
                    "source_key": source.key,
                    "publication_sequence": source.publication_sequence,
                    "source_versions_json": _canonical(source.versions),
                    "target_versions_json": _canonical(dict(versions)),
                    "completed_at": now,
                }
            outbox = self._rows("semantic_refresh_outbox")
            outbox_id: int | None = None
            if reason is not None:
                payload = {
                    "reason": reason,
                    "source_key": refresh_source.key,
                    "publication_sequence": (
                        refresh_source.publication_sequence
                    ),
                    "target_versions": dict(refresh_target_versions),
                }
                dedupe = _sha256(payload)
                existing = next(
                    (
                        row
                        for row in outbox
                        if row["dedupe_key"] == dedupe
                    ),
                    None,
                )
                if existing is None:
                    outbox_id = max(
                        (int(row["outbox_id"]) for row in outbox),
                        default=0,
                    ) + 1
                    outbox.append(
                        {
                            "outbox_id": outbox_id,
                            "dedupe_key": dedupe,
                            "reason": reason,
                            "payload_json": _canonical(payload),
                            "created_at": now,
                            "acked_at": None,
                            "acked_by": None,
                        }
                    )
                else:
                    outbox_id = int(existing["outbox_id"])
            self._replace(
                "gold_checkpoints",
                list(by_stage.values()),
                ("stage",),
            )
            self._replace(
                "semantic_refresh_outbox",
                outbox,
                ("outbox_id",),
            )
            return outbox_id

        return self.writer.run(operation)

    def pending_refreshes(self) -> list[dict[str, Any]]:
        return [
            dict(row) | {"payload": json.loads(row["payload_json"])}
            for row in sorted(
                self._rows("semantic_refresh_outbox"),
                key=lambda item: int(item["outbox_id"]),
            )
            if row["acked_at"] is None
        ]

    def acknowledge_refresh(
        self,
        outbox_id: int,
        actor: str = "fabric",
        *,
        expected_dedupe_key: str | None = None,
    ) -> bool:
        def operation() -> bool:
            rows = self._rows("semantic_refresh_outbox")
            changed = 0
            for row in rows:
                if (
                    int(row["outbox_id"]) == outbox_id
                    and row["acked_at"] is None
                    and (
                        expected_dedupe_key is None
                        or row["dedupe_key"] == expected_dedupe_key
                    )
                ):
                    row["acked_at"] = _spark_timestamp(self._clock())
                    row["acked_by"] = actor
                    changed += 1
            if changed > 1:
                raise GoldValidationError("outbox identity is not unique")
            self._replace(
                "semantic_refresh_outbox",
                rows,
                ("outbox_id",),
            )
            return changed == 1

        return self.writer.run(operation)

    def _rows(self, suffix: str) -> list[dict[str, Any]]:
        table = self.config.table(suffix)
        self.spark.catalog.refreshTable(table)
        return [
            _normalize_spark_row(row.asDict(recursive=True))
            for row in self.spark.table(table).collect()
        ]

    def _replace(
        self,
        suffix: str,
        rows: list[dict[str, Any]],
        keys: tuple[str, ...],
    ) -> None:
        identities = [tuple(row[key] for key in keys) for row in rows]
        if len(identities) != len(set(identities)):
            raise GoldValidationError(f"duplicate {suffix} identity")
        table = self.config.table(suffix)
        self.spark.createDataFrame(
            rows, schema=_STATE_SCHEMAS[suffix]
        ).write.format("delta").mode("overwrite").option(
            "overwriteSchema", "false"
        ).saveAsTable(table)
        observed = self._rows(suffix)
        expected = [_normalize_spark_row(row) for row in rows]
        if _canonical_rows(observed) != _canonical_rows(expected):
            raise GoldValidationError(f"{suffix} exact readback failed")


class FabricGoldJob(LocalGoldJob):
    """Reuse the proven aggregation logic with Fabric storage boundaries."""

    def __init__(
        self,
        source: FabricCommittedSource,
        store: FabricGoldStoreImpl,
        state: FabricGoldState,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.source = source
        self.store = store
        self.state = state
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.verify_output_hash = True

    def source_checkpoint(self) -> SourceCheckpoint:
        return self.source.checkpoint()

    def operations_source_checkpoint(self) -> SourceCheckpoint:
        return self.source.operations_checkpoint()

    def committed_outputs(self) -> list[CommittedOutput]:
        return self.source.committed_outputs()

    def _control_rows(self) -> dict[str, list[dict[str, Any]]]:
        return self.source.control_rows()


def _path_value(path: str, name: str) -> str:
    prefix = f"{name}="
    matches = [
        part[len(prefix):]
        for part in path.split("/")
        if part.startswith(prefix)
    ]
    if len(matches) != 1 or not matches[0]:
        raise GoldSourceError(f"output path has no unique {name} segment")
    return matches[0]


def _date_partition(value: str) -> str:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError as error:
        raise ValueError(f"invalid date partition {value!r}") from error


def _spark_timestamp(value: datetime) -> datetime:
    """Normalize timestamps to Spark's timezone-naive UTC readback form."""
    if not isinstance(value, datetime):
        raise TypeError("Fabric gold clock must return datetime")
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _normalize_spark_row_timestamp(value: Any) -> Any:
    """Interpret Spark SQL timestamp datetimes as UTC at the Fabric boundary."""
    if not isinstance(value, datetime):
        return value
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalize_spark_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        name: _normalize_spark_row_timestamp(value)
        for name, value in row.items()
    }


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
        default=lambda item: item.isoformat(),
    )


def _canonical_rows(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted(_canonical(dict(row)) for row in rows)


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()
