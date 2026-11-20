"""Fabric source, typed Gold store, and checkpoint/outbox state."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from functools import reduce
from typing import Any, Callable

from people_counter.fabric_namespace import FabricNamespaceConfig
from people_counter.fabric_control import ControlWriter
from people_counter.sjd_gold import (
    CommittedOutput,
    DIMENSION_TABLES,
    FACT_TARGET_TABLES,
    GoldSourceError,
    GoldValidationError,
    LocalGoldJob,
    SourceCheckpoint,
    _jsonable_row,
    _jsonable_spark_row,
    _records_output_document,
    _optional_datetime,
    _checkpoint_targets_match,
    _iso,
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
_SPARK_CHECKPOINT_VERSION = "spark-json-v2"


class FabricCommittedSource:
    """Read gold inputs only through authoritative committed pointers."""

    def __init__(
        self,
        spark_session: Any,
        attempt_adapter: Any,
        *,
        config: FabricNamespaceConfig | None = None,
    ) -> None:
        self.spark = spark_session
        self.attempts = attempt_adapter
        self.config = config or FabricNamespaceConfig.current()

    def checkpoint(self) -> SourceCheckpoint:
        if self.spark is None:
            rows = self._visible_rows()
            return SourceCheckpoint(
                max(
                    (
                        int(row["publication"]["publication_sequence"])
                        for row in rows
                    ),
                    default=0,
                ),
                {
                    name: _sha256([row[name] for row in rows])
                    for name in ("work", "attempt", "publication", "batch")
                },
            )
        frame = self._visible_frame()
        sequence = frame.agg(
            {"publication.publication_sequence": "max"}
        ).first()[0]
        versions = {
            name: _frame_checksum(
                frame, name, "publication.publication_sequence"
            )
            for name in ("work", "attempt", "publication", "batch")
        }
        return SourceCheckpoint(
            int(sequence or 0),
            versions,
        )

    def committed_outputs(self) -> list[CommittedOutput]:
        if self.spark is not None and self.spark.catalog.tableExists(
            self.config.table("committed_records")
        ):
            return [
                _output_from_json(str(row["output_json"]))
                for row in self.committed_output_frame().select(
                    "output_json"
                ).collect()
            ]
        result: list[CommittedOutput] = []
        for pointer in self._visible_rows():
            work = pointer["work"]
            attempt = pointer["attempt"]
            publication = pointer["publication"]
            batch = pointer["batch"]
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
            published_at = _optional_datetime(
                publication.get("published_at")
            ) or _optional_datetime(batch.get("committed_at"))
            enriched_attempt = dict(attempt) | {
                "batch_committed_at": batch.get("committed_at")
            }
            result.append(
                CommittedOutput(
                    str(work["work_id"]),
                    str(attempt["attempt_id"]),
                    int(publication["publication_sequence"]),
                    published_at,
                    dict(work) | payload,
                    enriched_attempt,
                    run,
                    tuple(line_counts),
                )
            )
        return result

    def _visible_rows(self) -> list[dict[str, dict[str, Any]]]:
        if self.spark is not None:
            return self._visible_spark_rows()
        return self._visible_python_rows()

    def _visible_frame(self):
        from pyspark.sql import functions as F

        work = self.spark.table(self._table("work")).alias("work")
        attempts = self.spark.table(self._table("attempts")).alias("attempt")
        publications = self.spark.table(
            self._table("publications")
        ).alias("publication")
        batches = self.spark.table(self._table("batches")).alias("batch")
        joined = (
            work.where(F.col("work.committed_attempt_id").isNotNull())
            .join(
                attempts,
                F.col("attempt.attempt_id")
                == F.col("work.committed_attempt_id"),
                "left",
            )
            .join(
                publications,
                (
                    F.col("publication.work_id") == F.col("work.work_id")
                )
                & (
                    F.col("publication.attempt_id")
                    == F.col("work.committed_attempt_id")
                ),
                "left",
            )
            .join(
                batches,
                F.col("batch.batch_id") == F.col("attempt.batch_id"),
                "left",
            )
            .select(
                F.struct(
                    *[F.col(f"work.{name}") for name in work.columns]
                ).alias("work"),
                F.struct(
                    *[F.col(f"attempt.{name}") for name in attempts.columns]
                ).alias("attempt"),
                F.struct(
                    *[
                        F.col(f"publication.{name}")
                        for name in publications.columns
                    ]
                ).alias("publication"),
                F.struct(
                    *[F.col(f"batch.{name}") for name in batches.columns]
                ).alias("batch"),
            )
        )
        invalid = joined.where(
            F.col("attempt.attempt_id").isNull()
            | F.col("publication.publication_sequence").isNull()
            | F.col("batch.batch_id").isNull()
            | (F.col("attempt.work_id") != F.col("work.work_id"))
            | (F.col("attempt.status") != F.lit("SUCCEEDED"))
            | (F.col("work.status") != F.lit("SUCCEEDED"))
            | (
                F.col("work.publication_sequence")
                != F.col("publication.publication_sequence")
            )
            | (
                F.col("publication.output_path")
                != F.col("attempt.output_path")
            )
            | (
                F.col("publication.output_sha256")
                != F.col("attempt.output_sha256")
            )
            | (F.col("publication.batch_id") != F.col("batch.batch_id"))
            | (F.col("batch.status") != F.lit("COMMITTED"))
        ).select("work.work_id").limit(1).first()
        if invalid is not None:
            raise GoldSourceError(
                f"invalid committed pointer for {invalid['work_id']}"
            )
        return joined

    def _visible_spark_rows(self) -> list[dict[str, dict[str, Any]]]:
        return [
            {
                name: _normalize_spark_row(value)
                for name, value in row.asDict(recursive=True).items()
            }
            for row in self._visible_frame().collect()
        ]

    def control_frame(self):
        from pyspark.sql import functions as F

        work = self.spark.table(self._table("work")).alias("work")
        attempts = self.spark.table(self._table("attempts")).alias("attempt")
        batches = self.spark.table(self._table("batches")).alias("batch")
        publications = self.spark.table(
            self._table("publications")
        ).alias("publication")
        return {
            "work": work.select("work.*"),
            "attempts": (
                attempts.join(
                    batches,
                    F.col("batch.batch_id") == F.col("attempt.batch_id"),
                    "left",
                )
                .join(
                    work,
                    F.col("work.work_id") == F.col("attempt.work_id"),
                    "left",
                )
                .join(
                    publications,
                    F.col("publication.attempt_id")
                    == F.col("attempt.attempt_id"),
                    "left",
                )
                .select(
                    F.col("attempt.*"),
                    F.col("batch.owner").alias("dispatcher_id"),
                    F.col("batch.runtime_key").alias("batch_runtime_key"),
                    F.col("batch.sealed_at").alias("batch_sealed_at"),
                    F.col("batch.committed_at").alias("batch_completed_at"),
                    F.col("batch.lease_expires_at").alias("batch_expired_at"),
                    F.col("work.updated_at").alias("work_updated_at"),
                    F.col("publication.published_at").alias("published_at"),
                )
                .orderBy("attempt.work_id", "attempt.attempt_id")
            ),
        }

    def committed_output_frame(self):
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            ArrayType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        visible = self._visible_frame()
        committed_table = self.config.table("committed_records")
        table_columns = (
            set(self.spark.table(committed_table).columns)
            if self.spark.catalog.tableExists(committed_table)
            else set()
        )
        required = {"output_schema_version", "output_json"}
        if required <= table_columns:
            pointers = visible.select(
                F.col("attempt.batch_id").alias("batch_id"),
                F.col("work.work_id").alias("work_id"),
                F.col("attempt.attempt_id").alias("attempt_id"),
            )
            materialized = (
                pointers.join(
                    self.spark.table(committed_table).select(
                        "batch_id",
                        "work_id",
                        "attempt_id",
                        "output_schema_version",
                        "output_json",
                    ),
                    ["batch_id", "work_id", "attempt_id"],
                    "inner",
                )
                .groupBy("batch_id", "work_id", "attempt_id")
                .agg(
                    F.count("*").alias("_materialized_records"),
                    F.countDistinct("output_schema_version").alias(
                        "_schema_versions"
                    ),
                    F.countDistinct("output_json").alias("_output_versions"),
                    F.min("output_schema_version").alias(
                        "output_schema_version"
                    ),
                    F.min("output_json").alias("_materialized_output_json"),
                    F.sum(
                        F.when(
                            F.col("output_schema_version").isNull()
                            | F.col("output_json").isNull(),
                            1,
                        ).otherwise(0)
                    ).alias("_missing"),
                )
            )
        else:
            # Legacy Spark/temporary-table compatibility. Canonical bytes are
            # authenticated natively from the immutable record_json rows.
            records = self._record_frame(visible)
            grouped = records.groupBy("work_id", "attempt_id").agg(
                F.sort_array(
                    F.collect_list(
                        F.struct(
                            "record_type",
                            "record_sequence",
                            "record_json",
                        )
                    )
                ).alias("_records")
            )
            record_schema = StructType(
                [
                    StructField("work_id", StringType()),
                    StructField("attempt_id", StringType()),
                    StructField("record_type", StringType()),
                    StructField("record_sequence", LongType()),
                    StructField("payload_json", StringType()),
                    StructField("processing_seconds", DoubleType()),
                    StructField("processed_frames", LongType()),
                    StructField("status", StringType()),
                ]
            )
            parsed_records = F.transform(
                grouped["_records"],
                lambda item: F.from_json(
                    item["record_json"], record_schema, {"mode": "FAILFAST"}
                ).withField("record_sequence", item["record_sequence"])
                .withField("record_type", item["record_type"]),
            )
            terminal_payload_schema = StructType(
                [
                    StructField("captured_at_utc", StringType()),
                    StructField("camera_id", StringType()),
                    StructField("location_id", StringType()),
                    StructField("camera_timezone", StringType()),
                    StructField("config_sha256", StringType()),
                    StructField("asset_id", StringType()),
                    StructField("asset_version", StringType()),
                    StructField("duration_seconds", DoubleType()),
                    StructField("video_duration_seconds", DoubleType()),
                    StructField("processing_seconds", DoubleType()),
                    StructField("processed_frames", LongType()),
                    StructField("distinct_people", LongType()),
                    StructField("line_in_count", LongType()),
                    StructField("line_out_count", LongType()),
                    StructField("completed_at", StringType()),
                ]
            )
            legacy_terminal = F.element_at(
                F.filter(
                    parsed_records,
                    lambda item: item["record_type"].isin(
                        "video_result", "error"
                    ),
                ),
                1,
            )
            lines = F.transform(
                F.filter(
                    parsed_records,
                    lambda item: item["record_type"] == "line_count",
                ),
                lambda item: F.from_json(
                    item["payload_json"],
                    "struct<observed_at_utc:string,video_seconds:double,"
                    "frame_in_count:long,frame_out_count:long,entries:long,"
                    "exits:long,camera_id:string,location_id:string>",
                    {"mode": "FAILFAST"},
                ).withField("work_id", grouped["work_id"])
                .withField("attempt_id", grouped["attempt_id"]),
            )
            run = (
                F.from_json(
                    legacy_terminal["payload_json"],
                    terminal_payload_schema,
                    {"mode": "FAILFAST"},
                )
                .withField("work_id", grouped["work_id"])
                .withField("attempt_id", grouped["attempt_id"])
                .withField(
                    "processing_seconds",
                    legacy_terminal["processing_seconds"],
                )
                .withField(
                    "processed_frames", legacy_terminal["processed_frames"]
                )
                .withField("status", legacy_terminal["status"])
            )
            materialized = grouped.select(
                "work_id",
                "attempt_id",
                F.lit(1).alias("output_schema_version"),
                F.lit(0).alias("_materialized_records"),
                F.lit(1).alias("_schema_versions"),
                F.lit(1).alias("_output_versions"),
                F.lit(0).alias("_missing"),
                F.to_json(
                    F.struct(
                        F.lit(1).alias("schema_version"),
                        grouped["work_id"],
                        grouped["attempt_id"],
                        F.sha2(
                            F.concat(
                                F.lit("["),
                                F.concat_ws(
                                    ",",
                                    F.transform(
                                        grouped["_records"],
                                        lambda item: item["record_json"],
                                    ),
                                ),
                                F.lit("]"),
                            ),
                            256,
                        ).alias("output_sha256"),
                        run.alias("run"),
                        lines.alias("line_counts"),
                    )
                ).alias("_materialized_output_json"),
            )
        joined = visible.join(
            materialized,
            (F.col("work.work_id") == materialized["work_id"])
            & (F.col("attempt.attempt_id") == materialized["attempt_id"]),
            "left",
        )
        invalid = (
            joined.where(
                F.col("_materialized_output_json").isNull()
                | (F.col("_missing") != 0)
                | (F.col("_schema_versions") != 1)
                | (F.col("_output_versions") != 1)
                | (F.col("output_schema_version") != 1)
                | (
                    F.get_json_object(
                        F.col("_materialized_output_json"),
                        "$.schema_version",
                    ).cast("int")
                    != 1
                )
                | (
                    F.get_json_object(
                        F.col("_materialized_output_json"), "$.work_id"
                    )
                    != F.col("work.work_id")
                )
                | (
                    F.get_json_object(
                        F.col("_materialized_output_json"), "$.attempt_id"
                    )
                    != F.col("attempt.attempt_id")
                )
                | (
                    F.get_json_object(
                        F.col("_materialized_output_json"),
                        "$.output_sha256",
                    )
                    != F.col("publication.output_sha256")
                )
            )
            .select("work.work_id")
            .limit(1)
            .first()
        )
        if invalid is not None:
            raise GoldSourceError(
                f"committed output materialization is invalid for "
                f"{invalid['work_id']}; run committed process recovery "
                "to backfill the attempt"
            )
        body_schema = StructType(
            [
                StructField("schema_version", LongType(), False),
                StructField("work_id", StringType(), False),
                StructField("attempt_id", StringType(), False),
                StructField("output_sha256", StringType(), False),
                StructField(
                    "run",
                    StructType(
                        [
                            StructField("work_id", StringType()),
                            StructField("attempt_id", StringType()),
                            StructField("captured_at_utc", StringType()),
                            StructField("camera_id", StringType()),
                            StructField("location_id", StringType()),
                            StructField("camera_timezone", StringType()),
                            StructField("config_sha256", StringType()),
                            StructField("asset_id", StringType()),
                            StructField("asset_version", StringType()),
                            StructField("duration_seconds", DoubleType()),
                            StructField(
                                "video_duration_seconds", DoubleType()
                            ),
                            StructField("processing_seconds", DoubleType()),
                            StructField("processed_frames", LongType()),
                            StructField("distinct_people", LongType()),
                            StructField("line_in_count", LongType()),
                            StructField("line_out_count", LongType()),
                            StructField("completed_at", StringType()),
                            StructField("status", StringType()),
                        ]
                    ),
                    False,
                ),
                StructField(
                    "line_counts",
                    ArrayType(
                        StructType(
                            [
                                StructField("work_id", StringType()),
                                StructField("attempt_id", StringType()),
                                StructField("observed_at_utc", StringType()),
                                StructField("video_seconds", DoubleType()),
                                StructField("frame_in_count", LongType()),
                                StructField("frame_out_count", LongType()),
                                StructField("entries", LongType()),
                                StructField("exits", LongType()),
                                StructField("camera_id", StringType()),
                                StructField("location_id", StringType()),
                            ]
                        )
                    ),
                    False,
                ),
            ]
        )
        payload_schema = StructType(
            [
                StructField("camera_id", StringType()),
                StructField("location_id", StringType()),
                StructField("captured_at_utc", StringType()),
                StructField("camera_timezone", StringType()),
                StructField("config_sha256", StringType()),
                StructField("config_json", StringType()),
                StructField("asset_id", StringType()),
                StructField("asset_version", StringType()),
                StructField("duration_seconds", DoubleType()),
                StructField("video_duration_seconds", DoubleType()),
                StructField("distinct_people", LongType()),
                StructField("line_in_count", LongType()),
                StructField("line_out_count", LongType()),
            ]
        )
        enriched = joined.withColumn(
            "_materialized",
            F.from_json(
                "_materialized_output_json",
                body_schema,
                {"mode": "FAILFAST"},
            ),
        )
        payload = F.from_json(
            F.col("work.payload_json"), payload_schema, {"mode": "FAILFAST"}
        )
        materialized_json = F.parse_json(F.col("_materialized_output_json"))
        published_at = F.coalesce(
            F.col("publication.published_at"),
            F.col("batch.committed_at"),
        )
        output = F.struct(
            F.col("work.work_id").alias("work_id"),
            F.col("attempt.attempt_id").alias("attempt_id"),
            F.col("publication.publication_sequence").alias(
                "publication_sequence"
            ),
            F.when(
                published_at.isNull(), F.lit(None)
            ).otherwise(
                F.date_format(published_at, "yyyy-MM-dd'T'HH:mm:ssXXX")
            ).alias("published_at"),
            F.struct(
                F.col("work.work_id").alias("work_id"),
                payload["camera_id"].alias("camera_id"),
                payload["location_id"].alias("location_id"),
                payload["captured_at_utc"].alias("captured_at_utc"),
                payload["camera_timezone"].alias("camera_timezone"),
                F.coalesce(
                    payload["config_sha256"],
                    F.col("work.config_sha256")
                    if "config_sha256"
                    in {
                        field.name
                        for field in enriched.schema["work"].dataType.fields
                    }
                    else F.lit(None),
                ).alias("config_sha256"),
                payload["config_json"].alias("config_json"),
                payload["asset_id"].alias("asset_id"),
                payload["asset_version"].alias("asset_version"),
                payload["duration_seconds"].alias("duration_seconds"),
                payload["video_duration_seconds"].alias(
                    "video_duration_seconds"
                ),
                payload["distinct_people"].alias("distinct_people"),
                payload["line_in_count"].alias("line_in_count"),
                payload["line_out_count"].alias("line_out_count"),
                F.col("work.payload_json").alias("payload_json"),
            ).alias("work"),
            F.struct(
                F.col("attempt.attempt_id").alias("attempt_id"),
                F.col("attempt.work_id").alias("work_id"),
                F.col("attempt.batch_id").alias("batch_id"),
                F.col("attempt.output_path").alias("output_path"),
                F.col("attempt.output_sha256").alias("output_sha256"),
                F.col("batch.committed_at").alias("batch_committed_at"),
            ).alias("attempt"),
            F.variant_get(
                materialized_json, "$.run", "variant"
            ).alias("run"),
            F.variant_get(
                materialized_json, "$.line_counts", "variant"
            ).alias("line_counts"),
        )
        return enriched.select(
            F.col("work.work_id").alias("work_id"),
            F.col("attempt.attempt_id").alias("attempt_id"),
            F.col("publication.publication_sequence").alias(
                "publication_sequence"
            ),
            F.to_json(output).alias("output_json"),
        )

    def _record_frame(self, visible):
        from pyspark.sql import functions as F

        committed_table = self.config.table("committed_records")
        if self.spark.catalog.tableExists(committed_table):
            pointers = visible.select(
                F.col("attempt.batch_id").alias("batch_id"),
                F.col("work.work_id").alias("work_id"),
                F.col("attempt.attempt_id").alias("attempt_id"),
            )
            return (
                pointers.join(
                    self.spark.table(committed_table).select(
                        "batch_id",
                        "work_id",
                        "attempt_id",
                        "record_type",
                        "record_sequence",
                        "record_json",
                    ),
                    ["batch_id", "work_id", "attempt_id"],
                    "inner",
                )
                .select(
                    "work_id",
                    "attempt_id",
                    "record_type",
                    "record_sequence",
                    "record_json",
                )
            )
        pointers = (
            visible.orderBy("publication.publication_sequence")
            .select(
                "work.work_id",
                "attempt.attempt_id",
                "publication.output_path",
                "attempt.batch_id",
            )
            .collect()
        )
        if not hasattr(self.attempts, "spark_session"):
            rows = []
            for pointer in pointers:
                batch_id = str(pointer["batch_id"])
                process_attempt = _path_value(
                    str(pointer["output_path"]), "attempt"
                )
                for record in self.attempts.read_records(
                    batch_id, process_attempt
                ):
                    rows.append({
                        **record,
                        "record_json": json.dumps(
                            record, sort_keys=True, separators=(",", ":")
                        ),
                    })
            schema = (
                "work_id string, attempt_id string, record_type string, "
                "record_sequence long, record_json string"
            )
            return self.spark.createDataFrame(rows, schema=schema)
        paths = []
        for pointer in pointers:
            path = self.attempts.attempt_path(
                str(pointer["batch_id"]),
                _path_value(str(pointer["output_path"]), "attempt"),
            )
            spark_path = (
                self.config.abfss_path(str(path))
                if hasattr(self.config, "abfss_path")
                else str(path)
            )
            paths.append(spark_path)
        if not paths:
            schema = (
                "work_id string, attempt_id string, record_type string, "
                "record_sequence long, record_json string"
            )
            return self.spark.createDataFrame([], schema=schema)
        frames = [
            self.spark.read.format("delta").load(path).select(
                "work_id",
                "attempt_id",
                "record_type",
                "record_sequence",
                "record_json",
            )
            for path in paths
        ]
        return reduce(
            lambda left, right: left.unionByName(right), frames
        )

    def _visible_python_rows(self) -> list[dict[str, dict[str, Any]]]:
        work = self._rows("work")
        attempts = {
            row["attempt_id"]: row for row in self._rows("attempts")
        }
        batches = {
            row["batch_id"]: row for row in self._rows("batches")
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
            batch = (
                None
                if attempt is None
                else batches.get(attempt.get("batch_id"))
            )
            if (
                attempt is None
                or publication is None
                or batch is None
                or attempt["work_id"] != row["work_id"]
                or attempt["status"] != "SUCCEEDED"
                or row["status"] != "SUCCEEDED"
                or row["publication_sequence"]
                != publication["publication_sequence"]
                or publication["output_path"] != attempt["output_path"]
                or publication["output_sha256"] != attempt["output_sha256"]
                or publication["batch_id"] != batch["batch_id"]
                or batch["status"] != "COMMITTED"
            ):
                raise GoldSourceError(
                    f"invalid committed pointer for {row['work_id']}"
                )
            visible.append(
                {
                    "work": row,
                    "attempt": attempt,
                    "publication": publication,
                    "batch": batch,
                }
            )
        return sorted(
            visible,
            key=lambda item: int(
                item["publication"]["publication_sequence"]
            ),
        )

    def _rows(self, suffix: str) -> list[dict[str, Any]]:
        name = self._table(suffix)
        self.spark.catalog.refreshTable(name)
        return [
            _normalize_spark_row(row.asDict(recursive=True))
            for row in self.spark.table(name).collect()
        ]

    def _table(self, suffix: str) -> str:
        return self.config.table(suffix)

    def control_rows(self) -> dict[str, list[dict[str, Any]]]:
        if self.spark is not None:
            return self._control_spark_rows()
        return self._control_python_rows()

    def _control_spark_rows(self) -> dict[str, list[dict[str, Any]]]:
        from pyspark.sql import functions as F

        work = self.spark.table(self._table("work")).alias("work")
        attempts = self.spark.table(self._table("attempts")).alias("attempt")
        batches = self.spark.table(self._table("batches")).alias("batch")
        publications = self.spark.table(
            self._table("publications")
        ).alias("publication")
        projected = (
            attempts.join(
                batches,
                F.col("batch.batch_id") == F.col("attempt.batch_id"),
                "left",
            )
            .join(
                work,
                F.col("work.work_id") == F.col("attempt.work_id"),
                "left",
            )
            .join(
                publications,
                F.col("publication.attempt_id")
                == F.col("attempt.attempt_id"),
                "left",
            )
            .select(
                F.col("attempt.*"),
                F.col("batch.owner").alias("dispatcher_id"),
                F.col("batch.runtime_key").alias("batch_runtime_key"),
                F.col("batch.sealed_at").alias("batch_sealed_at"),
                F.col("batch.committed_at").alias("batch_completed_at"),
                F.col("batch.lease_expires_at").alias("batch_expired_at"),
                F.col("work.updated_at").alias("work_updated_at"),
                F.col("publication.published_at").alias("published_at"),
            )
            .orderBy("attempt.work_id", "attempt.attempt_id")
        )
        return {
            "work": [
                _normalize_spark_row(row.asDict(recursive=True))
                for row in work.collect()
            ],
            "attempts": [
                _normalize_spark_row(row.asDict(recursive=True))
                for row in projected.collect()
            ],
        }

    def _control_python_rows(self) -> dict[str, list[dict[str, Any]]]:
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
                    "dispatcher_id": batch.get("owner"),
                    "batch_runtime_key": batch.get("runtime_key"),
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
        if self.spark is not None:
            names = ("work", "attempts", "batches", "publications")
            versions = {name: self._frame_version(name) for name in names}
            publications = self.spark.table(self._table("publications"))
            sequence = publications.agg(
                {"publication_sequence": "max"}
            ).first()[0]
            return SourceCheckpoint(
                int(sequence or 0),
                versions,
            )
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

    def _frame_version(self, suffix: str) -> str:
        frame = self.spark.table(self._table(suffix))
        return _spark_frame_checksum(
            frame,
            schema_version=_SPARK_CHECKPOINT_VERSION,
        )


class FabricGoldStoreImpl:
    """Typed Delta catalog tables with exact partition-replace readback."""

    def __init__(
        self,
        spark_session: Any,
        *,
        config: FabricNamespaceConfig | None = None,
    ) -> None:
        self.spark = spark_session
        self.config = config or FabricNamespaceConfig.current()
        self.writer = ControlWriter(
            spark_session, self.config.table("locks")
        )

    def bootstrap(self) -> None:
        for name in FACT_TARGET_TABLES + DIMENSION_TABLES:
            table = self._table(name)
            if self.spark.catalog.tableExists(table):
                continue
            frame = self.spark.createDataFrame(
                [],
                schema=_spark_schema(name),
            )
            frame.write.format("delta").mode("errorifexists").saveAsTable(
                table
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

    def _frame(self, name: str):
        table = self._table(name)
        if not self.spark.catalog.tableExists(table):
            return self.spark.createDataFrame([], schema=_spark_schema(name))
        self.spark.catalog.refreshTable(table)
        return self.spark.table(table)

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

    def fact_dates(self) -> dict[str, list[dict[str, Any]]]:
        columns = {
            "gold_flow_minute": "flow_date",
            "gold_flow_hour": "flow_date",
            "gold_video": "capture_date",
            "gold_operations_hour": "operation_date",
        }
        return {
            table: [
                row.asDict()
                for row in (
                    self._frame(table)
                    .select(column)
                    .where(f"{column} IS NOT NULL")
                    .distinct()
                    .collect()
                )
            ]
            for table, column in columns.items()
        }

    def validation_summary(self) -> dict[str, Any]:
        from pyspark.sql import functions as F

        primary_keys = {
            "gold_work_operations": "work_id",
            "gold_attempt_operations": "attempt_id",
            "gold_dim_date": "date_key",
            "gold_dim_time": "time_key",
            "gold_dim_camera": "camera_id",
            "gold_dim_location": "location_id",
            "gold_dim_video": "work_id",
            "gold_dim_model_config": "config_sha256",
        }
        frames = {
            name: self._frame(name)
            for name in FACT_TARGET_TABLES + DIMENSION_TABLES
        }
        counts = {name: frame.count() for name, frame in frames.items()}
        for table, key in primary_keys.items():
            frame = frames[table]
            duplicate = (
                frame.groupBy(key)
                .agg(F.count(F.lit(1)).alias("_count"))
                .where(F.col(key).isNull() | (F.col("_count") > 1))
                .limit(1)
                .first()
            )
            if duplicate is not None:
                raise GoldValidationError(
                    f"{table}.{key} must be non-null and unique"
                )
        work_ids = frames["gold_work_operations"].select("work_id")
        missing_attempts = self._missing_values(
            frames["gold_attempt_operations"],
            "work_id",
            work_ids,
            "work_id",
        )
        if missing_attempts:
            raise GoldValidationError(
                "gold_attempt_operations.work_id has unresolved "
                f"gold_work_operations keys: {missing_attempts}"
            )
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
            (
                "gold_video",
                "config_sha256",
                "gold_dim_model_config",
                "config_sha256",
            ),
            ("gold_video", "work_id", "gold_dim_video", "work_id"),
        )
        for fact_table, fact_key, dimension_table, dimension_key in relationships:
            missing = self._missing_values(
                frames[fact_table],
                fact_key,
                frames[dimension_table].select(dimension_key),
                dimension_key,
            )
            if missing:
                raise GoldValidationError(
                    f"{fact_table}.{fact_key} has unresolved "
                    f"{dimension_table} keys: {missing}"
                )
        return {
            "valid": True,
            "rules_checked": len(primary_keys) + len(relationships) + 1,
            "table_rows": counts,
        }

    @staticmethod
    def _missing_values(
        fact: Any, fact_key: str, dimension: Any, dimension_key: str
    ) -> list[Any]:
        rows = (
            fact.select(fact_key)
            .distinct()
            .join(
                dimension.select(dimension_key).distinct(),
                fact[fact_key] == dimension[dimension_key],
                "left_anti",
            )
            .orderBy(fact_key)
            .limit(10)
            .collect()
        )
        return [_jsonable_spark_row(row.asDict())[fact_key] for row in rows]

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
            observed = self._frame(name).where(
                f"{partition_column} = DATE "
                f"'{_date_partition(partition_value)}'"
            )
            if not _frames_equal(observed, frame):
                raise GoldValidationError(
                    f"{name} partition exact readback failed"
                )
            return len(normalized)

        return self.writer.run(operation)

    def replace_partition_frame(
        self,
        name: str,
        partition_column: str,
        partition_value: str,
        frame: Any,
    ) -> int:
        from pyspark.sql import functions as F

        frame = frame.select(
            *[
                F.col(field.name).cast(field.dataType).alias(field.name)
                for field in _spark_schema(name).fields
            ]
        )
        if frame.where(
            f"{partition_column} != DATE '{_date_partition(partition_value)}'"
        ).limit(1).first() is not None:
            raise ValueError(
                f"{name} replacement contains rows outside "
                f"{partition_column}={partition_value}"
            )
        row_count = frame.count()

        def operation() -> int:
            table = self._table(name)
            predicate = (
                f"{partition_column} = DATE "
                f"'{_date_partition(partition_value)}'"
            )
            if row_count:
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
            observed = self._frame(name).where(predicate)
            if not _frames_equal(observed, frame):
                raise GoldValidationError(
                    f"{name} partition exact readback failed"
                )
            return row_count

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
            if not _frames_equal(self._frame(name), frame):
                raise GoldValidationError(
                    f"{name} exact readback/cardinality failed"
                )
            return len(normalized)

        return self.writer.run(operation)

    def replace_table_frame(self, name: str, frame: Any) -> int:
        from pyspark.sql import functions as F

        frame = frame.select(
            *[
                F.col(field.name).cast(field.dataType).alias(field.name)
                for field in _spark_schema(name).fields
            ]
        )
        row_count = frame.count()

        def operation() -> int:
            frame.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "false"
            ).saveAsTable(self._table(name))
            if not _frames_equal(self._frame(name), frame):
                raise GoldValidationError(
                    f"{name} exact readback/cardinality failed"
                )
            return row_count

        return self.writer.run(operation)

    def _table(self, name: str) -> str:
        if not name.startswith("gold_"):
            raise ValueError(f"not a Fabric SJD gold table: {name!r}")
        return self.config.table(name)


class FabricGoldState:
    """Delta checkpoint/outbox state serialized with the control writer."""

    def __init__(
        self,
        spark_session: Any,
        *,
        config: FabricNamespaceConfig | None = None,
        clock: Callable[[], datetime] | None = None,
        auto_bootstrap: bool = True,
    ) -> None:
        self.spark = spark_session
        self.config = config or FabricNamespaceConfig.current()
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
        matches = self._target_rows("gold_checkpoints", stage=stage, limit=2)
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
            self._validate_unique("gold_checkpoints", ("stage",))
            self._validate_unique(
                "semantic_refresh_outbox", ("outbox_id",)
            )
            checkpoint_rows = []
            for stage, (source, versions) in checkpoints.items():
                checkpoint_rows.append({
                    "stage": stage,
                    "source_key": source.key,
                    "publication_sequence": source.publication_sequence,
                    "source_versions_json": _canonical(source.versions),
                    "target_versions_json": _canonical(dict(versions)),
                    "completed_at": now,
                })
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
                existing = self._target_rows(
                    "semantic_refresh_outbox", dedupe_key=dedupe, limit=2
                )
                if len(existing) > 1:
                    raise GoldValidationError(
                        "outbox dedupe identity is not unique"
                    )
                if not existing:
                    outbox_id = self._next_outbox_id()
                    self._merge(
                        "semantic_refresh_outbox",
                        [{
                            "outbox_id": outbox_id,
                            "dedupe_key": dedupe,
                            "reason": reason,
                            "payload_json": _canonical(payload),
                            "created_at": now,
                            "acked_at": None,
                            "acked_by": None,
                        }],
                        ("outbox_id",),
                    )
                else:
                    outbox_id = int(existing[0]["outbox_id"])
            if checkpoint_rows:
                self._merge("gold_checkpoints", checkpoint_rows, ("stage",))
            return outbox_id

        return self.writer.run(operation)

    def pending_refreshes(self) -> list[dict[str, Any]]:
        from pyspark.sql import functions as F

        frame = self.spark.table(
            self.config.table("semantic_refresh_outbox")
        ).where(F.col("acked_at").isNull()).orderBy("outbox_id")
        return [
            dict(row) | {"payload": json.loads(row["payload_json"])}
            for row in (
                _normalize_spark_row(item.asDict(recursive=True))
                for item in frame.collect()
            )
        ]

    def acknowledge_refresh(
        self,
        outbox_id: int,
        actor: str = "fabric",
        *,
        expected_dedupe_key: str | None = None,
    ) -> bool:
        def operation() -> bool:
            filters: dict[str, Any] = {"outbox_id": outbox_id}
            if expected_dedupe_key is not None:
                filters["dedupe_key"] = expected_dedupe_key
            rows = self._target_rows(
                "semantic_refresh_outbox", limit=2, **filters
            )
            eligible = [row for row in rows if row["acked_at"] is None]
            if len(rows) > 1:
                raise GoldValidationError("outbox identity is not unique")
            self._validate_unique(
                "semantic_refresh_outbox", ("outbox_id",)
            )
            if not eligible:
                return False
            from delta.tables import DeltaTable
            from pyspark.sql import functions as F

            DeltaTable.forName(
                self.spark, self.config.table("semantic_refresh_outbox")
            ).update(
                (F.col("outbox_id") == F.lit(outbox_id))
                & F.col("acked_at").isNull()
                & (
                    F.lit(True)
                    if expected_dedupe_key is None
                    else F.col("dedupe_key") == F.lit(expected_dedupe_key)
                ),
                {
                    "acked_at": F.lit(_spark_timestamp(self._clock())),
                    "acked_by": F.lit(actor),
                },
            )
            return True

        return self.writer.run(operation)

    def _rows(self, suffix: str) -> list[dict[str, Any]]:
        table = self.config.table(suffix)
        self.spark.catalog.refreshTable(table)
        return [
            _normalize_spark_row(row.asDict(recursive=True))
            for row in self.spark.table(table).collect()
        ]

    def _target_rows(
        self, suffix: str, *, limit: int, **values: Any
    ) -> list[dict[str, Any]]:
        from pyspark.sql import functions as F

        frame = self.spark.table(self.config.table(suffix))
        if not hasattr(frame, "where"):
            rows = self._rows(suffix)
            return [
                row
                for row in rows
                if all(row.get(name) == value for name, value in values.items())
            ][:limit]
        for name, value in values.items():
            frame = frame.where(F.col(name) == F.lit(value))
        return [
            _normalize_spark_row(row.asDict(recursive=True))
            for row in frame.limit(limit).collect()
        ]

    def _validate_unique(self, suffix: str, keys: tuple[str, ...]) -> None:
        from pyspark.sql import functions as F

        duplicate = (
            self.spark.table(self.config.table(suffix))
            .groupBy(*keys)
            .agg(F.count(F.lit(1)).alias("_count"))
            .where(F.col("_count") > 1)
            .limit(1)
            .first()
        )
        if duplicate is not None:
            label = "gold checkpoint stage" if suffix == "gold_checkpoints" else suffix
            raise GoldValidationError(f"duplicate {label} identity")

    def _next_outbox_id(self) -> int:
        from pyspark.sql import functions as F

        row = (
            self.spark.table(self.config.table("semantic_refresh_outbox"))
            .agg(F.max("outbox_id").alias("last_id"))
            .first()
        )
        return int(row["last_id"] or 0) + 1

    def _merge(
        self,
        suffix: str,
        rows: list[dict[str, Any]],
        keys: tuple[str, ...],
    ) -> None:
        from delta.tables import DeltaTable

        frame = self.spark.createDataFrame(
            rows, schema=_STATE_SCHEMAS[suffix]
        )
        condition = " AND ".join(
            f"target.`{key}` = source.`{key}`" for key in keys
        )
        (
            DeltaTable.forName(self.spark, self.config.table(suffix))
            .alias("target")
            .merge(frame.alias("source"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

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
        self._native_outputs = None
        self._native_control = None
        self._native_scope = False

    def run(self, **kwargs: Any) -> dict[str, Any]:
        self._clear_native_frames()
        self._native_scope = True
        try:
            return super().run(**kwargs)
        finally:
            self._native_scope = False
            self._clear_native_frames()

    def source_checkpoint(self) -> SourceCheckpoint:
        return self.source.checkpoint()

    def operations_source_checkpoint(self) -> SourceCheckpoint:
        return self.source.operations_checkpoint()

    def committed_outputs(self) -> list[CommittedOutput]:
        return self.source.committed_outputs()

    def _control_rows(self) -> dict[str, list[dict[str, Any]]]:
        return self.source.control_rows()

    def plan(
        self,
        *,
        lookback_hours: int = 48,
        now: datetime | None = None,
        full_rebuild: bool = False,
        reset: bool = False,
    ) -> Any:
        if not self._native_store:
            return super().plan(
                lookback_hours=lookback_hours,
                now=now,
                full_rebuild=full_rebuild,
                reset=reset,
            )
        if type(lookback_hours) is not int or lookback_hours < 1:
            raise ValueError("lookback_hours must be at least 1")
        from datetime import timedelta
        from people_counter.sjd_gold import GoldPlan, _required_datetime, _utc
        from pyspark.sql import functions as F

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
        outputs = self._output_frame()
        parsed = _parsed_output_frame(outputs)
        # Line observations can move an otherwise-undated output into several
        # partitions, so expand their structured array without Python.
        output_dates = (
            parsed.select(
                "publication_sequence",
                "published_at",
                "_captured_at",
                F.explode_outer("_line_counts").alias("_line"),
            )
            .withColumn(
                "_line_observed_at",
                F.when(
                    F.col("_line").isNotNull(),
                    F.coalesce(
                        F.to_timestamp(F.col("_line.observed_at_utc")),
                        F.timestamp_seconds(
                            F.unix_micros("_captured_at")
                            / F.lit(1_000_000.0)
                            + F.coalesce(
                                F.col("_line.video_seconds"), F.lit(0.0)
                            )
                        ),
                    ),
                ),
            )
            .select(
                "publication_sequence",
                "published_at",
                F.explode(
                    F.array_distinct(
                        F.array_compact(
                            F.array(
                                F.date_format(
                                    "_captured_at", "yyyy-MM-dd"
                                ),
                                F.date_format(
                                    "_line_observed_at", "yyyy-MM-dd"
                                ),
                            )
                        )
                    )
                ).alias("partition_date"),
            )
        ).select(
            "publication_sequence",
            "partition_date",
            "published_at",
        )
        control_dates = self._control_dates_frame(
            self.source.control_frame()
        )
        if all_history:
            selected = output_dates.select("partition_date").union(
                control_dates.select("partition_date")
            )
        else:
            prior_sequence = int(previous_flow["publication_sequence"])
            recent_outputs = output_dates.where(
                (F.col("publication_sequence") > prior_sequence)
                | (
                    F.col("published_at").isNotNull()
                    & (
                        F.to_timestamp("published_at")
                        >= F.lit(cutoff.replace(tzinfo=None))
                    )
                )
            )
            prior_completed = _required_datetime(
                previous_operations["completed_at"]
            )
            control_cutoff = min(
                cutoff,
                prior_completed - timedelta(hours=lookback_hours),
            )
            recent_control = control_dates.where(
                F.to_timestamp("partition_timestamp")
                >= F.lit(control_cutoff.replace(tzinfo=None))
            )
            if (
                previous_flow["source_key"] != flow_source.key
                and recent_outputs.where(
                    F.col("publication_sequence") > prior_sequence
                ).limit(1).first()
                is None
            ):
                recent_outputs = output_dates
            if (
                previous_operations["source_key"] != operations_source.key
                and recent_control.limit(1).first() is None
            ):
                recent_control = control_dates
            selected = recent_outputs.select("partition_date").union(
                recent_control.select("partition_date")
            )
        items = tuple(
            row["partition_date"]
            for row in selected.distinct().orderBy("partition_date").collect()
        )
        return GoldPlan(
            planned_at=_iso(planned_at),
            cutoff=_iso(cutoff),
            lookback_hours=lookback_hours,
            items=items,
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
        save_state: bool = True,
    ) -> dict[str, Any]:
        if not self._native_store:
            return super().build_facts(
                dates=dates,
                lookback_hours=lookback_hours,
                force=force,
                full_rebuild=full_rebuild,
                save_state=save_state,
            )
        if not self._native_scope:
            self._clear_native_frames()
        from people_counter.sjd_gold import (
            FACT_TABLES,
            OPERATIONAL_TABLES,
            _date_string,
            _normal_incremental_facts_noop,
        )

        flow_source = self.source_checkpoint()
        operations_source = self.operations_source_checkpoint()
        source = self._fact_source(flow_source, operations_source)
        previous = self.state.checkpoint("facts")
        automatic = dates is None
        current_versions = self.store.versions(FACT_TARGET_TABLES)
        targets_current = _checkpoint_targets_match(
            previous, current_versions
        )
        if _normal_incremental_facts_noop(
            automatic=automatic,
            force=force,
            full_rebuild=full_rebuild,
            checkpoint=previous,
            source_key=source.key,
            targets_current=targets_current,
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
        outputs = self._output_frame()
        refreshed = _iso(self._clock())
        frames = self._fact_frames(
            outputs, self._control_frame(), selected, refreshed
        )
        totals = {name: 0 for name in FACT_TABLES}
        for partition_date in selected:
            for table, column in (
                ("gold_flow_minute", "flow_date"),
                ("gold_flow_hour", "flow_date"),
                ("gold_video", "capture_date"),
                ("gold_operations_hour", "operation_date"),
            ):
                totals[table] += self.store.replace_partition_frame(
                    table,
                    column,
                    partition_date,
                    frames[table].where(
                        f"{column} = DATE "
                        f"'{_date_partition(partition_date)}'"
                    ),
                )
        for table in OPERATIONAL_TABLES:
            totals[table] = self.store.replace_table_frame(
                table, frames[table]
            )
        target_versions = self.store.versions(FACT_TARGET_TABLES)
        checkpoints = {
            f"facts:partition:{partition_date}": (source, target_versions)
            for partition_date in selected
        }
        complete_current = False
        if automatic:
            complete_current = (
                self.source_checkpoint().key == flow_source.key
                and self.operations_source_checkpoint().key
                == operations_source.key
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
        outbox_id = (
            self.state.save_checkpoints_and_enqueue(
                checkpoints,
                "gold facts changed" if selected else None,
                source,
                target_versions,
            )
            if save_state
            else None
        )
        result = {
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
        if not save_state:
            result["_checkpoint_updates"] = checkpoints
        return result

    @property
    def _native_store(self) -> bool:
        return hasattr(self.store, "replace_partition_frame")

    def _output_frame(self):
        if self._native_outputs is None:
            self._native_outputs = self.source.committed_output_frame().persist()
        return self._native_outputs

    def _control_frame(self):
        if self._native_control is None:
            self._native_control = {
                name: frame.persist()
                for name, frame in self.source.control_frame().items()
            }
        return self._native_control

    def _clear_native_frames(self) -> None:
        if self._native_outputs is not None:
            self._native_outputs.unpersist()
        for frame in (self._native_control or {}).values():
            frame.unpersist()
        self._native_outputs = None
        self._native_control = None

    def _control_dates_frame(self, control):
        from pyspark.sql import functions as F

        work_names = (
            "queued_at",
            "queue_entered_at",
            "created_at",
            "available_at",
            "updated_at",
        )
        attempt_names = (
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
        )
        frames = []
        for table, candidates, source_frame in (
            ("work", work_names, control["work"]),
            ("attempts", attempt_names, control["attempts"]),
        ):
            available = set(source_frame.columns)
            values = [
                F.col(name).cast("timestamp")
                for name in candidates
                if name in available
            ]
            frame = source_frame.select(
                F.explode(F.array_compact(F.array(*values))).alias(
                    "partition_timestamp"
                )
            )
            if table == "attempts" and "batch_expired_at" in available:
                frame = source_frame.where(
                    F.col("batch_expired_at").isNull()
                    | (F.col("status") == "EXPIRED")
                ).select(
                    F.explode(
                        F.array_compact(
                            F.array(
                                *[
                                    F.col(name).cast("timestamp")
                                    for name in candidates
                                    if name in available
                                ]
                            )
                        )
                    ).alias("partition_timestamp")
                )
            frames.append(frame)
        combined = frames[0].unionByName(frames[1]).where(
            F.col("partition_timestamp").isNotNull()
        )
        return combined.select(
            F.date_format("partition_timestamp", "yyyy-MM-dd").alias(
                "partition_date"
            ),
            "partition_timestamp",
        )

    def _fact_frames(self, outputs, control, dates, refreshed_at):
        from pyspark.sql import functions as F

        parsed = _parsed_output_frame(outputs)
        selected_dates = list(dates)
        line_rows = (
            parsed.where(
                F.col("camera_id").isNotNull()
                & F.col("location_id").isNotNull()
                & F.col("_captured_at").isNotNull()
            )
            .select(
                "work_id",
                "camera_id",
                "location_id",
                "_captured_at",
                F.explode("_line_counts").alias("_line"),
            )
            .withColumn(
                "_observed_at",
                F.coalesce(
                    F.to_timestamp("_line.observed_at_utc"),
                    F.timestamp_seconds(
                        F.unix_micros("_captured_at") / F.lit(1_000_000.0)
                        + F.coalesce(
                            F.col("_line.video_seconds"), F.lit(0.0)
                        )
                    ),
                ),
            )
            .withColumn(
                "_flow_camera",
                F.coalesce("_line.camera_id", "camera_id"),
            )
            .withColumn(
                "_flow_location",
                F.coalesce("_line.location_id", "location_id"),
            )
            .withColumn(
                "entries",
                F.coalesce(
                    F.col("_line.frame_in_count"),
                    F.col("_line.entries"),
                    F.lit(0),
                ).cast("long"),
            )
            .withColumn(
                "exits",
                F.coalesce(
                    F.col("_line.frame_out_count"),
                    F.col("_line.exits"),
                    F.lit(0),
                ).cast("long"),
            )
        )
        result = {}
        for table in (
            "gold_flow_minute",
            "gold_flow_hour",
        ):
            timestamp = (
                F.date_trunc("minute", "_observed_at")
                if table == "gold_flow_minute"
                else F.date_trunc("hour", "_observed_at")
            )
            timestamp_name = (
                "minute_utc" if table == "gold_flow_minute" else "hour_utc"
            )
            result[table] = (
                line_rows.withColumn(timestamp_name, timestamp)
                .withColumn(
                    "time_key",
                    F.hour(timestamp) * F.lit(60) + F.minute(timestamp),
                )
                .withColumn(
                    "flow_date",
                    F.date_format("_observed_at", "yyyy-MM-dd"),
                )
                .where(F.col("flow_date").isin(*selected_dates))
                .groupBy(
                    timestamp_name,
                    "time_key",
                    "_flow_camera",
                    "_flow_location",
                    "flow_date",
                )
                .agg(
                    F.sum("entries").cast("long").alias("entries"),
                    F.sum("exits").cast("long").alias("exits"),
                    (F.sum("entries") - F.sum("exits"))
                    .cast("long")
                    .alias("net_flow"),
                    F.countDistinct("work_id")
                    .cast("long")
                    .alias("source_videos"),
                )
                .select(
                    timestamp_name,
                    "time_key",
                    F.col("_flow_camera").alias("camera_id"),
                    F.col("_flow_location").alias("location_id"),
                    "entries",
                    "exits",
                    "net_flow",
                    "source_videos",
                    F.to_timestamp(F.lit(refreshed_at)).alias(
                        "refreshed_at"
                    ),
                    F.to_date("flow_date").alias("flow_date"),
                )
            )
        capture_date = F.date_format("_captured_at", "yyyy-MM-dd")
        result["gold_video"] = (
            parsed.where(
                F.col("_captured_at").isNotNull()
                & F.col("camera_id").isNotNull()
                & F.col("location_id").isNotNull()
                & capture_date.isin(*selected_dates)
            )
            .select(
                "work_id",
                F.col("_captured_at").alias("captured_at_utc"),
                (F.hour("_captured_at") * 60 + F.minute("_captured_at"))
                .cast("integer")
                .alias("time_key"),
                "camera_id",
                "location_id",
                "config_sha256",
                "video_duration_seconds",
                "processing_seconds",
                F.when(
                    F.col("video_duration_seconds").isNotNull()
                    & F.col("processing_seconds").isNotNull()
                    & (F.col("video_duration_seconds") > 0)
                    & (F.col("processing_seconds") > 0),
                    F.col("video_duration_seconds")
                    / F.col("processing_seconds"),
                ).alias("speed_x_realtime"),
                "distinct_people",
                "line_in_count",
                "line_out_count",
                "completed_at",
                F.to_date(capture_date).alias("capture_date"),
            )
        )
        operations = self._operation_events_frame(
            outputs, control, refreshed_at
        ).where(F.col("operation_date").isin(*dates))
        result["gold_operations_hour"] = (
            operations.groupBy(
                "hour_utc",
                "time_key",
                "operation_date",
            )
            .agg(
                F.sum("queued").cast("long").alias("queued"),
                F.sum("started").cast("long").alias("started"),
                F.sum("succeeded").cast("long").alias("succeeded"),
                F.sum("failed").cast("long").alias("failed"),
                F.sum("deferred").cast("long").alias("deferred"),
                F.sum("video_hours_completed")
                .cast("double")
                .alias("video_hours_completed"),
                F.collect_list("_processing").alias("_processing_values"),
                F.max("refreshed_at").alias("refreshed_at"),
            )
            .withColumn(
                "_processing_values",
                F.array_sort(
                    F.filter(
                        "_processing_values",
                        lambda value: value.isNotNull(),
                    )
                ),
            )
            .withColumn(
                "_processing_count",
                F.size("_processing_values"),
            )
            .withColumn(
                "average_processing_seconds",
                F.when(
                    F.col("_processing_count") > 0,
                    F.aggregate(
                        "_processing_values",
                        F.lit(0.0),
                        lambda total, value: total + value,
                    )
                    / F.col("_processing_count"),
                ),
            )
            .withColumn(
                "p95_processing_seconds",
                F.when(
                    F.col("_processing_count") > 0,
                    F.element_at(
                        "_processing_values",
                        F.ceil(F.col("_processing_count") * F.lit(0.95))
                        .cast("int"),
                    ),
                ),
            )
            .select(
                *[
                    field.name
                    for field in _spark_schema("gold_operations_hour").fields
                ]
            )
        )
        result.update(
            self._operational_frames(outputs, control, refreshed_at)
        )
        return result

    @staticmethod
    def _fact_payload_schema():
        from pyspark.sql.types import ArrayType, StructField, StructType

        return StructType(
            [
                StructField(table, ArrayType(_spark_schema(table)), False)
                for table in (
                    "gold_flow_minute",
                    "gold_flow_hour",
                    "gold_video",
                )
            ]
        )

    def _operation_events_frame(
        self, outputs, control, refreshed_at
    ):
        from pyspark.sql import functions as F
        parsed_outputs = _parsed_output_frame(outputs).select(
            "attempt_id",
            F.col("video_duration_seconds").alias("_video_duration"),
            F.col("processing_seconds").alias("_output_processing"),
            F.lit(True).alias("_has_output"),
        )
        failed_statuses = (
            "FAILED",
            "RETRY_WAIT",
            "TERMINAL_FAILED",
            "DEAD_LETTERED",
            "LEASE_LOST",
            "EXPIRED",
        )

        def timestamp(frame, name):
            if name not in frame.columns:
                return F.lit(None).cast("timestamp")
            return F.to_timestamp(name)

        def value(frame, name, data_type="string"):
            if name not in frame.columns:
                return F.lit(None).cast(data_type)
            return F.col(name)

        def project(timestamp, **counts):
            base = timestamp.alias("_timestamp")
            values = {
                name: counts.get(name, F.lit(0)).cast("long")
                for name in (
                    "queued",
                    "started",
                    "succeeded",
                    "failed",
                    "deferred",
                )
            }
            return [
                F.date_trunc("hour", base).alias("hour_utc"),
                (F.hour(base) * F.lit(60)).cast("integer").alias("time_key"),
                *[values[name].alias(name) for name in values],
                counts.get("_video_hours", F.lit(0.0))
                .cast("double")
                .alias("video_hours_completed"),
                counts.get("_processing", F.lit(None).cast("double")).alias(
                    "_processing"
                ),
                F.to_timestamp(F.lit(refreshed_at)).alias("refreshed_at"),
                F.date_format(base, "yyyy-MM-dd").alias("operation_date"),
            ]

        queued_at = F.coalesce(
            timestamp(control["work"], "queued_at"),
            timestamp(control["work"], "queue_entered_at"),
            timestamp(control["work"], "created_at"),
        )
        work_events = control["work"].where(queued_at.isNotNull()).select(
            *project(queued_at, queued=F.lit(1))
        )
        attempts = control["attempts"].join(
            parsed_outputs, "attempt_id", "left"
        )
        claimed_at = F.coalesce(
            timestamp(control["attempts"], "claimed_at"),
            timestamp(control["attempts"], "created_at"),
        )
        started_events = attempts.where(claimed_at.isNotNull()).select(
            *project(claimed_at, started=F.lit(1))
        )
        success = (F.col("status") == "SUCCEEDED") & F.coalesce(
            "_has_output", F.lit(False)
        )
        completed_at = F.when(
            success,
            F.coalesce(
                F.to_timestamp("published_at"),
                timestamp(attempts, "batch_completed_at"),
                timestamp(attempts, "completed_at"),
            ),
        ).otherwise(
            F.coalesce(
                F.when(
                    F.col("status") == "EXPIRED",
                    timestamp(attempts, "batch_expired_at"),
                ),
                F.when(
                    F.col("status").isin(*failed_statuses),
                    F.coalesce(
                        timestamp(attempts, "failed_at"),
                        timestamp(attempts, "batch_completed_at"),
                        timestamp(attempts, "work_updated_at"),
                    ),
                ),
                timestamp(attempts, "completed_at"),
                timestamp(attempts, "batch_completed_at"),
                timestamp(attempts, "sealed_at"),
                timestamp(attempts, "batch_sealed_at"),
            )
        )
        completion_events = attempts.where(
            completed_at.isNotNull()
        ).select(
            *project(
                completed_at,
                succeeded=F.when(success, 1).otherwise(0),
                failed=F.when(
                    F.col("status").isin(*failed_statuses), 1
                ).otherwise(0),
                deferred=F.when(F.col("status") == "RELEASED", 1).otherwise(
                    0
                ),
                _video_hours=F.when(
                    success,
                    F.coalesce(
                        F.col("_video_duration"), F.lit(0.0)
                    )
                    / F.lit(3600.0),
                ).otherwise(0.0),
                _processing=F.coalesce(
                    value(attempts, "processing_seconds", "double"),
                    "_output_processing",
                ),
            )
        )
        return work_events.unionByName(started_events).unionByName(
            completion_events
        )

    def _operational_frames(self, outputs, control, refreshed_at):
        work = control["work"]
        attempts = control["attempts"]
        from pyspark.sql import functions as F

        lease = attempts.select(
            F.col("attempt_id").alias("_lease_attempt_id"),
            F.struct(*[F.col(name) for name in attempts.columns]).alias(
                "_lease_attempt"
            ),
        )
        work_frame = work.join(
            lease,
            (
                F.col("lease_attempt_id")
                if "lease_attempt_id" in work.columns
                else F.lit(None)
            )
            == F.col("_lease_attempt_id"),
            "left",
        )
        projected_work = _native_operational_work_frame(
            work_frame, refreshed_at
        )
        output_run = outputs.select("attempt_id", "output_json")
        attempt_frame = attempts.join(output_run, "attempt_id", "left").join(
            work.select(
                "work_id",
                F.struct(*[F.col(name) for name in work.columns]).alias(
                    "_work"
                ),
            ),
            "work_id",
            "left",
        )
        projected_attempts = _native_operational_attempt_frame(
            attempt_frame
        )
        return {
            "gold_work_operations": projected_work,
            "gold_attempt_operations": projected_attempts,
        }

    def build_dimensions(
        self,
        *,
        full_rebuild: bool = False,
        force: bool = False,
        save_state: bool = True,
    ) -> dict[str, Any]:
        if not self._native_store:
            return super().build_dimensions(
                full_rebuild=full_rebuild,
                force=force,
                save_state=save_state,
            )
        source = self.source_checkpoint()
        fact_versions = self.store.versions(FACT_TARGET_TABLES)
        dimension_source = SourceCheckpoint(
            source.publication_sequence,
            source.versions
            | {
                f"target:{name}": str(version)
                for name, version in fact_versions.items()
            },
        )
        previous = self.state.checkpoint("dimensions")
        dimension_versions = self.store.versions(DIMENSION_TABLES)
        if (
            not full_rebuild
            and not force
            and previous
            and previous["source_key"] == dimension_source.key
            and _checkpoint_targets_match(previous, dimension_versions)
        ):
            return {
                "stage": "dimensions",
                "skipped": True,
                "source_key": dimension_source.key,
            }
        frames = self._dimension_frames(
            self._output_frame(),
            _iso(self._clock()),
        )
        counts = {
            table: self.store.replace_table_frame(table, frames[table])
            for table in DIMENSION_TABLES
        }
        target_versions = self.store.versions(DIMENSION_TABLES)
        checkpoints = {"dimensions": (dimension_source, target_versions)}
        outbox_id = (
            self.state.save_checkpoints_and_enqueue(
                checkpoints,
                "gold dimensions changed",
                dimension_source,
                target_versions,
            )
            if save_state
            else None
        )
        result = {
            "stage": "dimensions",
            "skipped": False,
            "source_key": dimension_source.key,
            "rows": counts,
            "target_versions": target_versions,
            "outbox_id": outbox_id,
        }
        if not save_state:
            result["_checkpoint_updates"] = checkpoints
        return result

    def validate(self) -> dict[str, Any]:
        if not self._native_store:
            return super().validate()
        return self.store.validation_summary()

    def _dimension_frames(self, outputs, refreshed_at):
        from pyspark.sql import functions as F
        metadata = (
            _parsed_output_frame(outputs)
            .where(
                F.col("_captured_at").isNotNull()
                & F.col("camera_id").isNotNull()
                & F.col("location_id").isNotNull()
            )
            .select(
                "work_id",
                F.coalesce("asset_id", "work_id").alias("asset_id"),
                F.coalesce("asset_version", F.lit("1")).alias(
                    "asset_version"
                ),
                "camera_id",
                "location_id",
                F.col("_captured_at").alias("captured_at_utc"),
                F.coalesce("camera_timezone", F.lit("UTC")).alias(
                    "camera_timezone"
                ),
                "config_sha256",
                F.coalesce(
                    F.get_json_object(
                        "output_json", "$.run.config_json"
                    ),
                    F.get_json_object(
                        "output_json", "$.work.config_json"
                    ),
                    F.lit("{}"),
                ).alias("config_json"),
                F.to_timestamp(F.lit(refreshed_at)).alias("refreshed_at"),
            )
        )
        bad_camera = (
            metadata.groupBy("camera_id")
            .agg(
                F.countDistinct("location_id").alias("locations"),
                F.countDistinct("camera_timezone").alias("timezones"),
            )
            .where((F.col("locations") > 1) | (F.col("timezones") > 1))
            .orderBy("camera_id")
            .limit(1)
            .first()
        )
        if bad_camera is not None:
            raise GoldValidationError(
                f"camera {bad_camera['camera_id']} must map to one location "
                "and timezone"
            )
        from zoneinfo import available_timezones

        valid_timezones = tuple(sorted(available_timezones()))
        invalid_timezone = metadata.select(
            "camera_id", "camera_timezone"
        ).where(~F.col("camera_timezone").isin(*valid_timezones)).limit(
            1
        ).first()
        if invalid_timezone is not None:
            raise GoldSourceError(
                f"camera {invalid_timezone['camera_id']} has invalid "
                f"IANA timezone {invalid_timezone['camera_timezone']!r}"
            )
        bad_config = (
            metadata.groupBy("config_sha256")
            .agg(F.countDistinct("config_json").alias("configs"))
            .where(F.col("configs") > 1)
            .orderBy("config_sha256")
            .limit(1)
            .first()
        )
        if bad_config is not None:
            raise GoldValidationError(
                f"config_sha256 {bad_config['config_sha256']} maps to "
                "multiple configurations"
            )

        camera = (
            metadata.groupBy("camera_id")
            .agg(
                F.min("location_id").alias("location_id"),
                F.min("camera_timezone").alias("camera_timezone"),
                F.min("captured_at_utc").alias("first_capture_utc"),
                F.max("captured_at_utc").alias("last_capture_utc"),
                F.countDistinct("work_id").cast("long").alias("video_count"),
                F.max("refreshed_at").alias("refreshed_at"),
            )
        )
        location = (
            metadata.groupBy("location_id")
            .agg(
                F.min("captured_at_utc").alias("first_capture_utc"),
                F.max("captured_at_utc").alias("last_capture_utc"),
                F.countDistinct("camera_id")
                .cast("long")
                .alias("camera_count"),
                F.countDistinct("work_id").cast("long").alias("video_count"),
                F.max("refreshed_at").alias("refreshed_at"),
            )
        )
        config = (
            metadata.groupBy("config_sha256")
            .agg(
                F.min("config_json").alias("config_json"),
                F.min("captured_at_utc").alias("first_capture_utc"),
                F.max("captured_at_utc").alias("last_capture_utc"),
                F.countDistinct("work_id").cast("long").alias("video_count"),
                F.max("refreshed_at").alias("refreshed_at"),
            )
            .select(
                "config_sha256",
                "config_json",
                F.get_json_object("config_json", "$.pipeline").alias(
                    "pipeline"
                ),
                F.get_json_object("config_json", "$.device_variant").alias(
                    "device_variant"
                ),
                F.get_json_object("config_json", "$.device").alias("device"),
                F.get_json_object("config_json", "$.batch_size")
                .cast("long")
                .alias("batch_size"),
                F.get_json_object("config_json", "$.sample_fps")
                .cast("double")
                .alias("sample_fps"),
                F.get_json_object("config_json", "$.detection_threshold")
                .cast("double")
                .alias("detection_threshold"),
                F.get_json_object("config_json", "$.use_fp16")
                .cast("boolean")
                .alias("use_fp16"),
                F.get_json_object("config_json", "$.detector_model").alias(
                    "detector_model"
                ),
                F.get_json_object(
                    "config_json", "$.camera_motion_compensation"
                ).cast("boolean").alias("camera_motion_compensation"),
                F.get_json_object("config_json", "$.line").alias(
                    "counting_line_json"
                ),
                "first_capture_utc",
                "last_capture_utc",
                "video_count",
                "refreshed_at",
            )
        )
        video = metadata.withColumn(
            "capture_date",
            F.date_format(
                F.to_timestamp("captured_at_utc"), "yyyy-MM-dd"
            ),
        ).withColumn(
            "time_key",
            F.hour("captured_at_utc") * 60 + F.minute("captured_at_utc"),
        ).select(
            "work_id",
            "asset_id",
            "asset_version",
            "camera_id",
            "location_id",
            "captured_at_utc",
            "capture_date",
            "time_key",
            "camera_timezone",
            "config_sha256",
            "refreshed_at",
        )
        fact_dates = []
        for table, column in (
            ("gold_flow_minute", "flow_date"),
            ("gold_flow_hour", "flow_date"),
            ("gold_video", "capture_date"),
            ("gold_operations_hour", "operation_date"),
        ):
            fact_dates.append(
                self.store._frame(table)
                .select(F.col(column).cast("date").alias("_date"))
                .where(F.col("_date").isNotNull())
            )
        bounds = fact_dates[0]
        for frame in fact_dates[1:]:
            bounds = bounds.unionByName(frame)
        bounds = bounds.distinct().agg(
            F.min("_date").alias("_start"), F.max("_date").alias("_end")
        ).first()
        fallback = refreshed_at[:10]
        start = str(bounds["_start"] or fallback)
        end = str(bounds["_end"] or fallback)
        date_frame = self.source.spark.range(1).select(
            F.explode(
                F.sequence(
                    F.to_date(F.lit(start)), F.to_date(F.lit(end))
                )
            ).alias("_date")
        )
        dimensions_date = date_frame.select(
            F.col("_date").alias("date_key"),
            F.year("_date").alias("calendar_year"),
            F.quarter("_date").alias("calendar_quarter"),
            F.month("_date").alias("calendar_month"),
            F.date_format("_date", "MMMM").alias("month_name"),
            F.date_format("_date", "MMM").alias("month_short_name"),
            F.date_format("_date", "yyyy-MM").alias("year_month"),
            F.dayofmonth("_date").alias("day_of_month"),
            F.date_format("_date", "EEEE").alias("day_name"),
            F.dayofweek("_date").alias("_iso_day_order"),
            F.weekofyear("_date").alias("iso_week_of_year"),
            F.year(
                F.date_sub(
                    "_date",
                    ((F.dayofweek("_date") + F.lit(5)) % F.lit(7))
                    - F.lit(3),
                )
            ).alias("iso_week_year"),
        ).select(
            "date_key",
            "calendar_year",
            "calendar_quarter",
            "calendar_month",
            "month_name",
            "month_short_name",
            "year_month",
            "day_of_month",
            ((F.col("_iso_day_order") + F.lit(5)) % F.lit(7) + F.lit(1))
            .cast("integer")
            .alias("iso_day_of_week"),
            "iso_week_year",
            "iso_week_of_year",
            F.concat_ws(
                "",
                F.lit(""),
                F.col("iso_week_year"),
                F.lit("-W"),
                F.lpad(F.col("iso_week_of_year"), 2, "0"),
            ).alias("iso_year_week"),
            F.when(
                F.col("_iso_day_order").isin(1, 7),
                F.lit(True),
            )
            .otherwise(F.lit(False))
            .alias("is_weekend"),
            "day_name",
            F.to_timestamp(F.lit(refreshed_at)).alias("refreshed_at"),
        )
        dimensions_time = (
            self.source.spark.range(1440)
            .select(
                F.col("id").cast("integer").alias("time_key"),
                F.floor(F.col("id") / F.lit(60))
                .cast("integer")
                .alias("hour_24"),
                (F.col("id") % F.lit(60))
                .cast("integer")
                .alias("minute_of_hour"),
            )
            .select(
                "time_key",
                "hour_24",
                "minute_of_hour",
                F.format_string(
                    "%02d:%02d", F.col("hour_24"), F.col("minute_of_hour")
                ).alias("time_label"),
                F.format_string("%02d:00", F.col("hour_24")).alias(
                    "hour_label"
                ),
                F.when(F.col("hour_24") < 6, F.lit("Night"))
                .when(F.col("hour_24") < 12, F.lit("Morning"))
                .when(F.col("hour_24") < 18, F.lit("Afternoon"))
                .otherwise(F.lit("Evening"))
                .alias("day_part"),
                F.to_timestamp(F.lit(refreshed_at)).alias("refreshed_at"),
            )
        )
        return {
            "gold_dim_date": dimensions_date,
            "gold_dim_time": dimensions_time,
            "gold_dim_camera": camera,
            "gold_dim_location": location,
            "gold_dim_video": video,
            "gold_dim_model_config": config,
        }


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
    if isinstance(value, Mapping):
        return {
            name: _normalize_spark_row_timestamp(item)
            for name, item in value.items()
        }
    if isinstance(value, list):
        return [_normalize_spark_row_timestamp(item) for item in value]
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


def _frame_checksum(frame: Any, entity: str, sequence_column: str) -> str:
    return _spark_frame_checksum(
        frame,
        row=frame[entity],
        sequence=frame[sequence_column],
        schema_version=_SPARK_CHECKPOINT_VERSION,
    )


def _spark_frame_checksum(
    frame: Any,
    *,
    row: Any | None = None,
    sequence: Any | None = None,
    schema_version: str,
) -> str:
    """Return an explicitly versioned Spark-native frame digest."""
    from pyspark.sql import functions as F

    if row is None:
        row = F.struct(*[F.col(name) for name in frame.columns])
    canonical = F.to_json(
        row,
        {"ignoreNullFields": "false", "timeZone": "UTC"},
    )
    if sequence is None:
        payload_rows = F.array_sort(F.collect_list(canonical))
    else:
        payload_rows = F.transform(
            F.array_sort(
                F.collect_list(
                    F.struct(
                        sequence.alias("_sequence"),
                        canonical.alias("_canonical"),
                    )
                )
            ),
            lambda item: item["_canonical"],
        )
    value = frame.select(
        F.sha2(
            F.concat(
                F.lit("["),
                F.concat_ws(",", payload_rows),
                F.lit("]"),
            ),
            256,
        ).alias("digest")
    ).first()
    return f"{schema_version}:{value['digest']}"


def _output_from_json(value: str) -> CommittedOutput:
    raw = json.loads(value)
    return CommittedOutput(
        str(raw["work_id"]),
        str(raw["attempt_id"]),
        int(raw["publication_sequence"]),
        _optional_datetime(raw.get("published_at")),
        dict(raw["work"]),
        dict(raw["attempt"]),
        dict(raw["run"]),
        tuple(dict(row) for row in raw["line_counts"]),
    )


def _parsed_output_frame(outputs: Any) -> Any:
    """Project the stable CommittedOutput JSON contract with Spark SQL."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    line_schema = ArrayType(
        StructType(
            [
                StructField("observed_at_utc", StringType()),
                StructField("video_seconds", DoubleType()),
                StructField("frame_in_count", LongType()),
                StructField("frame_out_count", LongType()),
                StructField("entries", LongType()),
                StructField("exits", LongType()),
                StructField("camera_id", StringType()),
                StructField("location_id", StringType()),
            ]
        )
    )
    metadata_schema = StructType(
        [
            StructField("camera_id", StringType()),
            StructField("location_id", StringType()),
            StructField("captured_at_utc", StringType()),
            StructField("camera_timezone", StringType()),
            StructField("config_sha256", StringType()),
            StructField("asset_id", StringType()),
            StructField("asset_version", StringType()),
            StructField("duration_seconds", DoubleType()),
            StructField("video_duration_seconds", DoubleType()),
            StructField("processing_seconds", DoubleType()),
            StructField("distinct_people", LongType()),
            StructField("line_in_count", LongType()),
            StructField("line_out_count", LongType()),
            StructField("completed_at", StringType()),
        ]
    )
    attempt_schema = StructType(
        [
            StructField("processing_seconds", DoubleType()),
            StructField("completed_at", StringType()),
            StructField("sealed_at", StringType()),
            StructField("published_at", StringType()),
            StructField("batch_committed_at", StringType()),
        ]
    )
    output_schema = StructType(
        [
            StructField("work_id", StringType(), False),
            StructField("attempt_id", StringType(), False),
            StructField("published_at", StringType()),
            StructField("work", metadata_schema),
            StructField("attempt", attempt_schema),
            StructField("run", metadata_schema),
            StructField("line_counts", line_schema),
            StructField("publication_sequence", LongType()),
        ]
    )
    parsed = outputs.select(
        "output_json",
        F.from_json("output_json", output_schema).alias("_output")
    ).select("output_json", "_output.*")
    captured = F.coalesce(
        F.to_timestamp("run.captured_at_utc"),
        F.to_timestamp("work.captured_at_utc"),
        F.to_timestamp("published_at"),
    )
    return (
        parsed.withColumn("_captured_at", captured)
        .withColumn(
            "_line_counts",
            F.coalesce(
                F.col("line_counts"), F.from_json(F.lit("[]"), line_schema)
            ),
        )
        .withColumn(
            "camera_id",
            F.coalesce("run.camera_id", "work.camera_id"),
        )
        .withColumn(
            "location_id",
            F.coalesce("run.location_id", "work.location_id"),
        )
        .withColumn(
            "config_sha256",
            F.coalesce("run.config_sha256", "work.config_sha256"),
        )
        .withColumn(
            "asset_id", F.coalesce("run.asset_id", "work.asset_id")
        )
        .withColumn(
            "asset_version",
            F.coalesce("run.asset_version", "work.asset_version"),
        )
        .withColumn(
            "camera_timezone",
            F.coalesce("run.camera_timezone", "work.camera_timezone"),
        )
        .withColumn(
            "video_duration_seconds",
            F.coalesce(
                "run.duration_seconds",
                "run.video_duration_seconds",
                "work.duration_seconds",
                "work.video_duration_seconds",
            ),
        )
        .withColumn(
            "processing_seconds",
            F.coalesce(
                "run.processing_seconds",
                "attempt.processing_seconds",
            ),
        )
        .withColumn(
            "distinct_people",
            F.coalesce("run.distinct_people", "work.distinct_people"),
        )
        .withColumn(
            "line_in_count",
            F.coalesce("run.line_in_count", "work.line_in_count"),
        )
        .withColumn(
            "line_out_count",
            F.coalesce("run.line_out_count", "work.line_out_count"),
        )
        .withColumn(
            "completed_at",
            F.coalesce(
                F.to_timestamp("attempt.completed_at"),
                F.to_timestamp("attempt.sealed_at"),
                F.to_timestamp("attempt.published_at"),
                F.to_timestamp("attempt.batch_committed_at"),
                F.to_timestamp("run.completed_at"),
                F.to_timestamp("work.completed_at"),
                F.to_timestamp("published_at"),
            ),
        )
    )


def _native_operational_work_frame(frame: Any, refreshed_at: str) -> Any:
    """Project operational work rows without row-wise Python execution."""
    from pyspark.sql import functions as F

    known = set(frame.columns)
    payload = F.col("payload_json") if "payload_json" in known else F.lit(None)
    refreshed = F.to_timestamp(F.lit(refreshed_at))
    lease_fields = {
        field.name
        for field in frame.schema["_lease_attempt"].dataType.fields
    }

    def merged(name: str) -> Any:
        from_payload = (
            F.get_json_object(payload, f"$.{name}")
            if "payload_json" in known
            else F.lit(None)
        )
        if name in known:
            return F.coalesce(F.col(name).cast("string"), from_payload)
        return from_payload

    def optional(expr: Any) -> Any:
        text = expr.cast("string")
        return F.when(text.isNull() | (text == F.lit("")), None).otherwise(text)

    created = F.to_timestamp(merged("created_at"))
    updated = F.to_timestamp(merged("updated_at"))
    available = F.to_timestamp(merged("available_at"))
    captured = F.to_timestamp(merged("captured_at_utc"))
    attempt_count = F.coalesce(merged("attempt_count").cast("long"), F.lit(0))
    raw_status = F.coalesce(merged("status"), F.lit(""))
    status = (
        F.when(
            (raw_status == "READY")
            & available.isNotNull()
            & (available > refreshed)
            & (attempt_count > 0),
            F.lit("RETRY_WAIT"),
        )
        .when(raw_status == "READY", F.lit("QUEUED"))
        .when(raw_status == "DEAD", F.lit("DEAD_LETTERED"))
        .otherwise(raw_status)
    )
    error = optional(merged("last_error"))
    source_uri = F.coalesce(
        optional(merged("source_uri")), optional(merged("source_video"))
    )
    values = {
        "work_id": optional(merged("work_id")),
        "asset_id": optional(merged("asset_id")),
        "asset_version": optional(merged("asset_version")),
        "source_uri": source_uri,
        "manifest_uri": optional(merged("manifest_uri")),
        "source_etag": optional(merged("source_etag")),
        "expected_size_bytes": merged("expected_size_bytes"),
        "expected_sha256": optional(merged("expected_sha256")),
        "camera_id": optional(merged("camera_id")),
        "location_id": optional(merged("location_id")),
        "captured_at_utc": captured,
        "camera_timezone": optional(merged("camera_timezone")),
        "duration_seconds": F.coalesce(
            merged("duration_seconds"), merged("video_duration_seconds")
        ),
        "priority": merged("priority"),
        "status": status,
        "received_at": created,
        "queued_at": created,
        "not_before_at": available,
        "attempt_count": attempt_count,
        "max_attempts": F.coalesce(
            merged("max_attempts").cast("long"), F.lit(0)
        ),
        "lease_owner_attempt_id": optional(merged("lease_attempt_id")),
        "lease_dispatcher_id": optional(merged("lease_owner")),
        "lease_acquired_at": F.to_timestamp(
            F.col("_lease_attempt.created_at")
            if "created_at" in lease_fields
            else F.lit(None)
        ),
        "lease_expires_at": F.to_timestamp(merged("lease_expires_at")),
        "last_heartbeat_at": F.lit(None),
        "committed_attempt_id": optional(merged("committed_attempt_id")),
        "completed_at": F.when(
            status.isin("SUCCEEDED", "DEAD_LETTERED"), updated
        ),
        "last_error_category": F.when(error.isNotNull(), "PROCESSING"),
        "last_error_type": F.when(status == "DEAD_LETTERED", "DeadLettered"),
        "last_error_message": error,
        "config_json": F.when(
            merged("config_json").isNotNull(), merged("config_json")
        ),
        "config_sha256": optional(
            F.col("config_sha256").cast("string")
            if "config_sha256" in known
            else F.lit(None)
        ),
        "capture_date": captured.cast("date"),
        "last_replay_id": optional(merged("last_replay_id")),
        "replay_generation": F.coalesce(
            merged("replay_generation").cast("long"), F.lit(0)
        ),
        "queue_entered_at": created,
    }
    return _project_native_frame(frame, "gold_work_operations", values)


def _native_operational_attempt_frame(frame: Any) -> Any:
    """Project operational attempt rows with Spark JSON/array expressions."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        ArrayType,
        StringType,
        StructField,
        StructType,
    )

    record_names = (
        "pipeline_run_id",
        "activity_run_id",
        "fabric_job_instance_id",
        "process_attempt_id",
        "sdk_version",
        "package_version",
        "bundle_manifest_sha256",
        "manifest_sha256",
        "config_sha256",
        "input_sha256",
        "input_payload_sha256",
        "source_size_bytes",
        "expected_size_bytes",
        "source_duration_seconds",
        "duration_seconds",
        "source_fps",
        "total_source_frames",
        "processed_frames",
        "effective_sample_fps",
        "sample_fps",
        "processing_seconds",
        "distinct_people",
        "line_in_count",
        "line_out_count",
        "retryable",
        "error_category",
        "error_type",
        "error_message",
        "captured_at_utc",
        "worker_execution_id",
        "executor_identity",
        "status",
    )
    record_schema = ArrayType(
        StructType(
            [
                StructField("record_type", StringType()),
                StructField("payload_json", StringType()),
                *[
                    StructField(name, StringType())
                    for name in record_names
                ],
            ]
        )
    )
    attempt_fields = set(frame.columns)

    def attempt_value(name: str) -> Any:
        return F.col(name) if name in attempt_fields else F.lit(None)

    records = F.from_json(
        attempt_value("records_json"), record_schema, {"mode": "FAILFAST"}
    )
    terminals = F.filter(
        records,
        lambda item: item["record_type"].isin("video_result", "error"),
    )
    terminal = F.when(
        F.size(terminals) > 1,
        F.raise_error(F.lit("attempt.records_json has multiple terminal rows")),
    ).otherwise(F.element_at(terminals, 1))

    def terminal_value(name: str) -> Any:
        top_level = terminal[name] if name in record_names else F.lit(None)
        payload = F.get_json_object(terminal["payload_json"], f"$.{name}")
        return F.coalesce(top_level, payload)

    def output_value(name: str) -> Any:
        return F.get_json_object(F.col("output_json"), f"$.run.{name}")

    # Struct fields are known only at runtime because control adapters can add
    # columns. Resolve them from the joined frame rather than assuming all
    # optional source fields exist.
    work_fields = {
        field.name
        for field in frame.schema["_work"].dataType.fields
    }

    def work_field(name: str) -> Any:
        direct = (
            F.col(f"_work.{name}").cast("string")
            if name in work_fields
            else F.lit(None)
        )
        payload = (
            F.get_json_object(F.col("_work.payload_json"), f"$.{name}")
            if "payload_json" in work_fields
            else F.lit(None)
        )
        return F.coalesce(direct, payload)

    def metadata(name: str) -> Any:
        return F.coalesce(
            output_value(name), terminal_value(name), work_field(name)
        )

    def optional(expr: Any) -> Any:
        text = expr.cast("string")
        return F.when(text.isNull() | (text == F.lit("")), None).otherwise(text)

    claimed = F.to_timestamp(attempt_value("created_at"))
    sealed = F.to_timestamp(attempt_value("sealed_at"))
    completed = F.coalesce(
        F.to_timestamp(attempt_value("published_at")),
        F.to_timestamp(attempt_value("batch_completed_at")),
        F.to_timestamp(attempt_value("completed_at")),
        sealed,
        F.to_timestamp(attempt_value("work_updated_at")),
    )
    captured = F.to_timestamp(metadata("captured_at_utc"))
    error_message = F.coalesce(
        optional(metadata("error_message")),
        optional(attempt_value("recovery_outcome")),
    )
    values = {
        "attempt_id": optional(F.col("attempt_id")),
        "work_id": optional(F.col("work_id")),
        "dispatcher_id": optional(
            F.coalesce(
                attempt_value("dispatcher_id"), attempt_value("batch_owner")
            )
        ),
        "pipeline_run_id": optional(metadata("pipeline_run_id")),
        "activity_run_id": optional(metadata("activity_run_id")),
        "fabric_job_instance_id": optional(
            F.coalesce(
                metadata("fabric_job_instance_id"),
                metadata("process_attempt_id"),
            )
        ),
        "sdk_version": optional(
            F.coalesce(metadata("sdk_version"), metadata("package_version"))
        ),
        "bundle_manifest_sha256": optional(
            F.coalesce(
                metadata("bundle_manifest_sha256"),
                metadata("manifest_sha256"),
            )
        ),
        "config_sha256": optional(
            F.coalesce(metadata("config_sha256"), work_field("config_sha256"))
        ),
        "status": F.coalesce(
            attempt_value("status").cast("string"), F.lit("")
        ),
        "claimed_at": claimed,
        "staging_started_at": claimed,
        "inference_started_at": claimed,
        "writing_started_at": sealed,
        "completed_at": completed,
        "last_heartbeat_at": sealed,
        "input_sha256": optional(
            F.coalesce(
                metadata("input_sha256"), metadata("input_payload_sha256")
            )
        ),
        "source_size_bytes": F.coalesce(
            metadata("source_size_bytes"), metadata("expected_size_bytes")
        ),
        "source_duration_seconds": F.coalesce(
            metadata("source_duration_seconds"), metadata("duration_seconds")
        ),
        "source_fps": metadata("source_fps"),
        "total_source_frames": metadata("total_source_frames"),
        "processed_frames": metadata("processed_frames"),
        "effective_sample_fps": F.coalesce(
            metadata("effective_sample_fps"), metadata("sample_fps")
        ),
        "processing_seconds": metadata("processing_seconds"),
        "distinct_people": metadata("distinct_people"),
        "line_in_count": metadata("line_in_count"),
        "line_out_count": metadata("line_out_count"),
        "retryable": metadata("retryable"),
        "error_category": optional(metadata("error_category")),
        "error_type": optional(metadata("error_type")),
        "error_message": error_message,
        "capture_date": captured.cast("date"),
        "worker_execution_id": optional(
            F.coalesce(
                metadata("worker_execution_id"),
                metadata("executor_identity"),
            )
        ),
    }
    return _project_native_frame(
        frame, "gold_attempt_operations", values
    )


def _project_native_frame(frame: Any, name: str, values: Mapping[str, Any]) -> Any:
    """Cast a native expression map to the stable gold table schema."""
    from pyspark.sql import functions as F

    missing = {field.name for field in _spark_schema(name)} - values.keys()
    if missing:
        raise ValueError(f"missing native projection fields for {name}: {missing}")
    return frame.select(
        *[
            F.coalesce(values[field.name], F.lit(None).cast(field.dataType))
            .cast(field.dataType)
            .alias(field.name)
            for field in _spark_schema(name)
        ]
    )


def _canonical_rows(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted(_canonical(dict(row)) for row in rows)


def _frames_equal(left: Any, right: Any) -> bool:
    return (
        left.exceptAll(right).limit(1).count() == 0
        and right.exceptAll(left).limit(1).count() == 0
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()
