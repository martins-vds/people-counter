"""Gold adapters exercised with real local Spark and Delta tables."""

import hashlib
import json
import time
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from people_counter.fabric_gold import (
    FabricCommittedSource,
    FabricGoldJob,
    FabricGoldState,
    FabricGoldStoreImpl,
)
from people_counter.sjd_gold import (
    CommittedOutput,
    DIMENSION_TABLES,
    FACT_TARGET_TABLES,
    GoldSourceError,
    GoldValidationError,
    LocalJsonGoldStore,
    PathDeltaGoldStore,
    SourceCheckpoint,
    SQLiteGoldState,
    _fact_rows_for_date,
    _jsonable_spark_row,
    _operational_rows,
    _spark_row,
    replace_delta_path_partition,
)
from people_counter.sjd_process import (
    LocalJsonAttemptAdapter,
    SparkDeltaAttemptAdapter,
)

NOW = datetime(2026, 10, 2, 17, tzinfo=UTC)


@pytest.fixture(scope="module")
def gold_spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    delta = pytest.importorskip("delta")
    from pyspark.sql import SparkSession

    root = tmp_path_factory.mktemp("gold-spark")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("gold-adapter-tests")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.databricks.delta.snapshotPartitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(root / "warehouse"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    environment = pytest.MonkeyPatch()
    environment.setenv("TZ", "UTC")
    time.tzset()
    spark = None
    try:
        # Match Fabric's UTC Python workers, not only Spark SQL's session timezone.
        spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        yield spark
    finally:
        if spark is not None:
            spark.stop()
        environment.undo()
        time.tzset()


@pytest.fixture
def gold_namespace(gold_spark, tmp_path):
    name = f"gold_test_{uuid4().hex}"
    gold_spark.sql(f"CREATE DATABASE {name} LOCATION '{tmp_path.as_uri()}'")
    config = SimpleNamespace(table=lambda suffix: f"{name}.{suffix}")
    yield config
    gold_spark.sql(f"DROP DATABASE {name} CASCADE")


def _records():
    return [
        {
            "work_id": "work",
            "attempt_id": "attempt",
            "record_type": "video_result",
            "record_sequence": 0,
            "payload_json": json.dumps({"captured_at_utc": "2026-10-01T23:59:30Z"}),
            "processing_seconds": 2.0,
            "processed_frames": 10,
            "status": "SUCCEEDED",
        },
        {
            "work_id": "work",
            "attempt_id": "attempt",
            "record_type": "line_count",
            "record_sequence": 1,
            "payload_json": json.dumps({"frame_in_count": 2, "video_seconds": 1}),
        },
    ]


def _pointer_rows():
    digest = hashlib.sha256(
        json.dumps(
            sorted(_records(), key=lambda row: row["record_type"]),
            allow_nan=False, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()
    path = "Files/output/batch=batch/attempt=process"
    return {
        "work": [
            {
                "work_id": "work",
                "committed_attempt_id": "attempt",
                "status": "SUCCEEDED",
                "publication_sequence": 2,
                "payload_json": json.dumps(
                    {
                        "camera_id": "camera", "location_id": "lobby",
                        "config_sha256": "config", "config_json": {},
                    }
                ),
                "updated_at": NOW.replace(tzinfo=None),
            }
        ],
        "attempts": [
            {
                "attempt_id": "attempt",
                "work_id": "work",
                "batch_id": "batch",
                "status": "SUCCEEDED",
                "output_path": path,
                "output_sha256": digest,
            }
        ],
        "batches": [
            {
                "batch_id": "batch",
                "status": "COMMITTED",
                "owner": "dispatcher",
                "runtime_key": "runtime",
                "sealed_at": NOW.replace(tzinfo=None),
                "committed_at": NOW.replace(tzinfo=None),
                "lease_expires_at": NOW.replace(tzinfo=None),
            }
        ],
        "publications": [
            {
                "work_id": "work",
                "attempt_id": "attempt",
                "batch_id": "batch",
                "publication_sequence": 2,
                "output_path": path,
                "output_sha256": digest,
                "published_at": NOW.replace(tzinfo=None),
            }
        ],
    }


@pytest.fixture
def committed_source(gold_spark, tmp_path):
    rows = _pointer_rows()
    prefix = f"source_{uuid4().hex}_"
    config = SimpleNamespace(table=lambda suffix: prefix + suffix)

    def install():
        for suffix, values in rows.items():
            gold_spark.createDataFrame(values).createOrReplaceTempView(
                config.table(suffix)
            )

    install()
    adapter = LocalJsonAttemptAdapter(tmp_path / "attempts")
    attempt_path = adapter.write_records(
        "batch", "process",
        _records() + [{"work_id": "uncommitted", "attempt_id": "other"}],
    )
    source = FabricCommittedSource(gold_spark, adapter, config=config)
    yield source, rows, install, attempt_path / "records.json"
    for suffix in rows:
        gold_spark.catalog.dropTempView(config.table(suffix))


def test_committed_source_and_job_use_authoritative_rows(committed_source, tmp_path):
    source, rows, install, records_path = committed_source
    checkpoint = source.checkpoint()
    assert checkpoint.publication_sequence == 2
    assert set(checkpoint.versions) == {"work", "attempt", "publication", "batch"}
    outputs = source.committed_outputs()
    assert records_path.is_file()
    assert len(outputs) == 1
    output = outputs[0]
    assert (output.work_id, output.attempt_id, output.publication_sequence) == (
        "work", "attempt", 2
    )
    assert output.work["camera_id"] == "camera"
    assert output.published_at == NOW
    assert output.attempt["batch_committed_at"] == NOW
    assert output.line_counts[0]["frame_in_count"] == 2
    controls = source.control_rows()
    assert controls["attempts"][0]["dispatcher_id"] == "dispatcher"
    assert controls["attempts"][0]["work_updated_at"] == NOW
    operations = source.operations_checkpoint()
    assert operations.publication_sequence == 2
    rows["work"].append(
        rows["work"][0] | {"work_id": "queued", "committed_attempt_id": None}
    )
    install()
    assert source.checkpoint() == checkpoint
    assert source.operations_checkpoint() != operations
    job = FabricGoldJob(
        source, LocalJsonGoldStore(tmp_path / "gold"),
        SQLiteGoldState(tmp_path / "gold.sqlite3", clock=lambda: NOW),
        clock=lambda: NOW,
    )
    assert job.source_checkpoint() == checkpoint
    assert job.operations_source_checkpoint() == source.operations_checkpoint()
    assert job.committed_outputs() == outputs
    assert job.run()["validation"]["valid"] is True
    rows["publications"][0]["published_at"] = None
    # Keep a concrete timestamp in the nullable column for schema inference.
    rows["publications"].append(rows["publications"][0] | {
        "attempt_id": "uncommitted", "published_at": NOW.replace(tzinfo=None)
    })
    install()
    assert source.committed_outputs()[0].published_at == NOW


def test_committed_source_prefers_shared_records_for_compatibility_api(
    committed_source, monkeypatch
):
    source, _, _, _ = committed_source
    source.spark.createDataFrame(
        [
            (
                "batch",
                "process",
                record["work_id"],
                record["attempt_id"],
                record["record_type"],
                record["record_sequence"],
                json.dumps(record, sort_keys=True, separators=(",", ":")),
            )
            for record in _records()
        ],
        (
            "batch_id string, process_attempt_id string, work_id string, "
            "attempt_id string, record_type string, record_sequence long, "
            "record_json string"
        ),
    ).createOrReplaceTempView(source.config.table("committed_records"))
    monkeypatch.setattr(
        source.attempts,
        "read_records",
        lambda *_: pytest.fail("attempt reader must not be used"),
    )

    outputs = source.committed_outputs()

    assert [(output.work_id, output.attempt_id) for output in outputs] == [
        ("work", "attempt")
    ]
    source.spark.catalog.dropTempView(source.config.table("committed_records"))


def test_committed_source_consumes_materialized_output_without_python_udf(
    committed_source, monkeypatch
):
    source, _, _, _ = committed_source
    from pyspark.sql import functions as F

    digest = _pointer_rows()["attempts"][0]["output_sha256"]
    output_json = json.dumps(
        {
            "schema_version": 1,
            "work_id": "work",
            "attempt_id": "attempt",
            "output_sha256": digest,
            "run": {
                "work_id": "work",
                "attempt_id": "attempt",
                "captured_at_utc": "2026-10-01T23:59:30Z",
                "processing_seconds": 2.0,
                "processed_frames": 10,
                "status": "SUCCEEDED",
                "custom_metadata": {"encoding": "utf-8"},
            },
            "line_counts": [
                {
                    "work_id": "work",
                    "attempt_id": "attempt",
                    "frame_in_count": 2,
                    "video_seconds": 1.0,
                }
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    source.spark.createDataFrame(
        [
            (
                "batch",
                "process",
                record["work_id"],
                record["attempt_id"],
                record["record_type"],
                record["record_sequence"],
                json.dumps(record, sort_keys=True, separators=(",", ":")),
                1,
                output_json,
            )
            for record in _records()
        ],
        (
            "batch_id string, process_attempt_id string, work_id string, "
            "attempt_id string, record_type string, record_sequence long, "
            "record_json string, output_schema_version long, output_json string"
        ),
    ).createOrReplaceTempView(source.config.table("committed_records"))
    monkeypatch.setattr(
        F, "udf", lambda *args, **kwargs: pytest.fail("Python UDF was used")
    )

    outputs = source.committed_outputs()
    frame = source.committed_output_frame()
    assert len(outputs) == 1
    assert outputs[0].run["processing_seconds"] == 2.0
    assert outputs[0].run["custom_metadata"] == {"encoding": "utf-8"}
    assert "BatchEvalPython" not in (
        frame._jdf.queryExecution().optimizedPlan().toString()
    )
    source.spark.catalog.dropTempView(source.config.table("committed_records"))


def test_committed_source_fails_closed_for_invalid_materialization(
    committed_source,
):
    source, _, _, _ = committed_source
    source.spark.createDataFrame(
        [
            (
                "batch",
                "process",
                "work",
                "attempt",
                "video_result",
                0,
                "{}",
                2,
                "{}",
            )
        ],
        (
            "batch_id string, process_attempt_id string, work_id string, "
            "attempt_id string, record_type string, record_sequence long, "
            "record_json string, output_schema_version long, output_json string"
        ),
    ).createOrReplaceTempView(source.config.table("committed_records"))

    with pytest.raises(GoldSourceError, match="materialization is invalid"):
        source.committed_output_frame().limit(1).collect()
    source.spark.catalog.dropTempView(source.config.table("committed_records"))


def test_committed_source_uses_filtered_spark_read_path(committed_source, monkeypatch):
    source, _, _, _ = committed_source
    monkeypatch.setattr(
        source,
        "_rows",
        lambda suffix: pytest.fail(f"full-table read of {suffix}"),
    )

    assert source.checkpoint().publication_sequence == 2
    assert len(source.committed_outputs()) == 1
    assert len(source.control_rows()["attempts"]) == 1


def test_committed_source_checkpoints_do_not_use_python_udfs(
    committed_source, monkeypatch
):
    source, _, _, _ = committed_source
    from pyspark.sql import functions as F

    monkeypatch.setattr(
        F, "udf", lambda *args, **kwargs: pytest.fail("checkpoint used Python UDF")
    )

    checkpoint = source.checkpoint()
    operations = source.operations_checkpoint()
    assert checkpoint.publication_sequence == 2
    assert operations.publication_sequence == 2
    assert all(
        version.startswith("spark-json-v2:")
        for version in checkpoint.versions.values()
    )
    assert all(
        version.startswith("spark-json-v2:")
        for version in operations.versions.values()
    )


def test_operational_frames_have_no_python_evaluation(committed_source):
    source, _, _, _ = committed_source
    job = FabricGoldJob(
        source,
        SimpleNamespace(),
        SimpleNamespace(),
        clock=lambda: NOW,
    )
    outputs = source.spark.createDataFrame(
        [], "attempt_id string, output_json string"
    )
    frames = job._operational_frames(
        outputs, source.control_frame(), NOW.isoformat()
    )

    for name, frame in frames.items():
        plan = frame._jdf.queryExecution().optimizedPlan().toString()
        assert "BatchEvalPython" not in plan, name


def test_operational_native_rows_match_local_projection(committed_source):
    source, _, _, _ = committed_source
    control = source.control_rows()
    committed = source.committed_outputs()
    outputs = source.spark.createDataFrame(
        [
            {
                "attempt_id": output.attempt_id,
                "output_json": json.dumps({
                    "work_id": output.work_id,
                    "attempt_id": output.attempt_id,
                    "publication_sequence": output.publication_sequence,
                    "published_at": (
                        None
                        if output.published_at is None
                        else output.published_at.isoformat()
                    ),
                    "work": output.work,
                    "attempt": output.attempt,
                    "run": output.run,
                    "line_counts": output.line_counts,
                }, default=lambda value: value.isoformat()),
            }
            for output in committed
        ],
        "attempt_id string, output_json string",
    )
    job = FabricGoldJob(
        source,
        SimpleNamespace(),
        SimpleNamespace(),
        clock=lambda: NOW,
    )

    actual = job._operational_frames(
        outputs, source.control_frame(), NOW.isoformat()
    )
    expected = _operational_rows(control, committed, NOW.isoformat())

    for name, frame in actual.items():
        rows = sorted(
            (
                _jsonable_spark_row(row.asDict(recursive=True))
                for row in frame.collect()
            ),
            key=lambda row: row[next(iter(row))],
        )
        expected_rows = sorted(
            (
                _jsonable_spark_row(_spark_row(name, row))
                for row in expected[name]
            ),
            key=lambda row: row[next(iter(row))],
        )
        assert len(rows) == len(expected_rows), name
        for actual_row, expected_row in zip(rows, expected_rows):
            for field, expected_value in expected_row.items():
                assert actual_row[field] == expected_value, (
                    name,
                    field,
                    actual_row[field],
                    expected_value,
                )


def test_fabric_gold_job_aggregates_without_materializing_outputs(
    gold_spark, gold_namespace, tmp_path, monkeypatch
):
    rows = _pointer_rows()
    prefix = f"source_{uuid4().hex}_"
    for suffix, values in rows.items():
        gold_spark.createDataFrame(values).createOrReplaceTempView(
            f"{prefix}{suffix}"
        )
    adapter = SparkDeltaAttemptAdapter(tmp_path / "attempts", gold_spark)
    adapter.write_records("batch", "process", _records())
    source = FabricCommittedSource(
        gold_spark,
        adapter,
        config=SimpleNamespace(table=lambda suffix: f"{prefix}{suffix}"),
    )
    store = FabricGoldStoreImpl(gold_spark, config=gold_namespace)
    store.bootstrap()
    _seed_lock(gold_spark, gold_namespace)
    state = FabricGoldState(
        gold_spark, config=gold_namespace, clock=lambda: NOW
    )
    job = FabricGoldJob(source, store, state, clock=lambda: NOW)
    monkeypatch.setattr(
        source,
        "committed_outputs",
        lambda: pytest.fail("committed outputs materialized on the driver"),
    )
    monkeypatch.setattr(
        source,
        "control_rows",
        lambda: pytest.fail("control rows materialized on the driver"),
    )
    monkeypatch.setattr(
        store,
        "read_table",
        lambda name: pytest.fail(f"gold table {name} materialized on driver"),
    )

    result = job.run()

    assert result["validation"]["valid"] is True
    assert result["facts"]["rows"]["gold_flow_minute"] == 1
    assert result["dimensions"]["rows"]["gold_dim_video"] == 1


def test_native_fact_frames_have_no_python_evaluation(gold_spark):
    output = {
        "work_id": "work",
        "attempt_id": "attempt",
        "publication_sequence": 1,
        "published_at": "2026-10-01T23:59:30Z",
        "work": {
            "camera_id": "camera",
            "location_id": "lobby",
            "config_sha256": "config",
            "config_json": {},
        },
        "attempt": {},
        "run": {
            "captured_at_utc": "2026-10-01T23:59:30Z",
            "duration_seconds": 60.0,
            "processing_seconds": 2.0,
        },
        "line_counts": [
            {
                "frame_in_count": 2,
                "frame_out_count": 1,
                "video_seconds": 1.0,
            }
        ],
    }
    outputs = gold_spark.createDataFrame(
        [
            (
                "work",
                "attempt",
                1,
                json.dumps(output, sort_keys=True),
            )
        ],
        (
            "work_id string, attempt_id string, publication_sequence long, "
            "output_json string"
        ),
    )
    work = gold_spark.createDataFrame(
        [],
        "work_id string, status string, queued_at timestamp",
    )
    attempts = gold_spark.createDataFrame(
        [],
        (
            "attempt_id string, work_id string, status string, "
            "created_at timestamp, published_at timestamp"
        ),
    )
    job = FabricGoldJob(
        SimpleNamespace(spark=gold_spark),
        SimpleNamespace(),
        SimpleNamespace(),
        clock=lambda: NOW,
    )

    events = job._operation_events_frame(
        outputs,
        {"work": work, "attempts": attempts},
        NOW.isoformat(),
    )
    fact_frames = job._fact_frames(
        outputs,
        {"work": work, "attempts": attempts},
        ["2026-10-01"],
        NOW.isoformat(),
    )

    for name in (
        "gold_flow_minute",
        "gold_flow_hour",
        "gold_video",
        "gold_operations_hour",
    ):
        plan = fact_frames[name]._jdf.queryExecution().optimizedPlan().toString()
        assert "BatchEvalPython" not in plan, name
    assert "BatchEvalPython" not in (
        events._jdf.queryExecution().optimizedPlan().toString()
    )
    expected = _fact_rows_for_date(
        [
            CommittedOutput(
                "work",
                "attempt",
                1,
                datetime.fromisoformat(
                    output["published_at"].replace("Z", "+00:00")
                ),
                output["work"],
                output["attempt"],
                output["run"],
                tuple(output["line_counts"]),
            )
        ],
        {},
        "2026-10-01",
        NOW.isoformat(),
    )
    for name in ("gold_flow_minute", "gold_flow_hour", "gold_video"):
        actual = [
            _jsonable_spark_row(row.asDict(recursive=True))
            for row in fact_frames[name].collect()
        ]
        expected_spark = [
            _jsonable_spark_row(_spark_row(name, row))
            for row in expected[name]
        ]
        assert actual == expected_spark, name


def test_record_frame_reads_multiple_delta_attempt_paths(
    gold_spark, tmp_path
):
    adapter = SparkDeltaAttemptAdapter(tmp_path / "attempts", gold_spark)
    for batch, process in (("one", "first"), ("two", "second")):
        adapter.write_records(
            batch,
            process,
            [
                {
                    "work_id": f"work-{batch}",
                    "attempt_id": f"attempt-{batch}",
                    "record_type": "video_result",
                    "record_sequence": 0,
                    "payload_json": "{}",
                }
            ],
        )
    visible = gold_spark.createDataFrame(
        [
            (
                {"work_id": "work-one"},
                {
                    "attempt_id": "attempt-one",
                    "batch_id": "one",
                },
                {
                    "output_path": "attempts/batch=one/attempt=first",
                    "publication_sequence": 1,
                },
            ),
            (
                {"work_id": "work-two"},
                {
                    "attempt_id": "attempt-two",
                    "batch_id": "two",
                },
                {
                    "output_path": "attempts/batch=two/attempt=second",
                    "publication_sequence": 2,
                },
            ),
        ],
        (
            "work struct<work_id:string>, "
            "attempt struct<attempt_id:string,batch_id:string>, "
            "publication struct<output_path:string,publication_sequence:long>"
        ),
    )
    source = FabricCommittedSource(
        gold_spark, adapter, config=SimpleNamespace(table=str)
    )

    frame = source._record_frame(visible)

    assert frame.count() == 2


def test_record_frame_reads_shared_committed_table_without_attempt_paths(
    gold_spark,
):
    table = f"committed_records_{uuid4().hex}"
    gold_spark.createDataFrame(
        [
            (
                "one",
                "first",
                "work-one",
                "attempt-one",
                "video_result",
                0,
                '{"record_sequence":0}',
            ),
            (
                "two",
                "second",
                "work-two",
                "attempt-two",
                "video_result",
                0,
                '{"record_sequence":0}',
            ),
        ],
        (
            "batch_id string, process_attempt_id string, work_id string, "
            "attempt_id string, record_type string, record_sequence long, "
            "record_json string"
        ),
    ).createOrReplaceTempView(table)
    visible = gold_spark.createDataFrame(
        [
            (
                {"work_id": "work-one"},
                {"attempt_id": "attempt-one", "batch_id": "one"},
                {"output_path": "ignored", "publication_sequence": 1},
            ),
            (
                {"work_id": "work-two"},
                {"attempt_id": "attempt-two", "batch_id": "two"},
                {"output_path": "ignored", "publication_sequence": 2},
            ),
        ],
        (
            "work struct<work_id:string>, "
            "attempt struct<attempt_id:string,batch_id:string>, "
            "publication struct<output_path:string,publication_sequence:long>"
        ),
    )
    source = FabricCommittedSource(
        gold_spark,
        SimpleNamespace(
            spark_session=gold_spark,
            attempt_path=lambda *_: pytest.fail(
                "shared records must not resolve attempt paths"
            ),
        ),
        config=SimpleNamespace(
            table=lambda suffix: table if suffix == "committed_records" else suffix
        ),
    )

    frame = source._record_frame(visible)

    assert frame.count() == 2
    assert sorted(row["work_id"] for row in frame.collect()) == [
        "work-one",
        "work-two",
    ]
    gold_spark.catalog.dropTempView(table)


@pytest.mark.parametrize(
    "table,field,value",
    [
        ("attempts", "work_id", "other"),
        ("attempts", "status", "FAILED"),
        ("work", "status", "RUNNING"),
        ("work", "publication_sequence", 99),
        ("publications", "output_path", "other"),
        ("publications", "output_sha256", "other"),
        ("publications", "batch_id", "other"),
        ("batches", "status", "SEALED"),
        ("attempts", "attempt_id", "missing"),
        ("publications", "attempt_id", "missing"),
        ("batches", "batch_id", "missing"),
    ],
)
def test_committed_source_rejects_broken_pointer(
    committed_source, table, field, value
):
    source, rows, install, _ = committed_source
    rows[table][0][field] = value
    install()
    with pytest.raises(GoldSourceError, match="invalid committed pointer"):
        source.committed_outputs()


@pytest.mark.parametrize(
    "path,message",
    [
        ("Files/batch=b", "unique attempt"),
        ("Files/attempt=a/batch=b/attempt=duplicate", "unique attempt"),
        ("Files/attempt=/batch=b", "unique attempt"),
        ("Files/attempt=a", "unique batch"),
    ],
)
def test_committed_source_requires_unique_output_path_segments(
    committed_source, path, message
):
    source, rows, install, _ = committed_source
    for table in ("attempts", "publications"):
        rows[table][0]["output_path"] = path
    install()
    with pytest.raises(GoldSourceError, match=message):
        source.committed_outputs()


@pytest.mark.parametrize("records", [[], [{"work_id": "other"}]])
def test_committed_source_rejects_missing_records(committed_source, records):
    source, _, _, records_path = committed_source
    records_path.write_text(json.dumps(records))
    with pytest.raises(GoldSourceError, match="has no records"):
        source.committed_outputs()


def test_committed_source_rejects_digest_and_nonobject_payload(committed_source):
    source, rows, install, records_path = committed_source
    records_path.write_text(json.dumps(
        [_records()[0] | {"processing_seconds": 99}]
    ))
    with pytest.raises(GoldSourceError, match="digest mismatch"):
        source.committed_outputs()
    records_path.write_text(json.dumps(_records()))
    rows["work"][0]["payload_json"] = "[]"
    install()
    with pytest.raises(GoldSourceError, match="payload must be an object"):
        source.committed_outputs()


def test_empty_source_and_unmatched_control_attempts(committed_source):
    source, rows, install, _ = committed_source
    rows["attempts"].append(rows["attempts"][0] | {
        "attempt_id": "unmatched", "work_id": "missing", "batch_id": "missing"
    })
    install()
    unmatched = next(
        row for row in source.control_rows()["attempts"]
        if row["attempt_id"] == "unmatched"
    )
    assert unmatched["dispatcher_id"] is None
    assert unmatched["work_updated_at"] is None
    assert unmatched["published_at"] is None
    for suffix in rows:
        name = source.config.table(suffix)
        source.spark.table(name).where("false").createOrReplaceTempView(name)
    assert source.checkpoint().publication_sequence == 0
    assert source.operations_checkpoint().publication_sequence == 0
    assert source.committed_outputs() == []
    assert source.control_rows() == {"work": [], "attempts": []}


def _seed_lock(spark, config):
    spark.createDataFrame(
        [("global", None, None)],
        "lock_name string, owner_id string, acquired_at timestamp",
    ).write.format("delta").saveAsTable(config.table("locks"))


def _minute(day="2026-10-01", entries=2):
    return {
        "minute_utc": f"{day}T12:00:00Z",
        "time_key": 720,
        "camera_id": "camera",
        "location_id": "lobby",
        "entries": entries,
        "exits": 1,
        "net_flow": entries - 1,
        "source_videos": 1,
        "refreshed_at": "2026-10-02T17:00:00Z",
        "flow_date": day,
    }


def test_catalog_gold_partition_replacement_and_versions(gold_spark, gold_namespace):
    _seed_lock(gold_spark, gold_namespace)
    store = FabricGoldStoreImpl(gold_spark, config=gold_namespace)
    assert store.read_table("gold_flow_minute") == []
    assert store.versions(["gold_flow_minute"]) == {"gold_flow_minute": 0}
    with pytest.raises(ValueError, match="not a Fabric"):
        store.read_table("work")
    with pytest.raises(ValueError, match="outside"):
        store.replace_partition(
            "gold_flow_minute", "flow_date", "2026-10-01", [_minute("2026-10-02")]
        )
    for day in ("2026-10-01", "2026-10-02"):
        assert store.replace_partition(
            "gold_flow_minute", "flow_date", day, [_minute(day)]
        ) == 1
    assert store.version("gold_flow_minute") == 2
    assert store.replace_partition(
        "gold_flow_minute", "flow_date", "2026-10-01", [_minute(entries=5)]
    ) == 1
    assert sorted(store.read_table("gold_flow_minute"), key=lambda r: r["flow_date"]) == [
        _minute(entries=5), _minute("2026-10-02")
    ]
    assert store.replace_partition(
        "gold_flow_minute", "flow_date", "2026-10-01", []
    ) == 0
    assert store.read_table("gold_flow_minute") == [_minute("2026-10-02")]
    assert store.replace_table("gold_flow_minute", [_minute()]) == 1
    assert store.read_table("gold_flow_minute") == [_minute()]
    assert store.replace_table("gold_flow_minute", []) == 0
    assert store.read_table("gold_flow_minute") == []
    store.bootstrap()
    versions = store.versions(FACT_TARGET_TABLES + DIMENSION_TABLES)
    store.bootstrap()
    assert store.versions(FACT_TARGET_TABLES + DIMENSION_TABLES) == versions
    assert all(version >= 1 for version in versions.values())
    with pytest.raises(ValueError, match="invalid date partition"):
        store.replace_partition("gold_flow_minute", "flow_date", "invalid", [])


def test_catalog_gold_writeback_validates_without_full_table_reads(
    gold_spark, gold_namespace, monkeypatch
):
    _seed_lock(gold_spark, gold_namespace)
    store = FabricGoldStoreImpl(gold_spark, config=gold_namespace)
    store.replace_partition(
        "gold_flow_minute", "flow_date", "2026-10-01", [_minute()]
    )
    monkeypatch.setattr(
        store,
        "read_table",
        lambda name: pytest.fail(f"full-table read of {name}"),
    )

    assert store.replace_partition(
        "gold_flow_minute", "flow_date", "2026-10-02", [_minute("2026-10-02")]
    ) == 1
    assert store.replace_table("gold_flow_minute", [_minute(entries=3)]) == 1
    assert store.fact_dates()["gold_flow_minute"] == [
        {"flow_date": datetime(2026, 10, 1).date()}
    ]
    assert store.replace_table("gold_flow_minute", []) == 0
    validation = store.validation_summary()
    assert validation["table_rows"]["gold_flow_minute"] == 0


def test_fabric_state_checkpoint_and_refresh_lifecycle(gold_spark, gold_namespace):
    _seed_lock(gold_spark, gold_namespace)
    state = FabricGoldState(gold_spark, config=gold_namespace, clock=lambda: NOW)
    state.bootstrap()
    assert state.checkpoint("facts") is None
    assert state.pending_refreshes() == []
    source = SourceCheckpoint(3, {"work": "version"})
    versions = {"gold_video": 2}
    state.save_checkpoint("facts", source, versions)
    checkpoint = state.checkpoint("facts")
    assert checkpoint["source_key"] == source.key
    assert checkpoint["publication_sequence"] == 3
    assert checkpoint["completed_at"] == NOW
    assert json.loads(checkpoint["source_versions_json"]) == source.versions
    assert json.loads(checkpoint["target_versions_json"]) == versions
    first = state.enqueue_refresh("facts changed", source, versions)
    assert state.enqueue_refresh("facts changed", source, versions) == first
    second = state.enqueue_refresh("dimensions changed", source, versions)
    pending = state.pending_refreshes()
    assert [row["outbox_id"] for row in pending] == [first, second]
    assert pending[0]["payload"]["target_versions"] == versions
    assert not state.acknowledge_refresh(
        first, expected_dedupe_key="not-the-observed-key"
    )
    assert state.acknowledge_refresh(
        first, "operator", expected_dedupe_key=pending[0]["dedupe_key"]
    )
    assert not state.acknowledge_refresh(first)
    assert not state.acknowledge_refresh(999)
    assert [row["outbox_id"] for row in state.pending_refreshes()] == [second]
    assert state.acknowledge_refresh(second)
    assert state.pending_refreshes() == []
    assert state.enqueue_refresh("facts changed", source, versions) == first
    assert state.pending_refreshes() == []


def test_fabric_state_mutations_and_reads_are_targeted(
    gold_spark, gold_namespace, monkeypatch
):
    _seed_lock(gold_spark, gold_namespace)
    state = FabricGoldState(gold_spark, config=gold_namespace, clock=lambda: NOW)
    source = SourceCheckpoint(3, {"work": "version"})
    versions = {"gold_video": 2}
    state.save_checkpoint("facts", source, versions)
    state.enqueue_refresh("facts changed", source, versions)
    monkeypatch.setattr(
        state,
        "_rows",
        lambda suffix: pytest.fail(f"full-table read of {suffix}"),
    )

    assert state.checkpoint("facts")["source_key"] == source.key
    assert len(state.pending_refreshes()) == 1
    outbox_id = state.pending_refreshes()[0]["outbox_id"]
    assert state.acknowledge_refresh(outbox_id)
    assert state.pending_refreshes() == []


def test_fabric_state_clock_contract_and_duplicate_checkpoint(
    gold_spark, gold_namespace
):
    _seed_lock(gold_spark, gold_namespace)
    state = FabricGoldState(
        gold_spark, config=gold_namespace, clock=lambda: "not a datetime"
    )
    with pytest.raises(TypeError, match="clock must return datetime"):
        state.save_checkpoint("facts", SourceCheckpoint(0, {}), {})
    # Failed writes deliberately retain the control lock; reset it explicitly.
    gold_spark.sql(
        f"UPDATE {gold_namespace.table('locks')} SET owner_id=NULL, acquired_at=NULL"
    )
    state = FabricGoldState(
        gold_spark, config=gold_namespace,
        clock=lambda: NOW.astimezone(timezone(timedelta(hours=3))),
        auto_bootstrap=False,
    )
    state.save_checkpoint("facts", SourceCheckpoint(0, {}), {})
    assert state.checkpoint("facts")["completed_at"] == NOW
    state = FabricGoldState(
        gold_spark, config=gold_namespace,
        clock=lambda: NOW.replace(tzinfo=None), auto_bootstrap=False,
    )
    state.save_checkpoint("dimensions", SourceCheckpoint(0, {}), {})
    assert state.checkpoint("dimensions")["completed_at"] == NOW
    table = gold_namespace.table("gold_checkpoints")
    gold_spark.table(table).where("stage='facts'").write.format("delta").mode(
        "append"
    ).saveAsTable(table)
    with pytest.raises(GoldValidationError, match="duplicate gold checkpoint"):
        state.checkpoint("facts")


@pytest.mark.parametrize(
    "ack_id,message",
    [(1, "outbox identity is not unique"), (999, "duplicate semantic_refresh_outbox")],
)
def test_fabric_state_rejects_duplicate_outbox_identity(
    gold_spark, gold_namespace, ack_id, message
):
    _seed_lock(gold_spark, gold_namespace)
    state = FabricGoldState(gold_spark, config=gold_namespace, clock=lambda: NOW)
    outbox_id = state.enqueue_refresh("changed", SourceCheckpoint(1, {}), {})
    assert outbox_id == 1
    table = gold_namespace.table("semantic_refresh_outbox")
    gold_spark.table(table).write.format("delta").mode("append").saveAsTable(table)
    with pytest.raises(GoldValidationError, match=message):
        state.acknowledge_refresh(ack_id)


def test_path_delta_store_preserves_other_partitions_and_clears_tables(
    gold_spark, tmp_path
):
    store = PathDeltaGoldStore(tmp_path, gold_spark)
    assert store.read_table("gold_flow_minute") == []
    assert store.versions(["gold_flow_minute"]) == {"gold_flow_minute": 0}
    assert store.replace_partition(
        "gold_flow_minute", "flow_date", "2026-10-01", []
    ) == 0
    assert store.replace_table("gold_dim_camera", []) == 0
    with pytest.raises(ValueError, match="invalid local table"):
        store.read_table("../escape")
    with pytest.raises(ValueError, match="outside"):
        store.replace_partition(
            "gold_flow_minute", "flow_date", "2026-10-01", [_minute("2026-10-02")]
        )
    for day in ("2026-10-01", "2026-10-02"):
        assert store.replace_partition(
            "gold_flow_minute", "flow_date", day, [_minute(day)]
        ) == 1
    assert store.version("gold_flow_minute") == 2
    assert store.replace_partition(
        "gold_flow_minute", "flow_date", "2026-10-01", []
    ) == 0
    assert store.read_table("gold_flow_minute") == [_minute("2026-10-02")]
    assert store.replace_table("gold_flow_minute", [_minute()]) == 1
    assert store.read_table("gold_flow_minute") == [_minute()]
    assert store.replace_table("gold_flow_minute", []) == 0
    assert store.read_table("gold_flow_minute") == []
    from people_counter.sjd_gold import _spark_row, _spark_schema

    frame = gold_spark.createDataFrame(
        [_spark_row("gold_flow_minute", _minute())],
        _spark_schema("gold_flow_minute"),
    )
    direct = tmp_path / "direct"
    assert replace_delta_path_partition(
        gold_spark, direct, frame, "flow_date", "2026-10-01"
    ) == 1
    assert replace_delta_path_partition(
        gold_spark, direct, frame.limit(0), "flow_date", "2026-10-01"
    ) == 0
    assert gold_spark.read.format("delta").load(str(direct)).count() == 0
    assert replace_delta_path_partition(
        gold_spark, tmp_path / "absent", frame.limit(0), "flow_date", "2026-10-01"
    ) == 0
