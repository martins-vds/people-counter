import hashlib
import json
import sqlite3
import sys
import tempfile
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest

from people_counter.sjd_control import SQLiteControlStore
from people_counter.sjd_gold import (
    CommittedOutput,
    DIMENSION_TABLES,
    FACT_TABLES,
    OPERATIONAL_TABLES,
    FabricGoldStore,
    FabricGoldSource,
    FabricGoldState,
    GoldSourceError,
    GoldValidationError,
    LocalGoldJob,
    LocalJsonGoldStore,
    SourceCheckpoint,
    UnsupportedGoldBackendError,
    _dimension_rows,
    _dimension_fact_dates,
    _dimension_source_rows,
    _load_delta_output_document,
    _checkpoint_targets_match,
    _fact_rows_for_date,
    _optional_float,
    _operational_rows,
    _output_dates,
    _sha256_json,
    main,
)


NOW = datetime(2026, 10, 2, 17, 0, tzinfo=timezone.utc)


def test_zero_metric_is_valid_and_preserved():
    assert _optional_float(0) == 0.0
    assert _optional_float("0") == 0.0


def test_missing_capture_time_uses_only_committed_publication_or_stays_undated():
    published = datetime(2026, 10, 6, 3, 16, tzinfo=timezone.utc)
    output = CommittedOutput(
        "work",
        "attempt",
        1,
        published,
        {
            "camera_id": "camera",
            "location_id": "location",
            "config_sha256": "a" * 64,
            "duration_seconds": 7.0,
        },
        {},
        {
            "processing_seconds": 8.0,
            "line_in_count": 0,
            "line_out_count": 0,
        },
        (),
    )

    assert _output_dates(output) == {"2026-10-06"}
    facts = _fact_rows_for_date(
        [output], {}, "2026-10-06", "2026-10-06T04:00:00Z"
    )
    assert facts["gold_video"][0]["captured_at_utc"] == "2026-10-06T03:16:00Z"

    captured = CommittedOutput(
        output.work_id,
        output.attempt_id,
        output.publication_sequence,
        published,
        output.work,
        output.attempt,
        {"captured_at_utc": "2026-10-05T01:02:03Z"},
        output.line_counts,
    )
    assert _output_dates(captured) == {"2026-10-05"}
    captured_in_work = CommittedOutput(
        captured.work_id,
        captured.attempt_id,
        captured.publication_sequence,
        captured.published_at,
        captured.work | {"captured_at_utc": "2026-10-04T01:02:03Z"},
        captured.attempt,
        {},
        captured.line_counts,
    )
    assert _output_dates(captured_in_work) == {"2026-10-04"}
    invalid = CommittedOutput(
        captured.work_id,
        captured.attempt_id,
        captured.publication_sequence,
        captured.published_at,
        captured.work,
        captured.attempt,
        {"captured_at_utc": "2026-10-05T01:02:03"},
        captured.line_counts,
    )
    with pytest.raises(GoldSourceError, match="invalid UTC timestamp"):
        _output_dates(invalid)

    undated = CommittedOutput(
        output.work_id,
        output.attempt_id,
        output.publication_sequence,
        None,
        output.work,
        output.attempt,
        output.run,
        output.line_counts,
    )
    assert _output_dates(undated) == set()
    assert (
        _fact_rows_for_date(
            [undated], {}, "2026-10-06", "2026-10-06T04:00:00Z"
        )["gold_video"]
        == []
    )
    assert (
        _fact_rows_for_date(
            [undated, output], {}, "2026-10-06", "2026-10-06T04:00:00Z"
        )["gold_video"][0]["work_id"]
        == "work"
    )
    missing_dimensions = CommittedOutput(
        output.work_id,
        output.attempt_id,
        output.publication_sequence,
        output.published_at,
        {
            "config_sha256": "a" * 64,
            "duration_seconds": 7.0,
        },
        output.attempt,
        output.run,
        output.line_counts,
    )
    assert (
        _fact_rows_for_date(
            [missing_dimensions], {}, "2026-10-06", "2026-10-06T04:00:00Z"
        )["gold_video"]
        == []
    )
    dimensions = _dimension_rows(
        [missing_dimensions, output],
        {name: [] for name in FACT_TABLES},
        "2026-10-06T04:00:00Z",
    )
    source_rows = _dimension_source_rows([missing_dimensions, output])
    assert [row["work_id"] for row in source_rows] == ["work"]
    assert source_rows[0]["captured_at_utc"] == "2026-10-06T03:16:00Z"
    assert _dimension_fact_dates(
        {
            name: (
                [{"capture_date": "2026-10-06"}]
                if name == "gold_video"
                else []
            )
            for name in FACT_TABLES
        }
    ) == ["2026-10-06"]
    assert [row["work_id"] for row in dimensions["gold_dim_video"]] == ["work"]
    assert (
        dimensions["gold_dim_video"][0]["captured_at_utc"]
        == "2026-10-06T03:16:00Z"
    )
    missing_camera = CommittedOutput(
        output.work_id,
        output.attempt_id,
        output.publication_sequence,
        output.published_at,
        output.work | {"camera_id": None},
        output.attempt,
        output.run,
        output.line_counts,
    )
    missing_location = CommittedOutput(
        output.work_id,
        output.attempt_id,
        output.publication_sequence,
        output.published_at,
        output.work | {"location_id": None},
        output.attempt,
        output.run,
        output.line_counts,
    )
    undated_with_dimensions = CommittedOutput(
        output.work_id,
        output.attempt_id,
        output.publication_sequence,
        None,
        output.work,
        output.attempt,
        output.run,
        output.line_counts,
    )
    assert all(
        _dimension_rows([candidate], {name: [] for name in FACT_TABLES}, published.isoformat())[
            "gold_dim_video"
        ]
        == []
        for candidate in (
            missing_camera,
            missing_location,
            undated_with_dimensions,
        )
    )


def test_dimension_source_helpers_cover_optional_benchmark_metadata():
    published = datetime(2026, 10, 6, 3, 16, tzinfo=timezone.utc)

    def output(work):
        return CommittedOutput(
            "work",
            "attempt",
            1,
            published,
            work,
            {},
            {},
            (),
        )

    valid = output({"camera_id": "camera", "location_id": "location"})
    missing_camera = output({"location_id": "location"})
    missing_location = output({"camera_id": "camera"})
    undated = CommittedOutput(
        "undated",
        "attempt-undated",
        2,
        None,
        {"camera_id": "camera", "location_id": "location"},
        {},
        {},
        (),
    )
    assert _dimension_source_rows(
        [missing_camera, valid, missing_location, undated]
    ) == [
        {
            "camera_id": "camera",
            "location_id": "location",
            "work_id": "work",
            "captured_at_utc": "2026-10-06T03:16:00Z",
        }
    ]
    assert _dimension_fact_dates(
        {
            "gold_flow_minute": [{"flow_date": "2026-10-05"}],
            "gold_flow_hour": [{"flow_date": None}],
            "gold_video": [{"capture_date": "2026-10-06"}],
            "gold_operations_hour": [{"operation_date": "2026-10-05"}],
        }
    ) == ["2026-10-05", "2026-10-06"]


def test_dimension_fact_dates_reads_each_fact_table_column():
    assert _dimension_fact_dates(
        {
            "gold_flow_minute": [{"flow_date": "2026-10-01"}],
            "gold_flow_hour": [{"flow_date": "2026-10-02"}],
            "gold_video": [{"capture_date": "2026-10-03"}],
            "gold_operations_hour": [{"operation_date": "2026-10-04"}],
        }
    ) == [
        "2026-10-01",
        "2026-10-02",
        "2026-10-03",
        "2026-10-04",
    ]


def test_delta_output_decoder_uses_active_session_and_verifies_records():
    records = [
        {
            "work_id": "work",
            "attempt_id": "attempt",
            "record_type": "video_result",
            "record_sequence": 0,
            "payload_json": json.dumps({"captured_at_utc": "2026-01-01T00:00:00Z"}),
            "processing_seconds": 1.0,
            "processed_frames": 1,
            "status": "SUCCEEDED",
        }
    ]

    class Reader:
        def format(self, value):
            assert value == "delta"
            return self

        def load(self, value):
            assert value.endswith("attempt")
            return self

        def where(self, value):
            assert "work_id" in value or "attempt_id" in value
            return self

        def select(self, value):
            assert value == "record_json"
            return self

        def collect(self):
            return [{"record_json": json.dumps(records[0])}]

    active = types.SimpleNamespace(read=Reader())
    spark_session = types.SimpleNamespace(
        getActiveSession=lambda: active,
    )
    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = spark_session
    pyspark.sql = pyspark_sql

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setitem(sys.modules, "pyspark", pyspark)
        monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)
        document = _load_delta_output_document(
            Path("/delta/attempt"),
            _sha256_json(records),
            verify_hash=True,
            work_id="work",
            attempt_id="attempt",
        )
    assert document["run"]["work_id"] == "work"
    assert document["run"]["attempt_id"] == "attempt"
    assert document["run"]["captured_at_utc"] == "2026-01-01T00:00:00Z"


def test_delta_output_decoder_rejects_missing_identity_and_bad_digest():
    with pytest.raises(GoldSourceError):
        _load_delta_output_document(
            Path("/delta/attempt"),
            "",
            verify_hash=False,
            work_id=None,
            attempt_id="attempt",
        )

    class Reader:
        def format(self, value):
            return self

        def load(self, value):
            return self

        def where(self, value):
            return self

        def select(self, value):
            return self

        def collect(self):
            return [
                {
                    "record_json": json.dumps(
                        {
                            "work_id": "work",
                            "attempt_id": "attempt",
                            "record_type": "video_result",
                            "record_sequence": 0,
                            "payload_json": "{}",
                        }
                    )
                }
            ]

    active = types.SimpleNamespace(read=Reader())
    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = types.SimpleNamespace(
        getActiveSession=lambda: active,
    )
    pyspark.sql = pyspark_sql
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setitem(sys.modules, "pyspark", pyspark)
        monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)
        with pytest.raises(GoldSourceError, match="digest mismatch"):
            _load_delta_output_document(
                Path("/delta/attempt"),
                "0" * 64,
                verify_hash=True,
                work_id="work",
                attempt_id="attempt",
            )


class GoldFixture:
    def __init__(self, root):
        self.root = root
        self.database = root / "control.sqlite3"
        self.control = SQLiteControlStore(
            self.database,
            root / "control-content",
            clock=lambda: NOW.timestamp(),
            id_factory=self._next_id,
        )
        self.identifiers = iter(f"id-{index}" for index in range(100))
        self.gold_root = root / "gold"

    def _next_id(self):
        return next(self.identifiers)

    def register(self, work_id, *, captured_at="2026-10-01T23:59:30Z"):
        self.control.register(
            work_id,
            {
                "asset_id": f"asset-{work_id}",
                "asset_version": "v1",
                "camera_id": "camera-a",
                "location_id": "lobby",
                "camera_timezone": "UTC",
                "captured_at_utc": captured_at,
                "duration_seconds": 120.0,
                "config_sha256": "config-a",
                "config_json": {
                    "pipeline": "rtdetr",
                    "batch_size": 4,
                    "line": [[0, 1], [2, 3]],
                },
            },
            runtime_key="runtime",
            duration_seconds=120,
            config_sha256="config-a",
            release_digest="release-a",
            available_at=NOW.timestamp(),
        )

    def publish(self, work_id, *, commit=True, lines=None):
        self.register(work_id)
        batch = self.control.claim(
            "driver",
            max_items=1,
            minimum_items=1,
            lease_seconds=1000,
            minimum_speed_x=1,
            safety_factor=1,
            margin_seconds=1,
        )
        assert batch is not None
        item = batch.items[0]
        output = {
            "run": {
                "work_id": work_id,
                "attempt_id": item.attempt_id,
                "captured_at_utc": "2026-10-01T23:59:30Z",
                "camera_id": "camera-a",
                "location_id": "lobby",
                "config_sha256": "config-a",
                "duration_seconds": 120.0,
                "processing_seconds": 30.0,
                "distinct_people": 3,
                "line_in_count": 4,
                "line_out_count": 1,
            },
            "line_counts": lines
            if lines is not None
            else [
                {
                    "work_id": work_id,
                    "attempt_id": item.attempt_id,
                    "observed_at_utc": "2026-10-01T23:59:50Z",
                    "frame_in_count": 2,
                    "frame_out_count": 1,
                },
                {
                    "work_id": work_id,
                    "attempt_id": item.attempt_id,
                    "observed_at_utc": "2026-10-02T00:00:10Z",
                    "frame_in_count": 2,
                    "frame_out_count": 0,
                },
            ],
        }
        path = self.root / f"{item.attempt_id}.json"
        content = json.dumps(output, sort_keys=True).encode()
        path.write_bytes(content)
        digest = hashlib.sha256(content).hexdigest()
        envelope = self.control.load_claim_envelope(batch.batch_id)
        self.control.seal_batch(
            batch.batch_id,
            [
                {
                    "work_id": work_id,
                    "attempt_id": item.attempt_id,
                    "output_path": str(path),
                    "output_sha256": digest,
                    "records": [
                        {
                            "executor_identity": "executor",
                            "partition_id": 0,
                            "task_attempt_id": 0,
                            "record_sequence": 0,
                        }
                    ],
                }
            ],
            envelope_sha256=batch.envelope_sha256,
            membership_sha256=envelope["membership_sha256"],
        )
        if commit:
            self.control.commit_batch(batch.batch_id)
        return item.attempt_id

    def job(self):
        return LocalGoldJob(
            self.database,
            self.gold_root,
            clock=lambda: NOW,
        )

    def rewrite_output(self, attempt_id, update):
        path = self.root / f"{attempt_id}.json"
        payload = json.loads(path.read_text())
        update(payload)
        content = json.dumps(payload, sort_keys=True).encode()
        path.write_bytes(content)
        digest = hashlib.sha256(content).hexdigest()
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "UPDATE attempts SET output_sha256 = ? WHERE attempt_id = ?",
                (digest, attempt_id),
            )
            connection.execute(
                "UPDATE publications SET output_sha256 = ? WHERE attempt_id = ?",
                (digest, attempt_id),
            )


@pytest.fixture
def gold_fixture():
    with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
        yield GoldFixture(Path(temporary))


def test_only_committed_pointer_output_is_visible(gold_fixture):
    committed_attempt = gold_fixture.publish("committed")
    gold_fixture.publish(
        "sealed-only",
        commit=False,
        lines=[
            {
                "observed_at_utc": "2026-10-02T00:00:20Z",
                "frame_in_count": 99,
                "frame_out_count": 0,
            }
        ],
    )

    outputs = gold_fixture.job().committed_outputs()

    assert [(item.work_id, item.attempt_id) for item in outputs] == [
        ("committed", committed_attempt)
    ]


def test_planning_and_fact_build_cover_cross_midnight_dates(gold_fixture):
    gold_fixture.publish("work-1")
    job = gold_fixture.job()

    plan = job.plan()
    outcome = job.build_facts(dates=plan.items)

    assert plan.items == ("2026-10-01", "2026-10-02")
    assert outcome["rows"] == {
        "gold_flow_minute": 2,
        "gold_flow_hour": 2,
        "gold_video": 1,
        "gold_operations_hour": 1,
        "gold_work_operations": 1,
        "gold_attempt_operations": 1,
    }
    minute = job.store.read_table("gold_flow_minute")
    assert [(row["flow_date"], row["entries"], row["exits"]) for row in minute] == [
        ("2026-10-01", 2, 1),
        ("2026-10-02", 2, 0),
    ]
    assert job.store.read_table("gold_video")[0]["speed_x_realtime"] == 4.0
    operations = job.store.read_table("gold_operations_hour")
    assert operations[0]["queued"] == 1
    assert operations[0]["started"] == 1
    assert operations[0]["succeeded"] == 1
    work = job.store.read_table("gold_work_operations")[0]
    assert work["status"] == "SUCCEEDED"
    assert work["queue_entered_at"] == "2026-10-02T17:00:00Z"
    attempt = job.store.read_table("gold_attempt_operations")[0]
    assert attempt["status"] == "SUCCEEDED"
    assert attempt["processing_seconds"] == 30.0
    assert attempt["distinct_people"] == 3
    assert attempt["line_in_count"] == 4
    assert attempt["line_out_count"] == 1


def test_operational_rows_map_stable_queue_and_dead_letter_states() -> None:
    control = {
        "work": [
            {
                "work_id": "retry",
                "payload_json": json.dumps(
                    {
                        "source_video": "abfss://video.mp4",
                        "captured_at_utc": "2026-10-02T16:00:00Z",
                    }
                ),
                "status": "READY",
                "attempt_count": 1,
                "max_attempts": 3,
                "available_at": NOW.timestamp() + 60,
                "created_at": NOW.timestamp() - 120,
                "updated_at": NOW.timestamp(),
                "replay_generation": 0,
            },
            {
                "work_id": "dead",
                "payload_json": "{}",
                "status": "DEAD",
                "attempt_count": 3,
                "max_attempts": 3,
                "available_at": NOW.timestamp(),
                "created_at": NOW.timestamp() - 300,
                "updated_at": NOW.timestamp(),
                "last_error": "processing failed",
                "replay_generation": 0,
            },
        ],
        "attempts": [],
    }

    result = _operational_rows(control, [], NOW.isoformat())

    assert tuple(result) == OPERATIONAL_TABLES
    dead, retry = result["gold_work_operations"]
    assert dead["work_id"] == "dead"
    assert dead["status"] == "DEAD_LETTERED"
    assert dead["last_error_category"] == "PROCESSING"
    assert dead["last_error_type"] == "DeadLettered"
    assert dead["completed_at"] == "2026-10-02T17:00:00Z"
    assert retry["work_id"] == "retry"
    assert retry["status"] == "RETRY_WAIT"
    assert retry["source_uri"] == "abfss://video.mp4"
    assert retry["capture_date"] == "2026-10-02"
    assert _operational_rows({}, [], NOW.isoformat()) == {
        "gold_work_operations": [],
        "gold_attempt_operations": [],
    }


def test_operational_rows_project_the_complete_report_contract() -> None:
    created = NOW.timestamp() - 300
    sealed = NOW.timestamp() - 60
    published = NOW.timestamp() - 30
    payload = {
        "asset_id": "asset-1",
        "asset_version": "v2",
        "source_uri": "abfss://video.mp4",
        "manifest_uri": "abfss://manifest.json",
        "source_etag": "etag-1",
        "expected_size_bytes": 1234,
        "expected_sha256": "a" * 64,
        "camera_id": "camera-1",
        "location_id": "lobby",
        "captured_at_utc": "2026-10-02T16:00:00Z",
        "camera_timezone": "UTC",
        "duration_seconds": 120.0,
        "priority": 7,
        "config_json": {"pipeline": "rtdetr"},
        "source_fps": 30.0,
        "total_source_frames": 3600,
    }
    terminal_payload = {
        "pipeline_run_id": "pipeline-1",
        "activity_run_id": "activity-1",
        "fabric_job_instance_id": "job-1",
        "sdk_version": "0.9.56",
        "bundle_manifest_sha256": "b" * 64,
        "config_sha256": "config-1",
        "input_sha256": "c" * 64,
        "source_size_bytes": 1234,
        "source_duration_seconds": 120.0,
        "source_fps": 30.0,
        "total_source_frames": 3600,
        "processed_frames": 120,
        "effective_sample_fps": 1.0,
        "processing_seconds": 40.0,
        "distinct_people": 5,
        "line_in_count": 4,
        "line_out_count": 2,
        "retryable": True,
        "error_category": "TRANSIENT",
        "error_type": "TimeoutError",
        "error_message": "worker timed out",
        "captured_at_utc": "2026-10-02T16:00:00Z",
        "worker_execution_id": "executor-1",
    }
    control = {
        "work": [
            {
                "work_id": "work-1",
                "payload_json": json.dumps(payload),
                "status": "LEASED",
                "attempt_count": 1,
                "max_attempts": 3,
                "available_at": created,
                "lease_owner": "dispatcher-1",
                "lease_attempt_id": "attempt-1",
                "lease_expires_at": NOW.timestamp() + 600,
                "committed_attempt_id": None,
                "config_sha256": "config-1",
                "created_at": created,
                "updated_at": NOW.timestamp(),
                "last_replay_id": "replay-1",
                "replay_generation": 2,
            }
        ],
        "attempts": [
            {
                "attempt_id": "attempt-1",
                "work_id": "work-1",
                "dispatcher_id": "dispatcher-1",
                "status": "FAILED",
                "created_at": created,
                "sealed_at": sealed,
                "published_at": published,
                "records_json": json.dumps(
                    [
                        {
                            "record_type": "error",
                            "payload_json": json.dumps(terminal_payload),
                        }
                    ]
                ),
            }
        ],
    }

    result = _operational_rows(control, [], NOW.isoformat())

    assert result["gold_work_operations"] == [
        {
            "work_id": "work-1",
            "asset_id": "asset-1",
            "asset_version": "v2",
            "source_uri": "abfss://video.mp4",
            "manifest_uri": "abfss://manifest.json",
            "source_etag": "etag-1",
            "expected_size_bytes": 1234,
            "expected_sha256": "a" * 64,
            "camera_id": "camera-1",
            "location_id": "lobby",
            "captured_at_utc": "2026-10-02T16:00:00Z",
            "camera_timezone": "UTC",
            "duration_seconds": 120.0,
            "priority": 7,
            "status": "LEASED",
            "received_at": "2026-10-02T16:55:00Z",
            "queued_at": "2026-10-02T16:55:00Z",
            "not_before_at": "2026-10-02T16:55:00Z",
            "attempt_count": 1,
            "max_attempts": 3,
            "lease_owner_attempt_id": "attempt-1",
            "lease_dispatcher_id": "dispatcher-1",
            "lease_acquired_at": "2026-10-02T16:55:00Z",
            "lease_expires_at": "2026-10-02T17:10:00Z",
            "last_heartbeat_at": None,
            "committed_attempt_id": None,
            "completed_at": None,
            "last_error_category": None,
            "last_error_type": None,
            "last_error_message": None,
            "config_json": '{"pipeline":"rtdetr"}',
            "config_sha256": "config-1",
            "capture_date": "2026-10-02",
            "last_replay_id": "replay-1",
            "replay_generation": 2,
            "queue_entered_at": "2026-10-02T16:55:00Z",
        }
    ]
    assert result["gold_attempt_operations"] == [
        {
            "attempt_id": "attempt-1",
            "work_id": "work-1",
            "dispatcher_id": "dispatcher-1",
            "pipeline_run_id": "pipeline-1",
            "activity_run_id": "activity-1",
            "fabric_job_instance_id": "job-1",
            "sdk_version": "0.9.56",
            "bundle_manifest_sha256": "b" * 64,
            "config_sha256": "config-1",
            "status": "FAILED",
            "claimed_at": "2026-10-02T16:55:00Z",
            "staging_started_at": "2026-10-02T16:55:00Z",
            "inference_started_at": "2026-10-02T16:55:00Z",
            "writing_started_at": "2026-10-02T16:59:00Z",
            "completed_at": "2026-10-02T16:59:30Z",
            "last_heartbeat_at": "2026-10-02T16:59:00Z",
            "input_sha256": "c" * 64,
            "source_size_bytes": 1234,
            "source_duration_seconds": 120.0,
            "source_fps": 30.0,
            "total_source_frames": 3600,
            "processed_frames": 120,
            "effective_sample_fps": 1.0,
            "processing_seconds": 40.0,
            "distinct_people": 5,
            "line_in_count": 4,
            "line_out_count": 2,
            "retryable": True,
            "error_category": "TRANSIENT",
            "error_type": "TimeoutError",
            "error_message": "worker timed out",
            "capture_date": "2026-10-02",
            "worker_execution_id": "executor-1",
        }
    ]


@pytest.mark.parametrize(
    ("records_json", "message"),
    [
        ("{", "records_json is invalid"),
        ("{}", "must be a JSON row list"),
        (
            json.dumps(
                [
                    {"record_type": "error", "payload_json": "{}"},
                    {"record_type": "video_result", "payload_json": "{}"},
                ]
            ),
            "multiple terminal rows",
        ),
        (
            json.dumps(
                [{"record_type": "error", "payload_json": "{"}]
            ),
            "terminal payload_json is invalid",
        ),
        (
            json.dumps(
                [{"record_type": "error", "payload_json": "[]"}]
            ),
            "terminal payload_json must be a JSON object",
        ),
    ],
)
def test_operational_attempt_projection_rejects_invalid_record_contract(
    records_json: str,
    message: str,
) -> None:
    control = {
        "work": [
            {
                "work_id": "work",
                "payload_json": "{}",
                "status": "READY",
                "attempt_count": 1,
                "max_attempts": 3,
                "available_at": NOW.timestamp(),
                "created_at": NOW.timestamp(),
                "updated_at": NOW.timestamp(),
                "replay_generation": 0,
            }
        ],
        "attempts": [
            {
                "attempt_id": "attempt",
                "work_id": "work",
                "status": "FAILED",
                "created_at": NOW.timestamp(),
                "records_json": records_json,
            }
        ],
    }

    with pytest.raises(GoldSourceError, match=message):
        _operational_rows(control, [], NOW.isoformat())


def test_sdk_relative_line_timestamp_is_derived_from_capture_time(gold_fixture):
    gold_fixture.publish(
        "relative-line",
        lines=[
            {
                "video_seconds": "45.5",
                "frame_in_count": 1,
                "frame_out_count": 0,
            }
        ],
    )
    job = gold_fixture.job()

    assert job.plan().items == ("2026-10-01", "2026-10-02")
    job.build_facts(dates=["2026-10-02"])
    row = job.store.read_table("gold_flow_minute")[0]
    assert row["minute_utc"] == "2026-10-02T00:00:00Z"
    assert row["entries"] == 1


def test_uncommitted_claim_changes_only_operations_source(gold_fixture):
    gold_fixture.publish("committed")
    job = gold_fixture.job()
    job.run()
    flow_before = job.source_checkpoint()
    operations_before = job.operations_source_checkpoint()

    gold_fixture.register("uncommitted")
    gold_fixture.control.claim(
        "driver",
        max_items=1,
        minimum_items=1,
        lease_seconds=1000,
        minimum_speed_x=1,
        safety_factor=1,
        margin_seconds=1,
    )

    assert job.source_checkpoint() == flow_before
    assert job.operations_source_checkpoint() != operations_before
    assert job.build_facts()["skipped"] is False
    operations = job.store.read_table("gold_operations_hour")
    assert sum(row["queued"] for row in operations) == 2
    assert sum(row["started"] for row in operations) == 2
    assert sum(row["succeeded"] for row in operations) == 1


def test_empty_partition_replacement_removes_stale_rows(gold_fixture):
    gold_fixture.publish("work-1")
    job = gold_fixture.job()
    stale = {
        "minute_utc": "2026-10-03T00:00:00Z",
        "flow_date": "2026-10-03",
    }
    job.store.replace_partition(
        "gold_flow_minute", "flow_date", "2026-10-03", [stale]
    )

    result = job.build_facts(dates=["2026-10-03"])

    assert result["rows"]["gold_flow_minute"] == 0
    assert job.store.read_table("gold_flow_minute") == []


def test_dimensions_and_referential_validation(gold_fixture):
    gold_fixture.publish("work-1")
    job = gold_fixture.job()
    job.build_facts(dates=["2026-10-01", "2026-10-02"])

    result = job.build_dimensions()

    assert result["rows"]["gold_dim_time"] == 1440
    assert job.store.read_table("gold_dim_camera")[0]["camera_id"] == "camera-a"
    assert job.store.read_table("gold_dim_video")[0]["work_id"] == "work-1"
    assert job.store.read_table("gold_dim_model_config")[0]["pipeline"] == "rtdetr"
    assert job.validate()["rules_checked"] == 25


def test_validation_rejects_an_unresolved_fact_key(gold_fixture):
    gold_fixture.publish("work-1")
    job = gold_fixture.job()
    job.build_facts(dates=["2026-10-01", "2026-10-02"])
    job.build_dimensions()
    rows = job.store.read_table("gold_video")
    rows[0]["camera_id"] = "missing-camera"
    job.store.replace_table("gold_video", rows)

    with pytest.raises(GoldValidationError, match="unresolved"):
        job.validate()


def test_validation_rejects_null_dimension_primary_key(gold_fixture):
    gold_fixture.publish("work-1")
    job = gold_fixture.job()
    job.build_facts(dates=["2026-10-01", "2026-10-02"])
    job.build_dimensions()
    cameras = job.store.read_table("gold_dim_camera")
    cameras[0]["camera_id"] = None
    job.store.replace_table("gold_dim_camera", cameras)

    with pytest.raises(GoldValidationError, match="non-null and unique"):
        job.validate()


def test_automatic_rerun_uses_source_and_target_checkpoints(gold_fixture):
    gold_fixture.publish("work-1")
    job = gold_fixture.job()

    first_facts = job.build_facts()
    first_dimensions = job.build_dimensions()
    fact_versions = job.store.versions(
        (
            "gold_flow_minute",
            "gold_flow_hour",
            "gold_video",
            "gold_operations_hour",
        )
    )

    assert first_facts["skipped"] is False
    assert first_dimensions["skipped"] is False
    assert job.build_facts()["skipped"] is True
    assert job.build_dimensions()["skipped"] is True
    assert job.store.versions(fact_versions) == fact_versions


def test_run_builds_both_stages_and_validates(gold_fixture):
    gold_fixture.publish("work-1")

    result = gold_fixture.job().run()

    assert result["facts"]["skipped"] is False
    assert result["dimensions"]["skipped"] is False
    assert result["validation"]["valid"] is True
    assert result["outbox_id"] == 1
    assert result["pending_refreshes"] == 1
    assert (
        gold_fixture.job().state.pending_refreshes()[0]["payload"]["reason"]
        == "gold run changed"
    )


def test_run_rerun_is_noop_without_refresh_or_target_churn(gold_fixture):
    gold_fixture.publish("work-1")
    job = gold_fixture.job()
    first = job.run()
    versions = job.store.versions(FACT_TABLES + DIMENSION_TABLES)
    with sqlite3.connect(gold_fixture.database) as connection:
        checkpoints = connection.execute(
            "SELECT * FROM gold_checkpoints ORDER BY stage"
        ).fetchall()

    second = job.run()

    assert first["outbox_id"] == 1
    assert second["facts"]["skipped"] is True
    assert second["dimensions"]["skipped"] is True
    assert second["outbox_id"] is None
    assert second["pending_refreshes"] == 1
    assert job.store.versions(FACT_TABLES + DIMENSION_TABLES) == versions
    with sqlite3.connect(gold_fixture.database) as connection:
        assert (
            connection.execute(
                "SELECT * FROM gold_checkpoints ORDER BY stage"
            ).fetchall()
            == checkpoints
        )


def test_run_dimension_only_repair_enqueues_one_refresh(gold_fixture):
    gold_fixture.publish("work-1")
    job = gold_fixture.job()
    job.run()
    cameras = job.store.read_table("gold_dim_camera")
    job.store.replace_table("gold_dim_camera", cameras)

    result = job.run()

    assert result["facts"]["skipped"] is True
    assert result["dimensions"]["skipped"] is False
    assert result["outbox_id"] == 2
    assert result["pending_refreshes"] == 2


def test_run_forwards_planning_and_maintenance_options(gold_fixture, monkeypatch):
    job = gold_fixture.job()
    facts = MagicMock(return_value={"skipped": True})
    dimensions = MagicMock(return_value={"skipped": True})
    monkeypatch.setattr(job, "build_facts", facts)
    monkeypatch.setattr(job, "build_dimensions", dimensions)
    monkeypatch.setattr(job, "validate", lambda: {"valid": True})

    job.run(lookback_hours=73, force=True, full_rebuild=True)
    job.run()

    assert facts.call_args_list == [
        call(
            lookback_hours=73,
            force=True,
            full_rebuild=True,
            save_state=False,
        ),
        call(
            lookback_hours=48,
            force=False,
            full_rebuild=False,
            save_state=False,
        ),
    ]
    assert dimensions.call_args_list == [
        call(full_rebuild=True, force=True, save_state=False),
        call(full_rebuild=False, force=False, save_state=False),
    ]


def test_run_combines_facts_only_checkpoint_update(gold_fixture, monkeypatch):
    job = gold_fixture.job()
    source = SourceCheckpoint(1, {"source": "version"})
    checkpoint = (source, {"gold_video": 1})
    monkeypatch.setattr(
        job,
        "build_facts",
        lambda **_kwargs: {
            "skipped": False,
            "_checkpoint_updates": {"facts": checkpoint},
        },
    )
    monkeypatch.setattr(
        job,
        "build_dimensions",
        lambda **_kwargs: {"skipped": True},
    )
    monkeypatch.setattr(job, "validate", lambda: {"valid": True})
    monkeypatch.setattr(job, "source_checkpoint", lambda: source)
    monkeypatch.setattr(job, "operations_source_checkpoint", lambda: source)
    monkeypatch.setattr(job.store, "versions", lambda _names: {"gold_video": 1})
    save = MagicMock(return_value=1)
    monkeypatch.setattr(job.state, "save_checkpoints_and_enqueue", save)
    monkeypatch.setattr(job.state, "pending_refreshes", lambda: [])

    result = job.run()

    assert result["outbox_id"] == 1
    save.assert_called_once()
    assert save.call_args.args[0] == {"facts": checkpoint}


def test_refresh_outbox_is_deduplicated_and_locally_acknowledged(gold_fixture):
    gold_fixture.publish("work-1")
    job = gold_fixture.job()
    job.build_facts(dates=["2026-10-01"])
    job.build_facts(dates=["2026-10-01"])

    pending = job.state.pending_refreshes()

    assert len(pending) == 2
    assert pending[0]["payload"]["reason"] == "gold facts changed"
    assert job.state.acknowledge_refresh(pending[0]["outbox_id"], "test") is True
    assert job.state.acknowledge_refresh(pending[0]["outbox_id"], "test") is False
    assert len(job.state.pending_refreshes()) == 1


def test_cli_help_is_spark_free_and_fabric_placeholder_fails_closed(
    gold_fixture, capsys
):
    with pytest.raises(SystemExit) as exit_info:
        main(["--help"])

    assert exit_info.value.code == 0
    assert "build-facts" in capsys.readouterr().out
    with pytest.raises(UnsupportedGoldBackendError, match="unsupported"):
        FabricGoldStore()


def test_fabric_gold_facades_require_explicit_spark_and_forward(
    monkeypatch,
):
    store_impl = MagicMock()
    store_impl.read_table.return_value = [{"id": 1}]
    source_impl = MagicMock()
    source_impl.committed_outputs.return_value = ["output"]
    state_impl = MagicMock()
    state_impl.pending_refreshes.return_value = ["refresh"]
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_gold.FabricGoldStoreImpl",
        lambda *_args, **_kwargs: store_impl,
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_gold.FabricCommittedSource",
        lambda *_args, **_kwargs: source_impl,
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_gold.FabricGoldState",
        lambda *_args, **_kwargs: state_impl,
    )

    assert FabricGoldStore(object()).read_table("gold_video") == [{"id": 1}]
    assert FabricGoldSource(object(), object()).committed_outputs() == ["output"]
    assert FabricGoldState(object()).pending_refreshes() == ["refresh"]


def test_checkpoint_target_matching_fails_closed_and_requires_exact_versions():
    current = {"gold_video": 4}

    assert _checkpoint_targets_match(None, current) is False
    assert (
        _checkpoint_targets_match(
            {"target_versions_json": "not-json"}, current
        )
        is False
    )
    assert (
        _checkpoint_targets_match(
            {"target_versions_json": json.dumps(current)}, current
        )
        is True
    )
    assert (
        _checkpoint_targets_match(
            {"target_versions_json": json.dumps({"gold_video": 3})}, current
        )
        is False
    )


def test_local_store_rejects_cross_partition_rows(gold_fixture):
    store = LocalJsonGoldStore(gold_fixture.gold_root)

    with pytest.raises(ValueError, match="outside"):
        store.replace_partition(
            "gold_video",
            "capture_date",
            "2026-10-01",
            [{"capture_date": "2026-10-02"}],
        )


def test_first_and_forced_plans_include_history_outside_lookback(gold_fixture):
    gold_fixture.publish("work-1")
    job = gold_fixture.job()

    assert job.plan(lookback_hours=1).items == ("2026-10-01", "2026-10-02")
    job.build_facts()

    assert job.plan(lookback_hours=1, reset=True).items == (
        "2026-10-01",
        "2026-10-02",
    )


def test_plan_includes_claimed_sealed_and_completed_operational_dates(
    gold_fixture,
):
    attempt_id = gold_fixture.publish("work-1")
    with sqlite3.connect(gold_fixture.database) as connection:
        batch_id = connection.execute(
            "SELECT batch_id FROM attempts WHERE attempt_id = ?",
            (attempt_id,),
        ).fetchone()[0]
        connection.execute(
            "UPDATE attempts SET created_at = ?, sealed_at = ? "
            "WHERE attempt_id = ?",
            (
                datetime(2026, 10, 2, 12, tzinfo=timezone.utc).timestamp(),
                datetime(2026, 10, 3, 12, tzinfo=timezone.utc).timestamp(),
                attempt_id,
            ),
        )
        completed = datetime(
            2026, 10, 4, 12, tzinfo=timezone.utc
        ).timestamp()
        connection.execute(
            "UPDATE batches SET sealed_at = ?, committed_at = ? "
            "WHERE batch_id = ?",
            (
                datetime(2026, 10, 3, 12, tzinfo=timezone.utc).timestamp(),
                completed,
                batch_id,
            ),
        )
        connection.execute(
            "UPDATE publications SET published_at = ? WHERE attempt_id = ?",
            (completed, attempt_id),
        )

    assert gold_fixture.job().plan().items == (
        "2026-10-01",
        "2026-10-02",
        "2026-10-03",
        "2026-10-04",
    )


def test_explicit_partition_build_does_not_advance_complete_checkpoint(
    gold_fixture,
):
    gold_fixture.publish("work-1")
    job = gold_fixture.job()

    result = job.build_facts(dates=["2026-10-01"])

    assert result["complete_checkpoint_advanced"] is False
    assert job.state.checkpoint("facts") is None
    assert job.state.checkpoint("facts:partition:2026-10-01") is not None
    automatic = job.build_facts()
    assert automatic["skipped"] is False
    assert automatic["complete_checkpoint_advanced"] is True
    assert job.state.checkpoint("facts") is not None


def test_changed_or_deleted_fact_target_prevents_skip(gold_fixture):
    gold_fixture.publish("work-1")
    job = gold_fixture.job()
    job.build_facts()
    job.store.replace_table("gold_flow_minute", [])

    result = job.build_facts()

    assert result["skipped"] is False
    assert len(job.store.read_table("gold_flow_minute")) == 2


def test_refresh_ack_can_require_the_observed_dedupe_key(gold_fixture):
    gold_fixture.publish("work-1")
    job = gold_fixture.job()
    job.build_facts()
    pending = job.state.pending_refreshes()[0]

    assert (
        job.state.acknowledge_refresh(
            pending["outbox_id"],
            "test",
            expected_dedupe_key="not-the-observed-key",
        )
        is False
    )
    assert (
        job.state.acknowledge_refresh(
            pending["outbox_id"],
            "test",
            expected_dedupe_key=pending["dedupe_key"],
        )
        is True
    )


@pytest.mark.parametrize(
    "update",
    [
        lambda payload: payload["run"].__setitem__("captured_at_utc", "2026-10-01T23:59:30"),
        lambda payload: payload["run"].__setitem__("processing_seconds", True),
        lambda payload: payload["run"].__setitem__("distinct_people", float("inf")),
        lambda payload: payload["line_counts"][0].__setitem__("frame_in_count", -1),
    ],
)
def test_source_validation_rejects_naive_or_invalid_metrics(
    gold_fixture, update
):
    attempt_id = gold_fixture.publish("work-1")
    gold_fixture.rewrite_output(attempt_id, update)

    with pytest.raises(GoldSourceError):
        gold_fixture.job().build_facts()


def test_dimension_build_rejects_invalid_camera_timezone(gold_fixture):
    attempt_id = gold_fixture.publish("work-1")
    gold_fixture.rewrite_output(
        attempt_id,
        lambda payload: payload["run"].__setitem__(
            "camera_timezone", "Mars/Olympus_Mons"
        ),
    )
    job = gold_fixture.job()
    job.build_facts()

    with pytest.raises(GoldSourceError, match="invalid IANA timezone"):
        job.build_dimensions()
