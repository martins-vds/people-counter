from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
import subprocess
import sys
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from people_counter.fabric_sjd_cutover import (
    CA_SHADOW_ONLY_TABLES,
    LEGACY_COMMITTED_VIEWS,
    LEGACY_TABLE_ALLOWLIST,
    WRITER_ITEM_IDS,
    CutoverError,
    SourceTableState,
    SourceViewState,
    StoppedWriterGate,
    WriterState,
    archive_legacy_tables,
    classify_ca_table,
    classify_legacy_table,
    classify_work_payload,
    complete_synthetic_cleanup,
    create_stable_tables,
    frame_content_sha256,
    append_journal_once,
    archive_and_delete_synthetic_work,
    archive_ca_tables,
    _history_json_default,
    _history_json,
    migrate_durable_rows,
    retire_archived_ca_tables,
    retire_archived_legacy_tables,
    legacy_retirement_bundle_path,
    retirement_bundle_path,
    sha256,
    stable_table_schemas,
    synthetic_work_inventory,
    validate_retirement_archives,
    validate_stopped_writer_gate,
)
from people_counter.fabric_sjd_cutover_jobs import (
    _stopped_gate,
    _stopped_gate_path,
    _work_evidence,
)
import people_counter.fabric_sjd_cutover_jobs as cutover_jobs


def _gate(
    *,
    routing: int = 0,
    sources: tuple[SourceTableState, ...] | None = None,
) -> StoppedWriterGate:
    if sources is None:
        sources = (
            SourceTableState(
                "people_counter_ca_work",
                True,
                7,
                0,
                "a" * 64,
            ),
        )
    writers = tuple(
        WriterState(item_id, "Completed") for item_id in sorted(WRITER_ITEM_IDS)
    )
    unsigned = {
        "captured_at": 100.0,
        "routing_to_ca": routing,
        "source_tables": [asdict(item) for item in sources],
        "writer_states": [asdict(item) for item in writers],
    }
    return StoppedWriterGate(
        captured_at=100.0,
        writer_states=writers,
        source_tables=sources,
        routing_to_ca=routing,
        evidence_sha256=sha256(unsigned),
    )


def test_spark_builder_starts_session_when_none_is_active() -> None:
    built = object()
    builder = MagicMock()
    builder.getOrCreate.return_value = built
    session = SimpleNamespace(
        getActiveSession=MagicMock(return_value=None),
        builder=builder,
    )
    with patch.dict(
        sys.modules,
        {"pyspark.sql": SimpleNamespace(SparkSession=session)},
    ):
        assert cutover_jobs._spark() is built
    session.getActiveSession.assert_called_once_with()
    builder.getOrCreate.assert_called_once_with()


def test_spark_reuses_active_session_without_builder() -> None:
    active = object()
    builder = MagicMock()
    session = SimpleNamespace(
        getActiveSession=MagicMock(return_value=active),
        builder=builder,
    )
    with patch.dict(
        sys.modules,
        {"pyspark.sql": SimpleNamespace(SparkSession=session)},
    ):
        assert cutover_jobs._spark() is active
    builder.getOrCreate.assert_not_called()


def test_stable_schema_is_complete_and_excludes_shadow_only_names() -> None:
    schemas = stable_table_schemas()
    assert "work" in schemas
    assert "gold_video" in schemas
    assert "migration_journal" in schemas
    assert "retirement_journal" in schemas
    assert "routing_allowlist" not in schemas
    assert "shadow_audit" not in schemas
    assert all("candidate" not in suffix for suffix in schemas)


@pytest.mark.parametrize("name", sorted(CA_SHADOW_ONLY_TABLES))
def test_shadow_tables_are_evidence_only(name: str) -> None:
    assert classify_ca_table(name) == "SHADOW_ONLY"


def test_ca_table_classification_is_fixed() -> None:
    assert (
        classify_ca_table("people_counter_ca_work")
        == "TRANSFERABLE_DURABLE_SCHEMA"
    )
    assert classify_ca_table("people_counter_ca_locks") == "EVIDENCE_ONLY"
    with pytest.raises(CutoverError, match="outside Candidate A"):
        classify_ca_table("people_counter_sjd_work")


def test_legacy_table_classification_is_fixed() -> None:
    assert (
        classify_legacy_table("people_counter_executor_partition_records")
        == "EXECUTOR_BENCHMARK"
    )
    assert classify_legacy_table("people_counter_gold_video") == "REPORTING"
    assert classify_legacy_table("people_counter_control_writer") == "CONTROL"
    assert classify_legacy_table("people_counter_video_work") == "PROCESSING"
    with pytest.raises(CutoverError, match="outside legacy allowlist"):
        classify_legacy_table("people_counter_sjd_work")


@pytest.mark.parametrize(
    ("payload", "classification"),
    [
        (
            {"source_video": "Files/incoming/2026/10/real.mp4"},
            "DURABLE_PRODUCTION",
        ),
        (
            {"source_video": "/lakehouse/default/Files/incoming/real.mp4"},
            "DURABLE_PRODUCTION",
        ),
        (
            {"source_video": "Files/_benchmark/video.mp4"},
            "SYNTHETIC_OR_SHADOW",
        ),
        (
            {"source_video": "Files/incoming/synthetic-video.mp4"},
            "SYNTHETIC_OR_SHADOW",
        ),
        ({"source_video": "Files/unknown/video.mp4"}, "UNCLASSIFIED_REFUSE"),
    ],
)
def test_work_payload_classification(
    payload: dict[str, object], classification: str
) -> None:
    assert classify_work_payload(json.dumps(payload)) == classification


def test_stopped_writer_gate_accepts_exact_fresh_cas_snapshot() -> None:
    gate = _gate()
    validate_stopped_writer_gate(gate, gate.source_tables, now=101.0)


@pytest.mark.parametrize("captured_at", [float("nan"), 102.0])
def test_stopped_writer_gate_refuses_nonfinite_or_future_time(
    captured_at: float,
) -> None:
    gate = _gate()
    invalid = StoppedWriterGate(
        captured_at=captured_at,
        writer_states=gate.writer_states,
        source_tables=gate.source_tables,
        routing_to_ca=gate.routing_to_ca,
        evidence_sha256=gate.evidence_sha256,
    )

    with pytest.raises(CutoverError, match="future-dated or stale"):
        validate_stopped_writer_gate(
            invalid,
            invalid.source_tables,
            now=101.0,
        )


@pytest.mark.parametrize(
    ("change", "match"),
    [
        ("active", "not stopped"),
        ("routing", "routing"),
        ("stale", "stale"),
        ("source", "changed"),
        ("hash", "hash differs"),
    ],
)
def test_stopped_writer_gate_fails_closed(change: str, match: str) -> None:
    gate = _gate(routing=1 if change == "routing" else 0)
    observed = gate.source_tables
    now = 1000.1 if change == "stale" else 101.0
    if change == "active":
        gate = StoppedWriterGate(
            gate.captured_at,
            (
                WriterState(next(iter(WRITER_ITEM_IDS)), "Running"),
                *tuple(
                    item
                    for item in gate.writer_states
                    if item.item_id != next(iter(WRITER_ITEM_IDS))
                ),
            ),
            gate.source_tables,
            gate.routing_to_ca,
            gate.evidence_sha256,
        )
    elif change == "source":
        observed = (
            SourceTableState(
                "people_counter_ca_work", True, 8, 0, "a" * 64
            ),
        )
    elif change == "hash":
        gate = StoppedWriterGate(
            gate.captured_at,
            gate.writer_states,
            gate.source_tables,
            gate.routing_to_ca,
            "0" * 64,
        )
    with pytest.raises(CutoverError, match=match):
        validate_stopped_writer_gate(gate, observed, now=now)


@pytest.mark.parametrize("duplicate_state", ["Completed", "Running"])
def test_stopped_writer_gate_refuses_duplicate_writer_evidence(
    duplicate_state: str,
) -> None:
    gate = _gate()
    duplicate = WriterState(gate.writer_states[0].item_id, duplicate_state)
    unsigned = {
        "captured_at": gate.captured_at,
        "routing_to_ca": gate.routing_to_ca,
        "source_tables": [asdict(item) for item in gate.source_tables],
        "writer_states": [
            asdict(item) for item in (*gate.writer_states, duplicate)
        ],
    }
    duplicated = StoppedWriterGate(
        captured_at=gate.captured_at,
        writer_states=(*gate.writer_states, duplicate),
        source_tables=gate.source_tables,
        routing_to_ca=gate.routing_to_ca,
        evidence_sha256=sha256(unsigned),
    )

    with pytest.raises(CutoverError, match="not unique"):
        validate_stopped_writer_gate(
            duplicated,
            duplicated.source_tables,
            now=101.0,
        )


def test_gate_base64_round_trip_and_rejects_malformed() -> None:
    gate = _gate()
    encoded = base64.b64encode(
        json.dumps(asdict(gate), sort_keys=True).encode("utf-8")
    ).decode("ascii")
    assert _stopped_gate(encoded) == gate
    with pytest.raises(ValueError, match="invalid"):
        _stopped_gate("not-base64")


def test_work_evidence_and_fabric_gate_path_are_exact() -> None:
    assert _work_evidence(["sjd-live-0944-test=" + "a" * 64]) == {
        "sjd-live-0944-test": "a" * 64
    }
    with pytest.raises(ValueError, match="duplicate"):
        _work_evidence(["work=a", "work=b"])
    with pytest.raises(ValueError, match="work_id=payload_sha256"):
        _work_evidence(["work"])
    with pytest.raises(ValueError, match="outside Fabric evidence"):
        _stopped_gate_path("Files/other/gate.json")
    sentinel = _gate()
    with patch.object(
        cutover_jobs,
        "_read_immutable_json",
        return_value={"schema": "gate"},
    ), patch.object(
        cutover_jobs,
        "_stopped_gate",
        return_value=sentinel,
    ) as decode:
        observed = _stopped_gate_path(
            f"{cutover_jobs.EVIDENCE_ROOT}/gates/gate.json"
        )
    assert observed is sentinel
    decode.assert_called_once()


@pytest.mark.parametrize("command", ["archive-legacy", "retire-legacy"])
def test_parser_exposes_explicit_legacy_retirement_commands(
    command: str,
) -> None:
    arguments = cutover_jobs._parser().parse_args(
        [
            command,
            "--bundle-id",
            "v20261010T000000Z-012345abcdef",
            "--gate-path",
            f"{cutover_jobs.EVIDENCE_ROOT}/gates/gate.json",
        ]
    )
    assert arguments.command == command
    assert arguments.bundle_id == "v20261010T000000Z-012345abcdef"
    assert arguments.gate_path.endswith("/gates/gate.json")
    for missing in ("--bundle-id", "--gate-path"):
        values = [
            command,
            "--bundle-id",
            "v20261010T000000Z-012345abcdef",
            "--gate-path",
            f"{cutover_jobs.EVIDENCE_ROOT}/gates/gate.json",
        ]
        position = values.index(missing)
        del values[position : position + 2]
        with pytest.raises(SystemExit):
            cutover_jobs._parser().parse_args(values)
    assert cutover_jobs._parser().prog == "pc-production-schema-cutover"


def test_frame_content_sha256_hashes_sorted_canonical_rows() -> None:
    expression = MagicMock()
    functions = SimpleNamespace(
        col=lambda name: f"column:{name}",
        struct=lambda *columns: ("struct", columns),
        to_json=lambda *_args, **_kwargs: expression,
    )
    ordered = SimpleNamespace(
        toLocalIterator=lambda: iter(
            [
                {"canonical_row": '{"a":1}'},
                {"canonical_row": '{"a":2}'},
            ]
        )
    )
    projected = SimpleNamespace(orderBy=lambda _name: ordered)
    frame = SimpleNamespace(
        columns=["a"],
        select=lambda _expression: projected,
    )
    with patch.dict(
        sys.modules,
        {"pyspark.sql": SimpleNamespace(functions=functions)},
    ):
        observed = frame_content_sha256(frame)

    expected = __import__("hashlib").sha256(
        b'{"a":1}\n{"a":2}\n'
    ).hexdigest()
    assert observed == expected
    expression.alias.assert_called_once_with("canonical_row")


def test_retirement_bundle_path_is_versioned_and_fixed() -> None:
    value = retirement_bundle_path("v20261008T000000Z-012345abcdef")
    assert value.endswith("/v20261008T000000Z-012345abcdef")
    with pytest.raises(CutoverError, match="canonical"):
        retirement_bundle_path("latest")


def test_legacy_retirement_allowlists_cover_every_live_writer_and_table() -> None:
    from people_counter.fabric_production_migration_live import LEGACY_TABLES

    assert len(LEGACY_TABLE_ALLOWLIST) == 30
    assert len(set(LEGACY_TABLE_ALLOWLIST)) == 30
    assert set(LEGACY_TABLES) == set(LEGACY_TABLE_ALLOWLIST)
    assert {
        "5618506e-aed9-43cd-8943-d92322ef4d4a",
        "9169409b-82dc-4a3e-afad-611220b9930a",
        "ff9203a8-655b-4fd0-aed5-a46f1ed0ad3c",
    } <= WRITER_ITEM_IDS
    assert len(WRITER_ITEM_IDS) == 12
    assert len(LEGACY_COMMITTED_VIEWS) == 3
    assert legacy_retirement_bundle_path(
        "v20261010T000000Z-012345abcdef"
    ).endswith("/legacy/v20261010T000000Z-012345abcdef")


def test_real_local_delta_additive_schema_has_exact_readback() -> None:
    script = """
from people_counter.fabric_sjd_cutover import create_stable_tables, stable_table_schemas
from people_counter.local_spark import create_local_spark_session
spark = create_local_spark_session(
    master="local[2]",
    app_name="stable-sjd-schema-readback",
    correlation_id="stable-sjd-schema-readback",
)
database = "people_counter_sjd_cutover_test"
try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database}`")
    spark.sql(f"USE `{database}`")
    first = create_stable_tables(spark)
    second = create_stable_tables(spark)
    assert first == second
    assert len(first) == len(stable_table_schemas())
    assert set(first) == {
        f"people_counter_sjd_{suffix}" for suffix in stable_table_schemas()
    }
    locks = spark.table("people_counter_sjd_locks").collect()
    assert len(locks) == 1
    assert locks[0].asDict(recursive=True) == {
        "lock_name": "global",
        "owner_id": None,
        "acquired_at": None,
    }
finally:
    spark.sql("USE default")
    spark.sql(f"DROP DATABASE IF EXISTS `{database}` CASCADE")
    spark.stop()
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr


class _Row(dict):
    def asDict(self, recursive: bool = False) -> dict[str, object]:
        return {
            key: (
                value.asDict(recursive=True)
                if recursive and isinstance(value, _NestedRow)
                else value
            )
            for key, value in self.items()
        }


class _NestedRow:
    def __init__(self, **values: object):
        self.values = values

    def asDict(self, recursive: bool = False) -> dict[str, object]:
        return dict(self.values)


class _Rows:
    def __init__(self, rows: list[_Row]):
        self.rows = rows

    def limit(self, _value: int):
        return self

    def collect(self) -> list[_Row]:
        return self.rows


def test_delta_history_json_is_recursive_strict_and_datetime_safe() -> None:
    value = _history_json(
        _Row(
            timestamp=datetime(2026, 10, 8, tzinfo=timezone.utc),
            operationMetrics=_NestedRow(numOutputRows="2"),
        )
    )
    assert value == {
        "operationMetrics": {"numOutputRows": "2"},
        "timestamp": "2026-10-08T00:00:00+00:00",
    }
    with pytest.raises(ValueError, match="Out of range float"):
        _history_json(_Row(metric=float("nan")))
    with pytest.raises(
        TypeError,
        match="^unsupported Delta history value: object$",
    ):
        _history_json_default(object())


class _Schema:
    def __init__(self, value: str):
        self.value = value

    def json(self) -> str:
        return json.dumps({"ddl": self.value}, sort_keys=True)


def test_create_stable_tables_exact_mock_readback_and_provider_failure() -> None:
    schemas: dict[str, _Schema] = {}
    current: list[_Schema] = []

    class Spark:
        def createDataFrame(self, _rows, schema):
            value = _Schema(schema)
            current[:] = [value]
            return SimpleNamespace(schema=value)

        def sql(self, statement):
            if statement.startswith("CREATE TABLE"):
                name = statement.split("`")[1]
                schemas[name] = current[0]
                return _Rows([])
            return _Rows(
                [
                    _Row(
                        format="delta",
                        properties={
                            "people_counter.cutover_id": (
                                "people_counter_sjd_0001"
                            ),
                            "people_counter.namespace": "stable-sjd",
                        },
                    )
                ]
            )

        def table(self, name):
            return SimpleNamespace(
                schema=schemas[name],
                limit=lambda _count: _Rows(
                    [
                        _Row(
                            lock_name="global",
                            owner_id=None,
                            acquired_at=None,
                        )
                    ]
                ),
            )

    result = create_stable_tables(Spark())
    assert len(result) == len(stable_table_schemas())
    assert all(len(value) == 64 for value in result.values())

    class BadSpark(Spark):
        def sql(self, statement):
            value = super().sql(statement)
            if statement.startswith("DESCRIBE DETAIL"):
                return _Rows([_Row(format="parquet", properties={})])
            return value

    with pytest.raises(CutoverError, match="provider/properties"):
        create_stable_tables(BadSpark())


def test_create_stable_tables_seeds_global_lock_transactionally() -> None:
    schema = _Schema(
        "lock_name string, owner_id string, acquired_at timestamp"
    )
    lock_rows: list[_Row] = []
    observed: dict[str, object] = {}

    class Writer:
        def __init__(self, rows: list[tuple[object, ...]]):
            self.rows = rows
            self.options: dict[str, object] = {}

        def format(self, value: str):
            assert value == "delta"
            return self

        def mode(self, value: str):
            assert value == "append"
            return self

        def option(self, key: str, value: object):
            self.options[key] = value
            return self

        def saveAsTable(self, name: str) -> None:
            assert name == "people_counter_sjd_locks"
            assert self.rows == [("global", None, None)]
            observed["options"] = self.options
            lock_rows.append(
                _Row(lock_name="global", owner_id=None, acquired_at=None)
            )

    class Table:
        def __init__(self):
            self.schema = schema

        def limit(self, count: int) -> _Rows:
            assert count == 2
            return _Rows(list(lock_rows))

    class Spark:
        catalog = SimpleNamespace(
            refreshTable=lambda name: observed.setdefault("refresh", name)
        )

        def createDataFrame(self, rows, schema):
            return SimpleNamespace(
                schema=_Schema(schema),
                write=Writer(list(rows)),
            )

        def sql(self, statement: str):
            if statement.startswith("CREATE TABLE"):
                return _Rows([])
            return _Rows(
                [
                    _Row(
                        format="delta",
                        properties={
                            "people_counter.cutover_id": (
                                "people_counter_sjd_0001"
                            ),
                            "people_counter.namespace": "stable-sjd",
                        },
                    )
                ]
            )

        def table(self, name: str) -> Table:
            assert name == "people_counter_sjd_locks"
            return Table()

    with patch(
        "people_counter.fabric_sjd_cutover.stable_table_schemas",
        return_value={
            "locks": "lock_name string, owner_id string, acquired_at timestamp"
        },
    ):
        create_stable_tables(Spark())
    assert observed == {
        "options": {
            "txnAppId": "people_counter_sjd_0001:bootstrap-lock",
            "txnVersion": "0",
        },
        "refresh": "people_counter_sjd_locks",
    }


@pytest.mark.parametrize(
    "rows",
    [
        [_Row(lock_name="global", owner_id="writer", acquired_at=None)],
        [_Row(lock_name="wrong", owner_id=None, acquired_at=None)],
        [
            _Row(lock_name="global", owner_id=None, acquired_at=None),
            _Row(lock_name="global", owner_id=None, acquired_at=None),
        ],
    ],
)
def test_create_stable_tables_rejects_invalid_global_lock_rows(
    rows: list[_Row],
) -> None:
    schema = _Schema(
        "lock_name string, owner_id string, acquired_at timestamp"
    )

    class Spark:
        def createDataFrame(self, _rows, schema):
            return SimpleNamespace(schema=_Schema(schema))

        def sql(self, statement: str):
            if statement.startswith("CREATE TABLE"):
                return _Rows([])
            return _Rows(
                [
                    _Row(
                        format="delta",
                        properties={
                            "people_counter.cutover_id": (
                                "people_counter_sjd_0001"
                            ),
                            "people_counter.namespace": "stable-sjd",
                        },
                    )
                ]
            )

        def table(self, _name: str):
            return SimpleNamespace(
                schema=schema,
                limit=lambda count: _Rows(rows) if count == 2 else None,
            )

    with (
        patch(
            "people_counter.fabric_sjd_cutover.stable_table_schemas",
            return_value={
                "locks": (
                    "lock_name string, owner_id string, "
                    "acquired_at timestamp"
                )
            },
        ),
        pytest.raises(CutoverError, match="global lock row"),
    ):
        create_stable_tables(Spark())


def test_append_journal_once_creates_and_reuses_exact_row() -> None:
    stored: list[_Row] = []
    created_at = datetime.now(timezone.utc)
    persisted_created_at = created_at.astimezone(timezone.utc).replace(tzinfo=None)

    class Frame:
        def where(self, _condition):
            return self

        def limit(self, _value):
            return self

        def collect(self):
            return stored

    class Writer:
        def format(self, _value):
            return self

        def mode(self, _value):
            return self

        def option(self, _name, _value):
            return self

        def saveAsTable(self, _name):
            stored[:] = [_Row({**expected, "created_at": persisted_created_at})]

    class Spark:
        def table(self, _name):
            return Frame()

        def createDataFrame(self, rows, schema):
            assert schema == stable_table_schemas()["migration_journal"]
            assert rows == [{**expected, "created_at": persisted_created_at}]
            return SimpleNamespace(write=Writer())

    expected = {
        "journal_id": "journal-1",
        "cutover_id": "people_counter_sjd_0001",
        "phase": "bootstrap",
        "status": "SUCCEEDED",
        "source_snapshot_sha256": "a" * 64,
        "target_snapshot_sha256": "b" * 64,
        "evidence_sha256": "c" * 64,
        "previous_evidence_sha256": "d" * 64,
        "created_at": created_at,
    }
    append_journal_once(
        Spark(),
        suffix="migration_journal",
        journal_id="journal-1",
        row={key: value for key, value in expected.items() if key != "journal_id"},
    )
    assert stored[0]["created_at"] == persisted_created_at
    append_journal_once(
        Spark(),
        suffix="migration_journal",
        journal_id="journal-1",
        row={
            **{key: value for key, value in expected.items() if key != "journal_id"},
            "created_at": created_at.astimezone(timezone.utc),
        },
    )
    stored[0]["status"] = "CONFLICT"
    with pytest.raises(CutoverError, match="journal conflict"):
        append_journal_once(
            Spark(),
            suffix="migration_journal",
            journal_id="journal-1",
            row={
                key: value for key, value in expected.items() if key != "journal_id"
            },
        )


def test_append_journal_once_rejects_naive_timestamp() -> None:
    class Frame:
        def where(self, _condition):
            return self

        def limit(self, _value):
            return self

        def collect(self):
            return []

    class Spark:
        def table(self, _name):
            return Frame()

    with pytest.raises(CutoverError, match="created_at must be timezone-aware"):
        append_journal_once(
            Spark(),
            suffix="retirement_journal",
            journal_id="retirement-1",
            row={
                "bundle_id": "retirement-1",
                "status": "RETIRED",
                "manifest_path": "Files/manifest.json",
                "manifest_sha256": "a" * 64,
                "zero_writer_proof_sha256": "b" * 64,
                "zero_routing_proof_sha256": "c" * 64,
                "rollback_proof_sha256": "d" * 64,
                "created_at": datetime.now(),
            },
        )


def test_migrate_durable_rows_empty_and_nonempty_fail_closed() -> None:
    source = (
        SourceTableState(
            "people_counter_ca_work", True, 1, 0, "a" * 64
        ),
    )
    gate = _gate()
    frame = MagicMock()
    frame.where.return_value.collect.return_value = []
    spark = SimpleNamespace(table=MagicMock(return_value=frame))
    with patch(
        "people_counter.fabric_sjd_cutover.snapshot_ca_tables",
        return_value=source,
    ), patch(
        "people_counter.fabric_sjd_cutover.validate_stopped_writer_gate"
    ):
        result = migrate_durable_rows(
            spark,
            durable_work_ids=[],
            gate=gate,
            source_names=["people_counter_ca_work"],
        )
        assert all(value == 0 for value in result.values())
        frame.where.return_value.collect.return_value = [
            _Row(
                work_id="real-1",
                payload_json=json.dumps(
                    {"source_video": "Files/incoming/real.mp4"}
                ),
            )
        ]
        with pytest.raises(CutoverError, match="table-specific graph"):
            migrate_durable_rows(
                spark,
                durable_work_ids=["real-1"],
                gate=gate,
                source_names=["people_counter_ca_work"],
            )
        frame.where.return_value.collect.return_value = [
            _Row(
                work_id="synthetic-1",
                payload_json=json.dumps(
                    {"source_video": "Files/_benchmark/synthetic.mp4"}
                ),
            )
        ]
        with pytest.raises(CutoverError, match="non-production"):
            migrate_durable_rows(
                spark,
                durable_work_ids=["synthetic-1"],
                gate=gate,
                source_names=["people_counter_ca_work"],
            )


def test_archive_ca_tables_copies_existing_and_records_missing() -> None:
    schema_json = '{"type":"struct","fields":[]}'
    schema_digest = __import__("hashlib").sha256(schema_json.encode()).hexdigest()
    states = (
        SourceTableState(
            "people_counter_ca_work", True, 3, 2, schema_digest
        ),
        SourceTableState("people_counter_ca_locks", False, None, 0, None),
    )
    writer = MagicMock()
    writer.format.return_value.mode.return_value.save.return_value = None
    source = SimpleNamespace(write=writer)
    archived = SimpleNamespace(
        count=lambda: 2,
        schema=SimpleNamespace(json=lambda: schema_json),
    )
    spark = SimpleNamespace(
        table=MagicMock(return_value=source),
        read=SimpleNamespace(
            format=lambda _value: SimpleNamespace(load=lambda _path: archived)
        ),
        sql=MagicMock(
            return_value=_Rows(
                [
                    _Row(
                        version=3,
                        operation="WRITE",
                        timestamp=datetime(2026, 10, 8, tzinfo=timezone.utc),
                    )
                ]
            )
        ),
    )
    with patch(
        "people_counter.fabric_sjd_cutover.snapshot_ca_tables",
        return_value=states,
    ) as snapshot, patch(
        "people_counter.fabric_sjd_cutover.frame_content_sha256",
        return_value="c" * 64,
    ):
        result = archive_ca_tables(
            spark,
            bundle_id="v20261008T000000Z-012345abcdef",
            table_names=[item.name for item in states],
        )
    assert result["manifest_sha256"]
    assert result["tables"][0]["classification"] == (
        "TRANSFERABLE_DURABLE_SCHEMA"
    )
    assert result["tables"][0]["history"][0]["timestamp"] == (
        "2026-10-08T00:00:00+00:00"
    )
    assert result["tables"][1]["classification"] == "EVIDENCE_ONLY"
    snapshot.assert_called_once_with(
        spark,
        [item.name for item in states],
    )
    writer.format.return_value.mode.return_value.save.assert_called_once_with(
        "Files/people-counter/sjd/v1/retirements/candidate-a/"
        "v20261008T000000Z-012345abcdef/tables/"
        "people_counter_ca_work"
    )


def test_archive_ca_tables_refuses_content_mismatch() -> None:
    schema_json = '{"type":"struct","fields":[]}'
    schema_digest = __import__("hashlib").sha256(schema_json.encode()).hexdigest()
    state = SourceTableState(
        "people_counter_ca_work", True, 3, 2, schema_digest
    )
    writer = MagicMock()
    source = SimpleNamespace(write=writer)
    archived = SimpleNamespace(
        count=lambda: 2,
        schema=SimpleNamespace(json=lambda: schema_json),
    )
    spark = SimpleNamespace(
        table=lambda _name: source,
        read=SimpleNamespace(
            format=lambda _value: SimpleNamespace(load=lambda _path: archived)
        ),
    )
    with patch(
        "people_counter.fabric_sjd_cutover.snapshot_ca_tables",
        return_value=(state,),
    ), patch(
        "people_counter.fabric_sjd_cutover.frame_content_sha256",
        side_effect=("a" * 64, "b" * 64),
    ):
        with pytest.raises(CutoverError, match="content differs"):
            archive_ca_tables(
                spark,
                bundle_id="v20261008T000000Z-012345abcdef",
                table_names=[state.name],
            )


def test_archive_ca_tables_reuses_matching_partial_archive() -> None:
    schema_json = '{"type":"struct","fields":[]}'
    schema_digest = __import__("hashlib").sha256(schema_json.encode()).hexdigest()
    state = SourceTableState(
        "people_counter_ca_work", True, 3, 2, schema_digest
    )
    source = SimpleNamespace(write=MagicMock())
    archived = SimpleNamespace(
        count=lambda: 2,
        schema=SimpleNamespace(json=lambda: schema_json),
    )
    spark = SimpleNamespace(
        table=MagicMock(return_value=source),
        read=SimpleNamespace(
            format=lambda _value: SimpleNamespace(load=lambda _path: archived)
        ),
        sql=MagicMock(return_value=_Rows([])),
    )
    with patch(
        "people_counter.fabric_sjd_cutover.snapshot_ca_tables",
        return_value=(state,),
    ), patch(
        "people_counter.fabric_sjd_cutover.frame_content_sha256",
        return_value="c" * 64,
    ):
        result = archive_ca_tables(
            spark,
            bundle_id="v20261008T000000Z-012345abcdef",
            table_names=[state.name],
            path_exists=lambda _path: True,
        )

    assert result["manifest_sha256"]
    assert not source.write.method_calls


def test_archive_legacy_tables_captures_exact_view_definitions() -> None:
    schema_json = '{"type":"struct","fields":[]}'
    schema_digest = __import__("hashlib").sha256(schema_json.encode()).hexdigest()
    states = tuple(
        SourceTableState(
            name,
            name == "people_counter_video_work",
            3 if name == "people_counter_video_work" else None,
            2 if name == "people_counter_video_work" else 0,
            schema_digest if name == "people_counter_video_work" else None,
        )
        for name in sorted(LEGACY_TABLE_ALLOWLIST)
    )
    views = tuple(
        SourceViewState(
            name,
            name == "people_counter_runs_committed",
            (
                "CREATE VIEW people_counter_runs_committed AS SELECT 1"
                if name == "people_counter_runs_committed"
                else None
            ),
            (
                __import__("hashlib").sha256(
                    b"CREATE VIEW people_counter_runs_committed AS SELECT 1"
                ).hexdigest()
                if name == "people_counter_runs_committed"
                else None
            ),
        )
        for name in sorted(LEGACY_COMMITTED_VIEWS)
    )
    writer = MagicMock()
    writer.format.return_value.mode.return_value.save.return_value = None
    source = SimpleNamespace(write=writer)
    archived = SimpleNamespace(
        count=lambda: 2,
        schema=SimpleNamespace(json=lambda: schema_json),
    )
    spark = SimpleNamespace(
        table=MagicMock(return_value=source),
        read=SimpleNamespace(
            format=lambda _value: SimpleNamespace(load=lambda _path: archived)
        ),
        sql=MagicMock(
            return_value=_Rows(
                [
                    _Row(
                        version=3,
                        operation="WRITE",
                        timestamp=datetime(2026, 10, 10, tzinfo=timezone.utc),
                    )
                ]
            )
        ),
    )
    with patch(
        "people_counter.fabric_sjd_cutover.snapshot_legacy_tables",
        return_value=states,
    ), patch(
        "people_counter.fabric_sjd_cutover.snapshot_legacy_views",
        return_value=views,
    ), patch(
        "people_counter.fabric_sjd_cutover.frame_content_sha256",
        return_value="c" * 64,
    ):
        result = archive_legacy_tables(
            spark,
            bundle_id="v20261010T000000Z-012345abcdef",
            table_names=LEGACY_TABLE_ALLOWLIST,
            view_names=LEGACY_COMMITTED_VIEWS,
        )

    assert result["schema"] == "people-counter-legacy-retirement-v1"
    assert result["views"] == [asdict(view) for view in views]
    writer.format.return_value.mode.return_value.save.assert_called_once_with(
        "Files/people-counter/sjd/v1/retirements/legacy/"
        "v20261010T000000Z-012345abcdef/tables/"
        "people_counter_video_work"
    )


def test_archive_legacy_tables_refuses_partial_allowlists() -> None:
    with pytest.raises(CutoverError, match="table allowlist differs"):
        archive_legacy_tables(
            SimpleNamespace(),
            bundle_id="v20261010T000000Z-012345abcdef",
            table_names=LEGACY_TABLE_ALLOWLIST[:-1],
        )
    with pytest.raises(CutoverError, match="view allowlist differs"):
        archive_legacy_tables(
            SimpleNamespace(),
            bundle_id="v20261010T000000Z-012345abcdef",
            table_names=LEGACY_TABLE_ALLOWLIST,
            view_names=LEGACY_COMMITTED_VIEWS[:-1],
        )


def test_archive_legacy_tables_wires_exact_archive_contract() -> None:
    spark = object()
    path_exists = object()
    states = tuple(
        SourceTableState(name, False, None, 0, None)
        for name in sorted(LEGACY_TABLE_ALLOWLIST)
    )
    views = tuple(
        SourceViewState(name, False, None, None)
        for name in sorted(LEGACY_COMMITTED_VIEWS)
    )
    expected = {"manifest_sha256": "a" * 64}
    with patch(
        "people_counter.fabric_sjd_cutover.snapshot_legacy_tables",
        return_value=states,
    ) as snapshot_tables, patch(
        "people_counter.fabric_sjd_cutover.snapshot_legacy_views",
        side_effect=(views, views),
    ) as snapshot_views, patch(
        "people_counter.fabric_sjd_cutover._archive_tables",
        return_value=expected,
    ) as archive:
        observed = archive_legacy_tables(
            spark,
            bundle_id="v20261010T000000Z-012345abcdef",
            table_names=LEGACY_TABLE_ALLOWLIST,
            view_names=LEGACY_COMMITTED_VIEWS,
            path_exists=path_exists,
        )

    assert observed is expected
    snapshot_tables.assert_called_once_with(spark, LEGACY_TABLE_ALLOWLIST)
    assert snapshot_views.call_args_list == [
        call(spark, LEGACY_COMMITTED_VIEWS),
        call(spark, LEGACY_COMMITTED_VIEWS),
    ]
    archive.assert_called_once_with(
        spark,
        bundle_id="v20261010T000000Z-012345abcdef",
        table_names=LEGACY_TABLE_ALLOWLIST,
        root=(
            "Files/people-counter/sjd/v1/retirements/legacy/"
            "v20261010T000000Z-012345abcdef"
        ),
        states=states,
        classification=classify_legacy_table,
        manifest_schema="people-counter-legacy-retirement-v1",
        views=views,
        path_exists=path_exists,
    )


def test_validate_retirement_archives_proves_exact_rollback_copy() -> None:
    schema_json = '{"type":"struct","fields":[]}'
    schema_digest = __import__("hashlib").sha256(schema_json.encode()).hexdigest()
    states = (
        SourceTableState(
            "people_counter_ca_work", True, 3, 2, schema_digest
        ),
        SourceTableState(
            "people_counter_ca_locks", False, None, 0, None
        ),
    )
    archived = SimpleNamespace(
        count=lambda: 2,
        schema=SimpleNamespace(json=lambda: schema_json),
    )
    spark = SimpleNamespace(
        read=SimpleNamespace(
            format=lambda _value: SimpleNamespace(load=lambda _path: archived)
        )
    )
    manifest = {
        "bundle_id": "v20261008T000000Z-012345abcdef",
        "tables": [
            {
                **asdict(states[0]),
                "archive_path": (
                    "Files/people-counter/sjd/v1/retirements/candidate-a/"
                    "v20261008T000000Z-012345abcdef/tables/"
                    "people_counter_ca_work"
                ),
                "content_sha256": "c" * 64,
            },
            {
                **asdict(states[1]),
                "archive_path": None,
                "content_sha256": None,
            },
        ],
    }
    manifest["manifest_sha256"] = sha256(manifest)

    with patch(
        "people_counter.fabric_sjd_cutover.frame_content_sha256",
        return_value="c" * 64,
    ):
        proof = validate_retirement_archives(spark, manifest, states)

    assert proof["rollback_proof_sha256"]
    assert proof["tables"][0]["restore_sql"].startswith(
        "CREATE TABLE `people_counter_ca_work`"
    )
    wrong_path = json.loads(json.dumps(manifest))
    wrong_path["tables"][0]["archive_path"] = (
        "Files/people-counter/sjd/v1/retirements/candidate-a/"
        "v20261008T000000Z-012345abcdef/tables/wrong"
    )
    wrong_path["manifest_sha256"] = sha256(
        {
            key: value
            for key, value in wrong_path.items()
            if key != "manifest_sha256"
        }
    )
    with pytest.raises(CutoverError, match="path is invalid"):
        validate_retirement_archives(spark, wrong_path, states)


def test_validate_legacy_archives_includes_view_restore_sql() -> None:
    state = SourceTableState(
        "people_counter_video_work", False, None, 0, None
    )
    views = [
        SourceViewState(name, False, None, None)
        for name in LEGACY_COMMITTED_VIEWS
    ]
    views[0] = SourceViewState(
        views[0].name,
        True,
        f"CREATE VIEW {views[0].name} AS SELECT 1",
        __import__("hashlib").sha256(
            f"CREATE VIEW {views[0].name} AS SELECT 1".encode()
        ).hexdigest(),
    )
    manifest = {
        "bundle_id": "v20261010T000000Z-012345abcdef",
        "schema": "people-counter-legacy-retirement-v1",
        "tables": [
            {
                **asdict(state),
                "archive_path": None,
                "content_sha256": None,
            }
        ],
        "views": [asdict(view) for view in views],
    }
    manifest["manifest_sha256"] = sha256(manifest)

    proof = validate_retirement_archives(
        SimpleNamespace(),
        manifest,
        (state,),
    )

    assert proof["schema"] == "people-counter-legacy-rollback-proof-v1"
    assert proof["views"] == [
        {
            "create_sql": views[0].create_sql,
            "definition_sha256": views[0].definition_sha256,
            "name": views[0].name,
        }
    ]
    assert proof["rollback_proof_sha256"]


def test_validate_legacy_archives_refuses_ambiguous_view_evidence() -> None:
    state = SourceTableState(
        "people_counter_video_work", False, None, 0, None
    )
    views = [
        asdict(SourceViewState(name, False, None, None))
        for name in LEGACY_COMMITTED_VIEWS
    ]
    base = {
        "bundle_id": "v20261010T000000Z-012345abcdef",
        "schema": "people-counter-legacy-retirement-v1",
        "tables": [
            {
                **asdict(state),
                "archive_path": None,
                "content_sha256": None,
            }
        ],
        "views": views,
    }
    cases: list[tuple[dict[str, object], str]] = []
    duplicate = json.loads(json.dumps(base))
    duplicate["views"].append(dict(duplicate["views"][0]))
    cases.append((duplicate, "view allowlist differs"))
    partial = json.loads(json.dumps(base))
    partial["views"][0]["create_sql"] = "CREATE VIEW partial AS SELECT 1"
    cases.append((partial, "missing legacy view"))
    malformed = json.loads(json.dumps(base))
    malformed["views"][0] = {
        "name": LEGACY_COMMITTED_VIEWS[0],
        "exists": True,
        "create_sql": "SELECT 1",
        "definition_sha256": "a" * 64,
    }
    cases.append((malformed, "definition evidence differs"))

    for manifest, message in cases:
        manifest["manifest_sha256"] = sha256(manifest)
        with pytest.raises(CutoverError, match=message):
            validate_retirement_archives(
                SimpleNamespace(),
                manifest,
                (state,),
            )


def test_validate_retirement_archives_refuses_changed_source() -> None:
    state = SourceTableState(
        "people_counter_ca_work", True, 3, 2, "a" * 64
    )
    manifest = {
        "bundle_id": "v20261008T000000Z-012345abcdef",
        "tables": [
            {
                **asdict(state),
                "row_count": 1,
                "archive_path": (
                    "Files/people-counter/sjd/v1/retirements/candidate-a/"
                    "v20261008T000000Z-012345abcdef/tables/"
                    "people_counter_ca_work"
                ),
                "content_sha256": "c" * 64,
            }
        ]
    }
    manifest["manifest_sha256"] = sha256(manifest)

    with pytest.raises(CutoverError, match="source differs"):
        validate_retirement_archives(SimpleNamespace(), manifest, (state,))


def test_retire_archived_ca_tables_drops_only_verified_existing_tables() -> None:
    before = (
        SourceTableState(
            "people_counter_ca_work", True, 3, 0, "a" * 64
        ),
        SourceTableState(
            "people_counter_ca_locks", False, None, 0, None
        ),
    )
    after = tuple(
        SourceTableState(state.name, False, None, 0, None)
        for state in before
    )
    spark = SimpleNamespace(
        sql=MagicMock(),
        catalog=SimpleNamespace(tableExists=lambda _name: False),
    )
    rollback = {
        "rollback_proof_sha256": "b" * 64,
        "schema": "people-counter-ca-rollback-proof-v1",
        "tables": [],
    }
    with patch(
        "people_counter.fabric_sjd_cutover.snapshot_ca_tables",
        side_effect=(before, before, after, after),
    ), patch(
        "people_counter.fabric_sjd_cutover.validate_stopped_writer_gate"
    ), patch(
        "people_counter.fabric_sjd_cutover.validate_retirement_archives",
        return_value=rollback,
    ):
        result = retire_archived_ca_tables(
            spark,
            gate=_gate(sources=before),
            manifest={
                "bundle_id": "bundle",
                "tables": [asdict(state) for state in before],
            },
            table_names=[state.name for state in before],
            path_exists=lambda _path: False,
        )

    spark.sql.assert_called_once_with("DROP TABLE `people_counter_ca_work`")
    assert result["dropped_tables"] == ["people_counter_ca_work"]
    assert result["rollback"] == rollback


def test_retire_archived_ca_tables_refuses_remaining_managed_path() -> None:
    state = SourceTableState(
        "people_counter_ca_work", True, 3, 0, "a" * 64
    )
    spark = SimpleNamespace(
        sql=MagicMock(),
        catalog=SimpleNamespace(tableExists=lambda _name: False),
    )
    with patch(
        "people_counter.fabric_sjd_cutover.snapshot_ca_tables",
        return_value=(state,),
    ), patch(
        "people_counter.fabric_sjd_cutover.validate_stopped_writer_gate"
    ), patch(
        "people_counter.fabric_sjd_cutover.validate_retirement_archives",
        return_value={"rollback_proof_sha256": "b" * 64},
    ):
        with pytest.raises(CutoverError, match="managed path remains"):
            retire_archived_ca_tables(
                spark,
                gate=_gate(sources=(state,)),
                manifest={
                    "bundle_id": "bundle",
                    "tables": [asdict(state)],
                },
                table_names=[state.name],
                path_exists=lambda _path: True,
            )


def test_retire_archived_ca_tables_resumes_after_prior_drop() -> None:
    source = SourceTableState(
        "people_counter_ca_work", True, 3, 0, "a" * 64
    )
    absent = SourceTableState(
        "people_counter_ca_work", False, None, 0, None
    )
    spark = SimpleNamespace(
        sql=MagicMock(),
        catalog=SimpleNamespace(tableExists=lambda _name: False),
    )
    with patch(
        "people_counter.fabric_sjd_cutover.snapshot_ca_tables",
        side_effect=((absent,), (absent,), (absent,)),
    ), patch(
        "people_counter.fabric_sjd_cutover.validate_retirement_archives",
        return_value={"rollback_proof_sha256": "b" * 64},
    ), patch(
        "people_counter.fabric_sjd_cutover.time.time",
        return_value=101.0,
    ):
        result = retire_archived_ca_tables(
            spark,
            gate=_gate(),
            manifest={
                "bundle_id": "bundle",
                "tables": [asdict(source)],
            },
            table_names=[source.name],
            path_exists=lambda _path: False,
        )

    spark.sql.assert_not_called()
    assert result["dropped_tables"] == [source.name]


def test_retire_archived_legacy_tables_drops_views_before_exact_tables() -> None:
    before = tuple(
        SourceTableState(
            name,
            name == "people_counter_video_work",
            3 if name == "people_counter_video_work" else None,
            2 if name == "people_counter_video_work" else 0,
            "a" * 64 if name == "people_counter_video_work" else None,
        )
        for name in sorted(LEGACY_TABLE_ALLOWLIST)
    )
    after = tuple(
        SourceTableState(state.name, False, None, 0, None)
        for state in before
    )
    before_views = tuple(
        SourceViewState(
            name,
            name == "people_counter_runs_committed",
            (
                f"CREATE VIEW {name} AS SELECT 1"
                if name == "people_counter_runs_committed"
                else None
            ),
            (
                "c" * 64
                if name == "people_counter_runs_committed"
                else None
            ),
        )
        for name in sorted(LEGACY_COMMITTED_VIEWS)
    )
    after_views = tuple(
        SourceViewState(view.name, False, None, None)
        for view in before_views
    )
    table_exists = MagicMock(return_value=False)
    path_exists = MagicMock(return_value=False)
    spark = SimpleNamespace(
        sql=MagicMock(),
        catalog=SimpleNamespace(tableExists=table_exists),
    )
    rollback = {
        "rollback_proof_sha256": "b" * 64,
        "schema": "people-counter-legacy-rollback-proof-v1",
        "tables": [],
        "views": [],
    }
    snapshots = [before, *([before] * len(before)), after]
    with patch(
        "people_counter.fabric_sjd_cutover.snapshot_legacy_tables",
        side_effect=snapshots,
    ) as snapshot_tables, patch(
        "people_counter.fabric_sjd_cutover.snapshot_legacy_views",
        side_effect=(before_views, after_views),
    ) as snapshot_views, patch(
        "people_counter.fabric_sjd_cutover.validate_stopped_writer_gate"
    ) as validate_gate, patch(
        "people_counter.fabric_sjd_cutover.validate_retirement_archives",
        return_value=rollback,
    ) as validate_archives:
        gate = _gate(sources=before)
        manifest = {
            "bundle_id": "v20261010T000000Z-012345abcdef",
            "tables": [asdict(state) for state in before],
            "views": [asdict(view) for view in before_views],
        }
        result = retire_archived_legacy_tables(
            spark,
            gate=gate,
            manifest=manifest,
            table_names=LEGACY_TABLE_ALLOWLIST,
            view_names=LEGACY_COMMITTED_VIEWS,
            path_exists=path_exists,
        )

    assert spark.sql.call_args_list == [
        call("DROP VIEW `people_counter_runs_committed`"),
        call("DROP TABLE `people_counter_video_work`"),
    ]
    assert result["dropped_tables"] == ["people_counter_video_work"]
    assert result["dropped_views"] == ["people_counter_runs_committed"]
    assert result["post_retirement_views"] == [
        asdict(view) for view in after_views
    ]
    assert set(result) == {
        "bundle_id",
        "dropped_tables",
        "dropped_views",
        "post_retirement",
        "post_retirement_views",
        "retired_at",
        "retirement_result_sha256",
        "rollback",
        "schema",
        "zero_routing_proof_sha256",
        "zero_writer_proof_sha256",
    }
    assert result["bundle_id"] == manifest["bundle_id"]
    assert result["rollback"] is rollback
    assert result["schema"] == "people-counter-legacy-retirement-result-v1"
    assert result["zero_writer_proof_sha256"] == gate.evidence_sha256
    assert len(result["zero_routing_proof_sha256"]) == 64
    assert len(result["retirement_result_sha256"]) == 64
    assert snapshot_tables.call_count == len(before) + 2
    assert all(
        item == call(spark, tuple(sorted(LEGACY_TABLE_ALLOWLIST)))
        for item in snapshot_tables.call_args_list
    )
    assert snapshot_views.call_args_list == [
        call(spark, tuple(sorted(LEGACY_COMMITTED_VIEWS))),
        call(spark, tuple(sorted(LEGACY_COMMITTED_VIEWS))),
    ]
    validate_gate.assert_called_once_with(gate, gate.source_tables)
    validate_archives.assert_called_once_with(spark, manifest, before)
    assert table_exists.call_args_list == [
        *(call(name) for name in sorted(LEGACY_COMMITTED_VIEWS)),
        *(call(name) for name in sorted(LEGACY_TABLE_ALLOWLIST)),
    ]
    assert path_exists.call_args_list == [
        call(f"Tables/dbo/{name}") for name in sorted(LEGACY_TABLE_ALLOWLIST)
    ]


@pytest.mark.parametrize(
    ("tables", "views", "message"),
    [
        (
            (*LEGACY_TABLE_ALLOWLIST, LEGACY_TABLE_ALLOWLIST[0]),
            LEGACY_COMMITTED_VIEWS,
            "table allowlist contains duplicates",
        ),
        (
            LEGACY_TABLE_ALLOWLIST,
            (*LEGACY_COMMITTED_VIEWS, LEGACY_COMMITTED_VIEWS[0]),
            "view allowlist contains duplicates",
        ),
        (
            LEGACY_TABLE_ALLOWLIST[:-1],
            LEGACY_COMMITTED_VIEWS,
            "table allowlist differs",
        ),
        (
            LEGACY_TABLE_ALLOWLIST,
            LEGACY_COMMITTED_VIEWS[:-1],
            "view allowlist differs",
        ),
    ],
)
def test_retire_archived_legacy_tables_refuses_unsafe_allowlists(
    tables: tuple[str, ...],
    views: tuple[str, ...],
    message: str,
) -> None:
    with pytest.raises(CutoverError, match=message):
        retire_archived_legacy_tables(
            SimpleNamespace(),
            gate=_gate(),
            manifest={},
            table_names=tables,
            view_names=views,
            path_exists=lambda _path: False,
        )


def test_retire_archived_legacy_tables_refuses_changed_view_definition() -> None:
    sources = tuple(
        SourceTableState(name, False, None, 0, None)
        for name in sorted(LEGACY_TABLE_ALLOWLIST)
    )
    expected_views = tuple(
        SourceViewState(name, False, None, None)
        for name in sorted(LEGACY_COMMITTED_VIEWS)
    )
    changed_views = (
        SourceViewState(
            expected_views[0].name,
            True,
            f"CREATE VIEW {expected_views[0].name} AS SELECT 1",
            "a" * 64,
        ),
        *expected_views[1:],
    )
    with patch(
        "people_counter.fabric_sjd_cutover.snapshot_legacy_tables",
        return_value=sources,
    ), patch(
        "people_counter.fabric_sjd_cutover.snapshot_legacy_views",
        return_value=changed_views,
    ), patch(
        "people_counter.fabric_sjd_cutover.validate_stopped_writer_gate"
    ):
        with pytest.raises(CutoverError, match="view definitions changed"):
            retire_archived_legacy_tables(
                SimpleNamespace(),
                gate=_gate(sources=sources),
                manifest={
                    "tables": [asdict(state) for state in sources],
                    "views": [asdict(view) for view in expected_views],
                },
                table_names=LEGACY_TABLE_ALLOWLIST,
                view_names=LEGACY_COMMITTED_VIEWS,
                path_exists=lambda _path: False,
            )


@pytest.mark.parametrize(
    "work_evidence",
    [
        {},
        {"production-work": "a" * 64},
        {"sjd-live-0944-invalid": "invalid"},
    ],
)
def test_synthetic_cleanup_refuses_unsafe_allowlists(
    work_evidence: dict[str, str],
) -> None:
    with pytest.raises(CutoverError):
        archive_and_delete_synthetic_work(
            SimpleNamespace(),
            cleanup_id="v20261008T000000Z-012345abcdef",
            work_evidence=work_evidence,
        )


def test_synthetic_work_inventory_serializes_spark_rows() -> None:
    rows = [
        _Row(
            work_id="sjd-live-0944-test",
            created_at=datetime(2026, 10, 8, tzinfo=timezone.utc),
        )
    ]
    frame = MagicMock()
    frame.where.return_value.orderBy.return_value.collect.return_value = rows
    spark = SimpleNamespace(table=MagicMock(return_value=frame))

    inventory = synthetic_work_inventory(spark)

    assert inventory["work"] == [
        {
            "created_at": "2026-10-08T00:00:00+00:00",
            "work_id": "sjd-live-0944-test",
        }
    ]
    assert inventory["inventory_sha256"]


def test_synthetic_cleanup_refuses_production_rooted_payload() -> None:
    row = _Row(
        work_id="sjd-live-0944-production",
        payload_json='{"source_video":"Files/incoming/real.mp4"}',
        payload_sha256="a" * 64,
        status="SUCCEEDED",
    )
    table = SimpleNamespace(
        where=lambda _predicate: SimpleNamespace(collect=lambda: [row])
    )
    spark = SimpleNamespace(table=lambda _name: table)

    with pytest.raises(CutoverError, match="payload evidence differs"):
        archive_and_delete_synthetic_work(
            spark,
            cleanup_id="v20261008T000000Z-012345abcdef",
            work_evidence={"sjd-live-0944-production": "a" * 64},
        )


def test_complete_synthetic_cleanup_refuses_malformed_manifest() -> None:
    base = {
        "archived_tables": {},
        "batch_ids": [],
        "cleanup_id": "v20261008T000000Z-012345abcdef",
        "schema": "people-counter-sjd-synthetic-cleanup-archive-v1",
        "work_evidence": {"sjd-live-0944-test": "a" * 64},
    }
    cases = [
        {**base, "batch_ids": "batch"},
        {**base, "archived_tables": []},
        {**base, "work_evidence": {"sjd-live-0944-test": "invalid"}},
    ]
    for manifest in cases:
        manifest["archive_manifest_sha256"] = sha256(manifest)
        with pytest.raises(CutoverError, match="manifest is invalid"):
            complete_synthetic_cleanup(SimpleNamespace(), manifest)


def test_synthetic_cleanup_archives_before_exact_deletes() -> None:
    class Frame:
        def __init__(self, owner, table, rows=None):
            self.owner = owner
            self.table = table
            self.rows = rows
            self.schema = SimpleNamespace(
                json=lambda: f'{{"table":"{self.table}"}}'
            )

        def where(self, _predicate):
            return self

        def collect(self):
            return (
                self.owner.rows[self.table]
                if self.rows is None
                else self.rows
            )

        def count(self):
            return len(self.collect())

        @property
        def write(self):
            frame = self

            class Writer:
                def format(self, _value):
                    return self

                def mode(self, _value):
                    return self

                def save(self, path):
                    frame.owner.archives[path] = Frame(
                        frame.owner,
                        frame.table,
                        list(frame.collect()),
                    )

            return Writer()

    class Spark:
        def __init__(self):
            self.rows = {
                "people_counter_sjd_work": [
                    _Row(
                        work_id="sjd-live-0944-pytorch-001",
                        payload_json=(
                            '{"source_video":"/lakehouse/default/Files/videos/'
                            'incoming/reviewed-demo.mp4"}'
                        ),
                        payload_sha256=(
                            "e51851892b94d9b3f984c95d5bfd4cd580c64e13"
                            "f7e5ec0d2bb924a05718e1d4"
                        ),
                        status="SUCCEEDED",
                    )
                ],
                "people_counter_sjd_batches": [
                    _Row(batch_id="batch-1", status="COMMITTED")
                ],
                "people_counter_sjd_batch_members": [
                    _Row(
                        batch_id="batch-1",
                        work_id="sjd-live-0944-pytorch-001",
                    )
                ],
                "people_counter_sjd_attempts": [
                    _Row(work_id="sjd-live-0944-pytorch-001")
                ],
                "people_counter_sjd_publications": [
                    _Row(work_id="sjd-live-0944-pytorch-001")
                ],
                "people_counter_sjd_replay_requests": [],
            }
            self.archives = {}
            self.statements = []
            self.read = SimpleNamespace(
                format=lambda _value: SimpleNamespace(
                    load=lambda path: self.archives[path]
                )
            )

        def table(self, name):
            return Frame(self, name)

        def sql(self, statement):
            self.statements.append(statement)
            table = statement.split("`")[1]
            self.rows[table] = []

    spark = Spark()
    manifests = []

    with patch(
        "people_counter.fabric_sjd_cutover.frame_content_sha256",
        return_value="c" * 64,
    ):
        result = archive_and_delete_synthetic_work(
            spark,
            cleanup_id="v20261008T000000Z-012345abcdef",
            work_evidence={
                "sjd-live-0944-pytorch-001": (
                    "e51851892b94d9b3f984c95d5bfd4cd580c64e13"
                    "f7e5ec0d2bb924a05718e1d4"
                )
            },
            write_archive_manifest=lambda value: (
                manifests.append(value),
                not spark.statements
                or pytest.fail("delete preceded archive manifest"),
            ),
        )

        assert manifests[0]["archive_manifest_sha256"]
        assert result["cleanup_sha256"]
        assert len(spark.statements) == 6
        assert not spark.rows["people_counter_sjd_work"]

        resumed = complete_synthetic_cleanup(spark, manifests[0])

    assert resumed["cleanup_sha256"] == result["cleanup_sha256"]
    assert len(spark.statements) == 12


def test_cutover_jobs_write_inventory_bootstrap_and_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Fs:
        def __init__(self):
            self.values: dict[str, str] = {}

        def exists(self, path):
            return path in self.values

        def put(self, path, content, overwrite):
            assert overwrite is False
            self.values[path] = content
            return True

        def head(self, path, _size):
            return self.values[path]

    fs = Fs()
    monkeypatch.setitem(sys.modules, "notebookutils", SimpleNamespace(fs=fs))
    value = {"schema": "proof", "value": 1}
    digest = cutover_jobs._write_immutable_json(
        f"{cutover_jobs.EVIDENCE_ROOT}/proof.json", value
    )
    assert digest == sha256(value)
    assert (
        cutover_jobs._write_immutable_json(
            f"{cutover_jobs.EVIDENCE_ROOT}/proof.json", value
        )
        == digest
    )

    state = SourceTableState("people_counter_ca_work", True, 0, 0, "a" * 64)
    spark = SimpleNamespace(
        catalog=SimpleNamespace(tableExists=lambda _name: False)
    )
    with patch.object(cutover_jobs, "snapshot_ca_tables", return_value=(state,)):
        inventory = cutover_jobs.inventory(spark, "inventory-1")
    assert inventory["evidence_sha256"]
    with patch.object(
        cutover_jobs,
        "create_stable_tables",
        return_value={"people_counter_sjd_work": "b" * 64},
    ):
        bootstrap = cutover_jobs.bootstrap(spark, "bootstrap-1")
    assert bootstrap["table_schema_sha256"]

    with patch.object(cutover_jobs, "_spark", return_value=spark), patch.object(
        cutover_jobs, "inventory", return_value={"command": "inventory"}
    ):
        assert cutover_jobs.main(["inventory", "--evidence-id", "run-1"]) == 0
    with patch.object(cutover_jobs, "_spark", return_value=spark), patch.object(
        cutover_jobs, "bootstrap", return_value={"command": "bootstrap"}
    ):
        assert cutover_jobs.main(["bootstrap", "--evidence-id", "run-2"]) == 0


def test_capture_legacy_gate_uses_exact_terminal_writers_and_live_tables() -> None:
    states = tuple(
        SourceTableState(name, False, None, 0, None)
        for name in sorted(LEGACY_TABLE_ALLOWLIST)
    )
    values = [
        f"{item_id}=Completed" for item_id in sorted(WRITER_ITEM_IDS)
    ]
    spark = SimpleNamespace(
        catalog=SimpleNamespace(tableExists=lambda name: name.endswith("routing_allowlist")),
        table=lambda _name: SimpleNamespace(count=lambda: 0),
    )

    with patch.object(
        cutover_jobs,
        "snapshot_legacy_tables",
        return_value=states,
    ) as snapshot:
        result = cutover_jobs.capture_legacy_gate(
            spark,
            gate_id="legacy-20261010",
            writer_states=values,
            captured_at=100.0,
        )

    snapshot.assert_called_once_with(spark, LEGACY_TABLE_ALLOWLIST)
    assert result["schema"] == "people-counter-legacy-stopped-writer-gate-v1"
    assert result["captured_at"] == 100.0
    assert result["routing_to_ca"] == 0
    assert result["source_tables"] == [asdict(item) for item in states]
    assert result["writer_states"] == [
        {"item_id": item_id, "state": "Completed"}
        for item_id in sorted(WRITER_ITEM_IDS)
    ]
    unsigned = {
        key: result[key]
        for key in (
            "captured_at",
            "routing_to_ca",
            "source_tables",
            "writer_states",
        )
    }
    assert result["evidence_sha256"] == sha256(unsigned)


def test_capture_legacy_gate_records_absent_routing_and_current_time() -> None:
    states = tuple(
        SourceTableState(name, False, None, 0, None)
        for name in sorted(LEGACY_TABLE_ALLOWLIST)
    )
    values = [
        f"{item_id}=Completed" for item_id in sorted(WRITER_ITEM_IDS)
    ]
    spark = SimpleNamespace(
        catalog=SimpleNamespace(tableExists=MagicMock(return_value=False)),
        table=MagicMock(),
    )

    with patch.object(
        cutover_jobs,
        "snapshot_legacy_tables",
        return_value=states,
    ), patch.object(cutover_jobs.time, "time", return_value=123.0):
        result = cutover_jobs.capture_legacy_gate(
            spark,
            gate_id="Legacy-20261010",
            writer_states=values,
        )

    spark.catalog.tableExists.assert_called_once_with(
        "people_counter_ca_routing_allowlist"
    )
    spark.table.assert_not_called()
    assert result["captured_at"] == 123.0
    assert result["routing_to_ca"] == 0


def test_capture_legacy_gate_refuses_candidate_a_routing() -> None:
    states = tuple(
        SourceTableState(name, False, None, 0, None)
        for name in sorted(LEGACY_TABLE_ALLOWLIST)
    )
    values = [
        f"{item_id}=Completed" for item_id in sorted(WRITER_ITEM_IDS)
    ]
    routing = SimpleNamespace(count=MagicMock(return_value=1))
    spark = SimpleNamespace(
        catalog=SimpleNamespace(tableExists=MagicMock(return_value=True)),
        table=MagicMock(return_value=routing),
    )

    with patch.object(
        cutover_jobs,
        "snapshot_legacy_tables",
        return_value=states,
    ), pytest.raises(CutoverError, match="routing to Candidate A is not zero"):
        cutover_jobs.capture_legacy_gate(
            spark,
            gate_id="legacy-20261010",
            writer_states=values,
            captured_at=100.0,
        )

    spark.table.assert_called_once_with("people_counter_ca_routing_allowlist")
    routing.count.assert_called_once_with()


@pytest.mark.parametrize(
    ("values", "match"),
    [
        (["bad"], "item_id=state"),
        (["unknown=Completed"], "outside allowlist"),
        (
            [
                *[
                    f"{item_id}=Completed"
                    for item_id in sorted(WRITER_ITEM_IDS)
                ],
                f"{sorted(WRITER_ITEM_IDS)[0]}=Completed",
            ],
            "duplicate writer state",
        ),
        (
            [
                f"{item_id}={'Running' if index == 0 else 'Completed'}"
                for index, item_id in enumerate(sorted(WRITER_ITEM_IDS))
            ],
            "writer is not stopped",
        ),
        (
            [
                f"{item_id}=Completed"
                for item_id in sorted(WRITER_ITEM_IDS)[1:]
            ],
            "exact allowlist",
        ),
    ],
)
def test_writer_states_fail_closed(values: list[str], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        cutover_jobs._writer_states(values)


def test_cutover_jobs_dispatches_legacy_gate_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(
        sys.modules,
        "notebookutils",
        SimpleNamespace(fs=SimpleNamespace()),
    )
    spark = object()
    values = [
        f"{item_id}=Completed" for item_id in sorted(WRITER_ITEM_IDS)
    ]
    result = {"schema": "people-counter-legacy-stopped-writer-gate-v1"}

    with patch.object(cutover_jobs, "_spark", return_value=spark), patch.object(
        cutover_jobs,
        "capture_legacy_gate",
        return_value=result,
    ) as capture, patch.object(
        cutover_jobs,
        "_write_immutable_json",
        return_value="a" * 64,
    ) as write:
        assert (
            cutover_jobs.main(
                [
                    "capture-legacy-gate",
                    "--gate-id",
                    "legacy-20261010",
                    *sum(
                        (["--writer-state", value] for value in values),
                        [],
                    ),
                ]
            )
            == 0
        )

    capture.assert_called_once_with(
        spark,
        gate_id="legacy-20261010",
        writer_states=values,
    )
    write.assert_called_once_with(
        f"{cutover_jobs.EVIDENCE_ROOT}/gates/legacy-20261010.json",
        result,
    )


def test_cutover_jobs_dispatch_exact_legacy_archive_and_retirement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fs = SimpleNamespace(exists=MagicMock(return_value=False))
    monkeypatch.setitem(sys.modules, "notebookutils", SimpleNamespace(fs=fs))
    spark = object()
    states = tuple(
        SourceTableState(name, False, None, 0, None)
        for name in sorted(LEGACY_TABLE_ALLOWLIST)
    )
    gate = _gate(sources=states)
    bundle_id = "v20261010T000000Z-012345abcdef"
    gate_path = f"{cutover_jobs.EVIDENCE_ROOT}/gates/gate.json"
    manifest = {
        "bundle_id": bundle_id,
        "manifest_sha256": "a" * 64,
        "schema": "people-counter-legacy-retirement-v1",
        "tables": [asdict(state) for state in states],
        "views": [
            asdict(SourceViewState(name, False, None, None))
            for name in LEGACY_COMMITTED_VIEWS
        ],
    }
    result = {
        "bundle_id": bundle_id,
        "retired_at": "2026-10-10T00:00:00+00:00",
        "retirement_result_sha256": "b" * 64,
        "rollback": {"rollback_proof_sha256": "c" * 64},
        "zero_routing_proof_sha256": "d" * 64,
        "zero_writer_proof_sha256": gate.evidence_sha256,
    }

    with patch.object(cutover_jobs, "_spark", return_value=spark), patch.object(
        cutover_jobs, "_stopped_gate_path", return_value=gate
    ), patch.object(
        cutover_jobs,
        "snapshot_legacy_tables",
        side_effect=(states, states),
    ) as snapshot, patch.object(
        cutover_jobs, "validate_stopped_writer_gate"
    ) as validate, patch.object(
        cutover_jobs, "archive_legacy_tables", return_value=manifest
    ) as archive, patch.object(
        cutover_jobs, "_write_immutable_json", return_value="a" * 64
    ) as write:
        assert (
            cutover_jobs.main(
                [
                    "archive-legacy",
                    "--bundle-id",
                    bundle_id,
                    "--gate-path",
                    gate_path,
                ]
            )
            == 0
        )

    assert snapshot.call_args_list == [
        call(spark, LEGACY_TABLE_ALLOWLIST),
        call(spark, LEGACY_TABLE_ALLOWLIST),
    ]
    assert validate.call_args_list == [call(gate, states), call(gate, states)]
    archive.assert_called_once_with(
        spark,
        bundle_id=bundle_id,
        table_names=LEGACY_TABLE_ALLOWLIST,
        view_names=LEGACY_COMMITTED_VIEWS,
        path_exists=fs.exists,
    )
    write.assert_called_once_with(
        f"{legacy_retirement_bundle_path(bundle_id)}/manifest.json",
        manifest,
    )

    with patch.object(cutover_jobs, "_spark", return_value=spark), patch.object(
        cutover_jobs, "_stopped_gate_path", return_value=gate
    ), patch.object(
        cutover_jobs, "archive_legacy_tables", return_value=manifest
    ) as archive, patch.object(
        cutover_jobs,
        "retire_archived_legacy_tables",
        return_value=result,
    ) as retire, patch.object(
        cutover_jobs, "_write_immutable_json", return_value="a" * 64
    ) as write, patch.object(
        cutover_jobs, "append_journal_once"
    ) as append:
        assert (
            cutover_jobs.main(
                [
                    "retire-legacy",
                    "--bundle-id",
                    bundle_id,
                    "--gate-path",
                    gate_path,
                ]
            )
            == 0
        )

    archive.assert_called_once_with(
        spark,
        bundle_id=bundle_id,
        table_names=LEGACY_TABLE_ALLOWLIST,
        view_names=LEGACY_COMMITTED_VIEWS,
        path_exists=fs.exists,
    )
    retire.assert_called_once_with(
        spark,
        gate=gate,
        manifest=manifest,
        table_names=LEGACY_TABLE_ALLOWLIST,
        view_names=LEGACY_COMMITTED_VIEWS,
        path_exists=fs.exists,
    )
    assert write.call_args_list == [
        call(
            f"{legacy_retirement_bundle_path(bundle_id)}/manifest.json",
            manifest,
        ),
        call(
            f"{legacy_retirement_bundle_path(bundle_id)}/retirement.json",
            result,
        ),
    ]
    append.assert_called_once()
