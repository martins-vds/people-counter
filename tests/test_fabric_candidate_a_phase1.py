from __future__ import annotations

import base64
import copy
import importlib
import json
import os
import sys
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from people_counter.fabric_candidate_a import (
    ENVIRONMENT_ID,
    FILES_ROOT,
    LAKEHOUSE_ID,
    TABLE_PREFIX,
    WORKSPACE_ID,
    FabricCandidateAConfig,
    build_sjd_v2_definition,
    validate_environment_library_policy,
)
from people_counter.fabric_candidate_a_control import (
    FabricControlStoreImpl,
    OneLakeEnvelopeWriter,
    _SCHEMAS,
)
from people_counter.fabric_candidate_a_gold import (
    FabricCommittedSource,
    FabricGoldJob,
    FabricGoldState,
    _normalize_spark_row_timestamp,
    _sha256,
    _spark_timestamp,
)
from people_counter.fabric_candidate_a_jobs import (
    _clear_stale_lock,
    _control_parser,
    _fabric_process_profile,
    _localize_process_inputs,
    control_main,
)
from people_counter.sjd_gold import (
    FACT_TABLES,
    GoldSourceError,
    SourceCheckpoint,
    _normal_incremental_facts_noop,
    _required_datetime,
)
from people_counter.sjd_process import (
    OneLakeDeltaAttemptAdapter,
    ProcessRouteMode,
    ProcessValidationError,
)


class MemoryFiles:
    def __init__(self) -> None:
        self.content: dict[str, str] = {}
        self.paths: set[str] = set()

    def exists(self, path: str) -> bool:
        return path in self.content or path in self.paths

    def read_text(self, path: str) -> str:
        return self.content[path]

    def create_text(self, path: str, content: str) -> None:
        if path in self.content:
            raise FileExistsError(path)
        self.content[path] = content


class ImmediateWriter:
    @staticmethod
    def run(operation):
        return operation()


class MemoryControlStore(FabricControlStoreImpl):
    def __init__(self, files: MemoryFiles) -> None:
        self.config = FabricCandidateAConfig()
        self.data = {name: [] for name in _SCHEMAS}
        self.writer = ImmediateWriter()
        self.envelopes = OneLakeEnvelopeWriter(
            FabricCandidateAConfig().file_path("control"), files
        )
        self._clock_value = 100.0
        self._clock = lambda: self._clock_value
        identifiers = iter(
            [
                "batch-1",
                "attempt-1",
                "attempt-2",
                "replay-1",
                "batch-2",
                "attempt-3",
            ]
        )
        self._id_factory = lambda: next(identifiers)

    def _rows(self, suffix: str):
        return copy.deepcopy(self.data[suffix])

    def _replace(self, suffix: str, rows):
        self.data[suffix] = copy.deepcopy(rows)


class FakeFrame:
    def __init__(self, spark, rows) -> None:
        self.spark = spark
        self.rows = copy.deepcopy(rows)
        self.write = self

    def format(self, _value):
        return self

    def mode(self, _value):
        return self

    def option(self, _key, _value):
        return self

    def save(self, path):
        self.spark.rows[path] = copy.deepcopy(self.rows)
        relative = path.split(
            f"/{FabricCandidateAConfig().lakehouse_id}/", 1
        )[-1]
        self.spark.files.paths.add(f"{relative}/_delta_log")


class FakeRead:
    def __init__(self, spark) -> None:
        self.spark = spark
        self.path = ""

    def format(self, _value):
        return self

    def load(self, path):
        self.path = path
        return self

    def select(self, *_columns):
        return self

    def collect(self):
        return copy.deepcopy(self.spark.rows[self.path])


class FakeSpark:
    def __init__(self, files: MemoryFiles) -> None:
        self.files = files
        self.rows = {}
        self.read = FakeRead(self)

    def createDataFrame(self, rows, schema):
        assert "record_json string" in schema
        return FakeFrame(self, rows)


def _decode_parts(payload):
    return {
        part["path"]: base64.b64decode(part["payload"])
        for part in payload["definition"]["parts"]
    }


def test_fixed_config_and_sjd_v2_mappings() -> None:
    config = FabricCandidateAConfig()
    assert (
        config.workspace_id,
        config.lakehouse_id,
        config.environment_id,
        config.table("work"),
        config.file_path("claims/a.json"),
        config.runtime,
    ) == (
        WORKSPACE_ID,
        LAKEHOUSE_ID,
        ENVIRONMENT_ID,
        TABLE_PREFIX + "work",
        FILES_ROOT + "claims/a.json",
        "2.0",
    )
    with pytest.raises(ValueError, match="fixed"):
        FabricCandidateAConfig(runtime="1.3")
    assert config.abfss_path(config.file_path("attempts")).endswith(
        f"/{LAKEHOUSE_ID}/{FILES_ROOT}attempts"
    )
    with pytest.raises(ValueError, match="fixed Candidate A root"):
        config.abfss_path("Files/production")

    shadow = FabricCandidateAConfig.production_shadow()
    adapter = OneLakeDeltaAttemptAdapter(
        shadow.file_path("attempts"),
        object(),
        MemoryFiles(),
        config=shadow,
        route_mode=ProcessRouteMode.SHADOW_SYNTHETIC,
    )
    assert adapter.config is shadow
    assert adapter.route_mode is ProcessRouteMode.SHADOW_SYNTHETIC
    default_shadow = OneLakeDeltaAttemptAdapter(
        shadow.file_path("attempts") + "/",
        object(),
        MemoryFiles(),
        config=shadow,
    )
    assert default_shadow.root == shadow.file_path("attempts").rstrip("/")
    assert default_shadow.route_mode is ProcessRouteMode.PRODUCTION_SHADOW
    for root, spark in (
        (None, object()),
        (shadow.file_path("attempts"), None),
    ):
        with pytest.raises(ProcessValidationError, match="unsupported"):
            OneLakeDeltaAttemptAdapter(
                root,
                spark,
                MemoryFiles(),
                config=shadow,
            )
    with pytest.raises(ProcessValidationError, match="route mode"):
        OneLakeDeltaAttemptAdapter(
            shadow.file_path("attempts"),
            object(),
            MemoryFiles(),
            config=shadow,
            route_mode=ProcessRouteMode.BENCHMARK,
        )
    with pytest.raises(ProcessValidationError, match="fixed Candidate A root"):
        OneLakeDeltaAttemptAdapter(
            config.file_path("attempts"),
            object(),
            MemoryFiles(),
            config=shadow,
            route_mode=ProcessRouteMode.SHADOW_SYNTHETIC,
        )
    with pytest.raises(ProcessValidationError, match="production"):
        production = FabricCandidateAConfig.production()
        OneLakeDeltaAttemptAdapter(
            production.file_path("attempts"),
            object(),
            MemoryFiles(),
            config=production,
        )

    for job in ("control", "process", "gold"):
        definition = build_sjd_v2_definition(job)
        assert definition["definition"]["format"] == "SparkJobDefinitionV2"
        parts = _decode_parts(definition)
        metadata = json.loads(parts["SparkJobDefinitionV1.json"])
        assert metadata["defaultLakehouseArtifactId"] == LAKEHOUSE_ID
        assert metadata["environmentArtifactId"] == ENVIRONMENT_ID
        assert metadata["additionalLibraryUris"] == []
        source = parts["Main/main.py"].decode()
        assert "people_counter.fabric_candidate_a_jobs import" in source
        assert "sys.path" not in source and "source" not in source.lower()


def test_fabric_committed_source_converts_staged_records_to_gold_document() -> None:
    records = [
        {
            "work_id": "work-1",
            "attempt_id": "attempt-1",
            "record_type": "video_result",
            "record_sequence": 0,
            "status": "SUCCEEDED",
            "payload_json": json.dumps(
                {"captured_at_utc": "2026-10-04T03:15:00Z"}
            ),
            "processed_frames": 4,
            "processing_seconds": 1.5,
        },
        {
            "work_id": "work-1",
            "attempt_id": "attempt-1",
            "record_type": "line_count",
            "record_sequence": 1,
            "status": "SUCCEEDED",
            "payload_json": json.dumps({"frame": 30}),
        },
    ]
    attempts = type(
        "Attempts",
        (),
        {"read_records": lambda self, batch, process: records},
    )()
    source = FabricCommittedSource(None, attempts)
    source._visible_rows = lambda: [
        {
            "work": {
                "work_id": "work-1",
                "payload_json": "{}",
            },
            "attempt": {"attempt_id": "attempt-1"},
            "publication": {
                "publication_sequence": 1,
                "output_path": (
                    "Files/_canary/people-counter/candidate-a/v1/attempts/"
                    "process/batch=batch-1/attempt=process-1"
                ),
                "output_sha256": _sha256(records),
                "published_at": 1.0,
            },
            "batch": {
                "batch_id": "batch-1",
                "status": "COMMITTED",
                "committed_at": 1.0,
            },
        }
    ]
    outputs = source.committed_outputs()
    assert outputs[0].run["processed_frames"] == 4
    assert outputs[0].line_counts[0]["frame"] == 30


def test_fabric_gold_optional_publication_time_uses_only_committed_batch_time() -> None:
    records = [
        {
            "work_id": "work-1",
            "attempt_id": "attempt-1",
            "record_type": "video_result",
            "record_sequence": 0,
            "status": "SUCCEEDED",
            "payload_json": json.dumps(
                {"captured_at_utc": "2026-10-05T00:00:00Z"}
            ),
        }
    ]
    attempts = type(
        "Attempts",
        (),
        {"read_records": lambda self, batch, process: records},
    )()
    source = FabricCommittedSource(None, attempts)
    pointer = {
        "work": {"work_id": "work-1", "payload_json": "{}"},
        "attempt": {"attempt_id": "attempt-1", "batch_id": "batch-1"},
        "publication": {
            "publication_sequence": 1,
            "output_path": (
                "Files/_benchmark/people-counter/candidate-a/v1/attempts/"
                "process/batch=batch-1/attempt=process-1"
            ),
            "output_sha256": _sha256(records),
            "published_at": None,
        },
        "batch": {
            "batch_id": "batch-1",
            "status": "COMMITTED",
            "committed_at": 1.0,
        },
    }
    source._visible_rows = lambda: [pointer]

    output = source.committed_outputs()[0]

    assert output.published_at == datetime.fromtimestamp(1, timezone.utc)
    assert output.attempt["batch_committed_at"] == 1.0
    pointer["batch"]["committed_at"] = None
    assert source.committed_outputs()[0].published_at is None


def test_fabric_gold_timestamp_matches_spark_naive_utc_readback() -> None:
    eastern = timezone(-timedelta(hours=4))
    assert _spark_timestamp(
        datetime(2026, 10, 4, 1, 2, 3, tzinfo=eastern)
    ) == datetime(2026, 10, 4, 5, 2, 3)
    naive = datetime(2026, 10, 4, 5, 2, 3)
    assert _spark_timestamp(naive) is naive
    with pytest.raises(TypeError, match="datetime"):
        _spark_timestamp("2026-10-04")  # type: ignore[arg-type]


def test_fabric_gold_normalizes_only_spark_datetime_values() -> None:
    naive = datetime(2026, 10, 4, 4, 28, 3, 545631)
    assert _normalize_spark_row_timestamp(naive) == naive.replace(
        tzinfo=timezone.utc
    )

    eastern = timezone(-timedelta(hours=4))
    aware = datetime(2026, 10, 4, 1, 2, 3, tzinfo=eastern)
    assert _normalize_spark_row_timestamp(aware) == datetime(
        2026, 10, 4, 5, 2, 3, tzinfo=timezone.utc
    )

    utc_text = "2026-10-04T05:02:03Z"
    assert _normalize_spark_row_timestamp(utc_text) == utc_text
    assert _required_datetime(utc_text) == datetime(
        2026, 10, 4, 5, 2, 3, tzinfo=timezone.utc
    )

    malformed = "2026-10-04T05:02:03"
    assert _normalize_spark_row_timestamp(malformed) == malformed
    with pytest.raises(GoldSourceError, match="invalid UTC timestamp"):
        _required_datetime(malformed)


def test_fabric_gold_normalizer_handles_tzinfo_without_an_offset() -> None:
    class MissingOffset(tzinfo):
        def utcoffset(self, _value):
            return None

    value = datetime(2026, 10, 4, 5, 2, 3, tzinfo=MissingOffset())

    assert _normalize_spark_row_timestamp(value) == value.replace(
        tzinfo=timezone.utc
    )


def test_fabric_gold_normalizer_does_not_use_local_timezone() -> None:
    previous = os.environ.get("TZ")
    try:
        os.environ["TZ"] = "America/Los_Angeles"
        time.tzset()
        value = datetime(
            2026,
            10,
            4,
            1,
            2,
            3,
            tzinfo=timezone(timedelta(hours=-4)),
        )

        normalized = _normalize_spark_row_timestamp(value)
        assert normalized == datetime(
            2026, 10, 4, 5, 2, 3, tzinfo=timezone.utc
        )
        assert normalized.tzinfo is timezone.utc
    finally:
        if previous is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous
        time.tzset()


@pytest.mark.parametrize(
    "changes",
    [
        {"automatic": False},
        {"force": True},
        {"full_rebuild": True},
        {"checkpoint": None},
        {"checkpoint": {"source_key": "different"}},
        {"targets_current": False},
    ],
)
def test_normal_incremental_facts_noop_requires_every_gate(
    changes: dict[str, object],
) -> None:
    arguments = {
        "automatic": True,
        "force": False,
        "full_rebuild": False,
        "checkpoint": {"source_key": "same"},
        "source_key": "same",
        "targets_current": True,
    }
    assert _normal_incremental_facts_noop(**arguments) is True

    arguments.update(changes)

    assert _normal_incremental_facts_noop(**arguments) is False


class _CollectedRow:
    def __init__(self, value: dict[str, object]) -> None:
        self.value = value

    def asDict(self, *, recursive: bool) -> dict[str, object]:
        assert recursive is True
        return copy.deepcopy(self.value)


class _CheckpointSpark:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.catalog = SimpleNamespace(refreshTable=lambda _table: None)

    def table(self, _table: str) -> SimpleNamespace:
        return SimpleNamespace(
            collect=lambda: [_CollectedRow(row) for row in self.rows]
        )


def test_fabric_checkpoint_rows_normalize_naive_spark_timestamps() -> None:
    completed_at = datetime(2026, 10, 4, 4, 28, 3, 545631)
    state = FabricGoldState(
        _CheckpointSpark(
            [
                {
                    "stage": "facts",
                    "source_key": "source",
                    "publication_sequence": 1,
                    "source_versions_json": "{}",
                    "target_versions_json": "{}",
                    "completed_at": completed_at,
                }
            ]
        ),
        auto_bootstrap=False,
    )

    checkpoint = state.checkpoint("facts")

    assert checkpoint is not None
    assert checkpoint["completed_at"] == completed_at.replace(
        tzinfo=timezone.utc
    )


def test_normal_fabric_gold_rerun_accepts_checkpoint_and_is_a_noop() -> None:
    flow = SourceCheckpoint(1, {"publication": "same"})
    operations = SourceCheckpoint(1, {"control": "same"})
    combined = FabricGoldJob._fact_source(flow, operations)
    target_versions = {name: 7 for name in FACT_TABLES}
    checkpoint = {
        "stage": "facts",
        "source_key": combined.key,
        "publication_sequence": 1,
        "source_versions_json": json.dumps(combined.versions),
        "target_versions_json": json.dumps(target_versions, sort_keys=True),
        "completed_at": datetime(2026, 10, 4, 4, 28, 3, 545631),
    }
    state = FabricGoldState(
        _CheckpointSpark([checkpoint]),
        auto_bootstrap=False,
    )

    class UnchangedSource:
        def checkpoint(self) -> SourceCheckpoint:
            return flow

        def operations_checkpoint(self) -> SourceCheckpoint:
            return operations

        def committed_outputs(self):
            raise AssertionError("unchanged committed work must not be scheduled")

        def control_rows(self):
            raise AssertionError("unchanged control work must not be scheduled")

    class UnchangedStore:
        def versions(self, names):
            assert tuple(names) == FACT_TABLES
            return dict(target_versions)

    job = FabricGoldJob(
        UnchangedSource(),
        UnchangedStore(),
        state,
    )

    result = job.build_facts()

    assert result == {
        "stage": "facts",
        "skipped": True,
        "source_key": combined.key,
        "partition_count": 0,
    }
    assert state.checkpoint("facts")["completed_at"] == checkpoint[
        "completed_at"
    ].replace(tzinfo=timezone.utc)


def test_stale_lock_clear_requires_an_exact_expected_owner() -> None:
    parser = _control_parser()
    parsed = parser.parse_args(
        ["clear-stale-lock", "--expected-owner-id", "owner-1"]
    )
    assert parsed.expected_owner_id == "owner-1"
    with pytest.raises(SystemExit):
        parser.parse_args(["clear-stale-lock"])
    claim = parser.parse_args(
        [
            "claim",
            "--owner",
            "owner-1",
            "--work-id",
            "work-1",
            "--max-items",
            "1",
            "--lease-seconds",
            "60",
        ]
    )
    assert claim.minimum_items == 1
    assert claim.minimum_speed_x == 1.0
    assert claim.safety_factor == 1.25
    assert claim.margin_seconds == 30.0
    assert claim.work_id == ["work-1"]
    replay = parser.parse_args(
        [
            "replay",
            "--work-id",
            "work-1",
            "--operator",
            "operator",
            "--reason",
            "reason",
        ]
    )
    assert replay.additional_attempts == 1


def test_control_parser_rejects_missing_required_and_non_numeric_values() -> None:
    parser = _control_parser()
    invalid = (
        [],
        ["register"],
        [
            "register",
            "--work-id",
            "work-1",
            "--payload-json",
            "{}",
            "--runtime-key",
            "cpu",
            "--duration-seconds",
            "not-float",
            "--config-sha256",
            "c" * 64,
            "--release-digest",
            "release",
        ],
        [
            "register",
            "--work-id",
            "work-1",
            "--payload-json",
            "{}",
            "--runtime-key",
            "cpu",
            "--duration-seconds",
            "1",
            "--config-sha256",
            "c" * 64,
            "--release-digest",
            "release",
            "--max-attempts",
            "not-int",
        ],
        ["claim"],
        [
            "claim",
            "--owner",
            "owner",
            "--work-id",
            "work-1",
            "--max-items",
            "not-int",
            "--lease-seconds",
            "60",
        ],
        [
            "claim",
            "--owner",
            "owner",
            "--work-id",
            "work-1",
            "--max-items",
            "1",
            "--lease-seconds",
            "bad",
        ],
        [
            "claim",
            "--owner",
            "owner",
            "--max-items",
            "1",
            "--lease-seconds",
            "60",
            "--minimum-speed-x",
            "bad",
        ],
        [
            "claim",
            "--owner",
            "owner",
            "--max-items",
            "1",
            "--lease-seconds",
            "60",
            "--safety-factor",
            "bad",
        ],
        [
            "claim",
            "--owner",
            "owner",
            "--max-items",
            "1",
            "--lease-seconds",
            "60",
            "--margin-seconds",
            "bad",
        ],
        [
            "claim",
            "--owner",
            "owner",
            "--max-items",
            "1",
            "--lease-seconds",
            "60",
            "--minimum-items",
            "bad",
        ],
        ["replay"],
        [
            "replay",
            "--work-id",
            "work",
            "--operator",
            "operator",
            "--reason",
            "reason",
            "--additional-attempts",
            "bad",
        ],
        ["recover", "--now", "bad"],
    )
    for arguments in invalid:
        with pytest.raises(SystemExit):
            parser.parse_args(arguments)

    register_arguments = [
        "register",
        "--work-id",
        "work-1",
        "--payload-json",
        "{}",
        "--runtime-key",
        "cpu",
        "--duration-seconds",
        "1",
        "--config-sha256",
        "c" * 64,
        "--release-digest",
        "release",
    ]
    for option in (
        "--work-id",
        "--payload-json",
        "--runtime-key",
        "--duration-seconds",
        "--config-sha256",
        "--release-digest",
    ):
        omitted = list(register_arguments)
        index = omitted.index(option)
        del omitted[index : index + 2]
        with pytest.raises(SystemExit):
            parser.parse_args(omitted)
    for arguments, required_options in (
        (
            [
                "claim",
                "--owner",
                "owner",
                "--max-items",
                "1",
                "--lease-seconds",
                "60",
            ],
            ("--owner", "--max-items", "--lease-seconds"),
        ),
        (
            [
                "replay",
                "--work-id",
                "work",
                "--operator",
                "operator",
                "--reason",
                "reason",
            ],
            ("--work-id", "--operator", "--reason"),
        ),
    ):
        for option in required_options:
            omitted = list(arguments)
            index = omitted.index(option)
            del omitted[index : index + 2]
            with pytest.raises(SystemExit):
                parser.parse_args(omitted)

    register = parser.parse_args(register_arguments)
    assert register.max_attempts == 3


def test_environment_policy_is_published_full_mode_only() -> None:
    validate_environment_library_policy(
        {
            "environmentArtifactId": ENVIRONMENT_ID,
            "runtimeVersion": "2.0",
            "libraryMode": "Full",
            "published": True,
            "additionalLibraryUris": [],
        }
    )
    with pytest.raises(ValueError, match="inline"):
        validate_environment_library_policy(
            {
                "environment_id": ENVIRONMENT_ID,
                "runtime": "2.0",
                "library_mode": "full",
                "publishState": "Succeeded",
                "inline_libraries": ["Files/wheel.whl"],
            }
        )
    valid = {
        "environmentArtifactId": ENVIRONMENT_ID,
        "runtimeVersion": "2.0",
        "libraryMode": "Full",
        "published": True,
        "additionalLibraryUris": [],
    }
    for field, value, message in (
        ("environmentArtifactId", "other", "fixed Fabric Environment"),
        ("runtimeVersion", "1.3", "Runtime 2.0"),
        ("published", False, "published Environment"),
        ("additionalLibraryUris", ["Files/wheel.whl"], "inline"),
    ):
        with pytest.raises(ValueError, match=message):
            validate_environment_library_policy(valid | {field: value})
    validate_environment_library_policy(
        {
            "environment_id": ENVIRONMENT_ID,
            "runtime": "2.0",
            "library_mode": "full",
            "publishState": "Succeeded",
            "inline_libraries": (),
        }
    )
    with pytest.raises(ValueError, match="Full"):
        validate_environment_library_policy(
            {
                "environmentArtifactId": ENVIRONMENT_ID,
                "runtimeVersion": "2.0",
                "libraryMode": "Quick",
                "published": True,
                "additionalLibraryUris": [],
            }
        )


def test_control_transitions_are_homogeneous_fenced_and_monotonic() -> None:
    store = MemoryControlStore(MemoryFiles())
    for work_id, runtime in (("a", "cpu"), ("b", "cpu"), ("c", "gpu")):
        store.register(
            work_id,
            {"batch_size": 1, "asset": work_id},
            runtime_key=runtime,
            duration_seconds=1,
            config_sha256="c" * 64,
            release_digest="release",
        )
    claim = store.claim(
        "owner",
        max_items=2,
        lease_seconds=60,
        minimum_speed_x=1,
        safety_factor=1,
        margin_seconds=0.1,
    )
    assert claim is not None
    assert [item.work_id for item in claim.items] == ["a", "b"]
    assert {item.fence for item in claim.items} == {1}
    envelope, digest = store.load_claim_envelope_with_digest(claim.batch_id)

    from people_counter.sjd_process import verify_envelope

    verified = verify_envelope(
        envelope, batch_id=claim.batch_id, envelope_sha256=digest
    )
    before = store.process_batch_state(verified).lease_expires_at
    store.heartbeat_process(verified, 120)
    assert store.process_batch_state(verified).lease_expires_at > before
    store.seal_batch(
        claim.batch_id,
        [
            {
                "work_id": item.work_id,
                "attempt_id": item.attempt_id,
                "succeeded": True,
                "output_path": (
                    f"Files/root/process/batch={claim.batch_id}/"
                    "attempt=process-1"
                ),
                "output_sha256": ("a" if item.work_id == "a" else "b") * 64,
                "records": [
                    {
                        "executor_identity": "executor-1",
                        "partition_id": 0,
                        "task_attempt_id": 0,
                        "record_sequence": 0,
                    }
                ],
            }
            for item in claim.items
        ],
        envelope_sha256=digest,
        membership_sha256=verified.membership_sha256,
    )
    assert store.commit_batch(claim.batch_id) == (1, 2)
    assert store.commit_batch(claim.batch_id) == (1, 2)
    assert [
        row["publication_sequence"] for row in store.data["publications"]
    ] == [1, 2]
    replay = store.replay("a", operator="tester", reason="verification")
    assert replay.generation == 1
    assert next(row for row in store.data["work"] if row["work_id"] == "a")[
        "status"
    ] == "READY"


def test_onelake_envelope_and_delta_attempts_are_immutable() -> None:
    files = MemoryFiles()
    envelopes = OneLakeEnvelopeWriter(
        FabricCandidateAConfig().file_path("control"), files
    )
    path, digest = envelopes.write("batch-1", {"batch_id": "batch-1"})
    assert envelopes.read(path, digest) == {"batch_id": "batch-1"}
    assert envelopes.write("batch-1", {"batch_id": "batch-1"}) == (
        path,
        digest,
    )

    spark = FakeSpark(files)
    adapter = OneLakeDeltaAttemptAdapter(
        FabricCandidateAConfig().file_path("attempts"), spark, files
    )
    records = [
        {
            "work_id": "work-1",
            "attempt_id": "attempt-1",
            "record_type": "video_result",
            "record_sequence": 0,
            "payload": {},
        }
    ]
    attempt_path = adapter.write_records("batch-1", "process-1", records)
    adapter.create_success("batch-1", "process-1", {"record_count": 1})
    loaded, marker = adapter.load_complete("batch-1", "process-1")
    assert loaded == records
    assert marker == {"record_count": 1}
    with pytest.raises(Exception, match="already exists"):
        adapter.write_records("batch-1", "process-1", records)
    assert attempt_path.endswith("batch=batch-1/attempt=process-1")


def test_fabric_modules_are_lazy_and_checked_in_mains_are_thin() -> None:
    before = set(sys.modules)
    for module in (
        "people_counter.fabric_candidate_a_control",
        "people_counter.fabric_candidate_a_gold",
        "people_counter.fabric_candidate_a_jobs",
    ):
        importlib.reload(importlib.import_module(module))
    imported = set(sys.modules) - before
    assert "pyspark" not in imported
    assert "delta" not in imported

    root = Path(__file__).parents[1] / "fabric" / "candidate_a"
    for job in ("control", "process", "gold"):
        source = (root / job / "Main" / "main.py").read_text()
        assert "people_counter.fabric_candidate_a_jobs import" in source
        assert "sys.path" not in source
        assert "except ImportError" not in source


def _pointer_rows() -> dict[str, list[dict[str, object]]]:
    return {
        "work": [
            {
                "work_id": "work-1",
                "status": "SUCCEEDED",
                "committed_attempt_id": "attempt-1",
                "publication_sequence": 2,
                "payload_json": "{}",
                "updated_at": 10.0,
            },
            {
                "work_id": "work-0",
                "status": "SUCCEEDED",
                "committed_attempt_id": "attempt-0",
                "publication_sequence": 1,
                "payload_json": "{}",
                "updated_at": 9.0,
            },
            {
                "work_id": "ready",
                "status": "READY",
                "committed_attempt_id": None,
                "publication_sequence": None,
                "payload_json": "{}",
                "updated_at": 8.0,
            },
        ],
        "attempts": [
            {
                "attempt_id": "attempt-1",
                "work_id": "work-1",
                "batch_id": "batch-1",
                "status": "SUCCEEDED",
                "output_path": "path-1",
                "output_sha256": "1" * 64,
            },
            {
                "attempt_id": "attempt-0",
                "work_id": "work-0",
                "batch_id": "batch-0",
                "status": "SUCCEEDED",
                "output_path": "path-0",
                "output_sha256": "0" * 64,
            },
        ],
        "publications": [
            {
                "work_id": "work-1",
                "attempt_id": "attempt-1",
                "batch_id": "batch-1",
                "publication_sequence": 2,
                "output_path": "path-1",
                "output_sha256": "1" * 64,
                "published_at": 10.0,
            },
            {
                "work_id": "work-0",
                "attempt_id": "attempt-0",
                "batch_id": "batch-0",
                "publication_sequence": 1,
                "output_path": "path-0",
                "output_sha256": "0" * 64,
                "published_at": 9.0,
            },
        ],
        "batches": [
            {
                "batch_id": "batch-1",
                "status": "COMMITTED",
                "sealed_at": 9.0,
                "committed_at": 10.0,
                "lease_expires_at": 20.0,
            },
            {
                "batch_id": "batch-0",
                "status": "COMMITTED",
                "sealed_at": 8.0,
                "committed_at": 9.0,
                "lease_expires_at": 19.0,
            },
        ],
    }


def _source_with_rows(
    rows: dict[str, list[dict[str, object]]],
) -> FabricCommittedSource:
    source = FabricCommittedSource(None, None)
    source._rows = lambda suffix: copy.deepcopy(rows[suffix])
    return source


def test_fabric_committed_source_filters_and_orders_only_valid_pointers() -> None:
    rows = _pointer_rows()
    rows["work"].insert(0, rows["work"].pop())
    source = _source_with_rows(rows)

    visible = source._visible_rows()

    assert [row["work"]["work_id"] for row in visible] == [
        "work-0",
        "work-1",
    ]
    assert source.checkpoint().publication_sequence == 2
    assert source.operations_checkpoint().publication_sequence == 2
    control = source.control_rows()
    assert len(control["work"]) == 3
    assert control["attempts"][0]["published_at"] == 10.0
    assert control["attempts"][1]["batch_completed_at"] == 9.0


@pytest.mark.parametrize(
    ("section", "index", "field", "value"),
    [
        ("attempts", 0, "attempt_id", "missing-attempt"),
        ("publications", 0, "attempt_id", "missing-publication"),
        ("attempts", 0, "work_id", "other-work"),
        ("attempts", 0, "status", "FAILED"),
        ("work", 0, "status", "LEASED"),
        ("publications", 0, "publication_sequence", 99),
        ("publications", 0, "output_path", "other-path"),
        ("publications", 0, "output_sha256", "f" * 64),
    ],
)
def test_fabric_committed_source_rejects_every_broken_pointer_identity(
    section: str, index: int, field: str, value: object
) -> None:
    rows = _pointer_rows()
    rows[section][index][field] = value

    with pytest.raises(GoldSourceError, match="invalid committed pointer"):
        _source_with_rows(rows)._visible_rows()


def test_fabric_process_profile_enforces_runtime_and_computes_reserve(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Conf:
        values = {
            "spark.dynamicAllocation.enabled": "true",
            "spark.speculation": "false",
            "spark.executor.cores": "8",
            "spark.executor.memory": "16g",
        }

        def get(self, name: str, default: object = None) -> object:
            return self.values.get(name, default)

    spark = SimpleNamespace(
        version="4.1.1.5",
        conf=Conf(),
        sparkContext=SimpleNamespace(
            _jvm=SimpleNamespace(
                java=SimpleNamespace(
                    lang=SimpleNamespace(
                        System=SimpleNamespace(
                            getProperty=lambda _name: "21.0.4"
                        )
                    )
                )
            )
        ),
    )
    monkeypatch.setattr(sys, "version_info", (3, 13))

    profile = _fabric_process_profile(spark)

    assert profile.executor_memory_bytes == 16 * 1024**3
    assert profile.memory_reserve_bytes == 4 * 1024**3
    assert profile.fixed_allocation is False
    for key, bad_value, message in (
        ("spark.dynamicAllocation.enabled", "false", "dynamic"),
        ("spark.speculation", "true", "speculation"),
        ("spark.executor.cores", "0", "executor core"),
        ("spark.executor.memory", "16x", "memory"),
    ):
        original = Conf.values[key]
        Conf.values[key] = bad_value
        try:
            with pytest.raises(RuntimeError, match=message):
                _fabric_process_profile(spark)
        finally:
            Conf.values[key] = original


def test_process_input_localizer_distributes_and_hashes_immutable_inputs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    files = {
        "synthetic.mp4": b"video",
        "config.json": b"config",
        "preprocessor_config.json": b"preprocessor",
        "model.safetensors": b"detector",
        "osnet_ain_x0_25.pt": b"reid",
    }
    for name, content in files.items():
        (tmp_path / name).write_bytes(content)
    spark_files = SimpleNamespace(get=lambda name: str(tmp_path / name))
    monkeypatch.setitem(
        sys.modules, "pyspark", SimpleNamespace(SparkFiles=spark_files)
    )
    payload = {
        "source_video": (
            "/lakehouse/default/Files/_canary/people-counter/"
            "candidate-a/v1/assets/synthetic.mp4"
        ),
        "source_sha256": _sha256_bytes(b"video"),
        "pipeline": "rtdetr-osnet",
        "detector_model": "r18",
        "model_format": "pytorch",
    }
    requested_batches: list[str] = []

    def load_envelope(batch: str) -> tuple[dict[str, object], str]:
        requested_batches.append(batch)
        return {"items": [{"work_id": "work-1", "payload": payload}]}, "digest"

    store = SimpleNamespace(load_claim_envelope_with_digest=load_envelope)
    spark = SimpleNamespace(
        sparkContext=SimpleNamespace(addFile=MagicMock())
    )

    enrichment = _localize_process_inputs(spark, store, "batch-1")

    assert enrichment["work-1"]["spark_localized_video_name"] == "synthetic.mp4"
    models = enrichment["work-1"]["spark_localized_models"]
    assert len(models) == 4
    assert models["rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors"][
        "sha256"
    ] == _sha256_bytes(b"detector")
    assert requested_batches == ["batch-1"]
    assert spark.sparkContext.addFile.call_count == 5
    assert spark.sparkContext.addFile.call_args_list[0].args == (
        f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
        f"{LAKEHOUSE_ID}/Files/_canary/people-counter/"
        "candidate-a/v1/assets/synthetic.mp4",
    )
    assert all(
        value["localized_name"] == path.rsplit("/", 1)[-1]
        for path, value in models.items()
    )
    assert spark.sparkContext.addFile.call_args_list[-1].args == (
        f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
        f"{LAKEHOUSE_ID}/Files/models/rtdetr_osnet/"
        "libre_reid_osnet/osnet_ain_x0_25.pt",
    )

    from people_counter.fabric_production_routing import sha256_json

    model_sha256 = sha256_json(
        {
            "schema": "people-counter-fixed-model-artifacts-v1",
            "artifacts": {
                "Files/models/rtdetr_osnet/rtdetr_v2_r18vd/model.safetensors":
                    _sha256_bytes(b"detector"),
                "Files/models/rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.pt":
                    _sha256_bytes(b"reid"),
            },
        }
    )
    synthetic_item = {
        "work_id": "work-1",
        "config_sha256": "c" * 64,
        "payload": payload,
    }
    store.load_claim_envelope_with_digest = lambda _batch: (
        {"items": [synthetic_item]},
        "digest",
    )
    synthetic_identity = {
        "work_id": "work-1",
        "source_sha256": payload["source_sha256"],
        "config_sha256": "c" * 64,
        "model_sha256": model_sha256,
    }
    assert _localize_process_inputs(
        spark,
        store,
        "batch-1",
        route_mode="SHADOW_SYNTHETIC",
        route_identity=synthetic_identity,
    )["work-1"]["spark_localized_models"] == models
    with pytest.raises(ProcessValidationError, match="identity is required"):
        _localize_process_inputs(
            spark, store, "batch-1", route_mode="SHADOW_SYNTHETIC"
        )
    for changed, message in (
        ({**synthetic_identity, "source_sha256": "a" * 64}, "video/config"),
        ({**synthetic_identity, "config_sha256": "a" * 64}, "video/config"),
        ({**synthetic_identity, "model_sha256": "a" * 64}, "model identity"),
    ):
        with pytest.raises(ProcessValidationError, match=message):
            _localize_process_inputs(
                spark,
                store,
                "batch-1",
                route_mode="SHADOW_SYNTHETIC",
                route_identity=changed,
            )

    for update, message in (
        ({"source_video": "/production/video.mp4"}, "default Lakehouse"),
        ({"source_sha256": "0" * 64}, "video digest"),
        ({"pipeline": "rfdetr-botsort"}, "RT-DETR"),
    ):
        changed = payload | update
        store.load_claim_envelope_with_digest = lambda _batch, item=changed: (
            {"items": [{"work_id": "work-1", "payload": item}]},
            "digest",
        )
        with pytest.raises(ValueError, match=message):
            _localize_process_inputs(spark, store, "batch-1")


def test_synthetic_localizer_pins_r50_onnx_artifacts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    files = {
        "synthetic.mp4": b"video-r50",
        "config.json": b"config-r50",
        "preprocessor_config.json": b"preprocessor-r50",
        "model.onnx": b"detector-r50",
        "osnet_ain_x0_25.onnx": b"reid-onnx",
    }
    for name, content in files.items():
        (tmp_path / name).write_bytes(content)
    monkeypatch.setitem(
        sys.modules,
        "pyspark",
        SimpleNamespace(
            SparkFiles=SimpleNamespace(
                get=lambda name: str(tmp_path / name)
            )
        ),
    )
    payload = {
        "source_video": (
            "/lakehouse/default/Files/_shadow/people-counter/"
            "candidate-a/v1/assets/synthetic.mp4"
        ),
        "source_sha256": _sha256_bytes(files["synthetic.mp4"]),
        "pipeline": "rtdetr-osnet",
        "detector_model": "r50",
        "model_format": "onnx",
    }
    item = {
        "work_id": "work-r50",
        "config_sha256": "c" * 64,
        "payload": payload,
    }
    store = SimpleNamespace(
        load_claim_envelope_with_digest=lambda batch: (
            {"items": [item]} if batch == "batch-r50" else {},
            "digest",
        )
    )
    spark = SimpleNamespace(
        sparkContext=SimpleNamespace(addFile=MagicMock())
    )
    from people_counter.fabric_production_routing import sha256_json

    identity = {
        "work_id": "work-r50",
        "source_sha256": payload["source_sha256"],
        "config_sha256": item["config_sha256"],
        "model_sha256": sha256_json(
            {
                "schema": "people-counter-fixed-model-artifacts-v1",
                "artifacts": {
                    "Files/models/rtdetr_osnet/rtdetr_v2_r50vd/model.onnx":
                        _sha256_bytes(files["model.onnx"]),
                    "Files/models/rtdetr_osnet/libre_reid_osnet/"
                    "osnet_ain_x0_25.onnx":
                        _sha256_bytes(files["osnet_ain_x0_25.onnx"]),
                },
            }
        ),
    }

    localized = _localize_process_inputs(
        spark,
        store,
        "batch-r50",
        route_mode="SHADOW_SYNTHETIC",
        route_identity=identity,
    )["work-r50"]

    models = localized["spark_localized_models"]
    assert set(models) == {
        "rtdetr_osnet/rtdetr_v2_r50vd/config.json",
        "rtdetr_osnet/rtdetr_v2_r50vd/preprocessor_config.json",
        "rtdetr_osnet/rtdetr_v2_r50vd/model.onnx",
        "rtdetr_osnet/libre_reid_osnet/osnet_ain_x0_25.onnx",
    }
    assert localized["spark_localized_video_name"] == "synthetic.mp4"
    assert spark.sparkContext.addFile.call_count == 5
    assert spark.sparkContext.addFile.call_args_list[-1].args[0].endswith(
        "/Files/models/rtdetr_osnet/libre_reid_osnet/"
        "osnet_ain_x0_25.onnx"
    )


def _sha256_bytes(value: bytes) -> str:
    import hashlib

    return hashlib.sha256(value).hexdigest()


def test_control_main_dispatches_all_safe_commands(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    @dataclass
    class Result:
        value: object

    store = MagicMock()
    store.register.return_value = Result("work-1")
    store.claim.return_value = None
    store.replay.return_value = Result("replay-1")
    store.recover.return_value = Result(1)
    store.reconcile.return_value = []
    monkeypatch.setattr(
        "people_counter.sjd_control.FabricControlStore",
        lambda *_args, **_kwargs: store,
    )
    monkeypatch.setattr(
        "people_counter.fabric_candidate_a_jobs._spark", lambda: object()
    )
    commands = (
        ["bootstrap"],
        [
            "register",
            "--work-id",
            "work-1",
            "--payload-base64",
            base64.urlsafe_b64encode(b'{"asset":"safe"}').decode(),
            "--runtime-key",
            "cpu",
            "--duration-seconds",
            "1",
            "--config-sha256",
            "c" * 64,
            "--release-digest",
            "release",
        ],
        [
            "claim",
            "--owner",
            "owner",
            "--work-id",
            "work-1",
            "--max-items",
            "1",
            "--lease-seconds",
            "60",
        ],
        [
            "replay",
            "--work-id",
            "work-1",
            "--operator",
            "operator",
            "--reason",
            "test",
        ],
        ["recover"],
        ["reconcile"],
    )
    for command in commands:
        assert control_main(command) == 0
        json.loads(capsys.readouterr().out)
    assert store.claim.call_args.kwargs["allowed_work_ids"] == ["work-1"]


def test_fabric_control_bootstrap_creates_tables_and_rejects_bad_lock() -> None:
    store = FabricControlStoreImpl.__new__(FabricControlStoreImpl)
    store.tables = {name: f"table_{name}" for name in _SCHEMAS}
    spark = MagicMock()
    spark.catalog.tableExists.return_value = False
    frame = MagicMock()
    spark.createDataFrame.return_value = frame
    frame.write.format.return_value.mode.return_value.saveAsTable.return_value = None
    store.spark = spark
    rows = [{"lock_name": "global", "owner_id": None, "acquired_at": None}]
    store._rows = lambda _suffix: copy.deepcopy(rows)

    store.bootstrap()

    assert spark.createDataFrame.call_count == len(_SCHEMAS)
    assert frame.write.format.return_value.mode.return_value.saveAsTable.call_count == len(
        _SCHEMAS
    )
    rows[:] = []
    with pytest.raises(RuntimeError, match="exactly one"):
        store.bootstrap()
    rows[:] = [{"lock_name": "global", "owner_id": "owner", "acquired_at": None}]
    with pytest.raises(RuntimeError, match="inconsistent"):
        store.bootstrap()


def test_process_batch_verification_rejects_each_stale_identity_and_fence() -> None:
    store = MemoryControlStore(MemoryFiles())
    store.register(
        "work-1",
        {"batch_size": 1},
        runtime_key="cpu",
        duration_seconds=1,
        config_sha256="c" * 64,
        release_digest="release",
    )
    claim = store.claim(
        "owner-1",
        max_items=1,
        lease_seconds=60,
        minimum_speed_x=1,
        safety_factor=1,
        margin_seconds=0.1,
    )
    assert claim is not None
    raw, digest = store.load_claim_envelope_with_digest(claim.batch_id)
    from people_counter.sjd_process import verify_envelope

    envelope = verify_envelope(
        raw, batch_id=claim.batch_id, envelope_sha256=digest
    )
    for changed in (
        replace(envelope, envelope_sha256="f" * 64),
        replace(envelope, membership_sha256="e" * 64),
        replace(
            envelope,
            items=(
                replace(envelope.items[0], fence=envelope.items[0].fence + 1),
            ),
        ),
    ):
        with pytest.raises(Exception):
            store.process_batch_state(changed)

    work = store.data["work"][0]
    original = copy.deepcopy(work)
    for field, value in (
        ("status", "READY"),
        ("lease_attempt_id", "other-attempt"),
        ("fence", int(work["fence"]) + 1),
    ):
        store.data["work"][0] = copy.deepcopy(original)
        store.data["work"][0][field] = value
        with pytest.raises(Exception, match="lost its work fence"):
            store.process_batch_state(envelope)
    store.data["work"][0] = original
    store.seal_batch(
        claim.batch_id,
        [
            {
                "work_id": envelope.items[0].work_id,
                "attempt_id": envelope.items[0].attempt_id,
                "succeeded": True,
                "output_path": "Files/safe",
                "output_sha256": "a" * 64,
                "records": [
                    {
                        "executor_identity": "executor-1",
                        "partition_id": 0,
                        "task_attempt_id": 0,
                        "record_sequence": 0,
                    }
                ],
            }
        ],
        envelope_sha256=digest,
        membership_sha256=envelope.membership_sha256,
    )
    store.data["work"][0]["fence"] = int(store.data["work"][0]["fence"]) + 1
    with pytest.raises(Exception, match="lost its work fence"):
        store.process_batch_state(envelope)
    store.data["work"][0]["fence"] = envelope.items[0].fence
    store.commit_batch(claim.batch_id)
    assert store.process_batch_state(envelope).status == "COMMITTED"


def test_recovery_retries_expired_lease_and_reconciliation_resolves_findings() -> None:
    store = MemoryControlStore(MemoryFiles())
    store.register(
        "work-1",
        {"batch_size": 1},
        runtime_key="cpu",
        duration_seconds=1,
        config_sha256="c" * 64,
        release_digest="release",
        max_attempts=2,
    )
    claim = store.claim(
        "owner-1",
        max_items=1,
        lease_seconds=10,
        minimum_speed_x=1,
        safety_factor=1,
        margin_seconds=0.1,
    )
    assert claim is not None

    report = store.recover(now=111.0)

    assert report.recovered == 1
    assert report.retried == 1
    assert report.dead == 0
    assert store.data["batches"][0]["status"] == "EXPIRED"
    assert store.data["attempts"][0]["status"] == "EXPIRED"
    assert store.data["work"][0]["status"] == "READY"
    assert store.recover(now=112.0).recovered == 0
    assert store.reconcile() == []

    store.data["work"][0]["status"] = "SUCCEEDED"
    store.data["work"][0]["committed_attempt_id"] = "missing"
    findings = store.reconcile()
    assert findings
    assert any(item.severity == "ERROR" for item in findings)
    store.data["work"][0]["status"] = "READY"
    store.data["work"][0]["committed_attempt_id"] = None
    assert store.reconcile() == []
    assert all(
        row["resolved_at"] is not None
        for row in store.data["reconciliation_findings"]
    )


def test_exact_recovery_requires_expiry_identity_fence_and_no_output() -> None:
    files = MemoryFiles()
    store = MemoryControlStore(files)
    store.register(
        "work-exact",
        {"batch_size": 1},
        runtime_key="cpu",
        duration_seconds=1,
        config_sha256="c" * 64,
        release_digest="release",
        max_attempts=1,
    )
    claim = store.claim(
        "owner-exact",
        max_items=1,
        lease_seconds=10,
        minimum_speed_x=1,
        safety_factor=1,
        margin_seconds=0.1,
    )
    assert claim is not None
    item = claim.items[0]
    arguments = {
        "work_id": item.work_id,
        "batch_id": claim.batch_id,
        "attempt_id": item.attempt_id,
        "owner": "owner-exact",
        "fence": item.fence,
        "lease_expires_at": claim.lease_expires_at,
        "envelope_sha256": claim.envelope_sha256,
        "membership_sha256": store.data["batches"][0]["membership_sha256"],
        "safe_skew_seconds": 1.0,
    }
    with pytest.raises(Exception, match="safe skew"):
        store.recover_exact(**arguments, now=claim.lease_expires_at)
    with pytest.raises(Exception, match="fence must be positive"):
        store.recover_exact(
            **{**arguments, "fence": 0},
            now=claim.lease_expires_at + 2,
        )
    for name, value in (
        ("work_id", "wrong-work"),
        ("batch_id", "wrong-batch"),
        ("owner", "wrong-owner"),
        ("fence", item.fence + 1),
        ("attempt_id", "wrong-attempt"),
        ("lease_expires_at", claim.lease_expires_at + 1),
        ("envelope_sha256", "e" * 64),
        ("membership_sha256", "m" * 64),
    ):
        with pytest.raises(Exception):
            store.recover_exact(
                **{**arguments, name: value},
                now=claim.lease_expires_at + 2,
            )

    row_mismatches = (
        ("work", "status", "READY"),
        ("batches", "status", "READY"),
        ("attempts", "status", "READY"),
        ("work", "lease_owner", "wrong-owner"),
        ("work", "lease_attempt_id", "wrong-attempt"),
        ("attempts", "work_id", "wrong-work"),
        ("attempts", "batch_id", "wrong-batch"),
        ("work", "fence", item.fence + 1),
        ("attempts", "fence", item.fence + 1),
        ("batch_members", "fence", item.fence + 1),
        ("batch_members", "payload_sha256", "p" * 64),
        ("work", "lease_expires_at", claim.lease_expires_at + 1),
        ("batches", "lease_expires_at", claim.lease_expires_at + 1),
        ("attempts", "lease_expires_at", claim.lease_expires_at + 1),
        ("batches", "envelope_sha256", "e" * 64),
        ("batches", "membership_sha256", "m" * 64),
        ("work", "attempt_count", 0),
        ("work", "committed_attempt_id", "committed-attempt"),
    )
    for table, field, value in row_mismatches:
        row = store.data[table][0]
        original = row[field]
        row[field] = value
        with pytest.raises(Exception):
            store.recover_exact(
                **arguments, now=claim.lease_expires_at + 2
            )
        row[field] = original

    duplicate_member = copy.deepcopy(store.data["batch_members"][0])
    store.data["batch_members"].append(duplicate_member)
    with pytest.raises(Exception, match="membership is not singular"):
        store.recover_exact(**arguments, now=claim.lease_expires_at + 2)
    store.data["batch_members"].pop()
    store.data["publications"].append({"work_id": item.work_id})
    with pytest.raises(Exception):
        store.recover_exact(**arguments, now=claim.lease_expires_at + 2)
    store.data["publications"].clear()
    duplicate_attempt = copy.deepcopy(store.data["attempts"][0])
    duplicate_attempt["attempt_id"] = "newer-attempt"
    store.data["attempts"].append(duplicate_attempt)
    with pytest.raises(Exception):
        store.recover_exact(**arguments, now=claim.lease_expires_at + 2)
    store.data["attempts"].pop()
    duplicate_attempt["fence"] = item.fence + 1
    store.data["attempts"].append(duplicate_attempt)
    with pytest.raises(Exception):
        store.recover_exact(**arguments, now=claim.lease_expires_at + 2)
    store.data["attempts"].pop()

    from people_counter.sjd_process import verify_envelope

    raw, digest = store.load_claim_envelope_with_digest(claim.batch_id)
    envelope_path = store.data["batches"][0]["envelope_path"]
    original_envelope = files.content[envelope_path]
    changed_envelope = copy.deepcopy(raw)
    changed_envelope["owner"] = "wrong-envelope-owner"
    changed_encoded = json.dumps(
        changed_envelope,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    changed_digest = _sha256_bytes(changed_encoded.encode())
    files.content[envelope_path] = changed_encoded
    store.data["batches"][0]["envelope_sha256"] = changed_digest
    with pytest.raises(Exception, match="immutable envelope differs"):
        store.recover_exact(
            **{**arguments, "envelope_sha256": changed_digest},
            now=claim.lease_expires_at + 2,
        )
    files.content[envelope_path] = original_envelope
    store.data["batches"][0]["envelope_sha256"] = digest
    verified = verify_envelope(
        raw, batch_id=claim.batch_id, envelope_sha256=digest
    )
    staged = (
        f"{store.config.file_path('attempts')}/process/"
        f"batch={claim.batch_id}/attempt={verified.execution_attempt_id}/_SUCCESS"
    )
    files.content[staged] = "{}"
    with pytest.raises(Exception, match="staged or committed"):
        store.recover_exact(
            **arguments, now=claim.lease_expires_at + 2
        )
    del files.content[staged]
    delta_log = staged.removesuffix("_SUCCESS") + "_delta_log"
    files.content[delta_log] = "{}"
    with pytest.raises(Exception, match="staged or committed"):
        store.recover_exact(
            **arguments, now=claim.lease_expires_at + 2
        )
    del files.content[delta_log]

    result = store.recover_exact(
        **{**arguments, "safe_skew_seconds": 0.0},
        now=claim.lease_expires_at + 2,
    )
    assert result["outcome"] == "DEAD"
    assert result["idempotent"] is False
    assert store.data["work"][0]["status"] == "DEAD"
    assert store.data["batches"][0]["status"] == "EXPIRED"
    assert store.data["attempts"][0]["status"] == "EXPIRED"
    assert store.data["attempts"][0]["recovery_outcome"] == "DEAD"
    terminal_mismatches = (
        ("work", "status", "READY"),
        ("work", "lease_owner", "owner-exact"),
        ("work", "lease_attempt_id", item.attempt_id),
        ("work", "lease_expires_at", claim.lease_expires_at),
        ("work", "committed_attempt_id", "committed-attempt"),
        ("batches", "owner", "wrong-owner"),
        ("work", "fence", item.fence + 1),
        ("attempts", "fence", item.fence + 1),
        ("batch_members", "fence", item.fence + 1),
        ("batch_members", "payload_sha256", "p" * 64),
        ("batches", "lease_expires_at", claim.lease_expires_at + 1),
        ("attempts", "lease_expires_at", claim.lease_expires_at + 1),
        ("batches", "envelope_sha256", "e" * 64),
        ("batches", "membership_sha256", "m" * 64),
        ("batches", "status", "LEASED"),
        ("attempts", "status", "LEASED"),
        ("attempts", "recovery_outcome", None),
    )
    for table, field, value in terminal_mismatches:
        row = store.data[table][0]
        original = row[field]
        row[field] = value
        with pytest.raises(Exception):
            store.recover_exact(
                **arguments, now=claim.lease_expires_at + 3
            )
        row[field] = original
    store.data["publications"].append({"work_id": item.work_id})
    with pytest.raises(Exception):
        store.recover_exact(**arguments, now=claim.lease_expires_at + 3)
    store.data["publications"].clear()
    duplicate_attempt = copy.deepcopy(store.data["attempts"][0])
    duplicate_attempt["attempt_id"] = "terminal-extra-attempt"
    store.data["attempts"].append(duplicate_attempt)
    with pytest.raises(Exception):
        store.recover_exact(**arguments, now=claim.lease_expires_at + 3)
    store.data["attempts"].pop()
    assert store.recover_exact(
        **arguments, now=claim.lease_expires_at + 3
    )["idempotent"] is True


def test_exact_recovery_accepts_expiry_boundary_and_default_clock() -> None:
    store = MemoryControlStore(MemoryFiles())
    store.register(
        "work-boundary",
        {"batch_size": 1},
        runtime_key="cpu",
        duration_seconds=1,
        config_sha256="c" * 64,
        release_digest="release",
        max_attempts=1,
    )
    claim = store.claim(
        "owner-boundary",
        max_items=1,
        lease_seconds=10,
        minimum_speed_x=1,
        safety_factor=1,
        margin_seconds=0.1,
    )
    assert claim is not None
    item = claim.items[0]
    store._clock_value = claim.lease_expires_at + 1.0

    result = store.recover_exact(
        work_id=item.work_id,
        batch_id=claim.batch_id,
        attempt_id=item.attempt_id,
        owner="owner-boundary",
        fence=item.fence,
        lease_expires_at=claim.lease_expires_at,
        envelope_sha256=claim.envelope_sha256,
        membership_sha256=store.data["batches"][0]["membership_sha256"],
        safe_skew_seconds=1.0,
    )

    assert result["outcome"] == "DEAD"
    assert result["idempotent"] is False


@pytest.mark.parametrize(
    ("table", "field", "value"),
    (
        ("work", "status", "READY"),
        ("work", "lease_owner", "still-owned"),
        ("batches", "status", "LEASED"),
        ("attempts", "status", "LEASED"),
        ("attempts", "recovery_outcome", None),
    ),
)
def test_exact_recovery_rejects_non_atomic_readback(
    table: str, field: str, value: object
) -> None:
    class CorruptingStore(MemoryControlStore):
        corrupt = False

        def _replace_many(self, **tables: list[dict[str, object]]) -> None:
            super()._replace_many(**tables)
            if self.corrupt and {"work", "batches", "attempts"} <= set(tables):
                self.data[table][0][field] = value

    store = CorruptingStore(MemoryFiles())
    store.register(
        "work-readback",
        {"batch_size": 1},
        runtime_key="cpu",
        duration_seconds=1,
        config_sha256="c" * 64,
        release_digest="release",
        max_attempts=1,
    )
    claim = store.claim(
        "owner-readback",
        max_items=1,
        lease_seconds=10,
        minimum_speed_x=1,
        safety_factor=1,
        margin_seconds=0.1,
    )
    assert claim is not None
    item = claim.items[0]
    store.corrupt = True
    with pytest.raises(Exception, match="readback differs"):
        store.recover_exact(
            work_id=item.work_id,
            batch_id=claim.batch_id,
            attempt_id=item.attempt_id,
            owner="owner-readback",
            fence=item.fence,
            lease_expires_at=claim.lease_expires_at,
            envelope_sha256=claim.envelope_sha256,
            membership_sha256=store.data["batches"][0]["membership_sha256"],
            now=claim.lease_expires_at + 1,
            safe_skew_seconds=0,
        )


def test_clear_stale_lock_uses_exact_owner_cas_and_exact_readback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Expression:
        def __eq__(self, _other: object) -> "Expression":
            return self

        def __and__(self, _other: object) -> "Expression":
            return self

        def cast(self, _name: str) -> "Expression":
            return self

    functions = SimpleNamespace(
        col=lambda _name: Expression(), lit=lambda _value: Expression()
    )
    state = {
        "rows": [
            {
                "lock_name": "global",
                "owner_id": "owner-1",
                "acquired_at": datetime(2026, 10, 4),
            }
        ]
    }
    frame = MagicMock()
    frame.select.return_value.limit.return_value.collect.side_effect = (
        lambda: copy.deepcopy(state["rows"])
    )
    spark = MagicMock()
    spark.table.return_value = frame

    class Delta:
        def update(self, **_kwargs: object) -> None:
            state["rows"][0]["owner_id"] = None
            state["rows"][0]["acquired_at"] = None

    delta_table = SimpleNamespace(forName=lambda *_args: Delta())
    monkeypatch.setitem(
        sys.modules, "pyspark.sql", SimpleNamespace(functions=functions)
    )
    monkeypatch.setitem(
        sys.modules, "delta.tables", SimpleNamespace(DeltaTable=delta_table)
    )

    result = _clear_stale_lock(spark, "owner-1")

    assert result["cleared_owner_id"] == "owner-1"
    with pytest.raises(RuntimeError, match="precondition"):
        _clear_stale_lock(spark, "other-owner")
