from __future__ import annotations

import copy
import json
import hashlib
import hmac
import io
from datetime import date, datetime, timezone
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

import people_counter.fabric_production_shadow_live as live
from people_counter.fabric_candidate_a import PRODUCTION_FILES_ROOT
from people_counter.fabric_production_migration import (
    JOURNAL_TABLE,
    MIGRATION_ID,
    ApplyStatus,
    JournalEntry,
    evidence_hash,
)
from people_counter.fabric_production_migration_live import (
    DISPATCHER_LEASE_TABLE,
    LOCK_TABLE,
    REGISTRATION_LEASE_TABLE,
    WORK_TABLE,
)
from people_counter.fabric_production_shadow import (
    PRODUCTION_ALLOWLIST_TABLE,
    PRODUCTION_AUDIT_TABLE,
    AuthorizationIntent,
)


HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
HASH_E = "e" * 64
HASH_F = "f" * 64
WORK_ID = "work-001"
ATTEMPT_ID = "attempt-001"
AUTH_ID = "1" * 64


class MemoryFiles:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.creates: list[str] = []

    def exists(self, path: str) -> bool:
        return path in self.values

    def read_bytes(self, path: str) -> bytes:
        return self.values[path]

    def create_bytes(self, path: str, content: bytes) -> None:
        if path in self.values:
            raise FileExistsError(path)
        self.values[path] = content
        self.creates.append(path)


class FakeRow(dict[str, Any]):
    def asDict(self, recursive: bool = True) -> dict[str, Any]:
        return dict(self)


class FakeFrame:
    def __init__(self, spark: "FakeSpark", rows: list[dict[str, Any]]) -> None:
        self.spark = spark
        self.rows = rows
        self.write = self
        self.target: str | None = None

    def collect(self) -> list[FakeRow]:
        assert self.target is not None
        return [FakeRow(value) for value in self.spark.tables[self.target]]

    def format(self, value: str) -> "FakeFrame":
        assert value == "delta"
        return self

    def mode(self, value: str) -> "FakeFrame":
        assert value == "append"
        return self

    def saveAsTable(self, table: str) -> None:
        if self.spark.fail_table == table:
            raise RuntimeError("injected ambiguous append")
        self.spark.tables.setdefault(table, []).extend(
            json.loads(json.dumps(self.rows))
        )


class FakeCatalog:
    def __init__(self, spark: "FakeSpark") -> None:
        self.spark = spark

    def tableExists(self, table: str) -> bool:
        return table in self.spark.tables

    def refreshTable(self, table: str) -> None:
        assert table in self.spark.tables


class FakeSpark:
    def __init__(self) -> None:
        self.tables = source_tables()
        self.catalog = FakeCatalog(self)
        self.fail_table: str | None = None
        self.artifact_reader = lambda path: path.startswith(
            ("/lakehouse/default/Files/", "Files/models/")
        )

    def table(self, table: str) -> FakeFrame:
        if table not in self.tables:
            raise KeyError(table)
        frame = FakeFrame(self, [])
        frame.target = table
        return frame

    def createDataFrame(self, rows: list[dict[str, Any]]) -> FakeFrame:
        return FakeFrame(self, rows)


def source_tables() -> dict[str, list[dict[str, Any]]]:
    identity = {
        "camera_sha256": HASH_A,
        "location_sha256": HASH_B,
        "model_sha256": HASH_C,
        "source_sha256": HASH_D,
        "config_sha256": HASH_E,
    }
    shared = {
        "work_id": WORK_ID,
        "attempt_id": ATTEMPT_ID,
        "output_path": f"{PRODUCTION_FILES_ROOT}attempts/work={WORK_ID}/data.json",
        "output_sha256": HASH_F,
    }
    provisional = JournalEntry(
        journal_id="journal-001",
        migration_id=MIGRATION_ID,
        migration_version=1,
        status=ApplyStatus.APPLIED,
        plan_sha256="9" * 64,
        before_sha256="7" * 64,
        after_sha256="8" * 64,
        receipts_sha256="6" * 64,
        previous_evidence_sha256="0" * 64,
        evidence_sha256="",
        error_text="",
    )
    journal = JournalEntry(
        **{
            **provisional.__dict__,
            "evidence_sha256": evidence_hash(provisional.evidence()),
        }
    )
    return {
        JOURNAL_TABLE: [journal.to_dict()],
        LOCK_TABLE: [
            {"lock_name": "global", "owner_id": None, "acquired_at": None}
        ],
        DISPATCHER_LEASE_TABLE: [],
        REGISTRATION_LEASE_TABLE: [],
        WORK_TABLE: [
            {
                **shared,
                **identity,
                "status": "SUCCEEDED",
                "committed_attempt_id": ATTEMPT_ID,
                "capture_date": date(2026, 10, 5),
                "payload": {
                    **identity,
                    "duration_seconds": 10.0,
                    "runtime_key": "cpu",
                    "source_video": (
                        "/lakehouse/default/Files/immutable/source.mp4"
                    ),
                },
            }
        ],
        "people_counter_video_attempts": [
            {
                **shared,
                "status": "SUCCEEDED",
                "fence": 4,
                "capture_date": date(2026, 10, 5),
            }
        ],
        "people_counter_ca_publications": [
            {**shared, "publication_sequence": 7, "published_at": 1_799_000_000.0}
        ],
        "people_counter_runs_committed": [
            {
                **shared,
                "records": [{"frame": 1, "count": 2.0}],
                "publication_sequence": 7,
            }
        ],
        PRODUCTION_ALLOWLIST_TABLE: [],
        PRODUCTION_AUDIT_TABLE: [],
    }


def request() -> dict[str, Any]:
    return {
        "rest_snapshot": {
            "reflex": {"id": live.REFLEX_ID, "active": False},
            "pipelines": [
                {"id": identifier, "active_jobs": []}
                for _, identifier in live.WRITER_ITEMS
            ],
        }
    }


def cas_for(spark: FakeSpark):
    def build(_: object):
        def cas(expected: str | None, replacement: str | None) -> None:
            row = spark.tables[LOCK_TABLE][0]
            if row["owner_id"] == expected:
                row["owner_id"] = replacement
                row["acquired_at"] = (
                    None
                    if replacement is None
                    else datetime.now(timezone.utc).isoformat()
                )

        return cas

    return build


def authorization_request(
    control: live.SparkShadowControl,
) -> tuple[dict[str, Any], AuthorizationIntent]:
    route = control.eligible_routes()[0]
    allowlist = {
        **route["identity"],
        "plan_sha256": "2" * 64,
        "approved_at": "2026-10-05T04:00:00+00:00",
        "approved_by": "release-control",
    }
    audit = {
        "audit_id": AUTH_ID,
        "work_id": WORK_ID,
        "plan_sha256": "2" * 64,
        "shadow_attempt_id": "AUTHORIZATION",
        "legacy_attempt_id": "3" * 64,
        "comparison_sha256": "4" * 64,
        "critical_findings": 0,
        "recorded_at": "2026-10-05T04:00:00+00:00",
    }
    row = {
        "authorization_id": AUTH_ID,
        "work_id": WORK_ID,
        "plan_sha256": "2" * 64,
        "work_identity_sha256": "3" * 64,
        "expires_at": "2027-10-05T04:10:00+00:00",
    }
    intent = live.authorization_intent_from_request(row, allowlist, audit)
    value = {
        **request(),
        "invocation_id": "invoke-auth-001",
        "authorization": {
            "row": row,
            "work": route["identity"],
            "allowlist": allowlist,
            "audit": audit,
            "intent": intent.as_dict(),
            "migration_plan_sha256": "9" * 64,
            "legacy_source_rows_sha256": route["source_rows_sha256"],
            "legacy_output_sha256": route["output_sha256"],
            "expires_at": "2027-10-05T04:10:00+00:00",
            "receipt_reviewed_at": "2026-10-05T04:00:00+00:00",
        },
    }
    return value, intent


def test_live_paths_are_fixed_and_reject_escape() -> None:
    assert live.request_path("invoke-001").endswith(
        "/invocations/invoke-001/request.json"
    )
    assert live.result_path("invoke-001").endswith(
        "/invocations/invoke-001/result.json"
    )
    assert live.failure_path("invoke-001").endswith(
        "/invocations/invoke-001/failure.json"
    )
    with pytest.raises(live.LiveShadowError):
        live.request_path("../escape")
    with pytest.raises(live.LiveShadowError):
        live.intent_path(AUTH_ID, "overwrite")
    assert live.NotebookShadowEvidenceFiles._path(
        live.request_path("invoke-001")
    ) == live.request_path("invoke-001")
    with pytest.raises(live.LiveShadowError, match="escaped"):
        live.NotebookShadowEvidenceFiles._path(
            "Files/_shadow/people-counter/candidate-a/v1/not-live.json"
        )


def test_migration_proof_validates_full_chain_and_uses_terminal_success() -> None:
    spark = FakeSpark()
    first = JournalEntry(
        **{
            **JournalEntry(
                journal_id="journal-first",
                migration_id=MIGRATION_ID,
                migration_version=1,
                status=ApplyStatus.APPLIED,
                plan_sha256="1" * 64,
                before_sha256="2" * 64,
                after_sha256="3" * 64,
                receipts_sha256="4" * 64,
                previous_evidence_sha256="0" * 64,
                evidence_sha256="",
                error_text="",
            ).__dict__,
        }
    )
    first = JournalEntry(
        **{
            **first.__dict__,
            "evidence_sha256": evidence_hash(first.evidence()),
        }
    )
    second = JournalEntry(
        journal_id="journal-second",
        migration_id=MIGRATION_ID,
        migration_version=1,
        status=ApplyStatus.NOOP,
        plan_sha256="5" * 64,
        before_sha256="3" * 64,
        after_sha256="3" * 64,
        receipts_sha256="6" * 64,
        previous_evidence_sha256=first.evidence_sha256,
        evidence_sha256="",
        error_text="",
    )
    second = JournalEntry(
        **{
            **second.__dict__,
            "evidence_sha256": evidence_hash(second.evidence()),
        }
    )
    spark.tables[JOURNAL_TABLE] = [second.to_dict(), first.to_dict()]
    control = live.SparkShadowControl(spark, MemoryFiles())

    proof = control.migration_proof()

    assert proof == {
        "journal_sha256": live.sha256_json(
            [first.to_dict(), second.to_dict()]
        ),
        "migration_id": MIGRATION_ID,
        "plan_sha256": second.plan_sha256,
        "status": "NOOP",
    }


def test_eligible_route_inventory_is_sorted_and_hash_pinned() -> None:
    control = live.SparkShadowControl(
        FakeSpark(), MemoryFiles(), clock=lambda: 1_799_000_000.0
    )

    routes = control.eligible_routes()

    assert [value["work_id"] for value in routes] == [WORK_ID]
    assert routes[0]["identity"] == {
        "work_id": WORK_ID,
        "camera_sha256": HASH_A,
        "location_sha256": HASH_B,
        "model_sha256": HASH_C,
        "source_sha256": HASH_D,
        "config_sha256": HASH_E,
    }
    assert routes[0]["output_sha256"] == HASH_F
    assert routes[0]["rows"]["work"]["capture_date"] == "2026-10-05"
    assert routes[0]["rows"]["attempt"]["capture_date"] == "2026-10-05"
    assert len(routes[0]["source_rows_sha256"]) == 64


@pytest.mark.parametrize(
    "mutation",
    [
        "missing-attempt",
        "invalid-identity",
        "outside-output",
    ],
)
def test_eligible_route_inventory_skips_incomplete_or_unsafe(
    mutation: str,
) -> None:
    spark = FakeSpark()
    if mutation == "missing-attempt":
        spark.tables["people_counter_video_attempts"] = []
        spark.tables["people_counter_video_attempts_committed"] = []
    elif mutation == "invalid-identity":
        spark.tables[WORK_TABLE][0]["model_sha256"] = "not-a-hash"
    else:
        spark.tables["people_counter_video_attempts"][0][
            "output_path"
        ] = "Files/outside/result.json"
    control = live.SparkShadowControl(spark, MemoryFiles())

    assert control.eligible_routes() == []


def test_route_diagnostics_are_redacted_and_predicate_consistent() -> None:
    spark = FakeSpark()
    secret = (
        "abfss://sensitive-container@storage.invalid/"
        "private/customer/video.mp4?sig=top-secret"
    )
    spark.tables[WORK_TABLE][0]["payload"]["source_video"] = secret
    control = live.SparkShadowControl(spark, MemoryFiles())

    diagnostics = control.route_diagnostics()

    assert len(diagnostics) == 1
    diagnostic = diagnostics[0]
    assert set(diagnostic) == {
        "candidate_id",
        "eligible",
        "fields",
        "joins",
        "rejection_codes",
    }
    assert len(diagnostic["candidate_id"]) == 64
    assert diagnostic["eligible"] is False
    assert diagnostic["fields"]["source_artifact_path_present"] is True
    assert diagnostic["fields"]["source_artifact_path_supported"] is False
    assert diagnostic["fields"]["source_artifact_readable"] is False
    assert "MISSING_READABLE_SOURCE_ARTIFACT" in diagnostic["rejection_codes"]
    encoded = json.dumps(diagnostics, sort_keys=True)
    assert secret not in encoded
    assert "top-secret" not in encoded
    assert all(
        isinstance(value, bool)
        for section in ("fields", "joins")
        for value in diagnostic[section].values()
    )
    assert diagnostic["eligible"] == (not diagnostic["rejection_codes"])


@pytest.mark.parametrize(
    ("mutation", "reason", "field"),
    [
        ("missing-pointer", "MISSING_POINTER", "pointer_present"),
        ("pointer-mismatch", "POINTER_MISMATCH", "attempt_pointer_match"),
        (
            "missing-camera",
            "MISSING_CAMERA_SHA256",
            "camera_sha256_present",
        ),
        (
            "invalid-config",
            "INVALID_CONFIG_SHA256",
            "config_sha256_valid",
        ),
        (
            "ambiguous-view",
            "DUPLICATE_AMBIGUOUS_COMMITTED_VIEW_ROW",
            "view_unique",
        ),
    ],
)
def test_route_diagnostic_reasons_match_observed_predicates(
    mutation: str,
    reason: str,
    field: str,
) -> None:
    spark = FakeSpark()
    work = spark.tables[WORK_TABLE][0]
    if mutation == "missing-pointer":
        work["committed_attempt_id"] = None
    elif mutation == "pointer-mismatch":
        work["committed_attempt_id"] = "other-attempt"
    elif mutation == "missing-camera":
        work.pop("camera_sha256")
        work["payload"].pop("camera_sha256")
    elif mutation == "invalid-config":
        work["config_sha256"] = "not-a-digest"
    else:
        spark.tables["people_counter_runs_committed"].append(
            dict(spark.tables["people_counter_runs_committed"][0])
        )

    diagnostic = live.SparkShadowControl(
        spark, MemoryFiles()
    ).route_diagnostics()[0]
    observations = {**diagnostic["fields"], **diagnostic["joins"]}

    assert reason in diagnostic["rejection_codes"]
    assert observations[field] is False
    assert diagnostic["eligible"] is False


def test_authorization_prepare_partial_replay_commits_and_releases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark, files = FakeSpark(), MemoryFiles()
    control = live.SparkShadowControl(
        spark, files, clock=lambda: 1_799_000_000.0
    )
    monkeypatch.setattr(live, "delta_control_writer_cas", cas_for(spark))
    value, intent = authorization_request(control)
    spark.fail_table = PRODUCTION_AUDIT_TABLE

    with pytest.raises(RuntimeError, match="injected ambiguous append"):
        control.authorize(value)

    owner = str(spark.tables[LOCK_TABLE][0]["owner_id"])
    assert owner.startswith(f"pc-shadow-auth-{AUTH_ID[:24]}-")
    assert len(spark.tables[PRODUCTION_ALLOWLIST_TABLE]) == 1
    assert spark.tables[PRODUCTION_AUDIT_TABLE] == []
    assert files.exists(live.intent_path(AUTH_ID, "00-prepared"))
    assert files.exists(live.intent_path(AUTH_ID, "10-allowlist"))
    assert not files.exists(live.intent_path(AUTH_ID, "30-committed"))

    spark.fail_table = None
    value["nonce"] = "nonce-recovery"
    result = control.authorize(value)

    assert result == {
        "authorization_id": AUTH_ID,
        "protocol_state": "COMMITTED",
        "recovered_from": "ALLOWLIST_APPENDED",
        "lock_retained": False,
    }
    assert spark.tables[LOCK_TABLE][0]["owner_id"] is None
    assert len(spark.tables[PRODUCTION_ALLOWLIST_TABLE]) == 1
    assert len(spark.tables[PRODUCTION_AUDIT_TABLE]) == 1
    assert files.exists(live.intent_path(AUTH_ID, "30-committed"))
    stored = json.loads(files.read_bytes(live.intent_path(AUTH_ID, "00-prepared")))
    assert stored["intent"] == intent.as_dict()


def test_authorization_active_same_operation_cannot_concurrently_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark, files = FakeSpark(), MemoryFiles()
    now = 1_799_000_000.0
    control = live.SparkShadowControl(spark, files, clock=lambda: now)
    monkeypatch.setattr(live, "delta_control_writer_cas", cas_for(spark))
    value, _ = authorization_request(control)
    spark.fail_table = PRODUCTION_AUDIT_TABLE

    with pytest.raises(RuntimeError, match="injected ambiguous append"):
        control.authorize(value)

    spark.fail_table = None
    spark.tables[LOCK_TABLE][0]["acquired_at"] = datetime.fromtimestamp(
        now, timezone.utc
    ).isoformat()
    value["nonce"] = "concurrent-invocation"
    with pytest.raises(live.LiveShadowError, match="active"):
        control.authorize(value)
    assert spark.tables[PRODUCTION_AUDIT_TABLE] == []


def test_authorization_conflict_never_repairs_or_releases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark, files = FakeSpark(), MemoryFiles()
    control = live.SparkShadowControl(
        spark, files, clock=lambda: 1_799_000_000.0
    )
    monkeypatch.setattr(live, "delta_control_writer_cas", cas_for(spark))
    value, _ = authorization_request(control)
    spark.tables[PRODUCTION_ALLOWLIST_TABLE].append(
        {**value["authorization"]["allowlist"], "approved_by": "other"}
    )

    with pytest.raises(live.LiveShadowError, match="conflict"):
        control.authorize(value)

    assert spark.tables[LOCK_TABLE][0]["owner_id"] is None
    assert spark.tables[PRODUCTION_AUDIT_TABLE] == []
    assert not files.exists(live.intent_path(AUTH_ID, "00-prepared"))


def test_authorization_exact_replay_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark, files = FakeSpark(), MemoryFiles()
    control = live.SparkShadowControl(
        spark, files, clock=lambda: 1_799_000_000.0
    )
    monkeypatch.setattr(live, "delta_control_writer_cas", cas_for(spark))
    value, _ = authorization_request(control)

    first = control.authorize(value)
    second = control.authorize(value)

    assert first["recovered_from"] == "EMPTY"
    assert second["recovered_from"] == "COMMITTED"
    assert len(spark.tables[PRODUCTION_ALLOWLIST_TABLE]) == 1
    assert len(spark.tables[PRODUCTION_AUDIT_TABLE]) == 1


def test_authorization_stale_committed_owner_is_exactly_released(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark, files = FakeSpark(), MemoryFiles()
    control = live.SparkShadowControl(
        spark, files, clock=lambda: 1_799_000_000.0
    )
    monkeypatch.setattr(live, "delta_control_writer_cas", cas_for(spark))
    value, _ = authorization_request(control)
    control.authorize(value)
    spark.tables[LOCK_TABLE][0].update(
        {
            "owner_id": f"pc-shadow-auth-{AUTH_ID[:24]}-deadbeef",
            "acquired_at": "2026-01-01T00:00:00+00:00",
        }
    )
    value["invocation_id"] = "invoke-auth-recovery"

    result = control.authorize(value)

    assert result["recovered_from"] == "COMMITTED"
    assert spark.tables[LOCK_TABLE][0]["owner_id"] is None


def test_authorization_expired_partial_intent_can_finish_exact_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark, files = FakeSpark(), MemoryFiles()
    now = [1_799_000_000.0]
    control = live.SparkShadowControl(spark, files, clock=lambda: now[0])
    monkeypatch.setattr(live, "delta_control_writer_cas", cas_for(spark))
    value, _ = authorization_request(control)
    spark.fail_table = PRODUCTION_AUDIT_TABLE
    with pytest.raises(RuntimeError, match="ambiguous append"):
        control.authorize(value)
    spark.fail_table = None
    now[0] = datetime.fromisoformat(
        value["authorization"]["expires_at"]
    ).timestamp() + 600
    value["invocation_id"] = "invoke-auth-expired-recovery"

    result = control.authorize(value)

    assert result["protocol_state"] == "COMMITTED"
    assert spark.tables[LOCK_TABLE][0]["owner_id"] is None


def test_authorization_ambiguous_allowlist_readback_retains_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark, files = FakeSpark(), MemoryFiles()
    control = live.SparkShadowControl(
        spark, files, clock=lambda: 1_799_000_000.0
    )
    monkeypatch.setattr(live, "delta_control_writer_cas", cas_for(spark))
    monkeypatch.setattr(control, "_append", lambda table, row: None)
    value, _ = authorization_request(control)

    with pytest.raises(live.LiveShadowError, match="allowlist append readback"):
        control.authorize(value)

    assert str(spark.tables[LOCK_TABLE][0]["owner_id"]).startswith(
        f"pc-shadow-auth-{AUTH_ID[:24]}-"
    )


def test_authorization_ambiguous_audit_readback_retains_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark, files = FakeSpark(), MemoryFiles()
    control = live.SparkShadowControl(
        spark, files, clock=lambda: 1_799_000_000.0
    )
    monkeypatch.setattr(live, "delta_control_writer_cas", cas_for(spark))
    original_append = control._append

    def drop_audit(table: str, row: Mapping[str, Any]) -> None:
        if table != PRODUCTION_AUDIT_TABLE:
            original_append(table, row)

    monkeypatch.setattr(control, "_append", drop_audit)
    value, _ = authorization_request(control)

    with pytest.raises(live.LiveShadowError, match="audit append readback"):
        control.authorize(value)

    assert len(spark.tables[PRODUCTION_ALLOWLIST_TABLE]) == 1
    assert spark.tables[PRODUCTION_AUDIT_TABLE] == []
    assert str(spark.tables[LOCK_TABLE][0]["owner_id"]).startswith(
        f"pc-shadow-auth-{AUTH_ID[:24]}-"
    )


def test_authorization_rejects_plan_pinned_legacy_hash_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark, files = FakeSpark(), MemoryFiles()
    control = live.SparkShadowControl(
        spark, files, clock=lambda: 1_799_000_000.0
    )
    monkeypatch.setattr(live, "delta_control_writer_cas", cas_for(spark))
    value, _ = authorization_request(control)
    value["authorization"]["legacy_output_sha256"] = "0" * 64

    with pytest.raises(live.LiveShadowError, match="route drifted"):
        control.authorize(value)

    assert spark.tables[LOCK_TABLE][0]["owner_id"] is None


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("missing", "identity is missing"),
        ("rows", "rows are not objects"),
        ("intent", "intent differs"),
        ("migration", "migration proof drifted"),
        ("quiescence", "quiescence preflight"),
    ],
)
def test_authorization_preflight_rejects_before_lock(
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    spark, files = FakeSpark(), MemoryFiles()
    control = live.SparkShadowControl(
        spark, files, clock=lambda: 1_799_000_000.0
    )
    monkeypatch.setattr(live, "delta_control_writer_cas", cas_for(spark))
    value, _ = authorization_request(control)
    if mutation == "missing":
        value.pop("authorization")
    elif mutation == "rows":
        value["authorization"]["audit"] = "not-an-object"
    elif mutation == "intent":
        value["authorization"]["intent"]["audit_sha256"] = "0" * 64
    elif mutation == "migration":
        value["authorization"]["migration_plan_sha256"] = "0" * 64
    else:
        value["rest_snapshot"]["reflex"]["active"] = True

    with pytest.raises(live.LiveShadowError, match=message):
        control.authorize(value)

    assert spark.tables[LOCK_TABLE][0]["owner_id"] is None


def test_status_exposes_partial_recovery_without_mutation() -> None:
    spark, files = FakeSpark(), MemoryFiles()
    control = live.SparkShadowControl(
        spark, files, clock=lambda: 1_799_000_000.0
    )
    files.create_bytes(
        live.intent_path(AUTH_ID, "00-prepared"),
        b"{}\n",
    )

    result = control.status({**request(), "authorization_id": AUTH_ID})

    assert result["authorization_recovery"] == {
        "authorization_id": AUTH_ID,
        "stages": {
            "00-prepared": True,
            "10-allowlist": False,
            "20-audit": False,
            "30-committed": False,
        },
        "partial": True,
        "recovery": "exact authorize replay required",
    }


def test_quiescence_includes_active_fixed_pipeline_jobs() -> None:
    control = live.SparkShadowControl(
        FakeSpark(), MemoryFiles(), clock=lambda: 1_799_000_000.0
    )
    value = request()
    value["rest_snapshot"]["pipelines"][0]["active_jobs"] = [
        {"id": "job-running", "status": "Running"}
    ]

    result = control.quiescence(value)

    assert result["active_writer_ids"] == [
        f"{live.WRITER_ITEMS[0][1]}:job-running"
    ]


def test_quiescence_uses_owner_and_expiry_for_active_leases() -> None:
    spark = FakeSpark()
    spark.tables[DISPATCHER_LEASE_TABLE] = [
        {
            "lock_name": "free-dispatcher",
            "owner_id": "",
            "expires_at": 1_800_000_000.0,
        },
        {
            "lock_name": "expired-dispatcher",
            "owner_id": "old-owner",
            "expires_at": 1_798_999_999.0,
        },
    ]
    spark.tables[REGISTRATION_LEASE_TABLE] = [
        {
            "lock_name": "free-registration",
            "owner_id": "",
            "expires_at": datetime(2030, 1, 1, tzinfo=timezone.utc),
        },
        {
            "lock_name": "active-registration",
            "owner_id": "live-owner",
            "expires_at": datetime(2030, 1, 1, tzinfo=timezone.utc),
        },
        {
            "lock_name": "boundary-registration",
            "owner_id": "boundary-owner",
            "expires_at": 1_799_000_000.0,
        },
    ]
    control = live.SparkShadowControl(
        spark, MemoryFiles(), clock=lambda: 1_799_000_000.0
    )

    result = control.quiescence(request())

    assert result["active_lease_ids"] == [
        f"{REGISTRATION_LEASE_TABLE}:active-registration:live-owner"
    ]


def test_quiescence_rejects_invalid_owned_lease_expiry() -> None:
    spark = FakeSpark()
    spark.tables[REGISTRATION_LEASE_TABLE] = [
        {
            "lock_name": "invalid-registration",
            "owner_id": "live-owner",
            "expires_at": None,
        }
    ]
    control = live.SparkShadowControl(
        spark, MemoryFiles(), clock=lambda: 1_799_000_000.0
    )

    with pytest.raises(
        live.LiveShadowError,
        match="registration_leases active lease expiry is invalid",
    ):
        control.quiescence(request())


def test_route_readback_is_fixed_to_legacy_or_shadow_namespace() -> None:
    control = live.SparkShadowControl(
        FakeSpark(), MemoryFiles(), clock=lambda: 1_799_000_000.0
    )
    context = {
        "authorization_id": AUTH_ID,
        "plan_sha256": "2" * 64,
        "identity": control.eligible_routes()[0]["identity"],
        "provenance": {
            "package_version": "0.9.11",
            "package_sha256": "6" * 64,
            "python_version": "3.13",
            "spark_version": "4.1.1",
            "java_version": "21",
            "fabric_runtime": "2.0",
        },
    }

    value = control.route(
        {
            "selected_work_id": WORK_ID,
            "route_kind": "legacy",
            "route_context": context,
        }
    )

    assert value["work_id"] == WORK_ID
    assert value["attempt_id"] == ATTEMPT_ID
    assert value["publication_count"] == 1
    assert value["output_path"].startswith(PRODUCTION_FILES_ROOT)
    with pytest.raises(live.LiveShadowError, match="route kind"):
        control.route(
            {
                "selected_work_id": WORK_ID,
                "route_kind": "production",
                "route_context": context,
            }
        )


def test_shadow_route_absent_then_exact_committed() -> None:
    spark = FakeSpark()
    files = MemoryFiles()
    control = live.SparkShadowControl(
        spark, files, clock=lambda: 1_799_000_000.0
    )
    context = {
        "authorization_id": AUTH_ID,
        "plan_sha256": "2" * 64,
        "identity": control.eligible_routes()[0]["identity"],
        "provenance": {
            "package_version": "0.9.11",
            "package_sha256": "6" * 64,
            "python_version": "3.13",
            "spark_version": "4.1.1",
            "java_version": "21",
            "fabric_runtime": "2.0",
        },
    }
    selected = {
        "selected_work_id": WORK_ID,
        "route_kind": "shadow",
        "route_context": context,
    }

    assert control.route(selected) == {"route": None}

    config = control.config
    spark.tables[config.table("work")] = [
        {
            "work_id": WORK_ID,
            "status": "SUCCEEDED",
            "committed_attempt_id": "shadow-attempt",
            "fence": 5,
        }
    ]
    spark.tables[config.table("attempts")] = [
        {
            "attempt_id": "shadow-attempt",
            "work_id": WORK_ID,
            "status": "SUCCEEDED",
            "fence": 5,
            "output_path": (
                "Files/_shadow/people-counter/candidate-a/v1/out.json"
            ),
            "output_sha256": HASH_F,
            "records_json": '[{"count":2.0}]',
        }
    ]
    spark.tables[config.table("publications")] = [
        {
            "publication_sequence": 1,
            "work_id": WORK_ID,
            "attempt_id": "shadow-attempt",
            "published_at": 1_799_000_000.0,
        }
    ]
    files.create_bytes(
        live.route_binding_path(WORK_ID),
        live._canonical(
            {
                "schema": "people-counter-shadow-route-binding-v1",
                "work_id": WORK_ID,
                "route_context": context,
            }
        ),
    )

    result = control.route(selected)

    assert result["attempt_id"] == "shadow-attempt"
    assert result["pointer_attempt_id"] == "shadow-attempt"
    assert result["pointer_fence"] == 5
    assert result["logical_total"] == 2.0
    with pytest.raises(live.LiveShadowError, match="binding differs"):
        control.route(
            {
                **selected,
                "route_context": {**context, "plan_sha256": "9" * 64},
            }
        )


def signed_request(
    files: MemoryFiles,
    *,
    invocation: str = "invoke-001",
    command: str = "status",
) -> tuple[dict[str, Any], str]:
    payload = {
        "schema": live.LIVE_SCHEMA,
        "invocation_id": invocation,
        "command": command,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "artifact_binding": {
            "workspace_id": live.WORKSPACE_ID,
            "lakehouse_id": live.LAKEHOUSE_ID,
            "environment_id": live.ENVIRONMENT_ID,
            "migration_id": MIGRATION_ID,
        },
        **request(),
    }
    key = bytes(range(32))
    encoded = live._canonical(payload)
    envelope = {
        "payload": payload,
        "payload_sha256": hashlib.sha256(encoded).hexdigest(),
        "signature": hmac.new(key, encoded, hashlib.sha256).hexdigest(),
    }
    files.create_bytes(
        live.request_path(invocation),
        live._canonical(envelope) + b"\n",
    )
    return payload, key.hex()


def test_signed_request_verification_and_tamper_rejection() -> None:
    files = MemoryFiles()
    payload, key = signed_request(files)

    assert live._load_request(files, "invoke-001", key) == payload

    envelope = json.loads(files.values[live.request_path("invoke-001")])
    envelope["payload"]["command"] = "snapshot"
    files.values[live.request_path("invoke-001")] = (
        live._canonical(envelope) + b"\n"
    )
    with pytest.raises(live.LiveShadowError, match="payload hash"):
        live._load_request(files, "invoke-001", key)
    with pytest.raises(live.LiveShadowError, match="32 bytes"):
        live._load_request(files, "invoke-001", "00")


def test_dispatch_has_no_generic_command_surface(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class Control:
        def snapshot(self, value):
            calls.append("snapshot")
            return {}

        def status(self, value):
            calls.append("status")
            return {}

        def authorize(self, value):
            calls.append("authorize")
            return {}

        def bootstrap(self):
            calls.append("bootstrap")
            return {}

        def register(self, value):
            calls.append("register")
            return {}

        def claim(self, value):
            calls.append("claim")
            return {}

        def recover_exact(self, value):
            calls.append("recover-exact")
            return {}

        def route(self, value):
            calls.append("route")
            return {}

    control = Control()
    for command in sorted(live.CONTROL_COMMANDS):
        assert live._dispatch("control", command, {}, control) == {}  # type: ignore[arg-type]
    with pytest.raises(live.LiveShadowError, match="not allowed"):
        live._dispatch("process", "snapshot", {}, control)  # type: ignore[arg-type]
    with pytest.raises(live.LiveShadowError, match="not allowed"):
        live._dispatch("other", "status", {}, control)  # type: ignore[arg-type]
    assert calls == sorted(live.CONTROL_COMMANDS)


def test_dispatch_runs_only_sdk_process_and_fixed_reconcile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import people_counter.fabric_candidate_a_jobs as jobs
    process_calls: list[object] = []
    monkeypatch.setattr(
        jobs,
        "process_main",
        lambda arguments, *, config, route_mode, route_identity: process_calls.append(
            (arguments, config, route_mode, route_identity)
        ) or 0,
    )

    class ReconcileControl:
        def reconcile(self, request):
            assert request["selected_work_id"] == WORK_ID
            return {"work_id": WORK_ID, "reconciled": True}

    process_control = live.SparkShadowControl(FakeSpark(), MemoryFiles())
    assert live._dispatch(
        "process",
        "process",
        {
            "batch_id": "batch-001",
            "selected_work_id": WORK_ID,
            "route_context": {"plan_sha256": "2" * 64},
        },
        process_control,
    ) == {"batch_id": "batch-001", "processed": True}
    assert live._dispatch(
        "reconcile",
        "compare",
        {"selected_work_id": WORK_ID},
        ReconcileControl(),  # type: ignore[arg-type]
    ) == {"work_id": WORK_ID, "reconciled": True}
    assert live._dispatch(
        "reconcile",
        "reconcile",
        {"selected_work_id": WORK_ID},
        ReconcileControl(),  # type: ignore[arg-type]
    ) == {"work_id": WORK_ID, "reconciled": True}
    assert process_calls[0][0] == [
        "--batch-id",
        "batch-001",
        "--mode",
        "sdk",
    ]
    assert process_calls[0][2] == "PRODUCTION_SHADOW"
    assert process_calls[0][3] is None
    with pytest.raises(live.LiveShadowError, match="batch identity"):
        live._dispatch(
            "process",
            "process",
            {"batch_id": "*"},
            object(),  # type: ignore[arg-type]
        )


def test_synthetic_process_route_is_typed_and_rejects_legacy_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import people_counter.fabric_candidate_a_jobs as jobs

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        jobs,
        "process_main",
        lambda arguments, **keywords: calls.append(
            {"arguments": arguments, **keywords}
        )
        or 0,
    )
    context = {
        "authorization_id": HASH_A,
        "comparison_mode": "NO_LEGACY_BASELINE",
        "identity": {
            "work_id": WORK_ID,
            "camera_sha256": HASH_A,
            "location_sha256": HASH_B,
            "model_sha256": HASH_C,
            "source_sha256": HASH_D,
            "config_sha256": HASH_E,
        },
        "plan_sha256": HASH_B,
        "provenance": {},
        "reviewed_at": "2026-10-05T14:00:00+00:00",
        "reviewer": "reviewer",
        "route_mode": "SHADOW_SYNTHETIC",
        "source_evidence_sha256": HASH_F,
    }
    files = MemoryFiles()
    control = live.SparkShadowControl(FakeSpark(), files)
    assert live._dispatch(
        "process",
        "process",
        {
            "batch_id": "batch-001",
            "selected_work_id": WORK_ID,
            "route_context": context,
        },
        control,
    ) == {"batch_id": "batch-001", "processed": True}
    assert calls[0]["route_mode"] == "SHADOW_SYNTHETIC"
    assert calls[0]["route_identity"] == context["identity"]
    binding = json.loads(files.read_bytes(live.route_binding_path(WORK_ID)))
    assert binding["route_context"] == context

    for changed in (
        {**context, "legacy_output_sha256": HASH_A},
        {**context, "comparison_mode": "LEGACY_PARITY"},
        {**context, "route_mode": "PRODUCTION"},
        {
            **context,
            "identity": {**context["identity"], "model_sha256": "not-a-hash"},
        },
        {
            **context,
            "identity": {**context["identity"], "work_id": "other-work"},
        },
        {**context, "identity": None},
        {
            **context,
            "identity": {**context["identity"], "unexpected": HASH_A},
        },
        {**context, "plan_sha256": "a" * 63},
        {**context, "plan_sha256": 7},
    ):
        with pytest.raises(live.LiveShadowError):
            live.validate_synthetic_route_context(changed, WORK_ID)


def test_exact_recovery_binds_evidence_rejects_active_job_and_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = MemoryFiles()
    binding = live._canonical(
        {
            "schema": "people-counter-shadow-route-binding-v1",
            "work_id": WORK_ID,
            "route_context": {"route_mode": "SHADOW_SYNTHETIC"},
        }
    )
    invocation = "failed-process"
    process_request = b'{"signed":"request"}'
    process_failure = live._canonical(
        {
            "schema": live.FAILURE_SCHEMA,
            "command": "process",
            "invocation_id": invocation,
            "exit_code": 1,
            "error_type": "ProcessValidationError",
        }
    )
    files.values[live.route_binding_path(WORK_ID)] = binding
    files.values[live.request_path(invocation)] = process_request
    files.values[live.failure_path(invocation)] = process_failure
    calls: list[dict[str, Any]] = []

    class Store:
        def __init__(self, spark, *, config, auto_bootstrap):
            assert spark is control.spark
            assert config is control.config
            assert auto_bootstrap is False

        def recover_exact(self, **keywords):
            calls.append(keywords)
            return {"outcome": "DEAD", "idempotent": False}

    import people_counter.fabric_candidate_a_control as candidate_control

    monkeypatch.setattr(candidate_control, "FabricControlStoreImpl", Store)
    recovery = {
        "attempt_id": ATTEMPT_ID,
        "batch_id": "batch-001",
        "envelope_sha256": HASH_A,
        "failure_sha256": hashlib.sha256(process_failure).hexdigest(),
        "fence": 1,
        "lease_expires_at": 1.0,
        "membership_sha256": HASH_B,
        "owner": "owner-001",
        "process_invocation_id": invocation,
        "request_sha256": hashlib.sha256(process_request).hexdigest(),
        "route_binding_sha256": hashlib.sha256(binding).hexdigest(),
        "work_id": WORK_ID,
    }
    rest = {
        "reflex": {"id": live.REFLEX_ID, "active": False},
        "pipelines": [
            {
                "id": identifier,
                "display_name": display_name,
                "active_jobs": [],
            }
            for display_name, identifier in live.WRITER_ITEMS
        ],
        "sjds": [
            {
                "job": job,
                "display_name": (
                    f"pc-ca-production-shadow-{job}-v001"
                ),
                "id": f"{job}-sjd-id",
                "active_jobs": [],
            }
            for job in ("control", "process", "reconcile")
        ],
    }
    control = live.SparkShadowControl(FakeSpark(), files)
    assert control.recover_exact(
        {"recovery": recovery, "rest_snapshot": rest}
    ) == {"outcome": "DEAD", "idempotent": False}
    assert calls[0]["work_id"] == WORK_ID
    assert calls[0]["attempt_id"] == ATTEMPT_ID
    assert calls[0] == {
        "work_id": WORK_ID,
        "batch_id": "batch-001",
        "attempt_id": ATTEMPT_ID,
        "owner": "owner-001",
        "fence": 1,
        "lease_expires_at": 1.0,
        "envelope_sha256": HASH_A,
        "membership_sha256": HASH_B,
    }

    for group in ("pipelines", "sjds"):
        active_rest = copy.deepcopy(rest)
        active_rest[group][0]["active_jobs"] = [{"status": "Running"}]
        with pytest.raises(live.LiveShadowError, match="active Fabric jobs"):
            control.recover_exact(
                {
                    "recovery": recovery,
                    "rest_snapshot": active_rest,
                }
            )
    for changed in (
        {**recovery, "route_binding_sha256": HASH_F},
        {**recovery, "request_sha256": HASH_F},
        {**recovery, "failure_sha256": HASH_F},
    ):
        with pytest.raises(live.LiveShadowError):
            control.recover_exact(
                {"recovery": changed, "rest_snapshot": rest}
            )
    for malformed in (
        {},
        {"recovery": {**recovery, "unexpected": True}, "rest_snapshot": rest},
        {"recovery": recovery},
        {
            "recovery": recovery,
            "rest_snapshot": {
                "reflex": {"active": False},
                "pipelines": [],
            },
        },
        {
            "recovery": recovery,
            "rest_snapshot": {
                **rest,
                "pipelines": [{}],
            },
        },
        {
            "recovery": recovery,
            "rest_snapshot": {
                **rest,
                "sjds": [{"active_jobs": None}],
            },
        },
        {
            "recovery": recovery,
            "rest_snapshot": {**rest, "reflex": {"active": True}},
        },
        {"recovery": recovery, "rest_snapshot": {**rest, "reflex": None}},
    ):
        with pytest.raises(live.LiveShadowError):
            control.recover_exact(malformed)

    missing = MemoryFiles()
    with pytest.raises(live.LiveShadowError, match="route binding"):
        live.SparkShadowControl(FakeSpark(), missing).recover_exact(
            {"recovery": recovery, "rest_snapshot": rest}
        )
    missing.values[live.route_binding_path(WORK_ID)] = binding
    missing.values[live.request_path(invocation)] = process_request
    with pytest.raises(live.LiveShadowError, match="process evidence"):
        live.SparkShadowControl(FakeSpark(), missing).recover_exact(
            {"recovery": recovery, "rest_snapshot": rest}
        )
    files.values[live.failure_path(invocation)] = b"[]"
    non_mapping_failure = {
        **recovery,
        "failure_sha256": hashlib.sha256(b"[]").hexdigest(),
    }
    with pytest.raises(live.LiveShadowError, match="reviewed process"):
        control.recover_exact(
            {"recovery": non_mapping_failure, "rest_snapshot": rest}
        )
    files.values[live.failure_path(invocation)] = live._canonical(
        {
            "schema": live.FAILURE_SCHEMA,
            "command": "process",
            "error_type": "RuntimeError",
        }
    )
    changed_failure = {
        **recovery,
        "failure_sha256": hashlib.sha256(
            files.values[live.failure_path(invocation)]
        ).hexdigest(),
    }
    with pytest.raises(live.LiveShadowError, match="reviewed process"):
        control.recover_exact(
            {"recovery": changed_failure, "rest_snapshot": rest}
        )
    files.values[live.result_path(invocation)] = b"{}"
    with pytest.raises(live.LiveShadowError, match="process evidence"):
        control.recover_exact(
            {"recovery": changed_failure, "rest_snapshot": rest}
        )


def test_recovery_snapshot_requires_complete_inactive_inventories() -> None:
    valid = {
        "reflex": {"id": live.REFLEX_ID, "active": False},
        "pipelines": [
            {
                "id": identifier,
                "display_name": display_name,
                "active_jobs": [],
            }
            for display_name, identifier in live.WRITER_ITEMS
        ],
        "sjds": [
            {
                "job": job,
                "display_name": (
                    f"pc-ca-production-shadow-{job}-v001"
                ),
                "id": f"{job}-sjd-id",
                "active_jobs": [],
            }
            for job in ("control", "process", "reconcile")
        ],
    }
    pipelines = live._exact_recovery_inventory(
        valid,
        group="pipelines",
        identity_fields=("id", "display_name"),
        expected={
            (identifier, display_name)
            for display_name, identifier in live.WRITER_ITEMS
        },
    )
    sjds = live._exact_recovery_inventory(
        valid,
        group="sjds",
        identity_fields=("job", "display_name"),
        expected={
            (job, f"pc-ca-production-shadow-{job}-v001")
            for job in ("control", "process", "reconcile")
        },
        require_unique_item_ids=True,
    )
    assert len(pipelines) == len(live.WRITER_ITEMS)
    assert len(sjds) == 3
    assert live._require_no_active_recovery_jobs(
        [*pipelines, *sjds]
    ) is None
    assert live._require_inactive_recovery_snapshot(valid) is None
    invalid = (
        {**valid, "reflex": None},
        {**valid, "reflex": {"id": "wrong", "active": False}},
        {**valid, "reflex": {"active": True}},
        {**valid, "pipelines": None},
        {**valid, "sjds": None},
        {**valid, "pipelines": []},
        {**valid, "sjds": []},
        {**valid, "pipelines": [None]},
        {**valid, "sjds": [{}]},
        {
            **valid,
            "pipelines": [
                {**valid["pipelines"][0], "id": "wrong"},
                *valid["pipelines"][1:],
            ],
        },
        {
            **valid,
            "sjds": [
                {**valid["sjds"][0], "id": valid["sjds"][1]["id"]},
                *valid["sjds"][1:],
            ],
        },
        {
            **valid,
            "sjds": [
                {**valid["sjds"][0], "id": None},
                *valid["sjds"][1:],
            ],
        },
        {
            **valid,
            "sjds": [
                {**valid["sjds"][0], "id": ""},
                *valid["sjds"][1:],
            ],
        },
        {
            **valid,
            "pipelines": [
                {**valid["pipelines"][0], "active_jobs": None},
                *valid["pipelines"][1:],
            ],
        },
        {
            **valid,
            "sjds": [
                {
                    **valid["sjds"][0],
                    "active_jobs": [{"status": "Running"}],
                },
                *valid["sjds"][1:],
            ],
        },
    )
    for snapshot in invalid:
        with pytest.raises(live.LiveShadowError):
            live._require_inactive_recovery_snapshot(snapshot)


def test_reconcile_is_plan_pinned_and_requires_passing_verdict() -> None:
    control = live.SparkShadowControl(FakeSpark(), MemoryFiles())
    summary = {
        "work_id": WORK_ID,
        "plan_sha256": "2" * 64,
        "passed": True,
        "finding_ids": [],
        "finding_types": [],
    }

    result = control.reconcile(
        {"selected_work_id": WORK_ID, "reconciliation": summary}
    )

    assert result["work_id"] == WORK_ID
    assert result["reconciled"] is True
    assert result["reconciliation_sha256"] == live.sha256_json(summary)
    with pytest.raises(live.LiveShadowError, match="exact passing"):
        control.reconcile(
            {
                "selected_work_id": WORK_ID,
                "reconciliation": {**summary, "passed": False},
            }
        )


def test_runtime_provenance_exact_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class System:
        @staticmethod
        def getProperty(name: str) -> str:
            assert name == "java.version"
            return "21.0.8"

    spark = SimpleNamespace(
        version="4.1.1",
        sparkContext=SimpleNamespace(
            _jvm=SimpleNamespace(
                java=SimpleNamespace(
                    lang=SimpleNamespace(System=System)
                )
            )
        ),
    )
    monkeypatch.setattr(live, "_spark", lambda: spark)
    monkeypatch.setattr(
        live.importlib.metadata, "version", lambda name: "0.9.11"
    )
    monkeypatch.setattr(
        live.platform, "python_version", lambda: "3.13.7"
    )

    assert live._runtime() == {
        "package_version": "0.9.11",
        "python_version": "3.13.7",
        "spark_version": "4.1.1",
        "java_version": "21.0.8",
        "fabric_runtime": "2.0",
    }


def test_live_main_writes_zero_result_and_safe_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = MemoryFiles()
    _, key = signed_request(files)
    spark = FakeSpark()
    monkeypatch.setattr(
        live,
        "_runtime",
        lambda: {
            "package_version": "0.9.11",
            "python_version": "3.13",
            "spark_version": "4.1.1",
            "java_version": "21",
            "fabric_runtime": "2.0",
        },
    )
    monkeypatch.setattr(
        live,
        "_dispatch",
        lambda job, command, request, control: {"ok": True},
    )

    assert live.main(
        [
            "status",
            "--invocation-id",
            "invoke-001",
            "--request-hmac-key",
            key,
        ],
        files=files,
        spark=spark,
        output=io.StringIO(),
        errors=io.StringIO(),
    ) == 0
    result = json.loads(files.read_bytes(live.result_path("invoke-001")))
    assert result["exit_code"] == 0
    assert result["result"] == {"ok": True}
    assert live.main(
        [
            "status",
            "--invocation-id",
            "invoke-001",
            "--request-hmac-key",
            key,
        ],
        files=files,
        spark=spark,
        output=io.StringIO(),
        errors=io.StringIO(),
    ) == 2

    failed_files = MemoryFiles()
    _, failed_key = signed_request(
        failed_files, invocation="invoke-002", command="status"
    )
    monkeypatch.setattr(
        live,
        "_dispatch",
        lambda *values: (_ for _ in ()).throw(RuntimeError("secret detail")),
    )
    assert live.main(
        [
            "status",
            "--invocation-id",
            "invoke-002",
            "--request-hmac-key",
            failed_key,
        ],
        files=failed_files,
        spark=spark,
        output=io.StringIO(),
        errors=io.StringIO(),
    ) == 2
    failure = json.loads(
        failed_files.read_bytes(live.failure_path("invoke-002"))
    )
    assert failure["exit_code"] == 1
    assert failure["error_type"] == "RuntimeError"
    assert "secret detail" not in json.dumps(failure)

    from people_counter.sjd_process import ProcessValidationError

    safe = live._safe_failure_reason(
        ProcessValidationError(
            "OneLake attempts require the route-mode fixed Candidate A root "
            "'do-not-persist-this-path'"
        )
    )
    assert safe == {
        "reason_code": "ATTEMPT_ROOT_MODE_MISMATCH",
        "reason_path": "attempt_store.root",
    }
    assert "do-not-persist" not in json.dumps(safe)
    cases = (
        (
            "process route mode differs",
            "PROCESS_ROUTE_MODE_INVALID",
            "route_context.route_mode",
        ),
        (
            "synthetic route binding differs",
            "SYNTHETIC_ROUTE_INVALID",
            "route_context",
        ),
        (
            "synthetic model identity differs",
            "MODEL_IDENTITY_MISMATCH",
            "route_context.identity.model_sha256",
        ),
        (
            "synthetic video/config identity differs",
            "INPUT_IDENTITY_MISMATCH",
            "claim.identity",
        ),
        (
            "lease admission rejected for secret work",
            "LEASE_ADMISSION_REJECTED",
            "claim.lease_expires_at",
        ),
        (
            "payload SHA-256 differs",
            "PAYLOAD_IDENTITY_MISMATCH",
            "claim.payload_sha256",
        ),
        (
            "staged terminal record differs",
            "STAGED_RECORD_INVALID",
            "attempt.records",
        ),
        (
            "secret unclassified detail",
            "UNCLASSIFIED_VALIDATION_FAILURE",
            "process",
        ),
    )
    for message, code, path in cases:
        assert live._safe_failure_reason(ProcessValidationError(message)) == {
            "reason_code": code,
            "reason_path": path,
        }


def test_live_main_records_redacted_spark_canonicalization_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = MemoryFiles()
    _, key = signed_request(
        files, invocation="invoke-canonical", command="status"
    )
    error = live.SparkCanonicalizationError(
        path='$[0]["capture_date"]',
        value=object(),
        stage="spark-row-normalization:people_counter_video_work",
        reason="unsupported Spark-returned type",
    )
    monkeypatch.setattr(live, "_runtime", lambda: {})
    monkeypatch.setattr(
        live,
        "_dispatch",
        lambda *values: (_ for _ in ()).throw(error),
    )

    assert live.main(
        [
            "status",
            "--invocation-id",
            "invoke-canonical",
            "--request-hmac-key",
            key,
        ],
        files=files,
        spark=FakeSpark(),
        output=io.StringIO(),
        errors=io.StringIO(),
    ) == 2

    failure_bytes = files.read_bytes(
        live.failure_path("invoke-canonical")
    )
    failure = json.loads(failure_bytes)
    assert failure["canonicalization"] == error.safe_metadata()
    assert b"do-not-leak" not in failure_bytes
