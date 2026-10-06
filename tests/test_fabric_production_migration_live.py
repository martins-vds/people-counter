from __future__ import annotations

import base64
import hashlib
import hmac
import io
import json
import sys
import types
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

import people_counter.fabric_production_migration_live as live
from people_counter.fabric_production_migration import (
    ENVIRONMENT_ID,
    LAKEHOUSE_ID,
    WORKSPACE_ID,
    FakeMigrationBackend,
    LeaseSnapshot,
    PreconditionError,
    WriterQuiescenceProof,
    build_plan,
    canonical_json,
)
from people_counter.fabric_production_migration_sjd import (
    build_migration_sjd_definition,
    decoded_metadata,
    export_migration_sjd,
    migration_main_source,
    migration_sjd_bytes,
)
from people_counter.fabric_reflex_definition import (
    REFLEX_RULE_NAME,
    REFLEX_TARGET_PIPELINE_ID,
    REFLEX_TARGET_WORKSPACE_ID,
)


KEY = b"test-inventory-signing-key"
NOW = 10_000.0
TOKEN = "single-use-token"


def reflex_definition(should_run: bool = False) -> bytes:
    return canonical_json(
        {
            "runSettings": {"isStopped": False},
            "rules": [
                {
                    "name": REFLEX_RULE_NAME,
                    "rule_settings": {
                        "shouldApplyRuleOnUpdate": True,
                        "shouldRun": should_run,
                    },
                    "action": {
                        "itemId": REFLEX_TARGET_PIPELINE_ID,
                        "itemType": "Pipeline",
                        "workspaceId": REFLEX_TARGET_WORKSPACE_ID,
                    },
                }
            ],
        }
    ).encode()


def inventory_bytes(
    *,
    should_run: bool = False,
    captured_at: float = NOW,
    expires_at: float = NOW + 200,
    jobs: list[dict[str, object]] | None = None,
    schedules: list[dict[str, object]] | None = None,
    extra_payload: dict[str, object] | None = None,
) -> bytes:
    payload = {
        "artifact_binding": {
            "environment_id": ENVIRONMENT_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "workspace_id": WORKSPACE_ID,
        },
        "captured_at": captured_at,
        "expires_at": expires_at,
        "fabric_jobs": jobs or [],
        "reflex_id": "c39f8c1d-e363-402d-b7f0-34f53ce31bcc",
        "reflex_definition_base64": base64.b64encode(
            reflex_definition(should_run)
        ).decode(),
        "schema": live.INVENTORY_SCHEMA,
        "writer_schedules": schedules or [],
    }
    payload.update(extra_payload or {})
    encoded = canonical_json(payload).encode()
    return canonical_json(
        {
            "payload": payload,
            "payload_sha256": hashlib.sha256(encoded).hexdigest(),
            "signature_sha256": hmac.new(KEY, encoded, hashlib.sha256).hexdigest(),
        }
    ).encode()


class MemoryFiles:
    def __init__(self, values: dict[str, bytes] | None = None) -> None:
        self.values = dict(values or {})
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


def test_inventory_hash_signature_freshness_and_reflex_state() -> None:
    inventory = live.InvocationInventory.from_bytes(
        inventory_bytes(), hmac_key=KEY, now=NOW
    )
    assert inventory.passed
    assert not inventory.reflex.enabled
    assert inventory.stopped_writer_ids == (
        "reflex:c39f8c1d-e363-402d-b7f0-34f53ce31bcc",
    )

    corrupt = json.loads(inventory_bytes())
    corrupt["payload"]["expires_at"] += 1
    with pytest.raises(live.LiveMigrationError, match="hash mismatch"):
        live.InvocationInventory.from_bytes(
            canonical_json(corrupt).encode(), hmac_key=KEY, now=NOW
        )
    with pytest.raises(live.LiveMigrationError, match="stale"):
        live.InvocationInventory.from_bytes(
            inventory_bytes(
                captured_at=NOW - live.INVENTORY_MAX_AGE_SECONDS - 1,
                expires_at=NOW + 1,
            ),
            hmac_key=KEY,
            now=NOW,
        )


@pytest.mark.parametrize(
    ("captured_at", "expires_at"),
    [
        (NOW, NOW),
        (NOW - live.INVENTORY_MAX_AGE_SECONDS, NOW + 1),
    ],
)
def test_inventory_freshness_accepts_exact_boundaries(
    captured_at: float, expires_at: float
) -> None:
    observed = live.InvocationInventory.from_bytes(
        inventory_bytes(captured_at=captured_at, expires_at=expires_at),
        hmac_key=KEY,
        now=NOW,
    )
    assert observed.captured_at == captured_at
    assert observed.expires_at == expires_at


@pytest.mark.parametrize(
    ("captured_at", "expires_at"),
    [
        (NOW + 0.001, NOW + 10),
        (NOW - 1, NOW - 0.001),
        (NOW - live.INVENTORY_MAX_AGE_SECONDS - 0.001, NOW + 1),
    ],
)
def test_inventory_freshness_rejects_outside_boundaries(
    captured_at: float, expires_at: float
) -> None:
    with pytest.raises(live.LiveMigrationError, match="stale, expired, or future-dated"):
        live.InvocationInventory.from_bytes(
            inventory_bytes(captured_at=captured_at, expires_at=expires_at),
            hmac_key=KEY,
            now=NOW,
        )


def test_inventory_rejects_active_run_writer_and_reflex() -> None:
    inventory = live.InvocationInventory.from_bytes(
        inventory_bytes(
            should_run=True,
            jobs=[{"run_id": "run-1", "state": "Running"}],
            schedules=[
                {
                    "definition_sha256": "a" * 64,
                    "enabled": True,
                    "schedule_id": "dispatcher",
                }
            ],
        ),
        hmac_key=KEY,
        now=NOW,
    )
    assert not inventory.passed
    assert inventory.active_run_ids == ("run-1",)
    assert inventory.active_writer_ids == (
        "dispatcher",
        "reflex:c39f8c1d-e363-402d-b7f0-34f53ce31bcc",
    )


def test_inventory_rejects_bad_signature_binding_and_schedule_evidence() -> None:
    wrong_key = b"wrong"
    with pytest.raises(live.LiveMigrationError, match="signature mismatch"):
        live.InvocationInventory.from_bytes(
            inventory_bytes(), hmac_key=wrong_key, now=NOW
        )

    bad_schedule = inventory_bytes(
        schedules=[
            {
                "definition_sha256": "bad",
                "enabled": False,
                "schedule_id": "dispatcher",
            }
        ]
    )
    with pytest.raises(live.LiveMigrationError, match="definition hash"):
        live.InvocationInventory.from_bytes(bad_schedule, hmac_key=KEY, now=NOW)

    duplicate_jobs = inventory_bytes(
        jobs=[
            {"run_id": "same", "state": "Completed"},
            {"run_id": "same", "state": "Failed"},
        ]
    )
    with pytest.raises(live.LiveMigrationError, match="unique"):
        live.InvocationInventory.from_bytes(duplicate_jobs, hmac_key=KEY, now=NOW)

    missing_job_id = inventory_bytes(jobs=[{"state": "Completed"}])
    with pytest.raises(live.LiveMigrationError, match="nonempty and unique"):
        live.InvocationInventory.from_bytes(missing_job_id, hmac_key=KEY, now=NOW)

    for invalid_state in (None, "", "mystery"):
        with pytest.raises(live.LiveMigrationError, match="recognized and explicit"):
            live.InvocationInventory.from_bytes(
                inventory_bytes(
                    jobs=[{"run_id": "job", "state": invalid_state}]
                ),
                hmac_key=KEY,
                now=NOW,
            )

    missing_schedule_id = inventory_bytes(
        schedules=[{"definition_sha256": "a" * 64, "enabled": False}]
    )
    with pytest.raises(live.LiveMigrationError, match="nonempty and unique"):
        live.InvocationInventory.from_bytes(
            missing_schedule_id, hmac_key=KEY, now=NOW
        )

    nonboolean_schedule = inventory_bytes(
        schedules=[
            {
                "definition_sha256": "a" * 64,
                "enabled": 0,
                "schedule_id": "dispatcher",
            }
        ]
    )
    with pytest.raises(live.LiveMigrationError, match="must be boolean"):
        live.InvocationInventory.from_bytes(
            nonboolean_schedule, hmac_key=KEY, now=NOW
        )


def test_inventory_requires_exact_object_envelope() -> None:
    with pytest.raises(live.LiveMigrationError, match="envelope fields"):
        live.InvocationInventory.from_bytes(
            b"[]", hmac_key=KEY, now=NOW
        )


def test_inventory_preserves_disabled_schedule_identity() -> None:
    inventory = live.InvocationInventory.from_bytes(
        inventory_bytes(
            schedules=[
                {
                    "definition_sha256": "a" * 64,
                    "enabled": False,
                    "schedule_id": "dispatcher",
                }
            ]
        ),
        hmac_key=KEY,
        now=NOW,
    )
    assert inventory.active_writer_ids == ()
    assert inventory.stopped_writer_ids == (
        "dispatcher",
        "reflex:c39f8c1d-e363-402d-b7f0-34f53ce31bcc",
    )


def test_json_evidence_normalization_is_total_and_deterministic() -> None:
    value = {
        "aware": datetime.fromtimestamp(NOW, timezone.utc),
        "binary": b"\x00\xff",
        "list": [None, True, 1, 2.5],
        "mapping": {1: "one"},
        "naive": datetime.fromtimestamp(NOW),
        "other": SimpleNamespace(value=1),
    }
    normalized = live._json_value(value)
    assert normalized["binary"] == "AP8="
    assert normalized["mapping"] == {"1": "one"}
    assert normalized["list"] == [None, True, 1, 2.5]
    assert normalized["aware"].endswith("+00:00")
    assert normalized["naive"].endswith("+00:00")
    assert normalized["other"].startswith("namespace")


class Row(dict):
    def asDict(self, recursive: bool = True) -> dict[str, Any]:
        return dict(self)


class DataType:
    def __init__(self, name: str) -> None:
        self.name = name

    def simpleString(self) -> str:
        return self.name


class Frame:
    def __init__(
        self,
        rows: list[dict[str, Any]],
        fields: list[tuple[str, str, bool]] | None = None,
    ) -> None:
        self._rows = [Row(row) for row in rows]
        names = sorted({key for row in rows for key in row})
        self.columns = names
        self.schema = SimpleNamespace(
            fields=[
                SimpleNamespace(name=name, dataType=DataType(kind), nullable=nullable)
                for name, kind, nullable in (
                    fields or [(name, "string", True) for name in names]
                )
            ]
        )

    def collect(self) -> list[Row]:
        return list(self._rows)

    def select(self, *columns: str) -> Frame:
        return Frame([{name: row.get(name) for name in columns} for row in self._rows])

    def orderBy(self, *columns: str) -> Frame:
        self._rows.sort(key=lambda row: tuple(str(row.get(name)) for name in columns))
        return self

    def toLocalIterator(self):
        return iter(self._rows)


class Spark:
    def __init__(self, tables: dict[str, Frame]) -> None:
        self.tables = tables
        self.catalog = SimpleNamespace(tableExists=lambda name: name in self.tables)

    def table(self, name: str) -> Frame:
        return self.tables[name]

    def sql(self, statement: str) -> Frame:
        name = statement.split("`")[1]
        return Frame([{"version": 7}])


def test_spark_evidence_callbacks_cover_owner_leases_pointer_and_hashes() -> None:
    spark = Spark(
        {
            live.LOCK_TABLE: Frame(
                [{"lock_name": "global", "owner_id": None, "acquired_at": None}]
            ),
            live.DISPATCHER_LEASE_TABLE: Frame(
                [
                    {
                        "lock_name": "global",
                        "owner_id": "expired",
                        "expires_at": NOW - 1,
                    }
                ]
            ),
            live.REGISTRATION_LEASE_TABLE: Frame(
                [
                    {
                        "lock_name": "global",
                        "owner_id": "",
                        "expires_at": NOW + 50,
                    }
                ]
            ),
            live.WORK_TABLE: Frame(
                [
                    {
                        "work_id": "w1",
                        "status": "SUCCEEDED",
                        "committed_attempt_id": "a1",
                        "lease_expires_at": None,
                    }
                ]
            ),
        }
    )
    reader = live.SparkEvidenceReader(spark, clock=lambda: NOW)
    assert reader.control_owner() is None
    assert reader.active_leases() == ()
    assert len(reader.committed_pointer_sha256()) == 64

    spark.tables[live.DISPATCHER_LEASE_TABLE] = Frame(
        [
            {
                "lock_name": "global",
                "owner_id": "live",
                "expires_at": datetime.fromtimestamp(NOW + 1, timezone.utc),
            }
        ]
    )
    assert reader.active_leases() == (
        "people_counter_dispatcher_leases:global:live",
    )
    spark.tables[live.DISPATCHER_LEASE_TABLE] = Frame([])
    spark.tables[live.WORK_TABLE] = Frame(
        [
            {
                "work_id": "w2",
                "status": "RUNNING",
                "committed_attempt_id": None,
                "lease_expires_at": datetime.fromtimestamp(
                    NOW + 10, timezone.utc
                ),
            }
        ]
    )
    assert reader.active_leases() == (
        "people_counter_video_work:w2:RUNNING",
    )


class StubReader:
    def __init__(self, owner: str | None = None) -> None:
        self.owner = owner

    def control_writer_row(self) -> dict[str, object]:
        return {"lock_name": "global", "owner_id": self.owner}


def test_migration_lock_exact_cas_and_ambiguous_write_fail_closed() -> None:
    reader = StubReader()

    def cas(expected: str | None, replacement: str | None) -> None:
        assert reader.owner == expected
        reader.owner = replacement

    lock = live.ControlWriterMigrationLock(reader, cas)  # type: ignore[arg-type]
    lock.acquire("migration-owner")
    assert reader.owner == "migration-owner"
    lock.release("migration-owner")
    assert reader.owner is None

    reader.owner = None

    def ambiguous(expected: str | None, replacement: str | None) -> None:
        raise TimeoutError("unknown outcome")

    with pytest.raises(live.LiveMigrationError, match="ambiguous migration lock"):
        live.ControlWriterMigrationLock(reader, ambiguous).acquire("owner")  # type: ignore[arg-type]

    reader.owner = "other"
    with pytest.raises(PreconditionError, match="owned"):
        lock.acquire("migration-owner")
    with pytest.raises(PreconditionError, match="changed before release"):
        lock.release("migration-owner")

    reader.owner = None
    with pytest.raises(PreconditionError, match="CAS lost"):
        live.ControlWriterMigrationLock(reader, lambda expected, replacement: None).acquire(  # type: ignore[arg-type]
            "migration-owner"
        )


def test_migration_lock_resolves_lost_ack_and_retains_ambiguous_release() -> None:
    reader = StubReader()

    def acquired_then_timeout(expected: str | None, replacement: str | None) -> None:
        reader.owner = replacement
        raise TimeoutError("lost acknowledgement")

    lock = live.ControlWriterMigrationLock(reader, acquired_then_timeout)  # type: ignore[arg-type]
    lock.acquire("owner")
    assert reader.owner == "owner"

    def release_timeout(expected: str | None, replacement: str | None) -> None:
        raise TimeoutError("unknown release")

    with pytest.raises(live.LiveMigrationError, match="ambiguous migration lock release"):
        live.ControlWriterMigrationLock(reader, release_timeout).release("owner")  # type: ignore[arg-type]
    assert reader.owner == "owner"

    def released_then_timeout(expected: str | None, replacement: str | None) -> None:
        reader.owner = replacement
        raise TimeoutError("lost release acknowledgement")

    live.ControlWriterMigrationLock(reader, released_then_timeout).release("owner")  # type: ignore[arg-type]
    assert reader.owner is None


def recovery_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[live.InvocationInventory, MemoryFiles, Any, dict[str, object]]:
    diagnostic = {
        "exception": {
            "message": "independent writer-quiescence proof is stale or future-dated",
            "traceback": (
                "in _apply_command\nin apply_plan\n"
                "in _check_apply_preconditions\nin _check_runtime_safety\n"
            ),
            "type": "PreconditionError",
        },
        "invocation_id": live.FAILED_RECOVERY_INVOCATION_ID,
        "redacted_arguments": [
            "apply",
            "--run-id",
            live.FAILED_RECOVERY_RUN_ID,
            "--invocation-id",
            live.FAILED_RECOVERY_INVOCATION_ID,
            "--owner",
            "martins-vds",
            "--plan-sha256",
            live.FAILED_RECOVERY_PLAN_SHA256,
        ],
        "run_id": live.FAILED_RECOVERY_RUN_ID,
        "stage": "spark-binding",
    }
    diagnostic_bytes = canonical_json(diagnostic).encode()
    monkeypatch.setattr(
        live,
        "FAILED_RECOVERY_DIAGNOSTIC_SHA256",
        hashlib.sha256(diagnostic_bytes).hexdigest(),
    )
    job = {
        "end_time_utc": "1970-01-01T02:45:00+00:00",
        "failure_reason_sha256": "a" * 64,
        "invocation_id": live.FAILED_RECOVERY_INVOCATION_ID,
        "item_id": "463803d9-ebe1-4162-8a7c-881081da9ee5",
        "job_id": live.FAILED_RECOVERY_JOB_ID,
        "job_state_sha256": "b" * 64,
        "job_type": "sparkjob",
        "start_time_utc": "1970-01-01T02:40:00+00:00",
        "status": "Failed",
    }
    inventory = live.InvocationInventory.from_bytes(
        inventory_bytes(
            captured_at=NOW,
            expires_at=NOW + 200,
            extra_payload={"lock_recovery": job},
        ),
        hmac_key=KEY,
        now=NOW,
    )
    files = MemoryFiles(
        {
            live.failure_path(
                live.FAILED_RECOVERY_RUN_ID,
                live.FAILED_RECOVERY_INVOCATION_ID,
            ): diagnostic_bytes,
            live.plan_path(live.FAILED_RECOVERY_RUN_ID): canonical_json(
                {"plan_sha256": live.FAILED_RECOVERY_PLAN_SHA256}
            ).encode(),
        }
    )
    row: dict[str, object] = {
        "acquired_at": datetime.fromtimestamp(9_800, timezone.utc),
        "lock_name": "global",
        "owner_id": (
            "migration:martins-vds:plan-20261004-03:"
            + "c" * 24
            + ":"
            + "d" * 32
        ),
    }

    class RecoveryReader:
        def control_writer_row(self):
            return dict(row)

        @staticmethod
        def active_leases():
            return ()

    return inventory, files, RecoveryReader(), row


def test_recovery_exact_binding_review_and_cas_clear(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inventory, files, reader, row = recovery_fixture(monkeypatch)
    args = SimpleNamespace(
        failed_invocation_id=live.FAILED_RECOVERY_INVOCATION_ID,
        failed_job_id=live.FAILED_RECOVERY_JOB_ID,
        recovery_execute=False,
        recovery_token=None,
        run_id=live.FAILED_RECOVERY_RUN_ID,
    )
    output = io.StringIO()
    assert live._recover_lock_command(
        args,
        spark_session=Spark({}),
        files=files,
        inventory=inventory,
        reader=reader,
        clock=lambda: NOW,
        output=output,
        table_path_exists=lambda name: False,
    ) == 0
    plan = json.loads(output.getvalue())["plan"]
    assert plan["binding"] == {
        "control_row_sha256": plan["evidence"]["control_row_sha256"],
        "evidence_sha256": plan["evidence_sha256"],
        "failed_invocation_id": live.FAILED_RECOVERY_INVOCATION_ID,
        "failed_job_id": live.FAILED_RECOVERY_JOB_ID,
        "migration_id": "people_counter_ca_0001",
        "physical_owner": row["owner_id"],
    }
    assert plan["evidence_sha256"] == live.evidence_hash(plan["evidence"])
    assert plan["recovery_token"] == (
        "recover-v1:" + live.evidence_hash(plan["binding"])
    )
    assert plan["schema"] == live.RECOVERY_SCHEMA
    assert plan["plan_sha256"] == live.evidence_hash(
        {key: value for key, value in plan.items() if key != "plan_sha256"}
    )
    review_body = {
        "expires_at": NOW + 100,
        "failed_invocation_id": live.FAILED_RECOVERY_INVOCATION_ID,
        "failed_job_id": live.FAILED_RECOVERY_JOB_ID,
        "plan_sha256": plan["plan_sha256"],
        "recovery_token_sha256": hashlib.sha256(
            plan["recovery_token"].encode()
        ).hexdigest(),
        "reviewed_at": NOW,
        "schema": live.RECOVERY_SCHEMA,
    }
    receipt = {
        **review_body,
        "receipt_sha256": live.evidence_hash(review_body),
    }
    files.values[
        live.recovery_review_path(
            live.FAILED_RECOVERY_JOB_ID,
            live.FAILED_RECOVERY_INVOCATION_ID,
        )
    ] = canonical_json(receipt).encode()
    args.recovery_execute = True
    args.recovery_token = plan["recovery_token"]
    calls: list[tuple[str, object]] = []

    def clear(owner: str, acquired_at: object) -> None:
        calls.append((owner, acquired_at))
        row["owner_id"] = None
        row["acquired_at"] = None

    output = io.StringIO()
    assert live._recover_lock_command(
        args,
        spark_session=Spark({}),
        files=files,
        inventory=inventory,
        reader=reader,
        clock=lambda: NOW,
        output=output,
        table_path_exists=lambda name: False,
        clear_cas=clear,
    ) == 0
    result = json.loads(output.getvalue())
    assert result["status"] == "cleared"
    assert result["after"]["owner_id"] is None
    assert calls == [
        (
            plan["evidence"]["physical_owner"],
            datetime.fromtimestamp(9_800, timezone.utc),
        )
    ]


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("failed_job_id", "other", "only the reviewed failure"),
        ("failed_invocation_id", "other", "only the reviewed failure"),
        ("run_id", "other", "only the reviewed failure"),
        ("recovery_token", "wrong", "does not bind"),
    ],
)
def test_recovery_rejects_identity_and_token_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: str,
    match: str,
) -> None:
    inventory, files, reader, _ = recovery_fixture(monkeypatch)
    args = SimpleNamespace(
        failed_invocation_id=live.FAILED_RECOVERY_INVOCATION_ID,
        failed_job_id=live.FAILED_RECOVERY_JOB_ID,
        recovery_execute=field == "recovery_token",
        recovery_token=None,
        run_id=live.FAILED_RECOVERY_RUN_ID,
    )
    setattr(args, field, value)
    with pytest.raises(live.LiveMigrationError, match=match):
        live._recover_lock_command(
            args,
            spark_session=Spark({}),
            files=files,
            inventory=inventory,
            reader=reader,
            clock=lambda: NOW,
            output=io.StringIO(),
            table_path_exists=lambda name: False,
        )


def test_recovery_rejects_table_result_receipt_and_cas_ambiguity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inventory, files, reader, row = recovery_fixture(monkeypatch)
    args = SimpleNamespace(
        failed_invocation_id=live.FAILED_RECOVERY_INVOCATION_ID,
        failed_job_id=live.FAILED_RECOVERY_JOB_ID,
        recovery_execute=False,
        recovery_token=None,
        run_id=live.FAILED_RECOVERY_RUN_ID,
    )
    with pytest.raises(live.LiveMigrationError, match="table or OneLake path"):
        live._recover_lock_command(
            args,
            spark_session=Spark({}),
            files=files,
            inventory=inventory,
            reader=reader,
            clock=lambda: NOW,
            output=io.StringIO(),
            table_path_exists=lambda name: name == live.JOURNAL_TABLE,
        )
    files.values[
        live.result_path(
            live.FAILED_RECOVERY_RUN_ID,
            live.FAILED_RECOVERY_PLAN_SHA256,
        )
    ] = b'{"receipts":[{}]}'
    with pytest.raises(live.LiveMigrationError, match="receipts are present"):
        live._recover_lock_command(
            args,
            spark_session=Spark({}),
            files=files,
            inventory=inventory,
            reader=reader,
            clock=lambda: NOW,
            output=io.StringIO(),
            table_path_exists=lambda name: False,
        )
    del files.values[
        live.result_path(
            live.FAILED_RECOVERY_RUN_ID,
            live.FAILED_RECOVERY_PLAN_SHA256,
        )
    ]
    live._recover_lock_command(
        args,
        spark_session=Spark({}),
        files=files,
        inventory=inventory,
        reader=reader,
        clock=lambda: NOW,
        output=io.StringIO(),
        table_path_exists=lambda name: False,
    )
    plan = json.loads(
        files.values[
            live.recovery_plan_path(
                live.FAILED_RECOVERY_JOB_ID,
                live.FAILED_RECOVERY_INVOCATION_ID,
            )
        ]
    )
    args.recovery_execute = True
    args.recovery_token = plan["recovery_token"]
    with pytest.raises(live.LiveMigrationError, match="review receipt"):
        live._recover_lock_command(
            args,
            spark_session=Spark({}),
            files=files,
            inventory=inventory,
            reader=reader,
            clock=lambda: NOW,
            output=io.StringIO(),
            table_path_exists=lambda name: False,
        )
    review_body = {
        "expires_at": NOW + 1,
        "failed_invocation_id": live.FAILED_RECOVERY_INVOCATION_ID,
        "failed_job_id": live.FAILED_RECOVERY_JOB_ID,
        "plan_sha256": plan["plan_sha256"],
        "recovery_token_sha256": hashlib.sha256(
            plan["recovery_token"].encode()
        ).hexdigest(),
        "reviewed_at": NOW,
        "schema": live.RECOVERY_SCHEMA,
    }
    files.values[
        live.recovery_review_path(
            live.FAILED_RECOVERY_JOB_ID,
            live.FAILED_RECOVERY_INVOCATION_ID,
        )
    ] = canonical_json(
        {**review_body, "receipt_sha256": live.evidence_hash(review_body)}
    ).encode()
    with pytest.raises(live.LiveMigrationError, match="ambiguous exact readback"):
        live._recover_lock_command(
            args,
            spark_session=Spark({}),
            files=files,
            inventory=inventory,
            reader=reader,
            clock=lambda: NOW,
            output=io.StringIO(),
            table_path_exists=lambda name: False,
            clear_cas=lambda owner, acquired: None,
        )
    assert row["owner_id"] is not None
    assert not files.exists(
        live.recovery_result_path(
            live.FAILED_RECOVERY_JOB_ID,
            live.FAILED_RECOVERY_INVOCATION_ID,
        )
    )


def test_construct_live_backend_wires_every_callback() -> None:
    inventory = live.InvocationInventory.from_bytes(
        inventory_bytes(), hmac_key=KEY, now=NOW
    )

    class Reader:
        def control_writer_row(self):
            return {
                "acquired_at": None,
                "lock_name": "global",
                "owner_id": None,
            }

        def active_leases(self):
            return ()

        def control_owner(self, *, migration_owner_id=None):
            assert migration_owner_id == "lock-owner:nonce"
            return None

        def legacy_objects(self):
            return ()

        def committed_pointer_sha256(self):
            return "b" * 64

        def committed_view_fingerprints(self):
            return (("view", "c" * 64),)

        def gold_sha256(self):
            return "d" * 64

    spark = Spark({})
    backend = live.construct_live_backend(
        spark,
        inventory,
        clock=lambda: NOW,
        owner_id="lock-owner",
        physical_owner_id="lock-owner:nonce",
        lease_token=TOKEN,
        reader=Reader(),  # type: ignore[arg-type]
        artifact_binding=lambda: {
            "environment_id": ENVIRONMENT_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "workspace_id": WORKSPACE_ID,
        },
    )
    snapshot = backend.discover()
    assert snapshot.active_run_ids == ()
    assert snapshot.active_lease_ids == ()
    assert snapshot.control_owner is None
    assert snapshot.lease == LeaseSnapshot(
        owner="lock-owner",
        token_sha256=hashlib.sha256(TOKEN.encode()).hexdigest(),
        expires_at=NOW + 200,
    )
    assert snapshot.committed_pointer_sha256 == "b" * 64
    assert snapshot.committed_view_fingerprints == (("view", "c" * 64),)
    assert snapshot.gold_sha256 == "d" * 64
    assert snapshot.writer_quiescence.passed


def test_runtime_artifact_binding_reads_actual_spark_context() -> None:
    values = {
        "spark.microsoft.fabric.workspace.id": WORKSPACE_ID,
        "spark.microsoft.fabric.lakehouse.id": LAKEHOUSE_ID,
        "spark.microsoft.fabric.environment.id": ENVIRONMENT_ID,
    }

    class Conf:
        def get(self, key: str) -> str:
            if key not in values:
                raise KeyError(key)
            return values[key]

    assert live.observed_artifact_binding(
        SimpleNamespace(conf=Conf())
    ) == {
        "environment_id": ENVIRONMENT_ID,
        "lakehouse_id": LAKEHOUSE_ID,
        "workspace_id": WORKSPACE_ID,
    }
    values["spark.microsoft.fabric.lakehouse.id"] = "wrong"
    assert live.observed_artifact_binding(SimpleNamespace(conf=Conf()))[
        "lakehouse_id"
    ] == "wrong"
    del values["spark.microsoft.fabric.environment.id"]
    with pytest.raises(live.LiveMigrationError, match="missing or ambiguous"):
        live.observed_artifact_binding(SimpleNamespace(conf=Conf()))


def test_runtime_artifact_binding_reads_spark_context_conf_fallback() -> None:
    values = {
        "trident.workspace.id": WORKSPACE_ID,
        "trident.lakehouse.id": LAKEHOUSE_ID,
        "trident.environment.id": ENVIRONMENT_ID,
    }

    class MissingSessionConf:
        @staticmethod
        def get(key):
            raise KeyError(key)

    class ContextConf:
        @staticmethod
        def get(key):
            if key not in values:
                raise KeyError(key)
            return values[key]

    spark = SimpleNamespace(
        conf=MissingSessionConf(),
        sparkContext=SimpleNamespace(
            getConf=lambda: ContextConf(),
        ),
    )
    assert live.observed_artifact_binding(spark) == {
        "environment_id": ENVIRONMENT_ID,
        "lakehouse_id": LAKEHOUSE_ID,
        "workspace_id": WORKSPACE_ID,
    }


def test_runtime_artifact_binding_reads_notebookutils_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = types.ModuleType("notebookutils")
    module.runtime = SimpleNamespace(  # type: ignore[attr-defined]
        context={
            "currentWorkspaceId": WORKSPACE_ID,
            "defaultLakehouseId": LAKEHOUSE_ID,
            "environmentId": ENVIRONMENT_ID,
        }
    )
    monkeypatch.setitem(sys.modules, "notebookutils", module)

    class Missing:
        @staticmethod
        def get(key):
            raise KeyError(key)

    spark = SimpleNamespace(
        conf=Missing(),
        sparkContext=SimpleNamespace(getConf=lambda: Missing()),
    )
    assert live.observed_artifact_binding(spark) == {
        "environment_id": ENVIRONMENT_ID,
        "lakehouse_id": LAKEHOUSE_ID,
        "workspace_id": WORKSPACE_ID,
    }


def test_live_backend_exposes_active_lease_and_owner_rejections() -> None:
    inventory = live.InvocationInventory.from_bytes(
        inventory_bytes(
            jobs=[{"run_id": "active-run", "state": "running"}],
        ),
        hmac_key=KEY,
        now=NOW,
    )

    class Reader:
        def control_writer_row(self):
            return {
                "acquired_at": None,
                "lock_name": "global",
                "owner_id": None,
            }

        def active_leases(self):
            return ("registration:owner",)

        def control_owner(self, *, migration_owner_id=None):
            return "other-owner"

        def legacy_objects(self):
            return ()

        def committed_pointer_sha256(self):
            return None

        def committed_view_fingerprints(self):
            return ()

        def gold_sha256(self):
            return None

    backend = live.construct_live_backend(
        Spark({}),
        inventory,
        clock=lambda: NOW,
        owner_id="migration-owner",
        physical_owner_id="migration-owner:nonce",
        lease_token=TOKEN,
        reader=Reader(),  # type: ignore[arg-type]
        artifact_binding=lambda: {
            "environment_id": ENVIRONMENT_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "workspace_id": WORKSPACE_ID,
        },
    )
    blockers = backend.discover()
    assert blockers.active_run_ids == ("active-run",)
    assert blockers.active_lease_ids == ("registration:owner",)
    assert blockers.control_owner == "other-owner"
    assert not blockers.writer_quiescence.passed


def test_live_cli_empty_defaults_are_nonrunnable_and_do_not_write() -> None:
    files = MemoryFiles()
    output = io.StringIO()
    errors = io.StringIO()
    with pytest.raises(live.LiveMigrationError, match="requires --run-id"):
        live.main(
            [],
            spark_session=object(),
            files=files,
            hmac_key=KEY,
            clock=lambda: NOW,
            output=output,
            errors=errors,
        )
    assert output.getvalue() == ""
    assert "requires --run-id" in errors.getvalue()
    assert files.creates == [
        live.stage_marker_path(
            "unbound-run", "unbound-invocation", "bootstrap"
        ),
        live.stage_marker_path("unbound-run", "unbound-invocation", "args"),
        live.stage_marker_path(
            "unbound-run", "unbound-invocation", "inventory-read"
        ),
        live.failure_path("unbound-run", "unbound-invocation"),
    ]


def test_live_cli_command_language_is_exact() -> None:
    parser = live._build_parser()
    command = next(action for action in parser._actions if action.dest == "command")
    assert tuple(command.choices) == (
        "inventory",
        "diagnose",
        "plan",
        "apply",
        "verify",
        "status",
        "recover-lock",
    )
    with pytest.raises(SystemExit):
        parser.parse_args(["arbitrary"])


def test_live_plan_apply_verify_status_and_noop_with_fixed_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "run-001"
    files = MemoryFiles({live.inventory_path(run_id): inventory_bytes()})
    owner_id = live.migration_owner_id("operator", run_id, TOKEN)
    physical_owner_id = f"{owner_id}:nonce"
    backend = FakeMigrationBackend(
        lease_owner=owner_id,
        lease_token=TOKEN,
        lease_expires_at=NOW + 200,
        current_time=NOW,
        writer_quiescence=WriterQuiescenceProof("a" * 64, NOW, True),
    )
    monkeypatch.setattr(live, "SparkEvidenceReader", lambda *args, **kwargs: StubReader())
    monkeypatch.setattr(live, "construct_live_backend", lambda *args, **kwargs: backend)
    monkeypatch.setattr(
        live,
        "observed_artifact_binding",
        lambda spark: {
            "environment_id": ENVIRONMENT_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "workspace_id": WORKSPACE_ID,
        },
    )
    monkeypatch.setattr(live, "uuid4", lambda: SimpleNamespace(hex="nonce"))
    plan_output = io.StringIO()
    common = [
        "--run-id",
        run_id,
        "--owner",
        "operator",
        "--lease-token",
        TOKEN,
        "--invocation-id",
        "invocation-001",
    ]
    assert live.main(
        ["plan", *common],
        spark_session=object(),
        files=files,
        hmac_key=KEY,
        clock=lambda: NOW,
        output=plan_output,
        errors=io.StringIO(),
    ) == 0
    plan = json.loads(plan_output.getvalue())
    backend._lease = replace(backend._lease, expires_at=NOW + 300)
    backend._writer_quiescence = replace(
        backend._writer_quiescence,
        captured_at=NOW + 1,
        inventory_sha256="b" * 64,
    )
    backend._time = NOW + 1

    class Lock:
        acquired = False
        released = False

        def acquire(self, owner: str) -> None:
            assert owner == physical_owner_id
            self.acquired = True

        def release(self, owner: str) -> None:
            assert owner == physical_owner_id
            self.released = True

    lock = Lock()
    apply_output = io.StringIO()
    assert live.main(
        [
            "apply",
            *common,
            "--execute",
            "--plan-sha256",
            plan["plan_sha256"],
            "--safety-token",
            f"1:people_counter_ca_0001:{plan['plan_sha256']}",
        ],
        spark_session=object(),
        files=files,
        hmac_key=KEY,
        clock=lambda: NOW,
        output=apply_output,
        errors=io.StringIO(),
        lock_factory=lambda reader: lock,  # type: ignore[arg-type]
    ) == 0
    evidence = json.loads(apply_output.getvalue())
    assert set(evidence) == {
        "inventory",
        "lock_owner_sha256",
        "migration_owner_id",
        "plan_sha256",
        "result",
        "run_id",
    }
    assert evidence["result"]["status"] == "applied"
    assert lock.acquired and lock.released
    result_evidence = json.loads(
        files.values[live.result_path(run_id, plan["plan_sha256"])]
    )
    assert result_evidence == evidence

    for command in ("status", "verify"):
        output = io.StringIO()
        assert live.main(
            [command, *common],
            spark_session=object(),
            files=files,
            hmac_key=KEY,
            clock=lambda: NOW,
            output=output,
            errors=io.StringIO(),
        ) == 0

    run2 = "run-002"
    files.values[live.inventory_path(run2)] = inventory_bytes()
    common2 = [
        "--run-id",
        run2,
        "--owner",
        "operator",
        "--lease-token",
        TOKEN,
        "--invocation-id",
        "invocation-002",
    ]
    output = io.StringIO()
    assert live.main(
        ["plan", *common2],
        spark_session=object(),
        files=files,
        hmac_key=KEY,
        clock=lambda: NOW,
        output=output,
        errors=io.StringIO(),
    ) == 0
    noop_plan = json.loads(output.getvalue())
    assert noop_plan["operations"] == []


def test_apply_without_execute_or_with_wrong_binding_is_nonmutating() -> None:
    run_id = "safe-run"
    files = MemoryFiles({live.inventory_path(run_id): inventory_bytes()})
    output = io.StringIO()
    assert live.main(
        [
            "apply",
            "--run-id",
            run_id,
            "--owner",
            "operator",
            "--lease-token",
            TOKEN,
        ],
        spark_session=object(),
        files=files,
        hmac_key=KEY,
        clock=lambda: NOW,
        output=output,
        errors=io.StringIO(),
    ) == 0
    assert json.loads(output.getvalue()) == {"execute": False, "status": "read_only"}
    assert files.creates == [
        live.stage_marker_path("safe-run", "unbound-invocation", "bootstrap"),
        live.stage_marker_path("safe-run", "unbound-invocation", "args"),
    ]


def test_apply_rejects_active_exact_reflex_before_lock() -> None:
    run_id = "active-reflex"
    owner_id = live.migration_owner_id("operator", run_id, TOKEN)
    inventory = live.InvocationInventory.from_bytes(
        inventory_bytes(should_run=True), hmac_key=KEY, now=NOW
    )
    backend = FakeMigrationBackend(
        lease_owner=owner_id,
        lease_token=TOKEN,
        lease_expires_at=NOW + 200,
        current_time=NOW,
        writer_quiescence=WriterQuiescenceProof(
            inventory.payload_sha256,
            NOW,
            False,
            inventory.active_writer_ids,
        ),
    )
    plan = build_plan(backend.discover())
    files = MemoryFiles(
        {
            live.plan_path(run_id): (
                canonical_json(live._redacted_plan(plan)).encode("utf-8") + b"\n"
            )
        }
    )
    args = SimpleNamespace(
        execute=True,
        plan_sha256=plan.sha256,
        safety_token=plan.safety_token,
    )

    with pytest.raises(live.LiveMigrationError, match="exact Reflex rule is active"):
        live._apply_command(
            args,
            spark_session=object(),
            files=files,
            inventory=inventory,
            backend=backend,  # type: ignore[arg-type]
            reader=StubReader(),  # type: ignore[arg-type]
            run_id=run_id,
            owner_id=owner_id,
            physical_owner_id=f"{owner_id}:nonce",
            lease_token=TOKEN,
            output=io.StringIO(),
            diagnostics=SimpleNamespace(mark=lambda stage: None),
            lock_factory=lambda reader: pytest.fail("lock must not be acquired"),
        )


def test_stored_plan_requires_both_exact_bindings() -> None:
    run_id = "stored-plan"
    backend = FakeMigrationBackend()
    plan = build_plan(backend.discover())
    files = MemoryFiles(
        {
            live.plan_path(run_id): (
                canonical_json(live._redacted_plan(plan)).encode("utf-8") + b"\n"
            )
        }
    )
    for plan_hash, safety_token in (
        (None, plan.safety_token),
        (plan.sha256, None),
    ):
        with pytest.raises(live.LiveMigrationError, match="requires"):
            live._stored_current_plan(
                SimpleNamespace(
                    plan_sha256=plan_hash,
                    safety_token=safety_token,
                ),
                files,
                backend,  # type: ignore[arg-type]
                run_id,
                owner_id="owner",
                lease_token=TOKEN,
            )

    with pytest.raises(live.LiveMigrationError, match="does not match"):
        live._stored_current_plan(
            SimpleNamespace(
                plan_sha256="f" * 64,
                safety_token=plan.safety_token,
            ),
            files,
            backend,  # type: ignore[arg-type]
            run_id,
            owner_id="owner",
            lease_token=TOKEN,
        )

    with pytest.raises(live.LiveMigrationError, match="safety token"):
        live._stored_current_plan(
            SimpleNamespace(
                plan_sha256=plan.sha256,
                safety_token="wrong",
            ),
            files,
            backend,  # type: ignore[arg-type]
            run_id,
            owner_id="owner",
            lease_token=TOKEN,
        )

    tampered = live._redacted_plan(plan)
    tampered["rollback_manifest"] = "tampered"
    files.values[live.plan_path(run_id)] = (
        canonical_json(tampered).encode("utf-8") + b"\n"
    )
    with pytest.raises(live.LiveMigrationError, match="fresh discovery"):
        live._stored_current_plan(
            SimpleNamespace(
                plan_sha256=plan.sha256,
                safety_token=plan.safety_token,
            ),
            files,
            backend,  # type: ignore[arg-type]
            run_id,
            owner_id="owner",
            lease_token=TOKEN,
        )

    files.values[live.plan_path(run_id)] = b"[]"
    with pytest.raises(live.LiveMigrationError, match="does not match"):
        live._stored_current_plan(
            SimpleNamespace(
                plan_sha256=plan.sha256,
                safety_token=plan.safety_token,
            ),
            files,
            backend,  # type: ignore[arg-type]
            run_id,
            owner_id="owner",
            lease_token=TOKEN,
        )


def test_apply_default_lock_wiring_and_partial_exit_are_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "partial-apply"
    owner_id = live.migration_owner_id("operator", run_id, TOKEN)
    inventory = live.InvocationInventory.from_bytes(
        inventory_bytes(), hmac_key=KEY, now=NOW
    )
    backend = FakeMigrationBackend(
        lease_owner=owner_id,
        lease_token=TOKEN,
        lease_expires_at=NOW + 200,
        current_time=NOW,
        writer_quiescence=WriterQuiescenceProof("a" * 64, NOW, True),
    )
    backend.fail_before.add("people_counter_ca_attempts")
    plan = build_plan(backend.discover())
    files = MemoryFiles(
        {
            live.plan_path(run_id): (
                canonical_json(live._redacted_plan(plan)).encode("utf-8") + b"\n"
            )
        }
    )
    reader = StubReader()
    sentinel_cas = object()
    seen: dict[str, object] = {}

    class Lock:
        def __init__(self, supplied_reader, supplied_cas) -> None:
            seen["reader"] = supplied_reader
            seen["cas"] = supplied_cas

        def acquire(self, owner: str) -> None:
            seen["acquired"] = owner

        def release(self, owner: str) -> None:
            pytest.fail("partial apply must retain the migration lock")

    monkeypatch.setattr(live, "ControlWriterMigrationLock", Lock)
    monkeypatch.setattr(
        live, "delta_control_writer_cas", lambda spark: sentinel_cas
    )
    code = live._apply_command(
        SimpleNamespace(
            execute=True,
            plan_sha256=plan.sha256,
            safety_token=plan.safety_token,
        ),
        spark_session="spark",
        files=files,
        inventory=inventory,
        backend=backend,  # type: ignore[arg-type]
        reader=reader,  # type: ignore[arg-type]
        run_id=run_id,
        owner_id=owner_id,
        physical_owner_id=f"{owner_id}:nonce",
        lease_token=TOKEN,
        output=io.StringIO(),
        diagnostics=SimpleNamespace(mark=lambda stage: None),
        lock_factory=None,
    )
    assert code == 3
    assert seen == {
        "acquired": f"{owner_id}:nonce",
        "cas": sentinel_cas,
        "reader": reader,
    }


def test_stored_plan_validates_fresh_not_stored_quiescence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "fresh-quiescence"
    owner_id = live.migration_owner_id("operator", run_id, TOKEN)
    backend = FakeMigrationBackend(
        lease_owner=owner_id,
        lease_token=TOKEN,
        lease_expires_at=NOW + 100,
        current_time=NOW,
        writer_quiescence=WriterQuiescenceProof("a" * 64, NOW - 50, True),
    )
    reviewed = build_plan(backend.discover())
    files = MemoryFiles(
        {
            live.plan_path(run_id): (
                canonical_json(live._redacted_plan(reviewed)).encode() + b"\n"
            )
        }
    )
    backend._writer_quiescence = WriterQuiescenceProof(
        "b" * 64, NOW, True
    )
    backend._lease = replace(backend._lease, expires_at=NOW + 200)
    seen: dict[str, object] = {}
    original = live.validate_apply_plan

    def validate(*args, **kwargs):
        seen["proof"] = kwargs["current"].writer_quiescence
        return original(*args, **kwargs)

    monkeypatch.setattr(live, "validate_apply_plan", validate)
    assert live._stored_current_plan(
        SimpleNamespace(
            plan_sha256=reviewed.sha256,
            safety_token=reviewed.safety_token,
        ),
        files,
        backend,  # type: ignore[arg-type]
        run_id,
        owner_id=owner_id,
        lease_token=TOKEN,
    ).sha256 == reviewed.sha256
    assert seen["proof"] == WriterQuiescenceProof("b" * 64, NOW, True)

    backend._time = NOW + live.INVENTORY_MAX_AGE_SECONDS + 1
    backend._lease = replace(
        backend._lease, expires_at=backend._time + 100
    )
    with pytest.raises(PreconditionError, match="stale or future-dated"):
        live._stored_current_plan(
            SimpleNamespace(
                plan_sha256=reviewed.sha256,
                safety_token=reviewed.safety_token,
            ),
            files,
            backend,  # type: ignore[arg-type]
            run_id,
            owner_id=owner_id,
            lease_token=TOKEN,
        )


def test_apply_validates_before_lock_and_releases_only_exact_preop_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "preop-release"
    owner_id = live.migration_owner_id("operator", run_id, TOKEN)
    inventory = live.InvocationInventory.from_bytes(
        inventory_bytes(), hmac_key=KEY, now=NOW
    )
    backend = FakeMigrationBackend(
        lease_owner=owner_id,
        lease_token=TOKEN,
        lease_expires_at=NOW + live.INVENTORY_MAX_AGE_SECONDS + 100,
        current_time=NOW,
        writer_quiescence=WriterQuiescenceProof("a" * 64, NOW, True),
    )
    plan = build_plan(backend.discover())
    files = MemoryFiles(
        {
            live.plan_path(run_id): (
                canonical_json(live._redacted_plan(plan)).encode() + b"\n"
            )
        }
    )
    args = SimpleNamespace(
        execute=True,
        plan_sha256=plan.sha256,
        safety_token=plan.safety_token,
    )
    backend._time = NOW + live.INVENTORY_MAX_AGE_SECONDS + 1
    with pytest.raises(PreconditionError, match="stale"):
        live._apply_command(
            args,
            spark_session=object(),
            files=files,
            inventory=inventory,
            backend=backend,  # type: ignore[arg-type]
            reader=StubReader(),  # type: ignore[arg-type]
            run_id=run_id,
            owner_id=owner_id,
            physical_owner_id="physical",
            lease_token=TOKEN,
            output=io.StringIO(),
            diagnostics=SimpleNamespace(mark=lambda stage: None),
            lock_factory=lambda reader: pytest.fail(
                "lock acquisition must follow deterministic validation"
            ),
        )

    backend._time = NOW
    events: list[str] = []

    class Lock:
        def acquire(self, owner: str) -> None:
            events.append("acquire")
            backend._time = NOW + live.INVENTORY_MAX_AGE_SECONDS + 1

        def release(self, owner: str) -> None:
            events.append("release")

    with pytest.raises(PreconditionError, match="stale"):
        live._apply_command(
            args,
            spark_session=object(),
            files=files,
            inventory=inventory,
            backend=backend,  # type: ignore[arg-type]
            reader=StubReader(),  # type: ignore[arg-type]
            run_id=run_id,
            owner_id=owner_id,
            physical_owner_id="physical",
            lease_token=TOKEN,
            output=io.StringIO(),
            diagnostics=SimpleNamespace(mark=lambda stage: None),
            lock_factory=lambda reader: Lock(),
        )
    assert events == ["acquire", "release"]

    backend._time = NOW
    events.clear()

    class AmbiguousLock(Lock):
        def release(self, owner: str) -> None:
            pytest.fail("ambiguous proof must retain the lock")

    monkeypatch.setattr(
        live,
        "_no_operation_began",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("ambiguous")),
    )
    with pytest.raises(live.LiveMigrationError, match="retained lock"):
        live._apply_command(
            args,
            spark_session=object(),
            files=files,
            inventory=inventory,
            backend=backend,  # type: ignore[arg-type]
            reader=StubReader(),  # type: ignore[arg-type]
            run_id=run_id,
            owner_id=owner_id,
            physical_owner_id="physical",
            lease_token=TOKEN,
            output=io.StringIO(),
            diagnostics=SimpleNamespace(mark=lambda stage: None),
            lock_factory=lambda reader: AmbiguousLock(),
        )
    assert events == ["acquire"]


def test_migration_sjd_is_installed_wheel_only_with_safe_defaults() -> None:
    definition = build_migration_sjd_definition()
    metadata = decoded_metadata(definition)
    assert metadata["commandLineArguments"] == ""
    assert metadata["defaultLakehouseArtifactId"] == LAKEHOUSE_ID
    assert metadata["environmentArtifactId"] == ENVIRONMENT_ID
    assert metadata["additionalLakehouseIds"] == []
    assert metadata["additionalLibraryUris"] == []
    assert metadata["retryPolicy"] is None
    source = migration_main_source().decode()
    assert "people_counter.fabric_production_migration_live import main" in source
    assert "%pip" not in source and "subprocess" not in source
    assert hashlib.sha256(migration_sjd_bytes()).hexdigest() == (
        "e226626c356174dc8ce9fa25d8320b7ca29ef3c7c144ef4f00f2d0dc916aed25"
    )


def test_migration_sjd_export_is_create_only_and_idempotent(tmp_path) -> None:
    destination = tmp_path / "export"
    first = export_migration_sjd(destination)
    second = export_migration_sjd(destination)
    assert first == second
    assert first["sha256"] == hashlib.sha256(
        (destination / live_path_name()).read_bytes()
    ).hexdigest()
    (destination / live_path_name()).write_text("different")
    with pytest.raises(FileExistsError, match="differing"):
        export_migration_sjd(destination)


def live_path_name() -> str:
    return "migration.SparkJobDefinitionV2.json"


def test_diagnostic_redaction_sanitizes_arguments_message_and_traceback() -> None:
    hmac_secret = "ab" * 32
    lease_secret = "lease-super-secret"
    safety_secret = "safety-super-secret"
    arguments = [
        "plan",
        "--run-id",
        "redaction-run",
        "--invocation-id",
        "redaction-invocation",
        f"--inventory-hmac-key={hmac_secret}",
        "--lease-token",
        lease_secret,
        "--safety-token",
        safety_secret,
    ]
    files = MemoryFiles()
    recorder = live.DiagnosticRecorder.create(files, arguments)
    recorder.mark("bootstrap")
    args = live._build_parser().parse_args(arguments)
    try:
        raise RuntimeError(
            f"failed with {hmac_secret} {lease_secret} {safety_secret}"
        )
    except RuntimeError as error:
        envelope = recorder.failure_envelope(
            error, args=args, spark_session=None
        )
    encoded = canonical_json(envelope)
    assert hmac_secret not in encoded
    assert lease_secret not in encoded
    assert safety_secret not in encoded
    assert encoded.count("<redacted>") >= 6
    assert envelope["redacted_arguments"] == [
        "plan",
        "--run-id",
        "redaction-run",
        "--invocation-id",
        "redaction-invocation",
        "--inventory-hmac-key=<redacted>",
        "--lease-token",
        "<redacted>",
        "--safety-token",
        "<redacted>",
    ]
    jwt = (
        "eyJabcdefghijklmnop.abcdefghijklmnop."
        "abcdEFGH"
    )
    assert live._sanitize_text(
        f"--lease-token leaked --safety-token=unsafe {jwt}", ()
    ) == (
        "--lease-token <redacted> --safety-token=<redacted> "
        "<redacted-jwt>"
    )
    assert live._sanitize_text("x" * 65537, ()) == "x" * 65536


def test_stage_markers_are_ordered_create_only_and_collision_safe() -> None:
    files = MemoryFiles()
    recorder = live.DiagnosticRecorder.create(
        files,
        [
            "plan",
            "--run-id",
            "marker-run",
            "--invocation-id",
            "marker-invocation",
        ],
    )
    for stage in live._STAGES:
        recorder.mark(stage)
    assert files.creates == [
        live.stage_marker_path("marker-run", "marker-invocation", stage)
        for stage in live._STAGES
    ]
    for sequence, stage in enumerate(live._STAGES):
        assert json.loads(
            files.values[
                live.stage_marker_path(
                    "marker-run", "marker-invocation", stage
                )
            ]
        ) == {
            "invocation_id": "marker-invocation",
            "run_id": "marker-run",
            "schema": live.DIAGNOSTIC_SCHEMA,
            "sequence": sequence,
            "stage": stage,
        }
    recorder.mark(live._STAGES[-1])
    assert len(files.creates) == len(live._STAGES)
    with pytest.raises(live.LiveMigrationError) as backwards:
        recorder.mark("plan-build")
    assert str(backwards.value) == "diagnostic stages cannot move backwards"
    files.values[
        live.stage_marker_path(
            "marker-run", "marker-invocation", live._STAGES[-1]
        )
    ] = b"different"
    with pytest.raises(FileExistsError):
        recorder.mark(live._STAGES[-1])


def test_main_reraises_original_when_failure_evidence_write_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = RuntimeError("original lease single-use-token")

    class DiagnosticWriteFails(MemoryFiles):
        def create_bytes(self, path: str, content: bytes) -> None:
            if path.endswith("/live-failure.json"):
                raise OSError("diagnostic storage unavailable")
            super().create_bytes(path, content)

    files = DiagnosticWriteFails()

    def fail(*args, **kwargs):
        raise original

    monkeypatch.setattr(live, "_run_command", fail)
    errors = io.StringIO()
    with pytest.raises(RuntimeError) as observed:
        live.main(
            [
                "plan",
                "--run-id",
                "original-run",
                "--invocation-id",
                "original-invocation",
                "--lease-token",
                TOKEN,
            ],
            spark_session=object(),
            files=files,
            hmac_key=KEY,
            errors=errors,
        )
    assert observed.value is original
    assert TOKEN not in errors.getvalue()
    expected_note = (
        "diagnostic-write failure: OSError: diagnostic storage unavailable"
    )
    assert expected_note in errors.getvalue()
    assert original.__notes__ == [expected_note]


@pytest.mark.parametrize(
    "path",
    [
        "../Files/people-counter/migrations/people_counter_ca_0001/x",
        "Files/people-counter/migrations/people_counter_ca_0001/../x",
        "Files/people-counter/migrations/people_counter_ca_0001/safe\\x",
        "Files/people-counter/migrations/people_counter_ca_0001/safe\x00x",
        "/Files/people-counter/migrations/people_counter_ca_0001/x",
    ],
)
def test_notebookutils_path_confinement_rejects_aliases(path: str) -> None:
    with pytest.raises(live.LiveMigrationError) as observed:
        live.NotebookUtilsEvidenceFiles._path(path)
    assert str(observed.value) == (
        "evidence path is outside the fixed migration root"
    )


def test_notebookutils_path_confinement_accepts_only_canonical_child() -> None:
    path = (
        "Files/people-counter/migrations/people_counter_ca_0001/"
        "diagnostics/run/invocation/evidence.json"
    )
    assert live.NotebookUtilsEvidenceFiles._path(path) == path
    with pytest.raises(live.LiveMigrationError) as observed:
        live.stage_marker_path("run", "invocation", "unknown")
    assert str(observed.value) == "diagnostic stage is not supported"


@pytest.mark.parametrize("head_value", [b'{"valid":true}\n', '{"valid":true}\n'])
def test_notebookutils_read_bytes_preserves_head_content_type(
    monkeypatch: pytest.MonkeyPatch, head_value: bytes | str
) -> None:
    module = types.ModuleType("notebookutils")
    module.fs = SimpleNamespace(  # type: ignore[attr-defined]
        head=lambda path, size: head_value
    )
    monkeypatch.setitem(sys.modules, "notebookutils", module)
    path = (
        "Files/people-counter/migrations/people_counter_ca_0001/"
        "inventory/readback.json"
    )
    assert live.NotebookUtilsEvidenceFiles().read_bytes(path) == (
        b'{"valid":true}\n'
    )


def test_write_failure_binds_handler_runtime_and_readback_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = MemoryFiles()
    recorder = live.DiagnosticRecorder.create(
        files,
        [
            "plan",
            "--run-id",
            "failure-handler-run",
            "--invocation-id",
            "failure-handler-invocation",
        ],
    )
    recorder.mark("bootstrap")
    runtime = {
        "fabric_runtime": "2.0",
        "java": "17",
        "python": "3.11",
        "spark": "3.5",
    }
    spark = object()

    def runtime_evidence(observed):
        assert observed is spark
        return runtime

    monkeypatch.setattr(live, "_runtime_evidence", runtime_evidence)
    path, digest = recorder.write_failure(
        ValueError("boom"),
        args=None,
        spark_session=spark,
        handler="wrapper",
    )
    assert path == live.failure_path(
        "failure-handler-run", "failure-handler-invocation", "wrapper"
    )
    content = files.values[path]
    assert digest == hashlib.sha256(content).hexdigest()
    evidence = json.loads(content)
    assert evidence["handler"] == "wrapper"
    assert evidence["runtime"] == runtime


def test_main_success_uses_process_arguments_active_spark_and_hex_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "process-arguments"
    files = MemoryFiles({live.inventory_path(run_id): inventory_bytes()})
    active_spark = object()
    pyspark = types.ModuleType("pyspark")
    pyspark.__path__ = []  # type: ignore[attr-defined]
    pyspark_sql = types.ModuleType("pyspark.sql")

    class SparkSession:
        @staticmethod
        def getActiveSession():
            return active_spark

    pyspark_sql.SparkSession = SparkSession  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "migration-main.py",
            "inventory",
            "--run-id",
            run_id,
            "--inventory-hmac-key",
            KEY.hex(),
        ],
    )
    output = io.StringIO()
    assert (
        live.main(
            None,
            spark_session=None,
            files=files,
            hmac_key=None,
            clock=lambda: NOW,
            output=output,
            errors=io.StringIO(),
        )
        == 0
    )
    assert json.loads(output.getvalue())["passed"] is True


def test_main_forwards_exact_runtime_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    spark = object()
    files = MemoryFiles()
    clock = lambda: NOW
    output = io.StringIO()
    lock_factory = lambda reader: object()

    def capture(args, **kwargs):
        captured["args"] = args
        captured.update(kwargs)
        return 17

    monkeypatch.setattr(live, "_run_command", capture)
    assert (
        live.main(
            [
                "inventory",
                "--run-id",
                "forwarding-run",
                "--invocation-id",
                "forwarding-invocation",
            ],
            spark_session=spark,
            files=files,
            hmac_key=KEY,
            clock=clock,
            output=output,
            errors=io.StringIO(),
            lock_factory=lock_factory,
        )
        == 17
    )
    assert captured["spark_session"] is spark
    assert captured["files"] is files
    assert captured["key"] is KEY
    assert captured["clock"] is clock
    assert captured["output"] is output
    assert captured["lock_factory"] is lock_factory
    assert captured["diagnostics"].run_id == "forwarding-run"


def test_main_reads_environment_hmac_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def capture(args, **kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(live, "_run_command", capture)
    monkeypatch.setenv("PC_MIGRATION_INVENTORY_HMAC_KEY", "environment-key")
    assert (
        live.main(
            [
                "inventory",
                "--run-id",
                "environment-key-run",
                "--invocation-id",
                "environment-key-invocation",
            ],
            spark_session=object(),
            files=MemoryFiles(),
            hmac_key=None,
            errors=io.StringIO(),
        )
        == 0
    )
    assert captured["key"] == b"environment-key"
    monkeypatch.delenv("PC_MIGRATION_INVENTORY_HMAC_KEY")
    assert (
        live.main(
            [
                "inventory",
                "--run-id",
                "empty-environment-key-run",
                "--invocation-id",
                "empty-environment-key-invocation",
            ],
            spark_session=object(),
            files=MemoryFiles(),
            hmac_key=None,
            errors=io.StringIO(),
        )
        == 0
    )
    assert captured["key"] == b""


def test_main_process_arguments_preserve_explicit_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def capture(args, **kwargs):
        captured["command"] = args.command
        return 0

    monkeypatch.setattr(live, "_run_command", capture)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "migration-main.py",
            "diagnose",
            "--run-id",
            "process-command-run",
            "--invocation-id",
            "process-command-invocation",
        ],
    )
    assert (
        live.main(
            None,
            spark_session=object(),
            files=MemoryFiles(),
            hmac_key=KEY,
            errors=io.StringIO(),
        )
        == 0
    )
    assert captured["command"] == "diagnose"


def test_main_exact_bootstrap_and_hex_key_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pyspark = types.ModuleType("pyspark")
    pyspark.__path__ = []  # type: ignore[attr-defined]
    pyspark_sql = types.ModuleType("pyspark.sql")

    class SparkSession:
        @staticmethod
        def getActiveSession():
            return None

    pyspark_sql.SparkSession = SparkSession  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)
    with pytest.raises(live.LiveMigrationError) as no_spark:
        live.main(
            [
                "inventory",
                "--run-id",
                "no-spark",
                "--invocation-id",
                "no-spark-invocation",
            ],
            spark_session=None,
            files=MemoryFiles(),
            hmac_key=KEY,
            errors=io.StringIO(),
        )
    assert str(no_spark.value) == "an active Spark session is required"

    with pytest.raises(live.LiveMigrationError) as bad_hex:
        live.main(
            [
                "inventory",
                "--run-id",
                "bad-hex",
                "--invocation-id",
                "bad-hex-invocation",
                "--inventory-hmac-key",
                "not-hexadecimal",
            ],
            spark_session=object(),
            files=MemoryFiles(),
            errors=io.StringIO(),
        )
    assert str(bad_hex.value) == (
        "inventory HMAC key argument must be hexadecimal"
    )


def test_main_successful_failure_evidence_is_exact_and_preserves_original(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "lease-secret-value"
    original = RuntimeError(f"original contains {secret}")
    files = MemoryFiles()
    spark = object()

    def runtime_evidence(observed):
        assert observed is spark
        return {
            "fabric_runtime": "2.0",
            "java": "21",
            "python": "3.13",
            "spark": "4.1",
        }

    def fail(*args, **kwargs):
        raise original

    monkeypatch.setattr(live, "_run_command", fail)
    monkeypatch.setattr(live, "_runtime_evidence", runtime_evidence)
    errors = io.StringIO()
    with pytest.raises(RuntimeError) as observed:
        live.main(
            [
                "plan",
                "--run-id",
                "failure-run",
                "--invocation-id",
                "failure-invocation",
                "--lease-token",
                secret,
            ],
            spark_session=spark,
            files=files,
            hmac_key=KEY,
            errors=errors,
        )
    assert observed.value is original
    path = live.failure_path("failure-run", "failure-invocation")
    content = files.values[path]
    evidence = json.loads(content)
    assert evidence["handler"] == "live"
    assert evidence["exception"]["type"] == "RuntimeError"
    assert evidence["exception"]["message"] == "original contains <redacted>"
    assert secret not in canonical_json(evidence)
    digest = hashlib.sha256(content).hexdigest()
    assert errors.getvalue().splitlines() == [
        f"diagnostic_evidence path={path} sha256={digest}",
        "RuntimeError: original contains <redacted>",
    ]
    assert not hasattr(original, "__notes__")


def test_diagnose_is_read_only_and_records_runtime_catalog_and_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "diagnose-run"
    invocation_id = "diagnose-invocation"
    inventory = live.InvocationInventory.from_bytes(
        inventory_bytes(), hmac_key=KEY, now=NOW
    )
    files = MemoryFiles({live.inventory_path(run_id): inventory_bytes()})
    required = {
        live.LOCK_TABLE,
        live.DISPATCHER_LEASE_TABLE,
        live.REGISTRATION_LEASE_TABLE,
        live.WORK_TABLE,
    }
    sql_calls: list[str] = []

    class Row:
        def __init__(self, value):
            self.value = value

        def asDict(self, recursive=True):
            return dict(self.value)

    class Catalog:
        @staticmethod
        def currentDatabase():
            return live.DEFAULT_LAKEHOUSE_NAME

        @staticmethod
        def tableExists(name):
            return name in required

    class Conf:
        values = {
            "spark.microsoft.fabric.workspace.id": WORKSPACE_ID,
            "spark.microsoft.fabric.lakehouse.id": LAKEHOUSE_ID,
            "spark.microsoft.fabric.environment.id": ENVIRONMENT_ID,
        }

        def get(self, key):
            if key not in self.values:
                raise KeyError(key)
            return self.values[key]

    class Spark:
        catalog = Catalog()
        conf = Conf()
        version = "3.5.5"

        @staticmethod
        def sql(statement):
            assert statement.startswith("DESCRIBE HISTORY `")
            sql_calls.append(statement)
            return SimpleNamespace(collect=lambda: [Row({"version": 7})])

    monkeypatch.setattr(
        live,
        "_import_diagnostics",
        lambda: {
            "modules": {"people_counter": "installed"},
            "package_source_sha256": "a" * 64,
            "package_version": live.EXPECTED_PACKAGE_VERSION,
        },
    )
    monkeypatch.setattr(
        live, "_notebookutils_diagnostics", lambda: {"fs_methods": ["exists", "head", "put"]}
    )
    arguments = [
        "diagnose",
        "--run-id",
        run_id,
        "--inventory-run-id",
        run_id,
        "--invocation-id",
        invocation_id,
    ]
    recorder = live.DiagnosticRecorder.create(files, arguments)
    for stage in ("bootstrap", "args", "inventory-read", "inventory-verify"):
        recorder.mark(stage)
    output = io.StringIO()
    assert (
        live._diagnose_command(
            spark_session=Spark(),
            files=files,
            inventory=inventory,
            inventory_run_id=run_id,
            diagnostics=recorder,
            output=output,
        )
        == 0
    )
    evidence = json.loads(
        files.values[live.diagnose_path(run_id, invocation_id)]
    )
    assert evidence["status"] == "passed"
    assert [check["name"] for check in evidence["checks"]] == [
        "imports-package-version",
        "notebookutils",
        "fixed-path-access",
        "runtime-binding",
        "spark-catalog-default-and-visibility",
        "delta-history-access",
    ]
    assert len(sql_calls) == len(required)
    assert all(statement.startswith("DESCRIBE HISTORY") for statement in sql_calls)
    assert all(
        path.startswith(live.diagnostic_root(run_id, invocation_id) + "/")
        for path in files.creates
    )


def test_generated_wrapper_redacts_and_reraises_original_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored: dict[str, bytes] = {}

    class WrapperFS:
        @staticmethod
        def exists(path):
            return path in stored

        @staticmethod
        def put(path, content, overwrite):
            assert overwrite is False
            if path in stored:
                return False
            stored[path] = content.encode("utf-8")
            return True

        @staticmethod
        def head(path, size):
            return stored[path].decode("utf-8")

    notebookutils = types.ModuleType("notebookutils")
    notebookutils.fs = WrapperFS()  # type: ignore[attr-defined]
    fake_live = types.ModuleType("people_counter.fabric_production_migration_live")
    original = RuntimeError(
        "wrapper leaked key-secret-123 lease-secret-456 safety-secret-789"
    )

    def fail(arguments):
        raise original

    fake_live.main = fail  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "notebookutils", notebookutils)
    monkeypatch.setitem(
        sys.modules, "people_counter.fabric_production_migration_live", fake_live
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "plan",
            "--run-id",
            "wrapper-run",
            "--invocation-id",
            "wrapper-invocation",
            "--inventory-hmac-key",
            "key-secret-123",
            "--lease-token=lease-secret-456",
            "--safety-token",
            "safety-secret-789",
        ],
    )
    namespace = {"__name__": "generated_wrapper"}
    exec(compile(migration_main_source(), "Main/main.py", "exec"), namespace)
    with pytest.raises(RuntimeError) as observed:
        namespace["_run"]()
    assert observed.value is original
    failure = next(
        content
        for path, content in stored.items()
        if path.endswith("/wrapper-failure.json")
    )
    assert b"key-secret-123" not in failure
    assert b"lease-secret-456" not in failure
    assert b"safety-secret-789" not in failure
    assert json.loads(failure)["exception"]["type"] == "RuntimeError"
