from __future__ import annotations

import hashlib
import io
import json
from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from people_counter.fabric_production_migration import (
    JOURNAL_TABLE,
    ENVIRONMENT_ID,
    LAKEHOUSE_ID,
    MIGRATION_ID,
    MIGRATION_VERSION,
    TABLE_ALLOWLIST,
    TABLE_SPECS,
    AdditiveOperation,
    ApplyStatus,
    ColumnSpec,
    DiscoverySnapshot,
    FakeMigrationBackend,
    JournalEntry,
    JournalIntegrityError,
    LeaseSnapshot,
    MigrationPlan,
    OperationKind,
    PreconditionError,
    QUIESCENCE_MAX_AGE_SECONDS,
    SparkMigrationBackend,
    TableSnapshot,
    WriterQuiescenceProof,
    WORKSPACE_ID,
    apply_plan,
    build_plan,
    compatibility_report,
    evidence_hash,
    journal_integrity_errors,
    main,
    render_operation,
    validate_apply_plan,
    verify_backend,
)


OWNER = "migration-operator"
TOKEN = "one-use-secret"


def leased_backend() -> FakeMigrationBackend:
    return FakeMigrationBackend(
        lease_owner=OWNER,
        lease_token=TOKEN,
        lease_expires_at=200.0,
        current_time=100.0,
    )


def test_fixed_identity_allowlist_and_immutable_deterministic_plan() -> None:
    backend = leased_backend()
    snapshot = backend.discover()
    plan = build_plan(snapshot)

    assert MIGRATION_ID == "people_counter_ca_0001"
    assert MIGRATION_VERSION == 1
    assert TABLE_ALLOWLIST == (
        "people_counter_ca_migration_journal",
        "people_counter_ca_attempts",
        "people_counter_ca_batch_members",
        "people_counter_ca_batches",
        "people_counter_ca_locks",
        "people_counter_ca_publications",
        "people_counter_ca_reconciliation_findings",
        "people_counter_ca_replay_requests",
        "people_counter_ca_routing_allowlist",
        "people_counter_ca_shadow_audit",
        "people_counter_ca_work",
    )
    assert plan.to_json() == build_plan(snapshot).to_json()
    assert plan.to_dict()["plan_sha256"] == plan.sha256
    assert plan.to_dict()["safety_token"] == plan.safety_token
    assert len(snapshot.sha256) == len(plan.sha256) == 64
    assert json.dumps(json.loads(plan.to_json()), separators=(",", ":"), sort_keys=True) == (
        plan.to_json()
    )
    with pytest.raises(FrozenInstanceError):
        snapshot.control_owner = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError, match="immutable tuples"):
        TableSnapshot(
            name=JOURNAL_TABLE,
            exists=True,
            columns=[],  # type: ignore[arg-type]
        )


def test_ast_and_renderer_can_only_create_exact_allowlisted_delta_tables() -> None:
    operation = AdditiveOperation(
        OperationKind.CREATE_TABLE_IF_NOT_EXISTS, JOURNAL_TABLE
    )
    sql = render_operation(operation)
    assert sql.startswith(
        "CREATE TABLE IF NOT EXISTS `people_counter_ca_migration_journal`"
    )
    assert " USING DELTA " in sql
    assert not any(
        destructive in sql.upper()
        for destructive in ("DROP ", "DELETE ", "TRUNCATE ", "REPLACE ", "ALTER ")
    )
    with pytest.raises(ValueError, match="outside production allowlist"):
        AdditiveOperation(OperationKind.CREATE_TABLE_IF_NOT_EXISTS, "other_table")
    with pytest.raises(ValueError):
        AdditiveOperation("drop_table", JOURNAL_TABLE)  # type: ignore[arg-type]
    rendered = {
        item.table_name: hashlib.sha256(
            render_operation(item).encode("utf-8")
        ).hexdigest()
        for item in build_plan(leased_backend().discover()).operations
    }
    assert set(rendered) == set(TABLE_ALLOWLIST)
    assert rendered == {
        "people_counter_ca_migration_journal": (
            "00685d3bf1f2e78211be5fb4acb3c57dbaffa4554fba0912ca0e096b7fae4c70"
        ),
        "people_counter_ca_attempts": (
            "8ce8b38d4f2aa752052323506ec2c4d92a17538b172fc87a583fa32ecef90695"
        ),
        "people_counter_ca_batch_members": (
            "92ec6768f664e4ea7d1f4bd3eee6862900b111d914c1b3849be3cad4d92f4cd9"
        ),
        "people_counter_ca_batches": (
            "254f35001425a234198b6c0824e9a29241fcbc173244e53003be297f1fb4aa5b"
        ),
        "people_counter_ca_locks": (
            "232b25acaba5ed1c7ea975ffb09f70033dd6f5bb925f002004336a9bc0ae0687"
        ),
        "people_counter_ca_publications": (
            "13f6fd9ca327b45fae2dfd2697970702bdcd84a6f8b9673a38c874dd42a60b66"
        ),
        "people_counter_ca_reconciliation_findings": (
            "0cae9139829251624cf4c8317b889fdae0698c950a151c66ba4dce0b458f62e2"
        ),
        "people_counter_ca_replay_requests": (
            "38b191bde9255a9253412b2456ddba3c1696056d4002d1383296f6598107b516"
        ),
        "people_counter_ca_routing_allowlist": (
            "d6ad660e71cbebe0000c0e8cf792fab9db4a33b7ab54c7128d8f0e8725fc160b"
        ),
        "people_counter_ca_shadow_audit": (
            "d17516175840b25fb0e29b5c33919884c301ccc4aa87c2188a0e5521c229f1d1"
        ),
        "people_counter_ca_work": (
            "cfae6f11add421e21fedb4bb73feae6623244630b9cffe7d03d6c5230b071c60"
        ),
    }


def test_migration_safety_token_is_exactly_bound_to_plan_hash() -> None:
    backend = leased_backend()
    plan = build_plan(backend.discover())

    assert plan.safety_token == f"{MIGRATION_VERSION}:{MIGRATION_ID}:{plan.sha256}"
    with pytest.raises(PreconditionError, match="migration ID and plan hash"):
        apply_plan(
            backend,
            plan,
            owner=OWNER,
            lease_token=TOKEN,
            safety_token="wrong-plan-token",
        )
    assert backend.applied_sql == []


def test_plan_identity_and_operation_set_are_exact() -> None:
    plan = build_plan(leased_backend().discover())

    for change in (
        {"migration_id": "other"},
        {"migration_version": MIGRATION_VERSION + 1},
    ):
        with pytest.raises(ValueError, match="migration plan identity is fixed"):
            replace(plan, **change)
    with pytest.raises(
        ValueError,
        match="plan operations must exactly match missing allowlisted tables",
    ):
        MigrationPlan(
            discovery=plan.discovery,
            compatibility=plan.compatibility,
            operations=(),
            rollback_manifest=plan.rollback_manifest,
        )


def test_compatibility_reports_quiescence_control_owner_delta_and_schema() -> None:
    malformed = TableSnapshot(
        name="people_counter_ca_work",
        exists=True,
        columns=(ColumnSpec("wrong", "string"),),
        provider="delta",
    )
    backend = FakeMigrationBackend(
        tables={malformed.name: malformed},
        active_run_ids=("run-b", "run-a"),
        control_owner="worker-1",
        supports_delta=False,
    )
    report = compatibility_report(backend.discover())
    assert not report.compatible
    assert report.blockers == (
        "backend does not support Delta tables",
        "control plane is not quiescent: run-a,run-b",
        "control lock is owned by 'worker-1'",
        "existing table has incompatible exact metadata: people_counter_ca_work",
    )


@pytest.mark.parametrize(
    ("change", "match"),
    [
        ("drift", "discovery drift"),
        (
            "active",
            "incompatible preconditions: control plane is not quiescent",
        ),
        (
            "active-lease",
            "incompatible preconditions: active work leases exist",
        ),
        ("owner", "incompatible preconditions: control lock is owned"),
        ("lease-owner", "lease owner mismatch"),
        ("lease-token", "lease token mismatch"),
        ("lease-missing", "lease expiry is missing or invalid"),
        ("lease-equal", "lease has expired"),
        ("lease-expired", "lease has expired"),
        ("writer-stale", "proof is stale or future-dated"),
        ("writer-future", "proof is stale or future-dated"),
    ],
)
def test_apply_refuses_failed_preconditions(change: str, match: str) -> None:
    backend = leased_backend()
    plan = build_plan(backend.discover())
    if change == "drift":
        backend.replace_table_for_test(
            TableSnapshot.from_spec(TABLE_SPECS[JOURNAL_TABLE])
        )
    elif change == "active":
        backend._active_run_ids = ("run-1",)
        plan = build_plan(backend.discover())
    elif change == "active-lease":
        backend._active_lease_ids = ("work-1:attempt-1",)
        plan = build_plan(backend.discover())
    elif change == "owner":
        backend._control_owner = "controller"
        plan = build_plan(backend.discover())
    elif change == "lease-owner":
        backend._lease = LeaseSnapshot("someone", backend._lease.token_sha256, 200.0)
        plan = build_plan(backend.discover())
    elif change == "lease-token":
        token = "wrong"
    elif change == "lease-missing":
        backend._lease = LeaseSnapshot(
            OWNER, backend._lease.token_sha256, None
        )
        plan = build_plan(backend.discover())
    elif change == "lease-equal":
        backend._lease = LeaseSnapshot(
            OWNER, backend._lease.token_sha256, backend.now()
        )
        plan = build_plan(backend.discover())
    elif change in {"writer-stale", "writer-future"}:
        backend._writer_quiescence = WriterQuiescenceProof(
            inventory_sha256="e" * 64,
            captured_at=-0.0 if change == "writer-stale" else backend.now() + 1,
            passed=True,
            stopped_trigger_ids=("trigger-1",),
        )
        if change == "writer-stale":
            backend._time = QUIESCENCE_MAX_AGE_SECONDS + 1
        plan = build_plan(backend.discover())
    else:
        backend._lease = LeaseSnapshot(OWNER, backend._lease.token_sha256, 99.0)
        plan = build_plan(backend.discover())
    with pytest.raises(PreconditionError, match=match):
        apply_plan(
            backend,
            plan,
            owner=OWNER,
            lease_token=token if change == "lease-token" else TOKEN,
            safety_token=plan.safety_token,
        )
    assert backend.applied_sql == []


def test_quiescence_cold_start_budget_accepts_boundary_only() -> None:
    backend = leased_backend()
    backend._lease = LeaseSnapshot(
        OWNER,
        backend._lease.token_sha256,
        QUIESCENCE_MAX_AGE_SECONDS + 10,
    )
    backend._writer_quiescence = WriterQuiescenceProof(
        "e" * 64, 0.0, True, stopped_trigger_ids=("trigger-1",)
    )
    plan = build_plan(backend.discover())
    backend._time = QUIESCENCE_MAX_AGE_SECONDS
    assert apply_plan(
        backend,
        plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=plan.safety_token,
    ).status is ApplyStatus.APPLIED

    second = leased_backend()
    second._lease = LeaseSnapshot(
        OWNER,
        second._lease.token_sha256,
        QUIESCENCE_MAX_AGE_SECONDS + 10,
    )
    second._writer_quiescence = WriterQuiescenceProof(
        "e" * 64, 0.0, True, stopped_trigger_ids=("trigger-1",)
    )
    second_plan = build_plan(second.discover())
    second._time = QUIESCENCE_MAX_AGE_SECONDS + 0.001
    with pytest.raises(PreconditionError, match="stale or future-dated"):
        apply_plan(
            second,
            second_plan,
            owner=OWNER,
            lease_token=TOKEN,
            safety_token=second_plan.safety_token,
        )


def test_validate_apply_plan_direct_current_and_noop_guards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = leased_backend()
    first = build_plan(backend.discover())
    assert apply_plan(
        backend,
        first,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=first.safety_token,
    ).status is ApplyStatus.APPLIED
    noop = build_plan(backend.discover())
    assert validate_apply_plan(
        backend,
        noop,
        owner=OWNER,
        lease_token=TOKEN,
    ) == backend.discover()

    planned = leased_backend()
    planned_snapshot = planned.discover()
    planned_plan = build_plan(planned_snapshot)
    planned._active_run_ids = ("newer-run",)
    assert validate_apply_plan(
        planned,
        planned_plan,
        owner=OWNER,
        lease_token=TOKEN,
        current=planned_snapshot,
    ) is planned_snapshot

    drifted = leased_backend()
    drift_plan = build_plan(drifted.discover())
    drifted.replace_table_for_test(
        TableSnapshot.from_spec(TABLE_SPECS[JOURNAL_TABLE])
    )
    drift_snapshot = drifted.discover()
    with pytest.raises(PreconditionError) as drift:
        validate_apply_plan(
            drifted,
            drift_plan,
            owner=OWNER,
            lease_token=TOKEN,
            current=drift_snapshot,
        )
    assert str(drift.value) == (
        "discovery drift: planned "
        f"{drift_plan.discovery.state_sha256}, "
        f"observed {drift_snapshot.state_sha256}"
    )

    incompatible = leased_backend()
    incompatible._active_run_ids = ("run-1",)
    incompatible._active_lease_ids = ("lease-1",)
    incompatible_plan = build_plan(incompatible.discover())
    with pytest.raises(PreconditionError) as blockers:
        validate_apply_plan(
            incompatible,
            incompatible_plan,
            owner=OWNER,
            lease_token=TOKEN,
            current=incompatible.discover(),
        )
    assert str(blockers.value) == (
        "incompatible preconditions: control plane is not quiescent: run-1; "
        "active work leases exist: lease-1"
    )

    no_journal = FakeMigrationBackend.exact(
        lease_owner=OWNER,
        lease_token=TOKEN,
        lease_expires_at=200.0,
        current_time=100.0,
    )
    no_journal_plan = build_plan(no_journal.discover())
    with pytest.raises(PreconditionError) as missing_journal:
        validate_apply_plan(
            no_journal,
            no_journal_plan,
            owner=OWNER,
            lease_token=TOKEN,
            current=no_journal.discover(),
        )
    assert str(missing_journal.value) == (
        "no-op requires a valid successful migration journal: "
        "migration journal is empty"
    )

    no_journal.replace_table_for_test(
        TableSnapshot(
            name="people_counter_ca_work",
            exists=True,
            columns=(ColumnSpec("wrong", "string"),),
            provider="delta",
        )
    )
    no_journal.replace_table_for_test(
        TableSnapshot(
            name="people_counter_ca_locks",
            exists=True,
            columns=(ColumnSpec("also_wrong", "string"),),
            provider="delta",
        )
    )
    inexact = build_plan(no_journal.discover())
    with pytest.raises(PreconditionError) as table_error:
        validate_apply_plan(
            no_journal,
            inexact,
            owner=OWNER,
            lease_token=TOKEN,
            current=no_journal.discover(),
        )
    assert str(table_error.value).startswith(
        "no-op state is not exact: people_counter_ca_locks: metadata differs; "
    )
    assert "; people_counter_ca_work: metadata differs; " in str(
        table_error.value
    )

    exact_no_journal = FakeMigrationBackend.exact(
        lease_owner=OWNER,
        lease_token=TOKEN,
        lease_expires_at=200.0,
        current_time=100.0,
    )
    exact_plan = build_plan(exact_no_journal.discover())
    monkeypatch.setattr(
        "people_counter.fabric_production_migration.verify_backend",
        lambda backend: SimpleNamespace(
            valid=False, journal_errors=("first", "second")
        ),
    )
    with pytest.raises(PreconditionError) as journal_error:
        validate_apply_plan(
            exact_no_journal,
            exact_plan,
            owner=OWNER,
            lease_token=TOKEN,
            current=exact_no_journal.discover(),
        )
    assert str(journal_error.value) == (
        "no-op requires a valid successful migration journal: first; second"
    )


def test_apply_checks_every_write_and_appends_hashed_journal_evidence() -> None:
    backend = leased_backend()
    plan = build_plan(backend.discover())
    result = apply_plan(
        backend,
        plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=plan.safety_token,
    )

    assert result.status is ApplyStatus.APPLIED
    assert result.plan_sha256 == plan.sha256
    assert result.before_sha256 == plan.discovery.sha256
    assert result.after_sha256
    assert len(result.receipts) == len(TABLE_ALLOWLIST)
    assert len(backend.applied_sql) == len(TABLE_ALLOWLIST)
    assert verify_backend(backend).valid
    entries = backend.journal_entries()
    assert len(entries) == 1
    assert entries[0].evidence_sha256 == result.journal_evidence_sha256
    assert len(result.after_sha256) == 64
    assert result.journal_evidence_sha256 is not None
    assert len(result.journal_evidence_sha256) == 64
    assert entries[0].previous_evidence_sha256 == "0" * 64
    for receipt, operation in zip(result.receipts, plan.operations, strict=True):
        assert receipt.operation_id == operation.operation_id
        assert receipt.sql_sha256 == hashlib.sha256(
            render_operation(operation).encode("utf-8")
        ).hexdigest()
        assert receipt.postwrite_table_sha256 == evidence_hash(
            TableSnapshot.from_spec(TABLE_SPECS[operation.table_name]).to_dict()
        )


@pytest.mark.parametrize("age", [0.0, 300.0])
def test_writer_quiescence_proof_accepts_reviewed_age_boundaries(
    age: float,
) -> None:
    backend = FakeMigrationBackend(
        lease_owner=OWNER,
        lease_token=TOKEN,
        lease_expires_at=400.0,
        current_time=age,
        writer_quiescence=WriterQuiescenceProof(
            inventory_sha256="e" * 64,
            captured_at=0.0,
            passed=True,
            stopped_trigger_ids=("trigger-1",),
        ),
    )
    plan = build_plan(backend.discover())

    result = apply_plan(
        backend,
        plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=plan.safety_token,
    )

    assert result.status is ApplyStatus.APPLIED


def test_apply_preserves_exact_preexisting_additive_tables() -> None:
    backend = leased_backend()
    backend.replace_table_for_test(
        TableSnapshot.from_spec(TABLE_SPECS[JOURNAL_TABLE])
    )
    plan = build_plan(backend.discover())

    result = apply_plan(
        backend,
        plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=plan.safety_token,
    )

    assert result.status is ApplyStatus.APPLIED
    assert JOURNAL_TABLE not in {receipt.table_name for receipt in result.receipts}
    assert verify_backend(backend).valid


def test_progress_drift_reports_exact_expected_and_observed_evidence() -> None:
    backend = leased_backend()
    original = backend.apply_operation

    def drift_after_first_write(operation, sql):
        original(operation, sql)
        if operation.table_name == JOURNAL_TABLE:
            backend.replace_table_for_test(
                TableSnapshot(
                    name="people_counter_ca_attempts",
                    exists=True,
                    columns=(ColumnSpec("wrong", "string"),),
                    provider="delta",
                )
            )

    backend.apply_operation = drift_after_first_write  # type: ignore[method-assign]
    plan = build_plan(backend.discover())
    result = apply_plan(
        backend,
        plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=plan.safety_token,
    )

    assert result.status is ApplyStatus.PARTIAL
    assert (
        "migration progress drift at people_counter_ca_attempts; expected="
        in (result.error or "")
    )
    assert ", observed=" in (result.error or "")


def test_lost_write_acknowledgement_is_resolved_by_exact_readback() -> None:
    backend = leased_backend()
    backend.fail_after.add("people_counter_ca_attempts")
    plan = build_plan(backend.discover())
    result = apply_plan(
        backend,
        plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=plan.safety_token,
    )
    assert result.status is ApplyStatus.APPLIED
    assert verify_backend(backend).valid


def test_lost_journal_acknowledgement_is_resolved_by_exact_readback() -> None:
    backend = leased_backend()
    backend.fail_journal_after_append = True
    plan = build_plan(backend.discover())
    result = apply_plan(
        backend,
        plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=plan.safety_token,
    )
    assert result.status is ApplyStatus.APPLIED
    assert len(backend.journal_entries()) == 1
    assert result.journal_evidence_sha256 == backend.journal_entries()[0].evidence_sha256


def test_prewrite_failure_returns_partial_and_journals_observed_evidence() -> None:
    backend = leased_backend()
    backend.fail_before.add("people_counter_ca_attempts")
    plan = build_plan(backend.discover())
    result = apply_plan(
        backend,
        plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=plan.safety_token,
    )
    assert result.status is ApplyStatus.PARTIAL
    assert "PostwriteVerificationError" in (result.error or "")
    assert result.journal_evidence_sha256
    partial_entry = backend.journal_entries()[0]
    assert partial_entry.status is ApplyStatus.PARTIAL
    assert partial_entry.error_text == result.error
    assert result.plan_sha256 == plan.sha256
    assert result.before_sha256 == plan.discovery.sha256
    assert result.after_sha256 == partial_entry.after_sha256
    assert result.after_sha256
    assert backend.discover().table(JOURNAL_TABLE).exists
    assert not backend.discover().table("people_counter_ca_attempts").exists


def test_apply_rechecks_quiescence_and_lease_between_operations() -> None:
    backend = leased_backend()
    original = backend.apply_operation

    def start_run_after_first_write(operation, sql):
        original(operation, sql)
        backend._active_run_ids = ("concurrent-run",)

    backend.apply_operation = start_run_after_first_write  # type: ignore[method-assign]
    plan = build_plan(backend.discover())
    result = apply_plan(
        backend,
        plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=plan.safety_token,
    )
    assert result.status is ApplyStatus.PARTIAL
    assert "not quiescent" in (result.error or "")
    assert len(backend.applied_sql) == 1
    assert backend.journal_entries() == ()


def test_apply_rechecks_independent_writer_proof_between_operations() -> None:
    backend = leased_backend()
    original = backend.apply_operation

    def invalidate_proof_after_first_write(operation, sql):
        original(operation, sql)
        backend._writer_quiescence = WriterQuiescenceProof(
            inventory_sha256="f" * 64,
            captured_at=backend.now(),
            passed=False,
        )

    backend.apply_operation = invalidate_proof_after_first_write  # type: ignore[method-assign]
    plan = build_plan(backend.discover())
    result = apply_plan(
        backend,
        plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=plan.safety_token,
    )

    assert result.status is ApplyStatus.PARTIAL
    assert "writer-quiescence proof did not pass" in (result.error or "")
    assert len(result.receipts) == 1
    assert result.journal_evidence_sha256 is None


def test_failed_write_with_failed_rediscovery_is_ambiguous() -> None:
    backend = leased_backend()
    backend.fail_before.add("people_counter_ca_attempts")
    plan = build_plan(backend.discover())
    # Permit the first exact receipt and the failed operation readback, then
    # make the failure-handler rediscovery ambiguous.
    original = backend.discover
    calls = 0

    def fail_handler_rediscovery():
        nonlocal calls
        calls += 1
        if calls == 6:
            raise OSError("unknown write outcome")
        return original()

    backend.discover = fail_handler_rediscovery  # type: ignore[method-assign]
    result = apply_plan(
        backend,
        plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=plan.safety_token,
    )
    assert result.status is ApplyStatus.AMBIGUOUS
    assert result.plan_sha256 == plan.sha256
    assert result.before_sha256 == plan.discovery.sha256
    assert result.after_sha256 == ""
    assert len(result.receipts) == 1
    assert result.receipts[0].table_name == JOURNAL_TABLE
    assert "rediscovery failed" in (result.error or "")


def test_second_apply_is_idempotent_non_mutating_noop_with_same_lease() -> None:
    backend = leased_backend()
    first_plan = build_plan(backend.discover())
    first = apply_plan(
        backend,
        first_plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=first_plan.safety_token,
    )
    writes = tuple(backend.applied_sql)
    journal = backend.journal_entries()
    second_plan = build_plan(backend.discover())
    second = apply_plan(
        backend,
        second_plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=second_plan.safety_token,
    )
    assert first.status is ApplyStatus.APPLIED
    assert second.status is ApplyStatus.NOOP
    assert second.plan_sha256 == second_plan.sha256
    assert second.before_sha256 == second_plan.discovery.sha256
    assert second.after_sha256 == second_plan.discovery.sha256
    assert second.receipts == ()
    assert tuple(backend.applied_sql) == writes
    assert backend.journal_entries() == journal


def test_noop_refuses_inexact_completed_state() -> None:
    backend = FakeMigrationBackend.exact(
        lease_owner=OWNER,
        lease_token=TOKEN,
        lease_expires_at=200.0,
        current_time=100.0,
    )
    backend.replace_table_for_test(
        TableSnapshot(
            name="people_counter_ca_work",
            exists=True,
            columns=(ColumnSpec("wrong", "string"),),
            provider="delta",
        )
    )
    plan = build_plan(backend.discover())

    with pytest.raises(PreconditionError, match="no-op state is not exact"):
        apply_plan(
            backend,
            plan,
            owner=OWNER,
            lease_token=TOKEN,
            safety_token=plan.safety_token,
        )


def test_verify_detects_exact_metadata_and_journal_tampering() -> None:
    backend = FakeMigrationBackend.exact()
    initial = verify_backend(backend)
    assert not initial.valid
    assert initial.journal_errors == ("migration journal is empty",)
    backend.replace_table_for_test(
        TableSnapshot(
            name="people_counter_ca_work",
            exists=True,
            columns=TABLE_SPECS["people_counter_ca_work"].columns,
            provider="parquet",
            properties=TABLE_SPECS["people_counter_ca_work"].properties,
        )
    )
    report = verify_backend(backend)
    assert not report.valid
    assert report.table_errors[0].startswith("people_counter_ca_work:")


def test_verify_rejects_foreign_migration_journal_identity() -> None:
    backend = FakeMigrationBackend.exact()
    provisional = JournalEntry(
        journal_id="foreign",
        migration_id="other",
        migration_version=MIGRATION_VERSION,
        status=ApplyStatus.APPLIED,
        plan_sha256="a" * 64,
        before_sha256="b" * 64,
        after_sha256="c" * 64,
        receipts_sha256="d" * 64,
        previous_evidence_sha256="0" * 64,
        evidence_sha256="",
    )
    backend._journal = [
        replace(
            provisional,
            evidence_sha256=evidence_hash(provisional.evidence()),
        )
    ]

    report = verify_backend(backend)
    assert not report.valid
    assert report.journal_errors == (
        "wrong migration identity at foreign",
    )


def test_fake_journal_is_append_only_and_hash_validated() -> None:
    backend = leased_backend()
    plan = build_plan(backend.discover())
    apply_plan(
        backend,
        plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=plan.safety_token,
    )
    original = backend.journal_entries()[0]
    backend.append_journal(original)
    assert backend.journal_entries() == (original,)
    invalid = type(original)(
        **{**original.__dict__, "evidence_sha256": "f" * 64}
    )
    with pytest.raises(JournalIntegrityError, match="conflicts"):
        backend.append_journal(invalid)


def test_journal_integrity_requires_identity_chain_uniqueness_and_success() -> None:
    assert journal_integrity_errors((), require_success=False) == ()
    assert journal_integrity_errors((), require_success=True) == (
        "migration journal is empty",
    )

    source = leased_backend()
    plan = build_plan(source.discover())
    apply_plan(
        source,
        plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=plan.safety_token,
    )
    entry = source.journal_entries()[0]
    assert journal_integrity_errors((entry,)) == ()
    duplicate_errors = journal_integrity_errors((entry, entry))
    assert f"duplicate journal_id: {entry.journal_id}" in duplicate_errors
    assert f"broken journal chain at {entry.journal_id}" in duplicate_errors

    partial = replace(entry, status=ApplyStatus.PARTIAL, evidence_sha256="")
    partial = replace(
        partial,
        evidence_sha256=evidence_hash(partial.evidence()),
    )
    assert (
        "migration journal has no successful terminal entry"
        in journal_integrity_errors((partial,))
    )


def test_spark_backend_discovers_exact_metadata_and_appends_only() -> None:
    spark = MagicMock()
    spark.catalog.tableExists.side_effect = lambda name: name == JOURNAL_TABLE
    fields = [
        SimpleNamespace(
            name=column.name,
            dataType=SimpleNamespace(
                simpleString=lambda value=column.data_type: value
            ),
            nullable=column.nullable,
        )
        for column in TABLE_SPECS[JOURNAL_TABLE].columns
    ]
    table = SimpleNamespace(
        schema=SimpleNamespace(fields=fields),
        collect=lambda: [],
    )
    spark.table.return_value = table
    spark.sql.return_value.collect.return_value = [
        {
            "format": "delta",
            "properties": dict(TABLE_SPECS[JOURNAL_TABLE].properties),
        }
    ]
    writer = MagicMock()
    frame = MagicMock()
    frame.write = writer
    writer.format.return_value = writer
    writer.mode.return_value = writer
    spark.createDataFrame.return_value = frame
    backend = SparkMigrationBackend(
        spark,
        artifact_binding=lambda: {
            "workspace_id": WORKSPACE_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "environment_id": ENVIRONMENT_ID,
        },
        clock=lambda: 10.0,
    )

    snapshot = backend.discover()
    assert snapshot.table(JOURNAL_TABLE) == TableSnapshot.from_spec(
        TABLE_SPECS[JOURNAL_TABLE]
    )
    operation = AdditiveOperation(
        OperationKind.CREATE_TABLE_IF_NOT_EXISTS, JOURNAL_TABLE
    )
    backend.apply_operation(operation, render_operation(operation))
    provisional = JournalEntry(
        journal_id="journal-1",
        status=ApplyStatus.PARTIAL,
        plan_sha256="a" * 64,
        before_sha256="b" * 64,
        after_sha256="c" * 64,
        receipts_sha256="d" * 64,
        previous_evidence_sha256="0" * 64,
        evidence_sha256="",
    )
    entry = JournalEntry(
        **{
            **provisional.__dict__,
            "evidence_sha256": evidence_hash(provisional.evidence()),
        }
    )
    backend.append_journal(entry)

    assert backend.now() == 10.0
    spark.sql.assert_any_call(render_operation(operation))
    spark.createDataFrame.assert_called_once()
    writer.format.assert_called_once_with("delta")
    writer.mode.assert_called_once_with("append")
    writer.saveAsTable.assert_called_once_with(JOURNAL_TABLE)


def test_spark_backend_rejects_wrong_fabric_artifact_binding() -> None:
    backend = SparkMigrationBackend(
        MagicMock(),
        artifact_binding=lambda: {
            "workspace_id": "00000000-0000-0000-0000-000000000000",
            "lakehouse_id": LAKEHOUSE_ID,
            "environment_id": ENVIRONMENT_ID,
        },
        clock=lambda: 1.0,
    )

    with pytest.raises(PreconditionError, match="artifact binding mismatch"):
        backend.discover()


def test_spark_backend_reconstructs_and_validates_append_only_journal_chain() -> None:
    source = leased_backend()
    plan = build_plan(source.discover())
    apply_plan(
        source,
        plan,
        owner=OWNER,
        lease_token=TOKEN,
        safety_token=plan.safety_token,
    )
    first = source.journal_entries()[0]
    provisional = JournalEntry(
        journal_id="journal-2",
        status=ApplyStatus.PARTIAL,
        plan_sha256="1" * 64,
        before_sha256="2" * 64,
        after_sha256="3" * 64,
        receipts_sha256="4" * 64,
        previous_evidence_sha256=first.evidence_sha256,
        evidence_sha256="",
        error_text="controlled failure",
    )
    second = JournalEntry(
        **{
            **provisional.__dict__,
            "evidence_sha256": evidence_hash(provisional.evidence()),
        }
    )
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    spark.table.return_value.collect.return_value = [
        second.to_dict(),
        first.to_dict(),
    ]
    backend = SparkMigrationBackend(
        spark,
        artifact_binding=lambda: {
            "workspace_id": WORKSPACE_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "environment_id": ENVIRONMENT_ID,
        },
        clock=lambda: 1.0,
    )
    assert backend.journal_entries() == (first, second)

    spark.table.return_value.collect.return_value = [
        first.to_dict(),
        {**second.to_dict(), "previous_evidence_sha256": "0" * 64},
    ]
    with pytest.raises(JournalIntegrityError, match="one append-only"):
        backend.journal_entries()


def test_rollback_manifest_is_text_only_and_has_no_backend_operation() -> None:
    plan = build_plan(leased_backend().discover())
    assert plan.rollback_manifest.startswith(
        f"Migration: {MIGRATION_ID} version {MIGRATION_VERSION}"
    )
    assert "TEXT-ONLY" in plan.rollback_manifest
    assert all(name in plan.rollback_manifest for name in TABLE_ALLOWLIST)
    assert all(
        operation.kind is OperationKind.CREATE_TABLE_IF_NOT_EXISTS
        for operation in plan.operations
    )


@pytest.mark.parametrize("command", ["plan", "apply", "verify", "status"])
def test_cli_defaults_are_non_mutating(command: str) -> None:
    backend = leased_backend()
    output = io.StringIO()
    exit_code = main([command], backend=backend, stdout=output)
    assert backend.applied_sql == []
    if command in {"plan", "apply"}:
        assert json.loads(output.getvalue())["operations"]
        assert exit_code == 0
    elif command == "verify":
        assert exit_code == 1
    else:
        assert exit_code == 0


def test_cli_apply_requires_explicit_execute_owner_and_token() -> None:
    backend = leased_backend()
    errors = io.StringIO()
    assert (
        main(
            ["apply", "--execute"],
            backend=backend,
            stdout=io.StringIO(),
            stderr=errors,
        )
        == 2
    )
    assert "--owner, --lease-token, and --safety-token" in errors.getvalue()
    assert backend.applied_sql == []

    plan = build_plan(backend.discover())
    output = io.StringIO()
    assert (
        main(
            [
                "apply",
                "--execute",
                "--owner",
                OWNER,
                "--lease-token",
                TOKEN,
                "--safety-token",
                plan.safety_token,
            ],
            backend=backend,
            stdout=output,
        )
        == 0
    )
    assert json.loads(output.getvalue())["status"] == "applied"
