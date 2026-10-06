from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from people_counter.fabric_production_migration import (
    ApplyStatus as MigrationApplyStatus,
    FakeMigrationBackend,
    JournalEntry as MigrationJournalEntry,
    evidence_hash as migration_evidence_hash,
)
from people_counter.fabric_production_routing import (
    ENVIRONMENT_ID,
    LAKEHOUSE_ID,
    MIGRATION_ID,
    MIGRATION_VERSION,
    SHADOW_FILES_ROOT,
    SHADOW_TABLE_PREFIX,
    WORKSPACE_ID,
    AllowlistRow,
    CommittedSnapshot,
    ComparisonTolerance,
    DeploymentGate,
    ExplicitClaimRequest,
    JournalEntry,
    QuiescenceProof,
    RoutingValidationError,
    ShadowPlan,
    ShadowWrite,
    WorkManifest,
    authorize_apply,
    claim_request,
    compare_committed_routes,
    shadow_file,
    shadow_table,
    validate_journal,
    validate_observed_allowlist,
    validate_quiescence,
    validate_shadow_writes,
)


NOW = datetime(2026, 10, 4, 14, 0, tzinfo=timezone.utc)
MIGRATION_PLAN_SHA256 = "9" * 64


def allowlist_row(work_id: str = "work-001", seed: str = "a") -> AllowlistRow:
    hashes = [
        format((int(seed, 16) + offset) % 16, "x") * 64
        for offset in range(5)
    ]
    return AllowlistRow(
        work_id=work_id,
        camera_sha256=hashes[0],
        location_sha256=hashes[1],
        model_sha256=hashes[2],
        source_sha256=hashes[3],
        config_sha256=hashes[4],
    )


def manifest(*rows: AllowlistRow) -> WorkManifest:
    return WorkManifest(
        manifest_id="manifest-2026-10-04",
        rows=rows or (allowlist_row(),),
    )


def plan(
    *,
    rows: tuple[AllowlistRow, ...] | None = None,
    tolerances: tuple[ComparisonTolerance, ...] = (),
) -> ShadowPlan:
    return ShadowPlan(
        gate=DeploymentGate(),
        migration_id=MIGRATION_ID,
        manifest=manifest(*(rows or (allowlist_row(),))),
        migration_plan_sha256=MIGRATION_PLAN_SHA256,
        tolerances=tolerances,
    )


def migration_backend(
    plan_sha256: str = MIGRATION_PLAN_SHA256,
) -> FakeMigrationBackend:
    backend = FakeMigrationBackend.exact()
    provisional = MigrationJournalEntry(
        journal_id="migration-proof",
        status=MigrationApplyStatus.APPLIED,
        plan_sha256=plan_sha256,
        before_sha256="1" * 64,
        after_sha256="2" * 64,
        receipts_sha256="3" * 64,
        previous_evidence_sha256="0" * 64,
        evidence_sha256="",
    )
    entry = replace(
        provisional,
        evidence_sha256=migration_evidence_hash(provisional.evidence()),
    )
    backend._journal = [entry]
    return backend


def journal_for(value: ShadowPlan) -> tuple[JournalEntry, ...]:
    return (
        JournalEntry.append(
            value,
            (),
            event="PLANNED",
            recorded_at=NOW - timedelta(minutes=2),
            details={"reviewer": "release-control"},
        ),
    )


def proof_for(
    value: ShadowPlan,
    journal: tuple[JournalEntry, ...],
) -> QuiescenceProof:
    return QuiescenceProof(
        migration_id=value.migration_id,
        plan_sha256=value.sha256,
        journal_head_sha256=journal[-1].sha256 if journal else "0" * 64,
        observed_at=NOW - timedelta(seconds=30),
    )


def snapshot(
    *,
    route: str,
    work_id: str = "work-001",
    output: dict[str, object] | None = None,
    provenance: dict[str, object] | None = None,
    metrics: dict[str, object] | None = None,
) -> CommittedSnapshot:
    allowed_provenance = {
        "camera_sha256": "a" * 64,
        "location_sha256": "b" * 64,
        "model_sha256": "c" * 64,
        "source_sha256": "d" * 64,
        "config_sha256": "e" * 64,
    }
    return CommittedSnapshot(
        work_id=work_id,
        attempt_id=f"{route}-attempt-1",
        output=output or {"status": "SUCCEEDED", "line_counts": [1, 3]},
        provenance=provenance or allowed_provenance,
        metrics=metrics or {"processing_seconds": 10.0, "frames": 300},
    )


def test_exact_target_and_migration_gates_are_non_overridable() -> None:
    gate = DeploymentGate()
    assert gate.as_dict() == {
        "workspace_id": WORKSPACE_ID,
        "lakehouse_id": LAKEHOUSE_ID,
        "environment_id": ENVIRONMENT_ID,
        "migration_version": MIGRATION_VERSION,
    }
    for field, bad_value in (
        ("workspace_id", "be6bd95d-45ff-462c-98c1-63e43da16d85"),
        ("lakehouse_id", "be6bd95d-45ff-462c-98c1-63e43da16d85"),
        ("environment_id", "be6bd95d-45ff-462c-98c1-63e43da16d85"),
        ("migration_version", "pc_ca_prod_shadow_v2"),
    ):
        with pytest.raises(RoutingValidationError, match=field):
            replace(gate, **{field: bad_value})


def test_manifest_is_explicit_sorted_unique_and_hash_bound() -> None:
    first = allowlist_row("work-001", "a")
    second = allowlist_row("work-002", "f")
    value = manifest(first, second)
    request = claim_request(
        ShadowPlan(
            DeploymentGate(), MIGRATION_ID, value, MIGRATION_PLAN_SHA256
        )
    )
    assert request.work_ids == ("work-001", "work-002")
    assert request.allowlist_rows == (first, second)
    assert request.manifest_sha256 == value.sha256
    validate_observed_allowlist(request, (first, second))

    with pytest.raises(RoutingValidationError, match="sorted"):
        manifest(second, first)
    with pytest.raises(RoutingValidationError, match="unique"):
        manifest(first, first)
    with pytest.raises(RoutingValidationError, match="explicit work rows"):
        WorkManifest("manifest-empty", ())
    with pytest.raises(RoutingValidationError, match="work_id"):
        replace(first, work_id="*")
    with pytest.raises(RoutingValidationError, match="64 lowercase"):
        replace(first, source_sha256="A" * 64)
    with pytest.raises(RoutingValidationError, match="differ"):
        validate_observed_allowlist(request, (replace(first, source_sha256="f" * 64), second))
    forged = replace(request, manifest_sha256="0" * 64)
    with pytest.raises(
        RoutingValidationError, match="allowlist manifest digest mismatch"
    ):
        validate_observed_allowlist(forged, request.allowlist_rows)


def test_claim_request_has_no_claim_all_or_predicate_surface() -> None:
    row = allowlist_row()
    request = claim_request(plan())
    assert request.work_ids == (row.work_id,)
    assert not hasattr(request, "claim_all")
    assert not hasattr(request, "predicate")
    with pytest.raises(RoutingValidationError, match="explicit work_id"):
        replace(request, work_ids=())
    with pytest.raises(RoutingValidationError, match="exactly match"):
        ExplicitClaimRequest(
            migration_id=MIGRATION_ID,
            manifest_id=request.manifest_id,
            manifest_sha256=request.manifest_sha256,
            work_ids=("work-other",),
            allowlist_rows=request.allowlist_rows,
        )


def test_safety_token_binds_migration_id_and_canonical_plan_hash() -> None:
    value = plan()
    assert value.migration_id in value.safety_token
    assert value.sha256 in value.safety_token
    assert value.as_dict()["semantic_refresh"] is False
    value.require_safety_token(value.safety_token)

    changed = plan(
        tolerances=(
            ComparisonTolerance("metrics", "processing_seconds", absolute=0.5),
        )
    )
    assert changed.sha256 != value.sha256
    with pytest.raises(RoutingValidationError, match="canonical plan hash"):
        changed.require_safety_token(value.safety_token)
    with pytest.raises(RoutingValidationError, match="canonical plan hash"):
        value.require_safety_token(value.safety_token.replace(MIGRATION_ID, MIGRATION_ID[:-1] + "e"))


def test_journal_is_plan_compatible_contiguous_and_hash_chained() -> None:
    value = plan()
    mutable_details = {"reviewer": "release-control", "nested": {"count": 1}}
    first = JournalEntry.append(
        value,
        (),
        event="PLANNED",
        recorded_at=NOW - timedelta(minutes=2),
        details=mutable_details,
    )
    stable_hash = first.sha256
    mutable_details["nested"]["count"] = 2
    assert first.sha256 == stable_hash
    second = JournalEntry.append(
        value,
        (first,),
        event="QUIESCENCE_VERIFIED",
        recorded_at=NOW - timedelta(minutes=1),
        details={"active_count": 0},
    )
    assert validate_journal(value, (first, second)) == (first, second)
    assert second.previous_sha256 == first.sha256

    with pytest.raises(RoutingValidationError, match="hash chain"):
        validate_journal(value, (first, replace(second, previous_sha256="f" * 64)))
    with pytest.raises(RoutingValidationError, match="contiguous"):
        validate_journal(value, (replace(first, sequence=1),))
    with pytest.raises(RoutingValidationError, match="incompatible"):
        validate_journal(
            plan(
                tolerances=(
                    ComparisonTolerance("metrics", "frames", absolute=1),
                )
            ),
            (first,),
        )
    rollback = JournalEntry.append(
        value,
        (first,),
        event="ROLLED_BACK",
        recorded_at=NOW,
    )
    after = replace(
        second,
        previous_sha256=rollback.sha256,
        sequence=2,
        recorded_at=NOW + timedelta(seconds=1),
    )
    with pytest.raises(RoutingValidationError, match="after rollback"):
        validate_journal(value, (first, rollback, after))
    with pytest.raises(RoutingValidationError, match="begin with PLANNED"):
        JournalEntry.append(
            value,
            (),
            event="APPLY_AUTHORIZED",
            recorded_at=NOW,
        )


def test_apply_requires_fresh_blocker_free_proof_at_current_journal_head() -> None:
    value = plan()
    journal = journal_for(value)
    proof = proof_for(value, journal)
    validate_quiescence(value, proof, journal, applied_at=NOW)
    authorization = authorize_apply(
        value,
        work_ids=value.manifest.work_ids,
        observed_allowlist=value.manifest.rows,
        safety_token=value.safety_token,
        proof=proof,
        journal=journal,
        migration_backend=migration_backend(),
        applied_at=NOW,
    )
    assert authorization.claim.work_ids == value.manifest.work_ids
    assert authorization.table_prefix == SHADOW_TABLE_PREFIX
    assert authorization.files_root == SHADOW_FILES_ROOT
    assert authorization.semantic_refresh is False
    assert authorization.migration_id == value.migration_id
    assert authorization.plan_sha256 == value.sha256
    assert authorization.journal_entry.event == "APPLY_AUTHORIZED"
    assert authorization.journal_entry.previous_sha256 == journal[-1].sha256
    assert dict(authorization.journal_entry.details) == {
        "manifest_id": value.manifest.manifest_id,
        "work_ids": value.manifest.work_ids,
        "quiescence_observed_at": proof.observed_at.isoformat(),
        "semantic_refresh": False,
    }

    with pytest.raises(RoutingValidationError, match="explicit --work-id"):
        authorize_apply(
            value,
            work_ids=(),
            observed_allowlist=value.manifest.rows,
            safety_token=value.safety_token,
            proof=proof,
            journal=journal,
            migration_backend=migration_backend(),
            applied_at=NOW,
        )
    with pytest.raises(RoutingValidationError, match="EXACTLY ONE"):
        authorize_apply(
            value,
            work_ids=("work-001", "work-001"),
            observed_allowlist=value.manifest.rows,
            safety_token=value.safety_token,
            proof=proof,
            journal=journal,
            migration_backend=migration_backend(),
            applied_at=NOW,
        )
    with pytest.raises(RoutingValidationError, match="observed work identities"):
        authorize_apply(
            value,
            work_ids=value.manifest.work_ids,
            observed_allowlist=(
                replace(value.manifest.rows[0], source_sha256="f" * 64),
            ),
            safety_token=value.safety_token,
            proof=proof,
            journal=journal,
            migration_backend=migration_backend(),
            applied_at=NOW,
        )

    blockers = {
        "enabled_writer_ids": ("dispatcher-00",),
        "active_work_ids": ("work-active",),
        "unreceipted_event_count": 1,
        "control_writer_owner_id": "owner-1",
        "live_writer_session_ids": ("spark-1",),
    }
    for field, blocker in blockers.items():
        with pytest.raises(RoutingValidationError, match="blockers"):
            validate_quiescence(
                value,
                replace(proof, **{field: blocker}),
                journal,
                applied_at=NOW,
            )
    with pytest.raises(RoutingValidationError, match="not fresh"):
        validate_quiescence(
            value,
            replace(proof, observed_at=NOW - timedelta(minutes=6)),
            journal,
            applied_at=NOW,
        )
    with pytest.raises(RoutingValidationError, match="journal head is stale"):
        validate_quiescence(
            value,
            replace(proof, journal_head_sha256="f" * 64),
            journal,
            applied_at=NOW,
        )
    with pytest.raises(RoutingValidationError, match="PLANNED journal"):
        authorize_apply(
            value,
            work_ids=value.manifest.work_ids,
            observed_allowlist=value.manifest.rows,
            safety_token=value.safety_token,
            proof=replace(proof, journal_head_sha256="0" * 64),
            journal=(),
            migration_backend=migration_backend(),
            applied_at=NOW,
        )
    with pytest.raises(
        RoutingValidationError, match="verified successful additive migration"
    ):
        authorize_apply(
            value,
            work_ids=value.manifest.work_ids,
            observed_allowlist=value.manifest.rows,
            safety_token=value.safety_token,
            proof=proof,
            journal=journal,
            migration_backend=FakeMigrationBackend.exact(),
            applied_at=NOW,
        )
    with pytest.raises(
        RoutingValidationError, match="plan hash differs"
    ):
        authorize_apply(
            value,
            work_ids=value.manifest.work_ids,
            observed_allowlist=value.manifest.rows,
            safety_token=value.safety_token,
            proof=proof,
            journal=journal,
            migration_backend=migration_backend("7" * 64),
            applied_at=NOW,
        )

    quiescence_entry = JournalEntry.append(
        value,
        journal,
        event="QUIESCENCE_VERIFIED",
        recorded_at=NOW - timedelta(minutes=1),
        details={"active_count": 0},
    )
    quiescent_journal = journal + (quiescence_entry,)
    quiescent_proof = replace(
        proof, journal_head_sha256=quiescence_entry.sha256
    )
    from_quiescence = authorize_apply(
        value,
        work_ids=value.manifest.work_ids,
        observed_allowlist=value.manifest.rows,
        safety_token=value.safety_token,
        proof=quiescent_proof,
        journal=quiescent_journal,
        migration_backend=migration_backend(),
        applied_at=NOW,
    )
    assert from_quiescence.journal_entry.previous_sha256 == quiescence_entry.sha256

    rolled_back = JournalEntry.append(
        value,
        journal,
        event="ROLLED_BACK",
        recorded_at=NOW - timedelta(seconds=10),
    )
    with pytest.raises(RoutingValidationError, match="not in an apply-authorizable"):
        authorize_apply(
            value,
            work_ids=value.manifest.work_ids,
            observed_allowlist=value.manifest.rows,
            safety_token=value.safety_token,
            proof=replace(proof, journal_head_sha256=rolled_back.sha256),
            journal=journal + (rolled_back,),
            migration_backend=migration_backend(),
            applied_at=NOW,
        )


def test_apply_cannot_be_authorized_twice_or_with_wrong_token() -> None:
    value = plan()
    journal = journal_for(value)
    proof = proof_for(value, journal)
    authorization = authorize_apply(
        value,
        work_ids=value.manifest.work_ids,
        observed_allowlist=value.manifest.rows,
        safety_token=value.safety_token,
        proof=proof,
        journal=journal,
        migration_backend=migration_backend(),
        applied_at=NOW,
    )
    advanced = journal + (authorization.journal_entry,)
    with pytest.raises(RoutingValidationError, match="already been authorized"):
        authorize_apply(
            value,
            work_ids=value.manifest.work_ids,
            observed_allowlist=value.manifest.rows,
            safety_token=value.safety_token,
            proof=replace(proof, journal_head_sha256=advanced[-1].sha256),
            journal=advanced,
            migration_backend=migration_backend(),
            applied_at=NOW,
        )
    with pytest.raises(RoutingValidationError, match="safety token"):
        authorize_apply(
            value,
            work_ids=value.manifest.work_ids,
            observed_allowlist=value.manifest.rows,
            safety_token="operator-approved",
            proof=proof,
            journal=journal,
            migration_backend=migration_backend(),
            applied_at=NOW,
        )


def test_shadow_targets_are_exact_and_shadow_publication_is_permitted() -> None:
    assert shadow_table("attempt_outputs") == (
        "pc_ca_prod_shadow_v1_attempt_outputs"
    )
    assert shadow_file("work=work-001/attempt=1/result.json") == (
        "Files/_shadow/people-counter/candidate-a/v1/"
        "work=work-001/attempt=1/result.json"
    )
    assert shadow_table("publications") == (
        "pc_ca_prod_shadow_v1_publications"
    )
    assert shadow_file(
        "attempts/work=work-001/attempt=one/pointer.json"
    ) == (
        "Files/_shadow/people-counter/candidate-a/v1/"
        "attempts/work=work-001/attempt=one/pointer.json"
    )
    with pytest.raises(RoutingValidationError, match="unsafe shadow table suffix"):
        shadow_table(None)  # type: ignore[arg-type]
    for path in (
        "../Tables/production",
        "/Files/_shadow/x",
        "abfss://other/path",
        "work=work-001//result.json",
    ):
        with pytest.raises(RoutingValidationError):
            shadow_file(path)

    writes = (
        ShadowWrite("TABLE", shadow_table("attempt_outputs"), "work-001"),
        ShadowWrite(
            "FILE",
            shadow_file("work=work-001/attempt=1/result.json"),
            "work-001",
        ),
    )
    assert validate_shadow_writes(plan(), writes) == writes
    with pytest.raises(RoutingValidationError, match="outside the shadow prefix"):
        ShadowWrite("TABLE", "people_counter_publications", "work-001")
    with pytest.raises(RoutingValidationError, match="unsafe shadow table suffix"):
        ShadowWrite(
            "TABLE",
            SHADOW_TABLE_PREFIX + "invalid-suffix",
            "work-001",
        )
    with pytest.raises(RoutingValidationError, match="outside the shadow root"):
        ShadowWrite("FILE", "Files/production/pointer.json", "work-001")
    with pytest.raises(RoutingValidationError, match="non-allowlisted"):
        validate_shadow_writes(
            plan(),
            (ShadowWrite("TABLE", shadow_table("attempt_outputs"), "work-999"),),
        )
    with pytest.raises(
        RoutingValidationError, match="apply requires at least one shadow write"
    ):
        validate_shadow_writes(plan(), ())


def test_committed_comparison_matches_with_section_specific_tolerances() -> None:
    value = plan(
        tolerances=(
            ComparisonTolerance("metrics", "processing_seconds", absolute=0.25),
            ComparisonTolerance("output", "confidence", relative=0.05),
            ComparisonTolerance("provenance", "clock_skew_ms", absolute=5.0),
        )
    )
    legacy = snapshot(
        route="legacy",
        output={"status": "SUCCEEDED", "confidence": 0.8},
        provenance={
            **snapshot(route="base").provenance,
            "clock_skew_ms": 10.0,
        },
        metrics={"processing_seconds": 10.0, "frames": 300},
    )
    shadow = snapshot(
        route="shadow",
        output={"status": "SUCCEEDED", "confidence": 0.82},
        provenance={
            **snapshot(route="base").provenance,
            "clock_skew_ms": 14.0,
        },
        metrics={"processing_seconds": 10.2, "frames": 300},
    )
    report = compare_committed_routes(value, legacy=(legacy,), shadow=(shadow,))
    assert report.matched
    assert report.compared_work_ids == ("work-001",)
    assert report.findings == ()
    assert report.semantic_refresh is False


def test_comparison_emits_deterministic_output_provenance_and_metric_findings() -> None:
    value = plan(
        tolerances=(
            ComparisonTolerance("metrics", "processing_seconds", absolute=0.1),
        )
    )
    legacy = snapshot(
        route="legacy",
        output={"status": "SUCCEEDED", "count": 2},
        provenance={
            **snapshot(route="base").provenance,
            "executor": "legacy",
        },
        metrics={"processing_seconds": 10.0, "frames": 300},
    )
    shadow = snapshot(
        route="shadow",
        output={"status": "FAILED", "count": 3},
        provenance={
            **snapshot(route="base").provenance,
            "source_sha256": "e" * 64,
        },
        metrics={"processing_seconds": 10.5, "frames": 299},
    )
    report = compare_committed_routes(value, legacy=(legacy,), shadow=(shadow,))
    assert not report.matched
    assert {finding.section for finding in report.findings} == {
        "output",
        "provenance",
        "metrics",
    }
    assert {finding.finding_type for finding in report.findings} == {
        "ALLOWLIST_PROVENANCE_MISMATCH",
        "OUT_OF_TOLERANCE",
        "VALUE_MISMATCH",
        "MISSING_FIELD",
    }
    processing = next(
        finding
        for finding in report.findings
        if finding.field == "processing_seconds"
    )
    assert processing.absolute_delta == 0.5
    assert processing.allowed_delta == 0.1
    rerun = compare_committed_routes(value, legacy=(legacy,), shadow=(shadow,))
    assert [item.finding_id for item in rerun.findings] == [
        item.finding_id for item in report.findings
    ]


def test_comparison_reports_missing_committed_snapshot_and_rejects_unsafe_input() -> None:
    value = plan()
    legacy = snapshot(route="legacy")
    report = compare_committed_routes(value, legacy=(legacy,), shadow=())
    assert len(report.findings) == 1
    assert report.findings[0].finding_type == "MISSING_COMMITTED_SNAPSHOT"
    assert report.compared_work_ids == ()

    with pytest.raises(RoutingValidationError, match="only committed"):
        replace(legacy, committed=False)
    with pytest.raises(RoutingValidationError, match="multiple committed"):
        compare_committed_routes(
            value,
            legacy=(legacy, replace(legacy, attempt_id="legacy-attempt-2")),
            shadow=(),
        )
    with pytest.raises(RoutingValidationError, match="non-manifest"):
        compare_committed_routes(
            value,
            legacy=(replace(legacy, work_id="work-unapproved"),),
            shadow=(),
        )


def test_tolerances_are_finite_nonnegative_unique_and_canonical() -> None:
    with pytest.raises(RoutingValidationError, match="nonnegative"):
        ComparisonTolerance("metrics", "duration", absolute=-0.1)
    with pytest.raises(RoutingValidationError, match="finite"):
        ComparisonTolerance("metrics", "duration", relative=float("inf"))
    with pytest.raises(RoutingValidationError, match="section"):
        ComparisonTolerance("unknown", "duration")
    duplicate = ComparisonTolerance("metrics", "duration")
    with pytest.raises(RoutingValidationError, match="duplicate"):
        plan(tolerances=(duplicate, duplicate))
    with pytest.raises(RoutingValidationError, match="sorted"):
        plan(
            tolerances=(
                ComparisonTolerance("provenance", "source"),
                ComparisonTolerance("metrics", "duration"),
            )
        )
