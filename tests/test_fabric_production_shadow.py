from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

import people_counter.fabric_production_shadow as shadow
from people_counter.fabric_candidate_a import (
    PRODUCTION_SHADOW_FILES_ROOT,
    PRODUCTION_SHADOW_TABLE_PREFIX,
    FabricCandidateAConfig,
)
from people_counter.fabric_production_migration import MIGRATION_ID
from people_counter.fabric_production_routing import AllowlistRow
from people_counter.fabric_production_routing import sha256_json
from people_counter.fabric_production_shadow import (
    PACKAGE_VERSION,
    PRODUCTION_ALLOWLIST_TABLE,
    PRODUCTION_AUDIT_TABLE,
    SHADOW_PUBLICATIONS_TABLE,
    SHADOW_SCHEMAS,
    AuthorizationResult,
    AuthorizationIntent,
    AuthorizationProtocolState,
    ClassifiedTarget,
    CommittedRoute,
    LegacySourceRows,
    MemoryAuthorizationStore,
    MigrationJournalProof,
    ProductionShadowError,
    ProductionShadowPlan,
    ReviewReceipt,
    RouteTolerance,
    RuntimeProvenance,
    ShadowQuiescence,
    SyntheticShadowPlan,
    TargetClass,
    WriteOperation,
    authorize_production_shadow,
    authorization_intent,
    classify_authorization_protocol,
    bootstrap_production_shadow,
    classify_target,
    compare_and_reconcile,
    execute_production_shadow,
    execute_synthetic_shadow,
    ingest_legacy_source,
    prove_legacy_unchanged,
    require_plan_pinned_legacy,
    reconcile_main,
    require_write_target,
)


NOW = datetime(2026, 10, 5, 4, 0, tzinfo=timezone.utc)
REFLEX_ID = "d68e9aa1-5a1e-4e46-ad2a-30b446dbb490"


def identity() -> AllowlistRow:
    return AllowlistRow(
        work_id="work-001",
        camera_sha256="a" * 64,
        location_sha256="b" * 64,
        model_sha256="c" * 64,
        source_sha256="d" * 64,
        config_sha256="e" * 64,
    )


def plan() -> ProductionShadowPlan:
    return ProductionShadowPlan(
        plan_id="shadow-001",
        work=identity(),
        migration_plan_sha256="9" * 64,
        created_at=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(minutes=10),
    )


def receipt(value: ProductionShadowPlan) -> ReviewReceipt:
    return ReviewReceipt(
        plan_sha256=value.sha256,
        work_id=value.work.work_id,
        work_identity_sha256=value.identity_sha256,
        expires_at=value.expires_at,
        reviewed_at=NOW - timedelta(seconds=30),
        reviewer="release-control",
    )


def migration() -> MigrationJournalProof:
    return MigrationJournalProof(
        migration_id=MIGRATION_ID,
        status="APPLIED",
        plan_sha256="9" * 64,
        journal_sha256="8" * 64,
    )


def quiescence(**overrides: object) -> ShadowQuiescence:
    values: dict[str, object] = {
        "observed_at": NOW - timedelta(seconds=20),
        "reflex_id": REFLEX_ID,
        "reflex_active": False,
    }
    values.update(overrides)
    return ShadowQuiescence(**values)  # type: ignore[arg-type]


def authorize(
    store: MemoryAuthorizationStore | None = None,
) -> tuple[ProductionShadowPlan, MemoryAuthorizationStore, AuthorizationResult]:
    value = plan()
    backend = store or MemoryAuthorizationStore()
    result = authorize_production_shadow(
        value,
        selected_work_ids=("work-001",),
        observed_work=identity(),
        safety_token=value.safety_token,
        receipt=receipt(value),
        receipt_mode=0o600,
        migration=migration(),
        quiescence=quiescence(),
        expected_reflex_id=REFLEX_ID,
        store=backend,
        now=NOW,
    )
    return value, backend, result


def legacy_rows() -> LegacySourceRows:
    shared = {
        "work_id": "work-001",
        "attempt_id": "legacy-attempt-001",
        "camera_sha256": "a" * 64,
        "location_sha256": "b" * 64,
        "model_sha256": "c" * 64,
        "source_sha256": "d" * 64,
        "config_sha256": "e" * 64,
        "output_path": "Files/people-counter/candidate-a/v1/attempts/legacy.json",
        "output_sha256": "f" * 64,
    }
    return LegacySourceRows(
        work={
            **shared,
            "status": "SUCCEEDED",
            "committed_attempt_id": "legacy-attempt-001",
            "payload": {"source_video": "/lakehouse/default/Files/input.mp4"},
        },
        attempt={**shared, "status": "SUCCEEDED"},
        publication={**shared, "publication_sequence": 7},
        committed_view={**shared, "visible": True},
    )


def pinned_plan(source=None) -> ProductionShadowPlan:
    selected = source or ingest_legacy_source(legacy_rows())
    return ProductionShadowPlan(
        plan_id="shadow-pinned-001",
        work=selected.identity,
        migration_plan_sha256="9" * 64,
        created_at=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(minutes=10),
        legacy_source_rows_sha256=selected.source_rows_sha256,
        legacy_output_sha256=selected.output_sha256,
    )


def test_plan_pinned_legacy_requires_exact_route_and_all_hashes() -> None:
    source = ingest_legacy_source(legacy_rows())
    value = pinned_plan(source)

    assert require_plan_pinned_legacy(value, source) is source

    for changed in (
        replace(value, work=replace(value.work, model_sha256="0" * 64)),
        replace(value, legacy_source_rows_sha256="0" * 64),
        replace(value, legacy_output_sha256="0" * 64),
    ):
        with pytest.raises(ProductionShadowError, match="plan-pinned"):
            require_plan_pinned_legacy(changed, source)
    other_rows = legacy_rows()
    other_rows = LegacySourceRows(
        {**other_rows.work, "work_id": "other-work"},
        {**other_rows.attempt, "work_id": "other-work"},
        {**other_rows.publication, "work_id": "other-work"},
        {**other_rows.committed_view, "work_id": "other-work"},
    )
    other = ingest_legacy_source(other_rows)
    with pytest.raises(ProductionShadowError, match="plan-pinned"):
        require_plan_pinned_legacy(value, other)


def provenance() -> RuntimeProvenance:
    return RuntimeProvenance(
        package_version=PACKAGE_VERSION,
        package_sha256="1" * 64,
        python_version="3.13.7",
        spark_version="4.1.1",
        java_version="21.0.8",
    )


def route(
    *,
    shadow: bool,
    authorization_id: str,
    plan_sha256: str,
    **overrides: object,
) -> CommittedRoute:
    path = (
        f"{PRODUCTION_SHADOW_FILES_ROOT}outputs/work=work-001/result.json"
        if shadow
        else "Files/people-counter/candidate-a/v1/outputs/result.json"
    )
    values: dict[str, object] = {
        "work_id": "work-001",
        "attempt_id": "attempt-shadow" if shadow else "attempt-legacy",
        "logical_identity_sha256": "2" * 64,
        "fence": 4,
        "pointer_fence": 4,
        "output_path": path,
        "output_sha256": "3" * 64,
        "sealed": True,
        "committed": True,
        "pointer_attempt_id": "attempt-shadow" if shadow else "attempt-legacy",
        "publication_sequence": 9 if shadow else 7,
        "publication_count": 1,
        "authorization_id": authorization_id,
        "plan_sha256": plan_sha256,
        "provenance": provenance(),
        "identity": identity(),
        "records": ({"frame": 1, "count": 2.0},),
        "logical_total": 2.0,
        "frame_count": 100,
        "timestamp": NOW,
    }
    values.update(overrides)
    return CommittedRoute(**values)  # type: ignore[arg-type]


def test_plan_is_exactly_one_expiring_and_token_binds_identity_and_expiry() -> None:
    value = plan()
    assert value.work_ids == ("work-001",)
    assert value.as_dict() == {
        "schema_version": 1,
        "plan_id": "shadow-001",
        "migration_id": MIGRATION_ID,
        "migration_plan_sha256": "9" * 64,
        "work": identity().as_dict(),
        "work_identity_sha256": sha256_json(identity().as_dict()),
        "work_ids": ["work-001"],
        "legacy_source_rows_sha256": None,
        "legacy_output_sha256": None,
        "created_at": (NOW - timedelta(minutes=1)).isoformat(),
        "expires_at": (NOW + timedelta(minutes=10)).isoformat(),
        "table_prefix": PRODUCTION_SHADOW_TABLE_PREFIX,
        "files_root": PRODUCTION_SHADOW_FILES_ROOT,
    }
    assert value.safety_token == plan().safety_token
    with pytest.raises(ProductionShadowError, match="token differs"):
        value.require_token(value.safety_token[:-1] + "0")
    with pytest.raises(ProductionShadowError, match="15 minutes"):
        replace(value, expires_at=value.created_at + timedelta(minutes=16))
    with pytest.raises(ProductionShadowError, match="not currently valid"):
        value.validate_at(value.expires_at)
    with pytest.raises(ProductionShadowError, match="not currently valid"):
        value.validate_at(value.created_at - timedelta(microseconds=1))


def test_review_receipt_exact_envelope_and_binding() -> None:
    value = plan()
    reviewed = receipt(value)
    assert reviewed.as_dict() == {
        "schema_version": 1,
        "plan_sha256": value.sha256,
        "work_id": "work-001",
        "work_identity_sha256": value.identity_sha256,
        "expires_at": value.expires_at.isoformat(),
        "reviewed_at": (NOW - timedelta(seconds=30)).isoformat(),
        "reviewer": "release-control",
    }
    for changed in (
        replace(reviewed, plan_sha256="0" * 64),
        replace(reviewed, work_id="work-002"),
        replace(reviewed, work_identity_sha256="0" * 64),
        replace(reviewed, expires_at=reviewed.expires_at - timedelta(seconds=1)),
    ):
        with pytest.raises(ProductionShadowError, match="receipt differs"):
            changed.validate(value, now=NOW, file_mode=0o600)
    with pytest.raises(ProductionShadowError, match="timestamp is invalid"):
        replace(reviewed, reviewed_at=value.created_at - timedelta(seconds=1)).validate(
            value, now=NOW, file_mode=0o600
        )


@pytest.mark.parametrize("selected", [(), ("work-001", "work-002")])
def test_authorization_requires_exactly_one_work(selected: tuple[str, ...]) -> None:
    value = plan()
    with pytest.raises(ProductionShadowError, match="EXACTLY ONE"):
        authorize_production_shadow(
            value,
            selected_work_ids=selected,
            observed_work=identity(),
            safety_token=value.safety_token,
            receipt=receipt(value),
            receipt_mode=0o600,
            migration=migration(),
            quiescence=quiescence(),
            expected_reflex_id=REFLEX_ID,
            store=MemoryAuthorizationStore(),
            now=NOW + timedelta(seconds=1),
        )


def test_exact_authorization_selection_accepts_only_pinned_identity() -> None:
    value = plan()

    shadow._require_exact_authorization_selection(
        value, ("work-001",), identity()
    )
    with pytest.raises(ProductionShadowError, match="selected work ID"):
        shadow._require_exact_authorization_selection(
            value, ("work-other",), identity()
        )
    with pytest.raises(ProductionShadowError, match="identity hashes"):
        shadow._require_exact_authorization_selection(
            value,
            ("work-001",),
            replace(identity(), source_sha256="0" * 64),
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("receipt_mode", 0o644, "mode 0600"),
        ("safety_token", "wrong", "token differs"),
        ("observed_work", replace(identity(), model_sha256="0" * 64), "hashes differ"),
        (
            "quiescence",
            quiescence(active_writer_ids=("writer-1",)),
            "no writers",
        ),
        (
            "quiescence",
            quiescence(active_lease_ids=("lease-1",)),
            "no writers",
        ),
        (
            "quiescence",
            quiescence(control_owner_id="owner-1"),
            "no writers",
        ),
        (
            "quiescence",
            quiescence(reflex_active=True),
            "Reflex must be inactive",
        ),
    ],
)
def test_authorization_fails_closed_on_identity_receipt_and_quiescence(
    field: str, value: object, message: str
) -> None:
    selected = plan()
    arguments: dict[str, object] = {
        "selected_work_ids": ("work-001",),
        "observed_work": identity(),
        "safety_token": selected.safety_token,
        "receipt": receipt(selected),
        "receipt_mode": 0o600,
        "migration": migration(),
        "quiescence": quiescence(),
        "expected_reflex_id": REFLEX_ID,
        "store": MemoryAuthorizationStore(),
        "now": NOW,
    }
    arguments[field] = value
    with pytest.raises(ProductionShadowError, match=message):
        authorize_production_shadow(selected, **arguments)  # type: ignore[arg-type]


def test_authorization_appends_exact_schema_rows_readbacks_and_replays() -> None:
    value, store, first = authorize()
    assert first.allowlist_appended and first.audit_appended
    assert first.row.authorized_at == NOW - timedelta(seconds=30)
    assert first.row.expires_at == value.expires_at
    assert first.row.safety_token_sha256 == hashlib.sha256(
        value.safety_token.encode()
    ).hexdigest()
    assert first.row.migration_journal_sha256 == migration().journal_sha256
    assert store.allowlist[0] == {
        **identity().as_dict(),
        "plan_sha256": value.sha256,
        "approved_at": (NOW - timedelta(seconds=30)).isoformat(),
        "approved_by": "release-control",
    }
    assert store.audit[0] == {
        "audit_id": first.row.authorization_id,
        "work_id": "work-001",
        "plan_sha256": value.sha256,
        "shadow_attempt_id": "AUTHORIZATION",
        "legacy_attempt_id": value.identity_sha256,
        "comparison_sha256": first.row.safety_token_sha256,
        "critical_findings": 0,
        "recorded_at": (NOW - timedelta(seconds=30)).isoformat(),
    }
    second = authorize_production_shadow(
        value,
        selected_work_ids=("work-001",),
        observed_work=identity(),
        safety_token=value.safety_token,
        receipt=receipt(value),
        receipt_mode=0o600,
        migration=migration(),
        quiescence=quiescence(),
        expected_reflex_id=REFLEX_ID,
        store=store,
        now=NOW + timedelta(seconds=1),
    )
    assert second.replayed
    assert len(store.allowlist) == len(store.audit) == 1


def test_authorization_intent_binds_both_exact_table_rows() -> None:
    _, store, result = authorize()
    intent = authorization_intent(
        result.row, store.allowlist[0], store.audit[0]
    )

    assert intent.allowlist_sha256 == sha256_json(store.allowlist[0])
    assert intent.audit_sha256 == sha256_json(store.audit[0])
    changed_allow = {**store.allowlist[0], "approved_by": "other"}
    changed_audit = {**store.audit[0], "critical_findings": 1}
    assert authorization_intent(
        result.row, changed_allow, store.audit[0]
    ).allowlist_sha256 != intent.allowlist_sha256
    assert authorization_intent(
        result.row, store.allowlist[0], changed_audit
    ).audit_sha256 != intent.audit_sha256


def test_authorization_conflict_fails_without_second_append() -> None:
    _, store, _ = authorize()
    store.allowlist[0]["approved_by"] = "somebody-else"
    with pytest.raises(ProductionShadowError, match="conflicts"):
        authorize(store)
    assert len(store.allowlist) == len(store.audit) == 1


def test_authorization_preflights_both_rows_before_atomic_append() -> None:
    value = plan()
    store = MemoryAuthorizationStore()
    store.audit.append(
        {
            "audit_id": sha256_json(
                {
                    "plan_sha256": value.sha256,
                    "work_id": value.work.work_id,
                    "work_identity_sha256": value.identity_sha256,
                    "expires_at": value.expires_at.isoformat(),
                }
            ),
            "work_id": "work-001",
            "plan_sha256": "0" * 64,
        }
    )
    with pytest.raises(ProductionShadowError, match="conflicts"):
        authorize_production_shadow(
            value,
            selected_work_ids=("work-001",),
            observed_work=identity(),
            safety_token=value.safety_token,
            receipt=receipt(value),
            receipt_mode=0o600,
            migration=migration(),
            quiescence=quiescence(),
            expected_reflex_id=REFLEX_ID,
            store=store,
            now=NOW,
        )
    assert store.allowlist == []


def test_authorization_rejects_one_sided_post_append_readback() -> None:
    class PartialStore(MemoryAuthorizationStore):
        def append_authorization(self, allowlist, audit) -> None:
            self.append_allowlist(allowlist)

    selected = plan()
    store = PartialStore()

    with pytest.raises(ProductionShadowError, match="readback failed"):
        authorize_production_shadow(
            selected,
            selected_work_ids=("work-001",),
            observed_work=identity(),
            safety_token=selected.safety_token,
            receipt=receipt(selected),
            receipt_mode=0o600,
            migration=migration(),
            quiescence=quiescence(),
            expected_reflex_id=REFLEX_ID,
            store=store,
            now=NOW,
        )

    assert len(store.allowlist) == 1
    assert store.audit == []


def _intent(**overrides: str) -> AuthorizationIntent:
    values = {
        "authorization_id": "1" * 64,
        "work_id": "work-001",
        "plan_sha256": "2" * 64,
        "work_identity_sha256": "3" * 64,
        "allowlist_sha256": "4" * 64,
        "audit_sha256": "5" * 64,
    }
    values.update(overrides)
    return AuthorizationIntent(**values)


@pytest.mark.parametrize(
    (
        "has_intent",
        "allowlist_exact",
        "allowlist_present",
        "audit_exact",
        "audit_present",
        "expected",
    ),
    [
        (False, False, False, False, False, AuthorizationProtocolState.EMPTY),
        (True, False, False, False, False, AuthorizationProtocolState.PREPARED),
        (
            True,
            True,
            True,
            False,
            False,
            AuthorizationProtocolState.ALLOWLIST_APPENDED,
        ),
        (
            True,
            False,
            False,
            True,
            True,
            AuthorizationProtocolState.AUDIT_APPENDED,
        ),
        (
            True,
            True,
            True,
            True,
            True,
            AuthorizationProtocolState.COMMITTED,
        ),
    ],
)
def test_authorization_protocol_state_machine_exact_transitions(
    has_intent: bool,
    allowlist_exact: bool,
    allowlist_present: bool,
    audit_exact: bool,
    audit_present: bool,
    expected: AuthorizationProtocolState,
) -> None:
    value = _intent()
    assert classify_authorization_protocol(
        value if has_intent else None,
        value,
        allowlist_exact=allowlist_exact,
        allowlist_present=allowlist_present,
        audit_exact=audit_exact,
        audit_present=audit_present,
    ) is expected


@pytest.mark.parametrize(
    "arguments",
    [
        {
            "intent": None,
            "allowlist_exact": True,
            "allowlist_present": True,
            "audit_exact": False,
            "audit_present": False,
        },
        {
            "intent": _intent(plan_sha256="9" * 64),
            "allowlist_exact": False,
            "allowlist_present": False,
            "audit_exact": False,
            "audit_present": False,
        },
        {
            "intent": _intent(),
            "allowlist_exact": False,
            "allowlist_present": True,
            "audit_exact": False,
            "audit_present": False,
        },
        {
            "intent": _intent(),
            "allowlist_exact": False,
            "allowlist_present": False,
            "audit_exact": False,
            "audit_present": True,
        },
    ],
)
def test_authorization_protocol_conflict_never_repairs(
    arguments: dict[str, object],
) -> None:
    assert classify_authorization_protocol(
        arguments["intent"],  # type: ignore[arg-type]
        _intent(),
        allowlist_exact=bool(arguments["allowlist_exact"]),
        allowlist_present=bool(arguments["allowlist_present"]),
        audit_exact=bool(arguments["audit_exact"]),
        audit_present=bool(arguments["audit_present"]),
    ) is AuthorizationProtocolState.CONFLICT


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        (SHADOW_PUBLICATIONS_TABLE, TargetClass.SHADOW_PUBLICATION_LEDGER),
        (
            f"{PRODUCTION_SHADOW_TABLE_PREFIX}work",
            TargetClass.SHADOW_TABLE,
        ),
        (
            f"{PRODUCTION_SHADOW_FILES_ROOT}attempts/work=work-001/"
            "attempt=attempt-1/pointer.json",
            TargetClass.SHADOW_ATTEMPT_POINTER,
        ),
        (
            f"{PRODUCTION_SHADOW_FILES_ROOT}diagnostics/publication-note.json",
            TargetClass.SHADOW_FILE,
        ),
        (PRODUCTION_ALLOWLIST_TABLE, TargetClass.PRODUCTION_AUX_APPEND),
        (PRODUCTION_AUDIT_TABLE, TargetClass.PRODUCTION_AUX_APPEND),
        ("people_counter_ca_publications", TargetClass.LEGACY_PRODUCTION),
        (
            "Files/people-counter/candidate-a/v1/output.json",
            TargetClass.LEGACY_PRODUCTION,
        ),
    ],
)
def test_target_classification_is_typed_not_substring(
    name: str, expected: TargetClass
) -> None:
    assert classify_target(name) == ClassifiedTarget(name, expected)


@pytest.mark.parametrize(
    "value",
    [
        PRODUCTION_SHADOW_FILES_ROOT,
        f"{PRODUCTION_SHADOW_FILES_ROOT}/absolute",
        f"{PRODUCTION_SHADOW_FILES_ROOT}a\\b",
        f"{PRODUCTION_SHADOW_FILES_ROOT}https://outside",
        f"{PRODUCTION_SHADOW_FILES_ROOT}a/./b",
        f"{PRODUCTION_SHADOW_FILES_ROOT}a/../b",
        f"{PRODUCTION_SHADOW_FILES_ROOT}a//b",
        f"{PRODUCTION_SHADOW_FILES_ROOT}bad?segment",
    ],
)
def test_shadow_path_classification_rejects_every_noncanonical_form(
    value: str,
) -> None:
    classified = classify_target(value)
    assert classified.name == value
    assert classified.target_class is TargetClass.OUTSIDE


@pytest.mark.parametrize("value", [None, 7, "", [], {}])
def test_target_classification_preserves_invalid_input_as_outside(
    value: object,
) -> None:
    classified = classify_target(value)  # type: ignore[arg-type]
    assert classified.name == str(value)
    assert classified.target_class is TargetClass.OUTSIDE


@pytest.mark.parametrize(
    "relative",
    [
        "other/work=x/attempt=y/pointer.json",
        "attempts/work=x/attempt=y/not-pointer.json",
        "attempts/not-work/attempt=y/pointer.json",
        "attempts/work=x/not-attempt/pointer.json",
    ],
)
def test_pointer_classification_requires_all_structural_components(
    relative: str,
) -> None:
    target = classify_target(PRODUCTION_SHADOW_FILES_ROOT + relative)
    assert target.name == PRODUCTION_SHADOW_FILES_ROOT + relative
    assert target.target_class is TargetClass.SHADOW_FILE


def test_outside_and_invalid_shadow_table_preserve_typed_result() -> None:
    invalid_table = PRODUCTION_SHADOW_TABLE_PREFIX + "Bad-Suffix"
    assert classify_target(invalid_table) == ClassifiedTarget(
        invalid_table, TargetClass.OUTSIDE
    )
    assert classify_target("unrelated") == ClassifiedTarget(
        "unrelated", TargetClass.OUTSIDE
    )


def test_target_policy_permits_shadow_publication_pointer_and_aux_append_only() -> None:
    assert (
        require_write_target(
            SHADOW_PUBLICATIONS_TABLE, WriteOperation.APPEND
        ).target_class
        is TargetClass.SHADOW_PUBLICATION_LEDGER
    )
    pointer = (
        f"{PRODUCTION_SHADOW_FILES_ROOT}attempts/work=work-001/"
        "attempt=attempt-1/pointer.json"
    )
    assert require_write_target(pointer, "CREATE").target_class is (
        TargetClass.SHADOW_ATTEMPT_POINTER
    )
    with pytest.raises(ProductionShadowError, match="append-only"):
        require_write_target(PRODUCTION_ALLOWLIST_TABLE, "UPDATE")
    with pytest.raises(ProductionShadowError, match="outside"):
        require_write_target("people_counter_ca_work", "APPEND")


def test_legacy_ingestion_is_read_only_exact_and_identity_only() -> None:
    rows = legacy_rows()
    before = ingest_legacy_source(rows)
    registration = before.registration_payload()
    assert registration["work_id"] == "work-001"
    assert "status" not in registration
    assert "committed_attempt_id" not in registration
    assert before.work_id == "work-001"
    assert before.attempt_id == "legacy-attempt-001"
    assert before.output_sha256 == "f" * 64
    assert before.identity == identity()
    assert set(before.row_hashes) == {
        "work",
        "attempt",
        "publication",
        "committed_view",
    }
    assert before.source_rows_sha256 == sha256_json(dict(before.row_hashes))
    after = ingest_legacy_source(rows)
    assert prove_legacy_unchanged(before, after)["unchanged"] is True
    assert rows.work["status"] == "SUCCEEDED"


def test_legacy_unchanged_proof_checks_each_independent_digest() -> None:
    before = ingest_legacy_source(legacy_rows())
    with pytest.raises(ProductionShadowError, match="hashes changed"):
        prove_legacy_unchanged(
            before,
            replace(
                before,
                row_hashes={**dict(before.row_hashes), "work": "0" * 64},
            ),
        )
    with pytest.raises(ProductionShadowError, match="hashes changed"):
        prove_legacy_unchanged(
            before,
            replace(before, source_rows_sha256="0" * 64),
        )


@pytest.mark.parametrize(
    ("part", "change", "message"),
    [
        ("work", {"status": "READY"}, "not SUCCEEDED"),
        (
            "work",
            {"committed_attempt_id": "other-attempt"},
            "pointer differs",
        ),
        (
            "publication",
            {"output_sha256": "0" * 64},
            "output_sha256 differs",
        ),
        (
            "committed_view",
            {"model_sha256": "0" * 64},
            "model_sha256 differs",
        ),
    ],
)
def test_legacy_ingestion_rejects_inconsistent_source_rows(
    part: str, change: dict[str, object], message: str
) -> None:
    rows = legacy_rows()
    values = {
        "work": dict(rows.work),
        "attempt": dict(rows.attempt),
        "publication": dict(rows.publication),
        "committed_view": dict(rows.committed_view),
    }
    values[part].update(change)
    with pytest.raises(ProductionShadowError, match=message):
        ingest_legacy_source(LegacySourceRows(**values))


class Bootstrap:
    def __init__(self) -> None:
        self.schemas: dict[str, tuple[tuple[str, str, bool], ...]] = {}

    def table_schema(self, table_name: str):
        return self.schemas.get(table_name)

    def create_table(self, table_name: str, schema):
        assert table_name.startswith(PRODUCTION_SHADOW_TABLE_PREFIX)
        self.schemas[table_name] = tuple(schema)


def test_bootstrap_is_create_only_exact_and_shadow_confined() -> None:
    backend = Bootstrap()
    created = bootstrap_production_shadow(backend)
    assert len(created) == len(SHADOW_SCHEMAS)
    assert all(name.startswith(PRODUCTION_SHADOW_TABLE_PREFIX) for name in created)
    assert bootstrap_production_shadow(backend) == ()


def test_bootstrap_rejects_existing_schema_drift() -> None:
    backend = Bootstrap()
    backend.schemas[f"{PRODUCTION_SHADOW_TABLE_PREFIX}locks"] = ()
    with pytest.raises(ProductionShadowError, match="schema readback differs"):
        bootstrap_production_shadow(backend)


class Execution:
    def __init__(self, result: CommittedRoute) -> None:
        self.result = result
        self.existing: CommittedRoute | None = None
        self.calls: list[str] = []

    def committed_route(self, work_id, *, config):
        assert config == FabricCandidateAConfig.production_shadow()
        self.calls.append("existing")
        return self.existing

    def register(self, registration, *, config):
        assert config == FabricCandidateAConfig.production_shadow()
        assert registration["work_id"] == "work-001"
        self.calls.append("register")

    def claim(self, work_id, *, config):
        assert config == FabricCandidateAConfig.production_shadow()
        self.calls.append("claim")
        return {"work_id": work_id, "batch_id": "batch-1"}

    def process(self, claim, *, config, provenance):
        assert config == FabricCandidateAConfig.production_shadow()
        assert provenance == globals()["provenance"]()
        self.calls.append("process")
        return {"records": [{"count": 2}]}

    def seal(self, claim, output, *, config):
        assert config == FabricCandidateAConfig.production_shadow()
        self.calls.append("seal")

    def publish(self, claim, *, authorization, config, provenance):
        assert config == FabricCandidateAConfig.production_shadow()
        assert provenance == globals()["provenance"]()
        assert authorization.authorization_id == self.result.authorization_id
        self.calls.append("publish")
        return self.result


def test_real_adapter_flow_is_parameterized_shadow_only_and_rerun_reuses_commit() -> None:
    value, _, approved = authorize()
    expected = route(
        shadow=True,
        authorization_id=approved.row.authorization_id,
        plan_sha256=value.sha256,
    )
    adapter = Execution(expected)
    source = ingest_legacy_source(legacy_rows())
    assert execute_production_shadow(source, approved, adapter, provenance()) is expected
    assert adapter.calls == [
        "existing",
        "register",
        "claim",
        "process",
        "seal",
        "publish",
    ]
    adapter.calls.clear()
    adapter.existing = expected
    assert execute_production_shadow(source, approved, adapter, provenance()) is expected
    assert adapter.calls == ["existing"]


def test_synthetic_flow_is_explicitly_no_baseline_and_idempotent() -> None:
    synthetic = SyntheticShadowPlan(
        plan_id="shadow-synthetic-001",
        work=identity(),
        payload={
            "duration_seconds": 10.0,
            "runtime_key": "cpu",
            "source_video": "/lakehouse/default/Files/synthetic/input.mp4",
            "source_sha256": "d" * 64,
        },
        source_evidence_sha256="f" * 64,
        created_at=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
    )
    authorization_id = sha256_json(
        {
            "comparison_mode": "NO_LEGACY_BASELINE",
            "plan_sha256": synthetic.sha256,
            "work_identity_sha256": synthetic.identity_sha256,
        }
    )
    expected = route(
        shadow=True,
        authorization_id=authorization_id,
        plan_sha256=synthetic.sha256,
    )
    adapter = Execution(expected)

    observed = execute_synthetic_shadow(
        synthetic,
        adapter,
        provenance(),
        reviewed_at=NOW,
        reviewer="release-control",
    )

    assert observed is expected
    assert synthetic.as_dict()["comparison_mode"] == "NO_LEGACY_BASELINE"
    assert synthetic.as_dict()["plan_type"] == "SHADOW_SYNTHETIC"
    assert adapter.calls == [
        "existing",
        "register",
        "claim",
        "process",
        "seal",
        "publish",
    ]
    adapter.calls.clear()
    adapter.existing = expected
    assert (
        execute_synthetic_shadow(
            synthetic,
            adapter,
            provenance(),
            reviewed_at=NOW,
            reviewer="release-control",
        )
        is expected
    )
    assert adapter.calls == ["existing"]


def test_synthetic_plan_rejects_legacy_comparison_claim() -> None:
    with pytest.raises(ProductionShadowError, match="comparison mode differs"):
        SyntheticShadowPlan(
            plan_id="shadow-synthetic-001",
            work=identity(),
            payload={"duration_seconds": 1.0},
            source_evidence_sha256="f" * 64,
            created_at=NOW,
            expires_at=NOW + timedelta(hours=1),
            comparison_mode="LEGACY_EQUIVALENT",
        )


def test_execution_rejects_source_authorization_identity_mismatch() -> None:
    value, _, approved = authorize()
    expected = route(
        shadow=True,
        authorization_id=approved.row.authorization_id,
        plan_sha256=value.sha256,
    )
    source = ingest_legacy_source(legacy_rows())
    with pytest.raises(ProductionShadowError, match="work IDs differ"):
        execute_production_shadow(
            replace(source, work_id="work-002"),
            approved,
            Execution(expected),
            provenance(),
        )
    with pytest.raises(ProductionShadowError, match="identities differ"):
        execute_production_shadow(
            replace(
                source,
                identity=replace(identity(), model_sha256="0" * 64),
            ),
            approved,
            Execution(expected),
            provenance(),
        )


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"pointer_fence": 3}, "fence is stale"),
        ({"publication_count": 2}, "duplicated"),
        ({"committed": False}, "sealed and committed"),
        (
            {"output_path": "Files/people-counter/candidate-a/v1/o.json"},
            "cannot mutate legacy production files",
        ),
    ],
)
def test_execution_rejects_stale_duplicate_uncommitted_or_production_output(
    override: dict[str, object], message: str
) -> None:
    value, _, approved = authorize()
    broken = route(
        shadow=True,
        authorization_id=approved.row.authorization_id,
        plan_sha256=value.sha256,
        **override,
    )
    adapter = Execution(broken)
    with pytest.raises((ProductionShadowError, ValueError), match=message):
        execute_production_shadow(
            ingest_legacy_source(legacy_rows()),
            approved,
            adapter,
            provenance(),
        )


def test_comparison_accepts_frame_timestamp_numeric_and_total_tolerances() -> None:
    value, _, approved = authorize()
    legacy = route(
        shadow=False,
        authorization_id=approved.row.authorization_id,
        plan_sha256=value.sha256,
    )
    shadow = route(
        shadow=True,
        authorization_id=approved.row.authorization_id,
        plan_sha256=value.sha256,
        records=({"frame": 1, "count": 2.05},),
        logical_total=2.1,
        frame_count=101,
        timestamp=NOW + timedelta(milliseconds=500),
    )
    report = compare_and_reconcile(
        legacy,
        shadow,
        approved.row,
        tolerance=RouteTolerance(
            frame_count=1,
            timestamp_seconds=0.5,
            logical_total=0.1,
            numeric_fields={"0.count": 0.05},
        ),
    )
    assert report.passed
    report.require_pass()


@pytest.mark.parametrize(
    ("override", "finding"),
    [
        ({"logical_identity_sha256": "0" * 64}, "LOGICAL_IDENTITY_MISMATCH"),
        ({"pointer_fence": 3}, "STALE_FENCE"),
        ({"publication_count": 2}, "DUPLICATE_PUBLICATION"),
        ({"committed": False}, "UNCOMMITTED_VISIBILITY"),
        ({"authorization_id": "0" * 64}, "AUTHORIZATION_LINK_MISMATCH"),
        (
            {"output_path": "Files/people-counter/candidate-a/v1/o.json"},
            "OUTPUT_PATH_OUTSIDE_SHADOW",
        ),
    ],
)
def test_critical_reconciliation_findings_fail_gate(
    override: dict[str, object], finding: str
) -> None:
    value, _, approved = authorize()
    legacy = route(
        shadow=False,
        authorization_id=approved.row.authorization_id,
        plan_sha256=value.sha256,
    )
    route_values = {
        "authorization_id": approved.row.authorization_id,
        "plan_sha256": value.sha256,
        **override,
    }
    shadow = route(shadow=True, **route_values)
    report = compare_and_reconcile(legacy, shadow, approved.row)
    assert finding in {item.finding_type for item in report.findings}
    with pytest.raises(ProductionShadowError, match="critical"):
        report.require_pass()


@pytest.mark.parametrize(
    ("override", "finding_type", "field"),
    [
        ({"work_id": "work-002"}, "WORK_ID_MISMATCH", "work_id"),
        (
            {"logical_identity_sha256": "0" * 64},
            "LOGICAL_IDENTITY_MISMATCH",
            "logical_identity_sha256",
        ),
        (
            {"identity": replace(identity(), source_sha256="0" * 64)},
            "IDENTITY_MISMATCH",
            "identity",
        ),
        ({"sealed": False}, "UNSEALED_OUTPUT", "sealed"),
        ({"committed": False}, "UNCOMMITTED_VISIBILITY", "committed"),
        (
            {"pointer_attempt_id": "other-attempt"},
            "POINTER_MISMATCH",
            "pointer_attempt_id",
        ),
        ({"pointer_fence": 3}, "STALE_FENCE", "pointer_fence"),
        (
            {"publication_count": 2},
            "DUPLICATE_PUBLICATION",
            "publication_count",
        ),
        (
            {"authorization_id": "0" * 64},
            "AUTHORIZATION_LINK_MISMATCH",
            "authorization_id",
        ),
        ({"plan_sha256": "0" * 64}, "AUDIT_PLAN_MISMATCH", "plan_sha256"),
        (
            {"output_sha256": "4" * 64},
            "OUTPUT_DIGEST_MISMATCH",
            "output_sha256",
        ),
    ],
)
def test_reconciliation_finding_contract_is_exact(
    override: dict[str, object], finding_type: str, field: str
) -> None:
    value, _, approved = authorize()
    legacy = route(
        shadow=False,
        authorization_id=approved.row.authorization_id,
        plan_sha256=value.sha256,
    )
    route_values = {
        "authorization_id": approved.row.authorization_id,
        "plan_sha256": value.sha256,
        **override,
    }
    shadow = route(shadow=True, **route_values)
    report = compare_and_reconcile(legacy, shadow, approved.row)
    finding = next(item for item in report.findings if item.finding_type == finding_type)
    assert finding.field == field
    assert finding.severity == "CRITICAL"
    assert finding.finding_id == sha256_json(
        {
            "work_id": legacy.work_id,
            "plan_sha256": approved.row.plan_sha256,
            "finding_type": finding_type,
            "field": field,
        }
    )


class ReconcileStore:
    def __init__(self, findings: list[dict[str, object]]) -> None:
        self.findings = findings

    def reconcile(self):
        return self.findings


def test_reconcile_main_uses_fixed_shadow_config_and_fails_critical(
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = FabricCandidateAConfig.production_shadow()
    assert reconcile_main([], config=config, store=ReconcileStore([])) == 0
    assert capsys.readouterr().out.strip() == "[]"
    assert (
        reconcile_main(
            [],
            config=config,
            store=ReconcileStore(
                [{"severity": "CRITICAL", "resolved_at": None}]
            ),
        )
        == 1
    )
    assert '"severity": "CRITICAL"' in capsys.readouterr().out
