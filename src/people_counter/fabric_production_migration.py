"""Fail-closed, additive-only migration for Candidate A production tables.

The module is deliberately self-contained and imports no Fabric or PySpark
packages.  A Spark session is accepted through :class:`SparkMigrationBackend`;
unit tests and offline planning use :class:`FakeMigrationBackend`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Protocol, TextIO


MIGRATION_ID = "people_counter_ca_0001"
MIGRATION_VERSION = 1
JOURNAL_TABLE = "people_counter_ca_migration_journal"
WORKSPACE_ID = "c31ee864-230d-4005-8fd5-7c7130ebf774"
LAKEHOUSE_ID = "883cff91-eaa8-40be-870f-6e9716303cb2"
ENVIRONMENT_ID = "3e580f48-9ff7-4bc6-af2e-a59158029ada"
# The controller bounds a live SJD to one hour and the observed Fabric cold
# start is five to seven minutes.  Keep the independently captured proof valid
# for that bounded execution plus a conservative fifteen-minute startup
# budget; callers still reject future-dated and expired inventories.
QUIESCENCE_MAX_AGE_SECONDS = 4500.0

TABLE_ALLOWLIST = (
    JOURNAL_TABLE,
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


class MigrationError(RuntimeError):
    """Base error for a refused or failed production migration."""


class PreconditionError(MigrationError):
    """The immutable plan no longer describes a safe write."""


class PostwriteVerificationError(MigrationError):
    """A write returned but its exact expected state was not observed."""


class JournalIntegrityError(MigrationError):
    """Append-only journal evidence is missing, conflicting, or unchained."""


class OperationKind(str, Enum):
    """The complete migration operation language.

    There is intentionally no arbitrary-SQL, alter, replace, truncate, or drop
    variant.
    """

    CREATE_TABLE_IF_NOT_EXISTS = "create_table_if_not_exists"


class ApplyStatus(str, Enum):
    APPLIED = "applied"
    NOOP = "noop"
    REFUSED = "refused"
    PARTIAL = "partial"
    AMBIGUOUS = "ambiguous"


@dataclass(frozen=True, order=True)
class ColumnSpec:
    name: str
    data_type: str
    nullable: bool = True

    def to_dict(self) -> dict[str, object]:
        return {
            "data_type": self.data_type,
            "name": self.name,
            "nullable": self.nullable,
        }


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[ColumnSpec, ...]
    provider: str = "delta"
    properties: tuple[tuple[str, str], ...] = (
        ("people_counter.migration_id", MIGRATION_ID),
        ("people_counter.migration_version", str(MIGRATION_VERSION)),
    )

    def to_dict(self) -> dict[str, object]:
        return {
            "columns": [column.to_dict() for column in self.columns],
            "name": self.name,
            "properties": dict(self.properties),
            "provider": self.provider,
        }


def _columns(definition: str) -> tuple[ColumnSpec, ...]:
    values: list[ColumnSpec] = []
    for item in definition.split(","):
        name, data_type = item.strip().split()
        values.append(ColumnSpec(name, data_type))
    return tuple(values)


_TABLE_DEFINITIONS = {
    JOURNAL_TABLE: (
        "journal_id string, migration_id string, migration_version bigint, "
        "status string, plan_sha256 string, before_sha256 string, "
        "after_sha256 string, receipts_sha256 string, previous_evidence_sha256 string, "
        "evidence_sha256 string, error_text string"
    ),
    "people_counter_ca_attempts": (
        "attempt_id string, work_id string, batch_id string, fence bigint, "
        "status string, lease_expires_at double, payload_sha256 string, "
        "output_path string, output_sha256 string, terminal_succeeded boolean, "
        "records_json string, recovery_outcome string, created_at double, sealed_at double"
    ),
    "people_counter_ca_batch_members": (
        "batch_id string, ordinal bigint, work_id string, attempt_id string, "
        "fence bigint, payload_sha256 string"
    ),
    "people_counter_ca_batches": (
        "batch_id string, owner string, runtime_key string, status string, "
        "lease_expires_at double, item_count bigint, membership_sha256 string, "
        "envelope_version bigint, envelope_path string, envelope_sha256 string, "
        "created_at double, sealed_at double, committed_at double"
    ),
    "people_counter_ca_locks": (
        "lock_name string, owner_id string, acquired_at timestamp"
    ),
    "people_counter_ca_publications": (
        "publication_sequence bigint, work_id string, attempt_id string, "
        "batch_id string, output_path string, output_sha256 string, published_at double"
    ),
    "people_counter_ca_reconciliation_findings": (
        "finding_id string, finding_type string, severity string, entity_key string, "
        "details_json string, first_seen_at double, last_seen_at double, resolved_at double"
    ),
    "people_counter_ca_replay_requests": (
        "replay_id string, work_id string, operator string, reason string, "
        "generation bigint, requested_at double"
    ),
    "people_counter_ca_routing_allowlist": (
        "work_id string, camera_sha256 string, location_sha256 string, "
        "model_sha256 string, source_sha256 string, config_sha256 string, "
        "plan_sha256 string, approved_at timestamp, approved_by string"
    ),
    "people_counter_ca_shadow_audit": (
        "audit_id string, work_id string, plan_sha256 string, shadow_attempt_id string, "
        "legacy_attempt_id string, comparison_sha256 string, critical_findings bigint, "
        "recorded_at timestamp"
    ),
    "people_counter_ca_work": (
        "work_id string, payload_json string, payload_sha256 string, runtime_key string, "
        "duration_seconds double, config_sha256 string, release_digest string, "
        "status string, attempt_count bigint, max_attempts bigint, "
        "original_max_attempts bigint, fence bigint, available_at double, "
        "lease_owner string, lease_attempt_id string, lease_expires_at double, "
        "committed_attempt_id string, publication_sequence bigint, "
        "replay_generation bigint, last_replay_id string, last_error string, "
        "created_at double, updated_at double"
    ),
}

TABLE_SPECS: Mapping[str, TableSpec] = MappingProxyType(
    {
        name: TableSpec(name, _columns(_TABLE_DEFINITIONS[name]))
        for name in TABLE_ALLOWLIST
    }
)


@dataclass(frozen=True)
class TableSnapshot:
    name: str
    exists: bool
    columns: tuple[ColumnSpec, ...] = ()
    provider: str | None = None
    properties: tuple[tuple[str, str], ...] = ()
    row_count: int | None = field(default=None, compare=False)
    version: int | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.columns, tuple) or not isinstance(
            self.properties, tuple
        ):
            raise TypeError("snapshot columns and properties must be immutable tuples")

    @classmethod
    def missing(cls, name: str) -> TableSnapshot:
        return cls(name=name, exists=False)

    @classmethod
    def from_spec(cls, spec: TableSpec) -> TableSnapshot:
        return cls(
            name=spec.name,
            exists=True,
            columns=spec.columns,
            provider=spec.provider,
            properties=spec.properties,
            row_count=0,
            version=0,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "columns": [column.to_dict() for column in self.columns],
            "exists": self.exists,
            "name": self.name,
            "properties": dict(self.properties),
            "provider": self.provider,
            "row_count": self.row_count,
            "schema_sha256": evidence_hash(
                [column.to_dict() for column in self.columns]
            ),
            "version": self.version,
        }


@dataclass(frozen=True, order=True)
class LegacyObjectSnapshot:
    """Read-only evidence for one pre-existing table, view, pointer, or gold object."""

    name: str
    object_type: str
    exists: bool
    schema_sha256: str | None = None
    row_count: int | None = None
    version: int | None = None
    content_sha256: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "content_sha256": self.content_sha256,
            "exists": self.exists,
            "name": self.name,
            "object_type": self.object_type,
            "row_count": self.row_count,
            "schema_sha256": self.schema_sha256,
            "version": self.version,
        }


@dataclass(frozen=True)
class WriterQuiescenceProof:
    """Independently captured writer inventory supplied to migration discovery."""

    inventory_sha256: str
    captured_at: float
    passed: bool
    active_writer_ids: tuple[str, ...] = ()
    stopped_trigger_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if len(self.inventory_sha256) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.inventory_sha256
        ):
            raise ValueError("writer inventory hash must be lowercase SHA-256")
        if not math.isfinite(self.captured_at) or self.captured_at < 0:
            raise ValueError("writer inventory capture time is invalid")
        if tuple(sorted(set(self.active_writer_ids))) != self.active_writer_ids:
            raise ValueError("active_writer_ids must be sorted and unique")
        if tuple(sorted(set(self.stopped_trigger_ids))) != self.stopped_trigger_ids:
            raise ValueError("stopped_trigger_ids must be sorted and unique")

    def to_dict(self) -> dict[str, object]:
        return {
            "active_writer_ids": list(self.active_writer_ids),
            "captured_at": self.captured_at,
            "inventory_sha256": self.inventory_sha256,
            "passed": self.passed,
            "stopped_trigger_ids": list(self.stopped_trigger_ids),
        }


def _default_quiescence_proof() -> WriterQuiescenceProof:
    return WriterQuiescenceProof("0" * 64, 0.0, False)


def _fake_quiescence_proof() -> WriterQuiescenceProof:
    return WriterQuiescenceProof(
        evidence_hash({"source": "fake-backend", "writers": []}),
        0.0,
        True,
        (),
        ("fake-stopped-trigger",),
    )


@dataclass(frozen=True)
class LeaseSnapshot:
    owner: str | None = None
    token_sha256: str | None = None
    expires_at: float | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "expires_at": self.expires_at,
            "owner": self.owner,
            "token_sha256": self.token_sha256,
        }


@dataclass(frozen=True)
class DiscoverySnapshot:
    tables: tuple[TableSnapshot, ...]
    active_run_ids: tuple[str, ...] = ()
    active_lease_ids: tuple[str, ...] = ()
    control_owner: str | None = None
    lease: LeaseSnapshot = field(default_factory=LeaseSnapshot)
    engine: str = "fake"
    supports_delta: bool = True
    legacy_objects: tuple[LegacyObjectSnapshot, ...] = ()
    committed_pointer_sha256: str | None = None
    committed_view_fingerprints: tuple[tuple[str, str], ...] = ()
    gold_sha256: str | None = None
    writer_quiescence: WriterQuiescenceProof = field(
        default_factory=_default_quiescence_proof
    )
    migration_id: str = MIGRATION_ID
    migration_version: int = MIGRATION_VERSION

    def __post_init__(self) -> None:
        if (
            not isinstance(self.tables, tuple)
            or not isinstance(self.active_run_ids, tuple)
            or not isinstance(self.active_lease_ids, tuple)
            or not isinstance(self.legacy_objects, tuple)
            or not isinstance(self.committed_view_fingerprints, tuple)
        ):
            raise TypeError("discovery collections must be immutable tuples")
        names = tuple(table.name for table in self.tables)
        if names != TABLE_ALLOWLIST:
            raise ValueError("discovery tables must be the exact ordered allowlist")
        if self.migration_id != MIGRATION_ID or self.migration_version != MIGRATION_VERSION:
            raise ValueError("discovery snapshot migration identity is fixed")
        if tuple(sorted(set(self.active_run_ids))) != self.active_run_ids:
            raise ValueError("active_run_ids must be sorted and unique")
        if tuple(sorted(set(self.active_lease_ids))) != self.active_lease_ids:
            raise ValueError("active_lease_ids must be sorted and unique")
        if tuple(sorted(self.legacy_objects)) != self.legacy_objects:
            raise ValueError("legacy_objects must be deterministically sorted")
        if tuple(sorted(self.committed_view_fingerprints)) != (
            self.committed_view_fingerprints
        ):
            raise ValueError("committed view fingerprints must be sorted")

    def to_dict(self) -> dict[str, object]:
        return {
            "active_run_ids": list(self.active_run_ids),
            "active_lease_ids": list(self.active_lease_ids),
            "control_owner": self.control_owner,
            "engine": self.engine,
            "lease": self.lease.to_dict(),
            "legacy_objects": [item.to_dict() for item in self.legacy_objects],
            "committed_pointer_sha256": self.committed_pointer_sha256,
            "committed_view_fingerprints": dict(
                self.committed_view_fingerprints
            ),
            "gold_sha256": self.gold_sha256,
            "writer_quiescence": self.writer_quiescence.to_dict(),
            "migration_id": self.migration_id,
            "migration_version": self.migration_version,
            "supports_delta": self.supports_delta,
            "tables": [table.to_dict() for table in self.tables],
        }

    @property
    def sha256(self) -> str:
        return evidence_hash(self.to_dict())

    @property
    def state_sha256(self) -> str:
        """Hash migration state while excluding one-use inventory timestamps."""

        value = self.to_dict()
        lease = dict(value["lease"])  # type: ignore[arg-type]
        lease["expires_at"] = None
        value["lease"] = lease
        proof = dict(value["writer_quiescence"])  # type: ignore[arg-type]
        proof["captured_at"] = None
        proof["inventory_sha256"] = None
        value["writer_quiescence"] = proof
        return evidence_hash(value)

    def table(self, name: str) -> TableSnapshot:
        if name not in TABLE_ALLOWLIST:
            raise ValueError(f"table is outside production allowlist: {name!r}")
        return self.tables[TABLE_ALLOWLIST.index(name)]


@dataclass(frozen=True)
class CompatibilityReport:
    compatible: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "blockers": list(self.blockers),
            "compatible": self.compatible,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class AdditiveOperation:
    kind: OperationKind
    table_name: str

    def __post_init__(self) -> None:
        if self.kind is not OperationKind.CREATE_TABLE_IF_NOT_EXISTS:
            raise ValueError(f"unsupported migration operation: {self.kind!r}")
        if self.table_name not in TABLE_ALLOWLIST:
            raise ValueError(f"table is outside production allowlist: {self.table_name!r}")

    @property
    def operation_id(self) -> str:
        return evidence_hash(
            {"kind": self.kind.value, "table": self.table_name}
        )[:24]

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind.value,
            "operation_id": self.operation_id,
            "table": self.table_name,
        }


@dataclass(frozen=True)
class MigrationPlan:
    discovery: DiscoverySnapshot
    compatibility: CompatibilityReport
    operations: tuple[AdditiveOperation, ...]
    rollback_manifest: str
    migration_id: str = MIGRATION_ID
    migration_version: int = MIGRATION_VERSION

    def __post_init__(self) -> None:
        if self.migration_id != MIGRATION_ID or self.migration_version != MIGRATION_VERSION:
            raise ValueError("migration plan identity is fixed")
        names = tuple(operation.table_name for operation in self.operations)
        expected = tuple(
            name for name in TABLE_ALLOWLIST if not self.discovery.table(name).exists
        )
        if names != expected:
            raise ValueError("plan operations must exactly match missing allowlisted tables")

    def evidence(self) -> dict[str, object]:
        return {
            "compatibility": self.compatibility.to_dict(),
            "discovery": self.discovery.to_dict(),
            "discovery_sha256": self.discovery.sha256,
            "migration_id": self.migration_id,
            "migration_version": self.migration_version,
            "operations": [operation.to_dict() for operation in self.operations],
            "rollback_manifest": self.rollback_manifest,
        }

    @property
    def sha256(self) -> str:
        return evidence_hash(self.evidence())

    def to_dict(self) -> dict[str, object]:
        return {
            **self.evidence(),
            "plan_sha256": self.sha256,
            "safety_token": self.safety_token,
        }

    def to_json(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def safety_token(self) -> str:
        """Return the exact operator acknowledgement bound to this plan."""

        return f"{self.migration_version}:{self.migration_id}:{self.sha256}"

    def require_safety_token(self, supplied: str) -> None:
        if not isinstance(supplied, str) or supplied != self.safety_token:
            raise PreconditionError(
                "safety token is not bound to the migration ID and plan hash"
            )


@dataclass(frozen=True)
class OperationReceipt:
    operation_id: str
    table_name: str
    sql_sha256: str
    postwrite_table_sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "operation_id": self.operation_id,
            "postwrite_table_sha256": self.postwrite_table_sha256,
            "sql_sha256": self.sql_sha256,
            "table": self.table_name,
        }


@dataclass(frozen=True)
class JournalEntry:
    journal_id: str
    status: ApplyStatus
    plan_sha256: str
    before_sha256: str
    after_sha256: str
    receipts_sha256: str
    previous_evidence_sha256: str
    evidence_sha256: str
    error_text: str = ""
    migration_id: str = MIGRATION_ID
    migration_version: int = MIGRATION_VERSION

    def evidence(self) -> dict[str, object]:
        return {
            "after_sha256": self.after_sha256,
            "before_sha256": self.before_sha256,
            "error_text": self.error_text,
            "journal_id": self.journal_id,
            "migration_id": self.migration_id,
            "migration_version": self.migration_version,
            "plan_sha256": self.plan_sha256,
            "previous_evidence_sha256": self.previous_evidence_sha256,
            "receipts_sha256": self.receipts_sha256,
            "status": self.status.value,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.evidence(), "evidence_sha256": self.evidence_sha256}


@dataclass(frozen=True)
class ApplyResult:
    status: ApplyStatus
    plan_sha256: str
    before_sha256: str
    after_sha256: str
    receipts: tuple[OperationReceipt, ...] = ()
    journal_evidence_sha256: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "after_sha256": self.after_sha256,
            "before_sha256": self.before_sha256,
            "error": self.error,
            "journal_evidence_sha256": self.journal_evidence_sha256,
            "plan_sha256": self.plan_sha256,
            "receipts": [receipt.to_dict() for receipt in self.receipts],
            "status": self.status.value,
        }


@dataclass(frozen=True)
class VerificationReport:
    valid: bool
    snapshot_sha256: str
    table_errors: tuple[str, ...]
    journal_errors: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "journal_errors": list(self.journal_errors),
            "snapshot_sha256": self.snapshot_sha256,
            "table_errors": list(self.table_errors),
            "valid": self.valid,
        }


class MigrationBackend(Protocol):
    def discover(self) -> DiscoverySnapshot: ...

    def apply_operation(self, operation: AdditiveOperation, sql: str) -> None: ...

    def append_journal(self, entry: JournalEntry) -> None: ...

    def journal_entries(self) -> tuple[JournalEntry, ...]: ...

    def now(self) -> float: ...


def canonical_json(value: object) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def evidence_hash(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def token_hash(token: str) -> str:
    if not token:
        raise ValueError("lease token must not be empty")
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _quoted_identifier(value: str) -> str:
    if value not in TABLE_ALLOWLIST and not any(
        value == column.name
        for spec in TABLE_SPECS.values()
        for column in spec.columns
    ):
        raise ValueError(f"identifier is not fixed by the migration contract: {value!r}")
    return f"`{value}`"


def render_operation(operation: AdditiveOperation) -> str:
    """Render one allowlisted create operation without accepting SQL input."""

    if operation.kind is not OperationKind.CREATE_TABLE_IF_NOT_EXISTS:
        raise ValueError(f"unsupported migration operation: {operation.kind!r}")
    spec = TABLE_SPECS[operation.table_name]
    columns = ", ".join(
        f"{_quoted_identifier(column.name)} {column.data_type.upper()}"
        for column in spec.columns
    )
    properties = ", ".join(
        f"'{key}'='{value}'" for key, value in spec.properties
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {_quoted_identifier(spec.name)} ({columns}) "
        f"USING DELTA TBLPROPERTIES ({properties})"
    )


def _table_errors(snapshot: DiscoverySnapshot) -> tuple[str, ...]:
    errors: list[str] = []
    for name in TABLE_ALLOWLIST:
        observed = snapshot.table(name)
        if not observed.exists:
            errors.append(f"{name}: missing")
            continue
        expected = TableSnapshot.from_spec(TABLE_SPECS[name])
        if observed != expected:
            errors.append(
                f"{name}: metadata differs; expected_sha256="
                f"{evidence_hash(expected.to_dict())}, observed_sha256="
                f"{evidence_hash(observed.to_dict())}"
            )
    return tuple(errors)


def compatibility_report(snapshot: DiscoverySnapshot) -> CompatibilityReport:
    blockers: list[str] = []
    warnings: list[str] = []
    if not snapshot.supports_delta:
        blockers.append("backend does not support Delta tables")
    if snapshot.active_run_ids:
        blockers.append(
            "control plane is not quiescent: " + ",".join(snapshot.active_run_ids)
        )
    if snapshot.active_lease_ids:
        blockers.append(
            "active work leases exist: " + ",".join(snapshot.active_lease_ids)
        )
    if snapshot.control_owner is not None:
        blockers.append(f"control lock is owned by {snapshot.control_owner!r}")
    if (
        not snapshot.writer_quiescence.passed
        or snapshot.writer_quiescence.active_writer_ids
    ):
        blockers.append("independent writer-quiescence proof did not pass")
    for name in TABLE_ALLOWLIST:
        table = snapshot.table(name)
        if table.exists and table != TableSnapshot.from_spec(TABLE_SPECS[name]):
            blockers.append(f"existing table has incompatible exact metadata: {name}")
    if snapshot.engine.lower() not in {"fake", "spark", "fabric-spark"}:
        warnings.append(f"unrecognized migration engine {snapshot.engine!r}")
    return CompatibilityReport(
        compatible=not blockers,
        blockers=tuple(blockers),
        warnings=tuple(warnings),
    )


def rollback_manifest(operations: Sequence[AdditiveOperation]) -> str:
    """Return non-executable rollback evidence, never a rollback operation."""

    names = [operation.table_name for operation in operations]
    lines = [
        f"Migration: {MIGRATION_ID} version {MIGRATION_VERSION}",
        "Mode: TEXT-ONLY OPERATOR MANIFEST; THIS MODULE CANNOT ROLLBACK.",
        "Rollback: stop routing and ignore additive structures.",
        "Policy: preserve journal evidence; no destructive rollback is implemented.",
        "Artifacts created by this plan:",
    ]
    lines.extend(f"- {name}" for name in names)
    if not names:
        lines.append("- none (idempotent no-op)")
    return "\n".join(lines) + "\n"


def build_plan(snapshot: DiscoverySnapshot) -> MigrationPlan:
    operations = tuple(
        AdditiveOperation(OperationKind.CREATE_TABLE_IF_NOT_EXISTS, name)
        for name in TABLE_ALLOWLIST
        if not snapshot.table(name).exists
    )
    return MigrationPlan(
        discovery=snapshot,
        compatibility=compatibility_report(snapshot),
        operations=operations,
        rollback_manifest=rollback_manifest(operations),
    )


def dry_run_plan(backend: MigrationBackend) -> str:
    return build_plan(backend.discover()).to_json()


def validate_apply_plan(
    backend: MigrationBackend,
    plan: MigrationPlan,
    *,
    owner: str,
    lease_token: str,
    current: DiscoverySnapshot | None = None,
) -> DiscoverySnapshot:
    """Validate every deterministic non-lock apply precondition.

    ``plan`` remains the immutable reviewed artifact.  ``current`` is the
    execution inventory discovery and is deliberately used for freshness and
    lease validation after state equivalence succeeds.
    """

    observed = backend.discover() if current is None else current
    if observed.state_sha256 != plan.discovery.state_sha256:
        raise PreconditionError(
            "discovery drift: planned "
            f"{plan.discovery.state_sha256}, observed {observed.state_sha256}"
        )
    if not plan.operations:
        errors = _table_errors(observed)
        if errors:
            raise PreconditionError(
                "no-op state is not exact: " + "; ".join(errors)
            )
    report = compatibility_report(observed)
    if not report.compatible:
        raise PreconditionError(
            "incompatible preconditions: " + "; ".join(report.blockers)
        )
    _check_runtime_safety(
        backend, observed, owner=owner, lease_token=lease_token
    )
    if not plan.operations:
        verification = verify_backend(backend)
        if not verification.valid:
            raise PreconditionError(
                "no-op requires a valid successful migration journal: "
                + "; ".join(verification.journal_errors)
            )
    return observed


def _check_runtime_safety(
    backend: MigrationBackend,
    snapshot: DiscoverySnapshot,
    *,
    owner: str,
    lease_token: str,
) -> None:
    if snapshot.active_run_ids:
        raise PreconditionError(
            "control plane is not quiescent: " + ",".join(snapshot.active_run_ids)
        )
    if snapshot.active_lease_ids:
        raise PreconditionError(
            "active work leases exist: " + ",".join(snapshot.active_lease_ids)
        )
    proof = snapshot.writer_quiescence
    if not proof.passed or proof.active_writer_ids:
        raise PreconditionError(
            "independent writer-quiescence proof did not pass"
        )
    proof_age = backend.now() - proof.captured_at
    if proof_age < 0 or proof_age > QUIESCENCE_MAX_AGE_SECONDS:
        raise PreconditionError(
            "independent writer-quiescence proof is stale or future-dated"
        )
    if snapshot.control_owner is not None:
        raise PreconditionError(
            f"control lock is owned by {snapshot.control_owner!r}"
        )
    lease = snapshot.lease
    if lease.owner != owner:
        raise PreconditionError(
            f"migration lease owner mismatch: expected {owner!r}, observed {lease.owner!r}"
        )
    if lease.token_sha256 != token_hash(lease_token):
        raise PreconditionError("migration lease token mismatch")
    if lease.expires_at is None or not math.isfinite(lease.expires_at):
        raise PreconditionError("migration lease expiry is missing or invalid")
    if lease.expires_at <= backend.now():
        raise PreconditionError("migration lease has expired")


def _check_progress(
    backend: MigrationBackend,
    plan: MigrationPlan,
    completed: Sequence[str],
    *,
    owner: str,
    lease_token: str,
) -> DiscoverySnapshot:
    snapshot = backend.discover()
    _check_runtime_safety(
        backend, snapshot, owner=owner, lease_token=lease_token
    )
    completed_names = frozenset(completed)
    for name in TABLE_ALLOWLIST:
        originally_present = plan.discovery.table(name).exists
        expected = (
            TableSnapshot.from_spec(TABLE_SPECS[name])
            if originally_present or name in completed_names
            else TableSnapshot.missing(name)
        )
        if snapshot.table(name) != expected:
            raise PreconditionError(
                f"migration progress drift at {name}; expected="
                f"{evidence_hash(expected.to_dict())}, observed="
                f"{evidence_hash(snapshot.table(name).to_dict())}"
            )
    return snapshot


def _exact_postwrite(
    backend: MigrationBackend,
    operation: AdditiveOperation,
) -> TableSnapshot:
    observed = backend.discover().table(operation.table_name)
    expected = TableSnapshot.from_spec(TABLE_SPECS[operation.table_name])
    if observed != expected:
        raise PostwriteVerificationError(
            f"exact postwrite verification failed for {operation.table_name}; "
            f"expected={evidence_hash(expected.to_dict())}, "
            f"observed={evidence_hash(observed.to_dict())}"
        )
    if observed.row_count != 0:
        raise PostwriteVerificationError(
            f"new table {operation.table_name} is not empty after creation"
        )
    if observed.version is None or observed.version < 0:
        raise PostwriteVerificationError(
            f"new table {operation.table_name} has no readable Delta version"
        )
    return observed


def _journal_entry(
    backend: MigrationBackend,
    *,
    status: ApplyStatus,
    plan: MigrationPlan,
    before: DiscoverySnapshot,
    after: DiscoverySnapshot,
    receipts: Sequence[OperationReceipt],
    error: str = "",
) -> JournalEntry:
    prior = backend.journal_entries()
    previous = prior[-1].evidence_sha256 if prior else "0" * 64
    receipts_sha256 = evidence_hash([receipt.to_dict() for receipt in receipts])
    identity = {
        "after_sha256": after.sha256,
        "before_sha256": before.sha256,
        "error_text": error,
        "migration_id": MIGRATION_ID,
        "migration_version": MIGRATION_VERSION,
        "plan_sha256": plan.sha256,
        "previous_evidence_sha256": previous,
        "receipts_sha256": receipts_sha256,
        "status": status.value,
    }
    journal_id = evidence_hash(identity)[:32]
    provisional = JournalEntry(
        journal_id=journal_id,
        status=status,
        plan_sha256=plan.sha256,
        before_sha256=before.sha256,
        after_sha256=after.sha256,
        receipts_sha256=receipts_sha256,
        previous_evidence_sha256=previous,
        evidence_sha256="",
        error_text=error,
    )
    return JournalEntry(
        **{
            **provisional.__dict__,
            "evidence_sha256": evidence_hash(provisional.evidence()),
        }
    )


def _append_verified_journal(
    backend: MigrationBackend,
    entry: JournalEntry,
) -> None:
    write_error: Exception | None = None
    try:
        backend.append_journal(entry)
    except Exception as error:
        write_error = error
    matches = [
        observed
        for observed in backend.journal_entries()
        if observed.journal_id == entry.journal_id
    ]
    if matches != [entry]:
        if write_error is not None:
            raise write_error
        raise JournalIntegrityError(
            f"journal append readback differs for {entry.journal_id}"
        )


def apply_plan(
    backend: MigrationBackend,
    plan: MigrationPlan,
    *,
    owner: str,
    lease_token: str,
    safety_token: str,
    on_operations_start: Callable[[], None] | None = None,
) -> ApplyResult:
    """Apply a fixed plan, resolving acknowledged writes by exact readback."""

    plan.require_safety_token(safety_token)
    current = backend.discover()
    if not plan.operations:
        validate_apply_plan(
            backend,
            plan,
            owner=owner,
            lease_token=lease_token,
            current=current,
        )
        return ApplyResult(
            status=ApplyStatus.NOOP,
            plan_sha256=plan.sha256,
            before_sha256=current.sha256,
            after_sha256=current.sha256,
        )

    before = validate_apply_plan(
        backend,
        plan,
        owner=owner,
        lease_token=lease_token,
        current=current,
    )
    if on_operations_start is not None:
        on_operations_start()
    receipts: list[OperationReceipt] = []
    try:
        for operation in plan.operations:
            _check_progress(
                backend,
                plan,
                [receipt.table_name for receipt in receipts],
                owner=owner,
                lease_token=lease_token,
            )
            sql = render_operation(operation)
            try:
                backend.apply_operation(operation, sql)
            except Exception:
                # A lost acknowledgement and a prewrite failure are
                # distinguished solely by the mandatory exact readback.
                pass
            observed = _exact_postwrite(backend, operation)
            receipts.append(
                OperationReceipt(
                    operation_id=operation.operation_id,
                    table_name=operation.table_name,
                    sql_sha256=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                    postwrite_table_sha256=evidence_hash(observed.to_dict()),
                )
            )
        after = _check_progress(
            backend,
            plan,
            [receipt.table_name for receipt in receipts],
            owner=owner,
            lease_token=lease_token,
        )
        errors = _table_errors(after)
        if errors:
            raise PostwriteVerificationError("; ".join(errors))
        entry = _journal_entry(
            backend,
            status=ApplyStatus.APPLIED,
            plan=plan,
            before=before,
            after=after,
            receipts=receipts,
        )
        _append_verified_journal(backend, entry)
        return ApplyResult(
            status=ApplyStatus.APPLIED,
            plan_sha256=plan.sha256,
            before_sha256=before.sha256,
            after_sha256=after.sha256,
            receipts=tuple(receipts),
            journal_evidence_sha256=entry.evidence_sha256,
        )
    except Exception as error:
        try:
            after = backend.discover()
        except Exception as discovery_error:
            return ApplyResult(
                status=ApplyStatus.AMBIGUOUS,
                plan_sha256=plan.sha256,
                before_sha256=before.sha256,
                after_sha256="",
                receipts=tuple(receipts),
                error=f"{type(error).__name__}: {error}; "
                f"rediscovery failed: {type(discovery_error).__name__}: {discovery_error}",
            )
        message = f"{type(error).__name__}: {error}"
        evidence: str | None = None
        journal_safe = False
        try:
            _check_runtime_safety(
                backend, after, owner=owner, lease_token=lease_token
            )
            journal_safe = True
        except PreconditionError:
            pass
        if journal_safe and after.table(JOURNAL_TABLE) == TableSnapshot.from_spec(
            TABLE_SPECS[JOURNAL_TABLE]
        ):
            entry = _journal_entry(
                backend,
                status=ApplyStatus.PARTIAL,
                plan=plan,
                before=before,
                after=after,
                receipts=receipts,
                error=message,
            )
            try:
                _append_verified_journal(backend, entry)
                evidence = entry.evidence_sha256
            except Exception as journal_error:
                message += (
                    f"; journal failure: {type(journal_error).__name__}: {journal_error}"
                )
        return ApplyResult(
            status=ApplyStatus.PARTIAL,
            plan_sha256=plan.sha256,
            before_sha256=before.sha256,
            after_sha256=after.sha256,
            receipts=tuple(receipts),
            journal_evidence_sha256=evidence,
            error=message,
        )


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def _journal_entry_errors(
    entry: JournalEntry,
    *,
    previous: str,
    duplicate: bool,
) -> list[str]:
    errors: list[str] = []
    hash_fields = (
        "plan_sha256",
        "before_sha256",
        "after_sha256",
        "receipts_sha256",
        "previous_evidence_sha256",
        "evidence_sha256",
    )
    if (
        entry.migration_id != MIGRATION_ID
        or entry.migration_version != MIGRATION_VERSION
    ):
        errors.append(f"wrong migration identity at {entry.journal_id}")
    errors.extend(
        f"invalid {name} at {entry.journal_id}"
        for name in hash_fields
        if not _is_sha256(getattr(entry, name))
    )
    if duplicate:
        errors.append(f"duplicate journal_id: {entry.journal_id}")
    if entry.previous_evidence_sha256 != previous:
        errors.append(f"broken journal chain at {entry.journal_id}")
    if evidence_hash(entry.evidence()) != entry.evidence_sha256:
        errors.append(f"invalid evidence hash at {entry.journal_id}")
    return errors


def journal_integrity_errors(
    entries: Sequence[JournalEntry],
    *,
    require_success: bool = True,
) -> tuple[str, ...]:
    if not entries:
        return ("migration journal is empty",) if require_success else ()
    journal_errors: list[str] = []
    previous = "0" * 64
    seen: set[str] = set()
    for entry in entries:
        journal_errors.extend(
            _journal_entry_errors(
                entry,
                previous=previous,
                duplicate=entry.journal_id in seen,
            )
        )
        seen.add(entry.journal_id)
        previous = entry.evidence_sha256
    if require_success and entries[-1].status not in {
        ApplyStatus.APPLIED,
        ApplyStatus.NOOP,
    }:
        journal_errors.append("migration journal has no successful terminal entry")
    return tuple(journal_errors)


def verify_backend(backend: MigrationBackend) -> VerificationReport:
    snapshot = backend.discover()
    table_errors = _table_errors(snapshot)
    journal_errors = journal_integrity_errors(backend.journal_entries())
    return VerificationReport(
        valid=not table_errors and not journal_errors,
        snapshot_sha256=snapshot.sha256,
        table_errors=table_errors,
        journal_errors=journal_errors,
    )


class FakeMigrationBackend:
    """Deterministic in-memory backend with injectable write failures."""

    def __init__(
        self,
        *,
        tables: Mapping[str, TableSnapshot] | None = None,
        active_run_ids: Sequence[str] = (),
        active_lease_ids: Sequence[str] = (),
        control_owner: str | None = None,
        lease_owner: str | None = None,
        lease_token: str | None = None,
        lease_expires_at: float | None = None,
        current_time: float = 0.0,
        supports_delta: bool = True,
        legacy_objects: Sequence[LegacyObjectSnapshot] = (),
        committed_pointer_sha256: str | None = None,
        committed_view_fingerprints: Sequence[tuple[str, str]] = (),
        gold_sha256: str | None = None,
        writer_quiescence: WriterQuiescenceProof | None = None,
    ) -> None:
        supplied = dict(tables or {})
        unknown = set(supplied).difference(TABLE_ALLOWLIST)
        if unknown:
            raise ValueError(f"fake backend tables outside allowlist: {sorted(unknown)!r}")
        self._tables = {
            name: supplied.get(name, TableSnapshot.missing(name))
            for name in TABLE_ALLOWLIST
        }
        self._active_run_ids = tuple(sorted(set(active_run_ids)))
        self._active_lease_ids = tuple(sorted(set(active_lease_ids)))
        self._control_owner = control_owner
        self._lease = LeaseSnapshot(
            owner=lease_owner,
            token_sha256=token_hash(lease_token) if lease_token is not None else None,
            expires_at=lease_expires_at,
        )
        self._time = current_time
        self._supports_delta = supports_delta
        self._legacy_objects = tuple(sorted(legacy_objects))
        self._committed_pointer_sha256 = committed_pointer_sha256
        self._committed_view_fingerprints = tuple(
            sorted(committed_view_fingerprints)
        )
        self._gold_sha256 = gold_sha256
        self._writer_quiescence = writer_quiescence or _fake_quiescence_proof()
        self._journal: list[JournalEntry] = []
        self.applied_sql: list[str] = []
        self.fail_before: set[str] = set()
        self.fail_after: set[str] = set()
        self.fail_journal_after_append = False
        self.fail_discovery_after_writes: int | None = None

    @classmethod
    def exact(
        cls,
        **kwargs: Any,
    ) -> FakeMigrationBackend:
        return cls(
            tables={
                name: TableSnapshot.from_spec(TABLE_SPECS[name])
                for name in TABLE_ALLOWLIST
            },
            **kwargs,
        )

    def now(self) -> float:
        return self._time

    def discover(self) -> DiscoverySnapshot:
        if (
            self.fail_discovery_after_writes is not None
            and len(self.applied_sql) >= self.fail_discovery_after_writes
        ):
            raise OSError("injected discovery failure")
        return DiscoverySnapshot(
            tables=tuple(self._tables[name] for name in TABLE_ALLOWLIST),
            active_run_ids=self._active_run_ids,
            active_lease_ids=self._active_lease_ids,
            control_owner=self._control_owner,
            lease=self._lease,
            engine="fake",
            supports_delta=self._supports_delta,
            legacy_objects=self._legacy_objects,
            committed_pointer_sha256=self._committed_pointer_sha256,
            committed_view_fingerprints=self._committed_view_fingerprints,
            gold_sha256=self._gold_sha256,
            writer_quiescence=self._writer_quiescence,
        )

    def apply_operation(self, operation: AdditiveOperation, sql: str) -> None:
        if operation.table_name in self.fail_before:
            raise OSError(f"injected prewrite failure for {operation.table_name}")
        if self._tables[operation.table_name].exists:
            raise FileExistsError(operation.table_name)
        if sql != render_operation(operation):
            raise ValueError("backend received non-canonical SQL")
        self.applied_sql.append(sql)
        self._tables[operation.table_name] = TableSnapshot.from_spec(
            TABLE_SPECS[operation.table_name]
        )
        if operation.table_name in self.fail_after:
            raise TimeoutError(
                f"injected lost acknowledgement for {operation.table_name}"
            )

    def append_journal(self, entry: JournalEntry) -> None:
        existing = [
            observed for observed in self._journal
            if observed.journal_id == entry.journal_id
        ]
        if existing:
            if existing == [entry]:
                return
            raise JournalIntegrityError("journal_id conflicts with immutable evidence")
        expected_previous = (
            self._journal[-1].evidence_sha256 if self._journal else "0" * 64
        )
        if entry.previous_evidence_sha256 != expected_previous:
            raise JournalIntegrityError("journal append does not extend current chain")
        if evidence_hash(entry.evidence()) != entry.evidence_sha256:
            raise JournalIntegrityError("journal evidence hash is invalid")
        self._journal.append(entry)
        current = self._tables[JOURNAL_TABLE]
        self._tables[JOURNAL_TABLE] = TableSnapshot(
            name=current.name,
            exists=current.exists,
            columns=current.columns,
            provider=current.provider,
            properties=current.properties,
            row_count=len(self._journal),
            version=(current.version or 0) + 1,
        )
        if self.fail_journal_after_append:
            raise TimeoutError("injected lost journal acknowledgement")

    def journal_entries(self) -> tuple[JournalEntry, ...]:
        return tuple(self._journal)

    def replace_table_for_test(self, table: TableSnapshot) -> None:
        if table.name not in TABLE_ALLOWLIST:
            raise ValueError("test table outside allowlist")
        self._tables[table.name] = table


class SparkMigrationBackend:
    """Thin production adapter around a supplied Fabric Spark session."""

    def __init__(
        self,
        spark_session: Any,
        *,
        artifact_binding: Callable[[], Mapping[str, str]],
        active_run_ids: Callable[[], Sequence[str]] = lambda: (),
        active_lease_ids: Callable[[], Sequence[str]] = lambda: (),
        control_owner: Callable[[], str | None] = lambda: None,
        lease: Callable[[], LeaseSnapshot] = LeaseSnapshot,
        legacy_objects: Callable[[], Sequence[LegacyObjectSnapshot]] = lambda: (),
        committed_pointer_sha256: Callable[[], str | None] = lambda: None,
        committed_view_fingerprints: Callable[
            [], Sequence[tuple[str, str]]
        ] = lambda: (),
        gold_sha256: Callable[[], str | None] = lambda: None,
        writer_quiescence: Callable[
            [], WriterQuiescenceProof
        ] = _default_quiescence_proof,
        clock: Callable[[], float],
    ) -> None:
        if spark_session is None:
            raise ValueError("spark_session is required")
        self.spark = spark_session
        self._artifact_binding = artifact_binding
        self._active_run_ids = active_run_ids
        self._active_lease_ids = active_lease_ids
        self._control_owner = control_owner
        self._lease = lease
        self._legacy_objects = legacy_objects
        self._committed_pointer_sha256 = committed_pointer_sha256
        self._committed_view_fingerprints = committed_view_fingerprints
        self._gold_sha256 = gold_sha256
        self._writer_quiescence = writer_quiescence
        self._clock = clock

    def now(self) -> float:
        return float(self._clock())

    def _validate_artifact_binding(self) -> None:
        observed = dict(self._artifact_binding())
        expected = {
            "workspace_id": WORKSPACE_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "environment_id": ENVIRONMENT_ID,
        }
        if observed != expected:
            raise PreconditionError(
                f"Fabric artifact binding mismatch: observed={observed!r}"
            )

    @staticmethod
    def _row_dict(row: Any) -> dict[str, Any]:
        if hasattr(row, "asDict"):
            return dict(row.asDict(recursive=True))
        if isinstance(row, Mapping):
            return dict(row)
        return dict(row)

    def _discover_table(self, name: str) -> TableSnapshot:
        if not bool(self.spark.catalog.tableExists(name)):
            return TableSnapshot.missing(name)
        fields = self.spark.table(name).schema.fields
        columns = tuple(
            ColumnSpec(
                field.name,
                str(field.dataType.simpleString()).lower(),
                bool(field.nullable),
            )
            for field in fields
        )
        rows = self.spark.sql(
            f"DESCRIBE DETAIL {_quoted_identifier(name)}"
        ).collect()
        if len(rows) != 1:
            raise MigrationError(f"DESCRIBE DETAIL returned {len(rows)} rows for {name}")
        detail = self._row_dict(rows[0])
        properties = tuple(
            sorted(
                (str(key), str(value))
                for key, value in dict(detail.get("properties") or {}).items()
                if str(key).startswith("people_counter.")
            )
        )
        frame = self.spark.table(name)
        count = int(frame.count()) if hasattr(frame, "count") else None
        version: int | None = None
        if count is not None:
            history = self.spark.sql(
                f"DESCRIBE HISTORY {_quoted_identifier(name)} LIMIT 1"
            ).collect()
            if len(history) != 1:
                raise MigrationError(
                    f"DESCRIBE HISTORY returned {len(history)} rows for {name}"
                )
            version = int(self._row_dict(history[0])["version"])
        return TableSnapshot(
            name=name,
            exists=True,
            columns=columns,
            provider=str(detail.get("format") or "").lower(),
            properties=properties,
            row_count=count,
            version=version,
        )

    def discover(self) -> DiscoverySnapshot:
        self._validate_artifact_binding()
        return DiscoverySnapshot(
            tables=tuple(self._discover_table(name) for name in TABLE_ALLOWLIST),
            active_run_ids=tuple(sorted(set(self._active_run_ids()))),
            active_lease_ids=tuple(sorted(set(self._active_lease_ids()))),
            control_owner=self._control_owner(),
            lease=self._lease(),
            engine="fabric-spark",
            supports_delta=True,
            legacy_objects=tuple(sorted(self._legacy_objects())),
            committed_pointer_sha256=self._committed_pointer_sha256(),
            committed_view_fingerprints=tuple(
                sorted(self._committed_view_fingerprints())
            ),
            gold_sha256=self._gold_sha256(),
            writer_quiescence=self._writer_quiescence(),
        )

    def apply_operation(self, operation: AdditiveOperation, sql: str) -> None:
        self._validate_artifact_binding()
        if sql != render_operation(operation):
            raise ValueError("backend received non-canonical SQL")
        self.spark.sql(sql)

    def journal_entries(self) -> tuple[JournalEntry, ...]:
        if not bool(self.spark.catalog.tableExists(JOURNAL_TABLE)):
            return ()
        values: list[JournalEntry] = []
        for row in self.spark.table(JOURNAL_TABLE).collect():
            item = self._row_dict(row)
            values.append(
                JournalEntry(
                    journal_id=str(item["journal_id"]),
                    migration_id=str(item["migration_id"]),
                    migration_version=int(item["migration_version"]),
                    status=ApplyStatus(str(item["status"])),
                    plan_sha256=str(item["plan_sha256"]),
                    before_sha256=str(item["before_sha256"]),
                    after_sha256=str(item["after_sha256"]),
                    receipts_sha256=str(item["receipts_sha256"]),
                    previous_evidence_sha256=str(
                        item["previous_evidence_sha256"]
                    ),
                    evidence_sha256=str(item["evidence_sha256"]),
                    error_text=str(item.get("error_text") or ""),
                )
            )
        ordered: list[JournalEntry] = []
        remaining = list(values)
        previous = "0" * 64
        while remaining:
            matches = [
                item
                for item in remaining
                if item.previous_evidence_sha256 == previous
            ]
            if len(matches) != 1:
                raise JournalIntegrityError(
                    "journal rows do not form one append-only evidence chain"
                )
            selected = matches[0]
            ordered.append(selected)
            remaining.remove(selected)
            previous = selected.evidence_sha256
        return tuple(ordered)

    def append_journal(self, entry: JournalEntry) -> None:
        entries = self.journal_entries()
        existing = [item for item in entries if item.journal_id == entry.journal_id]
        if existing:
            if existing == [entry]:
                return
            raise JournalIntegrityError("journal_id conflicts with immutable evidence")
        expected_previous = (
            entries[-1].evidence_sha256 if entries else "0" * 64
        )
        if entry.previous_evidence_sha256 != expected_previous:
            raise JournalIntegrityError("journal append does not extend current chain")
        if evidence_hash(entry.evidence()) != entry.evidence_sha256:
            raise JournalIntegrityError("journal evidence hash is invalid")
        row = entry.to_dict()
        self.spark.createDataFrame(
            [row],
            schema=_TABLE_DEFINITIONS[JOURNAL_TABLE],
        ).write.format("delta").mode("append").saveAsTable(JOURNAL_TABLE)


def status_report(backend: MigrationBackend) -> dict[str, object]:
    snapshot = backend.discover()
    report = compatibility_report(snapshot)
    return {
        "compatibility": report.to_dict(),
        "journal_entries": len(backend.journal_entries()),
        "migration_id": MIGRATION_ID,
        "migration_version": MIGRATION_VERSION,
        "pending_operations": len(build_plan(snapshot).operations),
        "snapshot_sha256": snapshot.sha256,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m people_counter.fabric_production_migration"
    )
    parser.add_argument("command", choices=("plan", "apply", "verify", "status"))
    parser.add_argument(
        "--execute",
        action="store_true",
        help="opt in to writes for apply; otherwise apply is a dry run",
    )
    parser.add_argument("--owner")
    parser.add_argument("--lease-token")
    parser.add_argument("--safety-token")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    backend: MigrationBackend | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    args = build_parser().parse_args(argv)
    output = stdout or sys.stdout
    errors = stderr or sys.stderr
    selected = backend or FakeMigrationBackend()
    if args.command in {"plan", "apply"}:
        plan = build_plan(selected.discover())
        if args.command == "plan" or not args.execute:
            print(plan.to_json(), file=output)
            return 0
        if not args.owner or not args.lease_token or not args.safety_token:
            print(
                "--execute requires --owner, --lease-token, and --safety-token",
                file=errors,
            )
            return 2
        result = apply_plan(
            selected,
            plan,
            owner=args.owner,
            lease_token=args.lease_token,
            safety_token=args.safety_token,
        )
        print(canonical_json(result.to_dict()), file=output)
        return 0 if result.status in {ApplyStatus.APPLIED, ApplyStatus.NOOP} else 1
    if args.command == "verify":
        report = verify_backend(selected)
        print(canonical_json(report.to_dict()), file=output)
        return 0 if report.valid else 1
    print(canonical_json(status_report(selected)), file=output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
