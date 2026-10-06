"""Executable, fail-closed production-shadow contracts for Candidate A.

The module contains no Fabric client construction.  It exposes narrow typed
ports used by the host controller and the installed-wheel Spark jobs.  Every
mutable target is classified structurally and all Candidate A data writes are
confined to the fixed production-shadow namespace.  The only exceptions are
append-only authorization records in the two production auxiliary tables
created by migration ``people_counter_ca_0001``.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any, Protocol

from people_counter.fabric_candidate_a import (
    FABRIC_RUNTIME,
    PRODUCTION_FILES_ROOT,
    PRODUCTION_SHADOW_FILES_ROOT,
    PRODUCTION_SHADOW_TABLE_PREFIX,
    FabricCandidateAConfig,
)
from people_counter.fabric_production_migration import MIGRATION_ID
from people_counter.fabric_production_routing import (
    AllowlistRow,
    RoutingValidationError,
    canonical_bytes,
    sha256_json,
)


SCHEMA_VERSION = 1
PACKAGE_VERSION = "0.9.11"
AUTHORIZATION_MAX_AGE = timedelta(minutes=15)
PRODUCTION_ALLOWLIST_TABLE = "people_counter_ca_routing_allowlist"
PRODUCTION_AUDIT_TABLE = "people_counter_ca_shadow_audit"
SHADOW_PUBLICATIONS_TABLE = (
    f"{PRODUCTION_SHADOW_TABLE_PREFIX}publications"
)
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SAFE_PART = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._=-]{0,127}$")


class ProductionShadowError(RuntimeError):
    """A production-shadow safety or consistency gate failed."""


def _utc(value: datetime, label: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() != timedelta(0)
    ):
        raise ProductionShadowError(f"{label} must be timezone-aware UTC")
    return value


def _hash(value: str, label: str) -> str:
    if not isinstance(value, str) or _HEX64.fullmatch(value) is None:
        raise ProductionShadowError(
            f"{label} must be 64 lowercase hexadecimal characters"
        )
    return value


def _identifier(value: str, label: str) -> str:
    if not isinstance(value, str) or _SAFE_ID.fullmatch(value) is None:
        raise ProductionShadowError(f"{label} is not a safe explicit identifier")
    if value.lower() in {"all", "*"}:
        raise ProductionShadowError(f"{label} cannot select all work")
    return value


def _canonical_mapping(value: Mapping[str, Any], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ProductionShadowError(f"{label} must be an object")
    try:
        encoded = canonical_bytes(dict(value))
        decoded = json.loads(encoded)
    except (RoutingValidationError, TypeError, ValueError) as error:
        raise ProductionShadowError(f"{label} is not canonical JSON") from error
    if not isinstance(decoded, dict):
        raise ProductionShadowError(f"{label} must be an object")
    return decoded


def _frozen_mapping(value: Mapping[str, Any], label: str) -> Mapping[str, Any]:
    return MappingProxyType(_canonical_mapping(value, label))


def _identity_sha(row: AllowlistRow) -> str:
    return sha256_json(row.as_dict())


@dataclass(frozen=True, slots=True)
class ProductionShadowPlan:
    """One expiring canonical plan for exactly one immutable legacy work."""

    plan_id: str
    work: AllowlistRow
    migration_plan_sha256: str
    created_at: datetime
    expires_at: datetime
    legacy_source_rows_sha256: str | None = None
    legacy_output_sha256: str | None = None
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _identifier(self.plan_id, "plan_id")
        if not isinstance(self.work, AllowlistRow):
            raise ProductionShadowError("plan work must be one AllowlistRow")
        _hash(self.migration_plan_sha256, "migration_plan_sha256")
        created = _utc(self.created_at, "created_at")
        expires = _utc(self.expires_at, "expires_at")
        if expires <= created:
            raise ProductionShadowError("plan expiry must be after creation")
        if expires - created > AUTHORIZATION_MAX_AGE:
            raise ProductionShadowError("plan validity exceeds 15 minutes")
        if self.legacy_source_rows_sha256 is not None:
            _hash(
                self.legacy_source_rows_sha256,
                "legacy_source_rows_sha256",
            )
        if self.legacy_output_sha256 is not None:
            _hash(self.legacy_output_sha256, "legacy_output_sha256")
        if self.schema_version != SCHEMA_VERSION:
            raise ProductionShadowError("unsupported shadow plan schema")

    @property
    def work_ids(self) -> tuple[str]:
        return (self.work.work_id,)

    @property
    def identity_sha256(self) -> str:
        return _identity_sha(self.work)

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "plan_id": self.plan_id,
            "migration_id": MIGRATION_ID,
            "migration_plan_sha256": self.migration_plan_sha256,
            "work": self.work.as_dict(),
            "work_identity_sha256": self.identity_sha256,
            "work_ids": [self.work.work_id],
            "legacy_source_rows_sha256": self.legacy_source_rows_sha256,
            "legacy_output_sha256": self.legacy_output_sha256,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "table_prefix": PRODUCTION_SHADOW_TABLE_PREFIX,
            "files_root": PRODUCTION_SHADOW_FILES_ROOT,
        }

    @property
    def sha256(self) -> str:
        return sha256_json(self.as_dict())

    @property
    def safety_token(self) -> str:
        binding = {
            "expires_at": self.expires_at.isoformat(),
            "plan_sha256": self.sha256,
            "work_id": self.work.work_id,
            "work_identity_sha256": self.identity_sha256,
        }
        digest = sha256_json(binding)
        return f"pc-ca-shadow-v1:{self.sha256}:{digest}"

    def validate_at(self, now: datetime) -> None:
        observed = _utc(now, "now")
        if observed < self.created_at or observed >= self.expires_at:
            raise ProductionShadowError("shadow plan is not currently valid")

    def require_token(self, supplied: str) -> None:
        if not isinstance(supplied, str) or not hmac.compare_digest(
            supplied, self.safety_token
        ):
            raise ProductionShadowError(
                "safety token differs from plan/work identities/expiry binding"
            )


@dataclass(frozen=True, slots=True)
class SyntheticShadowPlan:
    """Reviewed shadow-only execution with no legacy-comparison claim."""

    plan_id: str
    work: AllowlistRow
    payload: Mapping[str, Any]
    source_evidence_sha256: str
    created_at: datetime
    expires_at: datetime
    plan_type: str = "SHADOW_SYNTHETIC"
    comparison_mode: str = "NO_LEGACY_BASELINE"

    def __post_init__(self) -> None:
        _identifier(self.plan_id, "synthetic plan_id")
        if not isinstance(self.work, AllowlistRow):
            raise ProductionShadowError(
                "synthetic plan work must be one AllowlistRow"
            )
        _hash(self.source_evidence_sha256, "synthetic source evidence")
        _frozen_mapping(self.payload, "synthetic payload")
        created = _utc(self.created_at, "synthetic created_at")
        expires = _utc(self.expires_at, "synthetic expires_at")
        if expires <= created or expires - created > timedelta(hours=2):
            raise ProductionShadowError(
                "synthetic plan validity must be positive and at most two hours"
            )
        if self.plan_type != "SHADOW_SYNTHETIC":
            raise ProductionShadowError("synthetic plan type differs")
        if self.comparison_mode != "NO_LEGACY_BASELINE":
            raise ProductionShadowError("synthetic comparison mode differs")

    @property
    def identity_sha256(self) -> str:
        return _identity_sha(self.work)

    def as_dict(self) -> dict[str, object]:
        return {
            "plan_type": self.plan_type,
            "comparison_mode": self.comparison_mode,
            "plan_id": self.plan_id,
            "work": self.work.as_dict(),
            "work_identity_sha256": self.identity_sha256,
            "payload": json.loads(canonical_bytes(dict(self.payload))),
            "source_evidence_sha256": self.source_evidence_sha256,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "table_prefix": PRODUCTION_SHADOW_TABLE_PREFIX,
            "files_root": PRODUCTION_SHADOW_FILES_ROOT,
        }

    @property
    def sha256(self) -> str:
        return sha256_json(self.as_dict())

    @property
    def safety_token(self) -> str:
        return (
            f"pc-ca-shadow-synthetic-v1:{self.sha256}:"
            f"{sha256_json({'comparison_mode': self.comparison_mode, 'expires_at': self.expires_at.isoformat(), 'plan_sha256': self.sha256, 'work_identity_sha256': self.identity_sha256})}"
        )

    def validate_at(self, now: datetime) -> None:
        observed = _utc(now, "now")
        if observed < self.created_at or observed >= self.expires_at:
            raise ProductionShadowError("synthetic plan is not currently valid")

    def require_token(self, supplied: str) -> None:
        if not isinstance(supplied, str) or not hmac.compare_digest(
            supplied, self.safety_token
        ):
            raise ProductionShadowError("synthetic safety token differs")

    def registration_payload(self) -> dict[str, object]:
        return {
            "work_id": self.work.work_id,
            **self.work.as_dict(),
            "source_rows_sha256": self.source_evidence_sha256,
            "comparison_mode": self.comparison_mode,
            "payload": json.loads(canonical_bytes(dict(self.payload))),
        }


@dataclass(frozen=True, slots=True)
class ReviewReceipt:
    """Separate operator review receipt that must be stored with mode 0600."""

    plan_sha256: str
    work_id: str
    work_identity_sha256: str
    expires_at: datetime
    reviewed_at: datetime
    reviewer: str
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _hash(self.plan_sha256, "plan_sha256")
        _identifier(self.work_id, "work_id")
        _hash(self.work_identity_sha256, "work_identity_sha256")
        _utc(self.expires_at, "expires_at")
        _utc(self.reviewed_at, "reviewed_at")
        _identifier(self.reviewer, "reviewer")
        if self.reviewed_at >= self.expires_at:
            raise ProductionShadowError("review receipt is already expired")
        if self.schema_version != SCHEMA_VERSION:
            raise ProductionShadowError("unsupported review receipt schema")

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "plan_sha256": self.plan_sha256,
            "work_id": self.work_id,
            "work_identity_sha256": self.work_identity_sha256,
            "expires_at": self.expires_at.isoformat(),
            "reviewed_at": self.reviewed_at.isoformat(),
            "reviewer": self.reviewer,
        }

    def validate(
        self,
        plan: ProductionShadowPlan,
        *,
        now: datetime,
        file_mode: int,
    ) -> None:
        if file_mode & 0o777 != 0o600:
            raise ProductionShadowError("review receipt must have mode 0600")
        plan.validate_at(now)
        expected = (
            plan.sha256,
            plan.work.work_id,
            plan.identity_sha256,
            plan.expires_at,
        )
        observed = (
            self.plan_sha256,
            self.work_id,
            self.work_identity_sha256,
            self.expires_at,
        )
        if observed != expected:
            raise ProductionShadowError(
                "review receipt differs from canonical plan identity or expiry"
            )
        if self.reviewed_at < plan.created_at or self.reviewed_at > now:
            raise ProductionShadowError("review receipt timestamp is invalid")


@dataclass(frozen=True, slots=True)
class MigrationJournalProof:
    migration_id: str
    status: str
    plan_sha256: str
    journal_sha256: str

    def __post_init__(self) -> None:
        if self.migration_id != MIGRATION_ID:
            raise ProductionShadowError("migration journal ID is not reviewed")
        if self.status not in {"APPLIED", "NOOP"}:
            raise ProductionShadowError("migration journal is not successful")
        _hash(self.plan_sha256, "migration journal plan_sha256")
        _hash(self.journal_sha256, "migration journal sha256")


@dataclass(frozen=True, slots=True)
class ShadowQuiescence:
    """Fresh exact proof: inactive Reflex, no writers/leases, unowned lock."""

    observed_at: datetime
    reflex_id: str
    reflex_active: bool
    active_writer_ids: tuple[str, ...] = ()
    active_lease_ids: tuple[str, ...] = ()
    control_owner_id: str | None = None

    def __post_init__(self) -> None:
        _utc(self.observed_at, "quiescence observed_at")
        _identifier(self.reflex_id, "reflex_id")
        object.__setattr__(self, "active_writer_ids", tuple(self.active_writer_ids))
        object.__setattr__(self, "active_lease_ids", tuple(self.active_lease_ids))

    def validate(self, *, expected_reflex_id: str, now: datetime) -> None:
        current = _utc(now, "now")
        age = current - self.observed_at
        if age < timedelta(0) or age > timedelta(minutes=5):
            raise ProductionShadowError("quiescence proof is not fresh")
        if self.reflex_id != expected_reflex_id or self.reflex_active is not False:
            raise ProductionShadowError("exact Reflex must be inactive")
        if (
            self.active_writer_ids
            or self.active_lease_ids
            or self.control_owner_id is not None
        ):
            raise ProductionShadowError(
                "authorization requires no writers, no leases, and unowned control"
            )


@dataclass(frozen=True, slots=True)
class AuthorizationRow:
    authorization_id: str
    plan_sha256: str
    work: AllowlistRow
    work_identity_sha256: str
    expires_at: datetime
    authorized_at: datetime
    safety_token_sha256: str
    migration_journal_sha256: str
    reviewer: str

    def as_dict(self) -> dict[str, object]:
        return {
            "authorization_id": self.authorization_id,
            "plan_sha256": self.plan_sha256,
            **self.work.as_dict(),
            "work_identity_sha256": self.work_identity_sha256,
            "expires_at": self.expires_at.isoformat(),
            "authorized_at": self.authorized_at.isoformat(),
            "safety_token_sha256": self.safety_token_sha256,
            "migration_journal_sha256": self.migration_journal_sha256,
            "reviewer": self.reviewer,
        }


class AuthorizationStore(Protocol):
    """Serialized append/readback port for the two production auxiliary tables.

    Implementations use an immutable intent ledger plus the exact global
    ``people_counter_control_writer`` CAS.  Delta does not provide a
    cross-table transaction: the protocol is a serialized, idempotent
    two-table commit and must not be described as ACID.
    """

    def read_allowlist(self, work_id: str) -> Sequence[Mapping[str, Any]]: ...

    def append_allowlist(self, row: Mapping[str, Any]) -> None: ...

    def read_audit(self, authorization_id: str) -> Sequence[Mapping[str, Any]]: ...

    def append_audit(self, row: Mapping[str, Any]) -> None: ...

    def append_authorization(
        self,
        allowlist: Mapping[str, Any],
        audit: Mapping[str, Any],
    ) -> None:
        """Atomically append both rows or neither with deterministic keys."""
        ...

    def recover_partial_authorization(
        self,
        row: AuthorizationRow,
        allowlist: Mapping[str, Any],
        audit: Mapping[str, Any],
    ) -> None:
        """Finish only an intent-proven partial serialized authorization."""
        ...


class AuthorizationProtocolState(str, Enum):
    """Observable states of the serialized two-table authorization protocol."""

    EMPTY = "EMPTY"
    PREPARED = "PREPARED"
    ALLOWLIST_APPENDED = "ALLOWLIST_APPENDED"
    AUDIT_APPENDED = "AUDIT_APPENDED"
    COMMITTED = "COMMITTED"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True, slots=True)
class AuthorizationIntent:
    """Immutable identity for one recoverable two-table authorization."""

    authorization_id: str
    work_id: str
    plan_sha256: str
    work_identity_sha256: str
    allowlist_sha256: str
    audit_sha256: str

    def __post_init__(self) -> None:
        _hash(self.authorization_id, "authorization_id")
        _identifier(self.work_id, "intent work_id")
        for label, value in (
            ("intent plan_sha256", self.plan_sha256),
            ("intent work_identity_sha256", self.work_identity_sha256),
            ("intent allowlist_sha256", self.allowlist_sha256),
            ("intent audit_sha256", self.audit_sha256),
        ):
            _hash(value, label)

    def as_dict(self) -> dict[str, str]:
        return {
            "authorization_id": self.authorization_id,
            "work_id": self.work_id,
            "plan_sha256": self.plan_sha256,
            "work_identity_sha256": self.work_identity_sha256,
            "allowlist_sha256": self.allowlist_sha256,
            "audit_sha256": self.audit_sha256,
        }


def authorization_intent(
    row: AuthorizationRow,
    allowlist: Mapping[str, Any],
    audit: Mapping[str, Any],
) -> AuthorizationIntent:
    """Build the exact immutable recovery identity for an authorization."""

    return AuthorizationIntent(
        authorization_id=row.authorization_id,
        work_id=row.work.work_id,
        plan_sha256=row.plan_sha256,
        work_identity_sha256=row.work_identity_sha256,
        allowlist_sha256=sha256_json(dict(allowlist)),
        audit_sha256=sha256_json(dict(audit)),
    )


def classify_authorization_protocol(
    intent: AuthorizationIntent | None,
    expected_intent: AuthorizationIntent,
    *,
    allowlist_exact: bool,
    allowlist_present: bool,
    audit_exact: bool,
    audit_present: bool,
) -> AuthorizationProtocolState:
    """Classify one authorization without guessing or repairing conflicts.

    A partial state is recoverable only when the immutable intent is present
    and exactly proves the reviewed operation.  Rows that are present but not
    exact are always conflicts.
    """

    if allowlist_present and not allowlist_exact:
        return AuthorizationProtocolState.CONFLICT
    if audit_present and not audit_exact:
        return AuthorizationProtocolState.CONFLICT
    if intent is not None and intent != expected_intent:
        return AuthorizationProtocolState.CONFLICT
    if intent is None:
        if allowlist_present or audit_present:
            return AuthorizationProtocolState.CONFLICT
        return AuthorizationProtocolState.EMPTY
    if allowlist_exact and audit_exact:
        return AuthorizationProtocolState.COMMITTED
    if allowlist_exact:
        return AuthorizationProtocolState.ALLOWLIST_APPENDED
    if audit_exact:
        return AuthorizationProtocolState.AUDIT_APPENDED
    return AuthorizationProtocolState.PREPARED


@dataclass(frozen=True, slots=True)
class AuthorizationResult:
    row: AuthorizationRow
    allowlist_appended: bool
    audit_appended: bool

    @property
    def replayed(self) -> bool:
        return not self.allowlist_appended and not self.audit_appended


def _authorization_row(
    plan: ProductionShadowPlan,
    receipt: ReviewReceipt,
    proof: MigrationJournalProof,
    safety_token: str,
) -> AuthorizationRow:
    authorization_id = sha256_json(
        {
            "plan_sha256": plan.sha256,
            "work_id": plan.work.work_id,
            "work_identity_sha256": plan.identity_sha256,
            "expires_at": plan.expires_at.isoformat(),
        }
    )
    return AuthorizationRow(
        authorization_id=authorization_id,
        plan_sha256=plan.sha256,
        work=plan.work,
        work_identity_sha256=plan.identity_sha256,
        expires_at=plan.expires_at,
        authorized_at=receipt.reviewed_at,
        safety_token_sha256=hashlib.sha256(safety_token.encode()).hexdigest(),
        migration_journal_sha256=proof.journal_sha256,
        reviewer=receipt.reviewer,
    )


def _allowlist_payload(row: AuthorizationRow) -> dict[str, object]:
    return {
        **row.work.as_dict(),
        "plan_sha256": row.plan_sha256,
        "approved_at": row.authorized_at.isoformat(),
        "approved_by": row.reviewer,
    }


def _audit_payload(row: AuthorizationRow) -> dict[str, object]:
    return {
        "audit_id": row.authorization_id,
        "work_id": row.work.work_id,
        "plan_sha256": row.plan_sha256,
        "shadow_attempt_id": "AUTHORIZATION",
        "legacy_attempt_id": row.work_identity_sha256,
        "comparison_sha256": row.safety_token_sha256,
        "critical_findings": 0,
        "recorded_at": row.authorized_at.isoformat(),
    }


def _exact_existing(
    observed: Sequence[Mapping[str, Any]],
    expected: Mapping[str, Any],
    label: str,
) -> bool:
    rows = [_canonical_mapping(row, label) for row in observed]
    if not rows:
        return False
    if len(rows) != 1 or rows[0] != dict(expected):
        raise ProductionShadowError(f"{label} conflicts with reviewed authorization")
    return True


def _append_and_readback(
    *,
    read: Any,
    append: Any,
    key: str,
    expected: Mapping[str, Any],
    label: str,
) -> bool:
    if _exact_existing(read(key), expected, label):
        return False
    append(dict(expected))
    if not _exact_existing(read(key), expected, label):
        raise ProductionShadowError(f"{label} append readback failed")
    return True


def _require_exact_authorization_selection(
    plan: ProductionShadowPlan,
    selected_work_ids: Sequence[str],
    observed_work: AllowlistRow,
) -> None:
    selected = tuple(selected_work_ids)
    if len(selected) != 1:
        raise ProductionShadowError("authorization requires EXACTLY ONE work ID")
    _identifier(selected[0], "work_id")
    if selected != plan.work_ids:
        raise ProductionShadowError("selected work ID differs from canonical plan")
    if observed_work != plan.work:
        raise ProductionShadowError(
            "camera/location/model/source/config identity hashes differ"
        )


def authorize_production_shadow(
    plan: ProductionShadowPlan,
    *,
    selected_work_ids: Sequence[str],
    observed_work: AllowlistRow,
    safety_token: str,
    receipt: ReviewReceipt,
    receipt_mode: int,
    migration: MigrationJournalProof,
    quiescence: ShadowQuiescence,
    expected_reflex_id: str,
    store: AuthorizationStore,
    now: datetime,
) -> AuthorizationResult:
    """Authorize exactly one work and append/read back both fixed rows.

    An exact replay is a no-op.  Any row with the same logical key but
    different canonical content is a hard conflict.
    """

    _require_exact_authorization_selection(
        plan, selected_work_ids, observed_work
    )
    current = _utc(now, "now")
    plan.require_token(safety_token)
    receipt.validate(plan, now=current, file_mode=receipt_mode)
    if migration.plan_sha256 != plan.migration_plan_sha256:
        raise ProductionShadowError("migration journal plan hash differs")
    quiescence.validate(expected_reflex_id=expected_reflex_id, now=current)
    row = _authorization_row(plan, receipt, migration, safety_token)
    allowlist = _allowlist_payload(row)
    audit = _audit_payload(row)
    allowlist_exists = _exact_existing(
        store.read_allowlist(row.work.work_id),
        allowlist,
        PRODUCTION_ALLOWLIST_TABLE,
    )
    audit_exists = _exact_existing(
        store.read_audit(row.authorization_id),
        audit,
        PRODUCTION_AUDIT_TABLE,
    )
    if allowlist_exists != audit_exists:
        recover = getattr(store, "recover_partial_authorization", None)
        if not callable(recover):
            raise ProductionShadowError(
                "authorization rows are partially present; "
                "refusing non-atomic repair"
            )
        recover(row, allowlist, audit)
        if not _exact_existing(
            store.read_allowlist(row.work.work_id),
            allowlist,
            PRODUCTION_ALLOWLIST_TABLE,
        ) or not _exact_existing(
            store.read_audit(row.authorization_id),
            audit,
            PRODUCTION_AUDIT_TABLE,
        ):
            raise ProductionShadowError(
                "partial authorization recovery readback failed"
            )
        return AuthorizationResult(row, not allowlist_exists, not audit_exists)
    if not allowlist_exists:
        store.append_authorization(allowlist, audit)
        if not _exact_existing(
            store.read_allowlist(row.work.work_id),
            allowlist,
            PRODUCTION_ALLOWLIST_TABLE,
        ) or not _exact_existing(
            store.read_audit(row.authorization_id),
            audit,
            PRODUCTION_AUDIT_TABLE,
        ):
            raise ProductionShadowError("atomic authorization readback failed")
    allowlist_appended = not allowlist_exists
    audit_appended = not audit_exists
    return AuthorizationResult(row, allowlist_appended, audit_appended)


class TargetClass(str, Enum):
    SHADOW_TABLE = "SHADOW_TABLE"
    SHADOW_PUBLICATION_LEDGER = "SHADOW_PUBLICATION_LEDGER"
    SHADOW_FILE = "SHADOW_FILE"
    SHADOW_ATTEMPT_POINTER = "SHADOW_ATTEMPT_POINTER"
    PRODUCTION_AUX_APPEND = "PRODUCTION_AUX_APPEND"
    LEGACY_PRODUCTION = "LEGACY_PRODUCTION"
    OUTSIDE = "OUTSIDE"


class WriteOperation(str, Enum):
    CREATE = "CREATE"
    APPEND = "APPEND"
    REPLACE = "REPLACE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


@dataclass(frozen=True, slots=True)
class ClassifiedTarget:
    name: str
    target_class: TargetClass


def _canonical_shadow_relative(path: str) -> str | None:
    if not path.startswith(PRODUCTION_SHADOW_FILES_ROOT):
        return None
    relative = path[len(PRODUCTION_SHADOW_FILES_ROOT) :]
    pure = PurePosixPath(relative)
    if (
        not relative
        or pure.is_absolute()
        or "\\" in relative
        or "://" in relative
        or pure.as_posix() != relative
        or any(
            part in {"", ".", ".."} or _SAFE_PART.fullmatch(part) is None
            for part in pure.parts
        )
    ):
        return None
    return relative


def classify_target(name: str) -> ClassifiedTarget:
    """Classify exact typed targets; no substring heuristics are used."""

    if not isinstance(name, str) or not name:
        return ClassifiedTarget(str(name), TargetClass.OUTSIDE)
    if name in {PRODUCTION_ALLOWLIST_TABLE, PRODUCTION_AUDIT_TABLE}:
        return ClassifiedTarget(name, TargetClass.PRODUCTION_AUX_APPEND)
    if name == SHADOW_PUBLICATIONS_TABLE:
        return ClassifiedTarget(name, TargetClass.SHADOW_PUBLICATION_LEDGER)
    if name.startswith(PRODUCTION_SHADOW_TABLE_PREFIX):
        suffix = name[len(PRODUCTION_SHADOW_TABLE_PREFIX) :]
        if re.fullmatch(r"[a-z][a-z0-9_]{0,62}", suffix):
            return ClassifiedTarget(name, TargetClass.SHADOW_TABLE)
        return ClassifiedTarget(name, TargetClass.OUTSIDE)
    relative = _canonical_shadow_relative(name)
    if relative is not None:
        parts = PurePosixPath(relative).parts
        pointer = (
            len(parts) >= 4
            and parts[0] == "attempts"
            and parts[-1] == "pointer.json"
            and parts[1].startswith("work=")
            and parts[2].startswith("attempt=")
        )
        kind = (
            TargetClass.SHADOW_ATTEMPT_POINTER
            if pointer
            else TargetClass.SHADOW_FILE
        )
        return ClassifiedTarget(name, kind)
    if name.startswith("people_counter_") or name.startswith(PRODUCTION_FILES_ROOT):
        return ClassifiedTarget(name, TargetClass.LEGACY_PRODUCTION)
    return ClassifiedTarget(name, TargetClass.OUTSIDE)


def require_write_target(
    name: str,
    operation: WriteOperation | str,
) -> ClassifiedTarget:
    target = classify_target(name)
    try:
        requested = WriteOperation(operation)
    except ValueError as error:
        raise ProductionShadowError("unsupported write operation") from error
    if target.target_class in {TargetClass.LEGACY_PRODUCTION, TargetClass.OUTSIDE}:
        raise ProductionShadowError("write target is outside the shadow boundary")
    if (
        target.target_class is TargetClass.PRODUCTION_AUX_APPEND
        and requested is not WriteOperation.APPEND
    ):
        raise ProductionShadowError(
            "production auxiliary authorization tables are append-only"
        )
    return target


@dataclass(frozen=True, slots=True)
class LegacySourceRows:
    """Exact legacy rows needed to ingest one already-succeeded work."""

    work: Mapping[str, Any]
    attempt: Mapping[str, Any]
    publication: Mapping[str, Any]
    committed_view: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class LegacySourceSnapshot:
    work_id: str
    attempt_id: str
    output_path: str
    output_sha256: str
    identity: AllowlistRow
    payload: Mapping[str, Any]
    row_hashes: Mapping[str, str]
    source_rows_sha256: str

    def registration_payload(self) -> dict[str, object]:
        """Copy immutable identity only; never return mutable legacy rows."""

        return {
            "work_id": self.work_id,
            "legacy_attempt_id": self.attempt_id,
            "legacy_output_path": self.output_path,
            "legacy_output_sha256": self.output_sha256,
            **self.identity.as_dict(),
            "source_rows_sha256": self.source_rows_sha256,
            "payload": json.loads(canonical_bytes(dict(self.payload))),
        }


def _same(name: str, rows: Sequence[Mapping[str, Any]], key: str) -> object:
    values = {row.get(key) for row in rows}
    if len(values) != 1:
        raise ProductionShadowError(f"legacy {key} differs across {name}")
    return next(iter(values))


def _legacy_hashes(rows: Mapping[str, Mapping[str, Any]]) -> dict[str, str]:
    return {name: sha256_json(dict(value)) for name, value in sorted(rows.items())}


def ingest_legacy_source(rows: LegacySourceRows) -> LegacySourceSnapshot:
    """Read and validate one immutable SUCCEEDED legacy route."""

    copied = {
        "work": _canonical_mapping(rows.work, "legacy work"),
        "attempt": _canonical_mapping(rows.attempt, "legacy attempt"),
        "publication": _canonical_mapping(rows.publication, "legacy publication"),
        "committed_view": _canonical_mapping(
            rows.committed_view, "legacy committed view"
        ),
    }
    sequence = tuple(copied.values())
    work_id = str(_same("source rows", sequence, "work_id"))
    _identifier(work_id, "legacy work_id")
    attempt_id = str(_same("source rows", sequence, "attempt_id"))
    _identifier(attempt_id, "legacy attempt_id")
    if copied["work"].get("status") != "SUCCEEDED":
        raise ProductionShadowError("legacy work is not SUCCEEDED")
    if copied["attempt"].get("status") != "SUCCEEDED":
        raise ProductionShadowError("legacy attempt is not SUCCEEDED")
    if copied["work"].get("committed_attempt_id") != attempt_id:
        raise ProductionShadowError("legacy work pointer differs from attempt")
    for key in ("output_path", "output_sha256"):
        _same("source rows", sequence[1:], key)
    output_path = str(copied["attempt"]["output_path"])
    if not output_path.startswith(PRODUCTION_FILES_ROOT):
        raise ProductionShadowError("legacy output path is not production-owned")
    output_sha256 = _hash(
        str(copied["attempt"]["output_sha256"]), "legacy output_sha256"
    )
    identity = AllowlistRow(
        work_id=work_id,
        camera_sha256=str(_same("source rows", sequence, "camera_sha256")),
        location_sha256=str(_same("source rows", sequence, "location_sha256")),
        model_sha256=str(_same("source rows", sequence, "model_sha256")),
        source_sha256=str(_same("source rows", sequence, "source_sha256")),
        config_sha256=str(_same("source rows", sequence, "config_sha256")),
    )
    payload = copied["work"].get("payload")
    if not isinstance(payload, Mapping):
        raise ProductionShadowError("legacy work payload is missing")
    hashes = _legacy_hashes(copied)
    return LegacySourceSnapshot(
        work_id=work_id,
        attempt_id=attempt_id,
        output_path=output_path,
        output_sha256=output_sha256,
        identity=identity,
        payload=_frozen_mapping(payload, "legacy payload"),
        row_hashes=MappingProxyType(hashes),
        source_rows_sha256=sha256_json(hashes),
    )


def prove_legacy_unchanged(
    before: LegacySourceSnapshot,
    after: LegacySourceSnapshot,
) -> Mapping[str, object]:
    if (
        before.work_id != after.work_id
        or before.row_hashes != after.row_hashes
        or before.source_rows_sha256 != after.source_rows_sha256
    ):
        raise ProductionShadowError("legacy source hashes changed during shadow flow")
    return MappingProxyType(
        {
            "work_id": before.work_id,
            "before_sha256": before.source_rows_sha256,
            "after_sha256": after.source_rows_sha256,
            "unchanged": True,
        }
    )


def require_plan_pinned_legacy(
    plan: ProductionShadowPlan,
    source: LegacySourceSnapshot,
) -> LegacySourceSnapshot:
    """Require the exact reviewed route and every critical legacy hash."""

    expected = (
        plan.work.work_id,
        plan.work,
        plan.legacy_source_rows_sha256,
        plan.legacy_output_sha256,
    )
    observed = (
        source.work_id,
        source.identity,
        source.source_rows_sha256,
        source.output_sha256,
    )
    if expected != observed:
        raise ProductionShadowError(
            "plan-pinned legacy work/identity/source/output hashes drifted"
        )
    return source


_SHADOW_SCHEMAS: Mapping[str, tuple[tuple[str, str, bool], ...]] = MappingProxyType(
    {
        "locks": (
            ("lock_name", "string", False),
            ("owner_id", "string", True),
            ("acquired_at", "timestamp", True),
        ),
        "work": (
            ("work_id", "string", False),
            ("payload_json", "string", False),
            ("payload_sha256", "string", False),
            ("runtime_key", "string", False),
            ("duration_seconds", "double", False),
            ("config_sha256", "string", False),
            ("release_digest", "string", False),
            ("status", "string", False),
            ("attempt_count", "long", False),
            ("max_attempts", "long", False),
            ("original_max_attempts", "long", False),
            ("fence", "long", False),
            ("available_at", "double", False),
            ("lease_owner", "string", True),
            ("lease_attempt_id", "string", True),
            ("lease_expires_at", "double", True),
            ("committed_attempt_id", "string", True),
            ("publication_sequence", "long", True),
            ("replay_generation", "long", False),
            ("last_replay_id", "string", True),
            ("last_error", "string", True),
            ("created_at", "double", False),
            ("updated_at", "double", False),
        ),
        "batches": (
            ("batch_id", "string", False),
            ("owner", "string", False),
            ("runtime_key", "string", False),
            ("status", "string", False),
            ("lease_expires_at", "double", False),
            ("item_count", "long", False),
            ("membership_sha256", "string", False),
            ("envelope_version", "long", False),
            ("envelope_path", "string", False),
            ("envelope_sha256", "string", False),
            ("created_at", "double", False),
            ("sealed_at", "double", True),
            ("committed_at", "double", True),
        ),
        "batch_members": (
            ("batch_id", "string", False),
            ("ordinal", "long", False),
            ("work_id", "string", False),
            ("attempt_id", "string", False),
            ("fence", "long", False),
            ("payload_sha256", "string", False),
        ),
        "attempts": (
            ("attempt_id", "string", False),
            ("work_id", "string", False),
            ("batch_id", "string", False),
            ("fence", "long", False),
            ("status", "string", False),
            ("lease_expires_at", "double", False),
            ("payload_sha256", "string", False),
            ("output_path", "string", True),
            ("output_sha256", "string", True),
            ("terminal_succeeded", "boolean", True),
            ("records_json", "string", True),
            ("recovery_outcome", "string", True),
            ("created_at", "double", False),
            ("sealed_at", "double", True),
        ),
        "publications": (
            ("publication_sequence", "long", False),
            ("work_id", "string", False),
            ("attempt_id", "string", False),
            ("batch_id", "string", False),
            ("output_path", "string", False),
            ("output_sha256", "string", False),
            ("published_at", "double", False),
        ),
        "replay_requests": (
            ("replay_id", "string", False),
            ("work_id", "string", False),
            ("operator", "string", False),
            ("reason", "string", False),
            ("generation", "long", False),
            ("requested_at", "double", False),
        ),
        "reconciliation_findings": (
            ("finding_id", "string", False),
            ("finding_type", "string", False),
            ("severity", "string", False),
            ("entity_key", "string", False),
            ("details_json", "string", False),
            ("first_seen_at", "double", False),
            ("last_seen_at", "double", False),
            ("resolved_at", "double", True),
        ),
    }
)
SHADOW_SCHEMAS: Mapping[str, tuple[tuple[str, str, bool], ...]] = MappingProxyType(
    {
        suffix: tuple((name, data_type, True) for name, data_type, _ in columns)
        for suffix, columns in _SHADOW_SCHEMAS.items()
    }
)


class ShadowBootstrapBackend(Protocol):
    def table_schema(
        self, table_name: str
    ) -> Sequence[tuple[str, str, bool]] | None: ...

    def create_table(
        self,
        table_name: str,
        schema: Sequence[tuple[str, str, bool]],
    ) -> None: ...


def bootstrap_production_shadow(
    backend: ShadowBootstrapBackend,
) -> tuple[str, ...]:
    """Create absent shadow tables and prove every exact schema by readback."""

    config = FabricCandidateAConfig.production_shadow()
    created: list[str] = []
    for suffix, schema in SHADOW_SCHEMAS.items():
        table = config.table(suffix)
        observed = backend.table_schema(table)
        if observed is None:
            backend.create_table(table, schema)
            created.append(table)
            observed = backend.table_schema(table)
        if tuple(observed or ()) != tuple(schema):
            raise ProductionShadowError(f"shadow schema readback differs for {table}")
    return tuple(created)


@dataclass(frozen=True, slots=True)
class RuntimeProvenance:
    package_version: str
    package_sha256: str
    python_version: str
    spark_version: str
    java_version: str
    fabric_runtime: str = FABRIC_RUNTIME

    def __post_init__(self) -> None:
        if self.package_version != PACKAGE_VERSION:
            raise ProductionShadowError("installed package must be version 0.9.11")
        _hash(self.package_sha256, "package_sha256")
        if self.fabric_runtime != FABRIC_RUNTIME:
            raise ProductionShadowError("Fabric Runtime provenance differs")
        if not self.python_version.startswith("3.13"):
            raise ProductionShadowError("Python runtime provenance differs")
        if not self.spark_version.startswith("4.1.1"):
            raise ProductionShadowError("Spark runtime provenance differs")
        if not self.java_version.startswith("21"):
            raise ProductionShadowError("Java runtime provenance differs")


@dataclass(frozen=True, slots=True)
class CommittedRoute:
    work_id: str
    attempt_id: str
    logical_identity_sha256: str
    fence: int
    pointer_fence: int
    output_path: str
    output_sha256: str
    sealed: bool
    committed: bool
    pointer_attempt_id: str
    publication_sequence: int
    publication_count: int
    authorization_id: str
    plan_sha256: str
    provenance: RuntimeProvenance
    identity: AllowlistRow
    records: tuple[Mapping[str, Any], ...]
    logical_total: float
    frame_count: int
    timestamp: datetime

    def __post_init__(self) -> None:
        _identifier(self.work_id, "route work_id")
        _identifier(self.attempt_id, "route attempt_id")
        _hash(self.logical_identity_sha256, "logical_identity_sha256")
        _hash(self.output_sha256, "route output_sha256")
        _hash(self.authorization_id, "route authorization_id")
        _hash(self.plan_sha256, "route plan_sha256")
        if type(self.fence) is not int or type(self.pointer_fence) is not int:
            raise ProductionShadowError("route fences must be integers")
        if type(self.publication_sequence) is not int:
            raise ProductionShadowError("publication sequence must be an integer")
        if type(self.publication_count) is not int:
            raise ProductionShadowError("publication count must be an integer")
        if type(self.frame_count) is not int or self.frame_count < 0:
            raise ProductionShadowError("frame count must be nonnegative")
        if not math.isfinite(float(self.logical_total)):
            raise ProductionShadowError("logical total must be finite")
        _utc(self.timestamp, "route timestamp")
        object.__setattr__(
            self,
            "records",
            tuple(_frozen_mapping(item, "output record") for item in self.records),
        )


class ShadowExecutionAdapter(Protocol):
    """Proven Candidate A adapter surface, parameterized by fixed config."""

    def committed_route(
        self, work_id: str, *, config: FabricCandidateAConfig
    ) -> CommittedRoute | None: ...

    def register(
        self,
        registration: Mapping[str, Any],
        *,
        config: FabricCandidateAConfig,
    ) -> None: ...

    def claim(
        self,
        work_id: str,
        *,
        config: FabricCandidateAConfig,
    ) -> Mapping[str, Any]: ...

    def process(
        self,
        claim: Mapping[str, Any],
        *,
        config: FabricCandidateAConfig,
        provenance: RuntimeProvenance,
    ) -> Mapping[str, Any]: ...

    def seal(
        self,
        claim: Mapping[str, Any],
        output: Mapping[str, Any],
        *,
        config: FabricCandidateAConfig,
    ) -> None: ...

    def publish(
        self,
        claim: Mapping[str, Any],
        *,
        authorization: AuthorizationRow,
        config: FabricCandidateAConfig,
        provenance: RuntimeProvenance,
    ) -> CommittedRoute: ...


def execute_production_shadow(
    source: LegacySourceSnapshot,
    authorization: AuthorizationResult,
    adapter: ShadowExecutionAdapter,
    provenance: RuntimeProvenance,
) -> CommittedRoute:
    """Register/claim/process/seal/publish once, reusing an existing commit."""

    if source.work_id != authorization.row.work.work_id:
        raise ProductionShadowError("source and authorization work IDs differ")
    if source.identity != authorization.row.work:
        raise ProductionShadowError("source and authorization identities differ")
    config = FabricCandidateAConfig.production_shadow()
    existing = adapter.committed_route(source.work_id, config=config)
    if existing is not None:
        _validate_committed_route(existing, authorization.row, config)
        return existing
    adapter.register(source.registration_payload(), config=config)
    claim = _canonical_mapping(
        adapter.claim(source.work_id, config=config), "shadow claim"
    )
    if claim.get("work_id") != source.work_id:
        raise ProductionShadowError("claim returned a different work ID")
    output = _canonical_mapping(
        adapter.process(
            claim,
            config=config,
            provenance=provenance,
        ),
        "shadow output",
    )
    adapter.seal(claim, output, config=config)
    route = adapter.publish(
        claim,
        authorization=authorization.row,
        config=config,
        provenance=provenance,
    )
    _validate_committed_route(route, authorization.row, config)
    return route


def execute_synthetic_shadow(
    plan: SyntheticShadowPlan,
    adapter: ShadowExecutionAdapter,
    provenance: RuntimeProvenance,
    *,
    reviewed_at: datetime,
    reviewer: str,
) -> CommittedRoute:
    """Execute one reviewed synthetic route without production authorization."""

    synthetic_authorization = AuthorizationRow(
        authorization_id=sha256_json(
            {
                "comparison_mode": plan.comparison_mode,
                "plan_sha256": plan.sha256,
                "work_identity_sha256": plan.identity_sha256,
            }
        ),
        plan_sha256=plan.sha256,
        work=plan.work,
        work_identity_sha256=plan.identity_sha256,
        expires_at=plan.expires_at,
        authorized_at=_utc(reviewed_at, "synthetic reviewed_at"),
        safety_token_sha256=hashlib.sha256(
            plan.safety_token.encode()
        ).hexdigest(),
        migration_journal_sha256=plan.source_evidence_sha256,
        reviewer=_identifier(reviewer, "synthetic reviewer"),
    )
    config = FabricCandidateAConfig.production_shadow()
    existing = adapter.committed_route(plan.work.work_id, config=config)
    if existing is not None:
        _validate_committed_route(existing, synthetic_authorization, config)
        return existing
    adapter.register(plan.registration_payload(), config=config)
    claim = _canonical_mapping(
        adapter.claim(plan.work.work_id, config=config), "synthetic shadow claim"
    )
    if claim.get("work_id") != plan.work.work_id:
        raise ProductionShadowError("synthetic claim returned a different work ID")
    output = _canonical_mapping(
        adapter.process(claim, config=config, provenance=provenance),
        "synthetic shadow output",
    )
    adapter.seal(claim, output, config=config)
    route = adapter.publish(
        claim,
        authorization=synthetic_authorization,
        config=config,
        provenance=provenance,
    )
    _validate_committed_route(route, synthetic_authorization, config)
    return route


def _validate_committed_route(
    route: CommittedRoute,
    authorization: AuthorizationRow,
    config: FabricCandidateAConfig,
) -> None:
    if route.work_id != authorization.work.work_id:
        raise ProductionShadowError("published route work ID differs")
    if route.identity != authorization.work:
        raise ProductionShadowError("published route identity differs")
    if route.authorization_id != authorization.authorization_id:
        raise ProductionShadowError("published route authorization differs")
    if route.plan_sha256 != authorization.plan_sha256:
        raise ProductionShadowError("published route plan differs")
    if route.pointer_attempt_id != route.attempt_id:
        raise ProductionShadowError("published attempt pointer differs")
    if route.pointer_fence != route.fence:
        raise ProductionShadowError("published pointer fence is stale")
    if not route.sealed or not route.committed:
        raise ProductionShadowError("published route is not sealed and committed")
    config.validate_files_path(route.output_path)
    if route.publication_count != 1:
        raise ProductionShadowError("published route is duplicated")


@dataclass(frozen=True, slots=True)
class RouteTolerance:
    frame_count: int = 0
    timestamp_seconds: float = 0.0
    logical_total: float = 0.0
    numeric_fields: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if type(self.frame_count) is not int or self.frame_count < 0:
            raise ProductionShadowError("frame tolerance must be nonnegative")
        for label, value in {
            "timestamp_seconds": self.timestamp_seconds,
            "logical_total": self.logical_total,
            **dict(self.numeric_fields),
        }.items():
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) < 0
            ):
                raise ProductionShadowError(
                    f"numeric tolerance {label!r} must be finite and nonnegative"
                )
        object.__setattr__(
            self,
            "numeric_fields",
            MappingProxyType(dict(sorted(self.numeric_fields.items()))),
        )


@dataclass(frozen=True, slots=True)
class ShadowFinding:
    finding_id: str
    finding_type: str
    field: str
    legacy_value: object
    shadow_value: object
    severity: str = "CRITICAL"


@dataclass(frozen=True, slots=True)
class ShadowReconciliation:
    work_id: str
    plan_sha256: str
    findings: tuple[ShadowFinding, ...]

    @property
    def passed(self) -> bool:
        return not any(item.severity == "CRITICAL" for item in self.findings)

    def require_pass(self) -> None:
        if not self.passed:
            kinds = ",".join(item.finding_type for item in self.findings)
            raise ProductionShadowError(
                f"critical production-shadow reconciliation findings: {kinds}"
            )


def _finding(
    work_id: str,
    plan_sha256: str,
    finding_type: str,
    field: str,
    legacy: object,
    shadow: object,
) -> ShadowFinding:
    identity = {
        "work_id": work_id,
        "plan_sha256": plan_sha256,
        "finding_type": finding_type,
        "field": field,
    }
    return ShadowFinding(
        sha256_json(identity), finding_type, field, legacy, shadow
    )


def _append_mismatch(
    findings: list[ShadowFinding],
    *,
    work_id: str,
    plan_sha256: str,
    finding_type: str,
    field: str,
    legacy: object,
    shadow: object,
) -> None:
    if legacy != shadow:
        findings.append(
            _finding(
                work_id,
                plan_sha256,
                finding_type,
                field,
                legacy,
                shadow,
            )
        )


def _record_values(
    records: Sequence[Mapping[str, Any]],
) -> dict[str, object]:
    indexed: dict[str, object] = {}
    for index, record in enumerate(records):
        for key, value in sorted(record.items()):
            indexed[f"{index}.{key}"] = value
    return indexed


def _compare_records(
    legacy: CommittedRoute,
    shadow: CommittedRoute,
    tolerance: RouteTolerance,
) -> list[ShadowFinding]:
    left = _record_values(legacy.records)
    right = _record_values(shadow.records)
    findings: list[ShadowFinding] = []
    for field_name in sorted(set(left) | set(right)):
        first = left.get(field_name)
        second = right.get(field_name)
        allowed = tolerance.numeric_fields.get(field_name)
        numeric = (
            allowed is not None
            and not isinstance(first, bool)
            and not isinstance(second, bool)
            and isinstance(first, (int, float))
            and isinstance(second, (int, float))
        )
        matches = (
            abs(float(first) - float(second)) <= allowed
            if numeric
            else first == second
        )
        if not matches:
            findings.append(
                _finding(
                    legacy.work_id,
                    shadow.plan_sha256,
                    "OUTPUT_RECORD_MISMATCH",
                    field_name,
                    first,
                    second,
                )
            )
    return findings


def compare_and_reconcile(
    legacy: CommittedRoute,
    shadow: CommittedRoute,
    authorization: AuthorizationRow,
    *,
    tolerance: RouteTolerance = RouteTolerance(),
) -> ShadowReconciliation:
    """Compare exact routing integrity plus bounded output tolerances."""

    findings: list[ShadowFinding] = []
    context = {
        "work_id": legacy.work_id,
        "plan_sha256": authorization.plan_sha256,
    }
    checks = (
        ("WORK_ID_MISMATCH", "work_id", legacy.work_id, shadow.work_id),
        (
            "LOGICAL_IDENTITY_MISMATCH",
            "logical_identity_sha256",
            legacy.logical_identity_sha256,
            shadow.logical_identity_sha256,
        ),
        ("IDENTITY_MISMATCH", "identity", legacy.identity, shadow.identity),
        ("UNSEALED_OUTPUT", "sealed", True, shadow.sealed),
        ("UNCOMMITTED_VISIBILITY", "committed", True, shadow.committed),
        (
            "POINTER_MISMATCH",
            "pointer_attempt_id",
            shadow.attempt_id,
            shadow.pointer_attempt_id,
        ),
        ("STALE_FENCE", "pointer_fence", shadow.fence, shadow.pointer_fence),
        (
            "DUPLICATE_PUBLICATION",
            "publication_count",
            1,
            shadow.publication_count,
        ),
        (
            "AUTHORIZATION_LINK_MISMATCH",
            "authorization_id",
            authorization.authorization_id,
            shadow.authorization_id,
        ),
        (
            "AUDIT_PLAN_MISMATCH",
            "plan_sha256",
            authorization.plan_sha256,
            shadow.plan_sha256,
        ),
        (
            "PROVENANCE_MISMATCH",
            "runtime_provenance",
            legacy.provenance,
            shadow.provenance,
        ),
        (
            "OUTPUT_DIGEST_MISMATCH",
            "output_sha256",
            legacy.output_sha256,
            shadow.output_sha256,
        ),
    )
    for finding_type, field_name, first, second in checks:
        _append_mismatch(
            findings,
            finding_type=finding_type,
            field=field_name,
            legacy=first,
            shadow=second,
            **context,
        )
    target = classify_target(shadow.output_path)
    if target.target_class is not TargetClass.SHADOW_FILE:
        findings.append(
            _finding(
                legacy.work_id,
                authorization.plan_sha256,
                "OUTPUT_PATH_OUTSIDE_SHADOW",
                "output_path",
                PRODUCTION_SHADOW_FILES_ROOT,
                shadow.output_path,
            )
        )
    if abs(legacy.frame_count - shadow.frame_count) > tolerance.frame_count:
        findings.append(
            _finding(
                legacy.work_id,
                authorization.plan_sha256,
                "FRAME_COUNT_OUT_OF_TOLERANCE",
                "frame_count",
                legacy.frame_count,
                shadow.frame_count,
            )
        )
    total_delta = abs(legacy.logical_total - shadow.logical_total)
    if (
        total_delta > tolerance.logical_total
        and not math.isclose(total_delta, tolerance.logical_total)
    ):
        findings.append(
            _finding(
                legacy.work_id,
                authorization.plan_sha256,
                "LOGICAL_TOTAL_OUT_OF_TOLERANCE",
                "logical_total",
                legacy.logical_total,
                shadow.logical_total,
            )
        )
    timestamp_delta = abs((legacy.timestamp - shadow.timestamp).total_seconds())
    if timestamp_delta > tolerance.timestamp_seconds:
        findings.append(
            _finding(
                legacy.work_id,
                authorization.plan_sha256,
                "TIMESTAMP_OUT_OF_TOLERANCE",
                "timestamp",
                legacy.timestamp.isoformat(),
                shadow.timestamp.isoformat(),
            )
        )
    findings.extend(_compare_records(legacy, shadow, tolerance))
    return ShadowReconciliation(
        legacy.work_id,
        authorization.plan_sha256,
        tuple(findings),
    )


class MemoryAuthorizationStore:
    """Deterministic test/reference adapter with append-only semantics."""

    def __init__(self) -> None:
        self.allowlist: list[dict[str, Any]] = []
        self.audit: list[dict[str, Any]] = []

    def read_allowlist(self, work_id: str) -> Sequence[Mapping[str, Any]]:
        return [row for row in self.allowlist if row["work_id"] == work_id]

    def append_allowlist(self, row: Mapping[str, Any]) -> None:
        require_write_target(PRODUCTION_ALLOWLIST_TABLE, WriteOperation.APPEND)
        self.allowlist.append(_canonical_mapping(row, "allowlist row"))

    def read_audit(self, authorization_id: str) -> Sequence[Mapping[str, Any]]:
        return [
            row
            for row in self.audit
            if row["audit_id"] == authorization_id
        ]

    def append_audit(self, row: Mapping[str, Any]) -> None:
        require_write_target(PRODUCTION_AUDIT_TABLE, WriteOperation.APPEND)
        self.audit.append(_canonical_mapping(row, "audit row"))

    def append_authorization(
        self,
        allowlist: Mapping[str, Any],
        audit: Mapping[str, Any],
    ) -> None:
        if self.read_allowlist(str(allowlist["work_id"])) or self.read_audit(
            str(audit["audit_id"])
        ):
            raise ProductionShadowError("atomic authorization key conflict")
        allowlist_row = _canonical_mapping(allowlist, "allowlist row")
        audit_row = _canonical_mapping(audit, "audit row")
        self.allowlist.append(allowlist_row)
        self.audit.append(audit_row)


def reconcile_main(
    argv: Sequence[str] | None = None,
    *,
    config: FabricCandidateAConfig,
    store: Any | None = None,
) -> int:
    """Run fixed shadow control reconciliation through the proven SDK store.

    Comparison is performed by the controller's separate ``compare`` gate;
    this SJD validates the live shadow work/attempt/publication/pointer/fence
    state and persists Candidate A reconciliation findings.
    """

    import argparse
    from dataclasses import asdict, is_dataclass

    parser = argparse.ArgumentParser(prog="pc-production-shadow-reconcile-sjd")
    parser.parse_args(argv)
    config.require_mode("PRODUCTION_SHADOW")
    selected = store
    if selected is None:
        from people_counter.fabric_candidate_a_control import FabricControlStoreImpl

        try:
            from pyspark.sql import SparkSession
        except ImportError as error:
            raise RuntimeError("PySpark is required for shadow reconcile") from error
        spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        selected = FabricControlStoreImpl(spark, config=config)
    findings = tuple(selected.reconcile())
    payload = [
        asdict(item) if is_dataclass(item) else dict(item)
        for item in findings
    ]
    print(json.dumps(payload, allow_nan=False, default=str, sort_keys=True))
    critical = any(
        str(item.get("severity", "")).upper() in {"CRITICAL", "ERROR"}
        and item.get("resolved_at") is None
        for item in payload
    )
    return 1 if critical else 0
