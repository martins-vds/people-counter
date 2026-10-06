"""Offline safety contract for a production-routing shadow migration.

The module deliberately performs no Fabric, Spark, Delta, or OneLake calls.
It creates and validates immutable instructions that an external adapter may
apply only after all target, manifest, quiescence, and journal gates pass.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any

from people_counter.fabric_production_migration import (
    ApplyStatus as MigrationApplyStatus,
    MIGRATION_ID,
    MIGRATION_VERSION,
    MigrationBackend,
    journal_integrity_errors,
    verify_backend,
)


WORKSPACE_ID = "c31ee864-230d-4005-8fd5-7c7130ebf774"
LAKEHOUSE_ID = "883cff91-eaa8-40be-870f-6e9716303cb2"
ENVIRONMENT_ID = "3e580f48-9ff7-4bc6-af2e-a59158029ada"
SHADOW_TABLE_PREFIX = "pc_ca_prod_shadow_v1_"
SHADOW_FILES_ROOT = "Files/_shadow/people-counter/candidate-a/v1/"
JOURNAL_SCHEMA_VERSION = 1
PLAN_SCHEMA_VERSION = 1
QUIESCENCE_MAX_AGE = timedelta(minutes=5)
ZERO_SHA256 = "0" * 64

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_TABLE_SUFFIX = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_FILE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._=-]{0,127}$")
_SECTIONS = frozenset({"output", "provenance", "metrics"})
_JOURNAL_EVENTS = frozenset(
    {
        "PLANNED",
        "QUIESCENCE_VERIFIED",
        "APPLY_AUTHORIZED",
        "APPLIED",
        "RECONCILED",
        "ROLLED_BACK",
    }
)
_JOURNAL_TRANSITIONS = {
    "PLANNED": frozenset({"QUIESCENCE_VERIFIED", "APPLY_AUTHORIZED", "ROLLED_BACK"}),
    "QUIESCENCE_VERIFIED": frozenset({"APPLY_AUTHORIZED", "ROLLED_BACK"}),
    "APPLY_AUTHORIZED": frozenset({"APPLIED", "ROLLED_BACK"}),
    "APPLIED": frozenset({"RECONCILED", "ROLLED_BACK"}),
    "RECONCILED": frozenset({"RECONCILED", "ROLLED_BACK"}),
    "ROLLED_BACK": frozenset(),
}


class RoutingValidationError(ValueError):
    """A routing artifact does not satisfy the fail-closed contract."""


def canonical_bytes(value: object) -> bytes:
    """Return the sole JSON representation used by every contract digest."""

    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise RoutingValidationError("value is not canonical JSON") from error


def sha256_json(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _freeze_json(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _require_hash(value: str, name: str) -> str:
    if not isinstance(value, str) or _HEX64.fullmatch(value) is None:
        raise RoutingValidationError(
            f"{name} must be 64 lowercase hexadecimal characters"
        )
    return value


def _require_id(value: str, name: str) -> str:
    if not isinstance(value, str) or _SAFE_ID.fullmatch(value) is None:
        raise RoutingValidationError(f"{name} is not a safe explicit identifier")
    if value in {"*", "all", "ALL"}:
        raise RoutingValidationError(f"{name} cannot select all work")
    return value


def _require_uuid(value: str, name: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError) as error:
        raise RoutingValidationError(f"{name} must be a canonical UUID") from error
    if str(parsed) != value:
        raise RoutingValidationError(f"{name} must be a canonical lowercase UUID")
    return value


def _require_utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise RoutingValidationError(f"{name} must be timezone-aware")
    if value.utcoffset() != timedelta(0):
        raise RoutingValidationError(f"{name} must be expressed in UTC")
    return value


@dataclass(frozen=True)
class DeploymentGate:
    """Exact Fabric identities and schema generation approved for this rollout."""

    workspace_id: str = WORKSPACE_ID
    lakehouse_id: str = LAKEHOUSE_ID
    environment_id: str = ENVIRONMENT_ID
    migration_version: int = MIGRATION_VERSION

    def __post_init__(self) -> None:
        expected = {
            "workspace_id": WORKSPACE_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "environment_id": ENVIRONMENT_ID,
            "migration_version": MIGRATION_VERSION,
        }
        for name, required in expected.items():
            if getattr(self, name) != required:
                raise RoutingValidationError(
                    f"{name} must exactly equal the reviewed value {required!r}"
                )
        _require_uuid(self.workspace_id, "workspace_id")
        _require_uuid(self.lakehouse_id, "lakehouse_id")
        _require_uuid(self.environment_id, "environment_id")

    def as_dict(self) -> dict[str, str | int]:
        return {
            "workspace_id": self.workspace_id,
            "lakehouse_id": self.lakehouse_id,
            "environment_id": self.environment_id,
            "migration_version": self.migration_version,
        }


@dataclass(frozen=True)
class AllowlistRow:
    """One immutable, fully identified unit of work approved for shadowing."""

    work_id: str
    camera_sha256: str
    location_sha256: str
    model_sha256: str
    source_sha256: str
    config_sha256: str

    def __post_init__(self) -> None:
        _require_id(self.work_id, "work_id")
        for name in (
            "camera_sha256",
            "location_sha256",
            "model_sha256",
            "source_sha256",
            "config_sha256",
        ):
            _require_hash(getattr(self, name), name)

    def as_dict(self) -> dict[str, str]:
        return {
            "work_id": self.work_id,
            "camera_sha256": self.camera_sha256,
            "location_sha256": self.location_sha256,
            "model_sha256": self.model_sha256,
            "source_sha256": self.source_sha256,
            "config_sha256": self.config_sha256,
        }


@dataclass(frozen=True)
class WorkManifest:
    """An explicit manifest; wildcard, predicate, and claim-all modes do not exist."""

    manifest_id: str
    rows: tuple[AllowlistRow, ...]

    def __post_init__(self) -> None:
        _require_id(self.manifest_id, "manifest_id")
        object.__setattr__(self, "rows", tuple(self.rows))
        if not self.rows:
            raise RoutingValidationError("manifest must contain explicit work rows")
        if any(not isinstance(row, AllowlistRow) for row in self.rows):
            raise RoutingValidationError("manifest rows must be AllowlistRow values")
        work_ids = [row.work_id for row in self.rows]
        if len(set(work_ids)) != len(work_ids):
            raise RoutingValidationError("manifest work_id values must be unique")
        if work_ids != sorted(work_ids):
            raise RoutingValidationError("manifest rows must be sorted by work_id")

    @property
    def work_ids(self) -> tuple[str, ...]:
        return tuple(row.work_id for row in self.rows)

    def as_dict(self) -> dict[str, object]:
        return {
            "manifest_id": self.manifest_id,
            "rows": [row.as_dict() for row in self.rows],
        }

    @property
    def sha256(self) -> str:
        return sha256_json(self.as_dict())


@dataclass(frozen=True)
class ComparisonTolerance:
    """Absolute/relative tolerance for one flattened comparison field."""

    section: str
    field: str
    absolute: float = 0.0
    relative: float = 0.0

    def __post_init__(self) -> None:
        if self.section not in _SECTIONS:
            raise RoutingValidationError(
                f"tolerance section must be one of {sorted(_SECTIONS)!r}"
            )
        if not isinstance(self.field, str) or not self.field:
            raise RoutingValidationError("tolerance field must be nonempty")
        for name in ("absolute", "relative"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
            ):
                raise RoutingValidationError(
                    f"tolerance {name} must be finite and nonnegative"
                )

    def as_dict(self) -> dict[str, object]:
        return {
            "section": self.section,
            "field": self.field,
            "absolute": float(self.absolute),
            "relative": float(self.relative),
        }


@dataclass(frozen=True)
class ShadowPlan:
    """Canonical, hashable routing plan with no production publication action."""

    gate: DeploymentGate
    migration_id: str
    manifest: WorkManifest
    migration_plan_sha256: str
    tolerances: tuple[ComparisonTolerance, ...] = ()
    schema_version: int = PLAN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.gate, DeploymentGate):
            raise RoutingValidationError("plan gate must be DeploymentGate")
        if self.migration_id != MIGRATION_ID:
            raise RoutingValidationError(
                f"migration_id must be the reviewed value {MIGRATION_ID!r}"
            )
        if not isinstance(self.manifest, WorkManifest):
            raise RoutingValidationError("plan manifest must be WorkManifest")
        _require_hash(self.migration_plan_sha256, "migration_plan_sha256")
        object.__setattr__(self, "tolerances", tuple(self.tolerances))
        if self.schema_version != PLAN_SCHEMA_VERSION:
            raise RoutingValidationError("unsupported plan schema version")
        keys: set[tuple[str, str]] = set()
        ordered_keys: list[tuple[str, str]] = []
        for tolerance in self.tolerances:
            if not isinstance(tolerance, ComparisonTolerance):
                raise RoutingValidationError(
                    "plan tolerances must be ComparisonTolerance values"
                )
            key = (tolerance.section, tolerance.field)
            if key in keys:
                raise RoutingValidationError(f"duplicate tolerance for {key!r}")
            keys.add(key)
            ordered_keys.append(key)
        if ordered_keys != sorted(ordered_keys):
            raise RoutingValidationError(
                "plan tolerances must be sorted by section and field"
            )

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "gate": self.gate.as_dict(),
            "migration_id": self.migration_id,
            "migration_plan_sha256": self.migration_plan_sha256,
            "manifest": self.manifest.as_dict(),
            "manifest_sha256": self.manifest.sha256,
            "work_ids": list(self.manifest.work_ids),
            "claim_mode": "EXPLICIT_MANIFEST_ONLY",
            "shadow_table_prefix": SHADOW_TABLE_PREFIX,
            "shadow_files_root": SHADOW_FILES_ROOT,
            "production_publication": False,
            "production_view_update": False,
            "production_pointer_update": False,
            "semantic_refresh": False,
            "tolerances": [item.as_dict() for item in self.tolerances],
        }

    @property
    def sha256(self) -> str:
        return sha256_json(self.as_dict())

    @property
    def safety_token(self) -> str:
        return f"{MIGRATION_VERSION}:{self.migration_id}:{self.sha256}"

    def require_safety_token(self, supplied: str) -> None:
        if not isinstance(supplied, str) or not hmac.compare_digest(
            supplied, self.safety_token
        ):
            raise RoutingValidationError(
                "safety token does not match the migration ID and canonical plan hash"
            )


@dataclass(frozen=True)
class ExplicitClaimRequest:
    """The only legal claim surface for this rollout."""

    migration_id: str
    manifest_id: str
    manifest_sha256: str
    work_ids: tuple[str, ...]
    allowlist_rows: tuple[AllowlistRow, ...]

    def __post_init__(self) -> None:
        if self.migration_id != MIGRATION_ID:
            raise RoutingValidationError("claim migration_id is incompatible")
        _require_id(self.manifest_id, "manifest_id")
        _require_hash(self.manifest_sha256, "manifest_sha256")
        object.__setattr__(self, "work_ids", tuple(self.work_ids))
        object.__setattr__(self, "allowlist_rows", tuple(self.allowlist_rows))
        if not self.work_ids:
            raise RoutingValidationError("claim requires explicit work_id values")
        if len(set(self.work_ids)) != len(self.work_ids):
            raise RoutingValidationError("claim work_id values must be unique")
        for work_id in self.work_ids:
            _require_id(work_id, "work_id")
        if tuple(row.work_id for row in self.allowlist_rows) != self.work_ids:
            raise RoutingValidationError(
                "claim work_ids must exactly match its ordered allowlist rows"
            )


def claim_request(plan: ShadowPlan) -> ExplicitClaimRequest:
    return ExplicitClaimRequest(
        migration_id=plan.migration_id,
        manifest_id=plan.manifest.manifest_id,
        manifest_sha256=plan.manifest.sha256,
        work_ids=plan.manifest.work_ids,
        allowlist_rows=plan.manifest.rows,
    )


def validate_observed_allowlist(
    request: ExplicitClaimRequest,
    observed: Iterable[AllowlistRow],
) -> None:
    """Require apply-time source identities to equal the reviewed allowlist."""

    observed_rows = tuple(observed)
    if observed_rows != request.allowlist_rows:
        raise RoutingValidationError(
            "observed work identities differ from the explicit allowlist"
        )
    if sha256_json(
        {
            "manifest_id": request.manifest_id,
            "rows": [row.as_dict() for row in observed_rows],
        }
    ) != request.manifest_sha256:
        raise RoutingValidationError("observed allowlist manifest digest mismatch")


@dataclass(frozen=True)
class JournalEntry:
    """One append-only, hash-chained migration journal record."""

    sequence: int
    migration_id: str
    plan_sha256: str
    manifest_sha256: str
    event: str
    recorded_at: datetime
    previous_sha256: str
    details: Mapping[str, Any] = field(default_factory=dict)
    schema_version: int = JOURNAL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if type(self.sequence) is not int or self.sequence < 0:
            raise RoutingValidationError(
                "journal sequence must be a nonnegative integer"
            )
        if self.migration_id != MIGRATION_ID:
            raise RoutingValidationError("journal migration_id is incompatible")
        _require_hash(self.plan_sha256, "plan_sha256")
        _require_hash(self.manifest_sha256, "manifest_sha256")
        _require_hash(self.previous_sha256, "previous_sha256")
        _require_utc(self.recorded_at, "recorded_at")
        if self.event not in _JOURNAL_EVENTS:
            raise RoutingValidationError(f"unsupported journal event {self.event!r}")
        if self.schema_version != JOURNAL_SCHEMA_VERSION:
            raise RoutingValidationError("unsupported journal schema version")
        if not isinstance(self.details, Mapping):
            raise RoutingValidationError("journal details must be a mapping")
        canonical_bytes(_thaw_json(self.details))
        object.__setattr__(self, "details", _freeze_json(self.details))

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "sequence": self.sequence,
            "migration_id": self.migration_id,
            "plan_sha256": self.plan_sha256,
            "manifest_sha256": self.manifest_sha256,
            "event": self.event,
            "recorded_at": self.recorded_at.isoformat(),
            "previous_sha256": self.previous_sha256,
            "details": _thaw_json(self.details),
        }

    @property
    def sha256(self) -> str:
        return sha256_json(self.as_dict())

    @classmethod
    def append(
        cls,
        plan: ShadowPlan,
        journal: Sequence[JournalEntry],
        *,
        event: str,
        recorded_at: datetime,
        details: Mapping[str, Any] | None = None,
    ) -> JournalEntry:
        validated = validate_journal(plan, journal)
        if not validated and event != "PLANNED":
            raise RoutingValidationError("journal must begin with PLANNED")
        if validated and event not in _JOURNAL_TRANSITIONS[validated[-1].event]:
            raise RoutingValidationError(
                f"incompatible journal transition "
                f"{validated[-1].event} -> {event}"
            )
        previous = validated[-1].sha256 if validated else ZERO_SHA256
        return cls(
            sequence=len(validated),
            migration_id=plan.migration_id,
            plan_sha256=plan.sha256,
            manifest_sha256=plan.manifest.sha256,
            event=event,
            recorded_at=recorded_at,
            previous_sha256=previous,
            details={} if details is None else details,
        )


def validate_journal(
    plan: ShadowPlan, journal: Sequence[JournalEntry]
) -> tuple[JournalEntry, ...]:
    """Validate schema compatibility, identity binding, ordering, and hash chain."""

    entries = tuple(journal)
    previous = ZERO_SHA256
    previous_time: datetime | None = None
    previous_event: str | None = None
    for expected_sequence, entry in enumerate(entries):
        if not isinstance(entry, JournalEntry):
            raise RoutingValidationError("journal contains a non-JournalEntry value")
        if entry.sequence != expected_sequence:
            raise RoutingValidationError("journal sequence is not contiguous")
        if (
            entry.migration_id != plan.migration_id
            or entry.plan_sha256 != plan.sha256
            or entry.manifest_sha256 != plan.manifest.sha256
        ):
            raise RoutingValidationError("journal is incompatible with this plan")
        if entry.previous_sha256 != previous:
            raise RoutingValidationError("journal hash chain is invalid")
        if previous_time is not None and entry.recorded_at < previous_time:
            raise RoutingValidationError("journal timestamps are not monotonic")
        if previous_event is None:
            if entry.event != "PLANNED":
                raise RoutingValidationError("journal must begin with PLANNED")
        elif previous_event == "ROLLED_BACK":
            raise RoutingValidationError("journal contains an event after rollback")
        elif entry.event not in _JOURNAL_TRANSITIONS[previous_event]:
            raise RoutingValidationError(
                f"incompatible journal transition {previous_event} -> {entry.event}"
            )
        previous = entry.sha256
        previous_time = entry.recorded_at
        previous_event = entry.event
    return entries


@dataclass(frozen=True)
class QuiescenceProof:
    """Point-in-time proof that no legacy or shadow writer can race apply."""

    migration_id: str
    plan_sha256: str
    journal_head_sha256: str
    observed_at: datetime
    enabled_writer_ids: tuple[str, ...] = ()
    active_work_ids: tuple[str, ...] = ()
    unreceipted_event_count: int = 0
    control_writer_owner_id: str | None = None
    live_writer_session_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.migration_id != MIGRATION_ID:
            raise RoutingValidationError(
                "quiescence migration_id is incompatible"
            )
        _require_hash(self.plan_sha256, "plan_sha256")
        _require_hash(self.journal_head_sha256, "journal_head_sha256")
        _require_utc(self.observed_at, "observed_at")
        object.__setattr__(self, "enabled_writer_ids", tuple(self.enabled_writer_ids))
        object.__setattr__(self, "active_work_ids", tuple(self.active_work_ids))
        object.__setattr__(
            self, "live_writer_session_ids", tuple(self.live_writer_session_ids)
        )
        if (
            type(self.unreceipted_event_count) is not int
            or self.unreceipted_event_count < 0
        ):
            raise RoutingValidationError(
                "unreceipted_event_count must be a nonnegative integer"
            )


def validate_quiescence(
    plan: ShadowPlan,
    proof: QuiescenceProof,
    journal: Sequence[JournalEntry],
    *,
    applied_at: datetime,
) -> None:
    """Validate quiescence at apply time, including journal-head freshness."""

    entries = validate_journal(plan, journal)
    now = _require_utc(applied_at, "applied_at")
    expected_head = entries[-1].sha256 if entries else ZERO_SHA256
    if proof.migration_id != plan.migration_id or proof.plan_sha256 != plan.sha256:
        raise RoutingValidationError("quiescence proof is bound to another plan")
    if proof.journal_head_sha256 != expected_head:
        raise RoutingValidationError("quiescence proof journal head is stale")
    age = now - proof.observed_at
    if age < timedelta(0) or age > QUIESCENCE_MAX_AGE:
        raise RoutingValidationError("quiescence proof is not fresh at apply time")
    blockers = {
        "enabled_writer_ids": proof.enabled_writer_ids,
        "active_work_ids": proof.active_work_ids,
        "unreceipted_event_count": proof.unreceipted_event_count,
        "control_writer_owner_id": proof.control_writer_owner_id,
        "live_writer_session_ids": proof.live_writer_session_ids,
    }
    if any(
        (
            proof.enabled_writer_ids,
            proof.active_work_ids,
            proof.unreceipted_event_count != 0,
            proof.control_writer_owner_id is not None,
            proof.live_writer_session_ids,
        )
    ):
        raise RoutingValidationError(
            f"apply-time quiescence proof has blockers: {blockers!r}"
        )


@dataclass(frozen=True)
class ApplyAuthorization:
    """Validated, explicit instructions safe for an external apply adapter."""

    migration_id: str
    plan_sha256: str
    claim: ExplicitClaimRequest
    table_prefix: str
    files_root: str
    journal_entry: JournalEntry
    semantic_refresh: bool = False

    def __post_init__(self) -> None:
        if self.table_prefix != SHADOW_TABLE_PREFIX:
            raise RoutingValidationError("authorization table prefix is not shadow-only")
        if self.files_root != SHADOW_FILES_ROOT:
            raise RoutingValidationError("authorization files root is not shadow-only")
        if self.semantic_refresh is not False:
            raise RoutingValidationError("semantic refresh is forbidden")


def authorize_apply(
    plan: ShadowPlan,
    *,
    work_ids: Sequence[str],
    observed_allowlist: Iterable[AllowlistRow],
    safety_token: str,
    proof: QuiescenceProof,
    journal: Sequence[JournalEntry],
    migration_backend: MigrationBackend,
    applied_at: datetime,
) -> ApplyAuthorization:
    """Fail closed unless all apply-time gates agree on the same canonical plan."""

    selected = tuple(work_ids)
    if len(selected) != 1:
        raise RoutingValidationError(
            "production routing requires EXACTLY ONE explicit --work-id value"
        )
    for work_id in selected:
        _require_id(work_id, "work_id")
    if len(set(selected)) != len(selected):
        raise RoutingValidationError("--work-id values must be unique")
    if selected != plan.manifest.work_ids:
        raise RoutingValidationError(
            "explicit --work-id values must exactly match the reviewed manifest"
        )
    claim = claim_request(plan)
    validate_observed_allowlist(claim, observed_allowlist)
    plan.require_safety_token(safety_token)
    migration_verification = verify_backend(migration_backend)
    if not migration_verification.valid:
        raise RoutingValidationError(
            "apply requires a verified successful additive migration"
        )
    migration_entries = migration_backend.journal_entries()
    migration_errors = journal_integrity_errors(migration_entries)
    if migration_errors:
        raise RoutingValidationError(
            "additive migration journal is invalid: "
            + "; ".join(migration_errors)
        )
    migration_head = migration_entries[-1]
    if migration_head.status not in {
        MigrationApplyStatus.APPLIED,
        MigrationApplyStatus.NOOP,
    }:
        raise RoutingValidationError(
            "additive migration journal is not successful"
        )
    if migration_head.plan_sha256 != plan.migration_plan_sha256:
        raise RoutingValidationError(
            "migration journal plan hash differs from the reviewed routing plan"
        )
    entries = validate_journal(plan, journal)
    if not entries:
        raise RoutingValidationError("apply requires a compatible PLANNED journal")
    if any(entry.event in {"APPLY_AUTHORIZED", "APPLIED"} for entry in entries):
        raise RoutingValidationError("this plan has already been authorized or applied")
    if entries[-1].event not in {"PLANNED", "QUIESCENCE_VERIFIED"}:
        raise RoutingValidationError(
            "this plan is not in an apply-authorizable journal state"
        )
    validate_quiescence(plan, proof, entries, applied_at=applied_at)
    entry = JournalEntry.append(
        plan,
        entries,
        event="APPLY_AUTHORIZED",
        recorded_at=applied_at,
        details={
            "manifest_id": plan.manifest.manifest_id,
            "work_ids": list(plan.manifest.work_ids),
            "quiescence_observed_at": proof.observed_at.isoformat(),
            "semantic_refresh": False,
        },
    )
    return ApplyAuthorization(
        migration_id=plan.migration_id,
        plan_sha256=plan.sha256,
        claim=claim,
        table_prefix=SHADOW_TABLE_PREFIX,
        files_root=SHADOW_FILES_ROOT,
        journal_entry=entry,
    )


def shadow_table(suffix: str) -> str:
    """Return a table name inside the sole approved shadow namespace."""

    if not isinstance(suffix, str) or _TABLE_SUFFIX.fullmatch(suffix) is None:
        raise RoutingValidationError("unsafe shadow table suffix")
    return f"{SHADOW_TABLE_PREFIX}{suffix}"


def shadow_file(relative: str) -> str:
    """Return a file path inside the exact shadow root."""

    if not isinstance(relative, str):
        raise RoutingValidationError("shadow file path must be a string")
    path = PurePosixPath(relative)
    if (
        not relative
        or path.is_absolute()
        or "\\" in relative
        or "://" in relative
        or path.as_posix() != relative
        or any(
            part in {"", ".", ".."} or _FILE_SEGMENT.fullmatch(part) is None
            for part in path.parts
        )
    ):
        raise RoutingValidationError("unsafe shadow file path")
    target = f"{SHADOW_FILES_ROOT}{path.as_posix()}"
    if not target.startswith(SHADOW_FILES_ROOT):
        raise RoutingValidationError("shadow file escaped its exact root")
    return target


@dataclass(frozen=True)
class ShadowWrite:
    """One planned write, always tied to an allowlisted work ID."""

    kind: str
    target: str
    work_id: str

    def __post_init__(self) -> None:
        if self.kind not in {"TABLE", "FILE"}:
            raise RoutingValidationError("shadow write kind must be TABLE or FILE")
        _require_id(self.work_id, "work_id")
        if self.kind == "TABLE":
            if (
                not self.target.startswith(SHADOW_TABLE_PREFIX)
                or self.target != shadow_table(self.target[len(SHADOW_TABLE_PREFIX) :])
            ):
                raise RoutingValidationError("table write is outside the shadow prefix")
        elif not self.target.startswith(SHADOW_FILES_ROOT):
            raise RoutingValidationError("file write is outside the shadow root")
        else:
            relative = self.target[len(SHADOW_FILES_ROOT) :]
            if self.target != shadow_file(relative):
                raise RoutingValidationError("file write is not canonical")


def validate_shadow_writes(
    plan: ShadowPlan, writes: Iterable[ShadowWrite]
) -> tuple[ShadowWrite, ...]:
    validated = tuple(writes)
    if not validated:
        raise RoutingValidationError("apply requires at least one shadow write")
    allowed = set(plan.manifest.work_ids)
    if any(write.work_id not in allowed for write in validated):
        raise RoutingValidationError("shadow write references non-allowlisted work")
    return validated


@dataclass(frozen=True)
class CommittedSnapshot:
    """Committed output, provenance, and metrics for one route."""

    work_id: str
    attempt_id: str
    output: Mapping[str, Any]
    provenance: Mapping[str, Any]
    metrics: Mapping[str, Any]
    committed: bool = True

    def __post_init__(self) -> None:
        _require_id(self.work_id, "work_id")
        _require_id(self.attempt_id, "attempt_id")
        for name in ("output", "provenance", "metrics"):
            value = getattr(self, name)
            if not isinstance(value, Mapping):
                raise RoutingValidationError(f"{name} must be a mapping")
            canonical_bytes(dict(value))
            object.__setattr__(self, name, _freeze_json(value))
        if self.committed is not True:
            raise RoutingValidationError("only committed snapshots may be compared")


@dataclass(frozen=True)
class ReconciliationFinding:
    finding_id: str
    work_id: str
    section: str
    field: str
    finding_type: str
    legacy_value: object
    shadow_value: object
    absolute_delta: float | None
    allowed_delta: float | None
    severity: str = "ERROR"


@dataclass(frozen=True)
class ComparisonReport:
    migration_id: str
    plan_sha256: str
    compared_work_ids: tuple[str, ...]
    findings: tuple[ReconciliationFinding, ...]
    semantic_refresh: bool = False

    @property
    def matched(self) -> bool:
        return not self.findings


def _flatten(value: object, prefix: str = "") -> dict[str, object]:
    if isinstance(value, Mapping):
        if not value:
            return {prefix: {}}
        if any(not isinstance(key, str) or not key for key in value):
            raise RoutingValidationError("comparison mapping keys must be strings")
        flattened: dict[str, object] = {}
        for key in sorted(value):
            child = f"{prefix}.{key}" if prefix else key
            flattened.update(_flatten(value[key], child))
        return flattened
    if isinstance(value, (list, tuple)):
        if not value:
            return {prefix: []}
        flattened = {}
        for index, item in enumerate(value):
            child = f"{prefix}[{index}]"
            flattened.update(_flatten(item, child))
        return flattened
    return {prefix: value}


def _snapshot_index(
    snapshots: Iterable[CommittedSnapshot],
    allowed_work_ids: frozenset[str],
    route: str,
) -> dict[str, CommittedSnapshot]:
    indexed: dict[str, CommittedSnapshot] = {}
    for snapshot in snapshots:
        if not isinstance(snapshot, CommittedSnapshot):
            raise RoutingValidationError(f"{route} contains an invalid snapshot")
        if snapshot.work_id not in allowed_work_ids:
            raise RoutingValidationError(
                f"{route} contains non-manifest work_id {snapshot.work_id!r}"
            )
        if snapshot.work_id in indexed:
            raise RoutingValidationError(
                f"{route} has multiple committed snapshots for {snapshot.work_id!r}"
            )
        indexed[snapshot.work_id] = snapshot
    return indexed


def _finding(
    plan: ShadowPlan,
    *,
    work_id: str,
    section: str,
    field: str,
    finding_type: str,
    legacy: object,
    shadow: object,
    absolute_delta: float | None = None,
    allowed_delta: float | None = None,
) -> ReconciliationFinding:
    identity = {
        "migration_id": plan.migration_id,
        "plan_sha256": plan.sha256,
        "work_id": work_id,
        "section": section,
        "field": field,
        "finding_type": finding_type,
    }
    return ReconciliationFinding(
        finding_id=sha256_json(identity),
        work_id=work_id,
        section=section,
        field=field,
        finding_type=finding_type,
        legacy_value=legacy,
        shadow_value=shadow,
        absolute_delta=absolute_delta,
        allowed_delta=allowed_delta,
    )


def _compare_section(
    plan: ShadowPlan,
    work_id: str,
    section: str,
    legacy: Mapping[str, Any],
    shadow: Mapping[str, Any],
    tolerances: Mapping[tuple[str, str], ComparisonTolerance],
) -> list[ReconciliationFinding]:
    expected = _flatten(legacy)
    actual = _flatten(shadow)
    findings: list[ReconciliationFinding] = []
    for field_name in sorted(set(expected) | set(actual)):
        if field_name not in expected or field_name not in actual:
            findings.append(
                _finding(
                    plan,
                    work_id=work_id,
                    section=section,
                    field=field_name,
                    finding_type="MISSING_FIELD",
                    legacy=expected.get(field_name),
                    shadow=actual.get(field_name),
                )
            )
            continue
        left = expected[field_name]
        right = actual[field_name]
        tolerance = tolerances.get(
            (section, field_name),
            ComparisonTolerance(section, field_name),
        )
        numeric = (
            not isinstance(left, bool)
            and not isinstance(right, bool)
            and isinstance(left, (int, float))
            and isinstance(right, (int, float))
        )
        if numeric:
            left_number = float(left)
            right_number = float(right)
            if not math.isfinite(left_number) or not math.isfinite(right_number):
                raise RoutingValidationError("comparison values must be finite")
            delta = abs(left_number - right_number)
            allowed = float(tolerance.absolute) + float(tolerance.relative) * abs(
                left_number
            )
            if delta > allowed:
                findings.append(
                    _finding(
                        plan,
                        work_id=work_id,
                        section=section,
                        field=field_name,
                        finding_type="OUT_OF_TOLERANCE",
                        legacy=left,
                        shadow=right,
                        absolute_delta=delta,
                        allowed_delta=allowed,
                    )
                )
        elif left != right:
            findings.append(
                _finding(
                    plan,
                    work_id=work_id,
                    section=section,
                    field=field_name,
                    finding_type="VALUE_MISMATCH",
                    legacy=left,
                    shadow=right,
                )
            )
    return findings


def _allowlist_provenance_findings(
    plan: ShadowPlan,
    row: AllowlistRow,
    route: str,
    snapshot: CommittedSnapshot,
) -> list[ReconciliationFinding]:
    expected = {
        name: getattr(row, name)
        for name in (
            "camera_sha256",
            "location_sha256",
            "model_sha256",
            "source_sha256",
            "config_sha256",
        )
    }
    findings: list[ReconciliationFinding] = []
    for name, expected_value in expected.items():
        observed = snapshot.provenance.get(name)
        if observed != expected_value:
            findings.append(
                _finding(
                    plan,
                    work_id=row.work_id,
                    section="provenance",
                    field=f"{route}.{name}",
                    finding_type=(
                        "MISSING_ALLOWLIST_PROVENANCE"
                        if observed is None
                        else "ALLOWLIST_PROVENANCE_MISMATCH"
                    ),
                    legacy=expected_value,
                    shadow=observed,
                )
            )
    return findings


def compare_committed_routes(
    plan: ShadowPlan,
    *,
    legacy: Iterable[CommittedSnapshot],
    shadow: Iterable[CommittedSnapshot],
) -> ComparisonReport:
    """Compare only committed legacy/shadow results without refreshing semantics."""

    allowed = frozenset(plan.manifest.work_ids)
    legacy_by_id = _snapshot_index(legacy, allowed, "legacy")
    shadow_by_id = _snapshot_index(shadow, allowed, "shadow")
    tolerances = {
        (item.section, item.field): item for item in plan.tolerances
    }
    findings: list[ReconciliationFinding] = []
    compared: list[str] = []
    rows_by_id = {row.work_id: row for row in plan.manifest.rows}
    for work_id in plan.manifest.work_ids:
        legacy_snapshot = legacy_by_id.get(work_id)
        shadow_snapshot = shadow_by_id.get(work_id)
        if legacy_snapshot is None or shadow_snapshot is None:
            findings.append(
                _finding(
                    plan,
                    work_id=work_id,
                    section="snapshot",
                    field="committed",
                    finding_type="MISSING_COMMITTED_SNAPSHOT",
                    legacy=legacy_snapshot is not None,
                    shadow=shadow_snapshot is not None,
                )
            )
            continue
        compared.append(work_id)
        findings.extend(
            _allowlist_provenance_findings(
                plan, rows_by_id[work_id], "legacy", legacy_snapshot
            )
        )
        findings.extend(
            _allowlist_provenance_findings(
                plan, rows_by_id[work_id], "shadow", shadow_snapshot
            )
        )
        for section in ("output", "provenance", "metrics"):
            findings.extend(
                _compare_section(
                    plan,
                    work_id,
                    section,
                    getattr(legacy_snapshot, section),
                    getattr(shadow_snapshot, section),
                    tolerances,
                )
            )
    return ComparisonReport(
        migration_id=plan.migration_id,
        plan_sha256=plan.sha256,
        compared_work_ids=tuple(compared),
        findings=tuple(findings),
    )
