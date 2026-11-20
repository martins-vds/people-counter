"""Small, fail-closed retention policy for Fabric SJD history."""

from __future__ import annotations

from dataclasses import dataclass
import math
from collections.abc import Collection, Mapping, Sequence
from typing import Any


DEFAULT_RETENTION_DAYS = 30
MIN_RETENTION_DAYS = 7
MAX_COMPACTION_BATCHES = 100
MAX_COMPACTION_FINDINGS = 1_000
SECONDS_PER_DAY = 24 * 60 * 60


@dataclass(frozen=True)
class FabricRetentionPolicy:
    retention_days: float = DEFAULT_RETENTION_DAYS
    max_batches: int = MAX_COMPACTION_BATCHES
    max_findings: int = MAX_COMPACTION_FINDINGS

    def __post_init__(self) -> None:
        if (
            type(self.retention_days) not in {int, float}
            or not math.isfinite(self.retention_days)
            or self.retention_days < MIN_RETENTION_DAYS
        ):
            raise ValueError(
                f"retention_days must be finite and at least {MIN_RETENTION_DAYS}"
            )
        if type(self.max_batches) is not int or self.max_batches <= 0:
            raise ValueError("max_batches must be a positive integer")
        if self.max_batches > MAX_COMPACTION_BATCHES:
            raise ValueError(
                f"max_batches must not exceed {MAX_COMPACTION_BATCHES}"
            )
        if type(self.max_findings) is not int or self.max_findings <= 0:
            raise ValueError("max_findings must be a positive integer")
        if self.max_findings > MAX_COMPACTION_FINDINGS:
            raise ValueError(
                f"max_findings must not exceed {MAX_COMPACTION_FINDINGS}"
            )

    def cutoff(self, now: float) -> float:
        if type(now) not in {int, float} or not math.isfinite(now):
            raise ValueError("now must be finite")
        return float(now) - float(self.retention_days) * SECONDS_PER_DAY


@dataclass(frozen=True)
class FabricRetentionPlan:
    control_batches: tuple[Mapping[str, Any], ...]
    consumer_batch_ids: tuple[str, ...]
    finding_ids: tuple[str, ...]


def plan_fabric_retention(
    *,
    policy: FabricRetentionPolicy,
    cutoff: float,
    work: Sequence[Mapping[str, Any]],
    batches: Sequence[Mapping[str, Any]],
    attempts: Sequence[Mapping[str, Any]],
    publications: Sequence[Mapping[str, Any]],
    findings: Sequence[Mapping[str, Any]],
    existing_consumer_batch_ids: Collection[str],
) -> FabricRetentionPlan:
    """Select bounded terminal history while preserving every committed pointer."""
    protected_attempts = {
        str(row[field])
        for row in work
        for field in ("lease_attempt_id", "committed_attempt_id")
        if row.get(field) is not None
    }
    published_attempts = {str(row["attempt_id"]) for row in publications}
    protected_attempts.update(published_attempts)
    published_batches = {str(row["batch_id"]) for row in publications}
    attempts_by_batch: dict[str, list[Mapping[str, Any]]] = {}
    for row in attempts:
        attempts_by_batch.setdefault(str(row["batch_id"]), []).append(row)

    aged = []
    for batch in batches:
        terminal_at = _terminal_at(batch)
        if terminal_at is not None and terminal_at <= cutoff:
            aged.append((terminal_at, batch))
    terminal = sorted(
        aged,
        key=lambda item: (item[0], str(item[1]["batch_id"])),
    )
    active_batches = {
        batch_id
        for batch_id, rows in attempts_by_batch.items()
        if any(
            str(row["attempt_id"]) in protected_attempts
            and str(row["attempt_id"]) not in published_attempts
            for row in rows
        )
    }
    control = tuple(
        batch
        for _, batch in terminal
        if str(batch["batch_id"]) not in published_batches
        and not any(
            str(row["attempt_id"]) in protected_attempts
            for row in attempts_by_batch.get(str(batch["batch_id"]), ())
        )
    )[: policy.max_batches]
    existing_consumers = set(existing_consumer_batch_ids)
    consumers = tuple(
        str(batch["batch_id"])
        for _, batch in terminal
        if str(batch["batch_id"]) in existing_consumers
        and str(batch["batch_id"]) not in active_batches
    )[: policy.max_batches]
    resolved = tuple(
        str(row["finding_id"])
        for row in sorted(
            (
                row
                for row in findings
                if row.get("resolved_at") is not None
                and _finite(row["resolved_at"], "resolved_at") <= cutoff
            ),
            key=lambda row: (
                _finite(row["resolved_at"], "resolved_at"),
                str(row["finding_id"]),
            ),
        )
    )[: policy.max_findings]
    return FabricRetentionPlan(control, consumers, resolved)


def _terminal_at(batch: Mapping[str, Any]) -> float | None:
    status = batch.get("status")
    field = "committed_at" if status == "COMMITTED" else "lease_expires_at"
    if status not in {"COMMITTED", "EXPIRED"}:
        return None
    value = batch.get(field)
    return None if value is None else _finite(value, field)


def _finite(value: object, name: str) -> float:
    if (
        type(value) not in {int, float}
        or not math.isfinite(value)
    ):
        raise ValueError(f"{name} must be finite")
    return float(value)
