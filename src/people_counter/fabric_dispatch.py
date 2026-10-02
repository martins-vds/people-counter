"""Shared processing-engine routing rules for Fabric queue workflows."""

from __future__ import annotations

from typing import Final, Literal


ProcessingEngine = Literal["NOTEBOOK_04", "EXECUTOR_PARTITION"]

NOTEBOOK_04: Final[ProcessingEngine] = "NOTEBOOK_04"
EXECUTOR_PARTITION: Final[ProcessingEngine] = "EXECUTOR_PARTITION"
PROCESSING_ENGINES: Final[frozenset[str]] = frozenset(
    {NOTEBOOK_04, EXECUTOR_PARTITION}
)


def normalize_processing_engine(
    value: object,
    *,
    default: ProcessingEngine | None = NOTEBOOK_04,
) -> ProcessingEngine:
    """Normalize a configured engine while rejecting blank and unknown values."""
    if value is None:
        if default is None:
            raise ValueError("processing_engine is required")
        return default
    if not isinstance(value, str):
        raise ValueError("processing_engine must be a string")
    normalized = value.strip().upper()
    if normalized not in PROCESSING_ENGINES:
        allowed = ", ".join(sorted(PROCESSING_ENGINES))
        raise ValueError(f"processing_engine must be one of: {allowed}")
    return normalized  # type: ignore[return-value]


def compatible_queue_engine(value: object) -> ProcessingEngine:
    """Interpret legacy null queue values as notebook 04 during migration."""
    return normalize_processing_engine(value)


def validate_immutable_processing_engine(
    existing: object,
    requested: object,
) -> ProcessingEngine:
    """Return the requested engine when it does not reroute existing work."""
    existing_engine = compatible_queue_engine(existing)
    requested_engine = normalize_processing_engine(requested)
    if existing_engine != requested_engine:
        raise ValueError(
            "Existing work_id has conflicting processing_engine: "
            f"{existing_engine} != {requested_engine}"
        )
    return requested_engine
