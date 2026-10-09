"""Machine-readable Candidate A Fabric-only optimization manifest.

This module loads and validates the executable optimization manifest
derived from the read-only attachment ``spark-candidate-a-optimization.md``.
The manifest itself (``people_counter/data/candidate_a_optimization_manifest.json``)
enumerates every concrete, numbered recommendation/gate in the attachment
with a stable ID, applicability, implementation symbol, test symbol, live
proof evidence, correctness guardrail, and final status -- so the claim
"every still-applicable item was implemented" is independently, mechanically
checkable rather than resting on prose alone.

Nothing in this module (or the JSON it loads) modifies the attachment, which
remains read-only and untouched. Nothing here touches production
pipelines/notebooks, legacy gold/views/pointers, Reflex, semantic
models/reports, or production authorization -- items that would require
touching those surfaces are recorded with
``applicability=PROTECTED_SURFACE_OUT_OF_SCOPE`` and
``status=NOT_APPLICABLE``, never silently relabeled as something else.
"""

from __future__ import annotations

import importlib
import json
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

_DEFAULT_MANIFEST_PATH = (
    Path(__file__).resolve().parent / "data" / "candidate_a_optimization_manifest.json"
)


class ManifestError(ValueError):
    """The manifest file is malformed or internally inconsistent."""


class Applicability(str, Enum):
    """Whether a recommendation applies within this engagement's hard constraints."""

    APPLICABLE = "applicable"
    LEGACY_NOT_APPLICABLE = "legacy_not_applicable"
    PROTECTED_SURFACE_OUT_OF_SCOPE = "protected_surface_out_of_scope"


class ImplementationStatus(str, Enum):
    """Final disposition of one manifest item."""

    LIVE_PROVEN = "live_proven"
    IMPLEMENTED = "implemented"
    PARTIAL = "partial"
    NOT_IMPLEMENTED = "not_implemented"
    FABRIC_PLATFORM_BLOCKED = "fabric_platform_blocked"
    NOT_APPLICABLE = "not_applicable"
    DEFERRED_PENDING_TELEMETRY = "deferred_pending_telemetry"


@dataclass(frozen=True)
class OptimizationItem:
    """One concrete recommendation/gate extracted from the attachment."""

    id: str
    section: str
    title: str
    recommendation: str
    applicability: Applicability
    status: ImplementationStatus
    correctness_guardrail: str
    evidence: str
    footnote_refs: tuple[str, ...] = field(default_factory=tuple)
    implementation_symbol: str | None = None
    test_symbol: str | None = None

    def __post_init__(self) -> None:
        for required_name in ("id", "section", "title", "recommendation", "correctness_guardrail", "evidence"):
            if not getattr(self, required_name):
                raise ManifestError(f"item is missing required field {required_name!r}")
        if self.status in (ImplementationStatus.IMPLEMENTED, ImplementationStatus.LIVE_PROVEN):
            if not self.implementation_symbol:
                raise ManifestError(
                    f"item {self.id!r} has status {self.status.value!r} but no "
                    "implementation_symbol; a claimed implementation must cite the "
                    "symbol that implements it"
                )
        if self.applicability is not Applicability.APPLICABLE:
            if self.status is not ImplementationStatus.NOT_APPLICABLE:
                raise ManifestError(
                    f"item {self.id!r} has applicability {self.applicability.value!r} "
                    f"but status {self.status.value!r}; any non-applicable item must "
                    "carry status=not_applicable so it cannot be silently mislabeled "
                    "as implemented work"
                )


@dataclass(frozen=True)
class ManifestBaseline:
    """The verified baseline recorded alongside the manifest for traceability."""

    release_version: str
    wheel_sha256: str
    environment_id: str
    cpu_lock_sha256: str
    cpu_bundle_sha256: str
    best_measured_aggregate_cpu_throughput_factor: float
    scaling_efficiency_pct: float
    target_throughput_factor: float
    workspace_id: str
    lakehouse_id: str
    benchmark_environment_id: str
    capacity_id: str
    capacity_sku: str


@dataclass(frozen=True)
class OptimizationManifest:
    """The full parsed, validated optimization manifest."""

    schema_version: int
    source_document: str
    baseline: ManifestBaseline
    constraints: tuple[str, ...]
    items: tuple[OptimizationItem, ...]

    def by_id(self, item_id: str) -> OptimizationItem:
        for item in self.items:
            if item.id == item_id:
                return item
        raise KeyError(item_id)


def _parse_item(raw: dict[str, Any]) -> OptimizationItem:
    try:
        return OptimizationItem(
            id=raw["id"],
            section=raw["section"],
            title=raw["title"],
            recommendation=raw["recommendation"],
            applicability=Applicability(raw["applicability"]),
            status=ImplementationStatus(raw["status"]),
            correctness_guardrail=raw["correctness_guardrail"],
            evidence=raw["evidence"],
            footnote_refs=tuple(raw.get("footnote_refs") or ()),
            implementation_symbol=raw.get("implementation_symbol"),
            test_symbol=raw.get("test_symbol"),
        )
    except KeyError as error:
        raise ManifestError(f"manifest item missing required key {error}") from error
    except ValueError as error:
        raise ManifestError(f"manifest item {raw.get('id')!r} invalid: {error}") from error


def load_manifest(path: Path | None = None) -> OptimizationManifest:
    """Load and validate the optimization manifest from disk.

    Raises ``ManifestError`` on any structural problem: duplicate IDs,
    missing required fields, invalid enum values, or an
    applicability/status inconsistency. Never silently drops or repairs a
    malformed item.
    """
    manifest_path = path or _DEFAULT_MANIFEST_PATH
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ManifestError(f"manifest file not found: {manifest_path}") from error
    except json.JSONDecodeError as error:
        raise ManifestError(f"manifest file is not valid JSON: {error}") from error

    try:
        raw_items = raw["items"]
        raw_baseline = raw["baseline"]
        schema_version = raw["schema_version"]
        source_document = raw["source_document"]
        constraints = tuple(raw.get("constraints") or ())
    except KeyError as error:
        raise ManifestError(f"manifest envelope missing required key {error}") from error

    items = tuple(_parse_item(entry) for entry in raw_items)

    ids = [item.id for item in items]
    duplicates = {item_id for item_id in ids if ids.count(item_id) > 1}
    if duplicates:
        raise ManifestError(f"manifest has duplicate item ids: {sorted(duplicates)}")

    try:
        baseline = ManifestBaseline(**raw_baseline)
    except TypeError as error:
        raise ManifestError(f"manifest baseline is malformed: {error}") from error

    return OptimizationManifest(
        schema_version=schema_version,
        source_document=source_document,
        baseline=baseline,
        constraints=constraints,
        items=items,
    )


def verify_implementation_symbols_are_importable(
    manifest: OptimizationManifest,
) -> dict[str, str]:
    """Machine-verify every cited ``implementation_symbol`` actually exists.

    Returns a dict mapping item id -> error message for every item whose
    ``implementation_symbol`` fails to resolve via real ``importlib``
    import and attribute lookup. An empty dict means every claimed
    implementation symbol was proven to exist in the live codebase (not
    merely asserted in prose).
    """
    failures: dict[str, str] = {}
    for item in manifest.items:
        symbol = item.implementation_symbol
        if not symbol:
            continue
        module_name, _, attr_path = symbol.partition(":")
        if not attr_path:
            # Dotted "module.submodule.attr" form: split on the last dot(s)
            # by progressively trying to import the longest valid module
            # prefix, since symbols may themselves contain dots (methods).
            parts = symbol.split(".")
            resolved = False
            for split_at in range(len(parts), 0, -1):
                candidate_module = ".".join(parts[:split_at])
                try:
                    module = importlib.import_module(candidate_module)
                except ImportError:
                    continue
                obj: Any = module
                ok = True
                for attr in parts[split_at:]:
                    if not hasattr(obj, attr):
                        ok = False
                        break
                    obj = getattr(obj, attr)
                if ok:
                    resolved = True
                    break
            if not resolved:
                failures[item.id] = f"could not resolve implementation_symbol {symbol!r}"
            continue
        try:
            module = importlib.import_module(module_name)
        except ImportError as error:
            failures[item.id] = f"module {module_name!r} failed to import: {error}"
            continue
        obj = module
        for attr in attr_path.split("."):
            if not hasattr(obj, attr):
                failures[item.id] = (
                    f"module {module_name!r} has no attribute path {attr_path!r}"
                )
                break
            obj = getattr(obj, attr)
    return failures


def summarize(manifest: OptimizationManifest) -> dict[str, Any]:
    """Return aggregate counts used for the final RETURN report."""
    status_counts = Counter(item.status.value for item in manifest.items)
    applicability_counts = Counter(item.applicability.value for item in manifest.items)
    return {
        "total_items": len(manifest.items),
        "by_status": dict(status_counts),
        "by_applicability": dict(applicability_counts),
        "implemented_or_live_proven": status_counts[ImplementationStatus.IMPLEMENTED.value]
        + status_counts[ImplementationStatus.LIVE_PROVEN.value],
        "partial": status_counts[ImplementationStatus.PARTIAL.value],
        "not_implemented": status_counts[ImplementationStatus.NOT_IMPLEMENTED.value],
        "deferred_pending_telemetry": status_counts[
            ImplementationStatus.DEFERRED_PENDING_TELEMETRY.value
        ],
        "fabric_platform_blocked": status_counts[ImplementationStatus.FABRIC_PLATFORM_BLOCKED.value],
        "not_applicable": status_counts[ImplementationStatus.NOT_APPLICABLE.value],
    }
