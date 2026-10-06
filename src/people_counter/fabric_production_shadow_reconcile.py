"""Installed-wheel fixed production-shadow comparison/reconcile entry."""

from __future__ import annotations

from typing import Any, Sequence

from people_counter.fabric_production_shadow_live import (
    ShadowEvidenceFiles,
    main as live_main,
)


def main(
    argv: Sequence[str] | None = None,
    *,
    files: ShadowEvidenceFiles | None = None,
    spark: Any | None = None,
) -> int:
    """Run one signed compare/reconcile operation in the fixed namespace."""

    return live_main(
        argv,
        job="reconcile",
        files=files,
        spark=spark,
    )
