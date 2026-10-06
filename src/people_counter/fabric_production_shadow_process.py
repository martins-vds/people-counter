"""Installed-wheel fixed production-shadow SDK process entry."""

from __future__ import annotations

from typing import Sequence

from typing import Any

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
    """Run the exact signed batch with real SDK mode in the shadow namespace."""

    return live_main(
        argv,
        job="process",
        files=files,
        spark=spark,
    )
