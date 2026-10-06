"""Installed-wheel fixed production-shadow live control entry."""

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
    """Run one signed control operation; no generic Candidate A CLI is exposed."""

    return live_main(
        argv,
        job="control",
        files=files,
        spark=spark,
    )
