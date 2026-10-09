"""Production SJD entry points bound only to the stable namespace."""

from __future__ import annotations

from collections.abc import Sequence

from people_counter.fabric_sjd import FabricSjdConfig


def control_main(argv: Sequence[str] | None = None) -> int:
    from people_counter.fabric_sjd_runtime import control_main as run

    return run(argv, config=FabricSjdConfig())


def process_main(argv: Sequence[str] | None = None) -> int:
    from people_counter.fabric_sjd_runtime import process_main as run

    return run(argv, config=FabricSjdConfig(), route_mode="PRODUCTION")


def reconciliation_main(argv: Sequence[str] | None = None) -> int:
    arguments = ["reconcile"] if argv is None else list(argv)
    if arguments != ["reconcile"]:
        raise ValueError("stable reconciliation accepts only the reconcile command")
    return control_main(arguments)


def gold_main(argv: Sequence[str] | None = None) -> int:
    from people_counter.fabric_sjd_runtime import gold_main as run

    return run(argv, config=FabricSjdConfig())
