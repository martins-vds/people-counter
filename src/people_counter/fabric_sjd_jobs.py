"""Production SJD entry points bound only to the stable namespace."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from people_counter.fabric_sjd import FabricSjdConfig, ManualFabricSjdConfig


def _runtime_binding(
    argv: Sequence[str] | None,
) -> tuple[FabricSjdConfig, Sequence[str] | None]:
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument(
        "--manual-artifacts", nargs=3, metavar=("WORKSPACE", "LAKEHOUSE", "ENVIRONMENT")
    )
    arguments, remaining = parser.parse_known_args(
        sys.argv[1:] if argv is None else argv
    )
    if arguments.manual_artifacts is None:
        return FabricSjdConfig(), argv
    workspace, lakehouse, environment = arguments.manual_artifacts
    config = ManualFabricSjdConfig(
        workspace_id=workspace, lakehouse_id=lakehouse, environment_id=environment
    )
    import notebookutils

    config.validate_runtime_context(notebookutils.runtime.context)
    return config, remaining


def control_main(argv: Sequence[str] | None = None) -> int:
    from people_counter.fabric_sjd_runtime import control_main as run

    config, arguments = _runtime_binding(argv)
    return run(arguments, config=config)


def process_main(argv: Sequence[str] | None = None) -> int:
    from people_counter.fabric_sjd_runtime import process_main as run

    config, arguments = _runtime_binding(argv)
    return run(arguments, config=config, route_mode="PRODUCTION")


def dispatcher_main(argv: Sequence[str] | None = None) -> int:
    from people_counter.fabric_sjd_runtime import dispatcher_main as run

    config, arguments = _runtime_binding(argv)
    return run(arguments, config=config, route_mode="PRODUCTION")


def reconciliation_main(argv: Sequence[str] | None = None) -> int:
    config, remaining = _runtime_binding(argv)
    manual = isinstance(config, ManualFabricSjdConfig)
    arguments = ["reconcile"] if remaining is None or (manual and not remaining) else list(remaining)
    if arguments != ["reconcile"]:
        raise ValueError("stable reconciliation accepts only the reconcile command")
    if manual:
        from people_counter.fabric_sjd_runtime import control_main as run

        return run(arguments, config=config)
    return control_main(arguments)


def gold_main(argv: Sequence[str] | None = None) -> int:
    from people_counter.fabric_sjd_runtime import gold_main as run

    config, arguments = _runtime_binding(argv)
    return run(arguments, config=config)


def refresh_main(argv: Sequence[str] | None = None) -> int:
    from people_counter.fabric_sjd_runtime import refresh_main as run

    config, arguments = _runtime_binding(argv)
    return run(arguments, config=config)
