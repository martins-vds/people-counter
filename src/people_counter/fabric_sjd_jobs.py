"""Production SJD entry points bound only to the stable namespace."""

from __future__ import annotations

import argparse
import dataclasses
import sys
from collections.abc import Sequence

from people_counter.fabric_sjd import FILES_ROOT, FabricSjdConfig, ManualFabricSjdConfig


def _runtime_binding(
    argv: Sequence[str] | None,
) -> tuple[FabricSjdConfig | None, Sequence[str] | None]:
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument(
        "--artifacts", "--manual-artifacts", dest="artifacts",
        nargs=3, metavar=("WORKSPACE", "LAKEHOUSE", "ENVIRONMENT")
    )
    parser.add_argument("--files-root")
    arguments, remaining = parser.parse_known_args(
        sys.argv[1:] if argv is None else argv
    )
    if "--help" in remaining or "-h" in remaining:
        return None, remaining
    if arguments.artifacts is None:
        config = FabricSjdConfig.current()
        if arguments.files_root is None:
            return config, argv
        return dataclasses.replace(config, files_root=arguments.files_root), remaining
    workspace, lakehouse, environment = arguments.artifacts
    config = ManualFabricSjdConfig(
        workspace_id=workspace, lakehouse_id=lakehouse, environment_id=environment,
        files_root=FILES_ROOT if arguments.files_root is None else arguments.files_root,
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
    if config is None:
        from people_counter.fabric_sjd_runtime import control_main as run

        return run(["reconcile", *(remaining or ())], config=None)
    arguments = ["reconcile"] if remaining is None or not remaining else list(remaining)
    if arguments != ["reconcile"]:
        raise ValueError("stable reconciliation accepts only the reconcile command")
    from people_counter.fabric_sjd_runtime import control_main as run

    return run(arguments, config=config)


def gold_main(argv: Sequence[str] | None = None) -> int:
    from people_counter.fabric_sjd_runtime import gold_main as run

    config, arguments = _runtime_binding(argv)
    return run(arguments, config=config)


def refresh_main(argv: Sequence[str] | None = None) -> int:
    from people_counter.fabric_sjd_runtime import refresh_main as run

    config, arguments = _runtime_binding(argv)
    return run(arguments, config=config)
