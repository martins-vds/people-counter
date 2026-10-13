from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from people_counter.fabric_activator import (
    ActivatorAutomationError,
    validate_reflex_bundle,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate a source-controlled Fabric Activator definition."
    )
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args(argv)
    try:
        result = validate_reflex_bundle(arguments.bundle).to_dict()
    except ActivatorAutomationError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    output = json.dumps(result, allow_nan=False, indent=2, sort_keys=True) + "\n"
    if arguments.output is None:
        print(output, end="")
    else:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(output, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
