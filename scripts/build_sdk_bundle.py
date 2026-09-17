#!/usr/bin/env python3
"""Build a CPU or GPU deployment bundle from the repository root."""

from pathlib import Path

from people_counter.bundle import main


if __name__ == "__main__":
    raise SystemExit(
        main(project_root=Path(__file__).resolve().parents[1])
    )
