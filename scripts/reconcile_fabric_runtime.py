"""Thin CLI entry point for :func:`people_counter.fabric_reconcilers.main`."""

from __future__ import annotations

from people_counter.fabric_reconcilers import main

if __name__ == "__main__":
    raise SystemExit(main())
