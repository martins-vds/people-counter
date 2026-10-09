"""Compatibility alias for the generic Fabric SJD runtime.

Production entry points import :mod:`people_counter.fabric_sjd_runtime`
directly. This legacy module name remains only for existing Candidate A
definitions and callers.
"""

from __future__ import annotations

import sys

from people_counter import fabric_sjd_runtime as _runtime

sys.modules[__name__] = _runtime
