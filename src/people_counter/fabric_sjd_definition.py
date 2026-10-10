"""Deterministic definitions for stable production Spark Job Definitions."""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping

from people_counter.fabric_sjd import ENVIRONMENT_ID, LAKEHOUSE_ID


ENTRY_POINTS: Mapping[str, str] = {
    "control": "people_counter.fabric_sjd_jobs:control_main",
    "dispatcher": "people_counter.fabric_sjd_jobs:dispatcher_main",
    "process": "people_counter.fabric_sjd_jobs:process_main",
    "refresh": "people_counter.fabric_sjd_jobs:refresh_main",
    "reconciliation": "people_counter.fabric_sjd_jobs:reconciliation_main",
    "gold": "people_counter.fabric_sjd_jobs:gold_main",
    "cutover": "people_counter.fabric_sjd_cutover_jobs:main",
}


def _part(path: str, content: bytes) -> dict[str, str]:
    return {
        "path": path,
        "payload": base64.b64encode(content).decode("ascii"),
        "payloadType": "InlineBase64",
    }


def _main_source(entry_point: str) -> bytes:
    module, function = entry_point.split(":", 1)
    return (
        '"""Generated stable production SJD entry point."""\n\n'
        f"from {module} import {function} as main\n\n"
        'if __name__ == "__main__":\n'
        "    raise SystemExit(main())\n"
    ).encode("utf-8")


def build_sjd_definition(kind: str) -> dict[str, object]:
    if kind not in ENTRY_POINTS:
        raise ValueError(f"unsupported stable SJD kind {kind!r}")
    metadata = {
        "additionalLakehouseIds": [],
        "additionalLibraryUris": [],
        "commandLineArguments": "",
        "defaultLakehouseArtifactId": LAKEHOUSE_ID,
        "environmentArtifactId": ENVIRONMENT_ID,
        "executableFile": "main.py",
        "language": "Python",
        "mainClass": "",
        "retryPolicy": None,
    }
    metadata_bytes = json.dumps(
        metadata,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return {
        "definition": {
            "format": "SparkJobDefinitionV2",
            "parts": [
                _part("Main/main.py", _main_source(ENTRY_POINTS[kind])),
                _part("SparkJobDefinitionV1.json", metadata_bytes),
            ],
        }
    }
