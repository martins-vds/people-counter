"""Deterministic definitions for stable production Spark Job Definitions."""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from people_counter.fabric_target import FabricTarget


ENTRY_POINTS: Mapping[str, str] = {
    "control": "people_counter.fabric_sjd_jobs:control_main",
    "dispatcher": "people_counter.fabric_sjd_jobs:dispatcher_main",
    "process": "people_counter.fabric_sjd_jobs:process_main",
    "refresh": "people_counter.fabric_sjd_jobs:refresh_main",
    "reconciliation": "people_counter.fabric_sjd_jobs:reconciliation_main",
    "gold": "people_counter.fabric_sjd_jobs:gold_main",
}


def reviewed_compute_settings(existing: Mapping[str, Any]) -> dict[str, Any]:
    properties = {
        row["key"]: row["value"] for row in existing.get("sparkProperties", [])
    }
    properties.update({"spark.speculation": "false", "spark.task.cpus": "1", "spark.executor.instances": "2"})
    return {
        "driverCores": 8, "driverMemory": "56g",
        "executorCores": 8, "executorMemory": "56g",
        "dynamicExecutorAllocation": {"enabled": False},
        "runtimeVersion": "2.0",
        "sparkProperties": [{"key": key, "value": value} for key, value in sorted(properties.items())],
    }


def validate_reviewed_compute_settings(settings: Mapping[str, Any]) -> None:
    expected = reviewed_compute_settings(settings)
    for key, value in expected.items():
        if key == "sparkProperties":
            actual = {row["key"]: row["value"] for row in settings.get(key, [])}
            if actual != {row["key"]: row["value"] for row in value}:
                raise RuntimeError("Environment Spark properties differ from reviewed fixed allocation")
        elif settings.get(key) != value:
            raise RuntimeError(f"Environment {key} differs from reviewed fixed allocation")


def _schedule_configuration(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("schedule configuration must be an object")
    result = dict(value)
    if "interval" in result and type(result["interval"]) is not int:
        raise ValueError("schedule interval must be an integer")
    for key in ("startDateTime", "endDateTime"):
        text = result.get(key)
        if not isinstance(text, str):
            raise ValueError(f"schedule {key} must be an ISO datetime")
        try:
            instant = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(f"schedule {key} must be an ISO datetime") from error
        if result.get("localTimeZoneId") == "UTC":
            if instant.tzinfo is None:
                instant = instant.replace(tzinfo=timezone.utc)
            instant = instant.astimezone(timezone.utc)
        result[key] = instant.isoformat()
    return result


def validate_schedule_readback(observed: object, expected: Mapping[str, Any]) -> None:
    if (
        not isinstance(observed, Mapping)
        or type(observed.get("enabled")) is not bool
        or observed.get("enabled") is not expected["enabled"]
    ):
        raise ValueError("schedule enabled readback differs")
    if _schedule_configuration(observed.get("configuration")) != _schedule_configuration(expected.get("configuration")):
        raise ValueError("schedule configuration readback differs")
    if observed.get("executionData") != expected.get("executionData"):
        raise ValueError("schedule execution data readback differs")


def definition_part(path: str, content: bytes) -> dict[str, str]:
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


def build_sjd_definition(
    kind: str, *, config: FabricTarget, command_line_arguments: str = "",
) -> dict[str, object]:
    if kind not in ENTRY_POINTS:
        raise ValueError(f"unsupported stable SJD kind {kind!r}")
    if "\x00" in command_line_arguments:
        raise ValueError("command_line_arguments contains NUL")
    metadata = {
        "additionalLakehouseIds": [],
        "additionalLibraryUris": [],
        "commandLineArguments": command_line_arguments,
        "defaultLakehouseArtifactId": config.lakehouse_id,
        "environmentArtifactId": config.environment_id,
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
                definition_part("Main/main.py", _main_source(ENTRY_POINTS[kind])),
                definition_part("SparkJobDefinitionV1.json", metadata_bytes),
            ],
        }
    }
