"""Authoritative Fabric inference presets and automatically derived identities."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from people_counter.model_artifacts import (
    OSNET_FILENAME,
    OSNET_MODEL_DIR,
    OSNET_ONNX_FILENAME,
    RTDETR_MODEL_DIRS,
    RTDETR_ONNX_FILENAME,
    RTDETR_PIPELINE_DIR,
)

PROFILE_SCHEMA = "people-counter-inference-profile-v1"
PRESETS = {
    f"rtdetr-{detector}-{model_format}-cpu": {
        "pipeline": "rtdetr-osnet",
        "detector_model": detector,
        "model_format": model_format,
        "device_variant": "cpu",
        "device": "cpu",
        "batch_size": 1,
        "sample_fps": 1.0,
        "detection_threshold": 0.6,
        "use_fp16": False,
    }
    for detector in ("r18", "r50")
    for model_format in ("onnx", "pytorch")
}


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")


def _identity(schema: str, value: object) -> str:
    return hashlib.sha256(_canonical({"schema": schema, "value": value})).hexdigest()


def _preset(name: str) -> dict[str, Any]:
    if not isinstance(name, str) or name not in PRESETS:
        raise ValueError(f"Unsupported Fabric inference profile {name!r}; choose from {sorted(PRESETS)}")
    return dict(PRESETS[name])


def artifact_paths(name: str) -> tuple[str, ...]:
    preset = _preset(name)
    detector = RTDETR_MODEL_DIRS[preset["detector_model"]]
    onnx = preset["model_format"] == "onnx"
    detector_file = RTDETR_ONNX_FILENAME if onnx else "model.safetensors"
    reid_file = OSNET_ONNX_FILENAME if onnx else OSNET_FILENAME
    return (
        f"{RTDETR_PIPELINE_DIR}/{detector}/config.json",
        f"{RTDETR_PIPELINE_DIR}/{detector}/preprocessor_config.json",
        f"{RTDETR_PIPELINE_DIR}/{detector}/{detector_file}",
        f"{RTDETR_PIPELINE_DIR}/{OSNET_MODEL_DIR}/{reid_file}",
    )


def _overrides(values: Mapping[str, Any] | None) -> dict[str, Any]:
    if values is not None and not isinstance(values, Mapping):
        raise ValueError("profile overrides must be an object")
    overrides = dict(values or {})
    if overrides.keys() - {"batch_size", "sample_fps"}:
        raise ValueError("profile overrides support only batch_size and sample_fps")
    if "batch_size" in overrides:
        batch = overrides["batch_size"]
        if type(batch) is not int or batch not in {1, 2, 4}:
            raise ValueError("profile batch_size must be 1, 2, or 4")
    if "sample_fps" in overrides:
        fps = overrides["sample_fps"]
        if type(fps) not in (int, float) or not math.isfinite(fps) or fps <= 0:
            raise ValueError("profile sample_fps must be positive and finite")
        overrides["sample_fps"] = float(fps)
    return overrides


def resolve_profile(
    name: str,
    artifact_sha256: Mapping[str, str],
    *,
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    settings = _preset(name)
    selected_overrides = _overrides(overrides)
    settings.update(selected_overrides)
    expected = artifact_paths(name)
    if not isinstance(artifact_sha256, Mapping) or set(artifact_sha256) != set(expected):
        raise ValueError(f"profile artifact hashes must contain exactly {list(expected)}")
    hashes = {}
    for path in expected:
        digest = artifact_sha256[path]
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ValueError(f"Invalid profile artifact SHA-256: {path}")
        hashes[path] = digest
    effective = {**settings, "model_artifact_sha256": hashes}
    return {
        "profile_schema": PROFILE_SCHEMA,
        "profile_id": name,
        "profile_overrides": selected_overrides,
        **effective,
        "profile_sha256": _identity(PROFILE_SCHEMA, effective),
        "runtime_key": _identity("people-counter-runtime-compatibility-v1", effective),
    }


def prepare_profile(
    name: str, models_dir: Path, *, overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _overrides(overrides)
    hashes = {}
    for relative in artifact_paths(name):
        digest = hashlib.sha256()
        with (models_dir / relative).open("rb") as stream:
            for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                digest.update(chunk)
        hashes[relative] = digest.hexdigest()
    return resolve_profile(name, hashes, overrides=overrides)


def validate_profile(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    if "profile_schema" not in payload:
        return None
    if payload["profile_schema"] != PROFILE_SCHEMA:
        raise ValueError("Unsupported inference profile schema")
    overrides = payload.get("profile_overrides")
    if not isinstance(overrides, Mapping):
        raise ValueError("profile_overrides must be an object")
    name = payload.get("profile_id")
    artifacts = payload.get("model_artifact_sha256")
    if not isinstance(name, str) or not isinstance(artifacts, Mapping):
        raise ValueError("profile name and model artifact hashes are required")
    resolved = resolve_profile(name, artifacts, overrides=overrides)
    for key, value in resolved.items():
        if key not in payload or _canonical(payload[key]) != _canonical(value):
            raise ValueError(f"inference profile {key} differs from its resolved configuration")
    return resolved


def configuration_sha256(payload: Mapping[str, Any]) -> str:
    profile = validate_profile(payload)
    if profile is None:
        raise ValueError("A managed inference profile is required for configuration hashing")
    line = payload.get("line")
    if (
        not isinstance(line, (list, tuple)) or len(line) != 4
        or any(type(value) not in (int, float) or not math.isfinite(value) for value in line)
    ):
        raise ValueError("configuration line must contain four finite coordinates")
    return _identity(
        "people-counter-work-configuration-v1",
        {"profile_sha256": profile["profile_sha256"], "line": list(line)},
    )


def validate_registration_profile(
    payload: Mapping[str, Any], runtime_key: object, config_sha256: object,
) -> None:
    profile = validate_profile(payload)
    if profile is None:
        return
    if runtime_key != profile["runtime_key"]:
        raise ValueError("runtime_key differs from the resolved inference profile")
    if config_sha256 != configuration_sha256(payload):
        raise ValueError("config_sha256 differs from the resolved work configuration")
