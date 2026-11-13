"""Work requirements and SDK support, independent of submission provenance."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from people_counter.config import DETECTOR_FLOOR
from people_counter.inference_profiles import (
    PRESETS, PROFILE_OVERRIDE_FIELDS, PROFILE_SCHEMA, artifact_paths, validate_profile,
)
from people_counter.fabric_release_provenance import resolve_installed_package_version


@dataclass(frozen=True)
class CompatibilityDecision:
    supported: bool
    required_contract: dict[str, Any]
    reason_code: str | None = None
    reason: str | None = None


def evaluate_compatibility(
    payload: Mapping[str, Any], *, fabric: bool = False,
) -> CompatibilityDecision:
    contract = {
        "work_schema_version": payload.get("work_schema_version", 1),
        "payload_schema_version": payload.get("payload_schema_version", 1),
        "profile_schema": payload.get("profile_schema"),
        "profile_id": payload.get("profile_id"),
        "profile_overrides": payload.get("profile_overrides"),
        "pipeline": payload.get("pipeline", "rtdetr-osnet"),
        "detector_model": payload.get("detector_model", "r18"),
        "model_format": payload.get("model_format", "pytorch"),
        "device_variant": payload.get("device_variant", "cpu"),
        "device": payload.get("device", "cpu"),
        "batch_size": payload.get("batch_size", 1),
        "sample_fps": payload.get("sample_fps", 3.0),
        "detection_threshold": payload.get("detection_threshold", 0.6),
        "use_fp16": payload.get("use_fp16", False),
        "model_artifact_sha256": payload.get("model_artifact_sha256", {}),
    }
    schema_failure = _schema_incompatibility(payload, contract)
    if schema_failure is not None:
        return schema_failure
    pipelines = ("rtdetr-osnet",) if fabric else ("rtdetr-osnet", "rfdetr-botsort")
    for field, supported, code in (
        ("pipeline", pipelines, "UNSUPPORTED_PIPELINE"),
        ("detector_model", ("r18", "r50"), "UNSUPPORTED_DETECTOR"),
        ("model_format", ("pytorch", "onnx"), "UNSUPPORTED_MODEL_FORMAT"),
        ("device_variant", ("cpu",), "UNSUPPORTED_DEVICE"),
        ("device", ("cpu",), "UNSUPPORTED_DEVICE"),
    ):
        if contract[field] not in supported:
            return CompatibilityDecision(False, contract, code, f"Unsupported {field}: {contract[field]!r}")
    if _has_unsupported_inference_settings(contract):
        return CompatibilityDecision(False, contract, "UNSUPPORTED_INFERENCE_SETTINGS", "Inference settings are not supported by the CPU worker")
    artifacts = contract["model_artifact_sha256"]
    if not isinstance(artifacts, Mapping):
        raise ValueError("model_artifact_sha256 must be an object")
    validate_profile(payload)
    if fabric and artifacts:
        name = f"rtdetr-{contract['detector_model']}-{contract['model_format']}-cpu"
        if set(artifacts) != set(artifact_paths(name)):
            return CompatibilityDecision(False, contract, "UNSUPPORTED_MODEL_REQUIREMENTS", "Model artifact declarations differ from the required pipeline artifacts")
    return CompatibilityDecision(True, contract)


def _schema_incompatibility(
    payload: Mapping[str, Any], contract: dict[str, Any],
) -> CompatibilityDecision | None:
    for field, code in (
        ("work_schema_version", "UNSUPPORTED_WORK_SCHEMA"),
        ("payload_schema_version", "UNSUPPORTED_PAYLOAD_SCHEMA"),
    ):
        if type(contract[field]) is not int or contract[field] != 1:
            return CompatibilityDecision(False, contract, code, f"Unsupported {field}: {contract[field]!r}")
    profile_schema = contract["profile_schema"]
    if profile_schema is not None and profile_schema != PROFILE_SCHEMA:
        return CompatibilityDecision(False, contract, "UNSUPPORTED_PROFILE_SCHEMA", f"Unsupported profile schema: {profile_schema!r}")
    if profile_schema is not None and (
        not isinstance(payload.get("profile_id"), str) or payload["profile_id"] not in PRESETS
    ):
        return CompatibilityDecision(False, contract, "UNSUPPORTED_INFERENCE_PROFILE", f"Unsupported inference profile: {payload.get('profile_id')!r}")
    overrides = contract["profile_overrides"]
    if profile_schema is not None and isinstance(overrides, Mapping) and overrides.keys() - PROFILE_OVERRIDE_FIELDS:
        return CompatibilityDecision(False, contract, "UNSUPPORTED_INFERENCE_SETTINGS", "Inference profile requires unsupported overrides")
    return None


def _has_unsupported_inference_settings(contract: Mapping[str, Any]) -> bool:
    batch = contract["batch_size"]
    fps = contract["sample_fps"]
    threshold = contract["detection_threshold"]
    return (
        type(batch) is not int or batch not in (1, 2, 4)
        or (fps is not None and (type(fps) not in (int, float) or not math.isfinite(fps) or fps <= 0))
        or type(threshold) not in (int, float)
        or not math.isfinite(threshold) or not DETECTOR_FLOOR <= threshold <= 1
        or contract["use_fp16"] is not False
    )


def compatibility_evaluation(
    payload: Mapping[str, Any], *, fabric: bool, now: float,
    operator: str | None = None, reason: str | None = None,
) -> dict[str, Any]:
    decision = evaluate_compatibility(payload, fabric=fabric)
    return {
        "supported": decision.supported,
        "reason_code": decision.reason_code,
        "reason": decision.reason,
        "required_contract": decision.required_contract,
        "evaluating_sdk_version": resolve_installed_package_version(),
        "evaluated_at": now,
        "operator": operator,
        "operator_reason": reason,
    }
