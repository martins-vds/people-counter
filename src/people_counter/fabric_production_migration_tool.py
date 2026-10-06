"""Host-side gate controller for the fixed Candidate A production migration.

All resource identities, item names, and OneLake paths are constants.  The
controller accepts no SQL, table, Fabric item, workspace, or filesystem path
from an operator.  Live-changing commands require ``--execute``; ``snapshot``
and ``review`` never mutate Fabric.
"""

from __future__ import annotations

import argparse
import base64
import email.utils
import hashlib
import hmac
import json
import os
import re
import secrets
import shlex
import subprocess
import sys
import time
import uuid
import zipfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Protocol, TextIO

from people_counter.fabric_canary_tool import (
    FABRIC_API_ROOT,
    HTTPTransport,
    TokenProvider,
    UrllibTransport,
    make_token_provider,
)
from people_counter.fabric_production_migration import (
    ENVIRONMENT_ID,
    LAKEHOUSE_ID,
    MIGRATION_ID,
    WORKSPACE_ID,
    canonical_json,
)
from people_counter.fabric_production_migration_live import (
    FAILED_RECOVERY_INVOCATION_ID,
    FAILED_RECOVERY_JOB_ID,
    FAILED_RECOVERY_RUN_ID,
    INVENTORY_MAX_AGE_SECONDS,
    INVENTORY_SCHEMA,
    SJD_JOB_TIMEOUT_SECONDS,
    diagnose_path,
    failure_path,
    inventory_path,
    plan_path,
    recovery_plan_path,
    recovery_result_path,
    recovery_review_path,
    report_path,
    result_path,
)
from people_counter.fabric_production_migration_sjd import (
    build_migration_sjd_definition,
)
from people_counter.fabric_reflex_definition import (
    REFLEX_ID,
    parse_reflex_rule_definition,
)


ENVIRONMENT_NAME = "people-counter-dev"
LAKEHOUSE_NAME = "people_counter_dev"
REFLEX_NAME = "pc_manifest_arrival_activator"
MIGRATION_SJD_NAME = "pc-ca-production-migration-v001"
PROJECT_VERSION = "0.9.11"
PROJECT_WHEEL = f"people_counter-{PROJECT_VERSION}-py3-none-any.whl"
DIST_WHEEL = Path("dist") / PROJECT_WHEEL
LOCAL_STATE_ROOT = Path(".people-counter-production-migration")
REVIEW_MAX_AGE_SECONDS = 900.0
RECOVERY_REVIEW_MAX_AGE_SECONDS = 7200.0
INVENTORY_APPLY_MAX_AGE_SECONDS = 120.0
JOB_TYPE_PIPELINE = "Pipeline"
JOB_TYPE_SJD = "sparkjob"
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_TERMINAL_SUCCESS = frozenset({"completed", "succeeded"})
_TERMINAL_FAILURE = frozenset({"cancelled", "canceled", "failed", "deduped"})
_ACTIVE_STATES = frozenset(
    {"inprogress", "notstarted", "queued", "running", "starting"}
)
_KNOWN_STATES = _TERMINAL_SUCCESS | _TERMINAL_FAILURE | _ACTIVE_STATES

# This is the complete production writer allowlist.  Benchmark/canary items are
# intentionally absent even when they share the ``pc-`` display-name prefix.
WRITER_ITEMS: tuple[tuple[str, str], ...] = (
    ("pc-event-intake", "5548a877-38e0-4933-bf2d-0285250637d2"),
    ("pc-dispatcher-00", "12e33015-5c82-4ea0-a54e-fe145694ad02"),
    ("pc-watchdog", "2e415a3e-336a-42bf-9e3e-ee7c10c1722f"),
    ("pc-reconcile", "61d98ddf-0a4d-45b7-a21b-fa577f3f909e"),
    ("pc-gold-refresh", "dd4fc19b-7276-4402-9d30-76676f36555b"),
    ("pc-delta-maintenance", "a26b27a5-99db-4c95-9b2f-c048949a8246"),
    ("pc-replay", "f6153ed1-ef84-4270-98ae-c17398dadb61"),
    ("pc-backfill-register", "09d2e5d0-c90a-4a22-885b-b29688983d70"),
    ("pc-dispatcher-executor-00", "bc5209e3-177c-4729-b647-eb48fd33ec95"),
)


class ControllerError(RuntimeError):
    """A host-side precheck, readback, or orchestration gate failed."""


class FabricControllerAPI(Protocol):
    def list_items(self) -> list[Mapping[str, Any]]: ...

    def get_definition(self, item_type: str, item_id: str) -> Mapping[str, Any]: ...

    def list_schedules(
        self, item_id: str, job_type: str
    ) -> list[Mapping[str, Any]]: ...

    def list_job_instances(self, item_id: str) -> list[Mapping[str, Any]]: ...

    def environment_state(self) -> Mapping[str, Any]: ...

    def upload_environment_wheel(self, name: str, content: bytes) -> None: ...

    def delete_staged_environment_library(self, name: str) -> None: ...

    def publish_environment(self) -> Mapping[str, Any]: ...

    def create_sjd(self, payload: Mapping[str, Any]) -> Mapping[str, Any]: ...

    def update_sjd(
        self, item_id: str, payload: Mapping[str, Any]
    ) -> Mapping[str, Any]: ...

    def run_sjd(self, item_id: str, arguments: str) -> str: ...

    def get_job_instance(
        self, item_id: str, job_instance_id: str
    ) -> Mapping[str, Any]: ...


class OneLakeFiles(Protocol):
    def exists(self, path: str) -> bool: ...

    def read_bytes(self, path: str) -> bytes: ...

    def create_bytes(self, path: str, content: bytes) -> None: ...


@dataclass(frozen=True)
class SignedInventory:
    run_id: str
    path: str
    payload: Mapping[str, Any]
    envelope_bytes: bytes
    hmac_key: bytes

    @property
    def payload_sha256(self) -> str:
        return str(json.loads(self.envelope_bytes)["payload_sha256"])

    def redacted_summary(self) -> dict[str, object]:
        return {
            "hmac_key": "<redacted>",
            "inventory_path": self.path,
            "nonce": "<redacted>",
            "payload_sha256": self.payload_sha256,
            "run_id": self.run_id,
        }


@dataclass(frozen=True)
class InvocationResult:
    command: str
    run_id: str
    job_instance_id: str
    evidence: Mapping[str, Any]


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_bytes(value: object) -> bytes:
    return canonical_json(value).encode("utf-8")


def _safe_id(value: str, label: str) -> str:
    if _SAFE_ID.fullmatch(value) is None:
        raise ControllerError(f"{label} must be a safe 1-128 character identifier")
    return value


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ControllerError(f"{label} must be an object")
    return value


def _object_list(value: object, label: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(item, Mapping) for item in value):
        raise ControllerError(f"{label} must be a list of objects")
    return list(value)


def _definition_parts(definition: Mapping[str, Any]) -> dict[str, bytes]:
    body = _mapping(definition.get("definition"), "definition")
    parts = _object_list(body.get("parts"), "definition parts")
    decoded: dict[str, bytes] = {}
    for part in parts:
        path = part.get("path")
        payload = part.get("payload")
        if (
            not isinstance(path, str)
            or not path
            or path in decoded
            or part.get("payloadType") != "InlineBase64"
            or not isinstance(payload, str)
        ):
            raise ControllerError("definition part envelope is invalid or duplicate")
        try:
            decoded[path] = base64.b64decode(payload, validate=True)
        except ValueError as error:
            raise ControllerError("definition part is not canonical base64") from error
        if base64.b64encode(decoded[path]).decode("ascii") != payload:
            raise ControllerError("definition part is not canonical base64")
    return decoded


def _definition_evidence(definition: Mapping[str, Any]) -> dict[str, object]:
    parts = _definition_parts(definition)
    return {
        "definition_sha256": _sha256(_canonical_bytes(definition)),
        "parts": [
            {
                "path": path,
                "sha256": _sha256(content),
                "size_bytes": len(content),
            }
            for path, content in sorted(parts.items())
        ],
    }


def _exact_item_index(
    items: Sequence[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    by_id: dict[str, Mapping[str, Any]] = {}
    for item in items:
        identifier = item.get("id")
        if not isinstance(identifier, str) or not identifier or identifier in by_id:
            raise ControllerError("workspace item IDs must be nonempty and unique")
        by_id[identifier] = item
    expected = {
        ENVIRONMENT_ID: (ENVIRONMENT_NAME, "Environment"),
        LAKEHOUSE_ID: (LAKEHOUSE_NAME, "Lakehouse"),
        REFLEX_ID: (REFLEX_NAME, "Reflex"),
        **{identifier: (name, "DataPipeline") for name, identifier in WRITER_ITEMS},
    }
    for identifier, (name, item_type) in expected.items():
        item = by_id.get(identifier)
        if item is None or item.get("displayName") != name or item.get("type") != item_type:
            raise ControllerError(f"fixed Fabric item binding mismatch for {name}")
    migration = [
        item
        for item in items
        if item.get("displayName") == MIGRATION_SJD_NAME
        and item.get("type") == "SparkJobDefinition"
    ]
    if len(migration) > 1:
        raise ControllerError("multiple migration SJDs have the fixed display name")
    return by_id


def _normalize_schedule(
    raw: Mapping[str, Any], *, item_id: str, item_name: str
) -> dict[str, object]:
    schedule_id = raw.get("id")
    enabled = raw.get("enabled")
    if not isinstance(schedule_id, str) or not schedule_id:
        raise ControllerError("writer schedule ID is missing")
    if type(enabled) is not bool:
        raise ControllerError("writer schedule enabled state is unknown")
    return {
        "definition_sha256": _sha256(_canonical_bytes(raw)),
        "enabled": enabled,
        "item_id": item_id,
        "item_name": item_name,
        "schedule_id": schedule_id,
    }


def _normalize_job(
    raw: Mapping[str, Any], *, item_id: str, item_name: str
) -> dict[str, object]:
    run_id = raw.get("id") or raw.get("jobInstanceId")
    state = raw.get("status")
    job_type = raw.get("jobType")
    if not isinstance(run_id, str) or not run_id:
        raise ControllerError("Fabric job instance ID is missing")
    if not isinstance(state, str) or state.lower() not in _KNOWN_STATES:
        raise ControllerError(f"unknown Fabric job state for {item_name}")
    if not isinstance(job_type, str) or job_type.lower() != JOB_TYPE_PIPELINE.lower():
        raise ControllerError(f"unknown Fabric job type for {item_name}")
    return {
        "end_time_utc": raw.get("endTimeUtc"),
        "invoke_type": raw.get("invokeType"),
        "item_id": item_id,
        "item_name": item_name,
        "job_type": job_type,
        "run_id": run_id,
        "start_time_utc": raw.get("startTimeUtc"),
        "state": state,
    }


def _writer_evidence(
    api: FabricControllerAPI,
) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
    schedules: list[dict[str, object]] = []
    jobs: list[dict[str, object]] = []
    definitions: dict[str, object] = {}
    seen_runs: set[str] = set()
    seen_schedules: set[str] = set()
    for name, identifier in WRITER_ITEMS:
        definition = api.get_definition("DataPipeline", identifier)
        definitions[identifier] = _definition_evidence(definition)
        for raw in api.list_schedules(identifier, JOB_TYPE_PIPELINE):
            normalized = _normalize_schedule(raw, item_id=identifier, item_name=name)
            schedule_id = str(normalized["schedule_id"])
            if schedule_id in seen_schedules:
                raise ControllerError("writer schedule IDs must be globally unique")
            seen_schedules.add(schedule_id)
            schedules.append(normalized)
        for raw in api.list_job_instances(identifier):
            normalized = _normalize_job(raw, item_id=identifier, item_name=name)
            run_id = str(normalized["run_id"])
            if run_id in seen_runs:
                raise ControllerError("Fabric job instance IDs must be globally unique")
            seen_runs.add(run_id)
            jobs.append(normalized)
    schedules.sort(key=lambda value: str(value["schedule_id"]))
    jobs.sort(key=lambda value: str(value["run_id"]))
    return schedules, jobs, definitions


def _compact_job_inventory(
    jobs: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Keep active blockers verbatim and bind terminal history by hash/count."""

    state_counts: dict[str, int] = {}
    active: list[dict[str, object]] = []
    for job in jobs:
        state = str(job["state"]).lower()
        state_counts[state] = state_counts.get(state, 0) + 1
        if state in _ACTIVE_STATES:
            active.append(dict(job))
    return active, {
        "active_count": len(active),
        "all_jobs_sha256": _sha256(_canonical_bytes(list(jobs))),
        "state_counts": dict(sorted(state_counts.items())),
        "total_count": len(jobs),
    }


def _reflex_evidence(
    definition: Mapping[str, Any],
) -> tuple[dict[str, object], bytes]:
    parts = _definition_parts(definition)
    if "ReflexEntities.json" not in parts:
        raise ControllerError("Reflex definition has no ReflexEntities.json part")
    raw = parts["ReflexEntities.json"]
    parsed = parse_reflex_rule_definition(raw)
    return (
        {
            "definition": _definition_evidence(definition),
            "rule": parsed.to_dict(),
        },
        raw,
    )


def _migration_sjd_evidence(
    api: FabricControllerAPI, items: Sequence[Mapping[str, Any]]
) -> dict[str, object] | None:
    matches = [
        item
        for item in items
        if item.get("displayName") == MIGRATION_SJD_NAME
        and item.get("type") == "SparkJobDefinition"
    ]
    if not matches:
        return None
    identifier = matches[0].get("id")
    if not isinstance(identifier, str):
        raise ControllerError("migration SJD has no item ID")
    definition = api.get_definition("SparkJobDefinition", identifier)
    return {
        "definition": _definition_evidence(definition),
        "item_id": identifier,
        "semantic_valid": _validate_sjd_semantics(definition) is None,
    }


def capture_snapshot(
    api: FabricControllerAPI,
    *,
    now: float | None = None,
) -> dict[str, object]:
    """Capture only externally observable REST state.

    Spark tables, locks, leases, pointers, views, and gold evidence are
    intentionally absent; the live Spark driver owns those observations.
    """

    captured = time.time() if now is None else float(now)
    items = sorted(api.list_items(), key=lambda item: str(item.get("id", "")))
    _exact_item_index(items)
    schedules, jobs, definitions = _writer_evidence(api)
    active_jobs, job_inventory = _compact_job_inventory(jobs)
    reflex_definition = api.get_definition("Reflex", REFLEX_ID)
    reflex, reflex_raw = _reflex_evidence(reflex_definition)
    environment = _mapping(api.environment_state(), "Environment state")
    _verify_environment_policy(environment, deployed=False)
    payload: dict[str, object] = {
        "artifact_binding": {
            "environment_id": ENVIRONMENT_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "workspace_id": WORKSPACE_ID,
        },
        "captured_at": captured,
        "environment": environment,
        "expires_at": captured + INVENTORY_MAX_AGE_SECONDS,
        "fabric_job_inventory": job_inventory,
        "fabric_jobs": active_jobs,
        "item_definitions": definitions,
        "migration_sjd": _migration_sjd_evidence(api, items),
        "reflex": reflex,
        "reflex_definition_base64": base64.b64encode(reflex_raw).decode("ascii"),
        "reflex_id": REFLEX_ID,
        "schema": INVENTORY_SCHEMA,
        "workspace_items": {
            "items": [dict(item) for item in items],
            "sha256": _sha256(_canonical_bytes(items)),
        },
        "writer_schedules": schedules,
    }
    return payload


def capture_recovery_snapshot(
    api: FabricControllerAPI,
    *,
    now: float | None = None,
) -> dict[str, object]:
    """Capture the normal quiescence inventory plus one exact failed SJD job."""

    payload = capture_snapshot(api, now=now)
    item_id = _migration_sjd_id(api)
    state = _mapping(
        api.get_job_instance(item_id, FAILED_RECOVERY_JOB_ID),
        "failed recovery job instance",
    )
    failure = _mapping(state.get("failureReason"), "failed recovery job reason")
    required = {
        "id": FAILED_RECOVERY_JOB_ID,
        "itemId": item_id,
        "jobType": JOB_TYPE_SJD,
        "status": "Failed",
    }
    if any(state.get(name) != value for name, value in required.items()):
        raise ControllerError("Fabric REST recovery job is not the exact failed SJD")
    start = state.get("startTimeUtc")
    end = state.get("endTimeUtc")
    if not isinstance(start, str) or not start or not isinstance(end, str) or not end:
        raise ControllerError("Fabric REST recovery job timestamps are missing")
    stable_failure = {
        "error_code": failure.get("errorCode"),
        "is_retriable": failure.get("isRetriable"),
        "message": failure.get("message"),
    }
    stable_state = {
        "end_time_utc": end,
        "failure": stable_failure,
        "invoke_type": state.get("invokeType"),
        "item_id": state.get("itemId"),
        "job_id": state.get("id"),
        "job_type": state.get("jobType"),
        "root_activity_id": state.get("rootActivityId"),
        "start_time_utc": start,
        "status": state.get("status"),
    }
    payload["lock_recovery"] = {
        "end_time_utc": end,
        "failure_reason_sha256": _sha256(_canonical_bytes(stable_failure)),
        "invocation_id": FAILED_RECOVERY_INVOCATION_ID,
        "item_id": item_id,
        "job_id": FAILED_RECOVERY_JOB_ID,
        "job_state_sha256": _sha256(_canonical_bytes(stable_state)),
        "job_type": state["jobType"],
        "start_time_utc": start,
        "status": state["status"],
    }
    return payload


def sign_inventory(
    payload: Mapping[str, Any],
    run_id: str,
    *,
    random_bytes: Callable[[int], bytes] = secrets.token_bytes,
) -> SignedInventory:
    safe_run = _safe_id(run_id, "run ID")
    key = random_bytes(32)
    nonce = random_bytes(16).hex()
    if len(key) != 32 or len(nonce) != 32:
        raise ControllerError("cryptographic random source returned the wrong length")
    signed_payload = dict(payload)
    signed_payload["nonce"] = nonce
    payload_bytes = _canonical_bytes(signed_payload)
    envelope = {
        "payload": signed_payload,
        "payload_sha256": _sha256(payload_bytes),
        "signature_sha256": hmac.new(key, payload_bytes, hashlib.sha256).hexdigest(),
    }
    return SignedInventory(
        run_id=safe_run,
        path=inventory_path(safe_run),
        payload=signed_payload,
        envelope_bytes=_canonical_bytes(envelope) + b"\n",
        hmac_key=key,
    )


def upload_inventory(files: OneLakeFiles, inventory: SignedInventory) -> None:
    """Create once and require exact readback; collisions always fail."""

    if files.exists(inventory.path):
        raise FileExistsError(inventory.path)
    files.create_bytes(inventory.path, inventory.envelope_bytes)
    observed = files.read_bytes(inventory.path)
    if not hmac.compare_digest(_sha256(observed), _sha256(inventory.envelope_bytes)):
        raise ControllerError("OneLake inventory readback hash mismatch")


def _metadata(definition: Mapping[str, Any]) -> Mapping[str, Any]:
    parts = _definition_parts(definition)
    expected = {"Main/main.py", "SparkJobDefinitionV1.json"}
    if set(parts) not in (expected, expected | {".platform"}):
        raise ControllerError("migration SJD must contain exactly Main and metadata")
    try:
        value = json.loads(parts["SparkJobDefinitionV1.json"])
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ControllerError("migration SJD metadata is invalid JSON") from error
    return _mapping(value, "migration SJD metadata")


def _validate_sjd_semantics(definition: Mapping[str, Any]) -> None:
    expected = build_migration_sjd_definition()
    observed_parts = _definition_parts(definition)
    expected_parts = _definition_parts(expected)
    if observed_parts.get("Main/main.py") != expected_parts["Main/main.py"]:
        raise ControllerError("migration SJD semantic readback differs")
    metadata = _metadata(definition)
    if metadata != _metadata(expected):
        raise ControllerError("migration SJD bindings or saved defaults are unsafe")


def _environment_custom_wheels(state: Mapping[str, Any], stage: str) -> list[str]:
    value = _mapping(state.get(stage), f"Environment {stage}")
    if stage == "staged_libraries" and "customLibraries" in value:
        custom = _mapping(
            value.get("customLibraries"), "Environment staged custom libraries"
        )
        wheels = custom.get("wheelFiles")
        if not isinstance(wheels, list) or any(
            not isinstance(item, str) or not item.endswith(".whl")
            for item in wheels
        ):
            raise ControllerError("Environment staged wheel files are ambiguous")
        return sorted(wheels)
    libraries = _object_list(value.get("libraries"), f"Environment {stage} libraries")
    return sorted(
        str(item["name"])
        for item in libraries
        if item.get("libraryType") == "Custom"
        and isinstance(item.get("name"), str)
        and str(item["name"]).endswith(".whl")
    )


def _environment_public_packages(state: Mapping[str, Any]) -> dict[str, object]:
    published = _mapping(
        state.get("published_libraries"), "Environment published libraries"
    )
    libraries = _object_list(
        published.get("libraries"), "Environment published libraries"
    )
    external = sorted(
        (dict(item) for item in libraries if item.get("libraryType") != "Custom"),
        key=lambda item: _canonical_bytes(item),
    )
    staged = _mapping(state.get("staged_libraries"), "Environment staged libraries")
    environment_yml = staged.get("environmentYml")
    if not isinstance(environment_yml, str):
        raise ControllerError("Environment staged public packages are ambiguous")
    return {
        "published_external": external,
        "staged_environment_yml": environment_yml,
    }


def _verify_environment_policy(state: Mapping[str, Any], *, deployed: bool) -> None:
    for name in ("published_compute", "staged_compute"):
        compute = _mapping(state.get(name), f"Environment {name}")
        if str(compute.get("runtimeVersion")) != "2.0":
            raise ControllerError("Environment runtime must be exactly 2.0")
    expected = [PROJECT_WHEEL]
    staged = _environment_custom_wheels(state, "staged_libraries")
    published = _environment_custom_wheels(state, "published_libraries")
    if deployed and (staged != expected or published != expected):
        raise ControllerError(
            f"Environment must publish solely the {PROJECT_VERSION} project wheel"
        )
    if deployed:
        item = _mapping(state.get("item"), "Environment item")
        properties = _mapping(item.get("properties"), "Environment properties")
        details = _mapping(
            properties.get("publishDetails"), "Environment publish details"
        )
        if details.get("state") != "Success":
            raise ControllerError("Environment publish did not read back Success")


def _verify_wheel(content: bytes) -> str:
    if not content.startswith(b"PK"):
        raise ControllerError("built artifact is not a wheel")
    digest = _sha256(content)
    # The caller reads from a fixed path, so a transient in-memory ZipFile is
    # unnecessary: verify the filename/version before the build and hash bytes.
    if PROJECT_VERSION != "0.9.11" or not DIST_WHEEL.name.startswith(
        f"people_counter-{PROJECT_VERSION}-"
    ):
        raise ControllerError(
            f"wheel version binding is not exactly {PROJECT_VERSION}"
        )
    return digest


def build_wheel(
    *,
    runner: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
) -> tuple[bytes, str]:
    env = dict(os.environ)
    env["SOURCE_DATE_EPOCH"] = "315532800"
    runner(
        ["uv", "build", "--wheel", "--out-dir", str(DIST_WHEEL.parent)],
        check=True,
        env=env,
    )
    if not DIST_WHEEL.is_file() or DIST_WHEEL.is_symlink():
        raise ControllerError(f"exact wheel was not built at {DIST_WHEEL}")
    content = DIST_WHEEL.read_bytes()
    with zipfile.ZipFile(DIST_WHEEL) as archive:
        metadata_names = [
            name
            for name in archive.namelist()
            if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_names) != 1:
            raise ControllerError("wheel has ambiguous package metadata")
        metadata = archive.read(metadata_names[0]).decode("utf-8")
    if (
        "Name: people-counter\n" not in metadata
        or f"Version: {PROJECT_VERSION}\n" not in metadata
    ):
        raise ControllerError(
            f"wheel metadata is not people-counter {PROJECT_VERSION}"
        )
    return content, _verify_wheel(content)


def deploy(
    api: FabricControllerAPI,
    *,
    execute: bool,
    wheel_builder: Callable[[], tuple[bytes, str]] = build_wheel,
) -> dict[str, object]:
    wheel, wheel_sha = wheel_builder()
    definition = build_migration_sjd_definition()
    _validate_sjd_semantics(definition)
    definition_sha = _sha256(_canonical_bytes(definition))
    preview: dict[str, object] = {
        "definition_sha256": definition_sha,
        "execute": execute,
        "sjd_name": MIGRATION_SJD_NAME,
        "wheel": PROJECT_WHEEL,
        "wheel_sha256": wheel_sha,
    }
    if not execute:
        return preview
    state = _mapping(api.environment_state(), "Environment state")
    _verify_environment_policy(state, deployed=False)
    public_packages = _environment_public_packages(state)
    preview["public_packages_sha256"] = _sha256(_canonical_bytes(public_packages))
    staged_wheels = _environment_custom_wheels(state, "staged_libraries")
    published_wheels = _environment_custom_wheels(state, "published_libraries")
    if staged_wheels == [PROJECT_WHEEL] and published_wheels == [PROJECT_WHEEL]:
        _verify_environment_policy(state, deployed=True)
        published = state
        preview["environment_action"] = "already_current"
    else:
        for name in staged_wheels:
            if name != PROJECT_WHEEL:
                api.delete_staged_environment_library(name)
        api.upload_environment_wheel(PROJECT_WHEEL, wheel)
        api.publish_environment()
        published = _mapping(api.environment_state(), "Environment state")
        _verify_environment_policy(published, deployed=True)
        preview["environment_action"] = "published"
    if _environment_public_packages(published) != public_packages:
        raise ControllerError("Environment public packages changed during deployment")
    items = api.list_items()
    matches = [
        item
        for item in items
        if item.get("displayName") == MIGRATION_SJD_NAME
        and item.get("type") == "SparkJobDefinition"
    ]
    if len(matches) > 1:
        raise ControllerError("multiple migration SJDs have the fixed display name")
    if matches:
        item_id = str(matches[0]["id"])
        api.update_sjd(item_id, definition)
    else:
        created = api.create_sjd(definition)
        item_id = str(created.get("id", ""))
    if not item_id:
        raise ControllerError("migration SJD deployment returned no item ID")
    readback = api.get_definition("SparkJobDefinition", item_id)
    _validate_sjd_semantics(readback)
    return {**preview, "item_id": item_id, "readback": "verified"}


def _migration_sjd_id(api: FabricControllerAPI) -> str:
    matches = [
        item
        for item in api.list_items()
        if item.get("displayName") == MIGRATION_SJD_NAME
        and item.get("type") == "SparkJobDefinition"
    ]
    if len(matches) != 1 or not isinstance(matches[0].get("id"), str):
        raise ControllerError("exactly one deployed migration SJD is required")
    item_id = str(matches[0]["id"])
    _validate_sjd_semantics(api.get_definition("SparkJobDefinition", item_id))
    return item_id


def _poll_job(
    api: FabricControllerAPI,
    item_id: str,
    instance_id: str,
    *,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
    timeout: float,
    hidden: Sequence[str] = (),
) -> Mapping[str, Any]:
    deadline = clock() + timeout
    while True:
        state = _mapping(
            api.get_job_instance(item_id, instance_id), "Fabric job instance"
        )
        status = state.get("status")
        if not isinstance(status, str) or status.lower() not in _KNOWN_STATES:
            raise ControllerError("migration SJD returned an unknown job state")
        if status.lower() in _TERMINAL_SUCCESS:
            return state
        if status.lower() in _TERMINAL_FAILURE:
            detail = redact(state.get("failureReason"), hidden)
            raise ControllerError("migration SJD failed: " + detail)
        if clock() >= deadline:
            raise TimeoutError("timed out waiting for migration SJD")
        sleep(2.0)


def _redacted_arguments(arguments: Sequence[str]) -> list[str]:
    redacted: list[str] = []
    hide_next = False
    sensitive = {
        "--inventory-hmac-key",
        "--lease-token",
        "--recovery-token",
        "--safety-token",
    }
    for raw in arguments:
        value = str(raw)
        if hide_next:
            redacted.append("<redacted>")
            hide_next = False
            continue
        matched = next(
            (name for name in sensitive if value.startswith(name + "=")), None
        )
        if matched is not None:
            redacted.append(matched + "=<redacted>")
            continue
        redacted.append(value)
        hide_next = value in sensitive
    return redacted


def _job_failure_detail(
    files: OneLakeFiles,
    *,
    migration_run: str,
    invocation_id: str,
    instance_id: str,
    hidden: Sequence[str],
) -> dict[str, object]:
    recovered: list[dict[str, object]] = []
    recovery_errors: list[str] = []
    for handler in ("live", "wrapper"):
        path = failure_path(migration_run, invocation_id, handler)
        try:
            if not files.exists(path):
                continue
            content = files.read_bytes(path)
            value = _mapping(json.loads(content), f"{handler} failure diagnostic")
            exception = _mapping(
                value.get("exception"), f"{handler} failure exception"
            )
            recovered.append(
                {
                    "exception_message": redact(exception.get("message"), hidden),
                    "exception_type": exception.get("type"),
                    "path": path,
                    "sha256": _sha256(content),
                    "stage": value.get("stage"),
                }
            )
        except Exception as diagnostic_error:
            recovery_errors.append(
                f"{handler}:{type(diagnostic_error).__name__}:"
                f"{redact(diagnostic_error, hidden)}"
            )
    return {
        "diagnostic_errors": recovery_errors,
        "diagnostics": recovered,
        "job_instance_id": instance_id,
    }


def _writer_invocation_arguments(
    *,
    command: str,
    owner: str | None,
    lease_token: str | None,
    plan_sha256: str | None,
    safety_token: str | None,
) -> list[str]:
    if not owner or not lease_token:
        raise ControllerError(
            f"{command} invocation requires owner and lease token"
        )
    arguments = [
        "--owner",
        _safe_id(owner, "owner"),
        "--lease-token",
        lease_token,
    ]
    if command != "apply":
        return arguments
    if not plan_sha256 or not safety_token:
        raise ControllerError("apply invocation requires plan hash and safety token")
    return [
        *arguments,
        "--execute",
        "--plan-sha256",
        plan_sha256,
        "--safety-token",
        safety_token,
    ]


def _recovery_invocation_arguments(
    *,
    failed_job_id: str | None,
    failed_invocation_id: str | None,
    recovery_token: str | None,
    recovery_execute: bool,
) -> list[str]:
    if (
        failed_job_id != FAILED_RECOVERY_JOB_ID
        or failed_invocation_id != FAILED_RECOVERY_INVOCATION_ID
    ):
        raise ControllerError(
            "recover-lock invocation is not the exact reviewed failure"
        )
    arguments = [
        "--failed-job-id",
        failed_job_id,
        "--failed-invocation-id",
        failed_invocation_id,
    ]
    if not recovery_execute:
        return arguments
    if not recovery_token:
        raise ControllerError(
            "recover-lock execution requires the deterministic token"
        )
    return [
        *arguments,
        "--recovery-execute",
        "--recovery-token",
        recovery_token,
    ]


def _invocation_arguments(
    inventory: SignedInventory,
    *,
    command: str,
    migration_run: str,
    invocation_id: str,
    owner: str | None,
    lease_token: str | None,
    plan_sha256: str | None,
    safety_token: str | None,
    failed_job_id: str | None,
    failed_invocation_id: str | None,
    recovery_token: str | None,
    recovery_execute: bool,
) -> list[str]:
    if command not in {
        "diagnose",
        "plan",
        "apply",
        "verify",
        "status",
        "recover-lock",
        "review-recovery",
        "recover-lock",
        "review-recovery",
        "recover-lock",
    }:
        raise ControllerError("unsupported migration SJD command")
    arguments = [
        command,
        "--run-id",
        migration_run,
        "--inventory-run-id",
        inventory.run_id,
        "--inventory-hmac-key",
        inventory.hmac_key.hex(),
        "--invocation-id",
        invocation_id,
    ]
    if command == "recover-lock":
        arguments.extend(
            _recovery_invocation_arguments(
                failed_job_id=failed_job_id,
                failed_invocation_id=failed_invocation_id,
                recovery_token=recovery_token,
                recovery_execute=recovery_execute,
            )
        )
    elif command != "diagnose":
        arguments.extend(
            _writer_invocation_arguments(
                command=command,
                owner=owner,
                lease_token=lease_token,
                plan_sha256=plan_sha256,
                safety_token=safety_token,
            )
        )
    return arguments


def _invocation_evidence_path(
    *,
    command: str,
    migration_run: str,
    invocation_id: str,
    plan_sha256: str | None,
    failed_job_id: str | None,
    failed_invocation_id: str | None,
    recovery_execute: bool,
) -> str:
    if command == "apply" and plan_sha256 is not None:
        return result_path(migration_run, plan_sha256)
    if command == "recover-lock":
        if failed_job_id is None or failed_invocation_id is None:
            raise ControllerError("recover-lock evidence identity is missing")
        return (
            recovery_result_path(failed_job_id, failed_invocation_id)
            if recovery_execute
            else recovery_plan_path(failed_job_id, failed_invocation_id)
        )
    if command == "diagnose":
        return diagnose_path(migration_run, invocation_id)
    return report_path(migration_run, invocation_id, command)


def _read_invocation_evidence(
    files: OneLakeFiles,
    path: str,
    *,
    command: str,
) -> Mapping[str, Any]:
    try:
        evidence = json.loads(files.read_bytes(path))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ControllerError("migration SJD result evidence is invalid JSON") from error
    if not isinstance(evidence, Mapping):
        raise ControllerError("migration SJD result evidence must be an object")
    if command == "diagnose" and evidence.get("status") != "passed":
        failed = [
            str(check.get("name"))
            for check in _object_list(evidence.get("checks"), "diagnostic checks")
            if not check.get("passed")
        ]
        raise ControllerError(
            "migration diagnose checks failed: " + ",".join(failed)
        )
    return evidence


def invoke(
    api: FabricControllerAPI,
    files: OneLakeFiles,
    inventory: SignedInventory,
    *,
    command: str,
    migration_run_id: str | None = None,
    owner: str | None = None,
    lease_token: str | None = None,
    plan_sha256: str | None = None,
    safety_token: str | None = None,
    failed_job_id: str | None = None,
    failed_invocation_id: str | None = None,
    recovery_token: str | None = None,
    recovery_execute: bool = False,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
) -> InvocationResult:
    item_id = _migration_sjd_id(api)
    migration_run = (
        inventory.run_id
        if migration_run_id is None
        else _safe_id(migration_run_id, "migration run ID")
    )
    invocation_id = str(uuid.uuid4())
    arguments = _invocation_arguments(
        inventory,
        command=command,
        migration_run=migration_run,
        invocation_id=invocation_id,
        owner=owner,
        lease_token=lease_token,
        plan_sha256=plan_sha256,
        safety_token=safety_token,
        failed_job_id=failed_job_id,
        failed_invocation_id=failed_invocation_id,
        recovery_token=recovery_token,
        recovery_execute=recovery_execute,
    )
    # shlex.join performs data quoting only; no shell evaluates this string.
    instance_id = api.run_sjd(item_id, shlex.join(arguments))
    hidden = tuple(
        value
        for value in (
            inventory.hmac_key.hex(),
            lease_token,
            safety_token,
            recovery_token,
        )
        if isinstance(value, str) and value
    )
    try:
        _poll_job(
            api,
            item_id,
            instance_id,
            clock=clock,
            sleep=sleep,
            timeout=SJD_JOB_TIMEOUT_SECONDS,
            hidden=hidden,
        )
    except ControllerError as job_error:
        detail = _job_failure_detail(
            files,
            migration_run=migration_run,
            invocation_id=invocation_id,
            instance_id=instance_id,
            hidden=hidden,
        )
        raise ControllerError(
            f"{redact(job_error, hidden)}; diagnostic={canonical_json(detail)}"
        ) from job_error
    evidence_path = _invocation_evidence_path(
        command=command,
        migration_run=migration_run,
        invocation_id=invocation_id,
        plan_sha256=plan_sha256,
        failed_job_id=failed_job_id,
        failed_invocation_id=failed_invocation_id,
        recovery_execute=recovery_execute,
    )
    evidence = _read_invocation_evidence(
        files, evidence_path, command=command
    )
    return InvocationResult(command, migration_run, instance_id, evidence)


def _local_path(kind: str, run_id: str) -> Path:
    safe = _safe_id(run_id, "run ID")
    return LOCAL_STATE_ROOT / kind / f"{safe}.json"


def _write_local_create_only(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = _canonical_bytes(value) + b"\n"
    if path.exists():
        if path.is_symlink() or path.read_bytes() != content:
            raise FileExistsError(path)
        return
    path.write_bytes(content)
    os.chmod(path, 0o600)


def _read_local(kind: str, run_id: str) -> Mapping[str, Any]:
    path = _local_path(kind, run_id)
    if not path.is_file() or path.is_symlink():
        raise ControllerError(f"reviewed local {kind} artifact is missing")
    try:
        value = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ControllerError(f"local {kind} artifact is invalid JSON") from error
    return _mapping(value, f"local {kind} artifact")


def store_plan(
    inventory: SignedInventory, evidence: Mapping[str, Any]
) -> Mapping[str, Any]:
    plan = evidence.get("plan", evidence)
    plan = _mapping(plan, "plan evidence")
    plan_sha = plan.get("plan_sha256")
    safety_token_sha = plan.get("safety_token_sha256")
    if not isinstance(plan_sha, str) or _HEX64.fullmatch(plan_sha) is None:
        raise ControllerError("plan evidence has no exact SHA-256")
    if not isinstance(safety_token_sha, str) or _HEX64.fullmatch(safety_token_sha) is None:
        raise ControllerError("plan evidence has no redacted safety-token hash")
    artifact = {
        "inventory_payload": inventory.payload,
        "inventory_payload_sha256": inventory.payload_sha256,
        "plan": dict(plan),
        "plan_artifact_sha256": _sha256(_canonical_bytes(plan)),
        "plan_sha256": plan_sha,
        "run_id": inventory.run_id,
        "safety_token_sha256": safety_token_sha,
    }
    _write_local_create_only(_local_path("plans", inventory.run_id), artifact)
    return artifact


def _review_summary(
    artifact: Mapping[str, Any], run_id: str
) -> dict[str, object]:
    plan = _mapping(artifact.get("plan"), "local plan")
    return {
        "compatibility": plan.get("compatibility"),
        "inventory_payload_sha256": artifact.get("inventory_payload_sha256"),
        "operation_count": len(_object_list(plan.get("operations"), "plan operations")),
        "plan_artifact_sha256": artifact.get("plan_artifact_sha256"),
        "plan_sha256": artifact.get("plan_sha256"),
        "rollback_manifest": plan.get("rollback_manifest"),
        "run_id": run_id,
        "safety_token": "<redacted>",
    }


def review_plan(
    run_id: str,
    *,
    now: float | None = None,
) -> Mapping[str, Any]:
    reviewed_at = time.time() if now is None else float(now)
    artifact = _read_local("plans", run_id)
    if artifact.get("run_id") != run_id:
        raise ControllerError("local plan run ID mismatch")
    token_sha = str(artifact.get("safety_token_sha256"))
    if _HEX64.fullmatch(token_sha) is None:
        raise ControllerError("local plan safety-token hash is invalid")
    summary = _review_summary(artifact, run_id)
    receipt_body = {
        "expires_at": reviewed_at + REVIEW_MAX_AGE_SECONDS,
        "reviewed_at": reviewed_at,
        "safety_token_sha256": token_sha,
        "summary": summary,
    }
    receipt = {
        **receipt_body,
        "receipt_sha256": _sha256(_canonical_bytes(receipt_body)),
    }
    _write_local_create_only(_local_path("reviews", run_id), receipt)
    return receipt


def store_recovery_plan(evidence: Mapping[str, Any]) -> Mapping[str, Any]:
    recovery_evidence = _mapping(
        evidence.get("evidence"), "recovery plan evidence"
    )
    if (
        evidence.get("schema") != "people-counter-production-lock-recovery-v2"
        or recovery_evidence.get("failed_job_id") != FAILED_RECOVERY_JOB_ID
        or recovery_evidence.get("failed_invocation_id")
        != FAILED_RECOVERY_INVOCATION_ID
    ):
        raise ControllerError("recovery plan does not bind the reviewed failure")
    plan_sha = evidence.get("plan_sha256")
    token = evidence.get("recovery_token")
    if (
        not isinstance(plan_sha, str)
        or _HEX64.fullmatch(plan_sha) is None
        or not isinstance(token, str)
        or not token.startswith("recover-v1:")
    ):
        raise ControllerError("recovery plan identity is invalid")
    body = {key: value for key, value in evidence.items() if key != "plan_sha256"}
    if plan_sha != _sha256(_canonical_bytes(body)):
        raise ControllerError("recovery plan integrity mismatch")
    artifact = {
        "failed_invocation_id": FAILED_RECOVERY_INVOCATION_ID,
        "failed_job_id": FAILED_RECOVERY_JOB_ID,
        "plan": dict(evidence),
        "plan_sha256": plan_sha,
        "recovery_token_sha256": _sha256(token.encode()),
        "run_id": FAILED_RECOVERY_RUN_ID,
    }
    _write_local_create_only(
        _local_path("recovery-plans-v2", FAILED_RECOVERY_JOB_ID), artifact
    )
    return artifact


def review_recovery(
    files: OneLakeFiles,
    *,
    now: float | None = None,
) -> Mapping[str, Any]:
    reviewed_at = time.time() if now is None else float(now)
    artifact = _read_local("recovery-plans-v2", FAILED_RECOVERY_JOB_ID)
    plan = _mapping(artifact.get("plan"), "recovery plan")
    if (
        artifact.get("failed_job_id") != FAILED_RECOVERY_JOB_ID
        or artifact.get("failed_invocation_id") != FAILED_RECOVERY_INVOCATION_ID
        or artifact.get("plan_sha256") != plan.get("plan_sha256")
    ):
        raise ControllerError("local recovery plan binding mismatch")
    body = {
        "expires_at": reviewed_at + RECOVERY_REVIEW_MAX_AGE_SECONDS,
        "failed_invocation_id": FAILED_RECOVERY_INVOCATION_ID,
        "failed_job_id": FAILED_RECOVERY_JOB_ID,
        "plan_sha256": plan["plan_sha256"],
        "recovery_token_sha256": artifact["recovery_token_sha256"],
        "reviewed_at": reviewed_at,
        "schema": "people-counter-production-lock-recovery-v2",
    }
    receipt = {**body, "receipt_sha256": _sha256(_canonical_bytes(body))}
    _write_local_create_only(
        _local_path("recovery-reviews-v2", FAILED_RECOVERY_JOB_ID), receipt
    )
    path = recovery_review_path(
        FAILED_RECOVERY_JOB_ID, FAILED_RECOVERY_INVOCATION_ID
    )
    content = _canonical_bytes(receipt) + b"\n"
    if files.exists(path):
        if files.read_bytes(path) != content:
            raise FileExistsError(path)
    else:
        files.create_bytes(path, content)
    if files.read_bytes(path) != content:
        raise ControllerError("recovery review OneLake readback differs")
    return {**receipt, "path": path}


def validate_recovery_execution(
    recovery_token: str,
    *,
    now: float,
) -> Mapping[str, Any]:
    artifact = _read_local("recovery-plans-v2", FAILED_RECOVERY_JOB_ID)
    receipt = _read_local("recovery-reviews-v2", FAILED_RECOVERY_JOB_ID)
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    expiry = receipt.get("expires_at")
    if isinstance(expiry, bool) or not isinstance(expiry, (int, float)):
        raise ControllerError(
            "recovery execution token or review receipt binding is invalid"
        )
    if (
        receipt.get("receipt_sha256") != _sha256(_canonical_bytes(body))
        or float(expiry) < now
        or receipt.get("plan_sha256") != artifact.get("plan_sha256")
        or receipt.get("recovery_token_sha256")
        != _sha256(recovery_token.encode())
        or artifact.get("recovery_token_sha256")
        != _sha256(recovery_token.encode())
    ):
        raise ControllerError(
            "recovery execution token or review receipt binding is invalid"
        )
    return artifact


def _state_binding(payload: Mapping[str, Any]) -> str:
    """Bind material state while excluding capture-only inventory metadata."""

    workspace = _mapping(payload.get("workspace_items"), "workspace items")
    material = {
        "artifact_binding": payload.get("artifact_binding"),
        "environment": payload.get("environment"),
        "fabric_jobs": payload.get("fabric_jobs"),
        "item_definitions": payload.get("item_definitions"),
        "migration_sjd": payload.get("migration_sjd"),
        "reflex": payload.get("reflex"),
        "reflex_definition_base64": payload.get("reflex_definition_base64"),
        "reflex_id": payload.get("reflex_id"),
        "schema": payload.get("schema"),
        "workspace_items": workspace.get("items"),
        "writer_schedules": payload.get("writer_schedules"),
    }
    return _sha256(_canonical_bytes(material))


def validate_apply_review(
    run_id: str,
    *,
    plan_sha256: str,
    safety_token: str,
    fresh_snapshot: Mapping[str, Any],
    now: float | None = None,
) -> Mapping[str, Any]:
    current = time.time() if now is None else float(now)
    artifact = _read_local("plans", run_id)
    receipt = _read_local("reviews", run_id)
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    if not hmac.compare_digest(
        str(receipt.get("receipt_sha256")), _sha256(_canonical_bytes(body))
    ):
        raise ControllerError("review receipt integrity mismatch")
    if current > float(receipt.get("expires_at", -1)):
        raise ControllerError("review receipt has expired")
    if receipt.get("summary") != _review_summary(artifact, run_id):
        raise ControllerError("review receipt does not bind the exact plan artifact")
    if receipt.get("safety_token_sha256") != artifact.get("safety_token_sha256"):
        raise ControllerError("review receipt does not bind the plan safety token")
    if artifact.get("plan_sha256") != plan_sha256:
        raise ControllerError("apply plan SHA does not match reviewed plan")
    token_sha = _sha256(safety_token.encode())
    if not hmac.compare_digest(str(receipt.get("safety_token_sha256")), token_sha):
        raise ControllerError("apply safety token does not match review receipt")
    plan = _mapping(artifact.get("plan"), "reviewed plan")
    if artifact.get("plan_artifact_sha256") != _sha256(_canonical_bytes(plan)):
        raise ControllerError("reviewed plan artifact integrity mismatch")
    if plan.get("plan_sha256") != artifact.get("plan_sha256"):
        raise ControllerError("reviewed plan SHA binding mismatch")
    inventory_payload = _mapping(
        artifact.get("inventory_payload"), "reviewed inventory payload"
    )
    if artifact.get("inventory_payload_sha256") != _sha256(
        _canonical_bytes(inventory_payload)
    ):
        raise ControllerError("reviewed inventory payload integrity mismatch")
    fresh_captured_at = float(fresh_snapshot.get("captured_at", -1))
    if (
        current - fresh_captured_at > INVENTORY_APPLY_MAX_AGE_SECONDS
        or fresh_captured_at > current
    ):
        raise ControllerError("apply inventory is not fresh enough")
    if _state_binding(inventory_payload) != _state_binding(fresh_snapshot):
        raise ControllerError("fresh snapshot differs from the reviewed inventory")
    return artifact


def redact(value: object, secrets_to_hide: Sequence[str]) -> str:
    text = str(value)
    for secret in secrets_to_hide:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


class FabricRESTController:
    """Minimal fixed-scope Fabric REST implementation."""

    def __init__(
        self,
        token_provider: TokenProvider,
        *,
        transport: HTTPTransport | None = None,
        api_root: str = FABRIC_API_ROOT,
        timeout: float = 30.0,
        lro_timeout: float = 1800.0,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not api_root.startswith("https://"):
            raise ValueError("Fabric API root must use HTTPS")
        self.token_provider = token_provider
        self.transport = transport or UrllibTransport()
        self.api_root = api_root.rstrip("/")
        self.timeout = timeout
        self.lro_timeout = lro_timeout
        self.sleep = sleep
        self.monotonic = monotonic

    def _url(self, path_or_url: str) -> str:
        if path_or_url.startswith("https://"):
            return path_or_url
        return f"{self.api_root}/{path_or_url.lstrip('/')}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        value: object | bytes | None = None,
        expected: Sequence[int] = (200,),
        content_type: str = "application/json",
    ) -> tuple[Mapping[str, str], bytes]:
        token = self.token_provider.get_token()
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        body: bytes | None = None
        if value is not None:
            headers["Content-Type"] = content_type
            body = value if isinstance(value, bytes) else _canonical_bytes(value)
        response = self.transport.request(
            method,
            self._url(path),
            headers=headers,
            body=body,
            timeout=self.timeout,
        )
        if response.status not in expected:
            detail = redact(response.body.decode(errors="replace")[:2000], (token,))
            raise ControllerError(f"Fabric HTTP {response.status}: {detail}")
        return response.headers, response.body

    def _json(
        self,
        method: str,
        path: str,
        *,
        value: object | bytes | None = None,
        expected: Sequence[int] = (200,),
        content_type: str = "application/json",
    ) -> Mapping[str, Any]:
        _, body = self._request(
            method,
            path,
            value=value,
            expected=expected,
            content_type=content_type,
        )
        if not body:
            return {}
        try:
            result = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ControllerError("Fabric returned invalid JSON") from error
        return _mapping(result, "Fabric response")

    @staticmethod
    def _header(headers: Mapping[str, str], name: str) -> str | None:
        lowered = name.lower()
        return next(
            (str(value) for key, value in headers.items() if key.lower() == lowered),
            None,
        )

    @classmethod
    def _retry_after(cls, headers: Mapping[str, str]) -> float:
        value = cls._header(headers, "Retry-After")
        if value is None:
            return 1.0
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                parsed = email.utils.parsedate_to_datetime(value)
                return max(0.0, parsed.timestamp() - time.time())
            except (TypeError, ValueError, OverflowError):
                return 1.0

    @staticmethod
    def _response_mapping(body: bytes, label: str) -> Mapping[str, Any]:
        if not body:
            return {}
        try:
            value = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ControllerError(f"{label} is invalid JSON") from error
        if value is None:
            return {}
        return _mapping(value, label)

    @classmethod
    def _operation_location(cls, headers: Mapping[str, str]) -> str | None:
        location = cls._header(headers, "Location")
        operation_id = cls._header(headers, "x-ms-operation-id")
        if not location and operation_id:
            return f"/operations/{operation_id}"
        return location

    def _poll_operation(
        self,
        location: str,
        headers: Mapping[str, str],
    ) -> Mapping[str, Any]:
        deadline = self.monotonic() + self.lro_timeout
        current_headers = headers
        while True:
            if self.monotonic() >= deadline:
                raise TimeoutError("timed out waiting for Fabric operation")
            self.sleep(self._retry_after(current_headers))
            current_headers, current_body = self._request(
                "GET", location, expected=(200, 202, 429)
            )
            result = self._response_mapping(
                current_body, "Fabric operation response"
            )
            status = str(result.get("status", "")).lower()
            if status in _TERMINAL_FAILURE:
                raise ControllerError(f"Fabric operation failed: {result!r}")
            if status in _TERMINAL_SUCCESS or (
                status == "" and current_body and result
            ):
                resource = result.get("resourceLocation") or result.get(
                    "resultLocation"
                )
                if not isinstance(resource, str):
                    response_location = self._header(current_headers, "Location")
                    if response_location and response_location.rstrip("/") != location.rstrip(
                        "/"
                    ):
                        resource = response_location
                if isinstance(resource, str):
                    return self._json("GET", resource)
                return result

    def _lro_json(
        self,
        method: str,
        path: str,
        *,
        value: object | bytes | None = None,
        accepted: Sequence[int] = (200, 201, 202),
        content_type: str = "application/json",
    ) -> Mapping[str, Any]:
        headers, body = self._request(
            method,
            path,
            value=value,
            expected=accepted,
            content_type=content_type,
        )
        initial = self._response_mapping(body, "Fabric response")
        location = self._operation_location(headers)
        if not location:
            return initial
        return self._poll_operation(location, headers)

    def _list(self, path: str) -> list[Mapping[str, Any]]:
        result: list[Mapping[str, Any]] = []
        next_path: str | None = path
        while next_path is not None:
            page = self._json("GET", next_path)
            result.extend(_object_list(page.get("value"), "Fabric list value"))
            continuation = page.get("continuationUri")
            if continuation is not None and not isinstance(continuation, str):
                raise ControllerError("Fabric continuation URI is ambiguous")
            next_path = continuation or None
        return result

    def list_items(self) -> list[Mapping[str, Any]]:
        return self._list(f"/workspaces/{WORKSPACE_ID}/items")

    def get_definition(self, item_type: str, item_id: str) -> Mapping[str, Any]:
        roots = {
            "DataPipeline": "dataPipelines",
            "Reflex": "reflexes",
            "SparkJobDefinition": "sparkJobDefinitions",
        }
        try:
            root = roots[item_type]
        except KeyError as error:
            raise ControllerError("unsupported fixed definition type") from error
        format_query = (
            "?format=SparkJobDefinitionV2"
            if item_type == "SparkJobDefinition"
            else ""
        )
        return self._lro_json(
            "POST",
            f"/workspaces/{WORKSPACE_ID}/{root}/{item_id}/getDefinition"
            + format_query,
            accepted=(200, 202),
        )

    def list_schedules(
        self, item_id: str, job_type: str
    ) -> list[Mapping[str, Any]]:
        return self._list(
            f"/workspaces/{WORKSPACE_ID}/items/{item_id}/jobs/{job_type}/schedules"
        )

    def list_job_instances(self, item_id: str) -> list[Mapping[str, Any]]:
        return self._list(
            f"/workspaces/{WORKSPACE_ID}/items/{item_id}/jobs/instances"
        )

    def environment_state(self) -> Mapping[str, Any]:
        root = f"/workspaces/{WORKSPACE_ID}/environments/{ENVIRONMENT_ID}"
        return {
            "item": self._json("GET", root),
            "published_compute": self._json("GET", root + "/sparkcompute?beta=false"),
            "published_libraries": self._json("GET", root + "/libraries?beta=false"),
            "staged_compute": self._json("GET", root + "/sparkcompute?beta=true"),
            "staged_libraries": self._json("GET", root + "/libraries?beta=true"),
        }

    def upload_environment_wheel(self, name: str, content: bytes) -> None:
        self._request(
            "POST",
            f"/workspaces/{WORKSPACE_ID}/environments/{ENVIRONMENT_ID}"
            f"/staging/libraries/{name}",
            value=content,
            expected=(200, 201, 202),
            content_type="application/octet-stream",
        )

    def delete_staged_environment_library(self, name: str) -> None:
        self._request(
            "DELETE",
            f"/workspaces/{WORKSPACE_ID}/environments/{ENVIRONMENT_ID}"
            f"/staging/libraries/{name}",
            expected=(200, 202, 204),
        )

    def publish_environment(self) -> Mapping[str, Any]:
        return self._lro_json(
            "POST",
            f"/workspaces/{WORKSPACE_ID}/environments/{ENVIRONMENT_ID}"
            "/staging/publish?beta=false",
            accepted=(200, 202),
        )

    def create_sjd(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        body = {
            **payload,
            "displayName": MIGRATION_SJD_NAME,
            "description": "Reviewed people-counter production migration gate",
        }
        return self._lro_json(
            "POST",
            f"/workspaces/{WORKSPACE_ID}/sparkJobDefinitions",
            value=body,
            accepted=(201, 202),
        )

    def update_sjd(
        self, item_id: str, payload: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        return self._lro_json(
            "POST",
            f"/workspaces/{WORKSPACE_ID}/sparkJobDefinitions/{item_id}/updateDefinition",
            value=payload,
            accepted=(200, 202),
        )

    def run_sjd(self, item_id: str, arguments: str) -> str:
        headers, body = self._request(
            "POST",
            f"/workspaces/{WORKSPACE_ID}/sparkJobDefinitions/{item_id}"
            "/jobs/sparkjob/instances",
            value={"executionData": {"commandLineArguments": arguments}},
            expected=(202,),
        )
        value = json.loads(body) if body else {}
        instance = value.get("id") or value.get("jobInstanceId")
        if not isinstance(instance, str):
            location = next(
                (
                    item
                    for key, item in headers.items()
                    if key.lower() == "location"
                ),
                "",
            )
            instance = str(location).rstrip("/").split("/")[-1]
        if not instance:
            raise ControllerError("Fabric SJD invocation returned no job instance ID")
        return instance

    def get_job_instance(
        self, item_id: str, job_instance_id: str
    ) -> Mapping[str, Any]:
        return self._json(
            "GET",
            f"/workspaces/{WORKSPACE_ID}/items/{item_id}"
            f"/jobs/instances/{job_instance_id}",
        )


class AzureOneLakeFiles:
    """Create-only ADLS adapter constrained to the fixed migration root."""

    def __init__(self) -> None:
        try:
            from azure.identity import AzureCliCredential
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError as error:
            raise ControllerError(
                "publisher dependencies are required for OneLake access"
            ) from error
        service = DataLakeServiceClient(
            "https://onelake.dfs.fabric.microsoft.com",
            credential=AzureCliCredential(),
        )
        self._filesystem = service.get_file_system_client(WORKSPACE_ID)

    @staticmethod
    def _path(path: str) -> str:
        prefix = f"Files/people-counter/migrations/{MIGRATION_ID}/"
        candidate = PurePosixPath(path)
        if (
            "\\" in path
            or "\x00" in path
            or candidate.is_absolute()
            or str(candidate) != path
            or not path.startswith(prefix)
            or ".." in candidate.parts
        ):
            raise ControllerError("OneLake path is outside the fixed migration root")
        return f"{LAKEHOUSE_ID}/{path}"

    def exists(self, path: str) -> bool:
        client = self._filesystem.get_file_client(self._path(path))
        try:
            client.get_file_properties()
            return True
        except Exception as error:
            if getattr(error, "status_code", None) == 404:
                return False
            raise

    def read_bytes(self, path: str) -> bytes:
        return self._filesystem.get_file_client(self._path(path)).download_file().readall()

    def create_bytes(self, path: str, content: bytes) -> None:
        checked = self._path(path)
        parent = checked.rsplit("/", 1)[0]
        segments = parent.split("/")
        if segments[:2] != [LAKEHOUSE_ID, "Files"]:
            raise ControllerError("OneLake create parent is outside Lakehouse Files")
        current = f"{LAKEHOUSE_ID}/Files"
        for segment in segments[2:]:
            current = f"{current}/{segment}"
            directory = self._filesystem.get_directory_client(current)
            try:
                directory.create_directory()
            except Exception as error:
                if getattr(error, "status_code", None) != 409:
                    raise
        client = self._filesystem.get_file_client(checked)
        client.create_file(if_none_match="*")
        if content:
            client.append_data(content, offset=0, length=len(content))
        client.flush_data(len(content))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pc-production-migration-controller")
    parser.add_argument(
        "command",
        choices=(
            "snapshot",
            "deploy",
            "run",
            "diagnose",
            "plan",
            "review",
            "apply",
            "verify",
            "status",
            "recover-lock",
            "review-recovery",
        ),
        nargs="?",
        default="snapshot",
    )
    parser.add_argument("--run-id")
    parser.add_argument("--owner")
    parser.add_argument("--lease-token")
    parser.add_argument("--plan-sha256")
    parser.add_argument("--safety-token")
    parser.add_argument("--failed-job-id")
    parser.add_argument("--failed-invocation-id")
    parser.add_argument("--recovery-token")
    parser.add_argument("--recovery-execute", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--auth",
        choices=("azure-cli", "default", "managed-identity", "service-principal"),
        default="azure-cli",
    )
    return parser


def _required(args: argparse.Namespace, *names: str) -> list[str]:
    values: list[str] = []
    for name in names:
        value = getattr(args, name)
        if not isinstance(value, str) or not value:
            raise ControllerError("--" + name.replace("_", "-") + " is required")
        values.append(value)
    return values


def _new_inventory(
    api: FabricControllerAPI,
    run_id: str,
    *,
    now: float,
) -> SignedInventory:
    return sign_inventory(capture_snapshot(api, now=now), run_id)


def _print(value: object, output: TextIO) -> None:
    print(canonical_json(value), file=output)


def _review_recovery_command(
    args: argparse.Namespace,
    *,
    files: OneLakeFiles | None,
    now: float,
) -> Mapping[str, Any]:
    failed_job, failed_invocation = _required(
        args, "failed_job_id", "failed_invocation_id"
    )
    if (
        failed_job != FAILED_RECOVERY_JOB_ID
        or failed_invocation != FAILED_RECOVERY_INVOCATION_ID
    ):
        raise ControllerError(
            "review-recovery is not the exact reviewed failure"
        )
    return review_recovery(files or AzureOneLakeFiles(), now=now)


def _recover_controller_command(
    args: argparse.Namespace,
    fabric: FabricControllerAPI,
    *,
    files: OneLakeFiles | None,
    clock: Callable[[], float],
) -> Mapping[str, Any]:
    run_id = _required(args, "run_id")[0]
    failed_job, failed_invocation = _required(
        args, "failed_job_id", "failed_invocation_id"
    )
    if (
        run_id != FAILED_RECOVERY_RUN_ID
        or failed_job != FAILED_RECOVERY_JOB_ID
        or failed_invocation != FAILED_RECOVERY_INVOCATION_ID
    ):
        raise ControllerError("recover-lock is not the exact reviewed failure")
    lake = files or AzureOneLakeFiles()
    now = float(clock())
    inventory = sign_inventory(
        capture_recovery_snapshot(fabric, now=now),
        f"{run_id}.recover.{uuid.uuid4().hex}",
    )
    upload_inventory(lake, inventory)
    recovery_token = (
        _required(args, "recovery_token")[0]
        if args.recovery_execute
        else None
    )
    if recovery_token is not None:
        validate_recovery_execution(recovery_token, now=now)
    result = invoke(
        fabric,
        lake,
        inventory,
        command="recover-lock",
        migration_run_id=run_id,
        failed_job_id=failed_job,
        failed_invocation_id=failed_invocation,
        recovery_token=recovery_token,
        recovery_execute=args.recovery_execute,
        clock=clock,
    )
    response: dict[str, object] = {
        "command": "recover-lock",
        "execute": args.recovery_execute,
        "inventory": inventory.redacted_summary(),
        "job_instance_id": result.job_instance_id,
        "result": result.evidence,
    }
    if not args.recovery_execute:
        response["local_recovery_plan"] = store_recovery_plan(result.evidence)
    return response


def _apply_controller_command(
    args: argparse.Namespace,
    fabric: FabricControllerAPI,
    lake: OneLakeFiles,
    snapshot: Mapping[str, Any],
    run_id: str,
    *,
    clock: Callable[[], float],
) -> Mapping[str, Any]:
    owner, lease_token = _required(args, "owner", "lease_token")
    plan_sha, safety_token = _required(
        args, "plan_sha256", "safety_token"
    )
    artifact = validate_apply_review(
        run_id,
        plan_sha256=plan_sha,
        safety_token=safety_token,
        fresh_snapshot=snapshot,
        now=clock(),
    )
    inventory = sign_inventory(
        snapshot, f"{run_id}.apply.{uuid.uuid4().hex}"
    )
    upload_inventory(lake, inventory)
    result = invoke(
        fabric,
        lake,
        inventory,
        command="apply",
        migration_run_id=run_id,
        owner=owner,
        lease_token=lease_token,
        plan_sha256=plan_sha,
        safety_token=safety_token,
        clock=clock,
    )
    return {
        "command": "apply",
        "inventory": inventory.redacted_summary(),
        "job_instance_id": result.job_instance_id,
        "plan_sha256": artifact["plan_sha256"],
        "result": result.evidence,
    }


def _invoke_controller_command(
    args: argparse.Namespace,
    fabric: FabricControllerAPI,
    lake: OneLakeFiles,
    snapshot: Mapping[str, Any],
    run_id: str,
    *,
    clock: Callable[[], float],
) -> Mapping[str, Any]:
    inventory = sign_inventory(snapshot, run_id)
    upload_inventory(lake, inventory)
    command = "plan" if args.command == "run" else args.command
    owner: str | None = None
    lease_token: str | None = None
    if command != "diagnose":
        owner, lease_token = _required(args, "owner", "lease_token")
    result = invoke(
        fabric,
        lake,
        inventory,
        command=command,
        owner=owner,
        lease_token=lease_token,
        clock=clock,
    )
    response: dict[str, object] = {
        "command": command,
        "inventory": inventory.redacted_summary(),
        "job_instance_id": result.job_instance_id,
        "result": result.evidence,
    }
    if command == "plan":
        response["local_plan"] = store_plan(inventory, result.evidence)
    return response


def _standard_controller_command(
    args: argparse.Namespace,
    fabric: FabricControllerAPI,
    *,
    files: OneLakeFiles | None,
    clock: Callable[[], float],
) -> Mapping[str, Any]:
    run_id = _required(args, "run_id")[0]
    snapshot = capture_snapshot(fabric, now=clock())
    if args.command == "snapshot" or not args.execute:
        return {
            "execute": False,
            "payload_sha256": _sha256(_canonical_bytes(snapshot)),
            "reflex": snapshot["reflex"],
            "run_id": run_id,
            "status": "read_only",
        }
    lake = files or AzureOneLakeFiles()
    if args.command == "apply":
        return _apply_controller_command(
            args, fabric, lake, snapshot, run_id, clock=clock
        )
    return _invoke_controller_command(
        args, fabric, lake, snapshot, run_id, clock=clock
    )


def _run_controller_command(
    args: argparse.Namespace,
    fabric: FabricControllerAPI,
    *,
    files: OneLakeFiles | None,
    clock: Callable[[], float],
) -> object:
    if args.command == "deploy":
        return deploy(fabric, execute=args.execute)
    if args.command == "review":
        return review_plan(_required(args, "run_id")[0], now=clock())
    if args.command == "review-recovery":
        return _review_recovery_command(args, files=files, now=clock())
    if args.command == "recover-lock":
        return _recover_controller_command(
            args, fabric, files=files, clock=clock
        )
    return _standard_controller_command(
        args, fabric, files=files, clock=clock
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    api: FabricControllerAPI | None = None,
    files: OneLakeFiles | None = None,
    clock: Callable[[], float] = time.time,
    output: TextIO = sys.stdout,
    errors: TextIO = sys.stderr,
) -> int:
    args = _build_parser().parse_args(argv)
    try:
        fabric = api or FabricRESTController(make_token_provider(args.auth))
        _print(
            _run_controller_command(
                args, fabric, files=files, clock=clock
            ),
            output,
        )
        return 0
    except (ControllerError, FileExistsError, OSError, ValueError) as error:
        hidden = [
            value
            for value in (
                args.safety_token,
                args.lease_token,
                args.recovery_token,
            )
            if isinstance(value, str)
        ]
        print(redact(f"refused: {error}", hidden), file=errors)
        return 2
