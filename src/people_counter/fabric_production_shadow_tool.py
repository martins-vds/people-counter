"""Offline host controller for the fixed Candidate A production shadow.

The controller owns policy and evidence, not connectivity.  Callers inject a
fixed-scope backend and create-only evidence/local stores.  No default backend
constructs a Fabric client, and the command line accepts no resource, path,
table, namespace, SQL, or code arguments.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import secrets
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol, TextIO

from people_counter.fabric_candidate_a import (
    ENVIRONMENT_ID,
    FabricCandidateAConfig,
    LAKEHOUSE_ID,
    PRODUCTION_SHADOW_FILES_ROOT,
    WORKSPACE_ID,
)
from people_counter.fabric_production_migration import MIGRATION_ID
from people_counter.fabric_production_routing import AllowlistRow, canonical_bytes
from people_counter.fabric_production_shadow import (
    AUTHORIZATION_MAX_AGE,
    AuthorizationResult,
    CommittedRoute,
    LegacySourceRows,
    LegacySourceSnapshot,
    MigrationJournalProof,
    ProductionShadowError,
    ProductionShadowPlan,
    ReviewReceipt,
    RouteTolerance,
    RuntimeProvenance,
    ShadowQuiescence,
    SyntheticShadowPlan,
    authorize_production_shadow,
    bootstrap_production_shadow,
    compare_and_reconcile,
    execute_production_shadow,
    execute_synthetic_shadow,
    ingest_legacy_source,
    prove_legacy_unchanged,
    require_plan_pinned_legacy,
)
from people_counter.fabric_production_shadow_jobs import (
    SJD_NAMES,
    build_sjd_v2_definition,
)
from people_counter.fabric_reflex_definition import REFLEX_ID


CONTROLLER_ROOT = f"{PRODUCTION_SHADOW_FILES_ROOT}controller"
LOCAL_STATE_ROOT = Path(".people-counter-production-shadow")
PLAN_SCHEMA = "people-counter-production-shadow-controller-plan-v1"
INVENTORY_SCHEMA = "people-counter-production-shadow-controller-inventory-v1"
EVIDENCE_SCHEMA = "people-counter-production-shadow-controller-evidence-v1"
PREDEPLOY_SCHEMA = "people-counter-production-shadow-predeploy-snapshot-v1"
DEPLOY_PLAN_SCHEMA = "people-counter-production-shadow-fixed-deploy-plan-v1"
DEPLOY_REVIEW_SCHEMA = "people-counter-production-shadow-fixed-deploy-review-v1"
SJD_DISPLAY_NAMES = tuple(
    (job, f"pc-ca-production-shadow-{job}-v001") for job in SJD_NAMES
)
WORK_MUTATING_COMMANDS = frozenset(
    {
        "authorize",
        "bootstrap",
        "process",
        "compare",
        "reconcile",
        "synthetic-run",
    }
)
MUTATING_COMMANDS = WORK_MUTATING_COMMANDS | {"deploy"}
READ_ONLY_COMMANDS = frozenset(
    {
        "predeploy-snapshot",
        "deploy-plan",
        "deploy-review",
        "snapshot",
        "plan",
        "review",
        "status",
        "synthetic-plan",
        "synthetic-review",
    }
)
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class ShadowControllerError(RuntimeError):
    """A controller policy, identity, readback, or evidence gate failed."""


class CreateOnlyFiles(Protocol):
    """Minimal OneLake-compatible evidence port."""

    def exists(self, path: str) -> bool: ...

    def read_bytes(self, path: str) -> bytes: ...

    def create_bytes(self, path: str, content: bytes) -> None: ...


class LocalArtifacts(Protocol):
    """Separate local plan/review storage with observable file modes."""

    def create_bytes(self, path: str, content: bytes, *, mode: int) -> None: ...

    def read_bytes(self, path: str) -> bytes: ...

    def file_mode(self, path: str) -> int: ...


class ShadowControllerBackend(Protocol):
    """Fixed-scope port implemented by a host adapter or an offline fake."""

    def artifact_binding(self) -> Mapping[str, str]: ...

    def predeploy_snapshot(self) -> Mapping[str, Any]: ...

    def fixed_deployment_spec(self) -> Mapping[str, Any]: ...

    def deploy_fixed_definition(
        self, plan: Mapping[str, Any]
    ) -> Mapping[str, Any]: ...

    def shadow_deployment_state(self) -> Mapping[str, Any]: ...

    def sjd_state(self, display_name: str) -> Mapping[str, Any] | None: ...

    def read_legacy_rows(self) -> LegacySourceRows: ...

    def migration_proof(self) -> MigrationJournalProof: ...

    def quiescence(self) -> ShadowQuiescence: ...

    def status(self) -> Mapping[str, Any]: ...

    def deploy_sjd(
        self,
        display_name: str,
        definition: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...

    def read_allowlist(self, work_id: str) -> Sequence[Mapping[str, Any]]: ...

    def append_allowlist(self, row: Mapping[str, Any]) -> None: ...

    def read_audit(
        self, authorization_id: str
    ) -> Sequence[Mapping[str, Any]]: ...

    def append_audit(self, row: Mapping[str, Any]) -> None: ...

    def append_authorization(
        self,
        allowlist: Mapping[str, Any],
        audit: Mapping[str, Any],
    ) -> None: ...

    def table_schema(
        self, table_name: str
    ) -> Sequence[tuple[str, str, bool]] | None: ...

    def create_table(
        self,
        table_name: str,
        schema: Sequence[tuple[str, str, bool]],
    ) -> None: ...

    def authorization_result(self, plan_sha256: str) -> AuthorizationResult: ...

    def runtime_provenance(self) -> RuntimeProvenance: ...

    def committed_route(
        self, work_id: str, *, config: Any
    ) -> CommittedRoute | None: ...

    def register(
        self, registration: Mapping[str, Any], *, config: Any
    ) -> None: ...

    def claim(self, work_id: str, *, config: Any) -> Mapping[str, Any]: ...

    def process(
        self,
        claim: Mapping[str, Any],
        *,
        config: Any,
        provenance: RuntimeProvenance,
    ) -> Mapping[str, Any]: ...

    def seal(
        self,
        claim: Mapping[str, Any],
        output: Mapping[str, Any],
        *,
        config: Any,
    ) -> None: ...

    def publish(
        self,
        claim: Mapping[str, Any],
        *,
        authorization: Any,
        config: Any,
        provenance: RuntimeProvenance,
    ) -> CommittedRoute: ...

    def legacy_route(self, work_id: str) -> CommittedRoute: ...

    def shadow_route(self, work_id: str) -> CommittedRoute: ...

    def append_reconciliation(self, row: Mapping[str, Any]) -> None: ...


@dataclass(frozen=True, slots=True)
class SignedInventory:
    path: str
    payload: Mapping[str, Any]
    envelope_bytes: bytes
    hmac_key: bytes

    @property
    def payload_sha256(self) -> str:
        return str(json.loads(self.envelope_bytes)["payload_sha256"])

    @property
    def inventory_id(self) -> str:
        return Path(self.path).stem

    def redacted_summary(self) -> dict[str, object]:
        return {
            "hmac_key": "<redacted>",
            "inventory_id": self.inventory_id,
            "inventory_path": self.path,
            "nonce": "<redacted>",
            "payload_sha256": self.payload_sha256,
        }


class FileLocalArtifacts:
    """Create-only local implementation used by an explicitly wired host."""

    def __init__(self, root: Path = LOCAL_STATE_ROOT) -> None:
        self.root = root

    def _path(self, relative: str) -> Path:
        if (
            relative.startswith("/")
            or "\\" in relative
            or any(part in {"", ".", ".."} for part in relative.split("/"))
        ):
            raise ShadowControllerError("local artifact path escaped fixed root")
        path = self.root / relative
        if path.absolute().resolve(strict=False) != (
            self.root.absolute().resolve(strict=False) / relative
        ):
            raise ShadowControllerError("local artifact path escaped fixed root")
        return path

    def create_bytes(self, path: str, content: bytes, *, mode: int) -> None:
        target = self._path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(
            target,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            mode & 0o777,
        )
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(content)
        except BaseException:
            target.unlink(missing_ok=True)
            raise
        os.chmod(target, mode & 0o777)
        if target.read_bytes() != content:
            raise ShadowControllerError("local create readback differs")

    def read_bytes(self, path: str) -> bytes:
        return self._path(path).read_bytes()

    def file_mode(self, path: str) -> int:
        return self._path(path).stat().st_mode & 0o777


def _canonical(value: object) -> bytes:
    return canonical_bytes(value)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _hash(value: str, label: str) -> str:
    if not isinstance(value, str) or _HEX64.fullmatch(value) is None:
        raise ShadowControllerError(
            f"{label} must be 64 lowercase hexadecimal characters"
        )
    return value


def _safe_id(value: str, label: str) -> str:
    if not isinstance(value, str) or _SAFE_ID.fullmatch(value) is None:
        raise ShadowControllerError(f"{label} is not a safe explicit identity")
    return value


def _utc(timestamp: float) -> datetime:
    if not isinstance(timestamp, (int, float)) or isinstance(timestamp, bool):
        raise ShadowControllerError("clock must return a numeric timestamp")
    return datetime.fromtimestamp(float(timestamp), tz=timezone.utc)


def _datetime(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise ShadowControllerError(f"{label} must be an ISO UTC timestamp")
    try:
        result = datetime.fromisoformat(value)
    except ValueError as error:
        raise ShadowControllerError(f"{label} is invalid") from error
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ShadowControllerError(f"{label} must be UTC")
    return result


def _fixed_binding() -> dict[str, str]:
    return {
        "environment_id": ENVIRONMENT_ID,
        "lakehouse_id": LAKEHOUSE_ID,
        "migration_id": MIGRATION_ID,
        "workspace_id": WORKSPACE_ID,
    }


def _validated_binding(backend: ShadowControllerBackend) -> dict[str, str]:
    observed = dict(backend.artifact_binding())
    expected = _fixed_binding()
    if observed != expected:
        raise ShadowControllerError("Fabric artifact binding is not the reviewed set")
    return expected


def _quiescence_dict(value: ShadowQuiescence) -> dict[str, object]:
    return {
        "active_lease_ids": list(value.active_lease_ids),
        "active_writer_ids": list(value.active_writer_ids),
        "control_owner_id": value.control_owner_id,
        "observed_at": value.observed_at.isoformat(),
        "reflex_active": value.reflex_active,
        "reflex_id": value.reflex_id,
    }


def _migration_dict(value: MigrationJournalProof) -> dict[str, str]:
    return {
        "journal_sha256": value.journal_sha256,
        "migration_id": value.migration_id,
        "plan_sha256": value.plan_sha256,
        "status": value.status,
    }


def capture_snapshot(
    backend: ShadowControllerBackend,
    *,
    now: float,
    selected_work_id: str | None = None,
) -> tuple[dict[str, object], LegacySourceSnapshot | None]:
    """Capture only fixed artifacts and independently hash four legacy rows."""

    binding = _validated_binding(backend)
    eligible_reader = getattr(backend, "eligible_legacy_rows", None)
    if callable(eligible_reader):
        sources = sorted(
            (ingest_legacy_source(value) for value in eligible_reader()),
            key=lambda value: value.work_id,
        )
    else:
        sources = [ingest_legacy_source(backend.read_legacy_rows())]
    diagnostic_reader = getattr(backend, "route_diagnostics", None)
    route_diagnostics = (
        json.loads(_canonical(list(diagnostic_reader())))
        if callable(diagnostic_reader)
        else []
    )
    if len({value.work_id for value in sources}) != len(sources):
        raise ShadowControllerError("eligible legacy route inventory is duplicated")
    requested = selected_work_id
    if requested is None:
        requested = getattr(backend, "selected_work_id", None)
    matches = [
        value
        for value in sources
        if requested is None or value.work_id == requested
    ]
    if requested is not None and len(matches) != 1:
        raise ShadowControllerError("selected work is not exactly one eligible route")
    source = matches[0] if len(matches) == 1 else None
    sjds: list[dict[str, object]] = []
    for job, display_name in SJD_DISPLAY_NAMES:
        expected = build_sjd_v2_definition(job)
        observed = backend.sjd_state(display_name)
        if observed is not None:
            observed = json.loads(_canonical(dict(observed)))
            if observed.get("display_name") != display_name:
                raise ShadowControllerError("SJD state returned a different fixed name")
        sjds.append(
            {
                "display_name": display_name,
                "expected_definition_sha256": _sha256(_canonical(expected)),
                "job": job,
                "observed": observed,
                "observed_sha256": (
                    _sha256(_canonical(observed)) if observed is not None else None
                ),
            }
        )
    migration = backend.migration_proof()
    quiescence = backend.quiescence()
    if quiescence.reflex_id != REFLEX_ID:
        raise ShadowControllerError("quiescence proof names a different fixed Reflex")
    snapshot: dict[str, object] = {
        "artifact_binding": binding,
        "captured_at": _utc(now).isoformat(),
        "eligible_routes": [
            {
                "attempt_id": item.attempt_id,
                "identity": item.identity.as_dict(),
                "output_path_sha256": _sha256(item.output_path.encode()),
                "output_sha256": item.output_sha256,
                "row_hashes": dict(item.row_hashes),
                "source_rows_sha256": item.source_rows_sha256,
                "work_id": item.work_id,
            }
            for item in sources
        ],
        "legacy": (
            None
            if source is None
            else {
                "attempt_id": source.attempt_id,
                "identity": source.identity.as_dict(),
                "output_path_sha256": _sha256(source.output_path.encode()),
                "output_sha256": source.output_sha256,
                "row_hashes": dict(source.row_hashes),
                "source_rows_sha256": source.source_rows_sha256,
                "work_id": source.work_id,
            }
        ),
        "migration": _migration_dict(migration),
        "quiescence": _quiescence_dict(quiescence),
        "route_diagnostics": route_diagnostics,
        "schema": INVENTORY_SCHEMA,
        "sjds": sjds,
    }
    return json.loads(_canonical(snapshot)), source


def sign_inventory(
    payload: Mapping[str, Any],
    *,
    hmac_key: bytes | None = None,
    nonce: bytes | None = None,
) -> SignedInventory:
    """Sign one canonical inventory; key and nonce are never in summaries."""

    key = hmac_key or secrets.token_bytes(32)
    unique = nonce or secrets.token_bytes(16)
    if len(key) < 32 or len(unique) < 16:
        raise ShadowControllerError("inventory key or nonce is too short")
    canonical_payload = _canonical(dict(payload))
    payload_sha = _sha256(canonical_payload)
    nonce_hex = unique.hex()
    signature = hmac.new(
        key,
        canonical_payload + b"\0" + nonce_hex.encode(),
        hashlib.sha256,
    ).hexdigest()
    inventory_id = _sha256(
        payload_sha.encode() + b"\0" + nonce_hex.encode() + b"\0" + signature.encode()
    )
    envelope = {
        "algorithm": "HMAC-SHA256",
        "inventory_id": inventory_id,
        "nonce": nonce_hex,
        "payload": json.loads(canonical_payload),
        "payload_sha256": payload_sha,
        "schema": INVENTORY_SCHEMA,
        "signature": signature,
    }
    content = _canonical(envelope) + b"\n"
    path = _evidence_path(f"inventories/{inventory_id}.json")
    return SignedInventory(path, envelope["payload"], content, key)


def verify_inventory(inventory: SignedInventory) -> None:
    envelope = json.loads(inventory.envelope_bytes)
    payload = _canonical(envelope["payload"])
    if _sha256(payload) != envelope.get("payload_sha256"):
        raise ShadowControllerError("inventory payload hash differs")
    expected = hmac.new(
        inventory.hmac_key,
        payload + b"\0" + str(envelope["nonce"]).encode(),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected, str(envelope.get("signature"))):
        raise ShadowControllerError("inventory signature differs")


def _evidence_path(relative: str) -> str:
    if (
        relative.startswith("/")
        or "\\" in relative
        or any(part in {"", ".", ".."} for part in relative.split("/"))
    ):
        raise ShadowControllerError("controller evidence escaped fixed root")
    return f"{CONTROLLER_ROOT}/{relative}"


def _create_readback(files: CreateOnlyFiles, path: str, content: bytes) -> None:
    prefix = f"{CONTROLLER_ROOT}/"
    if not path.startswith(prefix) or _evidence_path(path[len(prefix) :]) != path:
        raise ShadowControllerError("create-only evidence escaped fixed root")
    if files.exists(path):
        raise FileExistsError(path)
    files.create_bytes(path, content)
    if files.read_bytes(path) != content:
        raise ShadowControllerError("create-only evidence readback differs")


def upload_inventory(files: CreateOnlyFiles, inventory: SignedInventory) -> None:
    verify_inventory(inventory)
    _create_readback(files, inventory.path, inventory.envelope_bytes)


def _plan_path(plan_sha256: str) -> str:
    return f"plans/{_hash(plan_sha256, 'plan_sha256')}.json"


def _review_path(plan_sha256: str) -> str:
    return f"reviews/{_hash(plan_sha256, 'plan_sha256')}.json"


def _predeploy_path(snapshot_sha256: str) -> str:
    return f"predeploy/{_hash(snapshot_sha256, 'predeploy snapshot sha256')}.json"


def _deploy_plan_path(plan_sha256: str) -> str:
    return f"deploy-plans/{_hash(plan_sha256, 'deployment plan sha256')}.json"


def _deploy_review_path(plan_sha256: str) -> str:
    return f"deploy-reviews/{_hash(plan_sha256, 'deployment plan sha256')}.json"


def capture_predeploy_snapshot(
    backend: ShadowControllerBackend,
    local: LocalArtifacts,
    *,
    now: float,
) -> dict[str, object]:
    """Capture and store REST-only deployment safety evidence.

    This path deliberately does not call a Spark job and does not use the
    create-only OneLake evidence port.  Route selection is explicitly absent;
    the authoritative eligible-route inventory is captured by ``snapshot``
    after the fixed shadow control SJD exists.
    """

    rest = json.loads(_canonical(dict(backend.predeploy_snapshot())))
    pipelines = rest.get("pipelines")
    reflex = rest.get("reflex")
    if not isinstance(pipelines, list) or not isinstance(reflex, Mapping):
        raise ShadowControllerError("REST predeploy evidence is incomplete")
    active_writers = [
        str(value.get("id"))
        for value in pipelines
        if isinstance(value, Mapping) and value.get("active_jobs")
    ]
    if bool(reflex.get("active")):
        active_writers.append(str(reflex.get("id")))
    if active_writers:
        raise ShadowControllerError("predeploy snapshot observed active writers")
    payload = {
        "artifact_binding": _fixed_binding(),
        "captured_at": _utc(now).isoformat(),
        "eligible_routes": None,
        "route_selection": None,
        "rest": rest,
        "schema": PREDEPLOY_SCHEMA,
        "writers_inactive": True,
    }
    snapshot_sha = _sha256(_canonical(payload))
    artifact = {
        "payload": payload,
        "snapshot_sha256": snapshot_sha,
        "schema": PREDEPLOY_SCHEMA,
    }
    content = _canonical(artifact) + b"\n"
    path = _predeploy_path(snapshot_sha)
    local.create_bytes(path, content, mode=0o644)
    if local.read_bytes(path) != content:
        raise ShadowControllerError("predeploy snapshot readback differs")
    return {
        "eligible_route_inventory": "deferred-to-postdeployment-snapshot",
        "path": path,
        "route_selection": None,
        "snapshot_sha256": snapshot_sha,
        "writers_inactive": True,
    }


def _load_predeploy(
    local: LocalArtifacts, snapshot_sha256: str
) -> Mapping[str, Any]:
    expected = _hash(snapshot_sha256, "predeploy snapshot sha256")
    try:
        artifact = json.loads(local.read_bytes(_predeploy_path(expected)))
    except (OSError, json.JSONDecodeError) as error:
        raise ShadowControllerError("local predeploy snapshot is unavailable") from error
    payload = artifact.get("payload") if isinstance(artifact, Mapping) else None
    if (
        not isinstance(payload, Mapping)
        or artifact.get("schema") != PREDEPLOY_SCHEMA
        or artifact.get("snapshot_sha256") != expected
        or _sha256(_canonical(dict(payload))) != expected
        or payload.get("artifact_binding") != _fixed_binding()
        or payload.get("writers_inactive") is not True
        or payload.get("route_selection") is not None
    ):
        raise ShadowControllerError("predeploy snapshot binding differs")
    return payload


def build_deployment_plan(
    backend: ShadowControllerBackend,
    local: LocalArtifacts,
    predeploy_snapshot_sha256: str,
    *,
    now: float,
) -> dict[str, object]:
    snapshot = _load_predeploy(local, predeploy_snapshot_sha256)
    spec = json.loads(_canonical(dict(backend.fixed_deployment_spec())))
    forbidden = {
        "work_id",
        "authorization_id",
        "table",
        "command",
        "data_path",
    }
    if forbidden.intersection(spec):
        raise ShadowControllerError("fixed deployment spec contains data-plane scope")
    created = _utc(now)
    plan = {
        "artifact_binding": _fixed_binding(),
        "created_at": created.isoformat(),
        "environment_policy_sha256": _sha256(
            _canonical(snapshot["rest"]["environment"])  # type: ignore[index]
        ),
        "expires_at": (created + AUTHORIZATION_MAX_AGE).isoformat(),
        "operation": "publish-project-wheel-and-upsert-three-shadow-sjds",
        "predeploy_snapshot_sha256": predeploy_snapshot_sha256,
        "schema": DEPLOY_PLAN_SCHEMA,
        "spec": spec,
        "writers_inactive": True,
    }
    plan_sha = _sha256(_canonical(plan))
    token = _sha256(b"fixed-shadow-deploy-v1\0" + plan_sha.encode())
    artifact = {
        "deployment_plan": plan,
        "deployment_plan_sha256": plan_sha,
        "deployment_token_sha256": _sha256(token.encode()),
        "schema": DEPLOY_PLAN_SCHEMA,
    }
    content = _canonical(artifact) + b"\n"
    path = _deploy_plan_path(plan_sha)
    local.create_bytes(path, content, mode=0o644)
    if local.read_bytes(path) != content:
        raise ShadowControllerError("fixed deployment plan readback differs")
    return {
        "deployment_plan_sha256": plan_sha,
        "deployment_token_sha256": artifact["deployment_token_sha256"],
        "expires_at": plan["expires_at"],
        "path": path,
        "scope": plan["operation"],
    }


def _load_deployment_plan(
    local: LocalArtifacts, plan_sha256: str, *, now: float
) -> tuple[dict[str, Any], str]:
    expected = _hash(plan_sha256, "deployment plan sha256")
    try:
        artifact = json.loads(local.read_bytes(_deploy_plan_path(expected)))
    except (OSError, json.JSONDecodeError) as error:
        raise ShadowControllerError("local deployment plan is unavailable") from error
    plan = artifact.get("deployment_plan") if isinstance(artifact, Mapping) else None
    if (
        not isinstance(plan, Mapping)
        or artifact.get("schema") != DEPLOY_PLAN_SCHEMA
        or artifact.get("deployment_plan_sha256") != expected
        or _sha256(_canonical(dict(plan))) != expected
        or plan.get("schema") != DEPLOY_PLAN_SCHEMA
        or plan.get("artifact_binding") != _fixed_binding()
        or plan.get("operation")
        != "publish-project-wheel-and-upsert-three-shadow-sjds"
        or plan.get("writers_inactive") is not True
    ):
        raise ShadowControllerError("fixed deployment plan binding differs")
    expires = _datetime(plan.get("expires_at"), "deployment plan expires_at")
    if _utc(now) > expires:
        raise ShadowControllerError("fixed deployment plan expired")
    token = _sha256(b"fixed-shadow-deploy-v1\0" + expected.encode())
    if artifact.get("deployment_token_sha256") != _sha256(token.encode()):
        raise ShadowControllerError("deployment token binding differs")
    return dict(plan), token


def review_deployment_plan(
    local: LocalArtifacts,
    plan_sha256: str,
    reviewer: str,
    *,
    now: float,
) -> dict[str, object]:
    plan, token = _load_deployment_plan(local, plan_sha256, now=now)
    receipt = {
        "deployment_plan_sha256": plan_sha256,
        "expires_at": plan["expires_at"],
        "reviewed_at": _utc(now).isoformat(),
        "reviewer": _safe_id(reviewer, "reviewer"),
        "scope": plan["operation"],
    }
    artifact = {"receipt": receipt, "schema": DEPLOY_REVIEW_SCHEMA}
    content = _canonical(artifact) + b"\n"
    path = _deploy_review_path(plan_sha256)
    local.create_bytes(path, content, mode=0o600)
    if local.read_bytes(path) != content or local.file_mode(path) != 0o600:
        raise ShadowControllerError("deployment review is not create-only mode 0600")
    return {
        "deployment_plan_sha256": plan_sha256,
        "deployment_token": token,
        "deployment_token_sha256": _sha256(token.encode()),
        "expires_at": plan["expires_at"],
        "receipt_path": path,
        "reviewer": receipt["reviewer"],
    }


def execute_fixed_deployment(
    backend: ShadowControllerBackend,
    local: LocalArtifacts,
    *,
    plan_sha256: str,
    deployment_token: str,
    now: float,
) -> dict[str, object]:
    plan, expected_token = _load_deployment_plan(local, plan_sha256, now=now)
    if not hmac.compare_digest(deployment_token, expected_token):
        raise ShadowControllerError("deployment token differs")
    try:
        review = json.loads(local.read_bytes(_deploy_review_path(plan_sha256)))
    except (OSError, json.JSONDecodeError) as error:
        raise ShadowControllerError("deployment review receipt is unavailable") from error
    receipt = review.get("receipt") if isinstance(review, Mapping) else None
    if (
        not isinstance(receipt, Mapping)
        or review.get("schema") != DEPLOY_REVIEW_SCHEMA
        or receipt.get("deployment_plan_sha256") != plan_sha256
        or receipt.get("expires_at") != plan["expires_at"]
        or receipt.get("scope") != plan["operation"]
        or local.file_mode(_deploy_review_path(plan_sha256)) != 0o600
    ):
        raise ShadowControllerError("deployment review receipt differs")
    result = json.loads(
        _canonical(dict(backend.deploy_fixed_definition(plan)))
    )
    return {
        "command": "deploy",
        "deployment_plan_sha256": plan_sha256,
        "result": result,
        "scope": plan["operation"],
    }


def build_plan(
    snapshot: Mapping[str, Any],
    *,
    now: float,
) -> ProductionShadowPlan:
    legacy = snapshot.get("legacy")
    migration = snapshot.get("migration")
    if not isinstance(legacy, Mapping) or not isinstance(migration, Mapping):
        raise ShadowControllerError("snapshot is missing legacy or migration proof")
    identity = legacy.get("identity")
    if not isinstance(identity, Mapping):
        raise ShadowControllerError("snapshot legacy identity is missing")
    work = AllowlistRow(**{key: str(value) for key, value in identity.items()})
    created = _utc(now)
    plan_binding = {
        "artifact_binding": _fixed_binding(),
        "created_at": created.isoformat(),
        "migration_plan_sha256": migration.get("plan_sha256"),
        "source_rows_sha256": legacy.get("source_rows_sha256"),
        "work": work.as_dict(),
    }
    plan_id = f"shadow-{_sha256(_canonical(plan_binding))[:32]}"
    return ProductionShadowPlan(
        plan_id=plan_id,
        work=work,
        migration_plan_sha256=str(migration.get("plan_sha256")),
        created_at=created,
        expires_at=created + AUTHORIZATION_MAX_AGE,
        legacy_source_rows_sha256=_hash(
            str(legacy.get("source_rows_sha256")),
            "legacy source rows sha256",
        ),
        legacy_output_sha256=_hash(
            str(legacy.get("output_sha256")),
            "legacy output sha256",
        ),
    )


def store_plan(
    local: LocalArtifacts,
    snapshot: Mapping[str, Any],
    plan: ProductionShadowPlan,
) -> dict[str, object]:
    artifact = {
        "artifact_binding": _fixed_binding(),
        "inventory_payload_sha256": _sha256(_canonical(dict(snapshot))),
        "plan": plan.as_dict(),
        "plan_sha256": plan.sha256,
        "safety_token_sha256": _sha256(plan.safety_token.encode()),
        "schema": PLAN_SCHEMA,
    }
    content = _canonical(artifact) + b"\n"
    path = _plan_path(plan.sha256)
    local.create_bytes(path, content, mode=0o644)
    if local.read_bytes(path) != content:
        raise ShadowControllerError("local plan readback differs")
    return {
        "path": path,
        "plan_sha256": plan.sha256,
        "safety_token_sha256": artifact["safety_token_sha256"],
    }


def _plan_from_dict(value: Mapping[str, Any]) -> ProductionShadowPlan:
    work_value = value.get("work")
    if not isinstance(work_value, Mapping):
        raise ShadowControllerError("stored plan work is missing")
    work = AllowlistRow(
        **{
            key: str(work_value[key])
            for key in (
                "work_id",
                "camera_sha256",
                "location_sha256",
                "model_sha256",
                "source_sha256",
                "config_sha256",
            )
        }
    )
    return ProductionShadowPlan(
        plan_id=str(value.get("plan_id")),
        work=work,
        migration_plan_sha256=str(value.get("migration_plan_sha256")),
        created_at=_datetime(value.get("created_at"), "plan created_at"),
        expires_at=_datetime(value.get("expires_at"), "plan expires_at"),
        legacy_source_rows_sha256=(
            None
            if value.get("legacy_source_rows_sha256") is None
            else str(value.get("legacy_source_rows_sha256"))
        ),
        legacy_output_sha256=(
            None
            if value.get("legacy_output_sha256") is None
            else str(value.get("legacy_output_sha256"))
        ),
        schema_version=int(value.get("schema_version", 0)),
    )


def _synthetic_plan_path(plan_sha256: str) -> str:
    return f"synthetic-plans/{_hash(plan_sha256, 'synthetic plan sha256')}.json"


def _synthetic_review_path(plan_sha256: str) -> str:
    return f"synthetic-reviews/{_hash(plan_sha256, 'synthetic plan sha256')}.json"


def build_synthetic_plan(
    backend: ShadowControllerBackend,
    local: LocalArtifacts,
    *,
    now: float,
) -> dict[str, object]:
    _validated_binding(backend)
    quiescence = backend.quiescence()
    try:
        validation_now = max(_utc(now), quiescence.observed_at)
        quiescence.validate(expected_reflex_id=REFLEX_ID, now=validation_now)
    except ProductionShadowError as error:
        raise ShadowControllerError(
            "synthetic plan requires quiescent production"
        ) from error
    proof = backend.migration_proof()
    if proof.status not in {"APPLIED", "NOOP"}:
        raise ShadowControllerError("synthetic plan requires applied migration")
    reader = getattr(backend, "synthetic_candidate", None)
    if not callable(reader):
        raise ShadowControllerError("synthetic candidate reader is unavailable")
    candidate = reader()
    identity = candidate.get("identity")
    payload = candidate.get("payload")
    if (
        candidate.get("comparison_mode") != "NO_LEGACY_BASELINE"
        or not isinstance(identity, Mapping)
        or not isinstance(payload, Mapping)
    ):
        raise ShadowControllerError("synthetic candidate evidence differs")
    created = _utc(now)
    plan = SyntheticShadowPlan(
        plan_id=(
            "shadow-synthetic-"
            + _sha256(_canonical(candidate))[:32]
        ),
        work=AllowlistRow(
            **{key: str(value) for key, value in identity.items()}
        ),
        payload=dict(payload),
        source_evidence_sha256=_hash(
            str(candidate.get("source_evidence_sha256")),
            "synthetic source evidence",
        ),
        created_at=created,
        expires_at=created + timedelta(hours=2),
    )
    artifact = {
        "artifact_binding": _fixed_binding(),
        "candidate_id": candidate.get("candidate_id"),
        "evidence": candidate.get("evidence"),
        "migration": _migration_dict(proof),
        "plan": plan.as_dict(),
        "plan_sha256": plan.sha256,
        "safety_token_sha256": _sha256(plan.safety_token.encode()),
        "schema": "people-counter-shadow-synthetic-plan-v1",
    }
    content = _canonical(artifact) + b"\n"
    path = _synthetic_plan_path(plan.sha256)
    local.create_bytes(path, content, mode=0o644)
    if local.read_bytes(path) != content:
        raise ShadowControllerError("synthetic plan readback differs")
    return {
        "comparison_mode": plan.comparison_mode,
        "expires_at": plan.expires_at.isoformat(),
        "path": path,
        "plan_sha256": plan.sha256,
        "plan_type": plan.plan_type,
        "safety_token_sha256": artifact["safety_token_sha256"],
        "work_id": plan.work.work_id,
    }


def _synthetic_plan_from_dict(value: Mapping[str, Any]) -> SyntheticShadowPlan:
    work = value.get("work")
    payload = value.get("payload")
    if not isinstance(work, Mapping) or not isinstance(payload, Mapping):
        raise ShadowControllerError("synthetic plan work or payload is missing")
    return SyntheticShadowPlan(
        plan_id=str(value["plan_id"]),
        work=AllowlistRow(
            **{
                key: str(work[key])
                for key in (
                    "work_id",
                    "camera_sha256",
                    "location_sha256",
                    "model_sha256",
                    "source_sha256",
                    "config_sha256",
                )
            }
        ),
        payload=dict(payload),
        source_evidence_sha256=str(value["source_evidence_sha256"]),
        created_at=_datetime(value["created_at"], "synthetic created_at"),
        expires_at=_datetime(value["expires_at"], "synthetic expires_at"),
        plan_type=str(value["plan_type"]),
        comparison_mode=str(value["comparison_mode"]),
    )


def load_synthetic_plan(
    local: LocalArtifacts, plan_sha256: str
) -> SyntheticShadowPlan:
    try:
        artifact = json.loads(local.read_bytes(_synthetic_plan_path(plan_sha256)))
    except (OSError, json.JSONDecodeError) as error:
        raise ShadowControllerError("synthetic plan is unavailable") from error
    value = artifact.get("plan") if isinstance(artifact, Mapping) else None
    if (
        not isinstance(value, Mapping)
        or artifact.get("schema") != "people-counter-shadow-synthetic-plan-v1"
    ):
        raise ShadowControllerError("synthetic plan artifact differs")
    plan = _synthetic_plan_from_dict(value)
    if plan.sha256 != plan_sha256 or artifact.get("plan_sha256") != plan_sha256:
        raise ShadowControllerError("synthetic plan hash differs")
    return plan


def review_synthetic_plan(
    local: LocalArtifacts,
    plan_sha256: str,
    reviewer: str,
    *,
    now: float,
) -> dict[str, object]:
    plan = load_synthetic_plan(local, plan_sha256)
    plan.validate_at(_utc(now))
    receipt = {
        "comparison_mode": plan.comparison_mode,
        "expires_at": plan.expires_at.isoformat(),
        "plan_sha256": plan.sha256,
        "plan_type": plan.plan_type,
        "reviewed_at": _utc(now).isoformat(),
        "reviewer": _safe_id(reviewer, "reviewer"),
        "work_identity_sha256": plan.identity_sha256,
    }
    artifact = {
        "receipt": receipt,
        "schema": "people-counter-shadow-synthetic-review-v1",
    }
    content = _canonical(artifact) + b"\n"
    path = _synthetic_review_path(plan.sha256)
    local.create_bytes(path, content, mode=0o600)
    if local.read_bytes(path) != content or local.file_mode(path) != 0o600:
        raise ShadowControllerError("synthetic review is not mode 0600")
    return {
        "comparison_mode": plan.comparison_mode,
        "expires_at": plan.expires_at.isoformat(),
        "plan_sha256": plan.sha256,
        "receipt_path": path,
        "reviewer": receipt["reviewer"],
        "safety_token": plan.safety_token,
    }


def execute_synthetic_run(
    backend: ShadowControllerBackend,
    files: CreateOnlyFiles,
    local: LocalArtifacts,
    *,
    plan_sha256: str,
    safety_token: str,
    now: float,
) -> dict[str, object]:
    plan = load_synthetic_plan(local, plan_sha256)
    plan.validate_at(_utc(now))
    plan.require_token(safety_token)
    try:
        review_artifact = json.loads(
            local.read_bytes(_synthetic_review_path(plan.sha256))
        )
    except (OSError, json.JSONDecodeError) as error:
        raise ShadowControllerError("synthetic review is unavailable") from error
    receipt = (
        review_artifact.get("receipt")
        if isinstance(review_artifact, Mapping)
        else None
    )
    if (
        not isinstance(receipt, Mapping)
        or review_artifact.get("schema")
        != "people-counter-shadow-synthetic-review-v1"
        or receipt.get("comparison_mode") != "NO_LEGACY_BASELINE"
        or receipt.get("plan_sha256") != plan.sha256
        or receipt.get("work_identity_sha256") != plan.identity_sha256
        or local.file_mode(_synthetic_review_path(plan.sha256)) != 0o600
    ):
        raise ShadowControllerError("synthetic review binding differs")
    configure = getattr(backend, "configure_synthetic", None)
    if not callable(configure):
        raise ShadowControllerError("synthetic backend support is unavailable")
    reviewed_at = _datetime(receipt["reviewed_at"], "synthetic reviewed_at")
    reviewer = str(receipt["reviewer"])
    configure(plan, reviewed_at=reviewed_at, reviewer=reviewer)

    wrong_token_rejected = False
    wrong_work_rejected = False
    try:
        plan.require_token("injected-invalid-token")
    except ProductionShadowError:
        wrong_token_rejected = True
    try:
        backend.claim("injected-wrong-work", config=FabricCandidateAConfig.production_shadow())  # type: ignore[attr-defined]
    except ProductionShadowError:
        wrong_work_rejected = True
    if not wrong_token_rejected or not wrong_work_rejected:
        raise ShadowControllerError("synthetic failure injection did not fail closed")

    bootstrap_production_shadow(backend)
    provenance = backend.runtime_provenance()
    first = execute_synthetic_shadow(
        plan,
        backend,  # type: ignore[arg-type]
        provenance,
        reviewed_at=reviewed_at,
        reviewer=reviewer,
    )
    second = execute_synthetic_shadow(
        plan,
        backend,  # type: ignore[arg-type]
        provenance,
        reviewed_at=reviewed_at,
        reviewer=reviewer,
    )
    if first != second:
        raise ShadowControllerError("synthetic idempotent rerun differs")
    reconciliation = {
        "comparison_mode": "NO_LEGACY_BASELINE",
        "finding_ids": [],
        "finding_types": [],
        "infrastructure_validation": True,
        "passed": True,
        "plan_sha256": plan.sha256,
        "production_route_parity_satisfied": False,
        "work_id": plan.work.work_id,
    }
    backend.append_reconciliation(reconciliation)
    evidence = {
        "comparison_mode": "NO_LEGACY_BASELINE",
        "failure_injections": {
            "wrong_token_rejected": wrong_token_rejected,
            "wrong_work_rejected": wrong_work_rejected,
        },
        "legacy_comparison_pass": False,
        "plan_sha256": plan.sha256,
        "production_allowlist_appended": False,
        "production_audit_appended": False,
        "production_route_parity_satisfied": False,
        "route": {
            "attempt_id_sha256": _sha256(first.attempt_id.encode()),
            "committed": first.committed,
            "idempotent_rerun_equal": first == second,
            "output_path_sha256": _sha256(first.output_path.encode()),
            "output_sha256": first.output_sha256,
            "pointer_matches": first.pointer_attempt_id == first.attempt_id,
            "publication_count": first.publication_count,
            "sealed": first.sealed,
        },
        "schema": "people-counter-shadow-synthetic-evaluation-v1",
    }
    evidence_path = (
        f"{CONTROLLER_ROOT}/synthetic/{plan.sha256}/evaluation.json"
    )
    _create_readback(files, evidence_path, _canonical(evidence) + b"\n")
    return {
        "comparison_mode": "NO_LEGACY_BASELINE",
        "evidence_path": evidence_path,
        "legacy_comparison_pass": False,
        "plan_sha256": plan.sha256,
        "production_route_parity_satisfied": False,
        "result": evidence,
    }


def load_plan(local: LocalArtifacts, plan_sha256: str) -> ProductionShadowPlan:
    expected_sha = _hash(plan_sha256, "plan_sha256")
    try:
        artifact = json.loads(local.read_bytes(_plan_path(expected_sha)))
    except (OSError, json.JSONDecodeError) as error:
        raise ShadowControllerError("local canonical plan is unavailable") from error
    if (
        not isinstance(artifact, Mapping)
        or artifact.get("schema") != PLAN_SCHEMA
        or artifact.get("artifact_binding") != _fixed_binding()
    ):
        raise ShadowControllerError("local plan envelope differs")
    plan_value = artifact.get("plan")
    if not isinstance(plan_value, Mapping):
        raise ShadowControllerError("local plan payload is missing")
    plan = _plan_from_dict(plan_value)
    if plan.sha256 != expected_sha or artifact.get("plan_sha256") != expected_sha:
        raise ShadowControllerError("local plan hash differs")
    if artifact.get("safety_token_sha256") != _sha256(plan.safety_token.encode()):
        raise ShadowControllerError("local deterministic token hash differs")
    return plan


def review_plan(
    local: LocalArtifacts,
    plan_sha256: str,
    reviewer: str,
    *,
    now: float,
) -> dict[str, object]:
    plan = load_plan(local, plan_sha256)
    current = _utc(now)
    plan.validate_at(current)
    receipt = ReviewReceipt(
        plan_sha256=plan.sha256,
        work_id=plan.work.work_id,
        work_identity_sha256=plan.identity_sha256,
        expires_at=plan.expires_at,
        reviewed_at=current,
        reviewer=_safe_id(reviewer, "reviewer"),
    )
    artifact = {
        "artifact_binding": _fixed_binding(),
        "receipt": receipt.as_dict(),
        "schema": "people-counter-production-shadow-controller-review-v1",
    }
    path = _review_path(plan.sha256)
    content = _canonical(artifact) + b"\n"
    local.create_bytes(path, content, mode=0o600)
    if local.read_bytes(path) != content or local.file_mode(path) != 0o600:
        raise ShadowControllerError("review receipt is not create-only mode 0600")
    return {
        "expires_at": plan.expires_at.isoformat(),
        "plan_sha256": plan.sha256,
        "receipt_path": path,
        "reviewer": receipt.reviewer,
        "safety_token": plan.safety_token,
    }


def load_review(
    local: LocalArtifacts,
    plan: ProductionShadowPlan,
    *,
    now: float,
) -> ReviewReceipt:
    path = _review_path(plan.sha256)
    try:
        artifact = json.loads(local.read_bytes(path))
    except (OSError, json.JSONDecodeError) as error:
        raise ShadowControllerError("separate local review receipt is unavailable") from error
    if (
        not isinstance(artifact, Mapping)
        or artifact.get("artifact_binding") != _fixed_binding()
        or artifact.get("schema")
        != "people-counter-production-shadow-controller-review-v1"
    ):
        raise ShadowControllerError("review receipt envelope differs")
    value = artifact.get("receipt")
    if not isinstance(value, Mapping):
        raise ShadowControllerError("review receipt payload is missing")
    receipt = ReviewReceipt(
        plan_sha256=str(value.get("plan_sha256")),
        work_id=str(value.get("work_id")),
        work_identity_sha256=str(value.get("work_identity_sha256")),
        expires_at=_datetime(value.get("expires_at"), "receipt expires_at"),
        reviewed_at=_datetime(value.get("reviewed_at"), "receipt reviewed_at"),
        reviewer=str(value.get("reviewer")),
        schema_version=int(value.get("schema_version", 0)),
    )
    receipt.validate(plan, now=_utc(now), file_mode=local.file_mode(path))
    return receipt


def redact(value: object, secrets_to_hide: Sequence[str]) -> str:
    text = str(value)
    for secret in secrets_to_hide:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


def _authorization_summary(value: AuthorizationResult) -> dict[str, object]:
    return {
        "allowlist_appended": value.allowlist_appended,
        "audit_appended": value.audit_appended,
        "authorization_id": value.row.authorization_id,
        "plan_sha256": value.row.plan_sha256,
        "replayed": value.replayed,
        "work_id": value.row.work.work_id,
    }


def _route_summary(value: CommittedRoute) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "committed": value.committed,
        "output_sha256": value.output_sha256,
        "plan_sha256": value.plan_sha256,
        "publication_count": value.publication_count,
        "sealed": value.sealed,
        "work_id": value.work_id,
    }


def _reconciliation_summary(value: Any) -> dict[str, object]:
    return {
        "finding_ids": [finding.finding_id for finding in value.findings],
        "finding_types": [finding.finding_type for finding in value.findings],
        "passed": value.passed,
        "plan_sha256": value.plan_sha256,
        "work_id": value.work_id,
    }


def _run_action(
    command: str,
    backend: ShadowControllerBackend,
    source: LegacySourceSnapshot,
    plan: ProductionShadowPlan,
    receipt: ReviewReceipt,
    safety_token: str,
    *,
    now: float,
) -> Mapping[str, Any]:
    prepare = getattr(backend, "prepare_plan_context", None)
    if callable(prepare):
        prepare(
            plan=plan,
            receipt=receipt,
            safety_token=safety_token,
            source=source,
        )
    if command == "deploy":
        deployed = []
        for job, display_name in SJD_DISPLAY_NAMES:
            result = backend.deploy_sjd(
                display_name, build_sjd_v2_definition(job)
            )
            deployed.append(
                {
                    "display_name": display_name,
                    "job": job,
                    "readback_sha256": _sha256(_canonical(dict(result))),
                }
            )
        return {"sjds": deployed}
    if command == "authorize":
        result = authorize_production_shadow(
            plan,
            selected_work_ids=plan.work_ids,
            observed_work=source.identity,
            safety_token=safety_token,
            receipt=receipt,
            receipt_mode=0o600,
            migration=backend.migration_proof(),
            quiescence=backend.quiescence(),
            expected_reflex_id=REFLEX_ID,
            store=backend,
            now=_utc(now),
        )
        return _authorization_summary(result)
    if command == "bootstrap":
        return {"created_tables": list(bootstrap_production_shadow(backend))}
    authorization = backend.authorization_result(plan.sha256)
    if authorization.row.work != plan.work:
        raise ShadowControllerError("stored authorization differs from canonical plan")
    if command == "process":
        route = execute_production_shadow(
            source,
            authorization,
            backend,
            backend.runtime_provenance(),
        )
        return _route_summary(route)
    legacy = backend.legacy_route(plan.work.work_id)
    shadow = backend.shadow_route(plan.work.work_id)
    reconciliation = compare_and_reconcile(
        legacy,
        shadow,
        authorization.row,
        tolerance=RouteTolerance(),
    )
    summary = _reconciliation_summary(reconciliation)
    if command == "compare":
        reconciliation.require_pass()
        return summary
    if command == "reconcile":
        reconciliation.require_pass()
        backend.append_reconciliation(summary)
        return summary
    raise ShadowControllerError(f"unsupported mutating command {command!r}")


def execute_mutation(
    command: str,
    backend: ShadowControllerBackend,
    files: CreateOnlyFiles,
    local: LocalArtifacts,
    *,
    plan_sha256: str,
    safety_token: str,
    now: float,
) -> dict[str, object]:
    """Execute one reviewed operation and prove legacy rows stayed byte-logical."""

    if command not in WORK_MUTATING_COMMANDS:
        raise ShadowControllerError("command is not a reviewed mutation")
    plan = load_plan(local, plan_sha256)
    plan.require_token(safety_token)
    receipt = load_review(local, plan, now=now)
    if hasattr(backend, "selected_work_id"):
        setattr(backend, "selected_work_id", plan.work.work_id)
    snapshot, before = capture_snapshot(
        backend, now=now, selected_work_id=plan.work.work_id
    )
    if before is None:
        raise ShadowControllerError("plan-pinned legacy route is unavailable")
    require_plan_pinned_legacy(plan, before)
    migration = snapshot.get("migration")
    if (
        not isinstance(migration, Mapping)
        or migration.get("plan_sha256") != plan.migration_plan_sha256
    ):
        raise ShadowControllerError("fresh migration proof differs from canonical plan")
    inventory = sign_inventory(snapshot)
    upload_inventory(files, inventory)
    try:
        result = _run_action(
            command,
            backend,
            before,
            plan,
            receipt,
            safety_token,
            now=now,
        )
    except Exception:
        after = ingest_legacy_source(backend.read_legacy_rows())
        prove_legacy_unchanged(before, after)
        raise
    else:
        after = ingest_legacy_source(backend.read_legacy_rows())
        legacy_proof = dict(prove_legacy_unchanged(before, after))
    evidence = {
        "command": command,
        "completed_at": _utc(now).isoformat(),
        "inventory_id": inventory.inventory_id,
        "inventory_payload_sha256": inventory.payload_sha256,
        "legacy_proof": legacy_proof,
        "plan_sha256": plan.sha256,
        "result": json.loads(_canonical(dict(result))),
        "schema": EVIDENCE_SCHEMA,
    }
    evidence_bytes = _canonical(evidence) + b"\n"
    evidence_path = _evidence_path(
        f"operations/{inventory.inventory_id}-{command}.json"
    )
    _create_readback(files, evidence_path, evidence_bytes)
    return {
        "command": command,
        "evidence_path": evidence_path,
        "inventory": inventory.redacted_summary(),
        "legacy_unchanged": True,
        "plan_sha256": plan.sha256,
        "result": result,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pc-production-shadow-controller")
    parser.add_argument(
        "command",
        choices=tuple(sorted(READ_ONLY_COMMANDS | MUTATING_COMMANDS)),
        nargs="?",
        default="snapshot",
    )
    parser.add_argument("--plan-sha256")
    parser.add_argument("--predeploy-snapshot-sha256")
    parser.add_argument("--deployment-plan-sha256")
    parser.add_argument("--deployment-token")
    parser.add_argument("--reviewer")
    parser.add_argument("--safety-token")
    parser.add_argument("--work-id")
    parser.add_argument(
        "--auth",
        choices=("azure-cli", "default", "managed-identity", "service-principal"),
        default="azure-cli",
    )
    parser.add_argument("--execute", action="store_true")
    return parser


def _required(args: argparse.Namespace, name: str) -> str:
    value = getattr(args, name)
    if not isinstance(value, str) or not value:
        raise ShadowControllerError("--" + name.replace("_", "-") + " is required")
    return value


def _print(value: object, output: TextIO) -> None:
    print(_canonical(value).decode(), file=output)


def _read_only(
    args: argparse.Namespace,
    backend: ShadowControllerBackend,
    local: LocalArtifacts,
    *,
    now: float,
) -> object:
    if args.command == "predeploy-snapshot":
        return capture_predeploy_snapshot(backend, local, now=now)
    if args.command == "deploy-plan":
        return build_deployment_plan(
            backend,
            local,
            _required(args, "predeploy_snapshot_sha256"),
            now=now,
        )
    if args.command == "deploy-review":
        return review_deployment_plan(
            local,
            _required(args, "deployment_plan_sha256"),
            _required(args, "reviewer"),
            now=now,
        )
    if args.command == "review":
        return review_plan(
            local,
            _required(args, "plan_sha256"),
            _required(args, "reviewer"),
            now=now,
        )
    if args.command == "synthetic-review":
        return review_synthetic_plan(
            local,
            _required(args, "plan_sha256"),
            _required(args, "reviewer"),
            now=now,
        )
    if args.command == "status":
        if args.plan_sha256 is not None:
            plan = load_plan(local, args.plan_sha256)
            select = getattr(backend, "select_plan_for_status", None)
            if callable(select):
                select(plan)
        _validated_binding(backend)
        return {
            "artifact_binding": _fixed_binding(),
            "execute": False,
            "schema": "people-counter-production-shadow-controller-status-v1",
            "status": json.loads(_canonical(dict(backend.status()))),
        }
    deployment_state = getattr(backend, "shadow_deployment_state", None)
    if args.command == "snapshot" and callable(deployment_state):
        state = json.loads(_canonical(dict(deployment_state())))
        if state.get("ready") is not True:
            return {
                "execute": False,
                "one_lake_write": False,
                "schema": "people-counter-production-shadow-predeployment-state-v1",
                "state": state,
            }
    if args.command == "synthetic-plan":
        return build_synthetic_plan(backend, local, now=now)
    snapshot, _ = capture_snapshot(
        backend, now=now, selected_work_id=args.work_id
    )
    response: dict[str, object] = {
        "execute": False,
        "payload_sha256": _sha256(_canonical(snapshot)),
        "schema": snapshot["schema"],
        "eligible_work_ids": [
            value["work_id"] for value in snapshot["eligible_routes"]  # type: ignore[index]
        ],
        "route_diagnostics": snapshot["route_diagnostics"],
        "work_id": (
            None
            if snapshot["legacy"] is None
            else snapshot["legacy"]["work_id"]  # type: ignore[index]
        ),
    }
    if args.command == "plan":
        if snapshot["legacy"] is None:
            raise ShadowControllerError(
                "plan requires --work-id when multiple routes are eligible"
            )
        plan = build_plan(snapshot, now=now)
        response["local_plan"] = store_plan(local, snapshot, plan)
    return response


def main(
    argv: Sequence[str] | None = None,
    *,
    backend: ShadowControllerBackend | None = None,
    files: CreateOnlyFiles | None = None,
    local: LocalArtifacts | None = None,
    clock: Callable[[], float] = time.time,
    output: TextIO = sys.stdout,
    errors: TextIO = sys.stderr,
) -> int:
    args = _build_parser().parse_args(argv)
    hidden = [args.safety_token] if isinstance(args.safety_token, str) else []
    try:
        if backend is None:
            from people_counter.fabric_canary_tool import make_token_provider
            from people_counter.fabric_production_shadow_backend import (
                FabricShadowControllerBackend,
                ShadowAzureOneLakeFiles,
                ShadowFabricREST,
            )
            live_files = files or ShadowAzureOneLakeFiles()
            backend = FabricShadowControllerBackend(
                ShadowFabricREST(make_token_provider(args.auth)),
                live_files,
                selected_work_id=args.work_id,
                clock=clock,
            )
            files = live_files
        elif args.work_id is not None and hasattr(backend, "selected_work_id"):
            setattr(backend, "selected_work_id", args.work_id)
        local_store = local or FileLocalArtifacts()
        now = float(clock())
        if args.command in READ_ONLY_COMMANDS:
            if args.execute:
                raise ShadowControllerError(
                    "--execute is invalid for read-only commands"
                )
            result = _read_only(args, backend, local_store, now=now)
        else:
            if not args.execute:
                raise ShadowControllerError(
                    f"{args.command} is refused without explicit --execute"
                )
            if args.command != "deploy" and files is None:
                raise ShadowControllerError(
                    "an explicit create-only evidence store is required"
                )
            if args.command == "deploy":
                result = execute_fixed_deployment(
                    backend,
                    local_store,
                    plan_sha256=_required(args, "deployment_plan_sha256"),
                    deployment_token=_required(args, "deployment_token"),
                    now=now,
                )
            else:
                if args.command == "synthetic-run":
                    result = execute_synthetic_run(
                        backend,
                        files,
                        local_store,
                        plan_sha256=_required(args, "plan_sha256"),
                        safety_token=_required(args, "safety_token"),
                        now=now,
                    )
                else:
                    result = execute_mutation(
                        args.command,
                        backend,
                        files,
                        local_store,
                        plan_sha256=_required(args, "plan_sha256"),
                        safety_token=_required(args, "safety_token"),
                        now=now,
                    )
        _print(result, output)
        return 0
    except (
        FileExistsError,
        OSError,
        ProductionShadowError,
        ShadowControllerError,
        TypeError,
        ValueError,
    ) as error:
        print(redact(f"refused: {error}", hidden), file=errors)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
