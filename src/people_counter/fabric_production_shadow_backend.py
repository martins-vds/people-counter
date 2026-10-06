"""Concrete fixed-scope host backend for the production-shadow controller."""

from __future__ import annotations

import hashlib
import hmac
import io
import json
import secrets
import shlex
import time
import uuid
import zipfile
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

from people_counter.fabric_candidate_a import (
    ENVIRONMENT_ID,
    LAKEHOUSE_ID,
    PRODUCTION_SHADOW_FILES_ROOT,
    WORKSPACE_ID,
    FabricCandidateAConfig,
)
from people_counter.fabric_production_migration import MIGRATION_ID
from people_counter.fabric_production_migration_tool import (
    PROJECT_WHEEL,
    AzureOneLakeFiles,
    ControllerError,
    FabricRESTController,
    WRITER_ITEMS,
    _environment_custom_wheels,
    _environment_public_packages,
    _definition_parts,
    _poll_job,
    _verify_environment_policy,
    build_wheel,
)
from people_counter.fabric_production_routing import AllowlistRow, canonical_bytes
from people_counter.fabric_production_shadow import (
    PACKAGE_VERSION,
    PRODUCTION_ALLOWLIST_TABLE,
    PRODUCTION_AUDIT_TABLE,
    AuthorizationIntent,
    AuthorizationResult,
    AuthorizationRow,
    CommittedRoute,
    LegacySourceRows,
    LegacySourceSnapshot,
    MigrationJournalProof,
    ProductionShadowError,
    ProductionShadowPlan,
    ReviewReceipt,
    RuntimeProvenance,
    SHADOW_SCHEMAS,
    ShadowQuiescence,
    SyntheticShadowPlan,
    authorization_intent,
)
from people_counter.fabric_production_shadow_jobs import (
    SJD_NAMES,
    build_sjd_v2_definition,
)
from people_counter.fabric_production_shadow_live import (
    FAILURE_SCHEMA,
    LIVE_SCHEMA,
    LIVE_ROOT,
    RESULT_SCHEMA,
    failure_path,
    request_path,
    result_path,
    started_path,
)
from people_counter.fabric_reflex_definition import REFLEX_ID
from people_counter.fabric_reflex_definition import parse_reflex_rule_definition


SJD_DISPLAY_NAMES = {
    job: f"pc-ca-production-shadow-{job}-v001" for job in SJD_NAMES
}
_ACTIVE = frozenset({"inprogress", "notstarted", "queued", "running", "starting"})
_SUCCESS = frozenset({"completed", "succeeded"})
_FAILURE = frozenset({"cancelled", "canceled", "failed", "deduped"})
_JOB_STATES = _ACTIVE | _SUCCESS | _FAILURE


def _active_jobs_exact(
    values: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    """Reject unclassified Fabric job states before proving quiescence."""

    active: list[Mapping[str, Any]] = []
    for value in values:
        if not isinstance(value, Mapping):
            raise LiveShadowBackendError(
                "Fabric job inventory contains a non-object"
            )
        status = value.get("status")
        normalized = status.lower() if isinstance(status, str) else ""
        if normalized not in _JOB_STATES:
            raise LiveShadowBackendError(
                "Fabric job inventory contains an unknown status"
            )
        if normalized in _ACTIVE:
            active.append(value)
    return active


class LiveShadowBackendError(ProductionShadowError):
    """A fixed REST, OneLake, Spark-job, or readback operation failed."""


class ShadowFabricREST(FabricRESTController):
    """Fabric REST adapter with only the three fixed shadow SJD mutations."""

    def create_shadow_sjd(
        self, display_name: str, definition: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        if display_name not in SJD_DISPLAY_NAMES.values():
            raise LiveShadowBackendError("SJD name is outside the fixed set")
        return self._lro_json(
            "POST",
            f"/workspaces/{WORKSPACE_ID}/sparkJobDefinitions",
            value={
                **definition,
                "displayName": display_name,
                "description": "Reviewed people-counter production shadow",
            },
            accepted=(201, 202),
        )

    def update_shadow_sjd(
        self,
        item_id: str,
        display_name: str,
        definition: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        exact = [
            item
            for item in self.list_items()
            if item.get("id") == item_id
            and item.get("displayName") == display_name
            and item.get("type") == "SparkJobDefinition"
        ]
        if len(exact) != 1 or display_name not in SJD_DISPLAY_NAMES.values():
            raise LiveShadowBackendError("SJD update target is not exact")
        return self._lro_json(
            "POST",
            f"/workspaces/{WORKSPACE_ID}/sparkJobDefinitions/"
            f"{item_id}/updateDefinition?format=SparkJobDefinitionV2",
            value=definition,
            accepted=(200, 202),
        )


class ShadowAzureOneLakeFiles(AzureOneLakeFiles):
    """Azure OneLake adapter confined to the fixed shadow controller root."""

    @staticmethod
    def _path(path: str) -> str:
        prefix = f"{PRODUCTION_SHADOW_FILES_ROOT}controller/"
        candidate = PurePosixPath(path)
        if (
            "\\" in path
            or "\x00" in path
            or candidate.is_absolute()
            or str(candidate) != path
            or not path.startswith(prefix)
            or ".." in candidate.parts
        ):
            raise LiveShadowBackendError(
                "OneLake path escaped the fixed shadow controller root"
            )
        return f"{LAKEHOUSE_ID}/{path}"

    def list_invocations(self) -> list[str]:
        """List fixed-root invocation directories for read-only diagnostics."""

        root = f"{LAKEHOUSE_ID}/{LIVE_ROOT}/invocations"
        result: list[str] = []
        for value in self._filesystem.get_paths(path=root, recursive=False):
            name = str(getattr(value, "name", ""))
            if not bool(getattr(value, "is_directory", False)):
                continue
            candidate = name.rsplit("/", 1)[-1]
            if candidate and "/" not in candidate and "\\" not in candidate:
                result.append(candidate)
        return sorted(result)


def _canonical(value: object) -> bytes:
    return canonical_bytes(value)


def _definition_body(value: Mapping[str, Any]) -> Mapping[str, Any]:
    body = value.get("definition")
    if not isinstance(body, Mapping):
        raise LiveShadowBackendError("SJD definition body is missing")
    return body


def _normalized_sjd_definition(value: Mapping[str, Any]) -> dict[str, object]:
    """Normalize Fabric's lossless SJD readback representation.

    Fabric reorders parts, reformats the metadata JSON, drops the request-only
    ``format`` member, and adds a generated ``.platform`` part.  The executable
    bytes and parsed fixed metadata remain authoritative.
    """

    parts = _definition_parts(value)
    required = {"Main/main.py", "SparkJobDefinitionV1.json"}
    if not required.issubset(parts) or set(parts) - required - {".platform"}:
        raise LiveShadowBackendError("SJD definition parts are not the fixed set")
    try:
        metadata = json.loads(parts["SparkJobDefinitionV1.json"])
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LiveShadowBackendError("SJD metadata is invalid") from error
    if not isinstance(metadata, Mapping):
        raise LiveShadowBackendError("SJD metadata is not an object")
    return {
        "main_sha256": hashlib.sha256(parts["Main/main.py"]).hexdigest(),
        "metadata": json.loads(_canonical(dict(metadata))),
    }


def _definition_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(_normalized_sjd_definition(value))).hexdigest()


def _parse_datetime(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise LiveShadowBackendError(f"{label} is not an ISO timestamp")
    try:
        result = datetime.fromisoformat(value)
    except ValueError as error:
        raise LiveShadowBackendError(f"{label} is invalid") from error
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


class FabricShadowControllerBackend:
    """Live implementation of the controller's fixed ``ShadowControllerBackend``.

    Every Spark interaction is an installed-wheel SJD command backed by a
    one-use signed request and create-only result/failure evidence.
    """

    def __init__(
        self,
        api: ShadowFabricREST,
        files: Any,
        *,
        selected_work_id: str | None = None,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        timeout: float = 3600.0,
        wheel_builder: Callable[[], tuple[bytes, str]] = build_wheel,
    ) -> None:
        self.api = api
        self.files = files
        self.selected_work_id = selected_work_id
        self.clock = clock
        self.monotonic = monotonic
        self.sleep = sleep
        self.timeout = timeout
        self.wheel_builder = wheel_builder
        self._snapshot: dict[str, Any] | None = None
        self._deployed: dict[str, Mapping[str, Any]] | None = None
        self._wheel_artifact: tuple[bytes, str] | None = None
        self._plan: ProductionShadowPlan | None = None
        self._receipt: ReviewReceipt | None = None
        self._source: LegacySourceSnapshot | None = None
        self._safety_token: str | None = None
        self._authorization: AuthorizationResult | None = None
        self._claim: dict[str, Any] | None = None
        self._processed = False
        self._synthetic_context: dict[str, Any] | None = None
        self._bootstrapped = False

    def prepare_plan_context(
        self,
        *,
        plan: ProductionShadowPlan,
        receipt: ReviewReceipt,
        safety_token: str,
        source: LegacySourceSnapshot,
    ) -> None:
        if source.work_id != plan.work.work_id or source.identity != plan.work:
            raise LiveShadowBackendError("plan context does not match legacy route")
        self.selected_work_id = plan.work.work_id
        self._plan = plan
        self._receipt = receipt
        self._source = source
        self._safety_token = safety_token

    def select_plan_for_status(self, plan: ProductionShadowPlan) -> None:
        """Bind read-only recovery status to one local canonical plan."""

        self._plan = plan
        self.selected_work_id = plan.work.work_id

    def artifact_binding(self) -> Mapping[str, str]:
        self._ensure_snapshot()
        return {
            "environment_id": ENVIRONMENT_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "migration_id": MIGRATION_ID,
            "workspace_id": WORKSPACE_ID,
        }

    def predeploy_snapshot(self) -> Mapping[str, Any]:
        """Return REST-only safety state without invoking Spark or OneLake."""

        return self._rest_snapshot()

    def _wheel(self) -> tuple[bytes, str]:
        if self._wheel_artifact is None:
            wheel, digest = self.wheel_builder()
            if hashlib.sha256(wheel).hexdigest() != digest:
                raise LiveShadowBackendError("wheel builder digest differs")
            self._wheel_artifact = wheel, digest
        return self._wheel_artifact

    @staticmethod
    def _source_digest(wheel: bytes) -> str:
        """Hash the exact non-metadata source payload carried by the wheel."""

        with zipfile.ZipFile(io.BytesIO(wheel)) as archive:
            entries = [
                name
                for name in archive.namelist()
                if ".dist-info/" not in name and not name.endswith("/")
            ]
            payload = [
                {
                    "path": name,
                    "sha256": hashlib.sha256(archive.read(name)).hexdigest(),
                }
                for name in sorted(entries)
            ]
        if not payload:
            raise LiveShadowBackendError("project wheel contains no source payload")
        return hashlib.sha256(_canonical(payload)).hexdigest()

    def fixed_deployment_spec(self) -> Mapping[str, Any]:
        wheel, wheel_sha = self._wheel()
        definitions = {
            job: {
                "display_name": display_name,
                "definition_sha256": _definition_sha256(
                    build_sjd_v2_definition(job)
                ),
            }
            for job, display_name in SJD_DISPLAY_NAMES.items()
        }
        return {
            "artifact_binding": self.artifact_binding_without_spark(),
            "definitions": definitions,
            "environment": {
                "project_wheel": PROJECT_WHEEL,
                "project_version": PACKAGE_VERSION,
                "public_libraries_preserved": True,
                "sole_custom_wheel": True,
            },
            "source_sha256": self._source_digest(wheel),
            "wheel_sha256": wheel_sha,
        }

    def shadow_deployment_state(self) -> Mapping[str, Any]:
        """Validate all fixed SJD presences/definitions using REST only."""

        items = self.api.list_items()
        jobs: dict[str, Any] = {}
        ready = True
        for job, display_name in SJD_DISPLAY_NAMES.items():
            matches = [
                item
                for item in items
                if item.get("displayName") == display_name
                and item.get("type") == "SparkJobDefinition"
            ]
            if len(matches) != 1 or not isinstance(matches[0].get("id"), str):
                jobs[job] = {
                    "display_name": display_name,
                    "item_id": None,
                    "state": "ABSENT" if not matches else "AMBIGUOUS",
                }
                ready = False
                continue
            item_id = str(matches[0]["id"])
            observed = self.api.get_definition("SparkJobDefinition", item_id)
            expected = build_sjd_v2_definition(job)
            exact = _normalized_sjd_definition(
                observed
            ) == _normalized_sjd_definition(expected)
            jobs[job] = {
                "definition_sha256": _definition_sha256(observed),
                "display_name": display_name,
                "item_id": item_id,
                "state": "READY" if exact else "DEFINITION_MISMATCH",
            }
            ready = ready and exact
        return {"jobs": jobs, "ready": ready}

    def _item_index(self) -> dict[str, Mapping[str, Any]]:
        items = self.api.list_items()
        indexed: dict[str, Mapping[str, Any]] = {}
        for item in items:
            identifier = item.get("id")
            if not isinstance(identifier, str) or not identifier or identifier in indexed:
                raise LiveShadowBackendError("workspace item IDs are ambiguous")
            indexed[identifier] = item
        expected = {
            ENVIRONMENT_ID: "Environment",
            LAKEHOUSE_ID: "Lakehouse",
            REFLEX_ID: "Reflex",
            **{identifier: "DataPipeline" for _, identifier in WRITER_ITEMS},
        }
        for identifier, item_type in expected.items():
            item = indexed.get(identifier)
            if item is None or item.get("type") != item_type:
                raise LiveShadowBackendError(
                    f"fixed {item_type} {identifier} is absent or different"
                )
        for name, identifier in WRITER_ITEMS:
            if indexed[identifier].get("displayName") != name:
                raise LiveShadowBackendError("fixed pipeline display name differs")
        return indexed

    def _rest_snapshot(self) -> dict[str, Any]:
        indexed = self._item_index()
        environment = self.api.environment_state()
        reflex_definition = self.api.get_definition("Reflex", REFLEX_ID)
        reflex_parts = _definition_parts(reflex_definition)
        try:
            reflex_raw = reflex_parts["ReflexEntities.json"]
        except KeyError as error:
            raise LiveShadowBackendError(
                "exact Reflex definition part is missing"
            ) from error
        reflex_rule = parse_reflex_rule_definition(reflex_raw)
        reflex_active = reflex_rule.enabled
        pipelines = []
        for name, identifier in WRITER_ITEMS:
            schedules = self.api.list_schedules(identifier, "Pipeline")
            jobs = self.api.list_job_instances(identifier)
            pipelines.append(
                {
                    "id": identifier,
                    "display_name": name,
                    "definition_sha256": hashlib.sha256(
                        _canonical(
                            self.api.get_definition("DataPipeline", identifier)
                        )
                    ).hexdigest(),
                    "schedules": schedules,
                    "active_jobs": _active_jobs_exact(jobs),
                }
            )
        sjds = []
        for job, display_name in SJD_DISPLAY_NAMES.items():
            matches = [
                value
                for value in indexed.values()
                if value.get("displayName") == display_name
                and value.get("type") == "SparkJobDefinition"
            ]
            if len(matches) > 1:
                raise LiveShadowBackendError("fixed SJD display name is duplicated")
            if not matches:
                sjds.append({"job": job, "display_name": display_name, "id": None})
                continue
            item_id = str(matches[0]["id"])
            definition = self.api.get_definition("SparkJobDefinition", item_id)
            sjds.append(
                {
                    "job": job,
                    "display_name": display_name,
                    "id": item_id,
                    "definition_sha256": hashlib.sha256(
                        _canonical(definition)
                    ).hexdigest(),
                    "schedules": self.api.list_schedules(item_id, "sparkjob"),
                    "active_jobs": _active_jobs_exact(
                        self.api.list_job_instances(item_id)
                    ),
                }
            )
        return {
            "captured_at": datetime.fromtimestamp(
                self.clock(), timezone.utc
            ).isoformat(),
            "environment": environment,
            "reflex": {
                "id": REFLEX_ID,
                "display_name": indexed[REFLEX_ID].get("displayName"),
                "active": reflex_active,
                "definition_sha256": hashlib.sha256(
                    _canonical(reflex_definition)
                ).hexdigest(),
                "rule": reflex_rule.to_dict(),
            },
            "pipelines": pipelines,
            "sjds": sjds,
        }

    def _sjd_id(self, job: str) -> str:
        if job not in SJD_DISPLAY_NAMES:
            raise LiveShadowBackendError("SJD job is outside the fixed set")
        matches = [
            item
            for item in self.api.list_items()
            if item.get("displayName") == SJD_DISPLAY_NAMES[job]
            and item.get("type") == "SparkJobDefinition"
        ]
        if len(matches) != 1 or not isinstance(matches[0].get("id"), str):
            raise LiveShadowBackendError(
                f"exactly one deployed {job} shadow SJD is required"
            )
        item_id = str(matches[0]["id"])
        observed = self.api.get_definition("SparkJobDefinition", item_id)
        expected = build_sjd_v2_definition(job)
        if _normalized_sjd_definition(observed) != _normalized_sjd_definition(
            expected
        ):
            raise LiveShadowBackendError(
                f"{job} SJD definition differs from installed-wheel export"
            )
        return item_id

    def _create_request(
        self,
        command: str,
        payload: Mapping[str, Any],
    ) -> tuple[str, bytes]:
        invocation = uuid.uuid4().hex
        key = secrets.token_bytes(32)
        value = {
            "schema": LIVE_SCHEMA,
            "invocation_id": invocation,
            "command": command,
            "artifact_binding": self.artifact_binding_without_spark(),
            "created_at": datetime.fromtimestamp(
                self.clock(), timezone.utc
            ).isoformat(),
            "rest_snapshot": self._rest_snapshot(),
            **deepcopy(dict(payload)),
        }
        encoded = _canonical(value)
        envelope = {
            "algorithm": "HMAC-SHA256",
            "payload": json.loads(encoded),
            "payload_sha256": hashlib.sha256(encoded).hexdigest(),
            "signature": hmac.new(key, encoded, hashlib.sha256).hexdigest(),
        }
        content = _canonical(envelope) + b"\n"
        path = request_path(invocation)
        if self.files.exists(path):
            raise LiveShadowBackendError("random invocation path already exists")
        self.files.create_bytes(path, content)
        if self.files.read_bytes(path) != content:
            raise LiveShadowBackendError("signed request readback differs")
        return invocation, key

    @staticmethod
    def artifact_binding_without_spark() -> dict[str, str]:
        return {
            "workspace_id": WORKSPACE_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "environment_id": ENVIRONMENT_ID,
            "migration_id": MIGRATION_ID,
        }

    def _invoke(
        self,
        job: str,
        command: str,
        payload: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        item_id = self._sjd_id(job)
        invocation, key = self._create_request(command, payload or {})
        arguments = shlex.join(
            [
                command,
                "--invocation-id",
                invocation,
                "--request-hmac-key",
                key.hex(),
            ]
        )
        instance = self.api.run_sjd(item_id, arguments)
        poll_error: BaseException | None = None
        try:
            state = _poll_job(
                self.api,
                item_id,
                instance,
                clock=self.monotonic,
                sleep=self.sleep,
                timeout=self.timeout,
                hidden=(key.hex(),),
            )
        except BaseException as error:
            poll_error = error
            state = {}
        path = result_path(invocation)
        if self.files.exists(path):
            value = json.loads(self.files.read_bytes(path))
            if (
                not isinstance(value, Mapping)
                or value.get("schema") != RESULT_SCHEMA
                or value.get("command") != command
                or value.get("invocation_id") != invocation
                or value.get("exit_code") != 0
                or not isinstance(value.get("result"), Mapping)
            ):
                raise LiveShadowBackendError("Spark result evidence is nonzero or invalid")
            if poll_error is not None:
                raise LiveShadowBackendError(
                    "Fabric job failed despite result evidence"
                ) from poll_error
            self._snapshot = None
            return dict(value["result"])
        failure = failure_path(invocation)
        diagnostic: object = None
        if self.files.exists(failure):
            recovered = json.loads(self.files.read_bytes(failure))
            if isinstance(recovered, Mapping) and recovered.get("schema") == FAILURE_SCHEMA:
                diagnostic = {
                    "error_type": recovered.get("error_type"),
                    "error_sha256": recovered.get("error_sha256"),
                }
        if poll_error is not None:
            raise LiveShadowBackendError(
                f"shadow SJD failed; recovered={diagnostic!r}"
            ) from poll_error
        raise LiveShadowBackendError(
            f"shadow SJD produced no result evidence; state={state!r}; "
            f"recovered={diagnostic!r}"
        )

    def _ensure_snapshot(self, *, refresh: bool = False) -> dict[str, Any]:
        if self._snapshot is None or refresh:
            payload: dict[str, Any] = {}
            if self.selected_work_id is not None:
                payload["selected_work_id"] = self.selected_work_id
            if self._authorization is not None:
                payload["authorization_id"] = (
                    self._authorization.row.authorization_id
                )
            self._snapshot = dict(self._invoke("control", "snapshot", payload))
        return self._snapshot

    def _selected_route(self) -> Mapping[str, Any]:
        routes = self._ensure_snapshot().get("eligible_routes")
        if not isinstance(routes, list):
            raise LiveShadowBackendError("Spark eligible-route inventory is missing")
        candidates = [
            item for item in routes
            if isinstance(item, Mapping)
            and (
                self.selected_work_id is None
                or item.get("work_id") == self.selected_work_id
            )
        ]
        if len(candidates) != 1:
            raise LiveShadowBackendError(
                "select exactly one eligible route with --work-id"
            )
        self.selected_work_id = str(candidates[0]["work_id"])
        return candidates[0]

    def eligible_legacy_rows(self) -> Sequence[LegacySourceRows]:
        routes = self._ensure_snapshot().get("eligible_routes")
        if not isinstance(routes, list):
            raise LiveShadowBackendError("eligible-route inventory is missing")
        result = []
        for route in routes:
            if not isinstance(route, Mapping) or not isinstance(route.get("rows"), Mapping):
                raise LiveShadowBackendError("eligible route evidence is invalid")
            rows = route["rows"]
            result.append(
                LegacySourceRows(
                    work=dict(rows["work"]),
                    attempt=dict(rows["attempt"]),
                    publication=dict(rows["publication"]),
                    committed_view=dict(rows["committed_view"]),
                )
            )
        return result

    def route_diagnostics(self) -> Sequence[Mapping[str, Any]]:
        value = self._ensure_snapshot().get("route_diagnostics")
        if not isinstance(value, list):
            raise LiveShadowBackendError("route diagnostics are missing")
        diagnostics: list[Mapping[str, Any]] = []
        for item in value:
            if (
                not isinstance(item, Mapping)
                or not isinstance(item.get("candidate_id"), str)
                or not isinstance(item.get("eligible"), bool)
                or not isinstance(item.get("rejection_codes"), list)
                or not isinstance(item.get("fields"), Mapping)
                or not isinstance(item.get("joins"), Mapping)
                or any(
                    not isinstance(observed, bool)
                    for section in (item["fields"], item["joins"])
                    for observed in section.values()
                )
                or any(
                    not isinstance(code, str)
                    for code in item["rejection_codes"]
                )
            ):
                raise LiveShadowBackendError(
                    "route diagnostics are not redaction-safe"
                )
            diagnostics.append(dict(item))
        return diagnostics

    def synthetic_candidate(self) -> Mapping[str, Any]:
        value = self._ensure_snapshot().get("synthetic_candidate")
        if (
            not isinstance(value, Mapping)
            or value.get("comparison_mode") != "NO_LEGACY_BASELINE"
            or not isinstance(value.get("identity"), Mapping)
            or not isinstance(value.get("payload"), Mapping)
            or not isinstance(value.get("evidence"), Mapping)
            or value["evidence"].get("source_bytes_verified") is not True
            or value["evidence"].get("config_canonical_verified") is not True
            or value["evidence"].get("model_bytes_verified") is not True
        ):
            raise LiveShadowBackendError(
                "byte-verified synthetic candidate is unavailable"
            )
        return dict(value)

    def configure_synthetic(
        self,
        plan: SyntheticShadowPlan,
        *,
        reviewed_at: datetime,
        reviewer: str,
    ) -> None:
        self.selected_work_id = plan.work.work_id
        self._plan = plan  # type: ignore[assignment]
        self._synthetic_context = {
            "authorization_id": hashlib.sha256(
                _canonical(
                    {
                        "comparison_mode": plan.comparison_mode,
                        "plan_sha256": plan.sha256,
                        "work_identity_sha256": plan.identity_sha256,
                    }
                )
            ).hexdigest(),
            "comparison_mode": plan.comparison_mode,
            "plan_sha256": plan.sha256,
            "source_evidence_sha256": plan.source_evidence_sha256,
            "identity": plan.work.as_dict(),
            "reviewed_at": reviewed_at.isoformat(),
            "reviewer": reviewer,
        }

    def read_legacy_rows(self) -> LegacySourceRows:
        rows = self._selected_route().get("rows")
        if not isinstance(rows, Mapping):
            raise LiveShadowBackendError("selected route rows are missing")
        return LegacySourceRows(
            work=dict(rows["work"]),
            attempt=dict(rows["attempt"]),
            publication=dict(rows["publication"]),
            committed_view=dict(rows["committed_view"]),
        )

    def sjd_state(self, display_name: str) -> Mapping[str, Any] | None:
        if display_name not in SJD_DISPLAY_NAMES.values():
            raise LiveShadowBackendError("SJD name is outside the fixed set")
        matches = [
            item
            for item in self.api.list_items()
            if item.get("displayName") == display_name
            and item.get("type") == "SparkJobDefinition"
        ]
        if not matches:
            return None
        if len(matches) != 1:
            raise LiveShadowBackendError("fixed SJD display name is duplicated")
        item_id = str(matches[0]["id"])
        return {
            "display_name": display_name,
            "item_id": item_id,
            "definition": self.api.get_definition(
                "SparkJobDefinition", item_id
            ),
            "schedules": self.api.list_schedules(item_id, "sparkjob"),
            "active_jobs": _active_jobs_exact(
                self.api.list_job_instances(item_id)
            ),
        }

    def migration_proof(self) -> MigrationJournalProof:
        value = self._ensure_snapshot()["migration"]
        return MigrationJournalProof(
            str(value["migration_id"]),
            str(value["status"]),
            str(value["plan_sha256"]),
            str(value["journal_sha256"]),
        )

    def quiescence(self) -> ShadowQuiescence:
        value = self._ensure_snapshot(refresh=True)["quiescence"]
        return ShadowQuiescence(
            _parse_datetime(value["observed_at"], "quiescence observed_at"),
            str(value["reflex_id"]),
            bool(value["reflex_active"]),
            tuple(str(item) for item in value.get("active_writer_ids", [])),
            tuple(str(item) for item in value.get("active_lease_ids", [])),
            (
                None
                if value.get("control_owner_id") is None
                else str(value["control_owner_id"])
            ),
        )

    def status(self) -> Mapping[str, Any]:
        deployment = self.shadow_deployment_state()
        if deployment.get("ready") is not True:
            return {
                "invocation_diagnostics": self._invocation_diagnostics(),
                "predeployment_state": deployment,
            }
        payload: dict[str, Any] = {}
        if self.selected_work_id is not None:
            payload["selected_work_id"] = self.selected_work_id
        if self._plan is not None:
            payload["authorization_id"] = hashlib.sha256(
                _canonical(
                    {
                        "plan_sha256": self._plan.sha256,
                        "work_id": self._plan.work.work_id,
                        "work_identity_sha256": self._plan.identity_sha256,
                        "expires_at": self._plan.expires_at.isoformat(),
                    }
                )
            ).hexdigest()
        result = dict(self._invoke("control", "status", payload))
        result["invocation_diagnostics"] = self._invocation_diagnostics()
        return result

    def _invocation_diagnostics(self) -> list[dict[str, str]]:
        list_invocations = getattr(self.files, "list_invocations", None)
        if not callable(list_invocations):
            return []
        result = []
        for invocation in list_invocations():
            identifier = str(invocation)
            started = self.files.exists(started_path(identifier))
            completed = self.files.exists(result_path(identifier))
            failed = self.files.exists(failure_path(identifier))
            if not started and not completed and not failed:
                classification = "UNSTARTED_DIAGNOSTIC"
            elif completed:
                classification = "COMPLETED"
            elif failed:
                classification = "FAILED"
            else:
                classification = "STARTED"
            result.append(
                {
                    "classification": classification,
                    "invocation_id": identifier,
                }
            )
        return sorted(result, key=lambda value: value["invocation_id"])

    def _deploy_all(self) -> dict[str, Mapping[str, Any]]:
        if self._deployed is not None:
            return self._deployed
        wheel, wheel_sha = self._wheel()
        state = self.api.environment_state()
        _verify_environment_policy(state, deployed=False)
        public_before = _environment_public_packages(state)
        staged = _environment_custom_wheels(state, "staged_libraries")
        # Fabric exposes custom-library names but not the published wheel
        # content hash.  A matching versioned filename therefore cannot prove
        # that the plan-bound bytes are installed.  Replace the staged project
        # wheel and publish on every fixed deployment/replay.
        for name in staged:
            self.api.delete_staged_environment_library(name)
        self.api.upload_environment_wheel(PROJECT_WHEEL, wheel)
        self.api.publish_environment()
        after = self.api.environment_state()
        _verify_environment_policy(after, deployed=True)
        if _environment_public_packages(after) != public_before:
            raise LiveShadowBackendError(
                "Environment public packages changed during deployment"
            )
        deployed: dict[str, Mapping[str, Any]] = {}
        items = self.api.list_items()
        for job, display_name in SJD_DISPLAY_NAMES.items():
            definition = build_sjd_v2_definition(job)
            matches = [
                item
                for item in items
                if item.get("displayName") == display_name
                and item.get("type") == "SparkJobDefinition"
            ]
            if len(matches) > 1:
                raise LiveShadowBackendError("fixed SJD name is duplicated")
            if matches:
                item_id = str(matches[0]["id"])
                self.api.update_shadow_sjd(
                    item_id, display_name, definition
                )
            else:
                created = self.api.create_shadow_sjd(display_name, definition)
                item_id = str(created.get("id", ""))
            if not item_id:
                raise LiveShadowBackendError("SJD deployment returned no item ID")
            readback = self.api.get_definition(
                "SparkJobDefinition", item_id
            )
            if _normalized_sjd_definition(
                readback
            ) != _normalized_sjd_definition(definition):
                raise LiveShadowBackendError("SJD deployment readback differs")
            deployed[display_name] = {
                "display_name": display_name,
                "item_id": item_id,
                "definition": readback,
                "wheel_sha256": wheel_sha,
            }
        self._deployed = deployed
        return deployed

    def deploy_fixed_definition(
        self, plan: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        """Execute only the wheel publish and three fixed SJD upserts."""

        if plan.get("operation") != (
            "publish-project-wheel-and-upsert-three-shadow-sjds"
        ):
            raise LiveShadowBackendError("deployment plan operation differs")
        if plan.get("artifact_binding") != self.artifact_binding_without_spark():
            raise LiveShadowBackendError("deployment artifact binding differs")
        spec = plan.get("spec")
        if not isinstance(spec, Mapping) or dict(spec) != dict(
            self.fixed_deployment_spec()
        ):
            raise LiveShadowBackendError("deployment source, wheel, or SJD hashes differ")
        rest = self._rest_snapshot()
        if hashlib.sha256(_canonical(rest["environment"])).hexdigest() != plan.get(
            "environment_policy_sha256"
        ):
            raise LiveShadowBackendError("Environment policy changed after predeploy")
        if bool(rest["reflex"].get("active")) or any(
            value.get("active_jobs") for value in rest["pipelines"]
        ):
            raise LiveShadowBackendError("writers became active after predeploy")
        deployed = self._deploy_all()
        state = self.shadow_deployment_state()
        if state.get("ready") is not True:
            raise LiveShadowBackendError("fixed SJD deployment readback is not ready")
        return {
            "environment": {
                "project_wheel": PROJECT_WHEEL,
                "wheel_sha256": spec["wheel_sha256"],
            },
            "sjds": [
                {
                    "definition_sha256": spec["definitions"][job][
                        "definition_sha256"
                    ],
                    "display_name": display_name,
                    "item_id": deployed[display_name]["item_id"],
                    "job": job,
                }
                for job, display_name in SJD_DISPLAY_NAMES.items()
            ],
        }

    def deploy_sjd(
        self, display_name: str, definition: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        job = next(
            (key for key, value in SJD_DISPLAY_NAMES.items() if value == display_name),
            None,
        )
        if job is None or _definition_body(definition) != _definition_body(
            build_sjd_v2_definition(job)
        ):
            raise LiveShadowBackendError("deployment input is not a fixed SJD")
        return self._deploy_all()[display_name]

    def _authorization_rows(
        self, authorization_id: str | None = None
    ) -> Mapping[str, Any]:
        payload: dict[str, Any] = {}
        if self.selected_work_id is not None:
            payload["selected_work_id"] = self.selected_work_id
        if authorization_id is not None:
            payload["authorization_id"] = authorization_id
        return self._invoke("control", "snapshot", payload)["authorization"]

    def read_allowlist(self, work_id: str) -> Sequence[Mapping[str, Any]]:
        if self.selected_work_id not in {None, work_id}:
            raise LiveShadowBackendError("allowlist read is not plan-pinned")
        self.selected_work_id = work_id
        return list(self._authorization_rows().get("allowlist", []))

    def read_audit(
        self, authorization_id: str
    ) -> Sequence[Mapping[str, Any]]:
        return list(
            self._authorization_rows(authorization_id).get("audit", [])
        )

    def append_allowlist(self, row: Mapping[str, Any]) -> None:
        raise LiveShadowBackendError(
            "standalone allowlist append is forbidden; use serialized protocol"
        )

    def append_audit(self, row: Mapping[str, Any]) -> None:
        raise LiveShadowBackendError(
            "standalone audit append is forbidden; use serialized protocol"
        )

    def append_authorization(
        self,
        allowlist: Mapping[str, Any],
        audit: Mapping[str, Any],
    ) -> None:
        if not all(
            value is not None
            for value in (
                self._plan,
                self._receipt,
                self._source,
                self._safety_token,
            )
        ):
            raise LiveShadowBackendError("reviewed authorization context is missing")
        plan = self._plan
        receipt = self._receipt
        source = self._source
        token = self._safety_token
        assert plan is not None and receipt is not None
        assert source is not None and token is not None
        row = AuthorizationRow(
            authorization_id=str(audit["audit_id"]),
            plan_sha256=plan.sha256,
            work=plan.work,
            work_identity_sha256=plan.identity_sha256,
            expires_at=plan.expires_at,
            authorized_at=receipt.reviewed_at,
            safety_token_sha256=hashlib.sha256(token.encode()).hexdigest(),
            migration_journal_sha256=self.migration_proof().journal_sha256,
            reviewer=receipt.reviewer,
        )
        intent = authorization_intent(row, allowlist, audit)
        result = self._invoke(
            "control",
            "authorize",
            {
                "selected_work_id": plan.work.work_id,
                "authorization_id": row.authorization_id,
                "authorization": {
                    "row": row.as_dict(),
                    "work": plan.work.as_dict(),
                    "allowlist": dict(allowlist),
                    "audit": dict(audit),
                    "intent": intent.as_dict(),
                    "migration_plan_sha256": plan.migration_plan_sha256,
                    "legacy_source_rows_sha256": source.source_rows_sha256,
                    "legacy_output_sha256": source.output_sha256,
                    "expires_at": plan.expires_at.isoformat(),
                    "receipt_reviewed_at": receipt.reviewed_at.isoformat(),
                },
            },
        )
        if result.get("protocol_state") != "COMMITTED":
            raise LiveShadowBackendError("authorization protocol did not commit")
        self._authorization = AuthorizationResult(row, True, True)

    def recover_partial_authorization(
        self,
        row: AuthorizationRow,
        allowlist: Mapping[str, Any],
        audit: Mapping[str, Any],
    ) -> None:
        """Replay the signed protocol; Spark verifies the immutable intent."""

        self.append_authorization(allowlist, audit)

    def authorization_result(self, plan_sha256: str) -> AuthorizationResult:
        if self._authorization is not None:
            if self._authorization.row.plan_sha256 != plan_sha256:
                raise LiveShadowBackendError("authorization plan hash differs")
            return self._authorization
        if self._plan is None or self._receipt is None or self._safety_token is None:
            raise LiveShadowBackendError("reviewed plan context is missing")
        plan = self._plan
        authorization_id = hashlib.sha256(
            _canonical(
                {
                    "plan_sha256": plan.sha256,
                    "work_id": plan.work.work_id,
                    "work_identity_sha256": plan.identity_sha256,
                    "expires_at": plan.expires_at.isoformat(),
                }
            )
        ).hexdigest()
        allow = self.read_allowlist(plan.work.work_id)
        audit = self.read_audit(authorization_id)
        if len(allow) != 1 or len(audit) != 1:
            raise LiveShadowBackendError(
                "authorization is missing, partial, or ambiguous"
            )
        row = AuthorizationRow(
            authorization_id=authorization_id,
            plan_sha256=plan.sha256,
            work=plan.work,
            work_identity_sha256=plan.identity_sha256,
            expires_at=plan.expires_at,
            authorized_at=self._receipt.reviewed_at,
            safety_token_sha256=hashlib.sha256(
                self._safety_token.encode()
            ).hexdigest(),
            migration_journal_sha256=self.migration_proof().journal_sha256,
            reviewer=self._receipt.reviewer,
        )
        expected_allowlist = {
            **row.work.as_dict(),
            "plan_sha256": row.plan_sha256,
            "approved_at": row.authorized_at.isoformat(),
            "approved_by": row.reviewer,
        }
        expected_audit = {
            "audit_id": row.authorization_id,
            "work_id": row.work.work_id,
            "plan_sha256": row.plan_sha256,
            "shadow_attempt_id": "AUTHORIZATION",
            "legacy_attempt_id": row.work_identity_sha256,
            "comparison_sha256": row.safety_token_sha256,
            "critical_findings": 0,
            "recorded_at": row.authorized_at.isoformat(),
        }
        if dict(allow[0]) != expected_allowlist or dict(audit[0]) != expected_audit:
            raise LiveShadowBackendError(
                "stored authorization rows conflict with reviewed plan"
            )
        expected_intent = authorization_intent(
            row, expected_allowlist, expected_audit
        )
        committed = (
            f"{PRODUCTION_SHADOW_FILES_ROOT}controller/live/authorization/"
            f"{authorization_id}/30-committed.json"
        )
        if not self.files.exists(committed):
            raise LiveShadowBackendError(
                "committed authorization intent evidence is missing"
            )
        evidence = json.loads(self.files.read_bytes(committed))
        if (
            not isinstance(evidence, Mapping)
            or evidence.get("state") != "COMMITTED"
            or evidence.get("intent") != expected_intent.as_dict()
        ):
            raise LiveShadowBackendError(
                "committed authorization intent conflicts"
            )
        self._authorization = AuthorizationResult(row, False, False)
        return self._authorization

    def table_schema(
        self, table_name: str
    ) -> Sequence[tuple[str, str, bool]] | None:
        config = FabricCandidateAConfig.production_shadow()
        suffix = next(
            (name for name in SHADOW_SCHEMAS if config.table(name) == table_name),
            None,
        )
        if suffix is None:
            raise LiveShadowBackendError("schema read is outside fixed shadow tables")
        return SHADOW_SCHEMAS[suffix] if self._bootstrapped else None

    def create_table(
        self,
        table_name: str,
        schema: Sequence[tuple[str, str, bool]],
    ) -> None:
        config = FabricCandidateAConfig.production_shadow()
        expected = {
            config.table(name): value for name, value in SHADOW_SCHEMAS.items()
        }
        if table_name not in expected or tuple(schema) != tuple(expected[table_name]):
            raise LiveShadowBackendError("table create is outside fixed schemas")
        if not self._bootstrapped:
            result = self._invoke("control", "bootstrap")
            if sorted(result.get("tables", [])) != sorted(expected):
                raise LiveShadowBackendError("shadow bootstrap readback differs")
            self._bootstrapped = True

    def runtime_provenance(self) -> RuntimeProvenance:
        wheel, digest = self.wheel_builder()
        if hashlib.sha256(wheel).hexdigest() != digest:
            raise LiveShadowBackendError("wheel digest differs")
        return RuntimeProvenance(
            PACKAGE_VERSION,
            digest,
            "3.13",
            "4.1.1",
            "21",
        )

    def _route_context(self) -> dict[str, Any]:
        if self._plan is None:
            raise LiveShadowBackendError("reviewed route context is missing")
        provenance = self.runtime_provenance()
        if self._synthetic_context is not None:
            return {
                **self._synthetic_context,
                "route_mode": "SHADOW_SYNTHETIC",
                "provenance": {
                    "package_version": provenance.package_version,
                    "package_sha256": provenance.package_sha256,
                    "python_version": provenance.python_version,
                    "spark_version": provenance.spark_version,
                    "java_version": provenance.java_version,
                    "fabric_runtime": provenance.fabric_runtime,
                },
            }
        authorization = self.authorization_result(self._plan.sha256).row
        return {
            "authorization_id": authorization.authorization_id,
            "plan_sha256": authorization.plan_sha256,
            "identity": authorization.work.as_dict(),
            "provenance": {
                "package_version": provenance.package_version,
                "package_sha256": provenance.package_sha256,
                "python_version": provenance.python_version,
                "spark_version": provenance.spark_version,
                "java_version": provenance.java_version,
                "fabric_runtime": provenance.fabric_runtime,
            },
        }

    @staticmethod
    def _committed(value: Mapping[str, Any]) -> CommittedRoute:
        if "route" in value and value.get("route") is None:
            raise LiveShadowBackendError("committed route is absent")
        route = value
        identity = AllowlistRow(**dict(route["identity"]))
        provenance = RuntimeProvenance(**dict(route["provenance"]))
        return CommittedRoute(
            work_id=str(route["work_id"]),
            attempt_id=str(route["attempt_id"]),
            logical_identity_sha256=str(route["logical_identity_sha256"]),
            fence=int(route["fence"]),
            pointer_fence=int(route["pointer_fence"]),
            output_path=str(route["output_path"]),
            output_sha256=str(route["output_sha256"]),
            sealed=bool(route["sealed"]),
            committed=bool(route["committed"]),
            pointer_attempt_id=str(route["pointer_attempt_id"]),
            publication_sequence=int(route["publication_sequence"]),
            publication_count=int(route["publication_count"]),
            authorization_id=str(route["authorization_id"]),
            plan_sha256=str(route["plan_sha256"]),
            provenance=provenance,
            identity=identity,
            records=tuple(dict(item) for item in route["records"]),
            logical_total=float(route["logical_total"]),
            frame_count=int(route["frame_count"]),
            timestamp=_parse_datetime(route["timestamp"], "route timestamp"),
        )

    def _read_route(self, kind: str) -> Mapping[str, Any]:
        if self.selected_work_id is None:
            raise LiveShadowBackendError("route is not plan-pinned")
        return self._invoke(
            "control",
            "route",
            {
                "selected_work_id": self.selected_work_id,
                "route_kind": kind,
                "route_context": self._route_context(),
            },
        )

    def committed_route(
        self, work_id: str, *, config: Any
    ) -> CommittedRoute | None:
        config.require_mode("PRODUCTION_SHADOW")
        if work_id != self.selected_work_id:
            raise LiveShadowBackendError("route read silently reselected work")
        value = self._read_route("shadow")
        if "route" in value and value.get("route") is None:
            return None
        return self._committed(value)

    def register(self, registration: Mapping[str, Any], *, config: Any) -> None:
        config.require_mode("PRODUCTION_SHADOW")
        if registration.get("work_id") != self.selected_work_id:
            raise LiveShadowBackendError("registration is not plan-pinned")
        self._invoke(
            "control",
            "register",
            {
                "selected_work_id": self.selected_work_id,
                "registration": dict(registration),
            },
        )

    def claim(self, work_id: str, *, config: Any) -> Mapping[str, Any]:
        config.require_mode("PRODUCTION_SHADOW")
        if work_id != self.selected_work_id:
            raise LiveShadowBackendError("claim is not exact plan work")
        claimed = dict(
            self._invoke(
                "control",
                "claim",
                {"selected_work_id": work_id, "work_id": work_id},
            )
        )
        items = claimed.get("items")
        if not isinstance(items, list) or len(items) != 1:
            raise LiveShadowBackendError("claim does not contain exactly one work")
        claimed["work_id"] = str(items[0]["work_id"])
        self._claim = claimed
        return claimed

    def process(
        self,
        claim: Mapping[str, Any],
        *,
        config: Any,
        provenance: RuntimeProvenance,
    ) -> Mapping[str, Any]:
        config.require_mode("PRODUCTION_SHADOW")
        if claim.get("work_id") != self.selected_work_id:
            raise LiveShadowBackendError("process claim is not plan-pinned")
        batch_id = str(claim.get("batch_id"))
        result = self._invoke(
            "process",
            "process",
            {
                "selected_work_id": self.selected_work_id,
                "batch_id": batch_id,
                "route_context": self._route_context(),
            },
        )
        if result.get("processed") is not True:
            raise LiveShadowBackendError("SDK process did not report success")
        self._processed = True
        return {"batch_id": batch_id, "work_id": self.selected_work_id}

    def recover_exact(
        self, recovery: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        """Invoke the fixed shadow-only exact recovery operation."""

        return self._invoke(
            "control", "recover-exact", {"recovery": dict(recovery)}
        )

    def seal(
        self,
        claim: Mapping[str, Any],
        output: Mapping[str, Any],
        *,
        config: Any,
    ) -> None:
        config.require_mode("PRODUCTION_SHADOW")
        if (
            not self._processed
            or claim.get("batch_id") != output.get("batch_id")
            or claim.get("work_id") != self.selected_work_id
        ):
            raise LiveShadowBackendError("process/seal readback differs")

    def publish(
        self,
        claim: Mapping[str, Any],
        *,
        authorization: Any,
        config: Any,
        provenance: RuntimeProvenance,
    ) -> CommittedRoute:
        config.require_mode("PRODUCTION_SHADOW")
        route = self._read_route("shadow")
        return self._committed(route)

    def legacy_route(self, work_id: str) -> CommittedRoute:
        if work_id != self.selected_work_id:
            raise LiveShadowBackendError("legacy route is not plan-pinned")
        return self._committed(self._read_route("legacy"))

    def shadow_route(self, work_id: str) -> CommittedRoute:
        if work_id != self.selected_work_id:
            raise LiveShadowBackendError("shadow route is not plan-pinned")
        return self._committed(self._read_route("shadow"))

    def append_reconciliation(self, row: Mapping[str, Any]) -> None:
        result = self._invoke(
            "reconcile",
            "reconcile",
            {
                "selected_work_id": self.selected_work_id,
                "reconciliation": dict(row),
            },
        )
        if result.get("reconciled") is not True:
            raise LiveShadowBackendError("reconcile SJD did not succeed")
