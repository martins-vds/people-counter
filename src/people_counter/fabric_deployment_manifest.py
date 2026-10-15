"""Typed deployment manifests for Fabric environments."""

from __future__ import annotations

import dataclasses
import json
import os
import re
import uuid
from collections.abc import Mapping
from enum import Enum
from pathlib import Path
from typing import Any

from people_counter.fabric_sjd import (
    ENVIRONMENT_ID,
    FABRIC_RUNTIME,
    FILES_ROOT,
    LAKEHOUSE_ID,
    TABLE_PREFIX,
    TABLE_SUFFIXES,
    WORKSPACE_ID,
)
from people_counter.fabric_sjd_definition import ENTRY_POINTS


MANIFEST_SCHEMA = "people-counter-deployment-manifest-v1"
_ENV_PREFIX = "PEOPLE_COUNTER_DEPLOY_"
_DATA_DIR = Path(__file__).resolve().parent / "data" / "deploy"
_SAFE_PATH_SEGMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._=-]*\Z")
_SAFE_TABLE_PREFIX = re.compile(r"[A-Za-z][A-Za-z0-9_]*_\Z")
_FORBIDDEN_KEY_PARTS = (
    "secret",
    "password",
    "token",
    "credential",
    "connectionstring",
    "apikey",
    "api_key",
)

SJD_ROLES = frozenset(ENTRY_POINTS)
SCHEDULED_ROLES = frozenset(
    {"control", "dispatcher", "reconciliation", "gold", "refresh"}
)
ON_DEMAND_ROLES = frozenset({"process", "cutover"})

if SCHEDULED_ROLES | ON_DEMAND_ROLES != SJD_ROLES:
    raise RuntimeError("SJD role partition does not match ENTRY_POINTS")
if SCHEDULED_ROLES & ON_DEMAND_ROLES:
    raise RuntimeError("SJD roles cannot be scheduled and on demand")


class ManifestValidationError(ValueError):
    """The deployment manifest is malformed or inconsistent."""


class ManifestLoadError(RuntimeError):
    """A deployment manifest document could not be loaded."""


class DeploymentEnvironment(str, Enum):
    """Supported deployment environments."""

    DEV = "dev"
    TEST = "test"
    PROD = "prod"


@dataclasses.dataclass(frozen=True, slots=True)
class ScheduleSpec:
    interval_minutes: int
    enabled: bool


@dataclasses.dataclass(frozen=True, slots=True)
class RoleBinding:
    role: str
    display_name: str
    item_id: str | None
    schedule: ScheduleSpec | None


@dataclasses.dataclass(frozen=True, slots=True)
class LakehouseBinding:
    lakehouse_id: str | None
    table_prefix: str
    files_root: str

    def table(self, suffix: str) -> str:
        if suffix not in TABLE_SUFFIXES:
            raise ManifestValidationError(f"invalid deployment table suffix {suffix!r}")
        return f"{self.table_prefix}{suffix}"

    def validate_table_name(self, table_name: str) -> str:
        if table_name not in {self.table(suffix) for suffix in TABLE_SUFFIXES}:
            raise ManifestValidationError(
                f"table is outside this environment's namespace: {table_name!r}"
            )
        return table_name

    def file_path(self, relative: str) -> str:
        parts = relative.split("/")
        if (
            not relative
            or relative.startswith("/")
            or "\\" in relative
            or any(
                part in {"", ".", ".."} or _SAFE_PATH_SEGMENT.fullmatch(part) is None
                for part in parts
            )
        ):
            raise ManifestValidationError(
                f"unsafe deployment relative path {relative!r}"
            )
        return f"{self.files_root}{relative}"

    def validate_files_path(self, path: str) -> str:
        if not path.startswith(self.files_root):
            raise ManifestValidationError(
                f"path is outside this environment's namespace: {path!r}"
            )
        return self.file_path(path.removeprefix(self.files_root))


@dataclasses.dataclass(frozen=True, slots=True)
class ArtifactRef:
    source_item_id: str | None
    source_name: str
    target_name: str
    target_item_id: str | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class ReportingBundle:
    semantic_model: ArtifactRef
    report: ArtifactRef


@dataclasses.dataclass(frozen=True, slots=True)
class RealTimeIntelligenceBinding:
    eventhouse_id: str | None
    kql_database_id: str | None
    queryset_id: str | None
    dashboard_id: str | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class ActivatorBinding:
    reflex_id: str | None
    trigger_name: str | None


@dataclasses.dataclass(frozen=True, slots=True)
class ReleaseProvenanceRef:
    require_identity: bool
    identity_schema: str


@dataclasses.dataclass(frozen=True, slots=True)
class DeploymentManifest:
    stage: DeploymentEnvironment
    workspace_id: str | None
    fabric_environment_id: str | None
    fabric_runtime: str
    lakehouse: LakehouseBinding
    roles: Mapping[str, RoleBinding]
    analytics_reporting: ReportingBundle
    operational_reporting: ReportingBundle
    real_time_intelligence: RealTimeIntelligenceBinding
    activator: ActivatorBinding
    release: ReleaseProvenanceRef

    def role(self, name: str) -> RoleBinding:
        try:
            return self.roles[name]
        except KeyError as error:
            raise ManifestValidationError(f"unknown SJD role {name!r}") from error

    def entry_point(self, name: str) -> str:
        return ENTRY_POINTS[self.role(name).role]

    def abfss_path(self, path: str) -> str:
        if self.workspace_id is None or self.lakehouse.lakehouse_id is None:
            raise ManifestValidationError(
                f"{self.stage.value} is not provisioned with a workspace/lakehouse id"
            )
        validated = self.lakehouse.validate_files_path(path)
        return (
            f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{self.lakehouse.lakehouse_id}/{validated}"
        )


def _text(value: object, label: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ManifestValidationError(f"{label} must be non-empty")
    return text


def _optional_text(value: object, label: str) -> str | None:
    return None if value is None else _text(value, label)


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ManifestValidationError(f"{label} must be an object")
    return value


def _boolean(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise ManifestValidationError(f"{label} must be a boolean")
    return value


def _integer(value: object, label: str) -> int:
    if type(value) is not int:
        raise ManifestValidationError(f"{label} must be an integer")
    return value


def _uuid(value: object, label: str) -> str:
    text = _text(value, label)
    try:
        canonical = str(uuid.UUID(text))
    except (AttributeError, TypeError, ValueError) as error:
        raise ManifestValidationError(f"{label} must be a canonical UUID") from error
    if canonical != text:
        raise ManifestValidationError(f"{label} must be a canonical UUID")
    return text


def _optional_uuid(value: object, label: str) -> str | None:
    return None if value is None else _uuid(value, label)


def _deep_merge(
    base: Mapping[str, Any], overlay: Mapping[str, Any]
) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in overlay.items():
        current = merged.get(key)
        if isinstance(current, Mapping) and isinstance(value, Mapping):
            merged[key] = _deep_merge(current, value)
        else:
            merged[key] = value
    return merged


def _reject_secret_like_keys(document: Mapping[str, Any], path: str = "") -> None:
    for key, value in document.items():
        qualified = f"{path}.{key}" if path else str(key)
        if any(part in str(key).lower() for part in _FORBIDDEN_KEY_PARTS):
            raise ManifestValidationError(
                f"deployment manifest field {qualified!r} looks like a secret"
            )
        if isinstance(value, Mapping):
            _reject_secret_like_keys(value, qualified)


def _load_json_file(path: Path, label: str) -> Mapping[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as error:
        raise ManifestLoadError(f"{label} not found at {path}") from error
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ManifestLoadError(f"{label} at {path} is not valid JSON: {error}") from error
    if not isinstance(document, Mapping):
        raise ManifestLoadError(f"{label} at {path} must be a JSON object")
    return document


def default_manifest_path(stage: DeploymentEnvironment) -> Path:
    return _DATA_DIR / f"{stage.value}.json"


def resolve_stage(
    explicit: DeploymentEnvironment | str | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> DeploymentEnvironment:
    if explicit is not None:
        return DeploymentEnvironment(explicit)
    active_env = os.environ if env is None else env
    raw = active_env.get(f"{_ENV_PREFIX}STAGE")
    if not raw:
        return DeploymentEnvironment.PROD
    try:
        return DeploymentEnvironment(raw)
    except ValueError as error:
        raise ManifestValidationError(
            f"{_ENV_PREFIX}STAGE={raw!r} must be dev, test, or prod"
        ) from error


def load_manifest_document(
    stage: DeploymentEnvironment,
    *,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    active_env = os.environ if env is None else env
    document = dict(
        _load_json_file(default_manifest_path(stage), "bundled deployment manifest")
    )
    overlay_path = active_env.get(f"{_ENV_PREFIX}CONFIG_PATH")
    if overlay_path:
        document = _deep_merge(
            document,
            _load_json_file(Path(overlay_path), "deployment manifest overlay file"),
        )
    overlay_json = active_env.get(f"{_ENV_PREFIX}CONFIG_JSON")
    if overlay_json:
        try:
            inline = json.loads(overlay_json)
        except json.JSONDecodeError as error:
            raise ManifestLoadError(
                f"{_ENV_PREFIX}CONFIG_JSON is not valid JSON: {error}"
            ) from error
        if not isinstance(inline, Mapping):
            raise ManifestLoadError(f"{_ENV_PREFIX}CONFIG_JSON must be a JSON object")
        document = _deep_merge(document, inline)
    _reject_secret_like_keys(document)
    return document


def _parse_schedule(value: object, label: str) -> ScheduleSpec | None:
    if value is None:
        return None
    document = _mapping(value, label)
    interval = _integer(document.get("interval_minutes"), f"{label}.interval_minutes")
    if not 1 <= interval <= 10_080:
        raise ManifestValidationError(
            f"{label}.interval_minutes must be between 1 and 10080"
        )
    return ScheduleSpec(
        interval_minutes=interval,
        enabled=_boolean(document.get("enabled", False), f"{label}.enabled"),
    )


def _parse_role(role: str, value: object) -> RoleBinding:
    document = _mapping(value, f"roles.{role}")
    schedule = _parse_schedule(document.get("schedule"), f"roles.{role}.schedule")
    if role in SCHEDULED_ROLES and schedule is None:
        raise ManifestValidationError(f"roles.{role} requires a schedule")
    if role in ON_DEMAND_ROLES and schedule is not None:
        raise ManifestValidationError(f"roles.{role} must not declare a schedule")
    return RoleBinding(
        role=role,
        display_name=_text(document.get("display_name"), f"roles.{role}.display_name"),
        item_id=_optional_uuid(document.get("item_id"), f"roles.{role}.item_id"),
        schedule=schedule,
    )


def _parse_artifact(value: object, label: str) -> ArtifactRef:
    document = _mapping(value, label)
    return ArtifactRef(
        source_item_id=_optional_uuid(
            document.get("source_item_id"), f"{label}.source_item_id"
        ),
        source_name=_text(document.get("source_name"), f"{label}.source_name"),
        target_name=_text(document.get("target_name"), f"{label}.target_name"),
        target_item_id=_optional_uuid(
            document.get("target_item_id"), f"{label}.target_item_id"
        ),
    )


def _parse_reporting(value: object, label: str) -> ReportingBundle:
    document = _mapping(value, label)
    return ReportingBundle(
        semantic_model=_parse_artifact(
            document.get("semantic_model"), f"{label}.semantic_model"
        ),
        report=_parse_artifact(document.get("report"), f"{label}.report"),
    )


def _verify_release_identity(
    stage: DeploymentEnvironment,
    workspace_id: str | None,
    lakehouse_id: str | None,
    environment_id: str | None,
    release: ReleaseProvenanceRef,
) -> None:
    from people_counter import fabric_release_provenance as provenance

    identity = provenance.RELEASE_IDENTITY
    if identity.schema != release.identity_schema:
        raise ManifestValidationError(
            f"{stage.value} release identity schema does not match the manifest"
        )
    mismatches = {
        name: (actual, expected)
        for name, actual, expected in (
            ("workspace_id", workspace_id, identity.workspace_id),
            ("lakehouse_id", lakehouse_id, identity.lakehouse_id),
            ("fabric_environment_id", environment_id, identity.environment_id),
        )
        if actual != expected
    }
    if mismatches:
        raise ManifestValidationError(
            f"{stage.value} release identity disagrees with manifest: {mismatches!r}"
        )


def verify_release_provenance(manifest: DeploymentManifest) -> str | None:
    if not manifest.release.require_identity:
        return None
    _verify_release_identity(
        manifest.stage,
        manifest.workspace_id,
        manifest.lakehouse.lakehouse_id,
        manifest.fabric_environment_id,
        manifest.release,
    )
    from people_counter import fabric_release_provenance as provenance

    return provenance.RELEASE_IDENTITY_SHA256


def parse_deployment_manifest(
    document: Mapping[str, Any],
    stage: DeploymentEnvironment,
) -> DeploymentManifest:
    if _text(document.get("schema"), "schema") != MANIFEST_SCHEMA:
        raise ManifestValidationError("unsupported deployment manifest schema")
    if _text(document.get("stage"), "stage") != stage.value:
        raise ManifestValidationError("manifest stage does not match requested stage")

    workspace_id = _optional_uuid(document.get("workspace_id"), "workspace_id")
    environment_id = _optional_uuid(
        document.get("fabric_environment_id"), "fabric_environment_id"
    )
    runtime = _text(document.get("fabric_runtime"), "fabric_runtime")
    lakehouse_document = _mapping(document.get("lakehouse"), "lakehouse")
    lakehouse_id = _optional_uuid(
        lakehouse_document.get("lakehouse_id"), "lakehouse.lakehouse_id"
    )
    table_prefix = _text(
        lakehouse_document.get("table_prefix"), "lakehouse.table_prefix"
    )
    files_root = _text(lakehouse_document.get("files_root"), "lakehouse.files_root")
    if not _SAFE_TABLE_PREFIX.fullmatch(table_prefix):
        raise ManifestValidationError("lakehouse.table_prefix has an unsafe value")
    if not files_root.endswith("/"):
        raise ManifestValidationError("lakehouse.files_root must end with '/'")

    if stage is DeploymentEnvironment.PROD:
        expected = {
            "workspace_id": WORKSPACE_ID,
            "fabric_environment_id": ENVIRONMENT_ID,
            "fabric_runtime": FABRIC_RUNTIME,
            "lakehouse_id": LAKEHOUSE_ID,
            "table_prefix": TABLE_PREFIX,
            "files_root": FILES_ROOT,
        }
        actual = {
            "workspace_id": workspace_id,
            "fabric_environment_id": environment_id,
            "fabric_runtime": runtime,
            "lakehouse_id": lakehouse_id,
            "table_prefix": table_prefix,
            "files_root": files_root,
        }
        mismatches = {
            key: (actual[key], value)
            for key, value in expected.items()
            if actual[key] != value
        }
        if mismatches and not document.get("allow_legacy_mismatch", False):
            raise ManifestValidationError(
                f"production deployment identity is fixed: {mismatches!r}"
            )
    elif files_root == FILES_ROOT:
        raise ManifestValidationError(
            f"{stage.value} must not reuse the production OneLake files namespace"
        )

    roles_document = _mapping(document.get("roles"), "roles")
    if set(roles_document) != SJD_ROLES:
        missing = sorted(SJD_ROLES - set(roles_document))
        extra = sorted(set(roles_document) - SJD_ROLES)
        raise ManifestValidationError(
            f"roles must declare exactly the seven SJD roles; "
            f"missing={missing} extra={extra}"
        )
    roles = {role: _parse_role(role, value) for role, value in roles_document.items()}

    release_document = _mapping(document.get("release"), "release")
    release = ReleaseProvenanceRef(
        require_identity=_boolean(
            release_document.get("require_identity", False),
            "release.require_identity",
        ),
        identity_schema=_text(
            release_document.get(
                "identity_schema", "people-counter-sjd-release-identity-v1"
            ),
            "release.identity_schema",
        ),
    )
    if release.require_identity:
        _verify_release_identity(
            stage, workspace_id, lakehouse_id, environment_id, release
        )
        from people_counter import fabric_release_provenance as provenance

        roles = {
            role: dataclasses.replace(
                binding,
                item_id=binding.item_id
                or provenance.RELEASE_IDENTITY.sjd_ids.get(f"stable-{role}"),
            )
            for role, binding in roles.items()
        }
        if any(binding.item_id is None for binding in roles.values()):
            raise ManifestValidationError("release identity omitted an SJD role")

    rti_document = _mapping(
        document.get("real_time_intelligence"), "real_time_intelligence"
    )
    activator_document = _mapping(document.get("activator"), "activator")
    return DeploymentManifest(
        stage=stage,
        workspace_id=workspace_id,
        fabric_environment_id=environment_id,
        fabric_runtime=runtime,
        lakehouse=LakehouseBinding(lakehouse_id, table_prefix, files_root),
        roles=roles,
        analytics_reporting=_parse_reporting(
            document.get("analytics_reporting"), "analytics_reporting"
        ),
        operational_reporting=_parse_reporting(
            document.get("operational_reporting"), "operational_reporting"
        ),
        real_time_intelligence=RealTimeIntelligenceBinding(
            eventhouse_id=_optional_uuid(
                rti_document.get("eventhouse_id"),
                "real_time_intelligence.eventhouse_id",
            ),
            kql_database_id=_optional_uuid(
                rti_document.get("kql_database_id"),
                "real_time_intelligence.kql_database_id",
            ),
            queryset_id=_optional_uuid(
                rti_document.get("queryset_id"),
                "real_time_intelligence.queryset_id",
            ),
            dashboard_id=_optional_uuid(
                rti_document.get("dashboard_id"),
                "real_time_intelligence.dashboard_id",
            ),
        ),
        activator=ActivatorBinding(
            reflex_id=_optional_uuid(
                activator_document.get("reflex_id"), "activator.reflex_id"
            ),
            trigger_name=_optional_text(
                activator_document.get("trigger_name"), "activator.trigger_name"
            ),
        ),
        release=release,
    )


def load_deployment_manifest(
    stage: DeploymentEnvironment | str | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> DeploymentManifest:
    resolved = resolve_stage(stage, env=env)
    return parse_deployment_manifest(
        load_manifest_document(resolved, env=env),
        resolved,
    )
