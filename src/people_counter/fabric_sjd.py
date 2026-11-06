"""Stable production namespace contract for Fabric Spark Job Definitions."""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum


WORKSPACE_ID = "c31ee864-230d-4005-8fd5-7c7130ebf774"
LAKEHOUSE_ID = "883cff91-eaa8-40be-870f-6e9716303cb2"
ENVIRONMENT_ID = "3e580f48-9ff7-4bc6-af2e-a59158029ada"
FABRIC_RUNTIME = "2.0"
TABLE_PREFIX = "people_counter_sjd_"
FILES_ROOT = "Files/people-counter/sjd/v1/"

TABLE_SUFFIXES = frozenset(
    {
        "locks",
        "work",
        "batches",
        "batch_members",
        "attempts",
        "publications",
        "replay_requests",
        "reconciliation_findings",
        "gold_checkpoints",
        "semantic_refresh_outbox",
        "migration_journal",
        "retirement_journal",
        "gold_flow_minute",
        "gold_flow_hour",
        "gold_video",
        "gold_operations_hour",
        "gold_work_operations",
        "gold_attempt_operations",
        "gold_dim_date",
        "gold_dim_time",
        "gold_dim_camera",
        "gold_dim_location",
        "gold_dim_video",
        "gold_dim_model_config",
    }
)
CUTOVER_TABLE_SUFFIXES = frozenset(
    {
        "migration_journal",
        "retirement_journal",
    }
)
BOOTSTRAP_TABLE_SUFFIXES = TABLE_SUFFIXES - CUTOVER_TABLE_SUFFIXES
_SAFE_PATH_SEGMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._=-]*\Z")


class StableNamespaceMode(str, Enum):
    PRODUCTION = "PRODUCTION"


@dataclass(frozen=True, slots=True)
class FabricSjdConfig:
    """Fixed, write-enabled production binding with no legacy aliases."""

    workspace_id: str = WORKSPACE_ID
    lakehouse_id: str = LAKEHOUSE_ID
    environment_id: str = ENVIRONMENT_ID
    table_prefix: str = TABLE_PREFIX
    files_root: str = FILES_ROOT
    runtime: str = FABRIC_RUNTIME
    mode: StableNamespaceMode = StableNamespaceMode.PRODUCTION
    stable_production: bool = True

    def __post_init__(self) -> None:
        self._validate_config((WORKSPACE_ID, LAKEHOUSE_ID, ENVIRONMENT_ID))

    def _validate_config(self, artifact_ids: tuple[str, str, str]) -> None:
        expected = {
            "workspace_id": artifact_ids[0],
            "lakehouse_id": artifact_ids[1],
            "environment_id": artifact_ids[2],
            "table_prefix": TABLE_PREFIX,
            "files_root": FILES_ROOT,
            "runtime": FABRIC_RUNTIME,
            "mode": StableNamespaceMode.PRODUCTION,
            "stable_production": True,
        }
        mismatches = {
            name: (getattr(self, name), value)
            for name, value in expected.items()
            if getattr(self, name) != value
        }
        if mismatches:
            raise ValueError(
                f"stable production configuration is fixed: {mismatches!r}"
            )
        for value in (self.workspace_id, self.lakehouse_id, self.environment_id):
            try:
                identity = uuid.UUID(value)
            except (AttributeError, TypeError, ValueError) as error:
                raise ValueError("Fabric artifact IDs must be canonical UUIDs") from error
            if str(identity) != value or identity.int == 0:
                raise ValueError("Fabric artifact IDs must be canonical UUIDs")

    def require_write_enabled(self) -> None:
        if not self.stable_production:
            raise ValueError("stable production writes are disabled")

    def table(self, suffix: str) -> str:
        if suffix not in TABLE_SUFFIXES:
            raise ValueError(f"invalid stable production table suffix {suffix!r}")
        return f"{self.table_prefix}{suffix}"

    def validate_table_name(self, table_name: str) -> str:
        if table_name not in {self.table(suffix) for suffix in TABLE_SUFFIXES}:
            raise ValueError(
                f"table is outside the stable production namespace: {table_name!r}"
            )
        return table_name

    def file_path(self, relative: str) -> str:
        parts = relative.split("/")
        if (
            not relative
            or relative.startswith("/")
            or "\\" in relative
            or any(
                part in {"", ".", ".."}
                or _SAFE_PATH_SEGMENT.fullmatch(part) is None
                for part in parts
            )
        ):
            raise ValueError(f"unsafe stable production relative path {relative!r}")
        return f"{self.files_root}{relative}"

    def validate_files_path(self, path: str) -> str:
        if not path.startswith(self.files_root):
            raise ValueError(
                f"path is outside the stable production namespace: {path!r}"
            )
        return self.file_path(path[len(self.files_root) :])

    def abfss_path(self, path: str) -> str:
        validated = self.validate_files_path(path)
        return (
            f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{self.lakehouse_id}/{validated}"
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class ManualFabricSjdConfig(FabricSjdConfig):
    """Explicit artifact binding; the stable namespace and write guards stay fixed."""

    workspace_id: str
    lakehouse_id: str
    environment_id: str

    def __post_init__(self) -> None:
        self._validate_config((self.workspace_id, self.lakehouse_id, self.environment_id))

    def validate_runtime_context(self, context: Mapping[str, object]) -> None:
        if not isinstance(context, Mapping):
            raise ValueError("Manual runtime context must be a mapping")
        for key, expected in (
            ("currentWorkspaceId", self.workspace_id),
            ("defaultLakehouseId", self.lakehouse_id),
            ("defaultLakehouseWorkspaceId", self.workspace_id),
            ("environmentId", self.environment_id),
            ("environmentWorkspaceId", self.workspace_id),
        ):
            if context.get(key) != expected:
                raise ValueError(
                    f"Manual runtime context {key} must match {expected}; "
                    f"observed {context.get(key)!r}"
                )
