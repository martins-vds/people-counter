"""Stable production namespace contract for Fabric Spark Job Definitions."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

from people_counter.fabric_target import FabricTarget

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
BOOTSTRAP_TABLE_SUFFIXES = TABLE_SUFFIXES
_SAFE_PATH_SEGMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._=-]*\Z")


class StableNamespaceMode(str, Enum):
    PRODUCTION = "PRODUCTION"


def validate_files_root(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith("Files/")
        or not value.endswith("/")
        or not value[6:-1]
        or any(
            _SAFE_PATH_SEGMENT.fullmatch(part) is None or part in {".", ".."}
            for part in value[6:-1].split("/")
        )
    ):
        raise ValueError("Fabric files root must be a safe directory below Files/")
    return value


@dataclass(frozen=True, slots=True)
class FabricSjdConfig(FabricTarget):
    """Explicit deployment binding with a guarded production namespace."""

    table_prefix: str = TABLE_PREFIX
    files_root: str = FILES_ROOT
    runtime: str = FABRIC_RUNTIME
    mode: StableNamespaceMode = StableNamespaceMode.PRODUCTION
    stable_production: bool = True

    def __post_init__(self) -> None:
        FabricTarget.__post_init__(self)
        expected = {
            "table_prefix": TABLE_PREFIX,
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
        validate_files_root(self.files_root)

    def namespace_arguments(self) -> list[str]:
        return [] if self.files_root == FILES_ROOT else ["--files-root", self.files_root]

    def command_line_arguments(self) -> list[str]:
        return [*FabricTarget.command_line_arguments(self), *self.namespace_arguments()]

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


@dataclass(frozen=True, slots=True)
class ManualFabricSjdConfig(FabricSjdConfig):
    """Compatibility spelling for an explicitly bound production target."""
