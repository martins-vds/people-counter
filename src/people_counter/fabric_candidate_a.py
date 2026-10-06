"""Fixed deployment contracts for Candidate A Fabric namespaces.

This module is intentionally standard-library only.  It describes artifacts
that may be built locally; it does not call Fabric or OneLake APIs.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

from people_counter.fabric_canary_tool import encode_part


WORKSPACE_ID = "c31ee864-230d-4005-8fd5-7c7130ebf774"
LAKEHOUSE_ID = "883cff91-eaa8-40be-870f-6e9716303cb2"
ENVIRONMENT_ID = "3e580f48-9ff7-4bc6-af2e-a59158029ada"
FABRIC_RUNTIME = "2.0"


class CandidateANamespaceMode(str, Enum):
    """Candidate A execution boundaries, ordered by promotion intent."""

    CANARY = "CANARY"
    BENCHMARK = "BENCHMARK"
    PRODUCTION_SHADOW = "PRODUCTION_SHADOW"
    PRODUCTION = "PRODUCTION"


CANARY_TABLE_PREFIX = "pc_ca_canary_v1_"
CANARY_FILES_ROOT = "Files/_canary/people-counter/candidate-a/v1/"
BENCHMARK_TABLE_PREFIX = "pc_ca_benchmark_v1_"
BENCHMARK_FILES_ROOT = "Files/_benchmark/people-counter/candidate-a/v1/"
PRODUCTION_SHADOW_TABLE_PREFIX = "pc_ca_prod_shadow_v1_"
PRODUCTION_SHADOW_FILES_ROOT = "Files/_shadow/people-counter/candidate-a/v1/"
PRODUCTION_TABLE_PREFIX = "people_counter_ca_"
PRODUCTION_FILES_ROOT = "Files/people-counter/candidate-a/v1/"
LEGACY_TABLE_PREFIX = "people_counter_"

# Compatibility names remain the fixed canary namespace.
TABLE_PREFIX = CANARY_TABLE_PREFIX
FILES_ROOT = CANARY_FILES_ROOT

_NAMESPACES = {
    CandidateANamespaceMode.CANARY: (
        CANARY_TABLE_PREFIX,
        CANARY_FILES_ROOT,
    ),
    CandidateANamespaceMode.BENCHMARK: (
        BENCHMARK_TABLE_PREFIX,
        BENCHMARK_FILES_ROOT,
    ),
    CandidateANamespaceMode.PRODUCTION_SHADOW: (
        PRODUCTION_SHADOW_TABLE_PREFIX,
        PRODUCTION_SHADOW_FILES_ROOT,
    ),
    CandidateANamespaceMode.PRODUCTION: (
        PRODUCTION_TABLE_PREFIX,
        PRODUCTION_FILES_ROOT,
    ),
}

_TABLE_SUFFIXES = frozenset(
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
        "gold_flow_minute",
        "gold_flow_hour",
        "gold_video",
        "gold_operations_hour",
        "gold_dim_date",
        "gold_dim_time",
        "gold_dim_camera",
        "gold_dim_location",
        "gold_dim_video",
        "gold_dim_model_config",
        "routing_allowlist",
        "shadow_audit",
    }
)
_SAFE_PATH_SEGMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._=-]*\Z")
_LEGACY_TABLE_NAMES = frozenset(
    f"{LEGACY_TABLE_PREFIX}{suffix}" for suffix in _TABLE_SUFFIXES
)

_JOB_MODULES = {
    "control": "people_counter.fabric_candidate_a_jobs.control_main",
    "process": "people_counter.fabric_candidate_a_jobs.process_main",
    "gold": "people_counter.fabric_candidate_a_jobs.gold_main",
}


@dataclass(frozen=True, slots=True)
class FabricCandidateAConfig:
    """An immutable Candidate A namespace and Fabric artifact binding.

    The default remains the original Phase 1 canary.  Supplying only ``mode``
    selects that mode's reviewed namespace; explicitly supplied namespace
    values must still match it exactly.
    """

    workspace_id: str = WORKSPACE_ID
    lakehouse_id: str = LAKEHOUSE_ID
    environment_id: str = ENVIRONMENT_ID
    table_prefix: str | None = None
    files_root: str | None = None
    runtime: str = FABRIC_RUNTIME
    mode: CandidateANamespaceMode = CandidateANamespaceMode.CANARY
    production_enabled: bool = False

    def __post_init__(self) -> None:
        try:
            mode = CandidateANamespaceMode(self.mode)
        except (TypeError, ValueError) as error:
            raise ValueError(
                f"invalid Candidate A namespace mode {self.mode!r}"
            ) from error
        object.__setattr__(self, "mode", mode)
        expected_prefix, expected_root = _NAMESPACES[mode]
        if self.table_prefix is None:
            object.__setattr__(self, "table_prefix", expected_prefix)
        if self.files_root is None:
            object.__setattr__(self, "files_root", expected_root)

        expected = {
            "workspace_id": WORKSPACE_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "environment_id": ENVIRONMENT_ID,
            "table_prefix": expected_prefix,
            "files_root": expected_root,
            "runtime": FABRIC_RUNTIME,
        }
        mismatches = {
            name: (getattr(self, name), value)
            for name, value in expected.items()
            if getattr(self, name) != value
        }
        if mismatches:
            raise ValueError(
                f"Candidate A {mode.value} configuration is fixed; "
                f"mismatches={mismatches!r}"
            )
        if self.production_enabled:
            raise ValueError(
                "direct PRODUCTION adapter execution is disabled; use the "
                "hash-bound production router after migration verification"
            )
        for value in (self.workspace_id, self.lakehouse_id, self.environment_id):
            if str(uuid.UUID(value)) != value:
                raise ValueError("Candidate A artifact IDs must be canonical UUIDs")

    @classmethod
    def for_mode(
        cls,
        mode: CandidateANamespaceMode | str,
        *,
        workspace_id: str = WORKSPACE_ID,
        lakehouse_id: str = LAKEHOUSE_ID,
        environment_id: str = ENVIRONMENT_ID,
        runtime: str = FABRIC_RUNTIME,
    ) -> FabricCandidateAConfig:
        """Create the exact reviewed namespace for ``mode``."""

        return cls(
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            environment_id=environment_id,
            runtime=runtime,
            mode=CandidateANamespaceMode(mode),
        )

    @classmethod
    def canary(cls) -> FabricCandidateAConfig:
        return cls.for_mode(CandidateANamespaceMode.CANARY)

    @classmethod
    def benchmark(cls) -> FabricCandidateAConfig:
        return cls.for_mode(CandidateANamespaceMode.BENCHMARK)

    @classmethod
    def production_shadow(cls) -> FabricCandidateAConfig:
        return cls.for_mode(CandidateANamespaceMode.PRODUCTION_SHADOW)

    @classmethod
    def production(cls) -> FabricCandidateAConfig:
        return cls.for_mode(CandidateANamespaceMode.PRODUCTION)

    def require_write_enabled(self) -> None:
        """Reject direct production writes; routing owns production enablement."""

        if self.mode is CandidateANamespaceMode.PRODUCTION:
            raise ValueError(
                "direct PRODUCTION writes are disabled; use the production router"
            )

    def validate_binding(
        self,
        *,
        workspace_id: str,
        lakehouse_id: str,
        environment_id: str,
        mode: CandidateANamespaceMode | str,
    ) -> None:
        """Fail closed when runtime artifacts or requested mode differ."""

        try:
            requested_mode = CandidateANamespaceMode(mode)
        except (TypeError, ValueError) as error:
            raise ValueError(f"invalid Candidate A namespace mode {mode!r}") from error
        observed = {
            "workspace_id": workspace_id,
            "lakehouse_id": lakehouse_id,
            "environment_id": environment_id,
            "mode": requested_mode,
        }
        expected = {
            "workspace_id": self.workspace_id,
            "lakehouse_id": self.lakehouse_id,
            "environment_id": self.environment_id,
            "mode": self.mode,
        }
        mismatches = {
            name: (observed[name], value)
            for name, value in expected.items()
            if observed[name] != value
        }
        if mismatches:
            raise ValueError(
                f"Candidate A namespace binding mismatch: {mismatches!r}"
            )

    def require_mode(self, mode: CandidateANamespaceMode | str) -> None:
        """Prevent a canary (or any other namespace) from being reused."""

        self.validate_binding(
            workspace_id=self.workspace_id,
            lakehouse_id=self.lakehouse_id,
            environment_id=self.environment_id,
            mode=mode,
        )

    def table(self, suffix: str) -> str:
        if suffix not in _TABLE_SUFFIXES:
            raise ValueError(f"invalid Candidate A table suffix {suffix!r}")
        return f"{self.table_prefix}{suffix}"

    def validate_table_name(self, table_name: str) -> str:
        """Return an exact table in this namespace, rejecting legacy aliases."""

        for suffix in _TABLE_SUFFIXES:
            if table_name == self.table(suffix):
                return table_name
        if (
            self.mode
            in {
                CandidateANamespaceMode.BENCHMARK,
                CandidateANamespaceMode.PRODUCTION_SHADOW,
            }
            and table_name in _LEGACY_TABLE_NAMES
        ):
            raise ValueError(
                f"{self.mode.value} cannot mutate legacy production tables"
            )
        raise ValueError(
            f"table is outside the {self.mode.value} Candidate A namespace: "
            f"{table_name!r}"
        )

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
            raise ValueError(f"unsafe Candidate A relative path {relative!r}")
        return f"{self.files_root}{relative}"

    def validate_files_path(self, path: str) -> str:
        """Return a safe exact-root path, rejecting cross-mode mutations."""

        root = str(self.files_root)
        if not path.startswith(root):
            if (
                self.mode
                in {
                    CandidateANamespaceMode.BENCHMARK,
                    CandidateANamespaceMode.PRODUCTION_SHADOW,
                }
                and path.startswith(PRODUCTION_FILES_ROOT)
            ):
                raise ValueError(
                    f"{self.mode.value} cannot mutate legacy production files"
                )
            raise ValueError(
                f"path is outside the {self.mode.value} Candidate A namespace: "
                f"{path!r}"
            )
        relative = path[len(root) :]
        return self.file_path(relative)

    def abfss_path(self, path: str) -> str:
        """Qualify one exact-root Files path for Spark/Delta operations."""

        try:
            validated = self.validate_files_path(path)
        except ValueError as error:
            raise ValueError(
                f"path must remain under the fixed Candidate A root: {path!r}"
            ) from error
        return (
            f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{self.lakehouse_id}/{validated}"
        )


# New code can use the namespace-specific name while existing imports and
# constructor behavior remain unchanged.
CandidateANamespaceConfig = FabricCandidateAConfig
CandidateAMode = CandidateANamespaceMode
FabricCandidateAMode = CandidateANamespaceMode


def validate_environment_library_policy(metadata: Mapping[str, Any]) -> None:
    """Require the one published Full-mode Runtime 2.0 Environment.

    Candidate A jobs import the installed wheel.  Inline libraries, session
    installs, and source fallbacks are deliberately forbidden.
    """

    environment_id = metadata.get(
        "environmentArtifactId", metadata.get("environment_id")
    )
    runtime = metadata.get("runtimeVersion", metadata.get("runtime"))
    mode = metadata.get("libraryMode", metadata.get("library_mode"))
    inline = metadata.get("additionalLibraryUris", metadata.get("inline_libraries", []))
    published = metadata.get("published", metadata.get("publishState") == "Succeeded")
    if environment_id != ENVIRONMENT_ID:
        raise ValueError("Candidate A requires the fixed Fabric Environment")
    if str(runtime) != FABRIC_RUNTIME:
        raise ValueError("Candidate A requires Fabric Runtime 2.0")
    if str(mode).lower() != "full":
        raise ValueError("Candidate A requires Full environment library mode")
    if published is not True:
        raise ValueError("Candidate A requires a published Environment")
    if inline not in (None, [], ()):
        raise ValueError("Candidate A forbids inline or session libraries")


def thin_main_source(job: str) -> bytes:
    """Return an installed-wheel-only SJD entry point."""

    try:
        target = _JOB_MODULES[job]
    except KeyError as error:
        raise ValueError(f"unsupported Candidate A job {job!r}") from error
    module, function = target.rsplit(".", 1)
    return (
        '"""Generated Candidate A Fabric SJD entry point."""\n\n'
        f"from {module} import {function} as main\n\n"
        'if __name__ == "__main__":\n'
        "    raise SystemExit(main())\n"
    ).encode("utf-8")


def build_sjd_v2_definition(
    job: str,
    *,
    command_line_arguments: str = "",
) -> dict[str, object]:
    """Build a deterministic SJD V2 definition bound to the fixed artifacts."""

    if "\x00" in command_line_arguments:
        raise ValueError("command_line_arguments contains NUL")
    metadata = {
        "additionalLakehouseIds": [],
        "additionalLibraryUris": [],
        "commandLineArguments": command_line_arguments,
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
                encode_part("Main/main.py", thin_main_source(job)),
                encode_part("SparkJobDefinitionV1.json", metadata_bytes),
            ],
        }
    }
