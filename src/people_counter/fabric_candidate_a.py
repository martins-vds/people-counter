"""Fixed deployment contract for the Candidate A Fabric canary.

This module is intentionally standard-library only.  It describes artifacts
that may be built locally; it does not call Fabric or OneLake APIs.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any, Mapping

from people_counter.fabric_canary_tool import encode_part


WORKSPACE_ID = "c31ee864-230d-4005-8fd5-7c7130ebf774"
LAKEHOUSE_ID = "883cff91-eaa8-40be-870f-6e9716303cb2"
ENVIRONMENT_ID = "3e580f48-9ff7-4bc6-af2e-a59158029ada"
TABLE_PREFIX = "pc_ca_canary_v1_"
FILES_ROOT = "Files/_canary/people-counter/candidate-a/v1/"
FABRIC_RUNTIME = "2.0"

_JOB_MODULES = {
    "control": "people_counter.fabric_candidate_a_jobs.control_main",
    "process": "people_counter.fabric_candidate_a_jobs.process_main",
    "gold": "people_counter.fabric_candidate_a_jobs.gold_main",
}


@dataclass(frozen=True)
class FabricCandidateAConfig:
    """The non-overridable Phase 1 canary boundary."""

    workspace_id: str = WORKSPACE_ID
    lakehouse_id: str = LAKEHOUSE_ID
    environment_id: str = ENVIRONMENT_ID
    table_prefix: str = TABLE_PREFIX
    files_root: str = FILES_ROOT
    runtime: str = FABRIC_RUNTIME

    def __post_init__(self) -> None:
        expected = {
            "workspace_id": WORKSPACE_ID,
            "lakehouse_id": LAKEHOUSE_ID,
            "environment_id": ENVIRONMENT_ID,
            "table_prefix": TABLE_PREFIX,
            "files_root": FILES_ROOT,
            "runtime": FABRIC_RUNTIME,
        }
        mismatches = {
            name: (getattr(self, name), value)
            for name, value in expected.items()
            if getattr(self, name) != value
        }
        if mismatches:
            raise ValueError(
                f"Candidate A configuration is fixed; mismatches={mismatches!r}"
            )
        for value in (self.workspace_id, self.lakehouse_id, self.environment_id):
            uuid.UUID(value)
        if not self.files_root.endswith("/"):
            raise ValueError("files_root must end with '/'")

    def table(self, suffix: str) -> str:
        if (
            not suffix
            or not suffix.isascii()
            or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789_" for character in suffix)
        ):
            raise ValueError(f"invalid Candidate A table suffix {suffix!r}")
        return f"{self.table_prefix}{suffix}"

    def file_path(self, relative: str) -> str:
        parts = relative.split("/")
        if (
            not relative
            or relative.startswith("/")
            or "\\" in relative
            or any(part in {"", ".", ".."} for part in parts)
        ):
            raise ValueError(f"unsafe Candidate A relative path {relative!r}")
        return f"{self.files_root}{relative}"

    def abfss_path(self, path: str) -> str:
        """Qualify one fixed-root Files path for Spark/Delta operations."""
        if (
            not path.startswith(self.files_root)
            or "\\" in path
            or "://" in path
            or ".." in path.split("/")
        ):
            raise ValueError(
                f"path must remain under the fixed Candidate A root: {path!r}"
            )
        return (
            f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{self.lakehouse_id}/{path}"
        )


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
