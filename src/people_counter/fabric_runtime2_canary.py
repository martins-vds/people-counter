"""Isolated Microsoft Fabric Runtime 2.0 Spark Job Definition canary.

The module is importable without PySpark.  Spark and Delta are imported only by
``run_fabric_canary`` after the strict, canary-only argument contract passes.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.metadata
import importlib.resources
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import threading
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Protocol


WORKSPACE_ID = "c31ee864-230d-4005-8fd5-7c7130ebf774"
LAKEHOUSE_ID = "883cff91-eaa8-40be-870f-6e9716303cb2"
WRITE_SCOPE = "_canary/people-counter/runtime2-sjd/v1"
SAFETY_TOKEN = "PC_CANARY_ONLY_V1"
FILES_PREFIX = f"Files/{WRITE_SCOPE}"
SCHEMA_VERSION = 1
MINIMUM_PARTITIONS = 2
HEX64 = re.compile(r"^[0-9a-f]{64}$")
AUTHORITATIVE_LAKEHOUSE_KEYS = frozenset(
    {
        "spark.hadoop.trident.lakehouse.id",
        "trident.lakehouse.id",
        "spark.microsoft.fabric.lakehouse.id",
    }
)
AUTHORITATIVE_WORKSPACE_KEYS = frozenset(
    {
        "spark.hadoop.trident.workspace.id",
        "spark.hadoop.trident.artifact.workspace.id",
        "spark.hadoop.trident.catalog.metastore.workspaceid",
        "spark.sql.trident.catalog.metastore.workspaceid",
        "trident.workspace.id",
        "trident.artifact.workspace.id",
        "spark.microsoft.fabric.workspace.id",
    }
)


class CanaryValidationError(RuntimeError):
    """The canary cannot safely execute or its result is invalid."""


class RunRootExistsError(CanaryValidationError):
    """The immutable run root already exists."""


@dataclass(frozen=True)
class CanaryArguments:
    workspace_id: str
    lakehouse_id: str
    run_id: str
    write_scope: str
    safety_token: str
    release_digest: str
    project_version: str
    package_identity: str
    definition_sha256: str
    source_archive_sha256: str
    partitions: int = MINIMUM_PARTITIONS

    @property
    def relative_run_root(self) -> str:
        return f"{FILES_PREFIX}/run={self.run_id}"


@dataclass(frozen=True)
class RuntimeObservation:
    python: str
    spark: str
    java: str
    scala: str | None
    delta: str | None
    package_version: str
    release_digest: str


class Storage(Protocol):
    def exists(self, relative: str) -> bool: ...

    def mkdir_exclusive(self, relative: str) -> None: ...

    def write_exclusive(self, relative: str, content: bytes) -> None: ...

    def read(self, relative: str) -> bytes: ...

    def delete_exact(self, relative: str, *, recursive: bool = False) -> None: ...


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_json(value: object) -> str:
    return sha256_bytes(canonical_bytes(value))


def _uuid(value: str, name: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError) as error:
        raise CanaryValidationError(f"{name} must be a canonical UUID") from error
    canonical = str(parsed)
    if value != canonical:
        raise CanaryValidationError(f"{name} must be a canonical lowercase UUID")
    return value


def _strict_scope(value: str) -> str:
    pure = PurePosixPath(value)
    if (
        value != WRITE_SCOPE
        or value.startswith("/")
        or ".." in pure.parts
        or "Tables" in pure.parts
        or "/Tables/" in f"/{value}/"
        or "\\" in value
        or "://" in value
        or "@" in value
    ):
        raise CanaryValidationError(
            f"write_scope must equal the relative canary scope {WRITE_SCOPE!r}"
        )
    return value


def validate_arguments(
    workspace_id: str,
    lakehouse_id: str,
    run_id: str,
    write_scope: str,
    safety_token: str,
    release_digest: str,
    project_version: str,
    package_identity: str,
    definition_sha256: str,
    source_archive_sha256: str,
    *,
    partitions: int = MINIMUM_PARTITIONS,
) -> CanaryArguments:
    """Require the exact reviewed canary identity and a real run UUID."""
    if _uuid(workspace_id, "workspace_id") != WORKSPACE_ID:
        raise CanaryValidationError("workspace_id does not match the canary workspace")
    if _uuid(lakehouse_id, "lakehouse_id") != LAKEHOUSE_ID:
        raise CanaryValidationError("lakehouse_id does not match people_counter_dev")
    _uuid(run_id, "run_id")
    _strict_scope(write_scope)
    if safety_token != SAFETY_TOKEN:
        raise CanaryValidationError("safety token mismatch")
    if not isinstance(release_digest, str) or not HEX64.fullmatch(release_digest):
        raise CanaryValidationError(
            "release_digest must be 64 lowercase hexadecimal characters"
        )
    if not isinstance(project_version, str) or not project_version.strip():
        raise CanaryValidationError("project_version must be a non-empty string")
    if not isinstance(package_identity, str) or not HEX64.fullmatch(package_identity):
        raise CanaryValidationError(
            "package_identity must be 64 lowercase hexadecimal characters"
        )
    if not isinstance(definition_sha256, str) or not HEX64.fullmatch(
        definition_sha256
    ):
        raise CanaryValidationError(
            "definition_sha256 must be 64 lowercase hexadecimal characters"
        )
    if not isinstance(source_archive_sha256, str) or not HEX64.fullmatch(
        source_archive_sha256
    ):
        raise CanaryValidationError(
            "source_archive_sha256 must be 64 lowercase hexadecimal characters"
        )
    if type(partitions) is not int or partitions < MINIMUM_PARTITIONS or partitions > 64:
        raise CanaryValidationError("partitions must be an integer between 2 and 64")
    return CanaryArguments(
        workspace_id,
        lakehouse_id,
        run_id,
        write_scope,
        safety_token,
        release_digest,
        project_version,
        package_identity,
        definition_sha256,
        source_archive_sha256,
        partitions,
    )


def qualified_abfss(arguments: CanaryArguments, relative: str) -> str:
    """Pin a relative Lakehouse path to both fixed Fabric identities."""
    if not relative.startswith("Files/") or "\\" in relative or "://" in relative:
        raise CanaryValidationError("qualified path must be Files-relative")
    if ".." in PurePosixPath(relative).parts:
        raise CanaryValidationError("qualified path escapes the fixed Lakehouse")
    return (
        f"abfss://{arguments.workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{arguments.lakehouse_id}/{relative}"
    )


def safe_child(run_root: str, suffix: str) -> str:
    """Return one qualified child and reject path escapes or alternate roots."""
    root = PurePosixPath(run_root)
    child = PurePosixPath(suffix)
    if (
        not run_root.startswith(f"{FILES_PREFIX}/run=")
        or child.is_absolute()
        or not suffix
        or ".." in child.parts
        or "Tables" in child.parts
        or "\\" in suffix
        or "://" in suffix
    ):
        raise CanaryValidationError("path is outside the exact canary run root")
    combined = root / child
    if combined.parts[: len(root.parts)] != root.parts:
        raise CanaryValidationError("path escaped the exact canary run root")
    return combined.as_posix()


class LocalCanaryStorage:
    """Local filesystem analogue with create-only writes and strict confinement."""

    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()

    def _path(self, relative: str) -> Path:
        if PurePosixPath(relative).is_absolute() or ".." in PurePosixPath(relative).parts:
            raise CanaryValidationError("storage path must be qualified and relative")
        candidate = (self.root / relative).resolve(strict=False)
        if candidate != self.root and self.root not in candidate.parents:
            raise CanaryValidationError("storage path escaped the configured root")
        return candidate

    def exists(self, relative: str) -> bool:
        return self._path(relative).exists()

    def mkdir_exclusive(self, relative: str) -> None:
        try:
            self._path(relative).mkdir(parents=True, exist_ok=False)
        except FileExistsError as error:
            raise RunRootExistsError(f"run root already exists: {relative}") from error

    def write_exclusive(self, relative: str, content: bytes) -> None:
        path = self._path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with path.open("xb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError as error:
            raise CanaryValidationError(f"immutable path already exists: {relative}") from error

    def read(self, relative: str) -> bytes:
        return self._path(relative).read_bytes()

    def delete_exact(self, relative: str, *, recursive: bool = False) -> None:
        path = self._path(relative)
        if recursive:
            shutil.rmtree(path)
        else:
            path.unlink()


def prepare_run_root(storage: Storage, arguments: CanaryArguments) -> str:
    run_root = arguments.relative_run_root
    if storage.exists(run_root):
        raise RunRootExistsError(f"run root already exists: {run_root}")
    storage.mkdir_exclusive(run_root)
    return run_root


def _version_prefix(value: str, expected: tuple[int, int], name: str) -> None:
    match = re.search(r"(\d+)\.(\d+)", value)
    if match is None or tuple(map(int, match.groups())) != expected:
        raise CanaryValidationError(
            f"{name} must be {expected[0]}.{expected[1]}.x, observed {value!r}"
        )


def validate_runtime(observation: RuntimeObservation) -> RuntimeObservation:
    _version_prefix(observation.python, (3, 13), "Python")
    _version_prefix(observation.spark, (4, 1), "Spark")
    if not re.search(r"(?:^|\D)21(?:\.|\D|$)", observation.java):
        raise CanaryValidationError(f"Java must be 21, observed {observation.java!r}")
    if observation.scala is not None:
        _version_prefix(observation.scala, (2, 13), "Scala")
    if observation.delta is not None:
        _version_prefix(observation.delta, (4, 2), "Delta")
    if not observation.package_version.strip():
        raise CanaryValidationError("people-counter package identity is missing")
    if not HEX64.fullmatch(observation.release_digest):
        raise CanaryValidationError("runtime release identity is invalid")
    return observation


def installed_package_identity() -> str:
    """Hash the installed canary and Candidate A executor source identities."""
    files = ("fabric_runtime2_canary.py", "sjd_process.py")
    return sha256_json(
        {
            name: sha256_bytes(
                importlib.resources.files("people_counter").joinpath(name).read_bytes()
            )
            for name in files
        }
    )


def observe_driver_runtime(
    spark: Any,
    release_digest: str,
    expected_project_version: str,
    *,
    package_version: Callable[[str], str] = importlib.metadata.version,
) -> RuntimeObservation:
    """Observe driver versions without importing Spark at module import time."""
    java = str(spark.sparkContext._jvm.java.lang.System.getProperty("java.version"))
    scala: str | None = None
    try:
        scala = str(spark.sparkContext._jvm.scala.util.Properties.versionNumberString())
    except Exception:
        pass
    delta: str | None
    try:
        delta = package_version("delta-spark")
    except importlib.metadata.PackageNotFoundError:
        delta = None
    observation = RuntimeObservation(
        python=platform.python_version(),
        spark=str(spark.version),
        java=java,
        scala=scala,
        delta=delta,
        package_version=package_version("people-counter"),
        release_digest=release_digest,
    )
    if observation.package_version != expected_project_version:
        raise CanaryValidationError(
            "installed people-counter version does not match the release manifest"
        )
    return validate_runtime(observation)


def _java_version() -> str:
    completed = subprocess.run(
        ["java", "-version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    output = "\n".join(part for part in (completed.stderr, completed.stdout) if part)
    for line in output.splitlines():
        if re.search(r"\b(?:openjdk|java)\s+version\b", line, re.IGNORECASE):
            return line
    return output


def _observable_scala_version() -> str | None:
    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home:
        return None
    jars = Path(spark_home) / "jars"
    matches = sorted(jars.glob("scala-library-2.13*.jar"))
    return "2.13" if matches else None


def _executor_runtime(
    release_digest: str,
    expected_project_version: str,
    expected_package_identity: str,
) -> RuntimeObservation:
    import pyspark

    for module in ("people_counter.sjd_process", "delta"):
        importlib.import_module(module)
    _executor_scratch_preflight()
    observation = validate_runtime(
        RuntimeObservation(
            python=platform.python_version(),
            spark=str(pyspark.__version__),
            java=_java_version(),
            scala=_observable_scala_version(),
            delta=_installed_delta_version(),
            package_version=importlib.metadata.version("people-counter"),
            release_digest=release_digest,
        )
    )
    _validate_package_identity(
        observation,
        expected_project_version,
        expected_package_identity,
        location="executor",
    )
    return observation


def _installed_delta_version() -> str | None:
    try:
        return importlib.metadata.version("delta-spark")
    except importlib.metadata.PackageNotFoundError:
        return None


def _executor_scratch_preflight() -> None:
    scratch = Path.cwd() / f".pc_canary_scratch_{uuid.uuid4()}"
    try:
        scratch.mkdir(exist_ok=False)
        test_file = scratch / "roundtrip"
        test_file.write_bytes(b"PC_CANARY_SCRATCH")
        if test_file.read_bytes() != b"PC_CANARY_SCRATCH":
            raise CanaryValidationError("executor scratch readback mismatch")
        test_file.unlink()
        scratch.rmdir()
    finally:
        if scratch.exists():
            shutil.rmtree(scratch)


def _validate_package_identity(
    observation: RuntimeObservation,
    expected_project_version: str,
    expected_package_identity: str,
    *,
    location: str,
) -> None:
    if observation.package_version != expected_project_version:
        raise CanaryValidationError(
            f"{location} people-counter version does not match the release manifest"
        )
    if installed_package_identity() != expected_package_identity:
        raise CanaryValidationError(
            f"{location} package source identity does not match the release manifest"
        )


def execute_canary_partition(
    rows: Iterable[Mapping[str, Any]],
) -> Iterable[dict[str, Any]]:
    """Serialized executor adapter around the installed Candidate A callable."""
    from pyspark import SparkFiles

    from people_counter.sjd_process import execute_sjd_partition

    materialized = [dict(row) for row in rows]
    if not materialized:
        return []
    release_digest = str(materialized[0]["release_digest"])
    expected_project_version = str(materialized[0]["project_version"])
    expected_package_identity = str(materialized[0]["package_identity"])
    runtime = _executor_runtime(
        release_digest,
        expected_project_version,
        expected_package_identity,
    )
    thread_id = threading.get_ident()
    cpu_count = os.cpu_count()
    source_by_work_id = {
        str(row["work_id"]): str(row["source_video"]) for row in materialized
    }
    records = execute_sjd_partition(
        _executor_local_probe_rows(materialized, SparkFiles)
    )
    return [
        {
            **record,
            "source_video": source_by_work_id[str(record["work_id"])],
            "executor_python": runtime.python,
            "executor_spark": runtime.spark,
            "executor_java": runtime.java,
            "executor_scala": runtime.scala,
            "executor_delta": runtime.delta,
            "package_version": runtime.package_version,
            "project_version": expected_project_version,
            "package_identity": expected_package_identity,
            "cpu_count": cpu_count,
            "thread_id": thread_id,
        }
        for record in records
    ]


def _executor_local_probe_rows(
    rows: Sequence[Mapping[str, Any]],
    spark_files: Any,
) -> list[dict[str, Any]]:
    """Resolve the hash-verified canary input distributed by Spark."""
    localized: list[dict[str, Any]] = []
    for row in rows:
        distributed_name = row.get("distributed_source_name")
        if (
            not isinstance(distributed_name, str)
            or not distributed_name
            or PurePosixPath(distributed_name).name != distributed_name
        ):
            raise CanaryValidationError("invalid distributed probe source name")
        localized.append(
            {
                **row,
                "source_video": spark_files.get(distributed_name),
            }
        )
    return localized


def _probe_row(
    arguments: CanaryArguments,
    partition: int,
    source_video: str,
    source_sha256: str,
    manifest_sha256: str,
) -> dict[str, Any]:
    payload = {
        "pipeline": "rtdetr-osnet",
        "source_video": source_video,
        "source_sha256": source_sha256,
        "batch_size": 1,
    }
    work_id = f"canary-partition-{partition:04d}"
    payload_sha256 = sha256_json(payload)
    return {
        **payload,
        "work_id": work_id,
        "attempt_id": f"{arguments.run_id}:{partition}",
        "batch_id": arguments.run_id,
        "process_attempt_id": arguments.run_id,
        "envelope_sha256": manifest_sha256,
        "manifest_sha256": manifest_sha256,
        "membership_sha256": manifest_sha256,
        "fence": 1,
        "input_payload_sha256": payload_sha256,
        "config_sha256": sha256_json({"mode": "probe", "schema": SCHEMA_VERSION}),
        "model_identity": sha256_json({"pipeline": "rtdetr-osnet", "mode": "probe"}),
        "release_digest": arguments.release_digest,
        "project_version": arguments.project_version,
        "package_identity": arguments.package_identity,
        "runtime_key": "fabric-runtime2:probe",
        "duration_seconds": 1.0,
        "planned_cost_seconds": 1.0,
        "planned_concurrency": arguments.partitions,
        "peak_rss_bytes": None,
        "duration_only_fallback": False,
        "bucket_id": partition,
        "wave_index": 0,
        "physical_partition": partition,
        "executor_cores": 1,
        "task_cpus": 1,
        "mode": "probe",
    }


def validate_probe_records(
    records: Sequence[Mapping[str, Any]],
    expected_rows: Sequence[Mapping[str, Any]],
    arguments: CanaryArguments,
) -> dict[str, Any]:
    """Validate exact membership, hashes, provenance, and task placement."""
    expected = {str(row["work_id"]): row for row in expected_rows}
    terminals: dict[str, list[Mapping[str, Any]]] = {key: [] for key in expected}
    task_identities: set[tuple[int, int, int]] = set()
    executors: set[str] = set()
    for record in records:
        work_id = str(record.get("work_id", ""))
        if work_id not in expected:
            raise CanaryValidationError(f"unexpected executor work identity: {work_id!r}")
        row = expected[work_id]
        _validate_record_matches(record, row, arguments, work_id)
        _validate_record_payload(record)
        task, identity = _record_provenance(record)
        if task in task_identities:
            raise CanaryValidationError("duplicate executor task identity")
        task_identities.add(task)
        executors.add(identity)
        if record.get("record_type") == "video_result":
            terminals[work_id].append(record)
    invalid = sorted(key for key, values in terminals.items() if len(values) != 1)
    if invalid:
        raise CanaryValidationError(
            f"expected one terminal per input identity: {invalid!r}"
        )
    if len(task_identities) != len(expected):
        raise CanaryValidationError("task identity membership mismatch")
    return {
        "executorIdentities": sorted(executors),
        "taskIdentities": [list(item) for item in sorted(task_identities)],
        "terminalCount": len(expected),
    }


def _validate_record_matches(
    record: Mapping[str, Any],
    row: Mapping[str, Any],
    arguments: CanaryArguments,
    work_id: str,
) -> None:
    checks = {
        "attempt_id": row["attempt_id"],
        "manifest_sha256": row["manifest_sha256"],
        "input_payload_sha256": row["input_payload_sha256"],
        "config_sha256": row["config_sha256"],
        "model_identity": row["model_identity"],
        "release_digest": arguments.release_digest,
        "project_version": arguments.project_version,
        "package_identity": arguments.package_identity,
        "physical_partition": row["physical_partition"],
        "partition_id": row["physical_partition"],
        "cpu_threads": 1,
        "status": "SUCCEEDED",
    }
    for name, expected_value in checks.items():
        if record.get(name) != expected_value:
            raise CanaryValidationError(f"executor {name} mismatch for {work_id!r}")


def _validate_record_payload(record: Mapping[str, Any]) -> None:
    try:
        payload = json.loads(str(record.get("payload_json", "")))
    except json.JSONDecodeError as error:
        raise CanaryValidationError("executor payload_json is invalid") from error
    if not isinstance(payload, dict):
        raise CanaryValidationError("executor payload_json must be an object")
    if record.get("record_payload_sha256") != sha256_json(payload):
        raise CanaryValidationError("executor record payload hash mismatch")


def _record_provenance(
    record: Mapping[str, Any],
) -> tuple[tuple[int, int, int], str]:
    host = record.get("executor_host")
    identity = record.get("executor_identity")
    if not isinstance(host, str) or not isinstance(identity, str):
        raise CanaryValidationError("executor host identity is missing")
    if identity.rpartition("@")[2] != host:
        raise CanaryValidationError("executor identity does not match its host")
    for name in ("stage_id", "partition_id", "task_attempt_id", "thread_id"):
        if type(record.get(name)) is not int or int(record[name]) < 0:
            raise CanaryValidationError(f"invalid executor {name}")
    return (
        (
            int(record["stage_id"]),
            int(record["partition_id"]),
            int(record["task_attempt_id"]),
        ),
        identity,
    )


def _artifact_documents(
    arguments: CanaryArguments,
    records: Sequence[Mapping[str, Any]],
    expected_rows: Sequence[Mapping[str, Any]],
    runtime: RuntimeObservation,
    lakehouse_observation: Mapping[str, str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    validation = validate_probe_records(records, expected_rows, arguments)
    records_hash = sha256_json([dict(record) for record in records])
    staging_identity = {
        "recordsPath": "attempts/records",
        "pointerDeltaPath": "pointer_delta",
        "runRoot": arguments.relative_run_root,
    }
    pointer = {
        "schemaVersion": SCHEMA_VERSION,
        "workspaceId": arguments.workspace_id,
        "lakehouseId": arguments.lakehouse_id,
        "writeScope": arguments.write_scope,
        "runId": arguments.run_id,
        "stagingIdentity": staging_identity,
        "recordsSha256": records_hash,
        "recordCount": len(records),
    }
    result = {
        "schemaVersion": SCHEMA_VERSION,
        "status": "SUCCEEDED",
        "workspaceId": arguments.workspace_id,
        "lakehouseId": arguments.lakehouse_id,
        "writeScope": arguments.write_scope,
        "runId": arguments.run_id,
        "releaseDigest": arguments.release_digest,
        "packageIdentity": arguments.package_identity,
        "sourceArchiveSha256": arguments.source_archive_sha256,
        "definitionSha256": arguments.definition_sha256,
        "runtime": runtime.__dict__,
        "provenance": validation,
        "pointerSha256": sha256_json(pointer),
        "recordsSha256": records_hash,
        "recordCount": len(records),
        "defaultLakehouseObservation": dict(lakehouse_observation or {}),
    }
    marker = {
        "schemaVersion": SCHEMA_VERSION,
        "status": "SUCCEEDED",
        "workspaceId": arguments.workspace_id,
        "lakehouseId": arguments.lakehouse_id,
        "writeScope": arguments.write_scope,
        "runId": arguments.run_id,
        "resultSha256": sha256_json(result),
        "pointerSha256": sha256_json(pointer),
        "recordsSha256": records_hash,
        "recordCount": len(records),
    }
    return marker, pointer, result


def validate_artifacts(
    storage: Storage,
    arguments: CanaryArguments,
    records: Sequence[Mapping[str, Any]],
    expected_rows: Sequence[Mapping[str, Any]],
    *,
    pointer_rows: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Reread and validate the complete acyclic result artifact chain."""
    root = arguments.relative_run_root
    documents: dict[str, dict[str, Any]] = {}
    for name in ("_SUCCESS", "pointer.json", "result.json"):
        try:
            value = json.loads(storage.read(safe_child(root, name)))
        except (OSError, json.JSONDecodeError) as error:
            raise CanaryValidationError(f"invalid canary artifact: {name}") from error
        if not isinstance(value, dict):
            raise CanaryValidationError(f"canary artifact must be an object: {name}")
        documents[name] = value
    marker, pointer, result = (
        documents["_SUCCESS"],
        documents["pointer.json"],
        documents["result.json"],
    )
    validation = validate_probe_records(records, expected_rows, arguments)
    records_hash = sha256_json([dict(record) for record in records])
    _validate_artifact_identities(marker, pointer, result, arguments)
    _validate_artifact_chain(
        marker, pointer, result, records_hash, len(records), validation
    )
    _validate_result_identity(result, arguments)
    _validate_result_runtime(result, arguments)
    observed_pointer_rows = (
        _read_local_pointer_rows(storage, root)
        if pointer_rows is None
        else pointer_rows
    )
    if [dict(row) for row in observed_pointer_rows] != [pointer]:
        raise CanaryValidationError("Delta pointer readback mismatch")
    return result


def _validate_artifact_identities(
    marker: Mapping[str, Any],
    pointer: Mapping[str, Any],
    result: Mapping[str, Any],
    arguments: CanaryArguments,
) -> None:
    identities = {
        "workspaceId": arguments.workspace_id,
        "lakehouseId": arguments.lakehouse_id,
        "writeScope": arguments.write_scope,
        "runId": arguments.run_id,
    }
    for document in (marker, pointer, result):
        for name, expected in identities.items():
            if document.get(name) != expected:
                raise CanaryValidationError(f"artifact {name} identity mismatch")
    expected_staging = {
        "recordsPath": "attempts/records",
        "pointerDeltaPath": "pointer_delta",
        "runRoot": arguments.relative_run_root,
    }
    if pointer.get("stagingIdentity") != expected_staging:
        raise CanaryValidationError("pointer staging identity mismatch")


def _validate_artifact_chain(
    marker: Mapping[str, Any],
    pointer: Mapping[str, Any],
    result: Mapping[str, Any],
    records_hash: str,
    record_count: int,
    validation: Mapping[str, Any],
) -> None:
    if pointer.get("recordsSha256") != records_hash:
        raise CanaryValidationError("pointer records hash mismatch")
    if result.get("pointerSha256") != sha256_json(pointer):
        raise CanaryValidationError("result pointer hash mismatch")
    if result.get("recordsSha256") != records_hash:
        raise CanaryValidationError("result records hash mismatch")
    if marker.get("resultSha256") != sha256_json(result):
        raise CanaryValidationError("success result hash mismatch")
    if marker.get("pointerSha256") != sha256_json(pointer):
        raise CanaryValidationError("success pointer hash mismatch")
    if marker.get("recordsSha256") != records_hash:
        raise CanaryValidationError("success records hash mismatch")
    if result.get("provenance") != validation:
        raise CanaryValidationError("result provenance summary mismatch")
    if result.get("status") != "SUCCEEDED" or marker.get("status") != "SUCCEEDED":
        raise CanaryValidationError("artifact status mismatch")
    if any(
        document.get("recordCount") != record_count
        for document in (marker, pointer, result)
    ):
        raise CanaryValidationError("artifact record count mismatch")


def _validate_result_identity(
    result: Mapping[str, Any],
    arguments: CanaryArguments,
) -> None:
    expected_result = {
        "releaseDigest": arguments.release_digest,
        "packageIdentity": arguments.package_identity,
        "sourceArchiveSha256": arguments.source_archive_sha256,
        "definitionSha256": arguments.definition_sha256,
    }
    for name, expected in expected_result.items():
        if result.get(name) != expected:
            raise CanaryValidationError(f"result {name} mismatch")


def _validate_result_runtime(
    result: Mapping[str, Any],
    arguments: CanaryArguments,
) -> None:
    runtime_value = result.get("runtime")
    if not isinstance(runtime_value, Mapping):
        raise CanaryValidationError("result runtime is missing")
    try:
        observed_runtime = RuntimeObservation(**runtime_value)
    except TypeError as error:
        raise CanaryValidationError("result runtime fields are invalid") from error
    validate_runtime(observed_runtime)
    if (
        observed_runtime.release_digest != arguments.release_digest
        or observed_runtime.package_version != arguments.project_version
    ):
        raise CanaryValidationError("result runtime identity mismatch")


def _read_local_pointer_rows(
    storage: Storage,
    root: str,
) -> list[Mapping[str, Any]]:
    try:
        pointer_row = json.loads(
            storage.read(safe_child(root, "pointer_delta/row.json"))
        )
    except (OSError, json.JSONDecodeError) as error:
        raise CanaryValidationError("invalid pointer Delta analogue") from error
    if not isinstance(pointer_row, Mapping):
        raise CanaryValidationError("invalid pointer Delta analogue")
    return [pointer_row]


def run_local_analogue(
    storage: LocalCanaryStorage,
    arguments: CanaryArguments,
    *,
    runtime: RuntimeObservation,
) -> dict[str, Any]:
    """Exercise immutable layout and validation without pretending JSON is Delta."""
    validate_runtime(runtime)
    if runtime.package_version != arguments.project_version:
        raise CanaryValidationError(
            "local people-counter version does not match the release manifest"
        )
    if installed_package_identity() != arguments.package_identity:
        raise CanaryValidationError(
            "local package source identity does not match the release manifest"
        )
    root = prepare_run_root(storage, arguments)
    input_content = b"people-counter-runtime2-canary-v1\n"
    input_path = safe_child(root, "input/probe.txt")
    storage.write_exclusive(input_path, input_content)
    rows = [
        _probe_row(
            arguments,
            index,
            str(storage._path(input_path)),
            sha256_bytes(input_content),
            sha256_json({"runId": arguments.run_id, "partitions": arguments.partitions}),
        )
        for index in range(arguments.partitions)
    ]
    records: list[dict[str, Any]] = []
    for row in rows:
        from people_counter.sjd_process import execute_sjd_partition

        injected = {
            **row,
            "_task_identity": {
                "stage_id": 1,
                "partition_id": row["physical_partition"],
                "task_attempt_id": row["physical_partition"],
                "attempt_number": 0,
                "executor_identity": f"local-{row['physical_partition']}@localhost",
                "executor_host": "localhost",
            },
        }
        records.extend(
            {
                **record,
                "thread_id": threading.get_ident(),
                "cpu_count": os.cpu_count(),
                "executor_python": runtime.python,
                "executor_spark": runtime.spark,
                "executor_java": runtime.java,
                "executor_scala": runtime.scala,
                "executor_delta": runtime.delta,
                "package_version": runtime.package_version,
                "project_version": arguments.project_version,
                "package_identity": arguments.package_identity,
            }
            for record in execute_sjd_partition([injected])
        )
    records = sorted(records, key=lambda item: str(item["work_id"]))
    storage.write_exclusive(
        safe_child(root, "attempts/records.json"),
        canonical_bytes(records),
    )
    storage.write_exclusive(
        safe_child(root, "attempts/_delta_log_analogue.json"),
        canonical_bytes({"format": "local-analogue", "recordCount": len(records)}),
    )
    marker, pointer, result = _artifact_documents(
        arguments, records, rows, runtime
    )
    storage.write_exclusive(
        safe_child(root, "pointer.json"), canonical_bytes(pointer)
    )
    storage.write_exclusive(
        safe_child(root, "pointer_delta/row.json"), canonical_bytes(pointer)
    )
    storage.write_exclusive(
        safe_child(root, "result.json"), canonical_bytes(result)
    )
    storage.write_exclusive(safe_child(root, "_SUCCESS"), canonical_bytes(marker))
    return validate_artifacts(storage, arguments, records, rows)


def _hadoop_storage(spark: Any, arguments: CanaryArguments) -> Storage:
    class HadoopStorage:
        def __init__(self) -> None:
            self.conf = spark.sparkContext._jsc.hadoopConfiguration()
            self.jvm = spark.sparkContext._jvm

        def _objects(self, relative: str) -> tuple[Any, Any]:
            path = self.jvm.org.apache.hadoop.fs.Path(
                qualified_abfss(arguments, relative)
            )
            return path.getFileSystem(self.conf), path

        def exists(self, relative: str) -> bool:
            fs, path = self._objects(relative)
            return bool(fs.exists(path))

        def mkdir_exclusive(self, relative: str) -> None:
            fs, path = self._objects(relative)
            if fs.exists(path) or not fs.mkdirs(path):
                raise RunRootExistsError(f"run root already exists: {relative}")

        def write_exclusive(self, relative: str, content: bytes) -> None:
            fs, path = self._objects(relative)
            stream = fs.create(path, False)
            try:
                stream.write(bytearray(content))
                stream.hflush()
            finally:
                stream.close()

        def read(self, relative: str) -> bytes:
            fs, path = self._objects(relative)
            stream = fs.open(path)
            try:
                expected = int(fs.getFileStatus(path).getLen())
                content = bytearray()
                while len(content) < expected:
                    value = int(stream.read())
                    if value < 0:
                        break
                    content.append(value)
                if len(content) != expected:
                    raise CanaryValidationError(
                        f"short read for {relative}: {len(content)} of {expected} bytes"
                    )
                return bytes(content)
            finally:
                stream.close()

        def delete_exact(self, relative: str, *, recursive: bool = False) -> None:
            fs, path = self._objects(relative)
            if not fs.delete(path, recursive):
                raise CanaryValidationError(f"failed to delete exact path: {relative}")

    return HadoopStorage()


def _normalized_binding_uuid(value: object, key: str) -> str:
    text = str(value).strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        text = text[1:-1].strip()
    text = text.strip("{}").lower()
    try:
        return str(uuid.UUID(text))
    except (ValueError, AttributeError) as error:
        raise CanaryValidationError(
            f"authoritative Fabric binding {key} is not a UUID"
        ) from error


def _all_spark_conf(spark: Any) -> dict[str, object]:
    try:
        values = spark.conf.getAll
        values = values() if callable(values) else values
    except Exception:
        return {}
    if not isinstance(values, Mapping):
        return {}
    return {str(key).lower(): value for key, value in values.items()}


def _authoritative_values(
    spark: Any,
    allowed_keys: frozenset[str],
) -> dict[str, str]:
    all_conf = _all_spark_conf(spark)
    observed: dict[str, str] = {}
    for key in sorted(allowed_keys):
        value = all_conf.get(key)
        if value is None:
            try:
                value = spark.conf.get(key)
            except Exception:
                continue
        observed[key] = _normalized_binding_uuid(value, key)
    return observed


def _require_binding_kind(
    observed: Mapping[str, str],
    expected: str,
    name: str,
) -> None:
    if not observed:
        raise CanaryValidationError(
            f"authoritative default {name} binding is not observable"
        )
    conflicts = {
        key: value for key, value in observed.items() if value != expected
    }
    if conflicts:
        raise CanaryValidationError(
            f"conflicting authoritative default {name} binding"
        )


def validate_default_lakehouse(
    spark: Any,
    storage: Storage,
    arguments: CanaryArguments,
) -> dict[str, str]:
    """Require explicit authoritative IDs before any persistent write."""
    lakehouses = _authoritative_values(spark, AUTHORITATIVE_LAKEHOUSE_KEYS)
    workspaces = _authoritative_values(spark, AUTHORITATIVE_WORKSPACE_KEYS)
    _require_binding_kind(lakehouses, arguments.lakehouse_id, "Lakehouse")
    _require_binding_kind(workspaces, arguments.workspace_id, "workspace")
    qualified = qualified_abfss(arguments, arguments.relative_run_root)
    expected_prefix = (
        f"abfss://{arguments.workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{arguments.lakehouse_id}/Files/"
    )
    if not qualified.startswith(expected_prefix):
        raise CanaryValidationError("qualified default Lakehouse path mismatch")
    return {**lakehouses, **workspaces}


def run_fabric_canary(arguments: CanaryArguments) -> dict[str, Any]:
    """Run the Spark/Delta canary only below its unique, create-only run root."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    storage = _hadoop_storage(spark, arguments)
    if (
        os.environ.get("PC_CANARY_SOURCE_ARCHIVE_SHA256")
        != arguments.source_archive_sha256
    ):
        raise CanaryValidationError(
            "runtime source archive identity does not match the release manifest"
        )
    runtime = observe_driver_runtime(
        spark,
        arguments.release_digest,
        arguments.project_version,
    )
    if installed_package_identity() != arguments.package_identity:
        raise CanaryValidationError(
            "driver package source identity does not match the release manifest"
        )
    lakehouse_observation = validate_default_lakehouse(spark, storage, arguments)
    root = prepare_run_root(storage, arguments)
    scratch = safe_child(root, "scratch/driver-roundtrip")
    storage.write_exclusive(scratch, b"scratch")
    if storage.read(scratch) != b"scratch":
        raise CanaryValidationError("driver scratch readback mismatch")
    storage.delete_exact(scratch)
    input_content = b"people-counter-runtime2-canary-v1\n"
    input_name = f"probe-{arguments.run_id}.txt"
    input_path = safe_child(root, f"input/{input_name}")
    storage.write_exclusive(input_path, input_content)
    input_uri = qualified_abfss(arguments, input_path)
    spark.sparkContext.addFile(input_uri)
    manifest_hash = sha256_json(
        {"runId": arguments.run_id, "partitions": arguments.partitions}
    )
    rows = []
    for index in range(arguments.partitions):
        row = _probe_row(
            arguments,
            index,
            input_path,
            sha256_bytes(input_content),
            manifest_hash,
        )
        row["distributed_source_name"] = input_name
        rows.append(row)
    readback = _write_and_read_delta_records(spark, root, rows, arguments)
    return _publish_fabric_result(
        spark,
        storage,
        arguments,
        runtime,
        lakehouse_observation,
        rows,
        readback,
    )


def _write_and_read_delta_records(
    spark: Any,
    root: str,
    rows: Sequence[Mapping[str, Any]],
    arguments: CanaryArguments,
) -> list[dict[str, Any]]:
    from pyspark.sql import types as T

    records_path = qualified_abfss(
        arguments, safe_child(root, "attempts/records")
    )
    rdd = spark.sparkContext.parallelize(rows, arguments.partitions)
    produced = rdd.mapPartitions(execute_canary_partition)
    (
        spark.createDataFrame(produced, schema=_probe_record_schema(T))
        .write.format("delta")
        .mode("errorifexists")
        .save(records_path)
    )
    readback = [
        row.asDict(recursive=True)
        for row in spark.read.format("delta").load(records_path).collect()
    ]
    readback.sort(key=lambda item: str(item["work_id"]))
    return readback


def _probe_record_schema(types: Any) -> Any:
    """Return the explicit Spark schema for executor probe records."""
    fields = (
        ("record_type", types.StringType(), False),
        ("work_id", types.StringType(), False),
        ("attempt_id", types.StringType(), False),
        ("source_video", types.StringType(), False),
        ("status", types.StringType(), False),
        ("payload_json", types.StringType(), False),
        ("error_type", types.StringType(), True),
        ("error_message", types.StringType(), True),
        ("retryable", types.BooleanType(), True),
        ("processed_frames", types.LongType(), True),
        ("processing_seconds", types.DoubleType(), True),
        ("emitted_at_utc", types.StringType(), False),
        ("batch_id", types.StringType(), False),
        ("process_attempt_id", types.StringType(), False),
        ("envelope_sha256", types.StringType(), False),
        ("manifest_sha256", types.StringType(), False),
        ("membership_sha256", types.StringType(), False),
        ("fence", types.LongType(), False),
        ("input_payload_sha256", types.StringType(), False),
        ("record_payload_sha256", types.StringType(), False),
        ("config_sha256", types.StringType(), False),
        ("model_identity", types.StringType(), False),
        ("release_digest", types.StringType(), False),
        ("runtime_key", types.StringType(), False),
        ("duration_seconds", types.DoubleType(), False),
        ("planned_cost_seconds", types.DoubleType(), False),
        ("planned_concurrency", types.LongType(), False),
        ("peak_rss_bytes", types.LongType(), True),
        ("duration_only_fallback", types.BooleanType(), False),
        ("bucket_id", types.LongType(), False),
        ("wave_index", types.LongType(), False),
        ("physical_partition", types.LongType(), False),
        ("physical_partition_id", types.LongType(), False),
        ("stage_id", types.LongType(), False),
        ("partition_id", types.LongType(), False),
        ("task_attempt_id", types.LongType(), False),
        ("task_attempt_number", types.LongType(), False),
        ("executor_identity", types.StringType(), False),
        ("executor_host", types.StringType(), False),
        ("record_sequence", types.LongType(), False),
        ("detector_batch_size", types.LongType(), False),
        ("cpu_threads", types.LongType(), False),
        ("runtime_loads", types.LongType(), False),
        ("runtime_cache_hits", types.LongType(), False),
        ("executor_python", types.StringType(), False),
        ("executor_spark", types.StringType(), False),
        ("executor_java", types.StringType(), False),
        ("executor_scala", types.StringType(), True),
        ("executor_delta", types.StringType(), True),
        ("package_version", types.StringType(), False),
        ("project_version", types.StringType(), False),
        ("package_identity", types.StringType(), False),
        ("cpu_count", types.LongType(), True),
        ("thread_id", types.LongType(), False),
    )
    return types.StructType(
        [
            types.StructField(name, data_type, nullable=nullable)
            for name, data_type, nullable in fields
        ]
    )


def _publish_fabric_result(
    spark: Any,
    storage: Storage,
    arguments: CanaryArguments,
    runtime: RuntimeObservation,
    lakehouse_observation: Mapping[str, str],
    rows: Sequence[Mapping[str, Any]],
    readback: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    root = arguments.relative_run_root
    marker, pointer, result = _artifact_documents(
        arguments,
        readback,
        rows,
        runtime,
        lakehouse_observation,
    )
    storage.write_exclusive(
        safe_child(root, "pointer.json"), canonical_bytes(pointer)
    )
    pointer_delta_path = qualified_abfss(
        arguments, safe_child(root, "pointer_delta")
    )
    (
        spark.createDataFrame([pointer])
        .write.format("delta")
        .mode("errorifexists")
        .save(pointer_delta_path)
    )
    pointer_rows = [
        row.asDict(recursive=True)
        for row in spark.read.format("delta")
        .load(pointer_delta_path)
        .collect()
    ]
    if pointer_rows != [pointer]:
        raise CanaryValidationError("Delta pointer readback mismatch")
    storage.write_exclusive(
        safe_child(root, "result.json"), canonical_bytes(result)
    )
    storage.write_exclusive(safe_child(root, "_SUCCESS"), canonical_bytes(marker))
    records_path = qualified_abfss(
        arguments, safe_child(root, "attempts/records")
    )
    live_records = [
        row.asDict(recursive=True)
        for row in spark.read.format("delta").load(records_path).collect()
    ]
    live_records.sort(key=lambda item: str(item["work_id"]))
    live_pointer_rows = [
        row.asDict(recursive=True)
        for row in spark.read.format("delta").load(pointer_delta_path).collect()
    ]
    return validate_artifacts(
        storage,
        arguments,
        live_records,
        rows,
        pointer_rows=live_pointer_rows,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--lakehouse-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--write-scope", required=True)
    parser.add_argument("--safety-token", required=True)
    parser.add_argument("--release-digest", required=True)
    parser.add_argument("--project-version", required=True)
    parser.add_argument("--package-identity", required=True)
    parser.add_argument("--definition-sha256", required=True)
    parser.add_argument("--source-archive-sha256", required=True)
    parser.add_argument("--partitions", type=int, default=MINIMUM_PARTITIONS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        arguments = validate_arguments(
            args.workspace_id,
            args.lakehouse_id,
            args.run_id,
            args.write_scope,
            args.safety_token,
            args.release_digest,
            args.project_version,
            args.package_identity,
            args.definition_sha256,
            args.source_archive_sha256,
            partitions=args.partitions,
        )
        result = run_fabric_canary(arguments)
    except Exception as error:
        print(
            json.dumps(
                {
                    "event": "canary_failed",
                    "errorType": type(error).__name__,
                    "message": str(error),
                    "status": "FAILED",
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps({"event": "canary_succeeded", **result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
