"""Preview, publish, and explicitly register a prepared manifest package."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shlex
import sys
import time
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import UUID

from people_counter.adls import AzureDataLakeStorage
from people_counter.customer_demo import read_demo_delta
from people_counter.fabric_automation import (
    AzureCliResourceTokenProvider,
    FabricAutomationClient,
)
from people_counter.fabric_release_provenance import resolve_published_release
from people_counter.fabric_rest import UrllibTransport
from people_counter.manifest import manifest_bytes, normalize_relative_path
from people_counter.manifest_publisher import (
    FILESYSTEM_NAME,
    STORAGE_ACCOUNT_NAME,
    ManifestPackagePublishConfig,
    ValidatedManifestPackage,
    publish_manifest_package,
    validate_manifest_package,
)
from people_counter.sjd_control import normalize_registration_requests

PARTITION_SIZE = 1000
INTAKE_ROOT = "Files/people-counter/sjd/v1/intake/backfill"
_DIGEST = re.compile(r"[0-9a-f]{64}")
_SOURCE_FIELDS = {
    "source_video", "source_sha256", "line", "counting_line",
    "camera_id", "location_id", "captured_at_utc", "camera_timezone",
}


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, allow_nan=False, indent=2) + "\n").encode()


def _digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _progress(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


@dataclass(frozen=True)
class BackfillConfig:
    workspace_id: str
    lakehouse_id: str
    environment_id: str
    control_sjd_id: str
    storage_account: str
    filesystem: str
    shortcut_path: str
    config_sha256: str
    profile: dict[str, Any]
    table_directory: str = "Tables/dbo"


def load_config(path: Path) -> BackfillConfig:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or raw.get("intake_mode") != "explicit":
        raise ValueError("configuration must declare intake_mode=explicit; disable competing arrival intake")
    values = dict(raw)
    del values["intake_mode"]
    values.setdefault("table_directory", "Tables/dbo")
    expected = set(BackfillConfig.__dataclass_fields__)
    if set(values) != expected:
        raise ValueError(f"configuration must contain {sorted(expected - {'table_directory'})} and intake_mode; table_directory is optional")
    directory = values["table_directory"]
    if not isinstance(directory, str) or re.fullmatch(
        r"Tables(?:/[A-Za-z0-9][A-Za-z0-9._=-]*)?", directory
    ) is None:
        raise ValueError("table_directory must be Tables or Tables/<schema>")
    for name in ("workspace_id", "lakehouse_id", "environment_id", "control_sjd_id"):
        if not isinstance(values[name], str):
            raise ValueError(f"{name} must be a UUID")
        values[name] = str(UUID(values[name]))
    for name, pattern in (
        ("storage_account", STORAGE_ACCOUNT_NAME),
        ("filesystem", FILESYSTEM_NAME),
        ("config_sha256", _DIGEST),
    ):
        if not isinstance(values[name], str) or pattern.fullmatch(values[name]) is None:
            raise ValueError(f"invalid {name}")
    shortcut = values["shortcut_path"]
    if not isinstance(shortcut, str) or not shortcut.startswith("Files/"):
        raise ValueError("shortcut_path must be a Lakehouse Files/ path")
    normalize_relative_path(shortcut)
    values["shortcut_path"] = shortcut.rstrip("/")
    profile = values["profile"]
    required = {
        "pipeline", "detector_model", "model_format", "device_variant",
        "batch_size", "sample_fps", "profile_id", "runtime_key",
        "model_artifact_sha256",
    }
    if not isinstance(profile, dict) or not required.issubset(profile):
        raise ValueError(f"profile must contain {sorted(required)}")
    if _SOURCE_FIELDS.intersection(profile):
        raise ValueError("profile cannot override per-video source metadata")
    for name in (
        "pipeline", "detector_model", "model_format", "device_variant",
        "profile_id", "runtime_key",
    ):
        if not isinstance(profile[name], str) or not profile[name].strip():
            raise ValueError(f"profile.{name} must be nonempty text")
    if type(profile["batch_size"]) is not int or profile["batch_size"] < 1:
        raise ValueError("profile.batch_size must be a positive integer")
    fps = profile["sample_fps"]
    if type(fps) not in (int, float) or not 0 < fps < float("inf"):
        raise ValueError("profile.sample_fps must be positive and finite")
    artifacts = profile["model_artifact_sha256"]
    if not isinstance(artifacts, dict) or not artifacts:
        raise ValueError("profile.model_artifact_sha256 must contain approved model hashes")
    for name, digest in artifacts.items():
        normalize_relative_path(name)
        if not isinstance(digest, str) or _DIGEST.fullmatch(digest) is None:
            raise ValueError(f"invalid model artifact digest: {name}")
    _json_bytes(values)
    return BackfillConfig(**values)


def publication_config(
    package: Path, video_root: Path, state: Path, config: BackfillConfig,
) -> ManifestPackagePublishConfig:
    return ManifestPackagePublishConfig(
        manifest_package_dir=package,
        video_root=video_root,
        storage_account=config.storage_account,
        filesystem=config.filesystem,
        staging_prefix="staging",
        incoming_prefix="incoming",
        checkpoint_path=state / "publication.sqlite3",
        rejection_report_path=state / "publication-rejections.csv",
        summary_report_path=state / "publication-summary.json",
        chunk_size=8 * 1024 * 1024,
    )


def registration_records(
    package: ValidatedManifestPackage, config: BackfillConfig, release_digest: str,
) -> list[dict[str, Any]]:
    records = []
    for plan in sorted(package.plans, key=lambda value: value.relative_path):
        payload = {
            **config.profile,
            "source_video": (
                f"/lakehouse/default/{config.shortcut_path}/{plan.incoming_video_path}"
            ),
            "source_sha256": plan.inspection.sha256,
            "line": list(plan.catalog.counting_line),
            "camera_id": plan.catalog.camera_id,
            "location_id": plan.catalog.location_id,
            "captured_at_utc": plan.captured_at_utc.isoformat().replace("+00:00", "Z"),
            "camera_timezone": plan.catalog.camera_timezone,
        }
        record = {
            "payload": payload,
            "runtime_key": config.profile["runtime_key"],
            "duration_seconds": plan.inspection.duration_seconds,
            "config_sha256": config.config_sha256,
            "release_digest": release_digest,
            "max_attempts": 3,
        }
        identity = {
            "workspace_id": config.workspace_id,
            "lakehouse_id": config.lakehouse_id,
            "asset_id": plan.asset_id,
            "asset_version": plan.asset_version,
            "record": record,
        }
        records.append({"work_id": f"backfill-{_digest(_json_bytes(identity))}", **record})
    return normalize_registration_requests(records)


class OneLakeFiles:
    """OneLake access under one explicit workspace and Lakehouse binding."""

    def __init__(self, config: BackfillConfig) -> None:
        from azure.core.exceptions import ResourceExistsError
        from azure.identity import DefaultAzureCredential
        from azure.storage.filedatalake import DataLakeServiceClient

        self._exists_error = ResourceExistsError
        self._lakehouse = config.lakehouse_id
        service = DataLakeServiceClient(
            "https://onelake.dfs.fabric.microsoft.com",
            credential=DefaultAzureCredential(),
        )
        self._filesystem = service.get_file_system_client(config.workspace_id)

    def _file(self, path: str) -> Any:
        return self._filesystem.get_file_client(f"{self._lakehouse}/{path}")

    def read_bytes(self, path: str) -> bytes:
        return self._file(path).download_file().readall()

    def read_text(self, path: str) -> str:
        return self.read_bytes(path).decode("utf-8")

    def verify_video(self, path: str, expected_size: int | None, expected_sha256: str) -> None:
        digest = hashlib.sha256()
        size = 0
        for chunk in self._file(path).download_file().chunks():
            size += len(chunk)
            digest.update(chunk)
        if (expected_size is not None and size != expected_size) or digest.hexdigest() != expected_sha256:
            raise ValueError(f"OneLake video size or SHA-256 mismatch: {path}")

    def write_partition(self, path: str, content: bytes) -> None:
        for directory in reversed(PurePosixPath(path).parents):
            if str(directory) == ".":
                continue
            try:
                self._filesystem.create_directory(f"{self._lakehouse}/{directory}")
            except self._exists_error:
                pass
        try:
            self._file(path).upload_data(content, overwrite=False)
        except self._exists_error:
            if self.read_bytes(path) != content:
                raise ValueError(f"immutable registration partition conflicts: {path}")
        if self.read_bytes(path) != content:
            raise ValueError(f"registration partition readback mismatch: {path}")


def _save(path: Path, value: object) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(_json_bytes(value))
    temporary.replace(path)


def _table_prefix(config: BackfillConfig) -> str:
    return (
        f"abfss://{config.workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{config.lakehouse_id}/{config.table_directory}/people_counter_sjd_"
    )


_WORK_COLUMNS = ["work_id", "status", "committed_attempt_id", "last_error"]


@contextmanager
def processing_reader(config: BackfillConfig) -> Iterator[Any]:
    try:
        import duckdb
    except ImportError as error:
        raise RuntimeError(
            "--wait requires the local Delta reader. Install with "
            "`uv sync --extra publisher --extra demo`."
        ) from error
    connection = duckdb.connect()
    try:
        _progress("Checking processing monitor access to the OneLake work table...")
        connection.execute(
            "INSTALL azure; LOAD azure; INSTALL delta; LOAD delta; "
            "SET azure_transport_option_type='curl';"
        )
        connection.execute(
            "CREATE SECRET onelake (TYPE azure, PROVIDER credential_chain, "
            "CHAIN 'cli', ACCOUNT_NAME 'onelake', "
            "ENDPOINT 'https://onelake.blob.fabric.microsoft.com')"
        )
        read_demo_delta(
            connection, _table_prefix(config), "work", _WORK_COLUMNS,
            [("work_id", "in", ["backfill-monitor-preflight"])],
        )
        yield connection
    finally:
        connection.close()


def processing_progress(
    work_ids: Sequence[str], rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    expected = set(work_ids)
    if not expected or len(expected) != len(work_ids):
        raise ValueError("processing monitor requires nonempty, unique work IDs")
    by_id: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        work_id = row["work_id"]
        if work_id not in expected:
            continue
        if work_id in by_id:
            raise RuntimeError(f"Duplicate processing rows for work ID {work_id}")
        by_id[work_id] = row
    counts: Counter[str] = Counter()
    failures = []
    committed = {}
    for work_id in work_ids:
        row = by_id.get(work_id)
        if row is None:
            counts["WAITING_REGISTRATION"] += 1
            continue
        status = row["status"]
        if not isinstance(status, str) or not status:
            raise ValueError(f"Invalid work status for {work_id}: {status!r}")
        attempt = row["committed_attempt_id"]
        if status == "SUCCEEDED":
            if isinstance(attempt, str) and attempt.strip():
                committed[work_id] = attempt
            else:
                status = "WAITING_COMMIT"
        counts[status] += 1
        if status in {"DEAD", "DEAD_LETTER", "FAILED", "QUARANTINED", "CANCELLED"}:
            failures.append({
                "work_id": work_id, "status": status, "last_error": row.get("last_error"),
            })
    return {
        "item_count": len(work_ids), "completed": len(committed),
        "processing_complete": len(committed) == len(work_ids),
        "counts": dict(sorted(counts.items())), "failures": failures,
        "committed_attempts": committed,
    }


def wait_for_processing(
    connection: Any, config: BackfillConfig, state_dir: Path,
) -> dict[str, Any]:
    plan = json.loads((state_dir / "registration-plan.json").read_text(encoding="utf-8"))
    work_ids = [record["work_id"] for record in plan["items"]]
    _progress("Waiting for backfill processing. Ctrl+C stops local monitoring; Fabric jobs continue.")
    while True:
        rows = []
        for offset in range(0, len(work_ids), PARTITION_SIZE):
            rows.extend(read_demo_delta(
                connection, _table_prefix(config), "work", _WORK_COLUMNS,
                [("work_id", "in", work_ids[offset:offset + PARTITION_SIZE])],
            ))
        report = processing_progress(work_ids, rows)
        report["status"] = (
            "failed" if report["failures"]
            else "processed" if report["processing_complete"] else "processing"
        )
        report["next_step"] = "Verify committed Gold results, Reconciliation, and Refresh separately."
        _save(state_dir / "processing-summary.json", report)
        statuses = ", ".join(f"{key}={value}" for key, value in report["counts"].items())
        _progress(f"Processing: {report['completed']}/{len(work_ids)} committed; {statuses}")
        if report["failures"]:
            raise RuntimeError(f"Backfill processing failed: {json.dumps(report['failures'])}")
        if report["processing_complete"]:
            return report
        time.sleep(15)


def _register_partition(
    client: FabricAutomationClient,
    config: BackfillConfig,
    partition_path: str,
    content: bytes,
    count: int,
    state_path: Path,
) -> dict[str, Any]:
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    if state.get("status") == "Completed":
        _progress(f"Registration {Path(partition_path).name}: Completed (previous submission)")
        return state
    if state.get("status") == "submitting":
        raise RuntimeError(
            f"submission outcome unknown; inspect Fabric job history before recovering {state_path}"
        )
    location = state.get("location")
    if location is None:
        _progress(f"Submitting registration {Path(partition_path).name}: {count} items")
        arguments = shlex.join([
            "--manual-artifacts", config.workspace_id, config.lakehouse_id,
            config.environment_id,
            "bulk-register", "--partition-path", partition_path,
            "--partition-sha256", _digest(content), "--max-items", str(count),
        ])
        state = {
            "status": "submitting", "partition_path": partition_path,
            "partition_sha256": _digest(content), "item_count": count,
        }
        _save(state_path, state)
        _, headers, body = client.request_response(
            "POST",
            f"/workspaces/{config.workspace_id}/sparkJobDefinitions/"
            f"{config.control_sjd_id}/jobs/sparkjob/instances",
            value={"executionData": {"commandLineArguments": arguments}},
            expected=(202,),
        )
        location = next(
            (value for key, value in headers.items() if key.lower() == "location"), None
        )
        if not location:
            response = json.loads(body) if body else {}
            job_id = response.get("id") or response.get("jobInstanceId")
            if not isinstance(job_id, str) or not job_id:
                raise RuntimeError("Fabric submission omitted the job ID and Location; inspect job history")
            location = (
                f"/workspaces/{config.workspace_id}/items/{config.control_sjd_id}"
                f"/jobs/instances/{job_id}"
            )
        state.update(status="submitted", location=location)
        _save(state_path, state)
    _progress(f"Registration job: {location}")
    deadline = time.monotonic() + 1800
    while time.monotonic() < deadline:
        job = client.request_json("GET", location)
        status = job.get("status")
        state.update(status=status, job=job)
        _save(state_path, state)
        _progress(f"Registration {Path(partition_path).name}: {status}")
        if status == "Completed":
            return state
        if status in {"Failed", "Cancelled", "Canceled", "Deduped"}:
            raise RuntimeError(f"Control registration job {status}: {job}")
        time.sleep(15)
    raise TimeoutError(f"Control registration timed out; rerun to resume polling {location}")


def submit(
    package: ValidatedManifestPackage,
    publication: ManifestPackagePublishConfig,
    config: BackfillConfig,
    state_dir: Path,
) -> dict[str, Any]:
    _progress("Verifying published release and model artifacts...")
    files = OneLakeFiles(config)
    release = resolve_published_release(files.read_text)
    if release.manifest.environment_id != config.environment_id:
        raise ValueError("published release Environment does not match the configured Environment")
    for name, digest in config.profile["model_artifact_sha256"].items():
        files.verify_video(f"Files/models/{name}", None, digest)
    records = registration_records(package, config, release.manifest_sha256)
    plan = {
        "target": {
            "workspace_id": config.workspace_id,
            "lakehouse_id": config.lakehouse_id,
            "environment_id": config.environment_id,
            "control_sjd_id": config.control_sjd_id,
            "storage_account": config.storage_account,
            "filesystem": config.filesystem,
        },
        "package_sha256": _digest(package.index_bytes),
        "items": records,
    }
    plan_bytes = _json_bytes(plan)
    state_dir.mkdir(parents=True, exist_ok=True)
    plan_path = state_dir / "registration-plan.json"
    if plan_path.exists() and plan_path.read_bytes() != plan_bytes:
        raise ValueError("state directory belongs to a different package, target, profile, or release")
    _save(plan_path, plan)
    storage = AzureDataLakeStorage(
        config.storage_account, config.filesystem, chunk_size=publication.chunk_size
    )
    _progress(f"Publishing {len(records)} videos and manifests (or verifying existing publications)...")
    summary = publish_manifest_package(publication, storage, validated_package=package)
    if summary.rejected or summary.published + summary.already_published != len(records):
        raise RuntimeError("publication incomplete; inspect publication-rejections.csv before retrying")
    _progress(
        f"Publication complete: {summary.published} published, "
        f"{summary.already_published} already published."
    )
    evidence = []
    for source, record in zip(
        sorted(package.plans, key=lambda value: value.relative_path), records
    ):
        remote = storage.stat(source.incoming_video_path)
        if remote is None:
            raise ValueError(f"published video missing: {source.incoming_video_path}")
        expected = manifest_bytes(
            source, storage_account=config.storage_account,
            filesystem=config.filesystem, source_etag=remote.etag,
        )
        shortcut_manifest = f"{config.shortcut_path}/{source.incoming_manifest_path}"
        if files.read_bytes(shortcut_manifest) != expected:
            raise ValueError(f"shortcut manifest does not match the publication: {shortcut_manifest}")
        files.verify_video(
            f"{config.shortcut_path}/{source.incoming_video_path}",
            source.inspection.size_bytes, source.inspection.sha256,
        )
        evidence.append({
            "work_id": record["work_id"],
            "manifest_path": shortcut_manifest,
            "source_manifest": json.loads(expected),
        })
        _progress(f"Verified Lakehouse source {len(evidence)}/{len(package.plans)}: {source.relative_path}")
    _save(state_dir / "publication-evidence.json", evidence)
    client = FabricAutomationClient(
        AzureCliResourceTokenProvider("https://api.fabric.microsoft.com"),
        transport=UrllibTransport(),
    )
    jobs = []
    root = f"{INTAKE_ROOT}/{_digest(plan_bytes)}"
    for offset in range(0, len(records), PARTITION_SIZE):
        partition = records[offset:offset + PARTITION_SIZE]
        name = f"partition-{offset // PARTITION_SIZE + 1:05d}"
        content = _json_bytes({"items": partition})
        path = f"{root}/{name}.json"
        files.write_partition(path, content)
        (state_dir / f"{name}.json").write_bytes(content)
        jobs.append(_register_partition(
            client, config, path, content, len(partition), state_dir / f"{name}-job.json"
        ))
    report = {
        "status": "registered", "item_count": len(records),
        "partition_count": len(jobs), "processing_complete": False,
        "jobs": jobs,
        "next_step": "Monitor Dispatcher, committed work, Reconciliation, Gold, and Refresh.",
    }
    _save(state_dir / "registration-summary.json", report)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-package-dir", type=Path, required=True)
    parser.add_argument("--video-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument(
        "--submit", action="store_true",
        help="Publish videos/manifests and register explicit backfill work (default: offline preview).",
    )
    parser.add_argument(
        "--wait", action="store_true",
        help="With --submit, monitor every registered work ID until processing commits. "
        "Ignored for offline preview. Ctrl+C stops monitoring, not Fabric jobs.",
    )
    args = parser.parse_args(argv)
    config = load_config(args.config)
    package_dir = args.manifest_package_dir.resolve()
    state = args.state_dir.resolve()
    if state == package_dir or state.is_relative_to(package_dir):
        raise ValueError("--state-dir must be outside the immutable manifest package")
    publication = publication_config(package_dir, args.video_root, state, config)
    package = validate_manifest_package(publication)
    count = len(package.plans)
    if not count:
        raise ValueError("backfill package must contain at least one video")
    if args.submit:
        try:
            if args.wait:
                with processing_reader(config) as connection:
                    report = submit(package, publication, config, state)
                    report.update(wait_for_processing(connection, config, state))
            else:
                report = submit(package, publication, config, state)
        except KeyboardInterrupt:
            _progress(
                "Local monitoring stopped. Accepted Fabric jobs continue; no cancellation was sent. "
                "Some partitions may not yet be submitted. "
                "Repeat the same command and state directory with --submit --wait to resume."
            )
            print(json.dumps({"status": "monitoring_stopped", "processing_complete": False}))
            return 130
    else:
        report = {
            "status": "preview", "item_count": count,
            "partition_count": (count + PARTITION_SIZE - 1) // PARTITION_SIZE,
            "workspace_id": config.workspace_id, "lakehouse_id": config.lakehouse_id,
            "control_sjd_id": config.control_sjd_id,
            "destination": f"abfss://{config.filesystem}@{config.storage_account}.dfs.core.windows.net/incoming/",
            "profile_id": config.profile["profile_id"],
            "package_sha256": _digest(package.index_bytes),
            "next_step": "Review the target and profile, then repeat with --submit. Cloud checks run only on submission.",
        }
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
