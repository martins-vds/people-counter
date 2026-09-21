"""Checkpointed CLI for catalog-driven ADLS video and manifest publication."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path, PurePosixPath

from people_counter.adls import (
    AzureDataLakeStorage,
    PublicationConflictError,
    PublicationStorage,
    RemoteObject,
)
from people_counter.manifest import (
    CONTENT_TYPES,
    CameraCatalogEntry,
    ManifestPlan,
    ManifestPublisherError,
    VideoInspection,
    build_manifest_plan,
    inspect_video,
    load_camera_catalog,
    load_video_inventory,
    manifest_bytes,
    normalize_relative_path,
    parse_utc_timestamp,
    prepared_manifest_bytes,
)

STORAGE_ACCOUNT_NAME = re.compile(r"^[a-z0-9]{3,24}$")
FILESYSTEM_NAME = re.compile(
    r"^(?!.*--)[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])$"
)
PLANNING_CONTRACT_VERSION = "2"
MANIFEST_PACKAGE_VERSION = 1


@dataclass(frozen=True)
class PublisherConfig:
    catalog_path: Path
    inventory_path: Path | None
    video_root: Path
    partition_prefix: str | None
    storage_account: str
    filesystem: str
    staging_prefix: str
    incoming_prefix: str
    checkpoint_path: Path
    rejection_report_path: Path
    generated_inventory_report_path: Path
    max_files: int
    dry_run: bool
    rehash: bool = False
    summary_report_path: Path = Path("manifest-summary.json")
    output_dir: Path | None = None


@dataclass(frozen=True)
class ManifestPackagePublishConfig:
    manifest_package_dir: Path
    video_root: Path
    storage_account: str
    filesystem: str
    staging_prefix: str
    incoming_prefix: str
    checkpoint_path: Path
    rejection_report_path: Path
    summary_report_path: Path
    chunk_size: int


@dataclass(frozen=True)
class ValidatedManifestPackage:
    plans: tuple[ManifestPlan, ...]
    index_bytes: bytes
    index: dict[str, object]


@dataclass
class PublishSummary:
    generator_version: str = ""
    catalog_sha256: str = ""
    inventory_sha256: str | None = None
    discovered: int = 0
    planned: int = 0
    published: int = 0
    already_published: int = 0
    rejected: int = 0
    total_video_duration_seconds: float = 0.0
    rejections_by_reason: dict[str, int] = field(default_factory=dict)
    rejections_by_camera: dict[str, int] = field(default_factory=dict)
    rejections_by_prefix: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class RejectionRecord:
    video_relative_path: str
    reason_code: str
    field: str
    observed_value: str
    explanation: str
    suggested_action: str
    retryable: bool
    camera_id: str
    source_path_prefix: str
    checkpoint_state: str = "REJECTED"


@dataclass(frozen=True)
class GeneratedInventoryRecord:
    video_relative_path: str
    camera_id: str
    location_id: str
    camera_timezone: str
    captured_at_utc: str
    capture_time_source: str
    asset_id: str
    asset_version: str
    size_bytes: int
    sha256: str
    frame_width: int
    frame_height: int
    duration_seconds: float
    prepared_manifest_path: str


@dataclass(frozen=True)
class CompletedCheckpoint:
    input_signature: str
    local_size: int
    local_mtime_ns: int
    incoming_video_path: str
    incoming_manifest_path: str
    video_etag: str
    manifest_sha256: str
    duration_seconds: float


class CheckpointStore:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path)
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS publications (
                relative_path TEXT PRIMARY KEY,
                fingerprint TEXT,
                state TEXT NOT NULL,
                asset_id TEXT,
                asset_version TEXT,
                manifest_uri TEXT,
                attempt_count INTEGER NOT NULL,
                error TEXT,
                input_signature TEXT,
                local_size INTEGER,
                local_mtime_ns INTEGER,
                incoming_video_path TEXT,
                incoming_manifest_path TEXT,
                video_etag TEXT,
                manifest_sha256 TEXT,
                duration_seconds REAL,
                reason_code TEXT,
                error_field TEXT,
                observed_value TEXT,
                suggested_action TEXT,
                retryable INTEGER,
                updated_at_utc TEXT NOT NULL
            )
            """
        )
        existing_columns = {
            row[1]
            for row in self._connection.execute(
                "PRAGMA table_info(publications)"
            )
        }
        for column, sql_type in (
            ("reason_code", "TEXT"),
            ("error_field", "TEXT"),
            ("observed_value", "TEXT"),
            ("suggested_action", "TEXT"),
            ("retryable", "INTEGER"),
        ):
            if column not in existing_columns:
                self._connection.execute(
                    f"ALTER TABLE publications ADD COLUMN {column} {sql_type}"
                )
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()

    def record(
        self,
        relative_path: str,
        state: str,
        *,
        plan: ManifestPlan | None = None,
        manifest_uri: str | None = None,
        error: str | None = None,
        input_signature: str | None = None,
        local_size: int | None = None,
        local_mtime_ns: int | None = None,
        video_etag: str | None = None,
        manifest_sha256: str | None = None,
        rejection: RejectionRecord | None = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        plan_values = _checkpoint_plan_values(plan)
        rejection_values = _checkpoint_rejection_values(rejection)
        self._connection.execute(
            """
            INSERT INTO publications (
                relative_path, fingerprint, state, asset_id, asset_version,
                manifest_uri, attempt_count, error, input_signature,
                local_size, local_mtime_ns, incoming_video_path,
                incoming_manifest_path, video_etag, manifest_sha256,
                duration_seconds, reason_code, error_field, observed_value,
                suggested_action, retryable, updated_at_utc
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?
            )
            ON CONFLICT(relative_path) DO UPDATE SET
                fingerprint = COALESCE(
                    excluded.fingerprint,
                    publications.fingerprint
                ),
                state = excluded.state,
                asset_id = COALESCE(
                    excluded.asset_id,
                    publications.asset_id
                ),
                asset_version = COALESCE(
                    excluded.asset_version,
                    publications.asset_version
                ),
                manifest_uri = COALESCE(
                    excluded.manifest_uri,
                    publications.manifest_uri
                ),
                attempt_count = CASE
                    WHEN excluded.state = 'DISCOVERED'
                    THEN publications.attempt_count + 1
                    ELSE publications.attempt_count
                END,
                error = excluded.error,
                input_signature = COALESCE(
                    excluded.input_signature,
                    publications.input_signature
                ),
                local_size = COALESCE(
                    excluded.local_size,
                    publications.local_size
                ),
                local_mtime_ns = COALESCE(
                    excluded.local_mtime_ns,
                    publications.local_mtime_ns
                ),
                incoming_video_path = COALESCE(
                    excluded.incoming_video_path,
                    publications.incoming_video_path
                ),
                incoming_manifest_path = COALESCE(
                    excluded.incoming_manifest_path,
                    publications.incoming_manifest_path
                ),
                video_etag = COALESCE(
                    excluded.video_etag,
                    publications.video_etag
                ),
                manifest_sha256 = COALESCE(
                    excluded.manifest_sha256,
                    publications.manifest_sha256
                ),
                duration_seconds = COALESCE(
                    excluded.duration_seconds,
                    publications.duration_seconds
                ),
                reason_code = excluded.reason_code,
                error_field = excluded.error_field,
                observed_value = excluded.observed_value,
                suggested_action = excluded.suggested_action,
                retryable = excluded.retryable,
                updated_at_utc = excluded.updated_at_utc
            """,
            (
                relative_path,
                plan_values[0],
                state,
                plan_values[1],
                plan_values[2],
                manifest_uri,
                error,
                input_signature,
                local_size,
                local_mtime_ns,
                plan_values[3],
                plan_values[4],
                video_etag,
                manifest_sha256,
                plan_values[5],
                *rejection_values,
                now,
            ),
        )
        self._connection.commit()

    def completed(self, relative_path: str) -> CompletedCheckpoint | None:
        row = self._connection.execute(
            """
            SELECT
                input_signature, local_size, local_mtime_ns,
                incoming_video_path, incoming_manifest_path, video_etag,
                manifest_sha256, duration_seconds
            FROM publications
            WHERE relative_path = ? AND state = 'MANIFEST_PUBLISHED'
            """,
            (relative_path,),
        ).fetchone()
        if row is None or any(value is None for value in row):
            return None
        return CompletedCheckpoint(
            input_signature=str(row[0]),
            local_size=int(row[1]),
            local_mtime_ns=int(row[2]),
            incoming_video_path=str(row[3]),
            incoming_manifest_path=str(row[4]),
            video_etag=str(row[5]),
            manifest_sha256=str(row[6]),
            duration_seconds=float(row[7]),
        )


def _checkpoint_plan_values(
    plan: ManifestPlan | None,
) -> tuple[
    str | None,
    str | None,
    str | None,
    str | None,
    str | None,
    float | None,
]:
    if plan is None:
        return (None, None, None, None, None, None)
    return (
        plan.fingerprint,
        plan.asset_id,
        plan.asset_version,
        plan.incoming_video_path,
        plan.incoming_manifest_path,
        plan.inspection.duration_seconds,
    )


def _checkpoint_rejection_values(
    rejection: RejectionRecord | None,
) -> tuple[str | None, str | None, str | None, str | None, int | None]:
    if rejection is None:
        return (None, None, None, None, None)
    return (
        rejection.reason_code,
        rejection.field,
        rejection.observed_value,
        rejection.suggested_action,
        int(rejection.retryable),
    )


def discover_videos(
    video_root: Path,
    partition_prefix: str | None,
    max_files: int,
) -> list[Path]:
    if not video_root.is_dir():
        raise ManifestPublisherError(
            f"Video root does not exist or is not a directory: {video_root}"
        )
    search_root = video_root
    if partition_prefix is not None:
        normalized = normalize_relative_path(partition_prefix, directory=True)
        search_root = video_root.joinpath(*_path_parts(normalized))
        if not search_root.is_dir():
            raise ManifestPublisherError(
                f"Partition prefix does not exist: {partition_prefix}"
            )
    paths = sorted(
        path
        for path in search_root.rglob("*")
        if path.is_file() and path.suffix.lower() in CONTENT_TYPES
    )
    symlinks = [path for path in paths if path.is_symlink()]
    if symlinks:
        raise ManifestPublisherError(
            f"Symbolic links are not supported: {symlinks[0]}"
        )
    if not paths:
        raise ManifestPublisherError("No supported video files were discovered")
    if len(paths) > max_files:
        raise ManifestPublisherError(
            f"Discovered {len(paths)} videos, exceeding --max-files={max_files}; "
            "use a narrower --partition-prefix"
        )
    return paths


def _path_parts(path: str) -> tuple[str, ...]:
    return tuple(part for part in path.rstrip("/").split("/") if part)


def _manifest_uri(config: PublisherConfig, plan: ManifestPlan) -> str:
    return (
        f"abfss://{config.filesystem}@{config.storage_account}"
        f".dfs.core.windows.net/{plan.incoming_manifest_path}"
    )


def _configuration_digest(config: PublisherConfig) -> str:
    digest = hashlib.sha256()
    for path in (config.catalog_path, config.inventory_path):
        if path is None:
            digest.update(b"<none>\0")
            continue
        digest.update(path.read_bytes())
        digest.update(b"\0")
    for value in (
        PLANNING_CONTRACT_VERSION,
        config.storage_account,
        config.filesystem,
        config.staging_prefix,
        config.incoming_prefix,
    ):
        digest.update(value.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def _file_sha256(path: Path | None) -> str | None:
    if path is None:
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _input_signature(
    configuration_digest: str,
    relative_path: str,
    local_size: int,
    local_mtime_ns: int,
) -> str:
    value = (
        f"{configuration_digest}\n{relative_path}\n"
        f"{local_size}\n{local_mtime_ns}"
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _checkpoint_is_published(
    checkpoint: CompletedCheckpoint,
    input_signature: str,
    storage: PublicationStorage,
) -> bool:
    if checkpoint.input_signature != input_signature:
        return False
    video = storage.stat(checkpoint.incoming_video_path)
    manifest = storage.stat(checkpoint.incoming_manifest_path)
    if (
        video is None
        or manifest is None
        or video.size != checkpoint.local_size
        or video.etag != checkpoint.video_etag
    ):
        return False
    content = storage.read_bytes(checkpoint.incoming_manifest_path)
    return hashlib.sha256(content).hexdigest() == checkpoint.manifest_sha256


def _verify_published(
    storage: PublicationStorage,
    config: PublisherConfig,
    plan: ManifestPlan,
) -> tuple[RemoteObject, bytes] | None:
    existing_manifest = storage.stat(plan.incoming_manifest_path)
    if existing_manifest is None:
        return None
    existing_video = storage.stat(plan.incoming_video_path)
    if existing_video is None:
        raise PublicationConflictError(
            f"Manifest exists without its video: {plan.incoming_manifest_path}"
        )
    if existing_video.size != plan.inspection.size_bytes:
        raise PublicationConflictError(
            f"Published video size conflicts: {plan.incoming_video_path}"
        )
    if storage.sha256(plan.incoming_video_path) != plan.inspection.sha256:
        raise PublicationConflictError(
            f"Published video checksum conflicts: {plan.incoming_video_path}"
        )
    expected = manifest_bytes(
        plan,
        storage_account=config.storage_account,
        filesystem=config.filesystem,
        source_etag=existing_video.etag,
    )
    if storage.read_bytes(plan.incoming_manifest_path) != expected:
        raise PublicationConflictError(
            f"Published manifest conflicts: {plan.incoming_manifest_path}"
        )
    return existing_video, expected


def publish_plan(
    storage: PublicationStorage,
    checkpoint: CheckpointStore,
    config: PublisherConfig,
    plan: ManifestPlan,
) -> bool:
    manifest_uri = _manifest_uri(config, plan)
    verified = _verify_published(storage, config, plan)
    if verified is not None:
        existing_video, expected = verified
        checkpoint.record(
            plan.relative_path,
            "MANIFEST_PUBLISHED",
            plan=plan,
            manifest_uri=manifest_uri,
            video_etag=existing_video.etag,
            manifest_sha256=hashlib.sha256(expected).hexdigest(),
        )
        return False

    published_video = storage.stat(plan.incoming_video_path)
    if published_video is None:
        storage.upload_file(
            plan.local_path,
            plan.staging_video_path,
            plan.content_type,
            plan.inspection.size_bytes,
            plan.inspection.sha256,
        )
        storage.rename(plan.staging_video_path, plan.incoming_video_path)
        published_video = storage.stat(plan.incoming_video_path)
    elif storage.sha256(plan.incoming_video_path) != plan.inspection.sha256:
        raise PublicationConflictError(
            f"Published video checksum conflicts: {plan.incoming_video_path}"
        )
    if (
        published_video is None
        or published_video.size != plan.inspection.size_bytes
    ):
        raise PublicationConflictError(
            f"Published video size conflicts: {plan.incoming_video_path}"
        )
    checkpoint.record(
        plan.relative_path,
        "VIDEO_PUBLISHED",
        plan=plan,
    )

    content = manifest_bytes(
        plan,
        storage_account=config.storage_account,
        filesystem=config.filesystem,
        source_etag=published_video.etag,
    )
    storage.upload_bytes(
        content,
        plan.staging_manifest_path,
        "application/json",
    )
    storage.rename(
        plan.staging_manifest_path,
        plan.incoming_manifest_path,
    )
    checkpoint.record(
        plan.relative_path,
        "MANIFEST_PUBLISHED",
        plan=plan,
        manifest_uri=manifest_uri,
        video_etag=published_video.etag,
        manifest_sha256=hashlib.sha256(content).hexdigest(),
    )
    return True


def write_rejection_report(
    path: Path,
    rejections: list[RejectionRecord],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=tuple(RejectionRecord.__dataclass_fields__),
        )
        writer.writeheader()
        writer.writerows(asdict(rejection) for rejection in rejections)
    temporary.replace(path)


def write_summary_report(path: Path, summary: PublishSummary) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(asdict(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def write_generated_inventory_report(
    path: Path,
    records: list[GeneratedInventoryRecord],
    rejected_paths: set[str],
    discovered_paths: set[str],
    partition_prefix: str | None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: dict[str, dict[str, object]] = {}
    if path.is_file():
        with path.open(newline="", encoding="utf-8") as existing:
            for row in csv.DictReader(existing):
                relative_path = row.get("video_relative_path")
                if relative_path:
                    rows[relative_path] = dict(row)
    for relative_path in tuple(rows):
        in_scope = (
            partition_prefix is None
            or relative_path.startswith(partition_prefix)
        )
        if in_scope and relative_path not in discovered_paths:
            rows.pop(relative_path)
    for record in records:
        rows[record.video_relative_path] = asdict(record)
    for rejected_path in rejected_paths:
        rows.pop(rejected_path, None)

    temporary = path.with_suffix(f"{path.suffix}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=tuple(GeneratedInventoryRecord.__dataclass_fields__),
        )
        writer.writeheader()
        writer.writerows(rows[key] for key in sorted(rows))
    temporary.replace(path)


def _generated_inventory_paths(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    with path.open(newline="", encoding="utf-8") as handle:
        return {
            row["video_relative_path"]
            for row in csv.DictReader(handle)
            if row.get("video_relative_path")
        }


def _generated_inventory_record(
    plan: ManifestPlan,
) -> GeneratedInventoryRecord:
    return GeneratedInventoryRecord(
        video_relative_path=plan.relative_path,
        camera_id=plan.catalog.camera_id,
        location_id=plan.catalog.location_id,
        camera_timezone=plan.catalog.camera_timezone,
        captured_at_utc=plan.captured_at_utc.isoformat().replace(
            "+00:00",
            "Z",
        ),
        capture_time_source=plan.capture_time_source,
        asset_id=plan.asset_id,
        asset_version=plan.asset_version,
        size_bytes=plan.inspection.size_bytes,
        sha256=plan.inspection.sha256,
        frame_width=plan.inspection.width,
        frame_height=plan.inspection.height,
        duration_seconds=plan.inspection.duration_seconds,
        prepared_manifest_path=(
            f"prepared-manifests/{plan.asset_id}/"
            f"{plan.asset_version}.json"
        ),
    )


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_bytes(content)
    temporary.replace(path)


def write_manifest_package(
    output_dir: Path,
    plans: list[ManifestPlan],
    summary: PublishSummary,
    *,
    traversal_complete: bool,
) -> None:
    entries = []
    for plan in sorted(plans, key=lambda item: item.relative_path):
        prepared_path = (
            Path("prepared-manifests")
            / plan.asset_id
            / f"{plan.asset_version}.json"
        )
        content = prepared_manifest_bytes(plan)
        _atomic_write(output_dir / prepared_path, content)
        entries.append(
            {
                "video_relative_path": plan.relative_path,
                "prepared_manifest_path": prepared_path.as_posix(),
                "prepared_manifest_sha256": hashlib.sha256(content).hexdigest(),
                "video_sha256": plan.inspection.sha256,
                "video_size_bytes": plan.inspection.size_bytes,
            }
        )
    manifest_package = {
        "manifest_package_version": MANIFEST_PACKAGE_VERSION,
        "planning_contract_version": PLANNING_CONTRACT_VERSION,
        "generator_version": summary.generator_version,
        "created_at_utc": datetime.now(timezone.utc).isoformat().replace(
            "+00:00",
            "Z",
        ),
        "complete": traversal_complete and summary.rejected == 0,
        "catalog_sha256": summary.catalog_sha256,
        "inventory_sha256": summary.inventory_sha256,
        "discovered": summary.discovered,
        "prepared": len(entries),
        "rejected": summary.rejected,
        "entries": entries,
    }
    _atomic_write(
        output_dir / "manifest-package.json",
        (
            json.dumps(
                manifest_package,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
            )
            + "\n"
        ).encode("utf-8"),
    )


def _prepared_value(
    payload: dict[str, object],
    name: str,
    expected_type: type | tuple[type, ...],
) -> object:
    value = payload.get(name)
    if not isinstance(value, expected_type) or isinstance(value, bool):
        raise ManifestPublisherError(
            f"Prepared manifest field {name!r} has the wrong type",
            reason_code="MANIFEST_PACKAGE_INVALID",
            field=name,
            observed_value=repr(value),
            suggested_action=(
                "Regenerate the package with a supported generator release."
            ),
        )
    return value


def _load_prepared_plan(
    content: bytes,
    video_root: Path,
    staging_prefix: str,
    incoming_prefix: str,
) -> ManifestPlan:
    payload = _decode_prepared_payload(content)
    relative_path, asset_id, asset_version, expected_sha256 = (
        _prepared_identity(payload)
    )
    expected_size, width, height, duration, counting_line = (
        _prepared_media(payload)
    )
    (
        captured_at,
        source_path_prefix,
        camera_id,
        location_id,
        camera_timezone,
        capture_time_source,
        content_type,
    ) = _prepared_descriptive_metadata(payload, relative_path)
    local_path = video_root.joinpath(*_path_parts(relative_path))
    if not local_path.is_file() or local_path.is_symlink():
        raise ManifestPublisherError(
            f"Package video is missing or is a symbolic link: {relative_path}",
            reason_code="MANIFEST_PACKAGE_VIDEO_MISSING",
            field="relative_video_path",
            observed_value=relative_path,
            suggested_action=(
                "Provide the original video tree alongside the package and "
                "use the same video root used during preparation."
            ),
        )

    filename = PurePosixPath(relative_path).name
    manifest_name = f"{PurePosixPath(filename).stem}.json"
    staging_root = normalize_relative_path(staging_prefix, directory=True)
    incoming_root = normalize_relative_path(incoming_prefix, directory=True)
    date_path = captured_at.strftime("%Y/%m/%d")
    staging_base = f"{staging_root}{asset_id}/{asset_version}"
    incoming_base = (
        f"{incoming_root}{date_path}/{asset_id}/{asset_version}"
    )
    catalog = CameraCatalogEntry(
        source_path_prefix=source_path_prefix,
        camera_id=camera_id,
        location_id=location_id,
        camera_timezone=camera_timezone,
        frame_width=width,
        frame_height=height,
        counting_line=counting_line,
        capture_time_source=capture_time_source,
        capture_time_regex=None,
        capture_time_format=None,
        effective_from_utc=datetime.min.replace(tzinfo=timezone.utc),
        effective_to_utc=None,
    )
    return ManifestPlan(
        local_path=local_path,
        relative_path=relative_path,
        catalog=catalog,
        captured_at_utc=captured_at,
        capture_time_source=capture_time_source,
        inspection=VideoInspection(
            size_bytes=expected_size,
            sha256=expected_sha256,
            width=width,
            height=height,
            duration_seconds=duration,
        ),
        asset_id=asset_id,
        asset_version=asset_version,
        staging_video_path=f"{staging_base}/{filename}",
        incoming_video_path=f"{incoming_base}/{filename}",
        staging_manifest_path=f"{staging_base}/{manifest_name}",
        incoming_manifest_path=f"{incoming_base}/{manifest_name}",
        content_type=content_type,
    )


def _decode_prepared_payload(content: bytes) -> dict[str, object]:
    try:
        payload = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ManifestPublisherError(
            "Prepared manifest is not valid UTF-8 JSON",
            reason_code="MANIFEST_PACKAGE_INVALID",
            suggested_action="Regenerate the manifest package.",
        ) from error
    if not isinstance(payload, dict):
        raise ManifestPublisherError(
            "Prepared manifest must contain one JSON object",
            reason_code="MANIFEST_PACKAGE_INVALID",
            suggested_action="Regenerate the manifest package.",
        )
    if payload.get("prepared_manifest_version") != 1:
        raise ManifestPublisherError(
            "Prepared manifest version is unsupported",
            reason_code="MANIFEST_PACKAGE_VERSION_UNSUPPORTED",
            field="prepared_manifest_version",
            observed_value=repr(payload.get("prepared_manifest_version")),
            suggested_action=(
                "Regenerate the package with this publisher version."
            ),
        )
    return payload


def _prepared_identity(
    payload: dict[str, object],
) -> tuple[str, str, str, str]:
    relative_path = normalize_relative_path(
        str(_prepared_value(payload, "relative_video_path", str))
    )
    asset_id = str(_prepared_value(payload, "asset_id", str))
    asset_version = str(_prepared_value(payload, "asset_version", str))
    expected_sha256 = str(
        _prepared_value(payload, "expected_sha256", str)
    )
    expected_asset_id = hashlib.sha256(
        relative_path.encode("utf-8")
    ).hexdigest()
    if asset_id != expected_asset_id or asset_version != expected_sha256:
        raise ManifestPublisherError(
            f"Prepared identity does not match {relative_path}",
            reason_code="MANIFEST_PACKAGE_IDENTITY_MISMATCH",
            field="asset_id,asset_version",
            observed_value=f"{asset_id},{asset_version}",
            suggested_action=(
                "Regenerate the package; do not edit prepared manifests."
            ),
        )
    if re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None:
        raise ManifestPublisherError(
            "Prepared video SHA-256 is invalid",
            reason_code="MANIFEST_PACKAGE_INVALID",
            field="expected_sha256",
            observed_value=expected_sha256,
            suggested_action="Regenerate the manifest package.",
        )
    return relative_path, asset_id, asset_version, expected_sha256


def _prepared_media(
    payload: dict[str, object],
) -> tuple[int, int, int, float, tuple[int, int, int, int]]:
    expected_size = int(
        _prepared_value(payload, "expected_size_bytes", int)
    )
    width = int(_prepared_value(payload, "frame_width", int))
    height = int(_prepared_value(payload, "frame_height", int))
    duration = float(
        _prepared_value(payload, "duration_seconds", (int, float))
    )
    if (
        expected_size <= 0
        or width <= 0
        or height <= 0
        or not math.isfinite(duration)
        or duration <= 0
    ):
        raise ManifestPublisherError(
            "Prepared video metadata must be positive and finite",
            reason_code="MANIFEST_PACKAGE_INVALID",
            suggested_action="Regenerate the manifest package.",
        )
    line_value = payload.get("counting_line")
    if (
        not isinstance(line_value, list)
        or len(line_value) != 4
        or any(not isinstance(value, int) for value in line_value)
    ):
        raise ManifestPublisherError(
            "Prepared counting_line must contain four integers",
            reason_code="MANIFEST_PACKAGE_INVALID",
            field="counting_line",
            observed_value=repr(line_value),
            suggested_action="Regenerate the manifest package.",
        )
    counting_line = tuple(int(value) for value in line_value)
    return expected_size, width, height, duration, counting_line


def _prepared_descriptive_metadata(
    payload: dict[str, object],
    relative_path: str,
) -> tuple[datetime, str, str, str, str, str, str]:
    captured_at = parse_utc_timestamp(
        str(_prepared_value(payload, "captured_at_utc", str)),
        "prepared captured_at_utc",
    )
    source_path_prefix = normalize_relative_path(
        str(_prepared_value(payload, "source_path_prefix", str)),
        directory=True,
    )
    camera_id = str(_prepared_value(payload, "camera_id", str)).strip()
    location_id = str(_prepared_value(payload, "location_id", str)).strip()
    camera_timezone = str(
        _prepared_value(payload, "camera_timezone", str)
    ).strip()
    capture_time_source = str(
        _prepared_value(payload, "capture_time_source", str)
    ).strip()
    content_type = str(
        _prepared_value(payload, "content_type", str)
    ).strip()
    if not all(
        (camera_id, location_id, camera_timezone, capture_time_source)
    ):
        raise ManifestPublisherError(
            "Prepared camera metadata contains blank values",
            reason_code="MANIFEST_PACKAGE_INVALID",
            suggested_action="Regenerate the manifest package.",
        )

    suffix = PurePosixPath(relative_path).suffix.lower()
    if CONTENT_TYPES.get(suffix) != content_type:
        raise ManifestPublisherError(
            "Prepared content type does not match the video extension",
            reason_code="MANIFEST_PACKAGE_INVALID",
            field="content_type",
            observed_value=content_type,
            suggested_action="Regenerate the manifest package.",
        )
    return (
        captured_at,
        source_path_prefix,
        camera_id,
        location_id,
        camera_timezone,
        capture_time_source,
        content_type,
    )


def load_manifest_package_plans(
    config: ManifestPackagePublishConfig,
) -> list[ManifestPlan]:
    return list(validate_manifest_package(config).plans)


def validate_manifest_package(
    config: ManifestPackagePublishConfig,
) -> ValidatedManifestPackage:
    index_path = config.manifest_package_dir / "manifest-package.json"
    manifest_package, index_bytes = _load_manifest_package_index(index_path)
    entries = manifest_package["entries"]
    assert isinstance(entries, list)

    plans = [
        _load_manifest_package_entry(config, entry)
        for entry in entries
    ]
    relative_paths = {plan.relative_path for plan in plans}
    identities = {(plan.asset_id, plan.asset_version) for plan in plans}
    if len(relative_paths) != len(plans) or len(identities) != len(plans):
        raise ManifestPublisherError(
            "Manifest package contains duplicate videos or asset identities",
            reason_code="MANIFEST_PACKAGE_INVALID",
            suggested_action="Regenerate the manifest package directory.",
        )
    return ValidatedManifestPackage(
        plans=tuple(plans),
        index_bytes=index_bytes,
        index=manifest_package,
    )


def _load_manifest_package_index(
    index_path: Path,
) -> tuple[dict[str, object], bytes]:
    try:
        index_bytes = index_path.read_bytes()
        manifest_package = json.loads(index_bytes)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ManifestPublisherError(
            f"Could not read manifest package index: {index_path}",
            reason_code="MANIFEST_PACKAGE_INVALID",
            suggested_action=(
                "Regenerate or recopy the manifest package directory."
            ),
        ) from error
    if not isinstance(manifest_package, dict):
        raise ManifestPublisherError(
            "Manifest package index must contain one JSON object",
            reason_code="MANIFEST_PACKAGE_INVALID",
            suggested_action="Regenerate the manifest package directory.",
        )
    _validate_manifest_package_header(manifest_package)
    return manifest_package, index_bytes


def _validate_manifest_package_header(
    manifest_package: dict[str, object],
) -> None:
    if (
        manifest_package.get("manifest_package_version")
        != MANIFEST_PACKAGE_VERSION
    ):
        raise ManifestPublisherError(
            "Manifest package version is unsupported",
            reason_code="MANIFEST_PACKAGE_VERSION_UNSUPPORTED",
            suggested_action=(
                "Regenerate the package with this publisher version."
            ),
        )
    if (
        manifest_package.get("planning_contract_version")
        != PLANNING_CONTRACT_VERSION
    ):
        raise ManifestPublisherError(
            "Manifest package planning contract is unsupported",
            reason_code="MANIFEST_PACKAGE_VERSION_UNSUPPORTED",
            suggested_action=(
                "Regenerate the package with this publisher version."
            ),
        )
    if manifest_package.get("complete") is not True:
        raise ManifestPublisherError(
            "Manifest package is incomplete because preparation had rejections",
            reason_code="MANIFEST_PACKAGE_INCOMPLETE",
            suggested_action=(
                "Resolve preparation rejections and regenerate the package."
            ),
        )
    entries = manifest_package.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ManifestPublisherError(
            "Manifest package contains no prepared entries",
            reason_code="MANIFEST_PACKAGE_INVALID",
            suggested_action="Regenerate the manifest package directory.",
        )
    prepared = manifest_package.get("prepared")
    discovered = manifest_package.get("discovered")
    rejected = manifest_package.get("rejected")
    if (
        not isinstance(prepared, int)
        or isinstance(prepared, bool)
        or not isinstance(discovered, int)
        or isinstance(discovered, bool)
        or not isinstance(rejected, int)
        or isinstance(rejected, bool)
        or prepared != len(entries)
        or discovered != prepared + rejected
        or rejected != 0
    ):
        raise ManifestPublisherError(
            "Manifest package counts are inconsistent",
            reason_code="MANIFEST_PACKAGE_INVALID",
            suggested_action="Regenerate the manifest package directory.",
        )



def _load_manifest_package_entry(
    config: ManifestPackagePublishConfig,
    entry: object,
) -> ManifestPlan:
    if not isinstance(entry, dict):
        raise ManifestPublisherError(
            "Manifest package entry must be a JSON object",
            reason_code="MANIFEST_PACKAGE_INVALID",
            suggested_action="Regenerate the manifest package directory.",
        )
    prepared_path = normalize_relative_path(
        str(entry.get("prepared_manifest_path", ""))
    )
    if not prepared_path.startswith("prepared-manifests/"):
        raise ManifestPublisherError(
            "Prepared manifest path is outside prepared-manifests/",
            reason_code="MANIFEST_PACKAGE_INVALID",
            field="prepared_manifest_path",
            observed_value=prepared_path,
            suggested_action="Regenerate the manifest package directory.",
        )
    try:
        content = (
            config.manifest_package_dir / prepared_path
        ).read_bytes()
    except OSError as error:
        raise ManifestPublisherError(
            f"Could not read prepared manifest: {prepared_path}",
            reason_code="MANIFEST_PACKAGE_INVALID",
            field="prepared_manifest_path",
            observed_value=prepared_path,
            suggested_action=(
                "Recopy or regenerate the manifest package directory."
            ),
        ) from error
    actual_sha256 = hashlib.sha256(content).hexdigest()
    if actual_sha256 != entry.get("prepared_manifest_sha256"):
        raise ManifestPublisherError(
            f"Prepared manifest checksum mismatch: {prepared_path}",
            reason_code="MANIFEST_PACKAGE_CHECKSUM_MISMATCH",
            field="prepared_manifest_sha256",
            observed_value=actual_sha256,
            suggested_action=(
                "Recopy or regenerate the package; do not publish it."
            )
        )
    plan = _load_prepared_plan(
        content,
        config.video_root,
        config.staging_prefix,
        config.incoming_prefix,
    )
    if (
        entry.get("video_relative_path") != plan.relative_path
        or entry.get("video_sha256") != plan.inspection.sha256
        or entry.get("video_size_bytes") != plan.inspection.size_bytes
    ):
        raise ManifestPublisherError(
            f"Manifest package index conflicts with {prepared_path}",
            reason_code="MANIFEST_PACKAGE_CHECKSUM_MISMATCH",
            suggested_action="Regenerate the manifest package directory.",
        )
    _verify_package_video(plan)
    return plan


def _verify_package_video(plan: ManifestPlan) -> None:
    if plan.local_path.stat().st_size != plan.inspection.size_bytes:
        raise ManifestPublisherError(
            f"Source video size changed: {plan.relative_path}",
            reason_code="MANIFEST_PACKAGE_VIDEO_MISMATCH",
            field="expected_size_bytes",
            observed_value=str(plan.local_path.stat().st_size),
            suggested_action=(
                "Restore the exact prepared video bytes or regenerate the "
                "manifest package from the current video tree."
            ),
        )
    digest = hashlib.sha256()
    with plan.local_path.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)
    if digest.hexdigest() != plan.inspection.sha256:
        raise ManifestPublisherError(
            f"Source video checksum changed: {plan.relative_path}",
            reason_code="MANIFEST_PACKAGE_VIDEO_MISMATCH",
            field="expected_sha256",
            observed_value=digest.hexdigest(),
            suggested_action=(
                "Restore the exact prepared video bytes or regenerate the "
                "manifest package from the current video tree."
            ),
        )


def _rejection_record(
    relative_path: str,
    error: ManifestPublisherError,
    plan: ManifestPlan | None,
) -> RejectionRecord:
    return RejectionRecord(
        video_relative_path=relative_path,
        reason_code=error.reason_code,
        field=error.field or "",
        observed_value=error.observed_value or "",
        explanation=str(error),
        suggested_action=error.suggested_action,
        retryable=error.retryable,
        camera_id=(
            error.camera_id
            or (plan.catalog.camera_id if plan is not None else "")
        ),
        source_path_prefix=(
            error.source_path_prefix
            or (
                plan.catalog.source_path_prefix
                if plan is not None
                else ""
            )
        ),
    )


def _increment(group: dict[str, int], value: str) -> None:
    key = value or "(unresolved)"
    group[key] = group.get(key, 0) + 1


def _path_catalog_context(
    relative_path: str,
    catalog: list[CameraCatalogEntry],
) -> tuple[str, str]:
    matches = [
        entry
        for entry in catalog
        if relative_path.startswith(entry.source_path_prefix)
    ]
    camera_ids = {entry.camera_id for entry in matches}
    prefixes = {entry.source_path_prefix for entry in matches}
    return (
        next(iter(camera_ids)) if len(camera_ids) == 1 else "",
        next(iter(prefixes)) if len(prefixes) == 1 else "",
    )


def run_publisher(
    config: PublisherConfig,
    *,
    storage: PublicationStorage | None,
    inspector: Callable[[Path], VideoInspection] = inspect_video,
) -> PublishSummary:
    _invalidate_output_index(config.output_dir)
    catalog = load_camera_catalog(config.catalog_path)
    inventory = load_video_inventory(config.inventory_path)
    videos = discover_videos(
        config.video_root,
        config.partition_prefix,
        config.max_files,
    )
    discovered_paths = {
        video.relative_to(config.video_root).as_posix()
        for video in videos
    }
    partition_prefix = (
        normalize_relative_path(config.partition_prefix, directory=True)
        if config.partition_prefix is not None
        else None
    )
    relevant_inventory = {
        path
        for path in inventory
        if partition_prefix is None or path.startswith(partition_prefix)
    }
    inventory_without_files = sorted(relevant_inventory - discovered_paths)
    if inventory_without_files:
        raise ManifestPublisherError(
            "Video inventory references a file outside the discovered "
            f"partition: {inventory_without_files[0]}"
        )
    checkpoint = CheckpointStore(config.checkpoint_path)
    summary = PublishSummary(
        generator_version=version("people-counter"),
        catalog_sha256=_file_sha256(config.catalog_path) or "",
        inventory_sha256=_file_sha256(config.inventory_path),
        discovered=len(videos),
    )
    rejections: list[RejectionRecord] = []
    generated_inventory: list[GeneratedInventoryRecord] = []
    prepared_plans: list[ManifestPlan] = []
    generated_inventory_paths = _generated_inventory_paths(
        config.generated_inventory_report_path
    )
    configuration_digest = _configuration_digest(config)
    traversal_complete = False
    try:
        for video in videos:
            relative_path = video.relative_to(config.video_root).as_posix()
            local_stat = video.stat()
            input_signature = _input_signature(
                configuration_digest,
                relative_path,
                local_stat.st_size,
                local_stat.st_mtime_ns,
            )
            completed = checkpoint.completed(relative_path)
            if (
                not config.dry_run
                and not config.rehash
                and storage is not None
                and completed is not None
                and relative_path in generated_inventory_paths
                and _checkpoint_is_published(
                    completed,
                    input_signature,
                    storage,
                )
            ):
                summary.planned += 1
                summary.already_published += 1
                summary.total_video_duration_seconds += (
                    completed.duration_seconds
                )
                checkpoint.record(
                    relative_path,
                    "MANIFEST_PUBLISHED",
                )
                continue
            checkpoint.record(
                relative_path,
                "DISCOVERED",
                input_signature=input_signature,
                local_size=local_stat.st_size,
                local_mtime_ns=local_stat.st_mtime_ns,
            )
            plan: ManifestPlan | None = None
            try:
                plan = build_manifest_plan(
                    video,
                    config.video_root,
                    catalog,
                    inventory,
                    staging_prefix=config.staging_prefix,
                    incoming_prefix=config.incoming_prefix,
                    inspector=inspector,
                )
                checkpoint.record(relative_path, "HASHED", plan=plan)
                prepared_plans.append(plan)
                generated_inventory.append(
                    _generated_inventory_record(plan)
                )
                summary.planned += 1
                summary.total_video_duration_seconds += (
                    plan.inspection.duration_seconds
                )
                if config.dry_run:
                    continue
                if storage is None:
                    raise RuntimeError("Publication storage is required")
                if publish_plan(storage, checkpoint, config, plan):
                    summary.published += 1
                else:
                    summary.already_published += 1
            except ManifestPublisherError as error:
                if plan is None:
                    camera_id, source_path_prefix = _path_catalog_context(
                        relative_path,
                        catalog,
                    )
                    if error.camera_id is None:
                        error.camera_id = camera_id or None
                    if error.source_path_prefix is None:
                        error.source_path_prefix = (
                            source_path_prefix or None
                        )
                rejection = _rejection_record(relative_path, error, plan)
                checkpoint.record(
                    relative_path,
                    "REJECTED",
                    plan=plan,
                    input_signature=input_signature,
                    local_size=local_stat.st_size,
                    local_mtime_ns=local_stat.st_mtime_ns,
                    error=str(error),
                    rejection=rejection,
                )
                rejections.append(rejection)
                summary.rejected += 1
                _increment(
                    summary.rejections_by_reason,
                    rejection.reason_code,
                )
                _increment(
                    summary.rejections_by_camera,
                    rejection.camera_id,
                )
                _increment(
                    summary.rejections_by_prefix,
                    rejection.source_path_prefix,
                )
        traversal_complete = True
    finally:
        checkpoint.close()
        write_rejection_report(config.rejection_report_path, rejections)
        write_generated_inventory_report(
            config.generated_inventory_report_path,
            generated_inventory,
            {
                rejection.video_relative_path
                for rejection in rejections
            },
            discovered_paths,
            partition_prefix,
        )
        write_summary_report(config.summary_report_path, summary)
        if config.output_dir is not None:
            write_manifest_package(
                config.output_dir,
                prepared_plans,
                summary,
                traversal_complete=traversal_complete,
            )
    return summary


def _invalidate_output_index(output_dir: Path | None) -> None:
    if output_dir is None:
        return
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "manifest-package.json").unlink(missing_ok=True)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("Value must be greater than zero")
    return parsed


def storage_account_name(value: str) -> str:
    if STORAGE_ACCOUNT_NAME.fullmatch(value) is None:
        raise argparse.ArgumentTypeError(
            "Storage account must contain 3-24 lowercase letters or digits"
        )
    return value


def filesystem_name(value: str) -> str:
    if FILESYSTEM_NAME.fullmatch(value) is None:
        raise argparse.ArgumentTypeError(
            "Filesystem must be a valid lowercase ADLS container name"
        )
    return value


def publish_manifest_package(
    config: ManifestPackagePublishConfig,
    storage: PublicationStorage,
    *,
    validated_package: ValidatedManifestPackage | None = None,
) -> PublishSummary:
    validated = (
        validate_manifest_package(config)
        if validated_package is None
        else validated_package
    )
    plans = list(validated.plans)
    package_bytes = validated.index_bytes
    manifest_package = validated.index
    summary = PublishSummary(
        generator_version=str(manifest_package["generator_version"]),
        catalog_sha256=str(manifest_package["catalog_sha256"]),
        inventory_sha256=manifest_package.get("inventory_sha256"),
        discovered=len(plans),
        planned=len(plans),
        total_video_duration_seconds=sum(
            plan.inspection.duration_seconds for plan in plans
        ),
    )
    checkpoint = CheckpointStore(config.checkpoint_path)
    rejections: list[RejectionRecord] = []
    package_digest = hashlib.sha256(package_bytes).hexdigest()
    publish_config = PublisherConfig(
        catalog_path=(
            config.manifest_package_dir / "manifest-package.json"
        ),
        inventory_path=None,
        video_root=config.video_root,
        partition_prefix=None,
        storage_account=config.storage_account,
        filesystem=config.filesystem,
        staging_prefix=config.staging_prefix,
        incoming_prefix=config.incoming_prefix,
        checkpoint_path=config.checkpoint_path,
        rejection_report_path=config.rejection_report_path,
        generated_inventory_report_path=(
            config.manifest_package_dir / "generated-video-inventory.csv"
        ),
        max_files=len(plans),
        dry_run=False,
        summary_report_path=config.summary_report_path,
    )
    try:
        for plan in plans:
            local_stat = plan.local_path.stat()
            input_signature = _input_signature(
                (
                    f"{PLANNING_CONTRACT_VERSION}:{package_digest}:"
                    f"{config.storage_account}:{config.filesystem}:"
                    f"{config.staging_prefix}:{config.incoming_prefix}"
                ),
                plan.relative_path,
                local_stat.st_size,
                local_stat.st_mtime_ns,
            )
            completed = checkpoint.completed(plan.relative_path)
            try:
                if (
                    completed is not None
                    and _checkpoint_is_published(
                        completed,
                        input_signature,
                        storage,
                    )
                ):
                    summary.already_published += 1
                    continue
                checkpoint.record(
                    plan.relative_path,
                    "DISCOVERED",
                    plan=plan,
                    input_signature=input_signature,
                    local_size=local_stat.st_size,
                    local_mtime_ns=local_stat.st_mtime_ns,
                )
                if publish_plan(
                    storage,
                    checkpoint,
                    publish_config,
                    plan,
                ):
                    summary.published += 1
                else:
                    summary.already_published += 1
            except ManifestPublisherError as error:
                rejection = _rejection_record(
                    plan.relative_path,
                    error,
                    plan,
                )
                checkpoint.record(
                    plan.relative_path,
                    "REJECTED",
                    plan=plan,
                    input_signature=input_signature,
                    local_size=local_stat.st_size,
                    local_mtime_ns=local_stat.st_mtime_ns,
                    error=str(error),
                    rejection=rejection,
                )
                rejections.append(rejection)
                summary.rejected += 1
                _increment(
                    summary.rejections_by_reason,
                    rejection.reason_code,
                )
                _increment(
                    summary.rejections_by_camera,
                    rejection.camera_id,
                )
                _increment(
                    summary.rejections_by_prefix,
                    rejection.source_path_prefix,
                )
    finally:
        checkpoint.close()
        write_rejection_report(config.rejection_report_path, rejections)
        write_summary_report(config.summary_report_path, summary)
    return summary


def build_prepare_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="prepare-manifests",
        description="Prepare a checksum-indexed manifest package directory.",
    )
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument(
        "--inventory",
        type=Path,
        help=(
            "Optional CSV containing timestamp overrides only for videos "
            "whose capture time cannot be derived automatically."
        ),
    )
    parser.add_argument("--video-root", type=Path, required=True)
    parser.add_argument("--partition-prefix")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--checkpoint",
        type=Path,
        help="Preparation checkpoint (default: OUTPUT_DIR/preparation.sqlite3).",
    )
    parser.add_argument("--max-files", type=positive_int, default=1000)
    return parser


def build_publish_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="publish-manifests",
        description=(
            "Validate a manifest package and atomically publish it to ADLS."
        ),
    )
    parser.add_argument(
        "--manifest-package-dir",
        type=Path,
        required=True,
    )
    parser.add_argument("--video-root", type=Path, required=True)
    parser.add_argument(
        "--storage-account",
        type=storage_account_name,
        required=True,
    )
    parser.add_argument("--filesystem", type=filesystem_name, required=True)
    parser.add_argument("--staging-prefix", default="staging")
    parser.add_argument("--incoming-prefix", default="incoming")
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=Path("manifest-publication.sqlite3"),
    )
    parser.add_argument(
        "--rejection-report",
        type=Path,
        default=Path("publication-rejections.csv"),
    )
    parser.add_argument(
        "--summary-report",
        type=Path,
        default=Path("publication-summary.json"),
    )
    parser.add_argument("--chunk-size-mib", type=positive_int, default=8)
    return parser


def build_parser() -> argparse.ArgumentParser:
    return build_publish_parser()


def prepare_main(argv: Sequence[str] | None = None) -> int:
    parser = build_prepare_parser()
    args = parser.parse_args(argv)
    output_dir = args.output_dir
    config = PublisherConfig(
        catalog_path=args.catalog,
        inventory_path=args.inventory,
        video_root=args.video_root,
        partition_prefix=args.partition_prefix,
        storage_account="unbound",
        filesystem="unbound",
        staging_prefix="staging",
        incoming_prefix="incoming",
        checkpoint_path=(
            args.checkpoint or output_dir / "preparation.sqlite3"
        ),
        rejection_report_path=output_dir / "rejection-report.csv",
        generated_inventory_report_path=(
            output_dir / "generated-video-inventory.csv"
        ),
        max_files=args.max_files,
        dry_run=True,
        summary_report_path=output_dir / "summary.json",
        output_dir=output_dir,
    )
    summary = run_publisher(config, storage=None)
    print(json.dumps(asdict(summary), sort_keys=True))
    return 2 if summary.rejected else 0


def publish_main(argv: Sequence[str] | None = None) -> int:
    args = build_publish_parser().parse_args(argv)
    config = ManifestPackagePublishConfig(
        manifest_package_dir=args.manifest_package_dir,
        video_root=args.video_root,
        storage_account=args.storage_account,
        filesystem=args.filesystem,
        staging_prefix=args.staging_prefix,
        incoming_prefix=args.incoming_prefix,
        checkpoint_path=args.checkpoint,
        rejection_report_path=args.rejection_report,
        summary_report_path=args.summary_report,
        chunk_size=args.chunk_size_mib * 1024 * 1024,
    )
    validated_package = validate_manifest_package(config)
    storage = AzureDataLakeStorage(
        config.storage_account,
        config.filesystem,
        chunk_size=config.chunk_size,
    )
    summary = publish_manifest_package(
        config,
        storage,
        validated_package=validated_package,
    )
    print(json.dumps(asdict(summary), sort_keys=True))
    return 2 if summary.rejected else 0


def main(argv: Sequence[str] | None = None) -> int:
    return publish_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
