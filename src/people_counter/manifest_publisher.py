"""Checkpointed CLI for catalog-driven ADLS video and manifest publication."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path

from people_counter.adls import (
    AzureDataLakeStorage,
    PublicationConflictError,
    PublicationStorage,
    RemoteObject,
)
from people_counter.manifest import (
    CONTENT_TYPES,
    ManifestPlan,
    ManifestPublisherError,
    VideoInspection,
    build_manifest_plan,
    inspect_video,
    load_camera_catalog,
    load_video_inventory,
    manifest_bytes,
    normalize_relative_path,
)

STORAGE_ACCOUNT_NAME = re.compile(r"^[a-z0-9]{3,24}$")
FILESYSTEM_NAME = re.compile(
    r"^(?!.*--)[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])$"
)


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
    max_files: int
    dry_run: bool
    rehash: bool = False


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
                updated_at_utc TEXT NOT NULL
            )
            """
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
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self._connection.execute(
            """
            INSERT INTO publications (
                relative_path, fingerprint, state, asset_id, asset_version,
                manifest_uri, attempt_count, error, input_signature,
                local_size, local_mtime_ns, incoming_video_path,
                incoming_manifest_path, video_etag, manifest_sha256,
                duration_seconds, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                updated_at_utc = excluded.updated_at_utc
            """,
            (
                relative_path,
                plan.fingerprint if plan is not None else None,
                state,
                plan.asset_id if plan is not None else None,
                plan.asset_version if plan is not None else None,
                manifest_uri,
                error,
                input_signature,
                local_size,
                local_mtime_ns,
                plan.incoming_video_path if plan is not None else None,
                plan.incoming_manifest_path if plan is not None else None,
                video_etag,
                manifest_sha256,
                (
                    plan.inspection.duration_seconds
                    if plan is not None
                    else None
                ),
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
    rejections: list[tuple[str, str]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(("video_relative_path", "error"))
        writer.writerows(rejections)
    temporary.replace(path)


def run_publisher(
    config: PublisherConfig,
    *,
    storage: PublicationStorage | None,
    inspector: Callable[[Path], VideoInspection] = inspect_video,
) -> PublishSummary:
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
    rejections: list[tuple[str, str]] = []
    configuration_digest = _configuration_digest(config)
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
                checkpoint.record(
                    relative_path,
                    "REJECTED",
                    plan=plan,
                    input_signature=input_signature,
                    local_size=local_stat.st_size,
                    local_mtime_ns=local_stat.st_mtime_ns,
                    error=str(error),
                )
                rejections.append((relative_path, str(error)))
                summary.rejected += 1
    finally:
        checkpoint.close()
        write_rejection_report(config.rejection_report_path, rejections)
    return summary


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="people-counter-publish-manifests",
        description="Generate and atomically publish video manifests to ADLS.",
    )
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--inventory", type=Path)
    parser.add_argument("--video-root", type=Path, required=True)
    parser.add_argument("--partition-prefix")
    parser.add_argument("--storage-account", type=storage_account_name)
    parser.add_argument("--filesystem", type=filesystem_name)
    parser.add_argument("--staging-prefix", default="staging")
    parser.add_argument("--incoming-prefix", default="incoming")
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=Path("manifest-publisher.sqlite3"),
    )
    parser.add_argument(
        "--rejection-report",
        type=Path,
        default=Path("manifest-rejections.csv"),
    )
    parser.add_argument("--max-files", type=positive_int, default=1000)
    parser.add_argument("--chunk-size-mib", type=positive_int, default=8)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--rehash",
        action="store_true",
        help="Ignore completed checkpoint entries and revalidate local bytes.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.dry_run and not args.storage_account:
        parser.error("--storage-account is required unless --dry-run is used")
    if not args.dry_run and not args.filesystem:
        parser.error("--filesystem is required unless --dry-run is used")

    config = PublisherConfig(
        catalog_path=args.catalog,
        inventory_path=args.inventory,
        video_root=args.video_root,
        partition_prefix=args.partition_prefix,
        storage_account=args.storage_account or "dry-run",
        filesystem=args.filesystem or "dry-run",
        staging_prefix=args.staging_prefix,
        incoming_prefix=args.incoming_prefix,
        checkpoint_path=args.checkpoint,
        rejection_report_path=args.rejection_report,
        max_files=args.max_files,
        dry_run=args.dry_run,
        rehash=args.rehash,
    )
    storage = (
        None
        if config.dry_run
        else AzureDataLakeStorage(
            config.storage_account,
            config.filesystem,
            chunk_size=args.chunk_size_mib * 1024 * 1024,
        )
    )
    summary = run_publisher(config, storage=storage)
    print(json.dumps(asdict(summary), sort_keys=True))
    return 2 if summary.rejected else 0


if __name__ == "__main__":
    raise SystemExit(main())
