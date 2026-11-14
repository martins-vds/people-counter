"""Local media access and durable operator jobs, independent of the web UI."""

from __future__ import annotations

import csv
import copy
import io
import json
import logging
import math
import os
import re
import shutil
import sqlite3
import stat
import threading
import traceback
import unicodedata
import uuid
import zipfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from people_counter import backfill_operator as backfill
from people_counter.adls import AzureDataLakeStorage
from people_counter.fabric_automation import redact_evidence
from people_counter.manifest import (
    CATALOG_COLUMNS, CONTENT_TYPES, CameraCatalogEntry, InventoryEntry, ManifestPublisherError,
    VideoMetadata, VideoValidationError, inspect_video,
    load_camera_catalog, load_video_inventory, parse_utc_timestamp, probe_video, resolve_video_metadata,
)
from people_counter.manifest_publisher import (
    MANIFEST_PACKAGE_VERSION, PublisherConfig, ValidatedManifestPackage,
    publish_manifest_package, run_publisher, validate_manifest_package,
)
from people_counter.portal_sources import AdlsFootageSource, SelectedFootageInput

LINE_FIELDS = ("counting_line_x1", "counting_line_y1", "counting_line_x2", "counting_line_y2")
EXCEPTION_FIELDS = ("video_relative_path", "captured_at_utc", "frame_width", "frame_height", *LINE_FIELDS)
REPORT_TYPES = {
    "summary": ("summary.json", "Summary", "JSON"),
    "rejections": ("rejection-report.csv", "Rejections", "CSV"),
    "inventory": ("generated-video-inventory.csv", "Footage inventory", "CSV"),
    "manifest": ("manifest-package.json", "Manifest package", "JSON"),
    "publication": ("publication-summary.json", "Publication summary", "JSON"),
    "publication-rejections": ("publication-rejections.csv", "Publication rejections", "CSV"),
    "registration": ("registration-summary.json", "Registration summary", "JSON"),
    "processing": ("processing-summary.json", "Processing summary", "JSON"),
}
REPORTS = tuple(spec[0] for spec in REPORT_TYPES.values())


def report_type(name: str) -> tuple[str, str, str]:
    if name in REPORT_TYPES:
        return REPORT_TYPES[name]
    for spec in REPORT_TYPES.values():
        if spec[0] == name:
            return spec
    raise ValueError("Unknown report")


def _draft_line(draft: Mapping[str, str], metadata: VideoMetadata | None) -> dict[str, str]:
    if metadata is None:
        raise VideoValidationError("Repair the media before checking its line.", reason_code="VIDEO_LINE_INVALID")
    try:
        line = {name: str(int(draft.get(name, ""))) for name in LINE_FIELDS}
    except ValueError as error:
        raise VideoValidationError(
            "Enter all four video-line coordinates as whole numbers.",
            reason_code="VIDEO_LINE_INVALID", suggested_action="Complete the manual line coordinates.",
        ) from error
    return {"frame_width": str(metadata.width), "frame_height": str(metadata.height), **line}


def _draft_exception(
    saved: Mapping[str, Any], draft: Mapping[str, str],
    metadata: VideoMetadata | None, camera_changed: bool,
) -> dict[str, Any]:
    row = dict(saved)
    if camera_changed and row.get("frame_width"):
        action = draft.get("geometry_action", "")
        if action not in {"keep", "clear"}:
            raise VideoValidationError(
                "Choose whether to keep or clear the video line before reassignment.",
                reason_code="ASSOCIATION_GEOMETRY_CHOICE_REQUIRED",
                suggested_action="Select a video-line choice in Camera association.",
            )
        if action == "clear":
            for name in ("frame_width", "frame_height", *LINE_FIELDS):
                row.pop(name, None)
    if "captured_at_utc" in draft:
        row["captured_at_utc"] = draft["captured_at_utc"]
    if draft.get("line_override") == "yes":
        row.update(_draft_line(draft, metadata))
    return row


def _configuration_covers_video(config: Mapping[str, Any], video: str) -> bool:
    folder = config["folder"]
    return not folder or video.startswith(folder.rstrip("/") + "/")


def _camera_configurations(
    cameras: list[dict[str, Any]], camera_id: str, video: str | None = None,
) -> list[dict[str, Any]]:
    configs = [item for item in cameras if item["catalog"]["camera_id"] == camera_id]
    if video is None:
        return configs
    matching = [item for item in configs if _configuration_covers_video(item, video)]
    return matching or configs


def _prefix_conflicts(row: Mapping[str, Any], other: Mapping[str, Any]) -> bool:
    return row["camera_id"] != other["camera_id"] and (
        other["source_path_prefix"].startswith(row["source_path_prefix"])
        or row["source_path_prefix"].startswith(other["source_path_prefix"])
    )


def _replace_inventory_exception(
    rows: list[dict[str, Any]], relative: str, exception: dict[str, Any],
) -> list[dict[str, Any]]:
    remaining = [row for row in rows if row["id"] != relative]
    if exception.get("captured_at_utc") or exception.get("frame_width"):
        remaining.append(exception)
    return remaining


def _readiness_issue(error: Exception, scope: str) -> dict[str, str]:
    code, action = {
        "assignment": ("MEDIA_UNREADABLE", "Repair or replace this video, then recheck."),
        "package": ("CAMERA_METADATA_INVALID", "Review the camera calibration and recording rules."),
    }[scope]
    return {
        "code": getattr(error, "reason_code", code), "scope": scope, "message": str(error),
        "action": getattr(error, "suggested_action", action),
    }


PORTAL_CATALOG_FIELDS = (*sorted(CATALOG_COLUMNS), "video_relative_path")
METADATA_FIELDS = (
    "camera_timezone", "capture_time_source", "capture_time_regex", "capture_time_format",
    "effective_from_utc", "effective_to_utc",
)


def csv_text(fields: tuple[str, ...], rows: list[dict[str, Any]]) -> str:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} is required")
    return value.strip()


def _name_slug(display_name: str) -> str:
    ascii_name = unicodedata.normalize("NFKD", display_name).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_name.lower()).strip("-")
    if not slug:
        raise ValueError("The display name must contain at least one ASCII letter or digit")
    return slug


def _camera_data(data: Mapping[str, Any]) -> tuple[str, str, dict[str, Any]]:
    catalog = data.get("catalog")
    if not isinstance(catalog, Mapping):
        raise ValueError("Camera configuration must contain catalog metadata")
    camera = _identifier(catalog.get("camera_id"), "Camera ID")
    location = _identifier(catalog.get("location_id"), "Location ID")
    normalized = {
        **data, "catalog": {**catalog, "camera_id": camera, "location_id": location},
    }
    return camera, location, normalized


class LocalFootageSource:
    """Filesystem boundary; cloud sources can later materialize behind this boundary."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve(strict=True)
        if not self.root.is_dir():
            raise ValueError("The configured footage source root must be a directory")
        self.video_root = self.root.parent
        self.assignment_prefix = ""
        self.logical_prefix = self.root.name + "/"
        self.label = f"Local folder / {self.root}"

    def canonical(self, relative: str, *, directory: bool = False) -> str:
        path = self.resolve(relative)
        if directory and not path.is_dir():
            raise ValueError("Select a footage folder")
        value = path.relative_to(self.root).as_posix()
        return "" if value == "." else value

    def relative_video(self, video: str) -> str:
        return self.logical_prefix + self.canonical(video)

    def properties(self, video: str) -> dict[str, Any]:
        path = self.resolve(video)
        if not path.is_file() or path.suffix.lower() not in CONTENT_TYPES or path.is_symlink():
            raise ValueError("Select a supported regular video file")
        metadata = path.stat()
        return {"size": metadata.st_size, "mtime_ns": metadata.st_mtime_ns}

    def videos(self, folder: str, limit: int = 10000) -> list[str]:
        result = []
        for path in self.resolve(folder).rglob("*"):
            if path.is_file() and path.suffix.lower() in CONTENT_TYPES:
                if path.is_symlink() or not path.resolve().is_relative_to(self.root):
                    raise ValueError("Symbolic links are not supported in package selection")
                result.append(path.relative_to(self.root).as_posix())
                if len(result) > limit:
                    raise ValueError(f"Selection exceeds the {limit:,}-video package limit")
        return sorted(result)

    def resolve(self, relative: str) -> Path:
        path = (self.root / relative).resolve()
        if not path.is_relative_to(self.root):
            raise ValueError("Path is outside the configured source root")
        if not path.exists():
            raise ValueError("The selected source path does not exist")
        return path

    def prefix(self, folder: str) -> str:
        path = self.resolve(folder)
        if not path.is_dir():
            raise ValueError("Select a footage folder")
        return path.relative_to(self.video_root).as_posix() + "/"

    def _children(self, folder: str) -> list[os.DirEntry[str]]:
        path = self.resolve(folder)
        if not path.is_dir():
            raise ValueError("Select a footage folder")
        try:
            with os.scandir(path) as children:
                return list(children)
        except OSError as error:
            logging.getLogger(__name__).warning("Cannot read footage folder %s", folder, exc_info=True)
            raise ValueError(f"Cannot read folder: {error.strerror or error}") from error

    def entries(self, folder: str) -> list[dict[str, Any]]:
        entries = []
        for child in self._children(folder):
            path = Path(child.path)
            row: dict[str, Any] = {
                "name": child.name, "path": path.relative_to(self.root).as_posix(),
                "blocked": False, "directory": False, "video": False,
                "size_bytes": None, "modified_utc": None, "error": None,
            }
            try:
                row["blocked"] = not path.resolve().is_relative_to(self.root)
                if not row["blocked"]:
                    metadata = child.stat()
                    row["directory"] = stat.S_ISDIR(metadata.st_mode)
                    regular = stat.S_ISREG(metadata.st_mode)
                    row["video"] = regular and path.suffix.lower() in CONTENT_TYPES
                    row["size_bytes"] = metadata.st_size if regular else None
                    row["modified_utc"] = datetime.fromtimestamp(
                        metadata.st_mtime, timezone.utc,
                    ).strftime("%Y-%m-%d %H:%M UTC")
            except (OSError, RuntimeError, OverflowError, ValueError) as error:
                logging.getLogger(__name__).warning("Cannot inspect footage entry %s", path, exc_info=True)
                row["error"] = f"Metadata unavailable: {error}"
            entries.append(row)
        return sorted(entries, key=lambda row: (row["blocked"], not row["directory"], row["name"].casefold()))

    def folders(self, folder: str) -> list[dict[str, Any]]:
        nodes = []
        for child in self._children(folder):
            path = Path(child.path)
            try:
                if path.resolve().is_relative_to(self.root) and child.is_dir():
                    nodes.append({
                        "name": child.name, "path": path.relative_to(self.root).as_posix(),
                        "children": None,
                    })
            except (OSError, RuntimeError) as error:
                logging.getLogger(__name__).warning("Cannot inspect folder %s", path, exc_info=True)
                raise ValueError(f"Cannot load folders: {error}") from error
        return sorted(nodes, key=lambda node: node["name"].casefold())

    def folder_tree(self, folder: str) -> list[dict[str, Any]]:
        relative = self.resolve(folder).relative_to(self.root)
        tree = self.folders("")
        branch = tree
        for depth in range(1, len(relative.parts) + 1):
            path = Path(*relative.parts[:depth]).as_posix()
            node = next((item for item in branch if item["path"] == path), None)
            if node is None:
                raise ValueError("The folder changed while browsing. Refresh and try again.")
            node["children"] = self.folders(path)
            branch = node["children"]
        return tree

    def frame(self, video: str, seconds: float = 0) -> tuple[bytes, int, int]:
        import cv2

        path = self.resolve(video)
        if not path.is_file() or path.suffix.lower() not in CONTENT_TYPES:
            raise ValueError("Select a supported video file")
        if not math.isfinite(seconds) or seconds < 0:
            raise ValueError("Preview time must be finite and nonnegative")
        capture = cv2.VideoCapture(str(path))
        try:
            if seconds:
                capture.set(cv2.CAP_PROP_POS_MSEC, seconds * 1000)
            ok, frame = capture.read()
            if not ok:
                raise ValueError("Cannot decode this frame; check the video or choose an earlier time")
            height, width = frame.shape[:2]
            encoded, content = cv2.imencode(".jpg", frame)
            if not encoded:
                raise ValueError("Cannot encode the preview frame")
            return content.tobytes(), width, height
        finally:
            capture.release()


class PortalStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS entries (
                    kind TEXT NOT NULL, id TEXT NOT NULL, data TEXT NOT NULL,
                    PRIMARY KEY(kind,id));
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL,
                    status TEXT NOT NULL, result TEXT, error TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
                CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY, job_id TEXT NOT NULL, message TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS package_revisions (
                    package_key TEXT NOT NULL, name TEXT NOT NULL, revision INTEGER NOT NULL,
                    job_id TEXT NOT NULL UNIQUE, deleted INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(package_key,revision));
            """)
        self._migrate_locations()

    def _migrate_locations(self) -> None:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            locations: dict[str, str] = {}
            updates = []
            for row in db.execute("SELECT kind,id,data FROM entries WHERE kind IN ('camera','camera_identity')"):
                original = json.loads(row["data"])
                if row["kind"] == "camera":
                    camera, location, normalized = _camera_data(original)
                    if normalized != original:
                        updates.append((json.dumps(normalized, allow_nan=False), row["id"]))
                else:
                    camera = row["id"]
                    location = _identifier(original.get("location_id"), "Location ID")
                previous = locations.setdefault(camera, location)
                if previous != location:
                    raise ValueError(
                        f"Camera {camera!r} must have one location; existing configurations conflict. "
                        "Correct those assignments before upgrading; no metadata was changed."
                    )
            for location in sorted(set(locations.values())):
                db.execute(
                    "INSERT INTO entries VALUES ('location',?,?) "
                    "ON CONFLICT(kind,id) DO NOTHING",
                    (location, json.dumps({"display_name": location})),
                )
            for row in db.execute("SELECT id,data FROM entries WHERE kind='location'").fetchall():
                data = json.loads(row["data"])
                if "display_name" not in data:
                    db.execute(
                        "UPDATE entries SET data=? WHERE kind='location' AND id=?",
                        (json.dumps({**data, "display_name": row["id"]}), row["id"]),
                    )
            for camera, location in locations.items():
                db.execute(
                    "INSERT INTO entries VALUES ('camera_identity',?,?) ON CONFLICT(kind,id) DO NOTHING",
                    (camera, json.dumps({"display_name": camera, "location_id": location})),
                )
            db.executemany("UPDATE entries SET data=? WHERE kind='camera' AND id=?", updates)

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def entries(self, kind: str) -> list[dict[str, Any]]:
        with self.connection() as db:
            return [
                {"id": row["id"], **json.loads(row["data"])}
                for row in db.execute("SELECT id,data FROM entries WHERE kind=? ORDER BY id", (kind,))
            ]

    def put(self, kind: str, identity: str, data: Mapping[str, Any]) -> None:
        normalized = dict(data)
        if kind == "location":
            identity = _identifier(identity, "Location ID")
        with self.connection() as db:
            if kind == "camera":
                db.execute("BEGIN IMMEDIATE")
                camera, location, normalized = _camera_data(data)
                if db.execute(
                    "SELECT 1 FROM entries WHERE kind='location' AND id=?", (location,),
                ).fetchone() is None:
                    raise ValueError("Select an existing location before assigning a camera")
                registered = db.execute(
                    "SELECT data FROM entries WHERE kind='camera_identity' AND id=?", (camera,),
                ).fetchone()
                if registered is None:
                    raise ValueError("Select an existing registered camera before saving calibration")
                if json.loads(registered["data"])["location_id"] != location:
                    raise ValueError(f"Camera {camera!r} must have one location; select its registered location")
            db.execute(
                "INSERT INTO entries VALUES (?,?,?) ON CONFLICT(kind,id) DO UPDATE SET data=excluded.data",
                (kind, identity, json.dumps(normalized, allow_nan=False)),
            )

    def add_location(self, display_name: str) -> str:
        display_name = _identifier(display_name, "Location name")
        identity = _name_slug(display_name)
        try:
            with self.connection() as db:
                db.execute(
                    "INSERT INTO entries VALUES ('location',?,?)",
                    (identity, json.dumps({"display_name": display_name})),
                )
        except sqlite3.IntegrityError as error:
            raise ValueError(f"Location {identity!r} already exists") from error
        return identity

    def add_camera(self, display_name: str, location_id: str) -> str:
        display_name = _identifier(display_name, "Camera name")
        location_id = _identifier(location_id, "Location ID")
        identity = f"{location_id}-{_name_slug(display_name)}"
        try:
            with self.connection() as db:
                db.execute("BEGIN IMMEDIATE")
                if db.execute(
                    "SELECT 1 FROM entries WHERE kind='location' AND id=?", (location_id,),
                ).fetchone() is None:
                    raise ValueError("Select an existing location before adding a camera")
                db.execute(
                    "INSERT INTO entries VALUES ('camera_identity',?,?)",
                    (identity, json.dumps({"display_name": display_name, "location_id": location_id})),
                )
        except sqlite3.IntegrityError as error:
            raise ValueError(f"Camera {identity!r} already exists; choose a different camera name") from error
        return identity

    def assign_footage(self, camera_id: str, videos: list[str]) -> None:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute(
                "SELECT 1 FROM entries WHERE kind='camera_identity' AND id=?", (camera_id,),
            ).fetchone() is None:
                raise ValueError("Select an existing registered camera")
            db.executemany(
                "INSERT INTO entries VALUES ('footage',?,?) "
                "ON CONFLICT(kind,id) DO UPDATE SET data=excluded.data",
                [(video, json.dumps({"camera_id": camera_id})) for video in videos],
            )

    def locations(self) -> list[dict[str, Any]]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT kind,id,data FROM entries WHERE kind IN ('location','camera_identity') ORDER BY id"
            ).fetchall()
        names = {
            row["id"]: json.loads(row["data"])["display_name"]
            for row in rows if row["kind"] == "location"
        }
        cameras: dict[str, set[str]] = {location: set() for location in names}
        for row in rows:
            if row["kind"] == "camera_identity":
                camera = row["id"]
                location = json.loads(row["data"])["location_id"]
                if location not in cameras:
                    raise ValueError(f"Camera {camera!r} references an unknown location")
                cameras[location].add(camera)
        return [
            {
                "id": location,
                "display_name": names[location],
                "camera_ids": sorted(identities), "camera_count": len(identities),
            }
            for location, identities in sorted(cameras.items())
        ]

    def enqueue(self, kind: str, payload: Mapping[str, Any]) -> str:
        identity = uuid.uuid4().hex
        with self.connection() as db:
            db.execute(
                "INSERT INTO jobs(id,kind,payload,status) VALUES (?,?,?,'QUEUED')",
                (identity, kind, json.dumps(dict(payload), allow_nan=False)),
            )
        return identity

    def job(self, identity: str) -> dict[str, Any] | None:
        with self.connection() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (identity,)).fetchone()
        if row is None:
            return None
        return {
            **dict(row), "payload": json.loads(row["payload"]),
            "result": json.loads(row["result"]) if row["result"] else None,
        }

    def jobs(self) -> list[dict[str, Any]]:
        with self.connection() as db:
            return [dict(row) for row in db.execute(
                "SELECT id,kind,status,error,created_at FROM jobs ORDER BY rowid DESC LIMIT 100"
            )]

    def event(self, identity: str, message: str) -> None:
        with self.connection() as db:
            db.execute("INSERT INTO events(job_id,message) VALUES (?,?)", (
                identity, str(redact_evidence(message)),
            ))

    def events(self, identity: str) -> list[str]:
        with self.connection() as db:
            return [row["message"] for row in db.execute(
                "SELECT message FROM events WHERE job_id=? ORDER BY sequence", (identity,)
            )]

    def claim(self) -> dict[str, Any] | None:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT id FROM jobs WHERE status='QUEUED' "
                "AND NOT EXISTS(SELECT 1 FROM jobs WHERE status='RUNNING') ORDER BY rowid LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            db.execute("UPDATE jobs SET status='RUNNING',error=NULL WHERE id=?", (row["id"],))
        return self.job(row["id"])

    def finish(self, identity: str, result: object = None, error: str | None = None) -> None:
        with self.connection() as db:
            db.execute("UPDATE jobs SET status=?,result=?,error=? WHERE id=?", (
                "FAILED" if error is not None else "SUCCEEDED",
                json.dumps(result, allow_nan=False), error, identity,
            ))

    def recover(self) -> None:
        with self.connection() as db:
            db.execute(
                "UPDATE jobs SET status='INTERRUPTED',error="
                "'Server stopped. Accepted Fabric jobs may still be running; resume with retained state.' "
                "WHERE status='RUNNING'"
            )

    def retry(self, identity: str) -> None:
        with self.connection() as db:
            changed = db.execute(
                "UPDATE jobs SET status='QUEUED',error=NULL,result=NULL "
                "WHERE id=? AND status IN ('FAILED','INTERRUPTED')", (identity,),
            ).rowcount
        if not changed:
            raise ValueError("Only failed or interrupted jobs can be resumed")


class PortalService:
    def __init__(
        self, source_root: Path, models_root: Path, state: Path, *,
        allow_cloud: bool = False, output_root: Path | None = None,
    ):
        self.source: LocalFootageSource | AdlsFootageSource = LocalFootageSource(source_root)
        self.local_source = self.source
        self.models_root = models_root.resolve()
        self.state = state.resolve()
        self.state.mkdir(parents=True, exist_ok=True)
        self.output_root = (output_root or self.state / "packages").resolve()
        if self.output_root.is_relative_to(self.source.root) or self.source.root.is_relative_to(self.output_root):
            raise ValueError("The package output root must be separate from the source root")
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.store = PortalStore(self.state / "portal.sqlite3")
        roots = self.store.entries("local_source")
        if roots and roots[0]["root"] != str(self.local_source.root):
            raise ValueError("This portal state belongs to a different local footage root. Restore that root or use a separate state directory.")
        if not roots:
            self.store.put("local_source", "root", {"root": str(self.local_source.root)})
        known = {item["id"] for item in self.store.entries("footage")}
        for item in self.store.entries("camera"):
            if item["video"] not in known:
                self.store.assign_footage(item["catalog"]["camera_id"], [item["video"]])
                known.add(item["video"])
        self.catalog = self.state / "backfill-catalog.json"
        self.allow_cloud = allow_cloud
        self.worker_error: str | None = None
        self._azure: Any = None

    @property
    def azure(self) -> Any:
        if self._azure is None:
            from people_counter.portal_azure import PortalAzureSession

            self._azure = PortalAzureSession()
        return self._azure

    def new_draft(self, name: str) -> dict[str, Any]:
        identity = uuid.uuid4().hex
        draft = {
            "name": _identifier(name, "Package name"), "source": {"kind": "local", "root": str(self.local_source.root)},
            "mode": "individual", "videos": [], "observations": {}, "folder": "", "destination": "",
        }
        self.store.put("draft", identity, draft)
        return {"id": identity, **draft}

    def draft(self, identity: str) -> dict[str, Any]:
        self.job_dir(identity)
        found = next((row for row in self.store.entries("draft") if row["id"] == identity), None)
        if found is None:
            raise ValueError("Package draft does not exist")
        return found

    def for_source(self, descriptor: Mapping[str, Any]) -> PortalService:
        kind = descriptor.get("kind", "local")
        if kind not in {"local", "adls"}:
            raise ValueError("Select Local folder or ADLS Gen2")
        if kind == "local" and descriptor.get("root", str(self.local_source.root)) != str(self.local_source.root):
            raise ValueError("The draft belongs to a different configured local root")
        result = copy.copy(self)
        result.source = self.local_source if kind == "local" else AdlsFootageSource(descriptor, self.azure, self.store)
        return result

    def full_preview(self, video: str, seconds: float, *, observations: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(self.source, AdlsFootageSource):
            raise ValueError("Full-download fallback applies only to ADLS footage")
        properties = dict(observations)
        if self.source.properties(video) != properties:
            raise ValueError("Footage changed after approval. Review and approve the full preview again.")
        if shutil.disk_usage(self.state).free < properties["size"] + 64 * 1024 * 1024:
            raise ValueError("Insufficient private disk space for the approved full preview")
        identity = uuid.uuid4().hex
        with TemporaryDirectory(prefix="full-preview-", dir=self.state) as directory:
            path = Path(directory) / Path(video).name
            inputs = SelectedFootageInput(self.source, [video], {video: properties})
            count = 0
            with path.open("wb") as handle:
                for chunk in inputs.chunks(self.source.relative_video(video)):
                    count += len(chunk)
                    if count > properties["size"]:
                        raise ValueError("ADLS footage grew during full download")
                    handle.write(chunk)
            if count != properties["size"] or self.source.properties(video) != properties:
                raise ValueError("ADLS footage changed or was truncated during full download")
            metadata = probe_video(path)
            content, width, height = LocalFootageSource(Path(directory)).frame(path.name, seconds)
            if (width, height) != (metadata.width, metadata.height):
                raise ValueError("Full preview dimensions conflict with media metadata")
        previews = self.state / "previews"
        previews.mkdir(exist_ok=True)
        (previews / (identity + ".jpg")).write_bytes(content)
        result = {
            "video": self.source.relative_video(video), "seconds": seconds,
            "observations": properties, "metadata": asdict(metadata),
        }
        self.store.put("source_preview", identity, result)
        return {"id": identity, **result}

    def update_draft(self, identity: str, changes: Mapping[str, Any]) -> None:
        draft = self.draft(identity)
        source = dict(changes.get("source", draft["source"]))
        if source.get("kind") == "local":
            source["root"] = str(self.local_source.root)
        scoped = self.for_source(source)
        changed_source = source != draft["source"]
        mode = "individual" if changed_source else changes.get("mode", draft["mode"])
        if mode not in {"individual", "recursive"}:
            raise ValueError("Select individual videos or a recursive folder")
        folder = "" if changed_source else scoped.source.canonical(changes.get("folder", draft["folder"]), directory=True)
        videos = [] if changed_source else changes.get("videos", draft["videos"])
        refresh = not changed_source and mode == "recursive" and any(key in changes for key in ("folder", "mode"))
        if refresh:
            videos = scoped.source.videos(folder)
        if not isinstance(videos, list) or len(videos) > 10000:
            raise ValueError("Select at most 10,000 videos")
        videos = sorted({scoped.source.canonical(video) for video in videos})
        previous = {} if changed_source or refresh else draft["observations"]
        observations = {
            video: previous[video] if video in previous else scoped.source.properties(video)
            for video in videos
        }
        saved = {
            **draft, "source": dict(source), "folder": folder, "mode": mode, "videos": videos,
            "observations": observations, "destination": str(changes.get("destination", draft["destination"])),
        }
        self.store.put("draft", identity, {key: value for key, value in saved.items() if key != "id"})
        if isinstance(scoped.source, AdlsFootageSource):
            self.store.put("source", scoped.source.identity, dict(source))

    def source_descriptor(self, identity: str) -> dict[str, Any]:
        if identity == "local":
            return {"kind": "local", "root": str(self.local_source.root)}
        found = next((row for row in self.store.entries("source") if row["id"] == identity), None)
        if found is None:
            raise ValueError("The source metadata is missing. Restore the original source descriptor.")
        return found

    def reference_context(self, video: str) -> dict[str, str]:
        if video.startswith("\0"):
            identity, separator, path = video[1:].partition(":")
            if not separator:
                raise ValueError("Invalid source-qualified footage reference")
            source = self.source_descriptor(identity)
            return {
                "video": path, "source_id": identity,
                "label": f"{source['account']} / {source['filesystem']}",
            }
        return {"video": video, "source_id": "local", "label": "Local folder"}

    def draft_input(self, draft: Mapping[str, Any]) -> SelectedFootageInput:
        return SelectedFootageInput(self.for_source(draft["source"]).source, draft["videos"], draft["observations"])

    def generate_draft(self, identity: str) -> str:
        draft = self.draft(identity)
        if not draft["videos"]:
            raise ValueError("Select footage before generating a package")
        scoped = self.for_source(draft["source"])
        assignments = scoped.footage_assignments()
        for video in draft["videos"]:
            if not assignments.get(video):
                raise ValueError(f"{video}: Choose an existing camera before generating")
        payload = {
            "source": draft["source"], "videos": draft["videos"], "observations": draft["observations"],
            "draft_id": identity,
        }
        if draft["source"]["kind"] == "adls":
            payload["execution_identity"] = self.azure.require_identity()
        return scoped.enqueue_preparation("", name=draft["name"], destination=draft["destination"], selection=payload)

    def job_dir(self, identity: str) -> Path:
        if len(identity) != 32 or any(char not in "0123456789abcdef" for char in identity):
            raise ValueError("Invalid job identity")
        return self.state / "jobs" / identity

    def validate_csv(self, fields: tuple[str, ...], rows: list[dict[str, Any]], *, inventory: bool = False) -> None:
        with TemporaryDirectory(dir=self.state) as directory:
            path = Path(directory) / "validation.csv"
            path.write_text(csv_text(fields, rows), encoding="utf-8")
            (load_video_inventory if inventory else load_camera_catalog)(path)

    def footage_assignments(self) -> dict[str, str]:
        assignments = {}
        for item in self.store.entries("camera"):
            assignments.setdefault(item["video"], item["catalog"]["camera_id"])
        assignments.update({
            item["id"]: item["camera_id"] for item in self.store.entries("footage")
        })
        prefix = self.source.assignment_prefix
        return {
            video.removeprefix(prefix): owner for video, owner in assignments.items()
            if (video.startswith(prefix) if prefix else not video.startswith("\0"))
        }

    def video_key(self, video: str) -> str:
        return self.source.assignment_prefix + self.source.canonical(video)

    def media_metadata(self, video: str) -> VideoMetadata:
        if isinstance(self.source, AdlsFootageSource):
            metadata = self.source.probe(video)
            self.source.frame(video)
            return metadata
        path = self.source.resolve(video)
        if not path.is_file() or path.suffix.lower() not in CONTENT_TYPES:
            raise ValueError("Select a supported video file")
        try:
            metadata = probe_video(path)
            self.source.frame(video)
        except OSError as error:
            logging.getLogger(__name__).warning("Cannot read footage %s: %s", video, error)
            raise VideoValidationError(
                f"Cannot read footage: {error}", reason_code="VIDEO_UNREADABLE",
                suggested_action="Restore access to the source file, then recheck.",
            ) from error
        return metadata

    def registered_camera(self, camera_id: str) -> dict[str, Any]:
        camera = next((row for row in self.store.entries("camera_identity") if row["id"] == camera_id), None)
        if camera is None:
            raise ValueError("Select an existing registered camera")
        if not any(row["id"] == camera["location_id"] for row in self.store.entries("location")):
            raise ValueError("The camera's location is missing; correct its location before preparation")
        return camera

    def footage_readiness(self, video: str, *, draft: Mapping[str, str] | None = None) -> dict[str, Any]:
        relative = self.source.relative_video(video)
        assignments = self.footage_assignments()
        saved_camera = assignments.get(video, "")
        if draft is not None:
            assignments = {**assignments, video: draft.get("camera_id", saved_camera)}
        inventory_rows = [
            row for row in self.store.entries("exception")
            if row.get("video_relative_path", row["id"]).startswith(self.source.logical_prefix)
        ]
        result: dict[str, Any] = {
            "media_ok": False, "metadata": None, "issues": [], "config_id": "",
            "camera_values": {}, "captured_at_utc": "", "capture_time_source": "",
            "camera_id": assignments.get(video, ""),
            "exception": next((row for row in inventory_rows if row["id"] == relative), {"video_relative_path": relative}),
        }
        self._check_readiness_media(video, result)
        if not result["camera_id"]:
            result["issues"].append({
                "code": "CAMERA_UNASSIGNED", "scope": "package",
                "message": "This footage has no camera.", "action": "Choose a camera in Camera association.",
            })
            return result
        try:
            self._check_readiness_camera(
                video, relative, assignments, inventory_rows, result, draft, result["camera_id"] != saved_camera,
            )
        except (ManifestPublisherError, ValueError, OSError) as error:
            logging.getLogger(__name__).warning("Package readiness failed for %s: %s", video, error)
            result["issues"].append(_readiness_issue(error, "package"))
        return result

    def _check_readiness_media(self, video: str, result: dict[str, Any]) -> None:
        try:
            result["metadata"] = self.media_metadata(video)
            result["media_ok"] = True
        except (ManifestPublisherError, ValueError, OSError) as error:
            logging.getLogger(__name__).warning("Media readiness failed for %s: %s", video, error)
            result["issues"].append(_readiness_issue(error, "assignment"))

    def _readiness_catalogs(
        self, assignments: Mapping[str, str], inventory_rows: list[dict[str, Any]],
    ) -> tuple[list[CameraCatalogEntry], dict[str, InventoryEntry]]:
        with TemporaryDirectory(dir=self.state) as directory:
            catalog = Path(directory) / "camera.csv"
            inventory = Path(directory) / "inventory.csv"
            catalog.write_text(csv_text(PORTAL_CATALOG_FIELDS, self.camera_catalog(assignments=assignments)), encoding="utf-8")
            inventory.write_text(csv_text(EXCEPTION_FIELDS, inventory_rows), encoding="utf-8")
            return load_camera_catalog(catalog), load_video_inventory(inventory)

    def _check_readiness_camera(
        self, video: str, relative: str, assignments: Mapping[str, str],
        inventory_rows: list[dict[str, Any]], result: dict[str, Any],
        draft: Mapping[str, str] | None, camera_changed: bool,
    ) -> None:
        self.registered_camera(result["camera_id"])
        configs = _camera_configurations(self.store.entries("camera"), result["camera_id"])
        if not configs:
            raise VideoValidationError(
                "Camera is not calibrated. Assign valid reference footage, then choose Calibrate camera.",
                reason_code="CAMERA_NOT_CALIBRATED",
            )
        if len(configs) == 1:
            result.update(camera_values=configs[0]["catalog"], config_id=configs[0]["id"])
        if draft is not None:
            result["exception"] = _draft_exception(result["exception"], draft, result["metadata"], camera_changed)
            inventory_rows = _replace_inventory_exception(inventory_rows, relative, result["exception"])
        entries, exceptions = self._readiness_catalogs(assignments, inventory_rows)
        if result["media_ok"]:
            self._resolve_readiness_geometry(video, relative, configs, result, entries, exceptions)

    def _resolve_readiness_geometry(
        self, video: str, relative: str, configs: list[dict[str, Any]], result: dict[str, Any],
        entries: list[CameraCatalogEntry], exceptions: dict[str, InventoryEntry],
    ) -> None:
        entry, captured_at, source = resolve_video_metadata(relative, entries, exceptions, result["metadata"])
        result.update(captured_at_utc=captured_at.isoformat().replace("+00:00", "Z"), capture_time_source=source)
        applicable = [
            row for row in configs
            if parse_utc_timestamp(row["catalog"]["effective_from_utc"], "effective from") == entry.effective_from_utc
            and (isinstance(self.source, AdlsFootageSource) or _configuration_covers_video(row, video))
        ]
        if len(applicable) == 1:
            result.update(camera_values=applicable[0]["catalog"], config_id=applicable[0]["id"])

    def assign_footage(self, camera_id: str, videos: list[str], *, geometry_action: str = "") -> None:
        if not videos:
            raise ValueError("Select one or more video files")
        self.registered_camera(camera_id)
        canonical = []
        for video in videos:
            self.source.properties(video)
            self.media_metadata(video)
            canonical.append(self.source.canonical(video))
        with self.store.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            for video in sorted(set(canonical)):
                previous = db.execute("SELECT data FROM entries WHERE kind='footage' AND id=?", (self.video_key(video),)).fetchone()
                if previous and json.loads(previous["data"])["camera_id"] == camera_id:
                    continue
                relative = self.source.relative_video(video)
                saved = db.execute("SELECT data FROM entries WHERE kind='exception' AND id=?", (relative,)).fetchone()
                exception = json.loads(saved["data"]) if saved else {}
                if exception.get("frame_width"):
                    if geometry_action not in {"keep", "clear"}:
                        raise ValueError("Choose whether to keep or clear the video line before reassignment")
                    if geometry_action == "clear":
                        for name in ("frame_width", "frame_height", *LINE_FIELDS):
                            exception.pop(name, None)
                        db.execute("DELETE FROM entries WHERE kind='exception' AND id=?", (relative,))
                        if exception.get("captured_at_utc"):
                            db.execute("INSERT INTO entries VALUES ('exception',?,?)", (relative, json.dumps(exception)))
                db.execute(
                    "INSERT INTO entries VALUES ('footage',?,?) ON CONFLICT(kind,id) DO UPDATE SET data=excluded.data",
                    (self.video_key(video), json.dumps({"camera_id": camera_id})),
                )

    def save_camera_line(self, camera_id: str, form: Mapping[str, str]) -> None:
        camera = self.registered_camera(camera_id)
        if form.get("camera_id", camera_id) != camera_id or form.get("location_id", camera["location_id"]) != camera["location_id"]:
            raise ValueError("Camera calibration cannot change camera or location")
        video = form.get("video", "")
        metadata = self.media_metadata(video)
        with self.store.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            owner = db.execute("SELECT data FROM entries WHERE kind='footage' AND id=?", (self.video_key(video),)).fetchone()
            if owner is None or json.loads(owner["data"])["camera_id"] != camera_id:
                raise ValueError("Choose reference footage already associated with this camera")
            configs = [row for row in self.store.entries("camera") if row["catalog"]["camera_id"] == camera_id]
            config_id = form.get("config_id", "")
            if not config_id and len(configs) == 1:
                config_id = configs[0]["id"]
            found = next((row for row in configs if row["id"] == config_id), None)
            if configs and found is None:
                raise ValueError("Choose the existing recording period to recalibrate")
            row = dict(found["catalog"]) if found else {
                "catalog_version": "1", "source_path_prefix": self.source.prefix(""),
                "camera_id": camera_id, "location_id": camera["location_id"],
                "camera_timezone": "UTC", "capture_time_source": "auto",
                "capture_time_regex": "", "capture_time_format": "",
                "effective_from_utc": "1970-01-01T00:00:00Z", "effective_to_utc": "",
            }
            row.update(
                frame_width=str(metadata.width), frame_height=str(metadata.height),
                **{name: str(int(form.get(name, ""))) for name in LINE_FIELDS},
            )
            config_id = config_id or uuid.uuid4().hex
            self.validate_csv(tuple(sorted(CATALOG_COLUMNS)), [
                other["catalog"] for other in configs if other["id"] != config_id
            ] + [row])
            data = {"catalog": row, "folder": found["folder"] if found else "", "video": self.video_key(video)}
            db.execute("INSERT OR REPLACE INTO entries VALUES ('camera',?,?)", (config_id, json.dumps(data)))

    def camera_catalog(self, *, assignments: Mapping[str, str] | None = None) -> list[dict[str, Any]]:
        cameras = self.store.entries("camera")
        assignments = self.footage_assignments() if assignments is None else assignments
        rows = [
            item["catalog"] for item in cameras
            if not any(_prefix_conflicts(item["catalog"], other["catalog"]) for other in cameras)
        ]
        for video, camera_id in assignments.items():
            relative = self.source.relative_video(video)
            rows.extend({
                **item["catalog"], "video_relative_path": relative,
                "source_path_prefix": relative.rsplit("/", 1)[0] + "/",
            } for item in _camera_configurations(
                cameras, camera_id, None if isinstance(self.source, AdlsFootageSource) else video,
            ))
        return rows

    def update_camera_metadata(
        self, camera_id: str, config_id: str, form: Mapping[str, str],
    ) -> None:
        cameras = self.store.entries("camera")
        item = next((
            item for item in cameras
            if item["id"] == config_id and item["catalog"]["camera_id"] == camera_id
        ), None)
        if item is None:
            raise ValueError("Unknown camera configuration")
        row = {**item["catalog"], **{name: form.get(name, "") for name in METADATA_FIELDS}}
        self.validate_csv(tuple(sorted(CATALOG_COLUMNS)), [
            row if other["id"] == config_id else other["catalog"]
            for other in cameras if other["catalog"]["camera_id"] == camera_id
        ])
        self.store.put("camera", config_id, {**item, "catalog": row})

    def save_calibration(self, form: Mapping[str, str]) -> str:
        if form.get("scope") not in {"camera", "video"}:
            raise ValueError("Select camera defaults or a per-video exception")
        video = form.get("video", "")
        _, width, height = self.source.frame(video)
        line = {name: str(int(form.get(name, ""))) for name in LINE_FIELDS}
        dimensions = {"frame_width": str(width), "frame_height": str(height)}
        if form.get("scope") == "video":
            relative = self.source.relative_video(video)
            row = {
                "video_relative_path": relative, "captured_at_utc": form.get("captured_at_utc", ""),
                **dimensions, **line,
            }
            self.validate_csv(EXCEPTION_FIELDS, [row], inventory=True)
            self.store.put("exception", relative, row)
            return "Saved per-video exception. Camera defaults are unchanged."
        folder = form.get("folder", "")
        camera_id = form.get("camera_id", "")
        if not any(item["id"] == camera_id for item in self.store.entries("camera_identity")):
            raise ValueError("Select an existing registered camera before saving calibration")
        assigned = self.footage_assignments().get(video)
        if assigned is not None and assigned != camera_id:
            raise ValueError("Assign this footage to the selected camera before changing its counting line")
        if folder and not video.startswith(folder.rstrip("/") + "/") and assigned != camera_id:
            raise ValueError("The preview video must be inside the selected camera folder")
        row = {
            "catalog_version": "1", "source_path_prefix": self.source.prefix(folder),
            "camera_id": form.get("camera_id", ""), "location_id": form.get("location_id", ""),
            "camera_timezone": form.get("camera_timezone", "UTC"),
            "capture_time_source": form.get("capture_time_source", "auto"),
            "capture_time_regex": form.get("capture_time_regex", ""),
            "capture_time_format": form.get("capture_time_format", ""),
            "effective_from_utc": form.get("effective_from_utc") or "1970-01-01T00:00:00Z",
            "effective_to_utc": form.get("effective_to_utc", ""),
            **dimensions, **line,
        }
        identity = form.get("config_id") or uuid.uuid4().hex
        cameras = self.store.entries("camera")
        if form.get("config_id") and not any(item["id"] == identity for item in cameras):
            raise ValueError("Unknown camera configuration")
        if any(
            item["id"] == identity and item["catalog"]["camera_id"] != camera_id for item in cameras
        ):
            raise ValueError("An existing counting-line configuration cannot move to a different camera")
        candidates = [
            item["catalog"] for item in cameras
            if item["id"] != identity and item["catalog"]["camera_id"] == camera_id
        ] + [row]
        self.validate_csv(tuple(sorted(CATALOG_COLUMNS)), candidates)
        self.store.put("camera", identity, {"catalog": row, "folder": folder, "video": self.video_key(video)})
        if assigned is None:
            self.store.assign_footage(camera_id, [self.video_key(video)])
        return "Saved camera configuration. Generate a new package to use this calibration."

    def video_exception(self, video: str) -> dict[str, Any]:
        self.source.properties(video)
        relative = self.source.relative_video(video)
        return next(
            (row for row in self.store.entries("exception") if row["id"] == relative),
            {"video_relative_path": relative},
        )

    def update_video_exception(
        self, video: str, changes: Mapping[str, str], remove: tuple[str, ...] = (),
    ) -> None:
        relative = self.video_exception(video)["video_relative_path"]
        with self.store.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            saved = db.execute(
                "SELECT data FROM entries WHERE kind='exception' AND id=?", (relative,),
            ).fetchone()
            row = {**(json.loads(saved["data"]) if saved else {}),
                   "video_relative_path": relative, **changes}
            for name in remove:
                row.pop(name, None)
            if row.get("captured_at_utc") or row.get("frame_width"):
                self.validate_csv(EXCEPTION_FIELDS, [row], inventory=True)
                db.execute(
                    "INSERT OR REPLACE INTO entries VALUES ('exception',?,?)",
                    (relative, json.dumps(row, allow_nan=False)),
                )
            else:
                db.execute("DELETE FROM entries WHERE kind='exception' AND id=?", (relative,))

    def save_line(self, form: Mapping[str, str]) -> None:
        video = form.get("video", "")
        if form.get("use_camera") == "yes":
            self.update_video_exception(video, {}, ("frame_width", "frame_height", *LINE_FIELDS))
        elif form.get("scope") == "video":
            _, width, height = self.source.frame(video)
            self.update_video_exception(video, {
                "frame_width": str(width), "frame_height": str(height),
                **{name: str(int(form.get(name, ""))) for name in LINE_FIELDS},
            })
        else:
            raise ValueError("Footage line edits are video-only; use camera calibration to change its default")

    def enqueue(self, kind: str, payload: Mapping[str, Any]) -> str:
        if kind not in {"prepare", "profiles", "publish", "submit"}:
            raise ValueError("Unsupported operator job")
        if kind in {"publish", "submit"} and not self.allow_cloud:
            raise ValueError("Cloud actions are disabled")
        return self.store.enqueue(kind, payload)

    def enqueue_preparation(
        self, folder: str, *, name: str = "", destination: str = "",
        selection: Mapping[str, Any] | None = None,
    ) -> str:
        if isinstance(self.source, AdlsFootageSource) and selection is None:
            raise ValueError("Use a package draft to freeze ADLS footage selection")
        self.source.prefix(folder)
        relative = self.source.canonical(folder, directory=True)
        folder = "" if relative == "." else relative
        calibrated = {item["catalog"]["camera_id"] for item in self.store.entries("camera")}
        payload = {
            "folder": folder,
            "cameras": self.camera_catalog(),
            "exceptions": self.store.entries("exception"),
            "uncalibrated": sorted({
                camera_id for video, camera_id in self.footage_assignments().items()
                if (video == folder or video.startswith(folder.rstrip("/") + "/") or not folder)
                and camera_id not in calibrated
            }),
        }
        if selection is not None:
            payload.update(selection)
            payload.update(self._selected_catalog_snapshot(selection["videos"], calibrated))
        if not name and not destination:
            return self.enqueue("prepare", payload)
        name = _identifier(name, "Package name")
        key = _name_slug(name)
        relative = Path(destination)
        parent = (self.output_root / relative).resolve()
        if relative.is_absolute() or ".." in relative.parts or not parent.is_relative_to(self.output_root):
            raise ValueError("Package destination must remain within the configured output root")
        identity = uuid.uuid4().hex
        with self.store.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute(
                "SELECT name,MAX(revision) AS revision FROM package_revisions WHERE package_key=?",
                (key,),
            ).fetchone()
            if previous["name"] and previous["name"].casefold() != name.casefold():
                raise ValueError("A different package name already uses this generated identifier")
            revision = (previous["revision"] or 0) + 1
            package = parent / key / f"v{revision}"
            if not package.resolve().is_relative_to(self.output_root):
                raise ValueError("Package destination must remain within the configured output root")
            try:
                package.mkdir(parents=True, exist_ok=False)
            except OSError as error:
                raise ValueError(f"Cannot reserve package destination: {error}") from error
            payload.update(
                output_dir=str(package), output_root=str(self.output_root),
                manifest_package_version=MANIFEST_PACKAGE_VERSION,
            )
            db.execute(
                "INSERT INTO jobs(id,kind,payload,status) VALUES (?,'prepare',?,'QUEUED')",
                (identity, json.dumps(payload, allow_nan=False)),
            )
            db.execute(
                "INSERT INTO package_revisions(package_key,name,revision,job_id) VALUES (?,?,?,?)",
                (key, name, revision, identity),
            )
        return identity

    def _selected_catalog_snapshot(self, videos: list[str], calibrated: set[str]) -> dict[str, Any]:
        selected = set(videos)
        relative = {self.source.relative_video(video) for video in videos}
        assignments = {video: owner for video, owner in self.footage_assignments().items() if video in selected}
        return {
            "cameras": [
                row for row in self.camera_catalog(assignments=assignments)
                if row.get("video_relative_path") in relative
            ],
            "uncalibrated": sorted({owner for owner in assignments.values() if owner not in calibrated}),
            "exceptions": [
                row for row in self.store.entries("exception")
                if row.get("video_relative_path", row["id"]) in relative
            ],
        }

    def packages(self) -> list[dict[str, Any]]:
        with self.store.connection() as db:
            rows = db.execute(
                "SELECT j.id,j.status,j.created_at,r.name,r.revision,r.deleted,j.payload "
                "FROM jobs j LEFT JOIN package_revisions r ON r.job_id=j.id "
                "WHERE j.kind='prepare' ORDER BY j.rowid DESC"
            ).fetchall()
        return [{
            **dict(row), "name": row["name"] or "Historical package",
            "destination": json.loads(row["payload"]).get(
                "output_dir", str(self.job_dir(row["id"]) / "package"),
            ),
            "schema_version": json.loads(row["payload"]).get("manifest_package_version"),
        } for row in rows]

    def package_dir(self, identity: str) -> Path:
        job = self.store.job(identity)
        if job is None or job["kind"] != "prepare":
            raise ValueError("Select an existing manifest package")
        with self.store.connection() as db:
            row = db.execute(
                "SELECT deleted FROM package_revisions WHERE job_id=?", (identity,),
            ).fetchone()
        if row and row["deleted"]:
            raise ValueError("Package artifacts were explicitly deleted")
        payload = job["payload"]
        allowed = Path(payload.get("output_root", self.job_dir(identity))).resolve()
        saved_path = Path(payload.get("output_dir", self.job_dir(identity) / "package"))
        path = saved_path.resolve()
        if path != saved_path:
            raise ValueError("Package destination changed since preparation was queued")
        if path == allowed or not path.is_relative_to(allowed) or path.is_relative_to(self.local_source.root):
            raise ValueError("Package destination is outside its saved output root")
        return path

    def _retain_reports(self, identity: str, package: Path) -> None:
        root = self.job_dir(identity)
        root.mkdir(parents=True, exist_ok=True)
        for name in REPORTS:
            path = package / name
            if path.is_file():
                shutil.copy2(path, root / name)

    def delete_package(self, identity: str) -> None:
        package = self.package_dir(identity)
        with self.store.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            active = db.execute(
                "SELECT 1 FROM jobs WHERE status IN ('QUEUED','RUNNING','INTERRUPTED') "
                "AND (id=? OR json_extract(payload,'$.package_id')=?)", (identity, identity),
            ).fetchone()
            if active:
                raise ValueError("Package is referenced by active or interrupted work")
            self._retain_reports(identity, package)
            # Keep a tombstone even for legacy, ungrouped packages.
            db.execute(
                "INSERT INTO package_revisions(package_key,name,revision,job_id,deleted) "
                "VALUES (?, 'Historical package',1,?,1) ON CONFLICT(job_id) DO UPDATE SET deleted=1",
                ("legacy-" + identity, identity),
            )
        try:
            archive = self.job_dir(identity) / "manifest-package.zip"
            if archive.exists():
                archive.unlink()
            if package.exists():
                shutil.rmtree(package)
        except OSError as error:
            logging.getLogger(__name__).exception("Deleted package artifact cleanup failed for %s", identity)
            raise ValueError("Package is marked deleted, but artifact cleanup failed; an administrator must remove the retained files") from error

    def enqueue_cloud(
        self, package_id: str, target: str, profile: str, action: str, wait: bool, *,
        overrides: Mapping[str, Any] | None = None, capacity: str = "",
    ) -> str:
        if not self.allow_cloud:
            raise ValueError("Cloud actions are disabled")
        execution_identity = self.azure.require_identity()
        package = self.store.job(package_id)
        if package is None or package["kind"] != "prepare" or package["status"] != "SUCCEEDED":
            raise ValueError("Select a successfully prepared package")
        self.package_dir(package_id)
        if action not in {"publish", "submit"}:
            raise ValueError("Select publication or explicit submission")
        if action == "publish" and overrides:
            raise ValueError("Inference overrides apply only to processing submissions")
        if capacity and capacity not in {f"F{2 ** exponent}" for exponent in range(1, 12)}:
            raise ValueError("Select an available capacity SKU or Unknown")
        config = backfill.load_target(self.catalog, target, profile, overrides=overrides)
        payload = {
            "package_id": package_id, "config": asdict(config), "wait": wait,
            "execution_identity": execution_identity,
        }
        if capacity:
            payload["capacity"] = {"sku": capacity, "source": "operator-declared"}
        # Serialize eligibility with deletion so a selected revision cannot disappear while queued.
        with self.store.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            self.package_dir(package_id)
            identity = uuid.uuid4().hex
            db.execute("INSERT INTO jobs(id,kind,payload,status) VALUES (?,?,?,'QUEUED')", (
                identity, action, json.dumps(payload, allow_nan=False),
            ))
        return identity

    def _prepare(self, identity: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        if payload.get("uncalibrated"):
            raise ValueError(
                "Save a counting line for assigned cameras before preparation: "
                + ", ".join(payload["uncalibrated"])
            )
        if not payload["cameras"]:
            raise ValueError("Add camera metadata before preparing a manifest package")
        folder = self.job_dir(identity)
        catalog = folder / "camera-catalog.csv"
        inventory = folder / "exceptions.csv"
        catalog.write_text(csv_text(PORTAL_CATALOG_FIELDS, payload["cameras"]), encoding="utf-8")
        inventory.write_text(csv_text(EXCEPTION_FIELDS, payload["exceptions"]), encoding="utf-8")
        package = self.package_dir(identity)
        source = self.for_source(payload.get("source", {"kind": "local"})).source
        inputs = (
            SelectedFootageInput(
                source, payload["videos"], payload["observations"],
                progress=lambda message: self.store.event(identity, message),
            ) if "videos" in payload else None
        )
        if payload.get("execution_identity"):
            self.azure.credential(payload["execution_identity"]).get_token("https://storage.azure.com/.default")
        config = PublisherConfig(
            catalog, inventory, self.local_source.video_root, source.prefix(payload["folder"]),
            "offline", "offline", "staging", "incoming", folder / "prepare.sqlite3",
            package / "rejection-report.csv", package / "generated-video-inventory.csv",
            10000, True, output_dir=package, summary_report_path=package / "summary.json",
        )

        def inspect(path: Path):
            self.store.event(identity, f"Inspecting and hashing {path.relative_to(self.local_source.root)}")
            return inspect_video(path)

        try:
            if inputs is None:
                summary = run_publisher(config, storage=None, inspector=inspect)
            else:
                summary = run_publisher(config, storage=None, input_source=inputs)
        finally:
            self._retain_reports(identity, package)
        if summary.rejected:
            raise ValueError(f"{summary.rejected} videos rejected. Review the rejection report, correct metadata, and prepare a new package.")
        if not summary.planned:
            raise ValueError("No supported videos were found in this folder")
        if "source" in payload:
            (package / "source-provenance.json").write_text(json.dumps({
                "source": payload["source"], "selected_videos": payload["videos"],
                "observations": payload["observations"],
                "reopen": "Use this portal revision with its original source. The metadata ZIP does not contain footage.",
            }, allow_nan=False, indent=2) + "\n", encoding="utf-8")
        return asdict(summary)

    def _cloud(self, identity: str, kind: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        if not self.allow_cloud:
            raise ValueError("Cloud actions are disabled")
        config = backfill._parse_config({"intake_mode": "explicit", **payload["config"]})
        execution_identity = payload.get("execution_identity") or self.azure.require_identity()
        credential = self.azure.credential(execution_identity)
        identity_policy = self.azure.policy(execution_identity)
        package_dir = self.package_dir(payload["package_id"])
        prepared = self.store.job(payload["package_id"])
        if prepared is None:
            raise ValueError("The prepared package job is missing")
        source_payload = prepared["payload"]
        source = self.for_source(source_payload.get("source", {"kind": "local"})).source
        inputs = (
            SelectedFootageInput(source, source_payload["videos"], source_payload["observations"])
            if "videos" in source_payload else None
        )
        publication = backfill.publication_config(
            package_dir, self.local_source.video_root, self.job_dir(identity), config,
        )
        package = validate_manifest_package(publication, input_source=inputs)
        self.protect_source_files(source, source_payload.get("videos", []), config, package)
        if kind == "publish":
            self.store.event(identity, "Publishing videos first and final manifests last")
            storage = AzureDataLakeStorage(
                config.storage_account, config.filesystem, chunk_size=publication.chunk_size,
                credential=credential,
                identity_policy=identity_policy,
            )
            summary = publish_manifest_package(publication, storage, validated_package=package, input_source=inputs)
            if summary.rejected:
                raise ValueError("Publication rejected videos; inspect the publication rejection report")
            return asdict(summary)
        if payload["wait"]:
            with backfill.processing_reader(config, credential=credential) as connection:
                result = backfill.submit(
                    package, publication, config, self.job_dir(identity),
                    credential=credential, input_source=inputs, identity_policy=identity_policy,
                )
                result.update(backfill.wait_for_processing(connection, config, self.job_dir(identity)))
                return result
        return backfill.submit(
            package, publication, config, self.job_dir(identity),
            credential=credential, input_source=inputs, identity_policy=identity_policy,
        )

    def protect_source_files(
        self, source: LocalFootageSource | AdlsFootageSource, videos: list[str],
        config: backfill.BackfillConfig, package: ValidatedManifestPackage,
    ) -> None:
        if not isinstance(source, AdlsFootageSource):
            return
        if (source.account, source.filesystem) != (config.storage_account, config.filesystem):
            return
        source_paths = {source.remote_path(video) for video in videos}
        if any(plan.staging_video_path in source_paths for plan in package.plans):
            raise ValueError("Publication staging would modify source footage. Choose a separate source or target.")

    def run_next_job(self) -> bool:
        job = self.store.claim()
        if job is None:
            return False
        identity, kind, payload = job["id"], job["kind"], job["payload"]
        self.job_dir(identity).mkdir(parents=True, exist_ok=True)
        self.store.event(identity, f"Started {kind}")
        try:
            with backfill.progress_reporter(lambda message: self.store.event(identity, message)):
                if kind == "prepare":
                    result = self._prepare(identity, payload)
                elif kind == "profiles":
                    result = backfill.prepare_catalog(
                        payload["target"], payload["binding"], self.models_root,
                        payload["profiles"], self.catalog,
                        overrides=payload.get("overrides"),
                    )
                else:
                    result = self._cloud(identity, kind, payload)
            self.store.finish(identity, result)
            self.store.event(identity, "Completed")
        except Exception as error:
            message = str(redact_evidence(f"{type(error).__name__}: {error}"))
            logging.getLogger(__name__).error("%s", redact_evidence(traceback.format_exc()))
            self.store.event(identity, message)
            self.store.finish(identity, error=message)
        return True

    def work(self, stop: threading.Event) -> None:
        try:
            self.store.recover()
            while not stop.is_set():
                if not self.run_next_job():
                    stop.wait(0.5)
        except Exception as error:
            self.worker_error = str(redact_evidence(f"Job worker stopped: {type(error).__name__}: {error}"))
            logging.getLogger(__name__).error("%s", self.worker_error)

    def report(self, identity: str, name: str) -> Path:
        name = report_type(name)[0]
        job = self.store.job(identity)
        if job is None or name not in REPORTS:
            raise ValueError("Unknown report")
        root = self.job_dir(identity)
        if (root / name).is_file():
            return root / name
        for path in (
            (self.package_dir(identity) if job["kind"] == "prepare" else root / "package") / name,
        ):
            if path.is_file():
                return path
        raise ValueError("Report is not available")

    def report_preview(
        self, identity: str, name: str, page: int, section: str | None = None,
    ) -> dict[str, Any]:
        if page < 1:
            raise ValueError("Report page must be positive")
        path = self.report(identity, name)
        if path.stat().st_size > 5 * 1024 * 1024:
            raise ValueError("This report exceeds the 5 MiB inline limit. Use Download to inspect it.")
        if path.suffix == ".json":
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (UnicodeError, json.JSONDecodeError) as error:
                raise ValueError(f"Cannot display JSON report: {error}") from error
            collections = (
                {key: value for key, value in data.items() if isinstance(value, list)}
                if isinstance(data, dict) else {"records": data} if isinstance(data, list) else {}
            )
            section = section or next(iter(collections), None)
            if section is not None and section not in collections:
                raise ValueError("Select an existing report section")
            records = collections[section] if section is not None else []
            return {
                "format": "json", "data": (
                    {key: value for key, value in data.items() if key not in collections}
                    if isinstance(data, dict) else None if collections else data
                ),
                "sections": {key: len(value) for key, value in collections.items()},
                "section": section, "records": records[(page - 1) * 50:page * 50],
                "has_next": len(records) > page * 50,
            }
        try:
            with path.open(newline="", encoding="utf-8-sig") as handle:
                reader = csv.reader(handle, strict=True)
                headers = next(reader, [])
                rows = list(islice(reader, (page - 1) * 50, page * 50 + 1))
        except (UnicodeError, csv.Error) as error:
            raise ValueError(f"Cannot display CSV report: {error}") from error
        return {
            "format": "csv", "headers": headers, "rows": rows[:50],
            "has_next": len(rows) > 50,
        }

    def archive(self, identity: str) -> Path:
        job = self.store.job(identity)
        if job is None or job["kind"] != "prepare" or job["status"] != "SUCCEEDED":
            raise ValueError("Only completed manifest packages can be downloaded")
        package = self.package_dir(identity)
        root = self.job_dir(identity)
        archive = root / "manifest-package.zip"
        if archive.exists():
            return archive
        with TemporaryDirectory(dir=root) as directory:
            pending = Path(directory) / "package.zip"
            with zipfile.ZipFile(pending, "w", compression=zipfile.ZIP_DEFLATED) as output:
                for path in sorted(package.rglob("*")):
                    if path.is_file():
                        output.write(path, path.relative_to(package))
            pending.replace(archive)
        return archive
