"""Local media access and durable operator jobs, independent of the web UI."""

from __future__ import annotations

import csv
import io
import json
import logging
import math
import os
import re
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
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from people_counter import backfill_operator as backfill
from people_counter.adls import AzureDataLakeStorage
from people_counter.fabric_automation import redact_evidence
from people_counter.manifest import (
    CATALOG_COLUMNS, CONTENT_TYPES, inspect_video, load_camera_catalog, load_video_inventory,
)
from people_counter.manifest_publisher import (
    PublisherConfig, publish_manifest_package, run_publisher, validate_manifest_package,
)

LINE_FIELDS = ("counting_line_x1", "counting_line_y1", "counting_line_x2", "counting_line_y2")
EXCEPTION_FIELDS = ("video_relative_path", "captured_at_utc", "frame_width", "frame_height", *LINE_FIELDS)
REPORTS = (
    "summary.json", "rejection-report.csv", "generated-video-inventory.csv",
    "manifest-package.json", "publication-summary.json", "publication-rejections.csv",
    "registration-summary.json", "processing-summary.json",
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
    def __init__(self, source_root: Path, models_root: Path, state: Path, *, allow_cloud: bool = False):
        self.source = LocalFootageSource(source_root)
        self.models_root = models_root.resolve()
        self.state = state.resolve()
        self.state.mkdir(parents=True, exist_ok=True)
        self.store = PortalStore(self.state / "portal.sqlite3")
        self.catalog = self.state / "backfill-catalog.json"
        self.allow_cloud = allow_cloud
        self.worker_error: str | None = None

    def job_dir(self, identity: str) -> Path:
        if len(identity) != 32 or any(char not in "0123456789abcdef" for char in identity):
            raise ValueError("Invalid job identity")
        return self.state / "jobs" / identity

    def validate_csv(self, fields: tuple[str, ...], rows: list[dict[str, Any]], *, inventory: bool = False) -> None:
        with TemporaryDirectory(dir=self.state) as directory:
            path = Path(directory) / "validation.csv"
            path.write_text(csv_text(fields, rows), encoding="utf-8")
            (load_video_inventory if inventory else load_camera_catalog)(path)

    def save_calibration(self, form: Mapping[str, str]) -> str:
        if form.get("scope") not in {"camera", "video"}:
            raise ValueError("Select camera defaults or a per-video exception")
        video = form.get("video", "")
        _, width, height = self.source.frame(video)
        line = {name: str(int(form.get(name, ""))) for name in LINE_FIELDS}
        dimensions = {"frame_width": str(width), "frame_height": str(height)}
        if form.get("scope") == "video":
            relative = self.source.resolve(video).relative_to(self.source.video_root).as_posix()
            row = {
                "video_relative_path": relative, "captured_at_utc": form.get("captured_at_utc", ""),
                **dimensions, **line,
            }
            self.validate_csv(EXCEPTION_FIELDS, [row], inventory=True)
            self.store.put("exception", relative, row)
            return "Saved per-video exception. Camera defaults are unchanged."
        folder = form.get("folder", "")
        if not self.source.resolve(video).is_relative_to(self.source.resolve(folder)):
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
        candidates = [item["catalog"] for item in cameras if item["id"] != identity] + [row]
        self.validate_csv(tuple(sorted(CATALOG_COLUMNS)), candidates)
        self.store.put("camera", identity, {"catalog": row, "folder": folder, "video": video})
        return "Saved camera configuration. Generate a new package to use this calibration."

    def enqueue(self, kind: str, payload: Mapping[str, Any]) -> str:
        if kind not in {"prepare", "profiles", "publish", "submit"}:
            raise ValueError("Unsupported operator job")
        if kind in {"publish", "submit"} and not self.allow_cloud:
            raise ValueError("Cloud actions are disabled")
        return self.store.enqueue(kind, payload)

    def enqueue_preparation(self, folder: str) -> str:
        self.source.prefix(folder)
        return self.enqueue("prepare", {
            "folder": folder,
            "cameras": [item["catalog"] for item in self.store.entries("camera")],
            "exceptions": self.store.entries("exception"),
        })

    def enqueue_cloud(self, package_id: str, target: str, profile: str, action: str, wait: bool) -> str:
        package = self.store.job(package_id)
        if package is None or package["kind"] != "prepare" or package["status"] != "SUCCEEDED":
            raise ValueError("Select a successfully prepared package")
        if action not in {"publish", "submit"}:
            raise ValueError("Select publication or explicit submission")
        config = backfill.load_target(self.catalog, target, profile)
        return self.enqueue(action, {"package_id": package_id, "config": asdict(config), "wait": wait})

    def _prepare(self, identity: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        if not payload["cameras"]:
            raise ValueError("Add camera metadata before preparing a manifest package")
        folder = self.job_dir(identity)
        catalog = folder / "camera-catalog.csv"
        inventory = folder / "exceptions.csv"
        catalog.write_text(csv_text(tuple(sorted(CATALOG_COLUMNS)), payload["cameras"]), encoding="utf-8")
        inventory.write_text(csv_text(EXCEPTION_FIELDS, payload["exceptions"]), encoding="utf-8")
        package = folder / "package"
        config = PublisherConfig(
            catalog, inventory, self.source.video_root, self.source.prefix(payload["folder"]),
            "offline", "offline", "staging", "incoming", folder / "prepare.sqlite3",
            package / "rejection-report.csv", package / "generated-video-inventory.csv",
            10000, True, output_dir=package, summary_report_path=package / "summary.json",
        )

        def inspect(path: Path):
            self.store.event(identity, f"Inspecting and hashing {path.relative_to(self.source.root)}")
            return inspect_video(path)

        summary = run_publisher(config, storage=None, inspector=inspect)
        if summary.rejected:
            raise ValueError(f"{summary.rejected} videos rejected. Review the rejection report, correct metadata, and prepare a new package.")
        if not summary.planned:
            raise ValueError("No supported videos were found in this folder")
        return asdict(summary)

    def _cloud(self, identity: str, kind: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        if not self.allow_cloud:
            raise ValueError("Cloud actions are disabled")
        config = backfill._parse_config({"intake_mode": "explicit", **payload["config"]})
        package_dir = self.job_dir(payload["package_id"]) / "package"
        publication = backfill.publication_config(
            package_dir, self.source.video_root, self.job_dir(identity), config,
        )
        package = validate_manifest_package(publication)
        if kind == "publish":
            self.store.event(identity, "Publishing videos first and final manifests last")
            storage = AzureDataLakeStorage(config.storage_account, config.filesystem, chunk_size=publication.chunk_size)
            summary = publish_manifest_package(publication, storage, validated_package=package)
            if summary.rejected:
                raise ValueError("Publication rejected videos; inspect the publication rejection report")
            return asdict(summary)
        if payload["wait"]:
            with backfill.processing_reader(config) as connection:
                result = backfill.submit(package, publication, config, self.job_dir(identity))
                result.update(backfill.wait_for_processing(connection, config, self.job_dir(identity)))
                return result
        return backfill.submit(package, publication, config, self.job_dir(identity))

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
        if self.store.job(identity) is None or name not in REPORTS:
            raise ValueError("Unknown report")
        root = self.job_dir(identity)
        for path in (root / "package" / name, root / name):
            if path.is_file():
                return path
        raise ValueError("Report is not available")

    def archive(self, identity: str) -> Path:
        job = self.store.job(identity)
        if job is None or job["kind"] != "prepare" or job["status"] != "SUCCEEDED":
            raise ValueError("Only completed manifest packages can be downloaded")
        root = self.job_dir(identity)
        archive = root / "manifest-package.zip"
        if archive.exists():
            return archive
        with TemporaryDirectory(dir=root) as directory:
            pending = Path(directory) / "package.zip"
            with zipfile.ZipFile(pending, "w", compression=zipfile.ZIP_DEFLATED) as output:
                for path in sorted((root / "package").rglob("*")):
                    if path.is_file():
                        output.write(path, path.relative_to(root / "package"))
            pending.replace(archive)
        return archive
