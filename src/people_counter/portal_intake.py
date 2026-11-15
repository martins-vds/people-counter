"""Durable, conditional ADLS intake; uncertain remote outcomes remain reserved."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from pathlib import PurePosixPath
from types import SimpleNamespace
from typing import Any

from azure.core import MatchConditions
from azure.core.exceptions import (
    AzureError,
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)

from .manifest import CONTENT_TYPES
from .portal_remote_media import complete_chunks
from .portal_sources import AdlsFootageSource

UPLOAD_CHUNK_BYTES = 8 * 1024 * 1024
_TERMINAL = {"completed", "cancelled"}
_SAFE_MOVE_STATES = {"pending", "rejected", "completed"}
_OPERATION_ERRORS = (AzureError, OSError, ValueError, sqlite3.Error, RuntimeError)


def _overlap(left: str, right: str) -> bool:
    return not left or not right or left == right or left.startswith(right + "/") or right.startswith(left + "/")


def _rewrite(path: str, old: str, new: str) -> str:
    return new + path[len(old):] if path == old or path.startswith(old + "/") else path


class PortalIntake:
    def __init__(self, service: Any) -> None:
        if not isinstance(service.source, AdlsFootageSource):
            raise ValueError("Footage intake requires the configured ADLS source")  # noqa: TRY004 - Portal validation errors are ValueError.
        self.service = service
        self.source = service.source
        self.store = service.store
        self.lock = service.operation_lock
        with self.lock:
            self._digests: dict[str, Any] = getattr(self.store, "_intake_digests", {})
            self.store._intake_digests = self._digests

    def _save(self, record: dict[str, Any]) -> dict[str, Any]:
        self.store.put("intake", record["id"], record)
        return dict(record)

    def record(self, identity: str) -> dict[str, Any]:
        if not isinstance(identity, str) or len(identity) != 32 or any(c not in "0123456789abcdef" for c in identity):
            raise ValueError("Invalid intake session ID")
        found = next((row for row in self.store.entries("intake") if row["id"] == identity), None)
        if found is None or found.get("source") != self.source.descriptor:
            raise ValueError("Intake session does not belong to this source")
        return found

    def _new(self, kind: str, **values: Any) -> dict[str, Any]:
        return {"id": uuid.uuid4().hex, "source": self.source.descriptor, "kind": kind,
                "execution_identity": dict(self.source.azure.require_identity()),
                "state": "queued", "message": "", "results": [], **values}

    def _guard(self, operation: dict[str, Any]) -> None:
        identity = operation.get("execution_identity")
        if not isinstance(identity, dict):
            raise ValueError("Intake execution identity is missing; restore reviewed credentials or cancel the upload")
        self.source.azure.check_identity(identity)

    def _protected(self) -> set[str]:
        result: set[str] = set()
        with self.store.connection() as db:
            rows = db.execute("SELECT kind,payload,status FROM jobs").fetchall()
        for row in rows:
            if row["kind"] == "intake" or (row["kind"] != "prepare" and row["status"] not in {"QUEUED", "RUNNING", "WAITING", "INTERRUPTED"}):
                continue
            self._snapshot_paths(json.loads(row["payload"]), result)
        return result

    def _snapshot_paths(self, value: Any, result: set[str]) -> None:
        if isinstance(value, dict):
            descriptor = value.get("source")
            if (isinstance(descriptor, dict) and descriptor.get("kind") == "adls"
                    and descriptor.get("account") == self.source.account
                    and descriptor.get("filesystem") == self.source.filesystem):
                root = self.source.canonical(descriptor.get("root", ""), directory=True)
                prefix = self.source.remote_root + "/" if self.source.remote_root else ""
                for video in value.get("videos", []):
                    physical = "/".join(part for part in (root, self.source.canonical(video)) if part)
                    if physical.startswith(prefix):
                        result.add(physical[len(prefix):])
            for child in value.values():
                self._snapshot_paths(child, result)
        elif isinstance(value, list):
            for child in value:
                self._snapshot_paths(child, result)

    def _available(self, paths: list[str], own: str | None = None, *, protect: bool = True) -> None:
        protected_paths = self._protected() if protect else set()
        for path in paths:
            if any(_overlap(path, protected) for protected in protected_paths):
                raise ValueError(f"Path is protected by generated evidence or an active job: {path}")
            for operation in self.store.entries("intake"):
                if operation.get("source") != self.source.descriptor or operation["id"] == own or operation["state"] in _TERMINAL:
                    continue
                if any(_overlap(path, reserved) for reserved in operation.get("reservations", [])):
                    raise ValueError(f"Path is reserved by an unfinished intake operation: {path}")

    def assert_available(self, paths: list[str]) -> None:
        with self.lock:
            self._available([self.source.canonical(path, directory=True) for path in paths], protect=False)

    def page(self, folder: str, token: str | None = None) -> tuple[list[dict[str, Any]], str | None]:
        with self.lock:
            entries, next_token = self.source.page(folder, token)
            stages = {row.get("stage") for row in self.store.entries("intake") if row.get("source") == self.source.descriptor}
            visible = []
            for item in entries:
                if item["path"] in stages and self._owned_stage(item["path"]):
                    continue
                item = dict(item)
                item["protected"] = False
                item["read_only"] = not item["directory"] and not item["video"]
                try:
                    self._available([item["path"]])
                except ValueError as error:
                    reserved = str(error).startswith("Path is reserved")
                    item.update(protected=True, read_only=True, blocked=reserved,
                                error=str(error) if reserved else None, write_error=str(error))
                visible.append(item)
            return visible, next_token

    def _client(self, path: str, directory: bool = False) -> Any:
        factory = self.source.client.get_directory_client if directory else self.source.client.get_file_client
        return factory(self.source.remote_path(path))

    def _properties(self, path: str) -> Any:
        self.source.assert_identity()
        try:
            value = self._client(path).get_file_properties()
        except ResourceNotFoundError:
            return None
        # SDK properties use Blob HEAD; ADLS folders carry this system metadata.
        directory = str(value.metadata.get("hdi_isfolder", "false")).lower() == "true"
        return SimpleNamespace(size=value.size, etag=value.etag, metadata=value.metadata,
                               is_directory=directory)

    def _parent(self, path: str) -> None:
        folder = str(PurePosixPath(path).parent)
        folder = "" if folder == "." else folder
        if not self.source.remote_path(folder):
            self.source.assert_identity()
            self.source.client.get_file_system_properties()
            return
        properties = self._properties(folder)
        if properties is None or not properties.is_directory:
            raise ValueError("Destination parent is not a folder")

    def _name(self, name: str) -> str:
        if not name or "/" in name or "\\" in name:
            raise ValueError("Provide one filename, not a path")
        return self.source.canonical(name)

    def _destination(self, path: str, replace: bool, etag: str | None) -> None:
        if "?" in self.source.remote_path(path):
            raise ValueError("Destination paths containing '?' are read-only: native rename interprets SAS suffixes")
        self._parent(path)
        properties = self._properties(path)
        if properties is not None:
            if getattr(properties, "is_directory", False):
                raise ValueError("Folder destination collisions cannot be replaced")
            if not replace or not etag or etag == "*" or properties.etag != etag:
                raise ValueError("Replacement requires confirmation of the current destination ETag")
        elif replace:
            raise ValueError("Reviewed replacement destination no longer exists")

    def create_folder(self, path: str) -> dict[str, Any]:
        path = self.source.canonical(path, directory=True)
        if not path:
            raise ValueError("Cannot create or mutate the configured root")
        with self.lock:
            self._available([path])
            self._parent(path)
            if self._properties(path) is not None:
                raise ValueError("Folder destination already exists")
            operation = self._new("folder", path=path, reservations=[path])
            self._save(operation)
            return self.complete(operation["id"])

    def start_upload(self, folder: str, name: str, size: int, replace: bool = False,
                     destination_etag: str | None = None) -> dict[str, Any]:
        folder = self.source.canonical(folder, directory=True)
        name = self._name(name)
        if PurePosixPath(name).suffix.lower() not in CONTENT_TYPES:
            raise ValueError("Select a supported video file")
        if type(size) is not int or not 0 < size <= self.service.max_upload_bytes:
            raise ValueError("Upload size exceeds the configured per-file limit or is empty")
        path = "/".join(part for part in (folder, name) if part)
        with self.lock:
            self._available([path])
            self._destination(path, replace, destination_etag)
            operation = self._new("upload", path=path, size=size, received=0, replace=replace,
                                  destination_etag=destination_etag, reservations=[path], state="creating")
            operation["stage"] = "/".join(part for part in (
                folder, f".pc-intake-{operation['id']}{PurePosixPath(name).suffix.lower()}",
            ) if part)
            operation["reservations"].append(operation["stage"])
            self._save(operation)
            try:
                self.source.assert_identity()
                self._client(operation["stage"]).create_file(
                    metadata={"pc_intake": operation["id"], "pc_source": self.source.identity},
                    etag="*", match_condition=MatchConditions.IfMissing, retry_total=0,
                )
                self._digests[operation["id"]] = hashlib.sha256()
                operation["state"] = "uploading"
            except _OPERATION_ERRORS as error:
                operation.update(state="recovery", message=str(error))
            return self._save(operation)

    def append_upload(self, identity: str, offset: int, data: bytes) -> dict[str, Any]:
        with self.lock:
            operation = self.record(identity)
            self._guard(operation)
            if operation["kind"] != "upload" or operation["state"] != "uploading":
                raise ValueError("Upload is not accepting bytes")
            if type(offset) is not int or offset != operation["received"]:
                raise ValueError("Upload offset must equal the received byte count")
            if not isinstance(data, bytes) or not 0 < len(data) <= UPLOAD_CHUNK_BYTES:
                raise ValueError("Upload chunk must be nonempty and within the chunk limit")
            if offset + len(data) > min(operation["size"], self.service.max_upload_bytes):
                raise ValueError("Upload exceeds the declared size or configured limit")
            digest = self._digests.get(identity)
            if digest is None:
                raise ValueError("Upload was interrupted; cancel and restart from byte zero")
            if not self._owned_stage(operation["stage"], identity):
                raise ValueError("Upload staging ownership changed")
            operation.update(state="appending", pending_offset=offset, pending_length=len(data))
            self._save(operation)
            try:
                self.source.assert_identity()
                self._client(operation["stage"]).append_data(data, offset=offset, length=len(data), retry_total=0)
                digest.update(data)
                operation.update(state="uploading", received=offset + len(data))
            except _OPERATION_ERRORS as error:
                operation.update(state="recovery", message=str(error))
            return self._save(operation)

    def ready_upload(self, identity: str) -> dict[str, Any]:
        with self.lock:
            operation = self.record(identity)
            self._guard(operation)
            if operation["kind"] != "upload" or operation["state"] != "uploading" or operation["received"] != operation["size"]:
                raise ValueError("Upload must contain exactly the declared bytes")
            digest = self._digests.get(identity)
            if digest is None:
                raise ValueError("Upload was interrupted; cancel and restart from byte zero")
            if not self._owned_stage(operation["stage"], identity):
                raise ValueError("Upload staging ownership changed")
            operation.update(state="flushing", expected_sha256=digest.hexdigest())
            self._save(operation)
            try:
                self.source.assert_identity()
                self._client(operation["stage"]).flush_data(operation["size"], retry_total=0)
                operation["state"] = "queued"
            except _OPERATION_ERRORS as error:
                operation.update(state="recovery", message=str(error))
            finally:
                self._digests.pop(identity, None)
            return self._save(operation)

    def _subtree(self, path: str, properties: Any) -> list[str]:
        if not getattr(properties, "is_directory", False):
            if PurePosixPath(path).suffix.lower() not in CONTENT_TYPES:
                raise ValueError("Unsupported files are read-only")
            return [path]
        self.source.assert_identity()
        videos = []
        for entry in self.source.client.get_paths(path=self.source.remote_path(path), recursive=True):
            if entry.is_directory:
                continue
            if PurePosixPath(entry.name).suffix.lower() not in CONTENT_TYPES:
                raise ValueError("Folder contains unsupported files; subtree is read-only")
            prefix = self.source.remote_root + "/" if self.source.remote_root else ""
            videos.append(self.source.canonical(entry.name.removeprefix(prefix)))
        return videos

    def start_move(self, paths: list[str], destination: str, rename: str | None = None,
                   replacements: dict[str, str] | None = None) -> dict[str, Any]:
        destination = self.source.canonical(destination, directory=True)
        paths = [self.source.canonical(path, directory=True) for path in paths]
        replacements = replacements or {}
        if not paths or any(not path for path in paths) or (rename is not None and len(paths) != 1):
            raise ValueError("Select non-root paths; rename requires a single selection")
        if any(_overlap(left, right) for index, left in enumerate(paths) for right in paths[index + 1:]):
            raise ValueError("Bulk selections must not overlap")
        with self.lock:
            self._available(paths)
            items = []
            for path in paths:
                name = self._name(rename) if rename is not None else PurePosixPath(path).name
                target = "/".join(part for part in (destination, name) if part)
                if _overlap(path, target):
                    raise ValueError("Cannot move a path onto itself, its ancestor or descendant")
                properties = self._properties(path)
                if properties is None:
                    raise ValueError("Move source does not exist")
                directory = bool(getattr(properties, "is_directory", False))
                if not directory and PurePosixPath(target).suffix.lower() not in CONTENT_TYPES:
                    raise ValueError("Destination must be a supported video file")
                self._available([target])
                self._destination(target, target in replacements, replacements.get(target))
                videos = self._subtree(path, properties)
                if not properties.etag or properties.etag == "*":
                    raise ValueError("Move requires a concrete source ETag")
                items.append({"path": path, "target": target, "directory": directory,
                              "source_etag": str(properties.etag), "destination_etag": replacements.get(target),
                              "videos": videos, "state": "pending",
                              "source_observations": {video: self.source.properties(video) for video in videos}})
            targets = [item["target"] for item in items]
            if len(set(targets)) != len(targets) or any(_overlap(target, path) for target in targets for path in paths):
                raise ValueError("Bulk destinations collide or overlap selected sources")
            operation = self._new("move", paths=paths, target=destination, results=items,
                                  reservations=paths + targets)
            return self._save(operation)

    def _hash(self, operation: dict[str, Any], path: str) -> tuple[str, dict[str, Any]]:
        before = self.source.properties(path)
        if path == operation["stage"]:
            owned = self._properties(path)
            if (owned is None or owned.is_directory or owned.metadata.get("pc_intake") != operation["id"]
                    or owned.metadata.get("pc_source") != self.source.identity
                    or {"size": owned.size, "etag": owned.etag} != before):
                raise ValueError("Upload staging ownership or version changed")
        if before["size"] != operation["size"]:
            raise ValueError("Stored upload size does not match")
        digest = hashlib.sha256()
        count = 0

        def check() -> None:
            self._guard(operation)
            self.source.assert_identity()
            if self.record(operation["id"])["state"] != "verifying":
                raise ValueError("Upload verification was cancelled")

        for chunk in complete_chunks(self.source.file_client(path), before["etag"], before["size"], check_identity=check):
            check()
            digest.update(chunk)
            count += len(chunk)
        check()
        if count != before["size"] or self.source.properties(path) != before:
            raise ValueError("Stored upload changed during verification")
        if digest.hexdigest() != operation.get("expected_sha256"):
            raise ValueError("Stored upload bytes differ from received bytes")
        return digest.hexdigest(), before

    def _rename(self, item: dict[str, Any]) -> None:
        self.source.assert_identity()
        client = self._client(item["path"], item["directory"])
        function = client.rename_directory if item["directory"] else client.rename_file
        reviewed = item.get("destination_etag")
        def capture(response: Any) -> None:
            http = response.http_response
            if 200 <= http.status_code < 300:
                item["renamed_etag"] = http.headers.get("ETag")
        function(
            self.source.filesystem + "/" + self.source.remote_path(item["target"]),
            source_etag=item["source_etag"], source_match_condition=MatchConditions.IfNotModified,
            etag=reviewed or "*",
            match_condition=MatchConditions.IfNotModified if reviewed else MatchConditions.IfMissing,
            retry_total=0,
            raw_response_hook=capture,
        )
        if not item.get("renamed_etag"):
            raise ValueError("Native rename did not provide a destination version; reconcile before updating references")

    def complete(self, identity: str) -> dict[str, Any]:
        with self.lock:
            operation = self.record(identity)
            if operation["state"] in _TERMINAL:
                return operation
            self._guard(operation)
            if operation["state"] != "queued":
                raise ValueError("Intake operation is not queued; reconcile its durable state first")
            self._available(operation["reservations"], identity)
            operation["state"] = "verifying" if operation["kind"] == "upload" else "running"
            self._save(operation)
        try:
            if operation["kind"] == "upload":
                self._complete_upload(operation)
            elif operation["kind"] == "folder":
                with self.lock:
                    operation["state"] = "mutating"
                    self._save(operation)
                    self.source.assert_identity()
                    self._client(operation["path"], True).create_directory(etag="*", match_condition=MatchConditions.IfMissing, retry_total=0)
                    operation["state"] = "remote_done"
                    self._save(operation)
                    operation["state"] = "completed"
                    self._save(operation)
            else:
                self._complete_move(operation)
        except _OPERATION_ERRORS as error:
            with self.lock:
                current = self.record(identity)
                if current["state"] == "cancelled":
                    return current
                current.update(state="recovery", message=f"Intake incomplete; paths remain reserved: {error}")
                return self._save(current)
        return self.record(identity)

    def _complete_upload(self, operation: dict[str, Any]) -> None:
        checksum, before = self._hash(operation, operation["stage"])
        with self.lock:
            self._guard(operation)
            if self.record(operation["id"])["state"] != "verifying":
                raise ValueError("Upload cancelled before finalization")
            operation.update(sha256=checksum, stage_verified=before, state="mutating")
            self._save(operation)
            try:
                self._rename({"path": operation["stage"], "target": operation["path"], "directory": False,
                              "source_etag": before["etag"], "destination_etag": operation["destination_etag"]})
            except (ResourceModifiedError, ResourceExistsError, ResourceNotFoundError):
                operation["remote_rejected"] = True
                self._save(operation)
                raise
            operation.update(state="remote_done")
            self._save(operation)
            operation["state"] = "verifying"
            self._save(operation)
        committed_hash, verified = self._hash(operation, operation["path"])
        if committed_hash != checksum:
            raise ValueError("Finalized upload checksum changed")
        with self.lock:
            if self.record(operation["id"])["state"] != "verifying":
                raise ValueError("Upload cancelled after finalization")
            operation.update(verified=verified, state="remote_done", finalized=True)
            self._save(operation)
            self._commit_item(operation, {"path": operation["path"], "target": operation["path"],
                                         "destination_etag": operation["destination_etag"], "videos": []})

    def _complete_move(self, operation: dict[str, Any]) -> None:
        for index, item in enumerate(operation["results"]):
            with self.lock:
                if self.record(operation["id"])["state"] == "cancelled":
                    return
                self._guard(operation)
                if item["directory"]:
                    current = self._properties(item["path"])
                    if (current is None or not current.is_directory
                            or sorted(self._subtree(item["path"], current)) != sorted(item["videos"])):
                        raise ValueError("Move subtree changed after preflight")
                for video, before in item["source_observations"].items():
                    if self.source.properties(video) != before:
                        raise ValueError("Move subtree changed after preflight")
                item["state"] = "mutating"
                self._save(operation)
                try:
                    self._rename(item)
                except (ResourceModifiedError, ResourceExistsError, ResourceNotFoundError):
                    item["state"] = "rejected"
                    self._save(operation)
                    raise
                if self._properties(item["path"]) is not None:
                    raise ValueError("Native move left the source present; remote completion is uncertain")
                target = self._properties(item["target"])
                if target is None or target.is_directory != item["directory"]:
                    raise ValueError("Native move destination is missing or has a different resource type")
                if target.etag != item["renamed_etag"]:
                    raise ValueError("Moved destination changed after native rename")
                item["target_etag"] = target.etag
                item["observations"] = {
                    _rewrite(video, item["path"], item["target"]): self.source.properties(_rewrite(video, item["path"], item["target"]))
                    for video in item["videos"]
                }
                if item["directory"]:
                    for old, before in item["source_observations"].items():
                        if item["observations"][_rewrite(old, item["path"], item["target"])] != before:
                            raise ValueError("Moved subtree versions changed; explicit footage re-review is required")
                item["state"] = "remote_done"
                operation["current_item"] = index
                self._save(operation)
                self._commit_item(operation, item)

    def _commit_item(self, operation: dict[str, Any], item: dict[str, Any]) -> None:
        self._guard(operation)
        old, new = item["path"], item["target"]
        if operation["kind"] == "move":
            target = self._properties(new)
            if target is None or target.etag != item["target_etag"]:
                raise ValueError("Moved destination changed before metadata commit")
        observations = dict(item.get("observations", {}))
        for path, before in observations.items():
            if self.source.properties(path) != before:
                raise ValueError("Moved destination changed before metadata commit")
        if item.get("destination_etag"):
            observations[new] = self.source.properties(new)
        if operation["kind"] == "upload" and self.source.properties(new) != operation["verified"]:
            raise ValueError("Finalized upload changed before metadata commit")
        with self.store.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            self._metadata(db, old, new, observations, bool(item.get("destination_etag")),
                           item.get("source_observations", {}))
            item["state"] = "completed"
            if operation["kind"] != "move" or all(row["state"] == "completed" for row in operation["results"]):
                operation["state"] = "completed"
                operation["message"] = "Completed"
            self._write(db, "intake", operation["id"], operation)

    @staticmethod
    def _write(db: Any, kind: str, identity: str, data: dict[str, Any]) -> None:
        db.execute("INSERT INTO entries(kind,id,data) VALUES (?,?,?) ON CONFLICT(kind,id) DO UPDATE SET data=excluded.data",
                   (kind, identity, json.dumps(data, allow_nan=False)))

    def _metadata(self, db: Any, old: str, new: str, observations: dict[str, Any], replacement: bool,
                  source_observations: dict[str, Any]) -> None:
        assignment, logical = self.source.assignment_prefix, self.source.logical_prefix
        if replacement:
            db.execute("DELETE FROM entries WHERE kind='exception' AND id=?", (logical + new,))
            for row in db.execute("SELECT id,data FROM entries WHERE kind='source_preview'").fetchall():
                if json.loads(row["data"]).get("video") == logical + new:
                    db.execute("DELETE FROM entries WHERE kind='source_preview' AND id=?", (row["id"],))
            self._write(db, "source_review", logical + new, {
                "video": logical + new, "observations": observations[new], "required": True,
                "message": "Recording replaced; review calibration and capture-time settings before generating.",
            })
        for row in db.execute("SELECT kind,id,data FROM entries WHERE kind IN ('footage','exception','camera','source_preview','source_review','draft')").fetchall():
            kind, identity = row["kind"], row["id"]
            data = json.loads(row["data"])
            new_id = identity
            if kind == "footage" and identity.startswith(assignment):
                new_id = assignment + _rewrite(identity[len(assignment):], old, new)
                if new_id == identity:
                    continue
            elif kind in {"exception", "source_review"} and identity.startswith(logical):
                new_id = logical + _rewrite(identity[len(logical):], old, new)
                for key in ("video", "video_relative_path"):
                    if isinstance(data.get(key), str):
                        data[key] = _rewrite(data[key], logical + old, logical + new)
            elif kind == "camera" and data.get("video", "").startswith(assignment):
                data["video"] = assignment + _rewrite(data["video"][len(assignment):], old, new)
                data["folder"] = _rewrite(data.get("folder", ""), old, new)
                catalog = data.get("catalog", {})
                for key in ("video_relative_path", "video_prefix", "source_path_prefix"):
                    if isinstance(catalog.get(key), str):
                        catalog[key] = _rewrite(catalog[key], logical + old, logical + new)
            elif kind == "source_preview" and data.get("video", "").startswith(logical):
                old_path = data["video"][len(logical):]
                data["video"] = _rewrite(data["video"], logical + old, logical + new)
                path = data["video"][len(logical):]
                if path in observations:
                    if data.get("observations") != source_observations.get(old_path):
                        db.execute("DELETE FROM entries WHERE kind='source_preview' AND id=?", (identity,))
                        continue
                    data["observations"] = observations[path]
            elif kind == "draft" and data.get("source") == self.source.descriptor:
                videos = data.get("videos", [])
                data["videos"] = list(dict.fromkeys(_rewrite(video, old, new) for video in videos))
                data["folder"] = _rewrite(data.get("folder", ""), old, new)
                data["observations"] = {_rewrite(path, old, new): value for path, value in data.get("observations", {}).items()}
                for path in data["videos"]:
                    if path in observations:
                        data["observations"][path] = observations[path]
                if replacement and new in data["videos"]:
                    data["review_required"] = True
            else:
                continue
            if new_id != identity:
                db.execute("DELETE FROM entries WHERE kind=? AND id=?", (kind, identity))
            self._write(db, kind, new_id, data)

    def _owned_stage(self, path: str, identity: str | None = None) -> bool:
        properties = self._properties(path)
        if properties is None:
            return False
        if properties.is_directory:
            return False
        metadata = getattr(properties, "metadata", {}) or {}
        owner = metadata.get("pc_intake")
        if not isinstance(owner, str):
            return False
        if identity is not None and owner != identity:
            return False
        if metadata.get("pc_source") != self.source.identity:
            return False
        try:
            return self.record(owner).get("stage") == path
        except ValueError:
            return False

    def cancel(self, identity: str) -> dict[str, Any]:
        with self.lock:
            operation = self.record(identity)
            self._digests.pop(identity, None)
            if operation["state"] in _TERMINAL:
                return operation
            if operation["kind"] == "move":
                if not all(item["state"] in _SAFE_MOVE_STATES for item in operation["results"]):
                    raise ValueError("Remote finalization may have started; operation requires reconciliation")
                operation.update(state="cancelled", message="Unattempted or rejected moves cancelled; completed items retained")
                return self._save(operation)
            if operation["kind"] != "upload":
                raise ValueError("Remote finalization may have started; operation requires reconciliation")
            if operation.get("stage_verified") is not None and not operation.get("remote_rejected"):
                operation.update(state="recovery", message="Cancellation requested after finalization began; destination may exist. Paths remain reserved.")
                return self._save(operation)
            operation["state"] = "cancelling"
            self._save(operation)
            try:
                properties = self._properties(operation["stage"])
                if properties is not None:
                    if not self._owned_stage(operation["stage"], identity):
                        raise ValueError("Staging ownership changed; refusing deletion")
                    self.source.assert_identity()
                    self._client(operation["stage"]).delete_file(etag=properties.etag, match_condition=MatchConditions.IfNotModified, retry_total=0)
                operation.update(state="cancelled", message="Upload cancelled; owned staging removed")
            except _OPERATION_ERRORS as error:
                operation.update(state="recovery", message=str(error))
            return self._save(operation)

    def reconcile(self, identity: str) -> dict[str, Any]:
        with self.lock:
            operation = self.record(identity)
            self._guard(operation)
            if operation["state"] != "recovery":
                raise ValueError("Select an operation requiring recovery")
            if operation["kind"] == "upload" and operation.get("stage_verified") and not operation.get("finalized"):
                stage = self._properties(operation["stage"])
                target = self._properties(operation["path"])
                if (stage is not None or target is None
                        or target.metadata.get("pc_intake") != identity
                        or target.metadata.get("pc_source") != self.source.identity):
                    raise ValueError("Upload outcome is unresolved; staging/destination ownership must be verified first")
                operation["state"] = "verifying"
                self._save(operation)
        self.recover({identity})
        current = self.record(identity)
        if current["kind"] == "move" and current["state"] == "recovery":
            return self.cancel(identity)
        return current

    def recover(self, identities: set[str] | None = None) -> list[dict[str, Any]]:
        recovered = []
        for saved in self.store.entries("intake"):
            if identities is not None and saved["id"] not in identities:
                continue
            if saved.get("source") != self.source.descriptor:
                continue
            with self.lock:
                operation = self.record(saved["id"])
                if operation["state"] in _TERMINAL:
                    continue
            try:
                self._guard(operation)
                if (operation["kind"] == "upload" and operation.get("stage_verified")
                        and not operation.get("finalized") and operation["state"] in {"verifying", "remote_done"}):
                    self._recover_upload(operation)
                with self.lock:
                    if operation["kind"] == "upload" and (not operation.get("stage_verified") or operation.get("remote_rejected")):
                        recovered.append(self.cancel(operation["id"]))
                        continue
                    if operation["kind"] == "move":
                        for item in operation["results"]:
                            if item["state"] == "remote_done":
                                self._commit_item(operation, item)
                    elif operation["kind"] == "upload" and operation.get("finalized"):
                        self._commit_item(operation, {"path": operation["path"], "target": operation["path"],
                                                    "destination_etag": operation["destination_etag"], "videos": []})
                    elif operation["kind"] == "folder" and operation["state"] == "remote_done":
                        operation["state"] = "completed"
                        self._save(operation)
                    if operation["state"] != "completed":
                        operation.update(state="recovery", message="Interrupted remote intent; verify storage before reconciliation. Paths remain reserved.")
                        self._save(operation)
            except _OPERATION_ERRORS as error:
                with self.lock:
                    operation = self.record(operation["id"])
                    operation.update(state="recovery", message=f"Recovery incomplete: {error}")
                    self._save(operation)
            recovered.append(self.record(operation["id"]))
        return recovered

    def _recover_upload(self, operation: dict[str, Any]) -> None:
        with self.lock:
            if self.record(operation["id"])["state"] not in {"verifying", "remote_done"}:
                raise ValueError("Upload recovery verification was cancelled")
            if self._properties(operation["stage"]) is not None:
                raise ValueError("Upload rename is incomplete; staging still exists")
            operation["state"] = "verifying"
            self._save(operation)
        checksum, verified = self._hash(operation, operation["path"])
        if checksum != operation["sha256"]:
            raise ValueError("Interrupted upload checksum changed")
        with self.lock:
            if self.record(operation["id"])["state"] != "verifying":
                raise ValueError("Upload recovery verification cancelled")
            operation.update(verified=verified, finalized=True, state="remote_done")
            self._save(operation)
