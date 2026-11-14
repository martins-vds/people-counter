"""Read-only ADLS footage and frozen package input selection."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable, Iterator, Mapping
from datetime import timezone
from pathlib import Path, PurePosixPath
from typing import Any

from people_counter.manifest import CONTENT_TYPES, VideoInspection, VideoMetadata, inspect_video, normalize_relative_path
from people_counter.manifest_publisher import FILESYSTEM_NAME, STORAGE_ACCOUNT_NAME


def source_identity(descriptor: Mapping[str, Any]) -> str:
    values = (descriptor["account"], descriptor["filesystem"], descriptor.get("root", ""))
    return "adls-" + hashlib.sha256("\n".join(values).encode()).hexdigest()[:24]


class AdlsFootageSource:
    def __init__(self, descriptor: Mapping[str, Any], azure: Any, preview_store: Any = None) -> None:
        from azure.storage.filedatalake import DataLakeServiceClient

        self.descriptor = dict(descriptor)
        self.account = str(descriptor.get("account", ""))
        self.filesystem = str(descriptor.get("filesystem", ""))
        if not STORAGE_ACCOUNT_NAME.fullmatch(self.account) or not FILESYSTEM_NAME.fullmatch(self.filesystem):
            raise ValueError("Select a valid ADLS account and filesystem")
        root = str(descriptor.get("root", ""))
        self.remote_root = normalize_relative_path(root, directory=True).rstrip("/") if root else ""
        self.identity = source_identity({**descriptor, "root": self.remote_root})
        # Filesystem names cannot contain NUL, so legacy local keys stay disjoint.
        self.assignment_prefix = "\0" + self.identity + ":"
        self.logical_prefix = self.identity + "/"
        self.label = f"ADLS / {self.account} / {self.filesystem}"
        self.azure = azure
        self.preview_store = preview_store
        self.execution_identity = azure.require_identity()
        credential = azure.credential(self.execution_identity, subscription=descriptor.get("subscription"))
        self.client = DataLakeServiceClient(
            f"https://{self.account}.dfs.core.windows.net", credential=credential,
            max_single_get_size=8 * 1024 * 1024, max_chunk_get_size=8 * 1024 * 1024,
            connection_timeout=10, read_timeout=10,
            _additional_pipeline_policies=[azure.policy(self.execution_identity)],
        ).get_file_system_client(self.filesystem)

    def _call(self, function: Any, *args: Any, **kwargs: Any) -> Any:
        from azure.core.exceptions import AzureError

        try:
            self.assert_identity()
            return function(*args, **kwargs)
        except AzureError as error:
            logging.getLogger(__name__).warning("ADLS source operation failed: %s", type(error).__name__)
            raise ValueError(
                f"ADLS access failed ({type(error).__name__}). Check Azure CLI sign-in, "
                "Storage Blob Data Reader/ACL permissions, and storage network access."
            ) from error

    def assert_identity(self) -> None:
        self.azure.check_identity(self.execution_identity)

    def canonical(self, path: str, *, directory: bool = False) -> str:
        if not path and directory:
            return ""
        return normalize_relative_path(path, directory=directory).rstrip("/")

    def remote_path(self, path: str) -> str:
        path = self.canonical(path, directory=not bool(path))
        return "/".join(part for part in (self.remote_root, path) if part)

    def relative_video(self, video: str) -> str:
        return self.logical_prefix + self.canonical(video)

    def prefix(self, folder: str) -> str:
        folder = self.canonical(folder, directory=True)
        return self.logical_prefix + (folder + "/" if folder else "")

    def file_client(self, video: str) -> Any:
        if PurePosixPath(video).suffix.lower() not in CONTENT_TYPES:
            raise ValueError("Select a supported video file")
        return self.client.get_file_client(self.remote_path(video))

    def properties(self, video: str) -> dict[str, Any]:
        value = self._call(self.file_client(video).get_file_properties)
        return {"size": int(value.size), "etag": str(value.etag)}

    def frame(self, video: str, seconds: float = 0) -> tuple[bytes, int, int]:
        from people_counter.portal_remote_media import remote_frame

        properties = self.properties(video)
        cached = self._preview(video, properties)
        if cached is not None and cached["seconds"] == seconds:
            path = self.preview_store.path.parent / "previews" / (cached["id"] + ".jpg")
            return path.read_bytes(), cached["metadata"]["width"], cached["metadata"]["height"]
        return remote_frame(
            self.file_client(video), properties["etag"], properties["size"], seconds,
            check_identity=self.assert_identity,
        )

    def probe(self, video: str) -> Any:
        from people_counter.portal_remote_media import remote_probe

        properties = self.properties(video)
        cached = self._preview(video, properties)
        if cached is not None:
            return VideoMetadata(**cached["metadata"])
        return remote_probe(
            self.file_client(video), properties["etag"], properties["size"],
            check_identity=self.assert_identity,
        )

    def _preview(self, video: str, properties: Mapping[str, Any]) -> dict[str, Any] | None:
        if self.preview_store is None:
            return None
        return next((
            row for row in self.preview_store.entries("source_preview")
            if row["video"] == self.relative_video(video) and row["observations"] == properties
        ), None)

    def page(self, folder: str, token: str | None = None) -> tuple[list[dict[str, Any]], str | None]:
        folder = self.canonical(folder, directory=True)
        paths = self.client.get_paths(path=self.remote_path(folder) or None, recursive=False, max_results=100)
        pages = paths.by_page(continuation_token=token)
        values = self._call(lambda: list(next(pages, [])))
        entries = []
        for value in values:
            path = value.name.removeprefix(self.remote_root + "/") if self.remote_root else value.name
            path = self.canonical(path, directory=bool(value.is_directory))
            modified = value.last_modified.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            entries.append({
                "name": PurePosixPath(path).name, "path": path, "directory": value.is_directory,
                "video": not value.is_directory and PurePosixPath(path).suffix.lower() in CONTENT_TYPES,
                "size_bytes": None if value.is_directory else value.content_length,
                "modified_utc": modified, "blocked": False, "error": None,
            })
        continuation = getattr(pages, "continuation_token", None)
        if continuation is not None and not isinstance(continuation, str):
            raise ValueError("ADLS returned an invalid continuation token")
        return sorted(entries, key=lambda row: (not row["directory"], row["name"].casefold())), continuation

    def entries(self, folder: str) -> list[dict[str, Any]]:
        return self.page(folder)[0]

    def folders(self, folder: str) -> list[dict[str, Any]]:
        rows, token = self.page(folder)
        if token:
            raise ValueError("This folder has more entries. Use paged package browsing to navigate its folders.")
        return [{"name": row["name"], "path": row["path"], "children": None} for row in rows if row["directory"]]

    def folder_tree(self, folder: str) -> list[dict[str, Any]]:
        return []

    def videos(self, folder: str, limit: int = 10000) -> list[str]:
        from azure.core.exceptions import AzureError

        paths = self.client.get_paths(path=self.remote_path(folder) or None, recursive=True)
        videos = []
        try:
            for value in paths:
                if not value.is_directory and PurePosixPath(value.name).suffix.lower() in CONTENT_TYPES:
                    path = value.name.removeprefix(self.remote_root + "/") if self.remote_root else value.name
                    videos.append(self.canonical(path))
                    if len(videos) > limit:
                        raise ValueError(f"Selection exceeds the {limit:,}-video package limit. Choose a smaller folder.")
        except AzureError as error:
            logging.getLogger(__name__).warning("ADLS recursive listing failed: %s", type(error).__name__)
            raise ValueError("Cannot enumerate ADLS folder. Check permissions and network access.") from error
        return sorted(videos)


class SelectedFootageInput:
    def __init__(
        self, source: Any, videos: list[str], observations: Mapping[str, Any] | None = None,
        progress: Callable[[str], None] | None = None,
    ) -> None:
        self.source = source
        self.videos = {source.relative_video(video): video for video in videos}
        self.observations = dict(observations or {})
        self.progress = progress

    def paths(self) -> list[str]:
        return sorted(self.videos)

    def stat(self, relative: str) -> Any:
        from people_counter.video_input import InputStat

        video = self.videos[relative]
        if isinstance(self.source, AdlsFootageSource):
            properties = self.source.properties(video)
            saved = self.observations.get(video)
            if saved and properties != saved:
                raise ValueError("ADLS footage changed after selection. Refresh selection and generate a new revision.")
            return InputStat(properties["size"], properties["etag"])
        metadata = self.source.resolve(video).stat()
        saved = self.observations.get(video)
        if saved and saved != {"size": metadata.st_size, "mtime_ns": metadata.st_mtime_ns}:
            raise ValueError("Local footage changed after selection. Refresh selection and generate a new revision.")
        return InputStat(metadata.st_size, str(metadata.st_mtime_ns))

    def local_path(self, relative: str) -> Path | None:
        if isinstance(self.source, AdlsFootageSource):
            return None
        return self.source.resolve(self.videos[relative])

    def chunks(self, relative: str) -> Iterator[bytes]:
        video = self.videos[relative]
        if isinstance(self.source, AdlsFootageSource):
            from people_counter.portal_remote_media import complete_chunks

            metadata = self.stat(relative)
            yield from complete_chunks(
                self.source.file_client(video), metadata.version, metadata.size,
                check_identity=self.source.assert_identity,
            )
        else:
            with self.source.resolve(video).open("rb") as handle:
                while chunk := handle.read(8 * 1024 * 1024):
                    yield chunk

    def inspect(self, relative: str) -> VideoInspection:
        if self.progress is not None:
            self.progress(f"Inspecting and hashing {self.videos[relative]}")
        if not isinstance(self.source, AdlsFootageSource):
            return inspect_video(self.source.resolve(self.videos[relative]))
        before = self.stat(relative)
        digest = hashlib.sha256()
        count = 0
        for chunk in self.chunks(relative):
            digest.update(chunk)
            count += len(chunk)
        metadata = self.source.probe(self.videos[relative])
        after = self.stat(relative)
        if count != before.size or after != before:
            raise ValueError("ADLS footage changed or was truncated while hashing")
        return VideoInspection(count, digest.hexdigest(), metadata.width, metadata.height, metadata.duration_seconds, metadata.embedded_capture_times)
