"""Replayable video inputs with logical paths and explicit source versions."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from people_counter.manifest import (
    CONTENT_TYPES,
    ManifestPublisherError,
    VideoInspection,
    inspect_video,
    normalize_relative_path,
)


@dataclass(frozen=True)
class InputStat:
    size: int
    version: str


class VideoInput(Protocol):
    def paths(self) -> list[str]: ...
    def stat(self, relative: str) -> InputStat: ...
    def inspect(self, relative: str) -> VideoInspection: ...
    def chunks(self, relative: str) -> Iterator[bytes]: ...
    def local_path(self, relative: str) -> Path | None: ...


class LocalVideoInput:
    def __init__(
        self,
        video_root: Path,
        partition_prefix: str | None = None,
        *,
        inspector: Callable[[Path], VideoInspection] = inspect_video,
        max_files: int = 10_000,
    ) -> None:
        if not video_root.is_dir():
            raise ManifestPublisherError(
                f"Video root does not exist or is not a directory: {video_root}"
            )
        search_root = video_root
        if partition_prefix is not None:
            normalized = normalize_relative_path(partition_prefix, directory=True)
            search_root = video_root.joinpath(*normalized.rstrip("/").split("/"))
            if not search_root.is_dir():
                raise ManifestPublisherError(
                    f"Partition prefix '{partition_prefix}' does not exist "
                    f"in video root: {video_root}"
                )
        files = sorted(
            path for path in search_root.rglob("*")
            if path.is_file() and path.suffix.lower() in CONTENT_TYPES
        )
        for path in files:
            if path.is_symlink():
                raise ManifestPublisherError(f"Symbolic links are not supported: {path}")
        if not files:
            raise ManifestPublisherError("No supported video files were discovered")
        if len(files) > max_files:
            raise ManifestPublisherError(
                f"Discovered {len(files)} videos, exceeding --max-files={max_files}; "
                "use a narrower --partition-prefix"
            )
        self._files = {
            path.relative_to(video_root).as_posix(): path
            for path in files
        }
        self._inspector = inspector

    def paths(self) -> list[str]:
        return list(self._files)

    def local_path(self, relative: str) -> Path:
        path = self._files[relative]
        if path.is_symlink() or not path.is_file():
            raise ManifestPublisherError(f"Video is missing or is a symbolic link: {relative}")
        return path

    def stat(self, relative: str) -> InputStat:
        properties = self.local_path(relative).stat()
        return InputStat(properties.st_size, str(properties.st_mtime_ns))

    def inspect(self, relative: str) -> VideoInspection:
        return self._inspector(self.local_path(relative))

    def chunks(self, relative: str) -> Iterator[bytes]:
        with self.local_path(relative).open("rb") as source:
            while chunk := source.read(8 * 1024 * 1024):
                yield chunk
