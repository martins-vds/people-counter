"""Local immutable content-addressed storage for development workflows."""

from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class LocalStorageError(RuntimeError):
    """Base error for local content storage."""


class PathConfinementError(LocalStorageError):
    """A requested path escapes the configured storage root."""


class ContentHashMismatchError(LocalStorageError):
    """Content does not match its expected SHA-256 digest."""


class ImmutableContentConflictError(LocalStorageError):
    """An immutable object path already contains different content."""


@dataclass(frozen=True)
class StoredObject:
    sha256: str
    size_bytes: int
    path: Path


class ContentAddressedStore:
    """Store immutable objects beneath one root using their SHA-256 digest."""

    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()
        self._create_directory_hierarchy(self.root)

    def confined_path(self, relative_path: str | Path) -> Path:
        relative = Path(relative_path)
        if relative.is_absolute():
            raise PathConfinementError("storage paths must be relative")
        candidate = (self.root / relative).resolve(strict=False)
        if candidate != self.root and self.root not in candidate.parents:
            raise PathConfinementError(
                f"storage path escapes root: {relative_path!s}"
            )
        return candidate

    def object_path(self, sha256: str) -> Path:
        digest = self._validated_digest(sha256)
        return self.confined_path(Path("sha256") / digest[:2] / digest)

    def put_bytes(
        self,
        content: bytes,
        *,
        expected_sha256: str | None = None,
    ) -> StoredObject:
        digest = hashlib.sha256(content).hexdigest()
        if expected_sha256 is not None:
            expected = self._validated_digest(expected_sha256)
            if digest != expected:
                raise ContentHashMismatchError(
                    f"expected SHA-256 {expected}, calculated {digest}"
                )
        destination = self.object_path(digest)
        self._create_directory_hierarchy(destination.parent)
        if destination.exists():
            return self._verify_existing(destination, digest, len(content))

        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{digest}.",
            dir=destination.parent,
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                self._atomic_rename_no_replace(temporary, destination)
            except FileExistsError:
                return self._verify_existing(destination, digest, len(content))
            self._fsync_directory(destination.parent)
            return StoredObject(digest, len(content), destination)
        finally:
            temporary.unlink(missing_ok=True)

    def put_json(self, value: Any) -> StoredObject:
        content = json.dumps(
            value,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return self.put_bytes(content)

    def read_bytes(self, sha256: str) -> bytes:
        digest = self._validated_digest(sha256)
        path = self.object_path(digest)
        try:
            content = path.read_bytes()
        except FileNotFoundError as error:
            raise FileNotFoundError(f"content object does not exist: {digest}") from error
        actual = hashlib.sha256(content).hexdigest()
        if actual != digest:
            raise ContentHashMismatchError(
                f"stored object {digest} calculated as {actual}"
            )
        return content

    def read_json(self, sha256: str) -> Any:
        return json.loads(self.read_bytes(sha256))

    @staticmethod
    def _validated_digest(value: str) -> str:
        if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
            raise ValueError("SHA-256 must be 64 lowercase hexadecimal characters")
        return value

    @staticmethod
    def _file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
        return digest.hexdigest()

    def _verify_existing(
        self,
        path: Path,
        expected_sha256: str,
        expected_size: int,
    ) -> StoredObject:
        actual_size = path.stat().st_size
        actual_digest = self._file_sha256(path)
        if actual_size != expected_size or actual_digest != expected_sha256:
            raise ImmutableContentConflictError(
                f"immutable object conflict at {path}: "
                f"expected {expected_sha256}/{expected_size}, "
                f"found {actual_digest}/{actual_size}"
            )
        return StoredObject(expected_sha256, expected_size, path)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _create_directory_hierarchy(self, path: Path) -> None:
        resolved = path.expanduser().resolve()
        current = Path(resolved.anchor)
        for part in resolved.parts[1:]:
            child = current / part
            try:
                child.mkdir()
            except FileExistsError:
                if not child.is_dir():
                    raise
            else:
                self._fsync_directory(current)
            current = child

    @staticmethod
    def _atomic_rename_no_replace(source: Path, destination: Path) -> None:
        if os.name == "posix":
            libc = ctypes.CDLL(None, use_errno=True)
            renameat2 = getattr(libc, "renameat2", None)
            if renameat2 is not None:
                result = renameat2(
                    -100,
                    os.fsencode(source),
                    -100,
                    os.fsencode(destination),
                    1,
                )
                if result == 0:
                    return
                error_number = ctypes.get_errno()
                if error_number == errno.EEXIST:
                    raise FileExistsError(destination)
                if error_number not in {errno.ENOSYS, errno.EINVAL}:
                    raise OSError(error_number, os.strerror(error_number))
        os.link(source, destination)
