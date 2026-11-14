"""Azure Data Lake Storage operations for immutable manifest publication."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, Protocol, TypedDict

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential
    from azure.core.pipeline.policies import SansIOHTTPPolicy

from people_counter.manifest import ManifestPublisherError


class PublicationConflictError(ManifestPublisherError):
    """Raised when an existing remote object conflicts with intended output."""

    reason_code = "PUBLICATION_CONFLICT"
    suggested_action = (
        "Inspect the existing staging/incoming object and checkpoint. Do not "
        "overwrite incoming data; resolve the ownership or identity conflict "
        "before retrying."
    )
    retryable = True


class IdentityPolicyOptions(TypedDict, total=False):
    _additional_pipeline_policies: list[SansIOHTTPPolicy]


@dataclass(frozen=True)
class RemoteObject:
    size: int
    etag: str


class PublicationStorage(Protocol):
    def stat(self, path: str) -> RemoteObject | None: ...

    def read_bytes(self, path: str) -> bytes: ...
    def sha256(self, path: str) -> str: ...

    def upload_file(
        self,
        local_path: Path,
        remote_path: str,
        content_type: str,
        expected_size: int,
        expected_sha256: str,
    ) -> None: ...

    def upload_bytes(
        self,
        content: bytes,
        remote_path: str,
        content_type: str,
    ) -> None: ...

    def rename(self, source_path: str, destination_path: str) -> None: ...


class StreamingPublicationStorage(PublicationStorage, Protocol):
    def upload_stream(
        self,
        chunks: Callable[[], Iterator[bytes]],
        remote_path: str,
        content_type: str,
        expected_size: int,
        expected_sha256: str,
    ) -> None: ...


class AzureDataLakeStorage:
    """Synchronous ADLS Gen2 client with chunk-level upload checkpoints."""

    def __init__(
        self,
        storage_account: str,
        filesystem: str,
        *,
        chunk_size: int,
        credential: TokenCredential | None = None,
        identity_policy: SansIOHTTPPolicy | None = None,
    ) -> None:
        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError as error:
            raise RuntimeError(
                "ADLS publication dependencies are missing. Install with "
                "`uv sync --extra publisher`."
            ) from error

        credential = credential if credential is not None else DefaultAzureCredential()
        policy_options: IdentityPolicyOptions = (
            {"_additional_pipeline_policies": [identity_policy]} if identity_policy is not None else {}
        )
        service = DataLakeServiceClient(
            account_url=(
                f"https://{storage_account}.dfs.core.windows.net"
            ),
            credential=credential,
            **policy_options,
        )
        self._filesystem = filesystem
        self._client = service.get_file_system_client(filesystem)
        self._chunk_size = chunk_size
        self._known_directories: set[str] = set()

    def _file_client(self, path: str) -> Any:
        return self._client.get_file_client(path)

    def _ensure_parent(self, path: str) -> None:
        from azure.core.exceptions import ResourceExistsError

        parent = PurePosixPath(path).parent
        current_parts: list[str] = []
        for part in parent.parts:
            current_parts.append(part)
            current = "/".join(current_parts)
            if current in self._known_directories:
                continue
            directory = self._client.get_directory_client(current)
            if not directory.exists():
                try:
                    directory.create_directory()
                except ResourceExistsError:
                    pass
            self._known_directories.add(current)

    def stat(self, path: str) -> RemoteObject | None:
        client = self._file_client(path)
        if not client.exists():
            return None
        properties = client.get_file_properties()
        return RemoteObject(
            size=int(properties.size),
            etag=str(properties.etag),
        )

    def read_bytes(self, path: str) -> bytes:
        client = self._file_client(path)
        if not client.exists():
            raise PublicationConflictError(f"Remote object does not exist: {path}")
        return bytes(client.download_file().readall())

    def sha256(self, path: str) -> str:
        client = self._file_client(path)
        if not client.exists():
            raise PublicationConflictError(
                f"Remote object does not exist: {path}"
            )
        digest = hashlib.sha256()
        for chunk in client.download_file().chunks():
            digest.update(chunk)
        return digest.hexdigest()

    def upload_file(
        self,
        local_path: Path,
        remote_path: str,
        content_type: str,
        expected_size: int,
        expected_sha256: str,
    ) -> None:
        if local_path.stat().st_size != expected_size:
            raise PublicationConflictError(
                f"Source video size changed after inspection: {local_path}"
            )

        def chunks() -> Iterator[bytes]:
            with local_path.open("rb") as source:
                while chunk := source.read(self._chunk_size):
                    yield chunk

        self.upload_stream(
            chunks, remote_path, content_type, expected_size, expected_sha256,
        )

    def upload_stream(
        self,
        chunks: Callable[[], Iterator[bytes]],
        remote_path: str,
        content_type: str,
        expected_size: int,
        expected_sha256: str,
    ) -> None:
        from azure.storage.filedatalake import ContentSettings

        if self._chunk_size <= 0:
            raise ValueError("Upload chunk size must be positive")
        existing = self.stat(remote_path)
        offset = existing.size if existing is not None else 0
        if offset > expected_size:
            raise PublicationConflictError(
                f"Staged object is larger than the source: {remote_path}"
            )
        client = self._file_client(remote_path)
        settings = ContentSettings(content_type=content_type)
        if offset:
            prefix = hashlib.sha256()
            remaining = offset
            for chunk in chunks():
                prefix.update(memoryview(chunk)[:remaining])
                remaining -= min(len(chunk), remaining)
                if not remaining:
                    break
            if remaining or prefix.hexdigest() != self.sha256(remote_path):
                client.delete_file()
                existing = None
                offset = 0
        self._ensure_parent(remote_path)
        if existing is None:
            client.create_file(content_settings=settings)

        digest = hashlib.sha256()
        source_offset = 0
        for chunk in chunks():
            for start in range(0, len(chunk), self._chunk_size):
                block = memoryview(chunk)[start:start + self._chunk_size]
                digest.update(block)
                end = source_offset + len(block)
                if end > expected_size:
                    raise PublicationConflictError(
                        f"Source video changed during upload: {remote_path}"
                    )
                if end > offset:
                    upload = bytes(block[max(0, offset - source_offset):])
                    client.append_data(upload, offset=offset, length=len(upload))
                    offset += len(upload)
                    client.flush_data(offset, content_settings=settings)
                source_offset = end
        if source_offset != expected_size or digest.hexdigest() != expected_sha256:
            raise PublicationConflictError(
                f"Source video changed during upload: {remote_path}"
            )
        uploaded = self.stat(remote_path)
        if (
            uploaded is None
            or uploaded.size != expected_size
            or self.sha256(remote_path) != expected_sha256
        ):
            raise PublicationConflictError(
                f"Staged object does not match source: {remote_path}"
            )

    def upload_bytes(
        self,
        content: bytes,
        remote_path: str,
        content_type: str,
    ) -> None:
        from azure.storage.filedatalake import ContentSettings

        existing = self.stat(remote_path)
        if existing is not None:
            if self.read_bytes(remote_path) != content:
                raise PublicationConflictError(
                    f"Existing staged manifest conflicts: {remote_path}"
                )
            return
        self._ensure_parent(remote_path)
        client = self._file_client(remote_path)
        client.create_file(
            content_settings=ContentSettings(content_type=content_type)
        )
        client.append_data(content, offset=0, length=len(content))
        client.flush_data(
            len(content),
            content_settings=ContentSettings(content_type=content_type),
        )

    def rename(self, source_path: str, destination_path: str) -> None:
        from azure.core import MatchConditions
        from azure.core.exceptions import (
            ResourceExistsError,
            ResourceModifiedError,
        )

        if self.stat(destination_path) is not None:
            raise PublicationConflictError(
                f"Destination already exists: {destination_path}"
            )
        if self.stat(source_path) is None:
            raise PublicationConflictError(
                f"Rename source does not exist: {source_path}"
            )
        self._ensure_parent(destination_path)
        try:
            self._file_client(source_path).rename_file(
                f"{self._filesystem}/{destination_path}",
                match_condition=MatchConditions.IfMissing,
            )
        except (ResourceExistsError, ResourceModifiedError) as error:
            raise PublicationConflictError(
                f"Destination already exists: {destination_path}"
            ) from error
