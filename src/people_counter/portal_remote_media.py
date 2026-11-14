"""ETag-pinned, disk-free remote media access.

Preview operations have a 64 MiB HTTP-attempt budget, 16 MiB range cache and
30-second deadline (including native decoding). Exhaustion raises
PreviewLimitError; invalid media/size raises RemoteMediaError; SDK errors retain
their types. Complete reads have no preview budget and verify the stored size
on every response. Optional identity guards run before HTTP attempts, including
SDK body retries, but not cache reads. Clients must use the standard
DataLakeFileClient pipeline.
"""

from __future__ import annotations

import io
import math
import multiprocessing
import re
import time
from collections import OrderedDict
from collections.abc import Callable, Iterator
from fractions import Fraction
from typing import Any

import av
import cv2
from av.video.stream import VideoStream
from azure.core import MatchConditions
from azure.core.exceptions import AzureError, HttpResponseError
from azure.storage.filedatalake import DataLakeFileClient

from .manifest import VideoMetadata

PREVIEW_BYTES = 64 * 1024 * 1024
CACHE_BYTES = 16 * 1024 * 1024
RANGE_BYTES = 1024 * 1024
PREVIEW_SECONDS = 30.0


class PreviewLimitError(ValueError):
    """A preview exhausted its transfer budget or wall-clock deadline."""


class RemoteMediaError(ValueError):
    """The reviewed source size, range response, or media is invalid."""


def _headers(message: Any) -> dict[str, str]:
    return {key.lower(): value for key, value in message.headers.items()}


class _Ranges:
    def __init__(
        self, client: DataLakeFileClient, etag: str, size: int,
        deadline: float | None = None, *,
        check_identity: Callable[[], None] | None = None,
    ) -> None:
        if not etag or etag == "*" or size < 0:
            raise ValueError("A concrete ETag and nonnegative source size are required")
        self.client, self.etag, self.size = client, etag, size
        self.deadline = deadline
        self.check_identity = check_identity
        self.request_bytes = 0

    def remaining(self) -> float:
        if self.deadline is None:
            return PREVIEW_SECONDS
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise PreviewLimitError("Remote preview deadline exceeded")
        return remaining

    def _request(self, request: Any) -> None:
        self.remaining()
        if self.check_identity is not None:
            self.check_identity()
        self.remaining()
        headers = _headers(request.http_request)
        match = re.fullmatch(r"bytes=(\d+)-(\d+)", headers.get("range", headers.get("x-ms-range", "")))
        if match is None:
            raise RemoteMediaError("SDK did not issue a bounded byte range")
        start, end = map(int, match.groups())
        length = end - start + 1
        if start < 0 or end >= self.size or length <= 0:
            raise RemoteMediaError("SDK issued an invalid byte range")
        # Reserve the entire attempt before transport, including failed/retried
        # bodies; SDK progress hooks exclude failed initial-response transfers.
        if self.deadline is not None and self.request_bytes + length > PREVIEW_BYTES:
            raise PreviewLimitError("Remote preview byte budget exceeded")
        self.request_bytes += length

    def _response(self, response: Any) -> None:
        self.remaining()
        http = response.http_response
        headers = _headers(http)
        if http.status_code != 206:
            if http.status_code == 401:
                # Fail before the credential policy can replay outside the
                # request hook. Authentication must be ready before decoding.
                raise HttpResponseError("Remote media authentication failed", response=http)
            if http.status_code < 300:
                raise RemoteMediaError("Remote service ignored the byte range")
            return  # The SDK raises the service's precise error.
        match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", headers.get("content-range", ""))
        if match is None:
            raise RemoteMediaError("Missing or invalid Content-Range")
        start, end, size = map(int, match.groups())
        if size != self.size or not 0 <= start <= end < size:
            raise RemoteMediaError("Remote source size changed or was truncated")
        if headers.get("etag") != self.etag:
            raise RemoteMediaError("Remote source ETag changed")
        if int(headers.get("content-length", "-1")) != end - start + 1:
            raise RemoteMediaError("Remote range length is inconsistent")
        requested = _headers(response.http_request)
        if requested.get("range", requested.get("x-ms-range")) != f"bytes={start}-{end}":
            raise RemoteMediaError("Remote service returned a different byte range")

    def fetch(self, offset: int, length: int) -> bytes:
        remaining = self.remaining()
        before = self.request_bytes
        downloader = self.client.download_file(
            offset=offset, length=length, etag=self.etag,
            match_condition=MatchConditions.IfNotModified,
            decompress=False, max_concurrency=1,
            retry_total=0, retry_connect=0, retry_read=0, retry_status=0,
            redirect_max=0,
            connection_timeout=remaining, read_timeout=remaining,
            raw_request_hook=self._request, raw_response_hook=self._response,
        )
        data = downloader.readall()
        self.remaining()
        if self.request_bytes == before:
            raise RemoteMediaError("SDK pipeline omitted byte-budget hooks")
        if len(data) != length:
            raise RemoteMediaError("Remote source was truncated or range length changed")
        return data


class RemoteRangeReader(io.RawIOBase):
    """Seekable SDK source; reads may return at most one 1 MiB cache block."""

    def __init__(
        self, client: DataLakeFileClient, etag: str, size: int,
        *, deadline: float | None = None,
        check_identity: Callable[[], None] | None = None,
    ) -> None:
        super().__init__()
        if size <= 0:
            raise RemoteMediaError("Remote video is empty")
        self.ranges = _Ranges(
            client, etag, size,
            time.monotonic() + PREVIEW_SECONDS if deadline is None else deadline,
            check_identity=check_identity,
        )
        self.position = 0
        self.cache: OrderedDict[int, bytes] = OrderedDict()
        self.cache_bytes = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        self._checkClosed()
        return self.position

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self._checkClosed()
        self.ranges.remaining()
        origins = {io.SEEK_SET: 0, io.SEEK_CUR: self.position, io.SEEK_END: self.ranges.size}
        if whence not in origins or origins[whence] + offset < 0:
            raise ValueError("Invalid remote seek")
        self.position = origins[whence] + offset
        return self.position

    def read(self, size: int = -1) -> bytes:
        self._checkClosed()
        self.ranges.remaining()
        if size == 0 or self.position >= self.ranges.size:
            return b""
        start = self.position // RANGE_BYTES * RANGE_BYTES
        block = self.cache.pop(start, None)
        if block is None:
            length = min(RANGE_BYTES, self.ranges.size - start)
            while self.cache_bytes + length > CACHE_BYTES:
                _, old = self.cache.popitem(last=False)
                self.cache_bytes -= len(old)
            block = self.ranges.fetch(start, length)
            self.cache_bytes += len(block)
        self.cache[start] = block
        offset = self.position - start
        count = len(block) - offset if size < 0 else min(size, len(block) - offset)
        self.position += count
        return block[offset:offset + count]

    def readinto(self, buffer: Any) -> int:
        data = self.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)

    def close(self) -> None:
        self.cache.clear()
        self.cache_bytes = 0
        super().close()


def complete_chunks(
    client: DataLakeFileClient, etag: str, size: int, chunk_size: int = 8 * 1024 * 1024,
    *, check_identity: Callable[[], None] | None = None,
) -> Iterator[bytes]:
    """Yield every original stored byte, pinned and exact; no preview limits."""
    if chunk_size <= 0:
        raise ValueError("Chunk size must be positive")
    ranges = _Ranges(client, etag, size, check_identity=check_identity)
    if size == 0:
        options: dict[str, Any] = {}
        if check_identity is not None:
            check_identity()
            options = {
                "raw_request_hook": lambda request: check_identity(),
                "retry_total": 0,
                "redirect_max": 0,
            }
        properties = client.get_file_properties(
            etag=etag, match_condition=MatchConditions.IfNotModified,
            **options,
        )
        if properties.size != 0 or properties.etag != etag:
            raise RemoteMediaError("Remote source size or ETag changed")
    for offset in range(0, size, chunk_size):
        yield ranges.fetch(offset, min(chunk_size, size - offset))


def _time_base(stream: VideoStream) -> Fraction:
    time_base = stream.time_base
    if time_base is None or time_base <= 0:
        raise RemoteMediaError("Primary video stream has a missing or invalid time base")
    return time_base


def _metadata(container: Any, stream: Any) -> VideoMetadata:
    time_base = _time_base(stream)
    duration = (
        float(stream.duration * time_base)
        if stream.duration is not None else 0.0
    )
    if not math.isfinite(duration) or duration <= 0:
        duration = float(container.duration / av.time_base) if container.duration is not None else 0.0
    width, height = stream.codec_context.width, stream.codec_context.height
    if width <= 0 or height <= 0 or not math.isfinite(duration) or duration <= 0:
        raise RemoteMediaError("Primary video stream has invalid dimensions or duration")
    capture_times = tuple(
        tags["creation_time"].strip()
        for tags in (stream.metadata, container.metadata)
        if tags.get("creation_time", "").strip()
    )
    return VideoMetadata(width, height, duration, capture_times)


def _decode(
    client: DataLakeFileClient, etag: str, size: int, seconds: float | None,
    deadline: float, check_identity: Callable[[], None] | None = None,
) -> Any:
    with RemoteRangeReader(client, etag, size, deadline=deadline, check_identity=check_identity) as source:
        with av.open(source, mode="r") as container:
            if not container.streams.video:
                raise RemoteMediaError("Remote media has no primary video stream")
            stream = container.streams.video[0]
            time_base = _time_base(stream)
            metadata = _metadata(container, stream)
            if seconds is None:
                return metadata
            if seconds >= metadata.duration_seconds:
                raise RemoteMediaError("No frame at the requested timestamp")
            start = stream.start_time or 0
            container.seek(start + int(seconds / time_base), stream=stream, backward=True)
            for frame in container.decode(stream):
                source.ranges.remaining()
                if frame.pts is None:
                    raise RemoteMediaError("Decoded frame has no presentation timestamp")
                if float((frame.pts - start) * time_base) + 1e-9 < seconds:
                    continue
                image = frame.to_ndarray(format="bgr24")
                ok, jpeg = cv2.imencode(".jpg", image)
                if not ok:
                    raise RemoteMediaError("JPEG encoding failed")
                return jpeg.tobytes(), frame.width, frame.height
            raise RemoteMediaError("No frame at the requested timestamp")


def _worker(
    connection: Any, client: DataLakeFileClient, etag: str, size: int,
    seconds: float | None, deadline: float, check_identity: Callable[[], None] | None,
) -> None:
    with connection:
        try:
            result = _decode(client, etag, size, seconds, deadline, check_identity)
        except av.FFmpegError as error:
            connection.send((False, RemoteMediaError(str(error))))
        except (AzureError, OSError, ValueError) as error:
            connection.send((False, error))
        else:
            connection.send((True, result))


def _run(
    client: DataLakeFileClient, etag: str, size: int, seconds: float | None,
    check_identity: Callable[[], None] | None = None,
) -> Any:
    deadline = time.monotonic() + PREVIEW_SECONDS
    if "fork" not in multiprocessing.get_all_start_methods():
        raise RemoteMediaError("Bounded remote decoding requires a fork-capable host")
    context = multiprocessing.get_context("fork")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(
        target=_worker, args=(child, client, etag, size, seconds, deadline, check_identity),
    )
    process.start()
    child.close()
    try:
        if not parent.poll(max(0.0, deadline - time.monotonic())):
            raise PreviewLimitError("Remote preview deadline exceeded")
        try:
            ok, result = parent.recv()
        except EOFError as error:
            raise RemoteMediaError("Remote decoder exited without a result") from error
        if time.monotonic() >= deadline:
            raise PreviewLimitError("Remote preview deadline exceeded")
        if not ok:
            raise result
        return result
    finally:
        parent.close()
        if process.is_alive():
            process.kill()
        process.join()
        process.close()


def remote_probe(
    client: DataLakeFileClient, etag: str, size: int,
    *, check_identity: Callable[[], None] | None = None,
) -> VideoMetadata:
    """Probe the primary video stream; never silently fall back to full download."""
    return _run(client, etag, size, None, check_identity)


def remote_frame(
    client: DataLakeFileClient, etag: str, size: int, seconds: float,
    *, check_identity: Callable[[], None] | None = None,
) -> tuple[bytes, int, int]:
    """JPEG of the first primary-stream frame at/after the requested seconds."""
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError("Frame timestamp must be finite and nonnegative")
    return _run(client, etag, size, seconds, check_identity)
