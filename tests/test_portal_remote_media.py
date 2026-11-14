from __future__ import annotations

import io
import subprocess
import struct
import time
from contextlib import nullcontext
from fractions import Fraction
from types import SimpleNamespace

import cv2
import numpy as np
import pytest
from azure.core import MatchConditions
from azure.core.exceptions import ResourceModifiedError, ServiceResponseError
from azure.core.pipeline.transport import HttpResponse, HttpTransport
from azure.storage.filedatalake import DataLakeFileClient

from people_counter import portal_remote_media as media
from people_counter.manifest import probe_video


class RangedClient:
    def __init__(self, data, *, etag='"snapshot"', reported_size=None, retries=0):
        self.data = data
        self.etag = etag
        self.reported_size = len(data) if reported_size is None else reported_size
        self.retries = retries
        self.requests = []

    def download_file(self, offset, length, **kwargs):
        assert kwargs["match_condition"] == MatchConditions.IfNotModified
        assert kwargs["decompress"] is False
        assert kwargs["max_concurrency"] == 1
        assert kwargs["retry_total"] == 0
        if kwargs["etag"] != self.etag:
            raise ResourceModifiedError("Source changed")
        headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
        for _ in range(self.retries + 1):
            kwargs["raw_request_hook"](
                SimpleNamespace(http_request=SimpleNamespace(headers=headers))
            )
            self.requests.append((offset, length))
        payload = self.data[offset:offset + length]
        response_headers = {
            "Content-Range": f"bytes {offset}-{offset + len(payload) - 1}/{self.reported_size}",
            "Content-Length": str(len(payload)),
            "ETag": self.etag,
        }
        kwargs["raw_response_hook"](
            SimpleNamespace(
                http_response=SimpleNamespace(headers=response_headers, status_code=206),
                http_request=SimpleNamespace(headers=headers),
            )
        )
        return SimpleNamespace(readall=lambda: payload)


@pytest.fixture(params=[False, True], ids=["moov-end", "faststart"])
def video(request, tmp_path):
    path = tmp_path / "capture.mp4"
    command = [
        "ffmpeg", "-v", "error", "-f", "lavfi", "-i",
        "color=c=red:s=160x96:r=10:d=2", "-f", "lavfi", "-i",
        "color=c=blue:s=160x96:r=10:d=2",
        "-filter_complex", "[0:v][1:v]concat=n=2:v=1:a=0",
        "-c:v", "libx264", "-g", "30", "-pix_fmt", "yuv420p",
        "-metadata", "creation_time=2026-04-03T12:30:00Z",
    ]
    if request.param:
        command += ["-movflags", "+faststart"]
    subprocess.run(command + [str(path)], check=True, capture_output=True)
    return path


@pytest.mark.parametrize("seconds", [1.3, 2.3, 3.0])
def test_metadata_and_later_frame_match_local(video, seconds):
    data = video.read_bytes()
    assert media.remote_probe(RangedClient(data), '"snapshot"', len(data)) == probe_video(video)
    jpeg, width, height = media.remote_frame(RangedClient(data), '"snapshot"', len(data), seconds)
    image = cv2.imdecode(np.frombuffer(jpeg, np.uint8), cv2.IMREAD_COLOR)
    local = cv2.VideoCapture(str(video))
    try:
        local.set(cv2.CAP_PROP_POS_MSEC, seconds * 1000)
        ok, expected = local.read()
    finally:
        local.release()
    assert ok
    assert (width, height) == (160, 96)
    assert np.max(np.abs(image.astype(float).mean((0, 1)) - expected.mean((0, 1)))) < 5
    assert image[:, :, 0 if seconds >= 2 else 2].mean() > 200


def test_reader_seek_cache_and_memory():
    data = bytes(range(256)) * (20 * 1024 * 1024 // 256)
    client = RangedClient(data)
    with media.RemoteRangeReader(client, '"snapshot"', len(data)) as reader:
        assert reader.readable() and reader.seekable() and not reader.writable()
        assert reader.read(123) == data[:123]
        calls = len(client.requests)
        reader.seek(0)
        target = bytearray(123)
        assert reader.readinto(target) == 123
        assert target == data[:123] and len(client.requests) == calls
        for offset in range(0, len(data), media.RANGE_BYTES):
            reader.seek(offset)
            assert reader.read(1) == data[offset:offset + 1]
            assert reader.cache_bytes <= 16 * 1024 * 1024
        assert reader.seek(-10, io.SEEK_END) == len(data) - 10
        assert reader.read(10) == data[-10:]
        assert reader.read(1) == b""
        with pytest.raises(ValueError):
            reader.seek(-1)
        with pytest.raises(ValueError):
            reader.seek(0, 99)
    with pytest.raises(ValueError):
        reader.read(1)


def test_actual_attempts_are_charged_before_transfer(monkeypatch):
    monkeypatch.setattr(media, "PREVIEW_BYTES", 3 * media.RANGE_BYTES)
    client = RangedClient(b"x" * (4 * media.RANGE_BYTES), retries=1)
    reader = media.RemoteRangeReader(client, '"snapshot"', len(client.data))
    assert reader.read(1) == b"x"
    reader.seek(media.RANGE_BYTES)
    with pytest.raises(media.PreviewLimitError, match="byte"):
        reader.read(1)
    assert sum(length for _, length in client.requests) == 3 * media.RANGE_BYTES


@pytest.mark.parametrize("client", [
    RangedClient(b"abc", etag='"new"'),
    RangedClient(b"ab", reported_size=3),
    RangedClient(b"abcd"),
])
def test_mutation_and_truncation_fail(client):
    with pytest.raises((ResourceModifiedError, media.RemoteMediaError)):
        list(media.complete_chunks(client, '"snapshot"', 3, chunk_size=2))


def test_complete_chunks_are_original_and_uncapped(monkeypatch):
    monkeypatch.setattr(media, "PREVIEW_BYTES", 1)
    data = b"\x1f\x8b\x08original-compressed-bytes"
    client = RangedClient(data)
    assert b"".join(media.complete_chunks(client, '"snapshot"', len(data), 3)) == data
    assert all(length <= 3 for _, length in client.requests)


def test_reader_deadline(monkeypatch):
    now = [10.0]
    monkeypatch.setattr(media.time, "monotonic", lambda: now[0])
    reader = media.RemoteRangeReader(RangedClient(b"abc"), '"snapshot"', 3)
    now[0] += 31
    with pytest.raises(media.PreviewLimitError, match="deadline"):
        reader.read(1)


def test_native_decoder_is_terminated(monkeypatch):
    monkeypatch.setattr(media, "PREVIEW_SECONDS", 0.1)
    monkeypatch.setattr(media, "_decode", lambda *args: time.sleep(5))
    started = time.monotonic()
    with pytest.raises(media.PreviewLimitError, match="deadline"):
        media.remote_probe(RangedClient(b"abc"), '"snapshot"', 3)
    assert time.monotonic() - started < 1


def test_malformed_media_and_invalid_timestamp_fail():
    with pytest.raises((media.RemoteMediaError, ValueError)):
        media.remote_probe(RangedClient(b"not a video"), '"snapshot"', 11)
    for seconds in [-1, float("nan"), float("inf")]:
        with pytest.raises(ValueError, match="Frame timestamp must be finite and nonnegative"):
            media.remote_frame(RangedClient(b"abc"), '"snapshot"', 3, seconds)


def test_no_frame_at_requested_time(video):
    data = video.read_bytes()
    with pytest.raises(media.RemoteMediaError, match="frame"):
        media.remote_frame(RangedClient(data), '"snapshot"', len(data), 20)


def test_large_moov_end_is_sought_not_fully_downloaded(video):
    data = video.read_bytes()
    moov = data.index(b"moov") - 4
    if moov < data.index(b"mdat"):
        pytest.skip("The faststart variant is covered separately")
    padding = struct.pack(">I4s", 70 * 1024 * 1024, b"free") + b"\0" * (70 * 1024 * 1024 - 8)
    data = data[:moov] + padding + data[moov:]
    client = RangedClient(data)
    result = media._decode(client, '"snapshot"', len(data), 3, time.monotonic() + 30)
    assert result[1:] == (160, 96)
    assert any(offset > 64 * 1024 * 1024 for offset, _ in client.requests)
    assert sum(length for _, length in client.requests) < 16 * 1024 * 1024


class SDKStream:
    def __init__(self, response):
        self.response = response
        self.content_length = len(response.payload)

    def __iter__(self):
        yield self.response.payload
        if self.response.fail_body:
            raise ServiceResponseError("Connection interrupted after transfer")


class SDKResponse(HttpResponse):
    def __init__(self, request, payload, total, *, fail_body=False, status=206):
        super().__init__(request, None)
        self.status_code = status
        self.reason = "OK"
        self.content_type = "application/octet-stream"
        self.payload = payload
        self.fail_body = fail_body
        start = int(request.headers["x-ms-range"].split("=")[1].split("-")[0])
        self.headers = {
            "Content-Range": f"bytes {start}-{start + len(payload) - 1}/{total}",
            "Content-Length": str(len(payload)),
            "ETag": '"snapshot"',
            "Last-Modified": "Fri, 03 Apr 2026 12:30:00 GMT",
            "x-ms-blob-type": "BlockBlob",
            "Content-Encoding": "gzip",
        }

    def body(self):
        return self.payload

    def stream_download(self, pipeline, **kwargs):
        assert kwargs["decompress"] is False
        return SDKStream(self)


class SDKTransport(HttpTransport):
    def __init__(self, data, failures=0, status=206):
        self.data, self.failures, self.status = data, failures, status
        self.attempt_bytes = 0
        self.calls = 0

    def open(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def send(self, request, **kwargs):
        assert request.headers["If-Match"] == '"snapshot"'
        start, end = map(int, request.headers["x-ms-range"].split("=")[1].split("-"))
        payload = self.data[start:end + 1]
        self.attempt_bytes += len(payload)
        self.calls += 1
        return SDKResponse(
            request, payload, len(self.data), fail_body=self.calls <= self.failures,
            status=self.status,
        )


def test_real_sdk_internal_retries_and_prefetch_cannot_bypass_budget(monkeypatch):
    monkeypatch.setattr(media, "PREVIEW_BYTES", 2 * media.RANGE_BYTES)
    transport = SDKTransport(b"x" * (3 * media.RANGE_BYTES), failures=2)
    client = DataLakeFileClient(
        "https://example.dfs.core.windows.net", "footage", "video.mp4",
        credential=None, transport=transport,
    )
    reader = media.RemoteRangeReader(client, '"snapshot"', len(transport.data))
    with pytest.raises(media.PreviewLimitError, match="byte"):
        reader.read(1)
    assert transport.calls == 2
    assert transport.attempt_bytes == 2 * media.RANGE_BYTES


def test_real_sdk_complete_chunks_preserve_raw_content():
    data = b"\x1f\x8bnot-a-valid-gzip-body" * 10
    transport = SDKTransport(data)
    client = DataLakeFileClient(
        "https://example.dfs.core.windows.net", "footage", "video.mp4",
        credential=None, transport=transport,
    )
    assert b"".join(media.complete_chunks(client, '"snapshot"', len(data), 7)) == data
    assert transport.attempt_bytes == len(data)


def test_real_sdk_rejects_ignored_range_before_reading_body():
    transport = SDKTransport(b"x" * 100, status=200)
    client = DataLakeFileClient(
        "https://example.dfs.core.windows.net", "footage", "video.mp4",
        credential=None, transport=transport,
    )
    with pytest.raises(media.RemoteMediaError, match="ignored"):
        media.RemoteRangeReader(client, '"snapshot"', 100).read(1)


def test_exact_64_mib_budget_and_16_mib_cache():
    client = RangedClient(b"x" * (65 * 1024 * 1024))
    with media.RemoteRangeReader(client, '"snapshot"', len(client.data)) as reader:
        for offset in range(64):
            reader.seek(offset * 1024 * 1024)
            assert reader.read(1) == b"x"
            assert reader.cache_bytes <= 16 * 1024 * 1024
        assert reader.cache_bytes == 16 * 1024 * 1024
        reader.seek(64 * 1024 * 1024)
        with pytest.raises(media.PreviewLimitError, match="byte"):
            reader.read(1)
        assert sum(length for _, length in client.requests) == 64 * 1024 * 1024


def test_complete_chunks_detects_mutation_between_chunks():
    client = RangedClient(b"abcdef")
    chunks = media.complete_chunks(client, '"snapshot"', 6, 2)
    assert next(chunks) == b"ab"
    client.etag = '"changed"'
    with pytest.raises(ResourceModifiedError):
        next(chunks)


def test_empty_complete_source_requires_pinned_exact_properties():
    client = RangedClient(b"")
    def properties(**kwargs):
        assert kwargs == {"etag": '"snapshot"', "match_condition": MatchConditions.IfNotModified}
        return SimpleNamespace(size=0, etag='"snapshot"')
    client.get_file_properties = properties
    assert list(media.complete_chunks(client, '"snapshot"', 0)) == []
    client.get_file_properties = lambda **kwargs: SimpleNamespace(size=1, etag='"snapshot"')
    with pytest.raises(media.RemoteMediaError):
        list(media.complete_chunks(client, '"snapshot"', 0))


@pytest.mark.parametrize("etag,size,chunk_size", [("", 3, 1), ("*", 3, 1), ('"snapshot"', -1, 1), ('"snapshot"', 3, 0)])
def test_complete_reads_reject_invalid_contract(etag, size, chunk_size):
    with pytest.raises(ValueError):
        list(media.complete_chunks(RangedClient(b"abc"), etag, size, chunk_size))


def test_preview_preserves_local_display_rotation(video, tmp_path):
    rotated = tmp_path / "rotated.mp4"
    subprocess.run(
        ["ffmpeg", "-v", "error", "-i", str(video), "-c", "copy",
         "-metadata:s:v:0", "rotate=90", str(rotated)],
        check=True, capture_output=True,
    )
    data = rotated.read_bytes()
    assert media.remote_probe(RangedClient(data), '"snapshot"', len(data)) == probe_video(rotated)
    jpeg, width, height = media.remote_frame(RangedClient(data), '"snapshot"', len(data), 1)
    local = cv2.VideoCapture(str(rotated))
    try:
        ok, frame = local.read()
    finally:
        local.release()
    assert ok
    assert (width, height) == (frame.shape[1], frame.shape[0])
    assert cv2.imdecode(np.frombuffer(jpeg, np.uint8), cv2.IMREAD_COLOR).shape == frame.shape


def test_remote_probe_preserves_sdk_mutation_error():
    with pytest.raises(ResourceModifiedError):
        media.remote_probe(RangedClient(b"abc", etag='"changed"'), '"snapshot"', 3)


def test_identity_change_stops_complete_chunks_before_next_request():
    client = RangedClient(b"abcdef")
    user = ["reviewed-user"]
    checks = []
    def check_identity():
        checks.append(user[0])
        if user[0] != "reviewed-user":
            raise ValueError("CLI identity changed")
    chunks = media.complete_chunks(client, '"snapshot"', 6, 2, check_identity=check_identity)
    assert next(chunks) == b"ab"
    user[0] = "different-user"
    with pytest.raises(ValueError, match="identity changed"):
        next(chunks)
    assert client.requests == [(0, 2)]
    assert checks == ["reviewed-user", "different-user"]


def test_range_identity_checks_do_not_run_for_cached_bytes():
    client = RangedClient(b"abc")
    checks = []
    with media.RemoteRangeReader(
        client, '"snapshot"', 3, check_identity=lambda: checks.append(True),
    ) as reader:
        assert reader.read(1) == b"a"
        assert reader.read(1) == b"b"
        reader.seek(0)
        assert reader.read(1) == b"a"
    assert checks == [True]


def test_identity_is_checked_for_real_sdk_internal_retry():
    transport = SDKTransport(b"x" * media.RANGE_BYTES, failures=1)
    client = DataLakeFileClient(
        "https://example.dfs.core.windows.net", "footage", "video.mp4",
        credential=None, transport=transport,
    )
    checks = []
    def check_identity():
        checks.append(True)
        if len(checks) == 2:
            raise ValueError("CLI identity changed")
    with media.RemoteRangeReader(
        client, '"snapshot"', len(transport.data), check_identity=check_identity,
    ) as reader:
        with pytest.raises(ValueError, match="identity changed"):
            reader.read(1)
    assert checks == [True, True]
    assert transport.calls == 1


@pytest.mark.parametrize("operation", [media.remote_probe, media.remote_frame])
def test_preview_operations_forward_identity_guard(operation):
    def check_identity():
        raise ValueError("CLI identity changed")
    args = () if operation is media.remote_probe else (0,)
    with pytest.raises(ValueError, match="identity changed"):
        operation(RangedClient(b"abc"), '"snapshot"', 3, *args, check_identity=check_identity)


def test_empty_complete_source_checks_identity_before_properties():
    client = RangedClient(b"")
    def check_identity():
        raise ValueError("CLI identity changed")
    with pytest.raises(ValueError, match="identity changed"):
        list(media.complete_chunks(client, '"snapshot"', 0, check_identity=check_identity))


@pytest.mark.parametrize("time_base", [None, Fraction(0), Fraction(-1, 10)])
@pytest.mark.parametrize("seconds", [None, 0.0])
def test_missing_or_invalid_time_base_is_a_clear_validation_error(monkeypatch, time_base, seconds):
    stream = SimpleNamespace(
        time_base=time_base, duration=40, start_time=0,
        codec_context=SimpleNamespace(width=160, height=96), metadata={},
    )
    container = SimpleNamespace(
        streams=SimpleNamespace(video=[stream]), duration=4_000_000, metadata={},
    )
    monkeypatch.setattr(media.av, "open", lambda *args, **kwargs: nullcontext(container))
    with pytest.raises(media.RemoteMediaError, match="time base"):
        media._decode(RangedClient(b"abc"), '"snapshot"', 3, seconds, time.monotonic() + 30)


def test_deadline_exact_boundary_and_final_fractional_second(monkeypatch):
    now = [10.0]
    monkeypatch.setattr(media.time, "monotonic", lambda: now[0])
    reader = media.RemoteRangeReader(RangedClient(b"abc"), '"snapshot"', 3, deadline=11)
    now[0] = 10.5
    assert reader.read(1) == b"a"
    now[0] = 11
    with pytest.raises(media.PreviewLimitError, match="deadline"):
        reader.read(1)


@pytest.mark.parametrize("headers", [{}, {"Range": "bytes=0-3"}, {"Range": "bytes=2-1"}, {"Range": "bytes=0-99"}])
def test_invalid_sdk_range_is_rejected_before_transfer(headers):
    ranges = media._Ranges(RangedClient(b"abc"), '"snapshot"', 3, time.monotonic() + 30)
    with pytest.raises(media.RemoteMediaError, match="range"):
        ranges._request(SimpleNamespace(http_request=SimpleNamespace(headers=headers)))
    assert ranges.request_bytes == 0


def test_unbounded_raw_read_and_short_block_do_not_advance_past_returned_bytes():
    data = b"abc"
    reader = media.RemoteRangeReader(RangedClient(data), '"snapshot"', len(data))
    assert reader.read(1) == b"a"
    assert reader.read() == b"bc"
    assert reader.tell() == 3 and reader.read() == b""
    reader.seek(1)
    assert reader.read(10) == b"bc"
    assert reader.tell() == 3


def test_cache_retains_recently_used_ranges_without_retransferring(monkeypatch):
    monkeypatch.setattr(media, "CACHE_BYTES", 2 * media.RANGE_BYTES)
    client = RangedClient(b"x" * (3 * media.RANGE_BYTES))
    with media.RemoteRangeReader(client, '"snapshot"', len(client.data)) as reader:
        for block in [0, 1, 0, 2, 0]:
            reader.seek(block * media.RANGE_BYTES)
            assert reader.read(1) == b"x"
        assert len(client.requests) == 3


def test_single_byte_complete_chunks_and_empty_identity_attempts():
    assert list(media.complete_chunks(RangedClient(b"abc"), '"snapshot"', 3, 1)) == [b"a", b"b", b"c"]
    with pytest.raises(ValueError, match="Chunk size must be positive"):
        list(media.complete_chunks(RangedClient(b"abc"), '"snapshot"', 3, 0))
    checks = []
    client = RangedClient(b"")
    def properties(**kwargs):
        assert kwargs["retry_total"] == 0 and kwargs["redirect_max"] == 0
        kwargs["raw_request_hook"](None)
        assert checks == [True, True]
        return SimpleNamespace(size=0, etag='"snapshot"')
    client.get_file_properties = properties
    assert list(media.complete_chunks(client, '"snapshot"', 0, check_identity=lambda: checks.append(True))) == []


@pytest.mark.parametrize("stream_duration,container_duration,expected", [
    (4, 2_000_000, 0.4), (None, 2_000_000, 2), (0, 2_000_000, 2),
    (float("nan"), 2_000_000, 2), (float("inf"), 2_000_000, 2),
])
def test_metadata_duration_priority_fallback_and_capture_tags(stream_duration, container_duration, expected):
    stream = SimpleNamespace(
        time_base=Fraction(1, 10), duration=stream_duration,
        codec_context=SimpleNamespace(width=1, height=1),
        metadata={"creation_time": " 2026-04-03T12:30:00Z "},
    )
    container = SimpleNamespace(duration=container_duration, metadata={})
    result = media._metadata(container, stream)
    assert (result.width, result.height, result.duration_seconds) == (1, 1, expected)
    assert result.embedded_capture_times == ("2026-04-03T12:30:00Z",)


@pytest.mark.parametrize("width,height,stream_duration,container_duration", [
    (0, 1, 5, None), (1, 0, 5, None),
    (1, 1, None, None), (1, 1, 0, None), (1, 1, None, 0),
    (1, 1, None, float("nan")), (1, 1, None, float("inf")),
])
def test_metadata_rejects_invalid_dimensions_and_missing_duration(width, height, stream_duration, container_duration):
    stream = SimpleNamespace(
        time_base=Fraction(1, 10), duration=stream_duration,
        codec_context=SimpleNamespace(width=width, height=height), metadata={},
    )
    with pytest.raises(media.RemoteMediaError, match="dimensions or duration"):
        media._metadata(SimpleNamespace(duration=container_duration, metadata={}), stream)


def test_direct_decoder_guard_forwarding_and_exact_end(video):
    data = video.read_bytes()
    def check_identity():
        raise ValueError("CLI identity changed")
    with pytest.raises(ValueError, match="CLI identity changed"):
        media._decode(RangedClient(data), '"snapshot"', len(data), None, time.monotonic() + 30, check_identity)
    with pytest.raises(media.PreviewLimitError, match="deadline"):
        media._decode(RangedClient(data), '"snapshot"', len(data), None, time.monotonic() - 1)
    with pytest.raises(media.RemoteMediaError, match="No frame"):
        media._decode(RangedClient(data), '"snapshot"', len(data), 4, time.monotonic() + 30)
    jpeg, width, height = media._decode(RangedClient(data), '"snapshot"', len(data), 2.3, time.monotonic() + 30)
    image = cv2.imdecode(np.frombuffer(jpeg, np.uint8), cv2.IMREAD_COLOR)
    assert (width, height) == (160, 96) and image[:, :, 0].mean() > 200


@pytest.mark.parametrize("start_time", [None, 0, 100])
def test_decoder_primary_stream_time_units_and_exact_later_frame(monkeypatch, start_time):
    start = start_time or 0
    stream = SimpleNamespace(
        time_base=Fraction(1, 10), duration=40, start_time=start_time,
        codec_context=SimpleNamespace(width=8, height=8), metadata={},
    )
    def seek(offset, *, stream, backward):
        assert offset == start + 25 and stream is video_stream and backward is True
    def decode(requested):
        assert requested is stream
        return [
            SimpleNamespace(
                pts=start + index, width=8, height=8,
                to_ndarray=lambda *, format, index=index: np.full((8, 8, 3), index, np.uint8),
            )
            for index in [10, 20, 25, 26]
        ]
    video_stream = stream
    container = SimpleNamespace(
        streams=SimpleNamespace(video=[stream]), duration=4_000_000, metadata={},
        seek=seek, decode=decode,
    )
    monkeypatch.setattr(media.av, "open", lambda *args, **kwargs: nullcontext(container))
    jpeg, width, height = media._decode(RangedClient(b"abc"), '"snapshot"', 3, 2.5, time.monotonic() + 30)
    image = cv2.imdecode(np.frombuffer(jpeg, np.uint8), cv2.IMREAD_COLOR)
    assert (width, height) == (8, 8) and image.mean() == 25


@pytest.mark.parametrize("failure", ["no-video", "past-end", "missing-pts", "no-frames", "jpeg"])
def test_decoder_failure_guards_are_explicit(monkeypatch, failure):
    stream = SimpleNamespace(
        time_base=Fraction(1, 10), duration=40, start_time=0,
        codec_context=SimpleNamespace(width=8, height=8), metadata={},
    )
    def seek(*args, **kwargs):
        assert failure != "past-end", "Out-of-duration requests must not start decoding"
    frame = SimpleNamespace(
        pts=None if failure == "missing-pts" else 10, width=8, height=8,
        to_ndarray=lambda **kwargs: np.zeros((8, 8, 3), np.uint8),
    )
    container = SimpleNamespace(
        streams=SimpleNamespace(video=[] if failure == "no-video" else [stream]),
        duration=4_000_000, metadata={}, seek=seek,
        decode=lambda requested: [] if failure == "no-frames" else [frame],
    )
    monkeypatch.setattr(media.av, "open", lambda *args, **kwargs: nullcontext(container))
    if failure == "jpeg":
        monkeypatch.setattr(media.cv2, "imencode", lambda *args: (False, None))
    expected = {
        "no-video": "primary video stream", "past-end": "No frame",
        "missing-pts": "presentation timestamp", "no-frames": "No frame", "jpeg": "JPEG encoding failed",
    }[failure]
    with pytest.raises(media.RemoteMediaError, match=expected):
        media._decode(RangedClient(b"abc"), '"snapshot"', 3, 4 if failure == "past-end" else 1, time.monotonic() + 30)
