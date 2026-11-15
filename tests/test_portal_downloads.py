import io
import zipfile
from types import SimpleNamespace

import pytest

from people_counter.portal_downloads import stream_archive


def test_bulk_download_streams_original_bytes_and_retains_relative_names():
    videos = {"north/clip.mp4": b"original-video", "south/clip.mp4": b"other-video"}
    inputs = SimpleNamespace(paths=lambda: sorted(videos), chunks=lambda path: iter([videos[path]]))
    data = b"".join(stream_archive(inputs))
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        assert archive.namelist() == sorted(videos)
        assert {path: archive.read(path) for path in archive.namelist()} == videos


def test_source_failure_does_not_finish_a_success_shaped_partial_archive():
    def chunks(path):
        yield b"partial"
        raise ValueError("Source changed during transfer")
    inputs = SimpleNamespace(paths=lambda: ["clip.mp4"], chunks=chunks)
    stream = stream_archive(inputs)
    received = bytearray()
    with pytest.raises(ValueError, match="Source changed"):
        for block in stream:
            received.extend(block)
    with pytest.raises(zipfile.BadZipFile):
        zipfile.ZipFile(io.BytesIO(received))
