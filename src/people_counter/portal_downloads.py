"""Bounded streaming ZIP downloads over the existing video-input interface."""

from __future__ import annotations

import logging
import io
import queue
import threading
import zipfile
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing_extensions import Buffer

from people_counter.manifest import normalize_relative_path
from people_counter.video_input import VideoInput, closing_iterator


class _ArchiveSink(io.RawIOBase):
    def __init__(self, output: queue.Queue, stopped: threading.Event):
        super().__init__()
        self.output, self.stopped = output, stopped
        self.position = 0
        self.failed = False

    def emit(self, value: bytes | Exception | None) -> None:
        while not self.stopped.is_set():
            try:
                self.output.put(value, timeout=0.1)
                return
            except queue.Full:
                continue
        raise BrokenPipeError("Download consumer disconnected")

    def write(self, data: Buffer) -> int:
        view = memoryview(data).cast("B")
        if not self.failed:
            for offset in range(0, len(view), 64 * 1024):
                self.emit(bytes(view[offset:offset + 64 * 1024]))
        self.position += len(view)
        return len(view)

    def tell(self) -> int:
        return self.position

    def flush(self) -> None:
        pass


def _produce(inputs: VideoInput, sink: _ArchiveSink) -> None:
    try:
        with zipfile.ZipFile(sink, "w", compression=zipfile.ZIP_STORED) as archive:
            try:
                for path in inputs.paths():
                    name = normalize_relative_path(path)
                    with archive.open(name, "w", force_zip64=True) as entry:
                        with closing_iterator(inputs.chunks(path)) as values:
                            for chunk in values:
                                entry.write(chunk)
            except Exception:
                # Do not emit a central directory after a source-read failure.
                sink.failed = True
                raise
        sink.emit(None)
    except BrokenPipeError:
        logging.getLogger(__name__).info("Bulk download consumer disconnected")
    except Exception as error:
        logging.getLogger(__name__).warning("Bulk download failed (%s)", type(error).__name__)
        sink.emit(error)


def stream_archive(inputs: VideoInput) -> Iterator[bytes]:
    output: queue.Queue[bytes | Exception | None] = queue.Queue(maxsize=2)
    stopped = threading.Event()
    worker = threading.Thread(target=_produce, args=(inputs, _ArchiveSink(output, stopped)), daemon=True)
    worker.start()
    try:
        while True:
            value = output.get()
            if isinstance(value, Exception):
                raise value
            if value is None:
                break
            yield value
    finally:
        stopped.set()
        worker.join(timeout=5)
