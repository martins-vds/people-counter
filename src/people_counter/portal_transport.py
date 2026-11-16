"""HTTP pools owned by the process making the Storage request."""

from __future__ import annotations

import os
from typing import Any

from azure.core.pipeline.transport import HttpRequest, HttpResponse, HttpTransport, RequestsTransport


class ProcessLocalTransport(HttpTransport):
    def __init__(self, *, connection_timeout: float, read_timeout: float) -> None:
        self._options = {"connection_timeout": connection_timeout, "read_timeout": read_timeout}
        self._transports: dict[int, RequestsTransport] = {}

    def _current(self) -> RequestsTransport:
        pid = os.getpid()
        transport = self._transports.get(pid)
        if transport is None:
            # Keep inherited pools untouched; the parent still owns their TLS state.
            transport = self._transports.setdefault(pid, RequestsTransport(**self._options))
        return transport

    def open(self) -> None:
        self._current().open()

    def send(self, request: HttpRequest, **kwargs: Any) -> HttpResponse:
        return self._current().send(request, **kwargs)

    def close(self) -> None:
        transport = self._transports.get(os.getpid())
        if transport is not None:
            transport.close()

    def __enter__(self) -> ProcessLocalTransport:
        self.open()
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
