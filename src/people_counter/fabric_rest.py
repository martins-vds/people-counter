"""Minimal Microsoft Fabric REST client used by deployment tooling."""

from __future__ import annotations

import email.utils
import json
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from people_counter.fabric_sjd import WORKSPACE_ID


FABRIC_API_ROOT = "https://api.fabric.microsoft.com/v1"
FABRIC_RESOURCE = "https://api.fabric.microsoft.com"
_TERMINAL_SUCCESS = frozenset({"completed", "succeeded"})
_TERMINAL_FAILURE = frozenset({"cancelled", "canceled", "failed", "deduped"})


class FabricRestError(RuntimeError):
    """A Fabric API request or long-running operation failed."""


class TokenProvider(Protocol):
    def get_token(self) -> str: ...


class AzureCliTokenProvider:
    def get_token(self) -> str:
        try:
            process = subprocess.run(
                [
                    "az",
                    "account",
                    "get-access-token",
                    "--resource",
                    FABRIC_RESOURCE,
                    "--query",
                    "accessToken",
                    "--output",
                    "tsv",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except (OSError, subprocess.SubprocessError) as error:
            raise FabricRestError("Azure CLI token acquisition failed") from error
        token = process.stdout.strip()
        if not token:
            raise FabricRestError("Azure CLI returned an empty Fabric token")
        return token

    def __repr__(self) -> str:
        return "AzureCliTokenProvider(<redacted>)"


def make_token_provider(mode: str) -> TokenProvider:
    if mode != "azure-cli":
        raise ValueError(f"unsupported Fabric authentication mode {mode!r}")
    return AzureCliTokenProvider()


@dataclass(frozen=True)
class HTTPResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes = b""


class HTTPTransport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> HTTPResponse: ...


class UrllibTransport:
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> HTTPResponse:
        request = urllib.request.Request(
            url,
            data=body,
            headers=dict(headers),
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return HTTPResponse(
                    response.status,
                    dict(response.headers.items()),
                    response.read(),
                )
        except urllib.error.HTTPError as error:
            return HTTPResponse(
                error.code,
                dict(error.headers.items()),
                error.read(),
            )


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise FabricRestError(f"{label} must be an object")
    return value


def _object_list(value: object, label: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or any(
        not isinstance(item, Mapping) for item in value
    ):
        raise FabricRestError(f"{label} must be a list of objects")
    return list(value)


def _redact(value: object, secrets: Sequence[str]) -> str:
    text = str(value)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


class FabricRESTController:
    """Small authenticated Fabric client with LRO and pagination support."""

    def __init__(
        self,
        token_provider: TokenProvider,
        *,
        workspace_id: str = WORKSPACE_ID,
        transport: HTTPTransport | None = None,
        api_root: str = FABRIC_API_ROOT,
        timeout: float = 30.0,
        lro_timeout: float = 1800.0,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not api_root.startswith("https://"):
            raise ValueError("Fabric API root must use HTTPS")
        self.token_provider = token_provider
        self.workspace_id = workspace_id
        self.transport = transport or UrllibTransport()
        self.api_root = api_root.rstrip("/")
        self.timeout = timeout
        self.lro_timeout = lro_timeout
        self.sleep = sleep
        self.monotonic = monotonic

    def _url(self, path_or_url: str) -> str:
        if path_or_url.startswith("https://"):
            return path_or_url
        return f"{self.api_root}/{path_or_url.lstrip('/')}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        value: object | bytes | None = None,
        expected: Sequence[int] = (200,),
        content_type: str = "application/json",
    ) -> tuple[Mapping[str, str], bytes]:
        token = self.token_provider.get_token()
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        body: bytes | None = None
        if value is not None:
            headers["Content-Type"] = content_type
            body = value if isinstance(value, bytes) else _canonical_bytes(value)
        response = self.transport.request(
            method,
            self._url(path),
            headers=headers,
            body=body,
            timeout=self.timeout,
        )
        if response.status not in expected:
            detail = _redact(
                response.body.decode(errors="replace")[:2000],
                (token,),
            )
            raise FabricRestError(f"Fabric HTTP {response.status}: {detail}")
        return response.headers, response.body

    def _json(
        self,
        method: str,
        path: str,
        *,
        value: object | bytes | None = None,
        expected: Sequence[int] = (200,),
        content_type: str = "application/json",
    ) -> Mapping[str, Any]:
        _, body = self._request(
            method,
            path,
            value=value,
            expected=expected,
            content_type=content_type,
        )
        if not body:
            return {}
        try:
            result = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise FabricRestError("Fabric returned invalid JSON") from error
        return _mapping(result, "Fabric response")

    @staticmethod
    def _header(headers: Mapping[str, str], name: str) -> str | None:
        lowered = name.lower()
        return next(
            (
                str(value)
                for key, value in headers.items()
                if key.lower() == lowered
            ),
            None,
        )

    @classmethod
    def _retry_after(cls, headers: Mapping[str, str]) -> float:
        value = cls._header(headers, "Retry-After")
        if value is None:
            return 1.0
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                parsed = email.utils.parsedate_to_datetime(value)
                return max(0.0, parsed.timestamp() - time.time())
            except (TypeError, ValueError, OverflowError):
                return 1.0

    @staticmethod
    def _response_mapping(body: bytes, label: str) -> Mapping[str, Any]:
        if not body:
            return {}
        try:
            value = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise FabricRestError(f"{label} is invalid JSON") from error
        if value is None:
            return {}
        return _mapping(value, label)

    @classmethod
    def _operation_location(cls, headers: Mapping[str, str]) -> str | None:
        location = cls._header(headers, "Location")
        operation_id = cls._header(headers, "x-ms-operation-id")
        return f"/operations/{operation_id}" if not location and operation_id else location

    def _poll_operation(
        self,
        location: str,
        headers: Mapping[str, str],
    ) -> Mapping[str, Any]:
        deadline = self.monotonic() + self.lro_timeout
        current_headers = headers
        while True:
            if self.monotonic() >= deadline:
                raise TimeoutError("timed out waiting for Fabric operation")
            self.sleep(self._retry_after(current_headers))
            current_headers, current_body = self._request(
                "GET",
                location,
                expected=(200, 202, 429),
            )
            result = self._response_mapping(
                current_body,
                "Fabric operation response",
            )
            status = str(result.get("status", "")).lower()
            if status in _TERMINAL_FAILURE:
                raise FabricRestError(f"Fabric operation failed: {result!r}")
            if status in _TERMINAL_SUCCESS or (
                status == "" and current_body and result
            ):
                resource = result.get("resourceLocation") or result.get(
                    "resultLocation"
                )
                if not isinstance(resource, str):
                    response_location = self._header(
                        current_headers,
                        "Location",
                    )
                    if (
                        response_location
                        and response_location.rstrip("/")
                        != location.rstrip("/")
                    ):
                        resource = response_location
                return (
                    self._json("GET", resource)
                    if isinstance(resource, str)
                    else result
                )

    def _lro_json(
        self,
        method: str,
        path: str,
        *,
        value: object | bytes | None = None,
        accepted: Sequence[int] = (200, 201, 202),
        content_type: str = "application/json",
    ) -> Mapping[str, Any]:
        headers, body = self._request(
            method,
            path,
            value=value,
            expected=accepted,
            content_type=content_type,
        )
        initial = self._response_mapping(body, "Fabric response")
        location = self._operation_location(headers)
        return initial if not location else self._poll_operation(location, headers)

    def _list(self, path: str) -> list[Mapping[str, Any]]:
        result: list[Mapping[str, Any]] = []
        next_path: str | None = path
        while next_path is not None:
            page = self._json("GET", next_path)
            result.extend(_object_list(page.get("value"), "Fabric list value"))
            continuation = page.get("continuationUri")
            if continuation is not None and not isinstance(continuation, str):
                raise FabricRestError("Fabric continuation URI is ambiguous")
            next_path = continuation or None
        return result

    def list_items(self) -> list[Mapping[str, Any]]:
        return self._list(f"/workspaces/{self.workspace_id}/items")
