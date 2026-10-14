"""Hardened Microsoft Fabric automation client.

This complements :mod:`people_counter.fabric_rest` with the pieces needed by
unattended deployment tooling:

* Bounded ``429``/``503`` retry handling on *every* request (not only while
  polling a long-running operation).
* Long-running operation (LRO) polling that can be *resumed* from a bare
  operation id, so a crashed deployment can continue an in-flight Fabric
  operation instead of resubmitting it.
* A machine-readable :class:`DeploymentEvidence` record describing the
  outcome of a deployment action, suitable for logging or audit storage.

:class:`TokenProvider` and :class:`HTTPTransport` are structural
(:class:`typing.Protocol`) types, so
:class:`people_counter.fabric_rest.AzureCliTokenProvider` satisfies
:class:`TokenProvider` here without any inheritance or import coupling
between the two modules.
"""

from __future__ import annotations

import email.utils
import json
import os
import re
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

DEFAULT_API_ROOT = "https://api.fabric.microsoft.com/v1"
DEFAULT_MAX_RETRIES = 5
_RETRYABLE_STATUSES = frozenset({429, 503})
_TERMINAL_SUCCESS = frozenset({"completed", "succeeded"})
_TERMINAL_FAILURE = frozenset({"cancelled", "canceled", "failed", "deduped"})
_DEFAULT_RETRY_AFTER_SECONDS = 1.0
_SENSITIVE_KEY = re.compile(
    r"(?:authorization|credential|password|privatekey|secret|token)",
    re.IGNORECASE,
)
_SENSITIVE_TEXT = re.compile(
    r"(?i)\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]+"
)
_GUID = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_GITHUB_OIDC_AUDIENCE = "api://AzureADTokenExchange"
_GITHUB_OIDC_HOST_SUFFIX = ".actions.githubusercontent.com"


class FabricApiError(RuntimeError):
    """A Fabric automation request or long-running operation failed."""


class TokenProvider(Protocol):
    def get_token(self) -> str: ...


@dataclass(frozen=True)
class AzureCliResourceTokenProvider:
    """Acquire a token for one explicit resource without retaining it."""

    resource: str

    def __post_init__(self) -> None:
        parsed = urlsplit(self.resource)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("token resource must be an HTTPS origin")

    def get_token(self) -> str:
        scope = f"{self.resource.rstrip('/')}/.default"
        try:
            process = subprocess.run(
                [
                    "az",
                    "account",
                    "get-access-token",
                    "--scope",
                    scope,
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
            raise FabricApiError(
                f"Azure CLI token acquisition failed for {self.resource}"
            ) from error
        token = process.stdout.strip()
        if not token:
            raise FabricApiError("Azure CLI returned an empty access token")
        return token

    def __repr__(self) -> str:
        return f"AzureCliResourceTokenProvider(resource={self.resource!r})"


@dataclass
class GitHubOidcResourceTokenProvider:
    """Exchange a GitHub Actions OIDC assertion for one Azure resource token."""

    resource: str
    client_id: str
    tenant_id: str
    request_url: str
    request_token: str = field(repr=False)
    _access_token: str | None = field(default=None, init=False, repr=False)
    _expires_at: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        resource = urlsplit(self.resource)
        request_url = urlsplit(self.request_url)
        request_hostname = request_url.hostname or ""
        is_github_oidc_host = (
            request_hostname.endswith(_GITHUB_OIDC_HOST_SUFFIX)
            and request_hostname.count(".") == 3
        )
        if resource.scheme != "https" or not resource.netloc:
            raise ValueError("token resource must be an HTTPS origin")
        if request_url.scheme != "https" or not is_github_oidc_host:
            raise ValueError(
                "GitHub OIDC request URL has an unsupported origin "
                f"(scheme={request_url.scheme!r}, host={request_hostname!r})"
            )
        if _GUID.fullmatch(self.client_id) is None:
            raise ValueError("Azure client ID must be a UUID")
        if _GUID.fullmatch(self.tenant_id) is None:
            raise ValueError("Azure tenant ID must be a UUID")
        if not self.request_token:
            raise ValueError("GitHub OIDC request token is required")

    @staticmethod
    def _response_json(response: Any, label: str) -> Mapping[str, Any]:
        try:
            body = response.read()
        except OSError as error:
            raise FabricApiError(f"{label} response could not be read") from error
        return _response_mapping(body, label)

    def _github_assertion(self) -> str:
        separator = "&" if "?" in self.request_url else "?"
        assertion_url = (
            f"{self.request_url}{separator}"
            f"{urlencode({'audience': _GITHUB_OIDC_AUDIENCE})}"
        )
        request = Request(
            assertion_url,
            headers={"Authorization": f"Bearer {self.request_token}"},
        )
        try:
            with urlopen(request, timeout=60) as response:
                payload = self._response_json(response, "GitHub OIDC")
        except OSError as error:
            raise FabricApiError("GitHub OIDC assertion request failed") from error
        assertion = payload.get("value")
        if not isinstance(assertion, str) or not assertion:
            raise FabricApiError("GitHub OIDC response omitted the assertion")
        return assertion

    def get_token(self) -> str:
        now = time.time()
        if self._access_token is not None and now < self._expires_at - 60:
            return self._access_token
        assertion = self._github_assertion()
        scope = f"{self.resource.rstrip('/')}/.default"
        form = urlencode(
            {
                "client_assertion": assertion,
                "client_assertion_type": (
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
                ),
                "client_id": self.client_id,
                "grant_type": "client_credentials",
                "scope": scope,
            }
        ).encode()
        request = Request(
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token",
            data=form,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=60) as response:
                payload = self._response_json(response, "Microsoft Entra token")
        except OSError as error:
            raise FabricApiError("Microsoft Entra token exchange failed") from error
        token = payload.get("access_token")
        expires_in = payload.get("expires_in")
        if not isinstance(token, str) or not token:
            raise FabricApiError("Microsoft Entra response omitted the access token")
        if (
            not isinstance(expires_in, int)
            or isinstance(expires_in, bool)
            or expires_in <= 60
        ):
            raise FabricApiError("Microsoft Entra token lifetime is invalid")
        self._access_token = token
        self._expires_at = now + expires_in
        return token

    def __repr__(self) -> str:
        return (
            "GitHubOidcResourceTokenProvider("
            f"resource={self.resource!r}, client_id={self.client_id!r}, "
            f"tenant_id={self.tenant_id!r})"
        )


def make_resource_token_provider(
    resource: str,
    *,
    env: Mapping[str, str] | None = None,
) -> TokenProvider:
    """Use GitHub OIDC in CI and Azure CLI in an interactive environment."""
    selected = os.environ if env is None else env
    request_url = selected.get("ACTIONS_ID_TOKEN_REQUEST_URL")
    request_token = selected.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
    if request_url is None and request_token is None:
        return AzureCliResourceTokenProvider(resource)
    if request_url is None or request_token is None:
        raise FabricApiError("GitHub OIDC environment is incomplete")
    client_id = selected.get("ARM_CLIENT_ID")
    tenant_id = selected.get("ARM_TENANT_ID")
    if not client_id or not tenant_id:
        raise FabricApiError("Azure workload identity environment is incomplete")
    return GitHubOidcResourceTokenProvider(
        resource=resource,
        client_id=client_id,
        tenant_id=tenant_id,
        request_url=request_url,
        request_token=request_token,
    )


@dataclass(frozen=True)
class HTTPResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes = b""


class HTTPResponseLike(Protocol):
    @property
    def status(self) -> int: ...

    @property
    def headers(self) -> Mapping[str, str]: ...

    @property
    def body(self) -> bytes: ...


class HTTPTransport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> HTTPResponseLike: ...


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
        raise FabricApiError(f"{label} must be an object")
    return value


def _object_list(value: object, label: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or any(
        not isinstance(item, Mapping) for item in value
    ):
        raise FabricApiError(f"{label} must be a list of objects")
    return list(value)


def _response_mapping(body: bytes, label: str) -> Mapping[str, Any]:
    if not body:
        return {}
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FabricApiError(f"{label} is invalid JSON") from error
    if value is None:
        return {}
    return _mapping(value, label)


def _redact(value: object, secrets: Sequence[str]) -> str:
    text = str(value)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    return _SENSITIVE_TEXT.sub("<redacted>", text)


def _secret_values(value: object, *, sensitive: bool = False) -> tuple[str, ...]:
    if isinstance(value, Mapping):
        values: list[str] = []
        for key, item in value.items():
            values.extend(
                _secret_values(
                    item,
                    sensitive=sensitive
                    or _SENSITIVE_KEY.search(str(key).replace("_", "")) is not None,
                )
            )
        return tuple(values)
    if isinstance(value, (list, tuple)):
        return tuple(
            secret
            for item in value
            for secret in _secret_values(item, sensitive=sensitive)
        )
    if sensitive and isinstance(value, str) and value:
        return (value,)
    return ()


def redact_evidence(value: object) -> Any:
    """Return a JSON-safe value with secret-bearing fields removed."""
    if isinstance(value, Mapping):
        return {
            str(key): (
                "<redacted>"
                if _SENSITIVE_KEY.search(str(key).replace("_", ""))
                else redact_evidence(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact_evidence(item) for item in value]
    if isinstance(value, str):
        return _SENSITIVE_TEXT.sub("<redacted>", value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return str(value)


def _header(headers: Mapping[str, str], name: str) -> str | None:
    lowered = name.lower()
    return next(
        (str(value) for key, value in headers.items() if key.lower() == lowered),
        None,
    )


def _retry_after_seconds(headers: Mapping[str, str]) -> float:
    value = _header(headers, "Retry-After")
    if value is None:
        return _DEFAULT_RETRY_AFTER_SECONDS
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            parsed = email.utils.parsedate_to_datetime(value)
            return max(0.0, parsed.timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return _DEFAULT_RETRY_AFTER_SECONDS


@dataclass(frozen=True)
class OperationHandle:
    """A resumable reference to a Fabric long-running operation.

    Only the bare ``operation_id`` needs to survive a process restart (for
    example, persisted from a prior :class:`DeploymentEvidence`); it is
    sufficient to resume polling without resubmitting the original request.
    """

    operation_id: str
    result_path: str | None = None

    def __post_init__(self) -> None:
        if not self.operation_id:
            raise ValueError("operation_id must be non-empty")

    def location(self) -> str:
        return f"/operations/{self.operation_id}"

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> "OperationHandle | None":
        operation_id = _header(headers, "x-ms-operation-id")
        if operation_id:
            return cls(operation_id)
        location = _header(headers, "Location")
        if location:
            return cls(location.rstrip("/").rsplit("/", 1)[-1])
        return None


@dataclass(frozen=True)
class DeploymentEvidence:
    """Machine-readable outcome of a single Fabric automation action.

    ``to_dict``/``to_json`` are stable (sorted keys, no floats/NaN) so the
    record can be diffed or archived as deployment audit evidence.
    """

    action: str
    status: str
    operation_id: str | None = None
    resource: Mapping[str, Any] | None = None
    error: str | None = None
    attempts: int = 1

    def operation_handle(self) -> OperationHandle | None:
        """Rebuild a resumable handle from this evidence's operation id."""
        return OperationHandle(self.operation_id) if self.operation_id else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "status": self.status,
            "operationId": self.operation_id,
            "resource": (
                redact_evidence(dict(self.resource))
                if self.resource is not None
                else None
            ),
            "error": redact_evidence(self.error),
            "attempts": self.attempts,
        }

    def to_json(self) -> str:
        return json.dumps(
            self.to_dict(),
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
        )


class FabricAutomationClient:
    """Authenticated Fabric client with bounded retries and resumable LROs."""

    def __init__(
        self,
        token_provider: TokenProvider,
        *,
        transport: HTTPTransport,
        api_root: str = DEFAULT_API_ROOT,
        timeout: float = 30.0,
        lro_timeout: float = 1800.0,
        max_retries: int = DEFAULT_MAX_RETRIES,
        default_headers: Mapping[str, str] | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not api_root.startswith("https://"):
            raise ValueError("Fabric API root must use HTTPS")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        self.token_provider = token_provider
        self.transport = transport
        self.api_root = api_root.rstrip("/")
        self.timeout = timeout
        self.lro_timeout = lro_timeout
        self.max_retries = max_retries
        self.default_headers = dict(default_headers or {})
        if any(key.lower() == "authorization" for key in self.default_headers):
            raise ValueError("default headers cannot override Authorization")
        self.sleep = sleep
        self.monotonic = monotonic
        root = urlsplit(self.api_root)
        self._api_origin = (root.scheme.lower(), root.netloc.lower())

    def _url(self, path_or_url: str) -> str:
        if path_or_url.startswith("https://"):
            target = urlsplit(path_or_url)
            if (target.scheme.lower(), target.netloc.lower()) != self._api_origin:
                raise FabricApiError(
                    "refusing to send an access token to a different API origin"
                )
            return path_or_url
        if "://" in path_or_url:
            raise FabricApiError("API paths must be relative or HTTPS URLs")
        return f"{self.api_root}/{path_or_url.lstrip('/')}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        value: object | bytes | None = None,
        expected: Sequence[int] = (200,),
        content_type: str = "application/json",
    ) -> tuple[int, Mapping[str, str], bytes]:
        headers_base: dict[str, str] = {
            "Accept": "application/json",
            **self.default_headers,
        }
        body: bytes | None = None
        if value is not None:
            headers_base["Content-Type"] = content_type
            body = value if isinstance(value, bytes) else _canonical_bytes(value)

        url = self._url(path)
        request_secrets = _secret_values(value)
        attempts = 0
        while True:
            token = self.token_provider.get_token()
            headers = dict(headers_base)
            headers["Authorization"] = f"Bearer {token}"
            response = self.transport.request(
                method,
                url,
                headers=headers,
                body=body,
                timeout=self.timeout,
            )
            attempts += 1
            if response.status in expected:
                return response.status, response.headers, response.body
            if response.status in _RETRYABLE_STATUSES and attempts <= self.max_retries:
                self.sleep(_retry_after_seconds(response.headers))
                continue
            detail = _redact(
                response.body.decode(errors="replace")[:2000],
                (token, *request_secrets),
            )
            raise FabricApiError(
                f"Fabric HTTP {response.status} after {attempts} attempt(s): {detail}"
            )

    def request_json(
        self,
        method: str,
        path: str,
        *,
        value: object | bytes | None = None,
        expected: Sequence[int] = (200,),
        content_type: str = "application/json",
    ) -> Mapping[str, Any]:
        _, _, body = self._request(
            method, path, value=value, expected=expected, content_type=content_type
        )
        return _response_mapping(body, "Fabric response")

    def request_response(
        self,
        method: str,
        path: str,
        *,
        value: object | bytes | None = None,
        expected: Sequence[int] = (200,),
        content_type: str = "application/json",
    ) -> tuple[int, Mapping[str, str], bytes]:
        """Return the status, headers, and bytes for APIs with header state."""
        return self._request(
            method,
            path,
            value=value,
            expected=expected,
            content_type=content_type,
        )

    def list_pages(
        self,
        path: str,
        *,
        value_key: str = "value",
        empty_error_codes: Sequence[str] = (),
    ) -> list[Mapping[str, Any]]:
        result: list[Mapping[str, Any]] = []
        next_path: str | None = path
        while next_path is not None:
            if empty_error_codes:
                status, _, body = self._request(
                    "GET",
                    next_path,
                    expected=(200, 404),
                )
                page = _response_mapping(body, "Fabric response")
                if status == 404:
                    error_code = page.get("errorCode")
                    if error_code in empty_error_codes:
                        return result
                    raise FabricApiError(
                        "Fabric list returned HTTP 404 with unexpected error "
                        f"code {error_code!r}"
                    )
            else:
                page = self.request_json("GET", next_path)
            result.extend(
                _object_list(
                    page.get(value_key),
                    f"Fabric list {value_key!r}",
                )
            )
            continuation = page.get("continuationUri")
            if continuation is not None and not isinstance(continuation, str):
                raise FabricApiError("Fabric continuation URI is ambiguous")
            next_path = continuation or None
        return result

    def submit_operation(
        self,
        method: str,
        path: str,
        *,
        value: object | bytes | None = None,
        accepted: Sequence[int] = (200, 201, 202),
        content_type: str = "application/json",
    ) -> tuple[OperationHandle | None, Mapping[str, Any]]:
        """Submit a request that may start a Fabric long-running operation.

        Returns ``(None, result)`` when Fabric answered synchronously, or
        ``(handle, {})`` when an async operation was accepted.
        """
        status, headers, body = self._request(
            method, path, value=value, expected=accepted, content_type=content_type
        )
        initial = _response_mapping(body, "Fabric response")
        handle = OperationHandle.from_headers(headers) if status == 202 else None
        if handle is not None and "/getDefinition" in path:
            handle = OperationHandle(
                handle.operation_id,
                f"{handle.location()}/result",
            )
        return handle, initial

    def resume_operation(
        self, handle: OperationHandle, *, action: str = "resume"
    ) -> DeploymentEvidence:
        """Resume polling a previously submitted operation by its id alone."""
        return self._poll(handle, action=action)

    def run_operation(
        self,
        method: str,
        path: str,
        *,
        action: str,
        value: object | bytes | None = None,
        accepted: Sequence[int] = (200, 201, 202),
        content_type: str = "application/json",
    ) -> DeploymentEvidence:
        """Submit a request and drive it to a terminal :class:`DeploymentEvidence`."""
        handle, initial = self.submit_operation(
            method, path, value=value, accepted=accepted, content_type=content_type
        )
        if handle is None:
            return DeploymentEvidence(action=action, status="succeeded", resource=initial)
        return self._poll(handle, action=action)

    def _poll(self, handle: OperationHandle, *, action: str) -> DeploymentEvidence:
        deadline = self.monotonic() + self.lro_timeout
        attempts = 0
        while True:
            if self.monotonic() >= deadline:
                return DeploymentEvidence(
                    action=action,
                    status="failed",
                    operation_id=handle.operation_id,
                    error="timed out waiting for Fabric operation",
                    attempts=attempts,
                )
            attempts += 1
            _, headers, body = self._request(
                "GET",
                handle.location(),
                expected=(200,),
            )
            result = _response_mapping(body, "Fabric operation response")
            status = str(result.get("status", "")).lower()
            if status in _TERMINAL_FAILURE:
                return DeploymentEvidence(
                    action=action,
                    status="failed",
                    operation_id=handle.operation_id,
                    resource=result,
                    error=f"Fabric operation failed: {result!r}",
                    attempts=attempts,
                )
            if status in _TERMINAL_SUCCESS:
                resource_path = result.get("resultLocation") or result.get(
                    "resourceLocation"
                ) or handle.result_path
                final = (
                    self.request_json("GET", resource_path)
                    if isinstance(resource_path, str)
                    else result
                )
                return DeploymentEvidence(
                    action=action,
                    status="succeeded",
                    operation_id=handle.operation_id,
                    resource=final,
                    attempts=attempts,
                )
            self.sleep(_retry_after_seconds(headers))
