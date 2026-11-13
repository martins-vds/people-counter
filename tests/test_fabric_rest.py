from __future__ import annotations

import io
import subprocess
import threading
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace

import pytest

import people_counter.fabric_rest as fabric_rest
from people_counter.fabric_rest import (
    AzureCliTokenProvider,
    FabricRESTController,
    FabricRestError,
    HTTPResponse,
    UrllibTransport,
    make_token_provider,
)

from tests.fabric_fixtures import WORKSPACE_ID


@dataclass
class _TokenProvider:
    token: str = "secret-token"

    def get_token(self) -> str:
        return self.token


@dataclass
class _Transport:
    responses: list[HTTPResponse]
    requests: list[dict[str, object]] = field(default_factory=list)

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> HTTPResponse:
        self.requests.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "body": body,
                "timeout": timeout,
            }
        )
        return self.responses.pop(0)


def test_token_provider_factory_is_explicit() -> None:
    assert isinstance(make_token_provider("azure-cli"), AzureCliTokenProvider)
    with pytest.raises(ValueError, match="unsupported"):
        make_token_provider("interactive")


def test_azure_cli_token_provider_uses_fixed_fabric_resource(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def run(command, **kwargs):
        observed["command"] = command
        observed["kwargs"] = kwargs
        return SimpleNamespace(stdout="token-value\n")

    monkeypatch.setattr(fabric_rest.subprocess, "run", run)
    provider = AzureCliTokenProvider()

    assert provider.get_token() == "token-value"
    assert observed == {
        "command": [
            "az",
            "account",
            "get-access-token",
            "--resource",
            "https://api.fabric.microsoft.com",
            "--query",
            "accessToken",
            "--output",
            "tsv",
        ],
        "kwargs": {
            "check": True,
            "capture_output": True,
            "text": True,
            "timeout": 60,
        },
    }
    assert repr(provider) == "AzureCliTokenProvider(<redacted>)"


@pytest.mark.parametrize(
    "result",
    [
        OSError("missing az"),
        subprocess.CalledProcessError(1, ["az"]),
        SimpleNamespace(stdout=" \n"),
    ],
)
def test_azure_cli_token_provider_surfaces_failures(
    monkeypatch: pytest.MonkeyPatch,
    result: object,
) -> None:
    def run(*_args, **_kwargs):
        if isinstance(result, BaseException):
            raise result
        return result

    monkeypatch.setattr(fabric_rest.subprocess, "run", run)

    with pytest.raises(FabricRestError, match="token"):
        AzureCliTokenProvider().get_token()


class _UrlResponse:
    status = 201
    headers = {"X-Test": "value"}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    @staticmethod
    def read() -> bytes:
        return b"created"


def test_urllib_transport_returns_success_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def urlopen(request, *, timeout):
        observed["request"] = request
        observed["timeout"] = timeout
        return _UrlResponse()

    monkeypatch.setattr(fabric_rest.urllib.request.OpenerDirector, "open", lambda self, request, *, timeout: urlopen(request, timeout=timeout))

    response = UrllibTransport().request(
        "POST",
        "https://fabric.example/items",
        headers={"Content-Type": "application/json"},
        body=b"{}",
        timeout=12.0,
    )

    request = observed["request"]
    assert response == HTTPResponse(201, {"X-Test": "value"}, b"created")
    assert request.get_method() == "POST"
    assert request.full_url == "https://fabric.example/items"
    assert request.data == b"{}"
    assert request.headers == {"Content-type": "application/json"}
    assert observed["timeout"] == 12.0


def test_urllib_transport_returns_http_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def urlopen(_request, *, timeout):
        assert timeout == 7.0
        raise urllib.error.HTTPError(
            "https://fabric.example/items",
            429,
            "throttled",
            {"Retry-After": "2"},
            io.BytesIO(b"retry"),
        )

    monkeypatch.setattr(fabric_rest.urllib.request.OpenerDirector, "open", lambda self, request, *, timeout: urlopen(request, timeout=timeout))

    response = UrllibTransport().request(
        "GET",
        "https://fabric.example/items",
        headers={},
        body=None,
        timeout=7.0,
    )

    assert response == HTTPResponse(429, {"Retry-After": "2"}, b"retry")


def test_urllib_transport_preserves_method_without_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def urlopen(request, *, timeout):
        observed["request"] = request
        observed["timeout"] = timeout
        return _UrlResponse()

    monkeypatch.setattr(fabric_rest.urllib.request.OpenerDirector, "open", lambda self, request, *, timeout: urlopen(request, timeout=timeout))

    UrllibTransport().request(
        "DELETE",
        "https://fabric.example/items/one",
        headers={},
        body=None,
        timeout=9.0,
    )

    request = observed["request"]
    assert request.get_method() == "DELETE"
    assert request.data is None
    assert observed["timeout"] == 9.0


def test_request_authenticates_and_serializes_canonical_json() -> None:
    transport = _Transport([HTTPResponse(200, {}, b'{"ok":true}')])
    client = FabricRESTController(
        _TokenProvider(),
        transport=transport,
        api_root="https://fabric.example/v1",
        workspace_id=WORKSPACE_ID,
    )

    result = client._json("POST", "/items", value={"z": 1, "a": 2})

    assert result == {"ok": True}
    assert transport.requests == [
        {
            "method": "POST",
            "url": "https://fabric.example/v1/items",
            "headers": {
                "Accept": "application/json",
                "Authorization": "Bearer secret-token",
                "Content-Type": "application/json",
            },
            "body": b'{"a":2,"z":1}',
            "timeout": 30.0,
        }
    ]


def test_request_redacts_token_from_http_errors() -> None:
    transport = _Transport(
        [HTTPResponse(403, {}, b"denied secret-token")]
    )
    client = FabricRESTController(_TokenProvider(), transport=transport, workspace_id=WORKSPACE_ID)

    with pytest.raises(FabricRestError, match="Fabric HTTP 403") as captured:
        client._json("GET", "/items")

    assert "secret-token" not in str(captured.value)
    assert "<redacted>" in str(captured.value)


def test_request_rejects_non_finite_json_before_transport() -> None:
    transport = _Transport([])
    client = FabricRESTController(_TokenProvider(), transport=transport, workspace_id=WORKSPACE_ID)

    with pytest.raises(ValueError, match="Out of range float values"):
        client._json("POST", "/items", value={"invalid": float("nan")})

    assert transport.requests == []


def test_invalid_utf8_error_body_is_safely_reported() -> None:
    transport = _Transport(
        [HTTPResponse(500, {}, b"\xff secret-token")]
    )
    client = FabricRESTController(_TokenProvider(), transport=transport, workspace_id=WORKSPACE_ID)

    with pytest.raises(FabricRestError) as captured:
        client._json("GET", "/items")

    assert "\ufffd <redacted>" in str(captured.value)


def test_list_items_follows_continuation_uri() -> None:
    transport = _Transport(
        [
            HTTPResponse(
                200,
                {},
                b'{"value":[{"id":"one"}],"continuationUri":'
                b'"https://fabric.example/next"}',
            ),
            HTTPResponse(200, {}, b'{"value":[{"id":"two"}]}'),
        ]
    )
    client = FabricRESTController(
        _TokenProvider(),
        workspace_id=WORKSPACE_ID,
        transport=transport,
        api_root="https://fabric.example/v1",
    )

    assert client.list_items() == [{"id": "one"}, {"id": "two"}]
    assert transport.requests[1]["url"] == "https://fabric.example/next"


@pytest.mark.parametrize("url", [
    "https://attacker.invalid/next",
    "https://api.fabric.microsoft.com.attacker.invalid/next",
    "https://api.fabric.microsoft.com:444/next",
    "https://user@api.fabric.microsoft.com/v1/items",
    "https://:secret@api.fabric.microsoft.com/v1/items",
    "https://api.fabric.microsoft.com/v1/items#fragment",
    "http://api.fabric.microsoft.com/v1/items",
    "//attacker.invalid/next",
])
def test_authenticated_requests_reject_unsafe_destinations(url):
    transport = _Transport([])
    client = FabricRESTController(_TokenProvider(), workspace_id=WORKSPACE_ID, transport=transport)
    with pytest.raises(FabricRestError, match="origin"):
        client._json("GET", url)
    assert transport.requests == []


@pytest.mark.parametrize("field", ["Location", "continuationUri", "resourceLocation", "resultLocation"])
def test_server_supplied_destinations_cannot_forward_tokens(field):
    url = "https://attacker.invalid/next"
    responses = {
        "Location": [HTTPResponse(202, {"Location": url}, b"{}")],
        "continuationUri": [HTTPResponse(200, {}, ('{"value":[],"continuationUri":"' + url + '"}').encode())],
        "resourceLocation": [
            HTTPResponse(202, {"Location": "/operations/one"}, b"{}"),
            HTTPResponse(200, {}, ('{"status":"Succeeded","resourceLocation":"' + url + '"}').encode()),
        ],
        "resultLocation": [
            HTTPResponse(202, {"Location": "/operations/one"}, b"{}"),
            HTTPResponse(200, {}, ('{"status":"Succeeded","resultLocation":"' + url + '"}').encode()),
        ],
    }
    transport = _Transport(responses[field])
    client = FabricRESTController(
        _TokenProvider(), workspace_id=WORKSPACE_ID, transport=transport,
        sleep=lambda _: None, monotonic=lambda: 0,
    )
    with pytest.raises(FabricRestError, match="origin"):
        if field == "continuationUri":
            client.list_items()
        else:
            client._lro_json("POST", "/items")
    assert all(str(call["url"]).startswith("https://api.fabric.microsoft.com/") for call in transport.requests)


def test_urllib_transport_does_not_follow_authenticated_redirects():
    paths = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            paths.append(self.path)
            self.send_response(302)
            self.send_header("Location", "/redirected")
            self.end_headers()

        def log_message(self, *_args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with pytest.raises(FabricRestError, match="redirect"):
            UrllibTransport().request(
                "GET", f"http://127.0.0.1:{server.server_port}/initial",
                headers={"Authorization": "Bearer synthetic-token"}, body=None, timeout=5,
            )
        assert paths == ["/initial"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_lro_polls_and_reads_resource_location() -> None:
    transport = _Transport(
        [
            HTTPResponse(202, {"x-ms-operation-id": "operation"}, b"{}"),
            HTTPResponse(
                200,
                {"Retry-After": "0"},
                b'{"status":"Succeeded","resourceLocation":"/result"}',
            ),
            HTTPResponse(200, {}, b'{"id":"created"}'),
        ]
    )
    client = FabricRESTController(
        _TokenProvider(),
        transport=transport,
        sleep=lambda _: None,
        monotonic=lambda: 0.0,
        workspace_id=WORKSPACE_ID,
    )

    assert client._lro_json("POST", "/items", value={}) == {
        "id": "created"
    }
    assert [request["url"] for request in transport.requests] == [
        "https://api.fabric.microsoft.com/v1/items",
        "https://api.fabric.microsoft.com/v1/operations/operation",
        "https://api.fabric.microsoft.com/v1/result",
    ]


def test_lro_reads_result_location() -> None:
    transport = _Transport(
        [
            HTTPResponse(202, {"Location": "/operations/one"}, b"{}"),
            HTTPResponse(
                200,
                {},
                b'{"status":"Completed","resultLocation":"/result"}',
            ),
            HTTPResponse(200, {}, b'{"id":"created"}'),
        ]
    )
    client = FabricRESTController(
        _TokenProvider(),
        transport=transport,
        sleep=lambda _: None,
        monotonic=lambda: 0.0,
        workspace_id=WORKSPACE_ID,
    )

    assert client._lro_json("POST", "/items", value={}) == {
        "id": "created"
    }


def test_lro_surfaces_terminal_failure() -> None:
    transport = _Transport(
        [
            HTTPResponse(202, {"Location": "/operations/one"}, b"{}"),
            HTTPResponse(200, {}, b'{"status":"Failed","error":"boom"}'),
        ]
    )
    client = FabricRESTController(
        _TokenProvider(),
        transport=transport,
        sleep=lambda _: None,
        monotonic=lambda: 0.0,
        workspace_id=WORKSPACE_ID,
    )

    with pytest.raises(FabricRestError, match="operation failed"):
        client._lro_json("POST", "/items", value={})


def test_non_https_api_root_is_rejected() -> None:
    with pytest.raises(ValueError, match="must use HTTPS"):
        FabricRESTController(
            _TokenProvider(),
            api_root="http://fabric.example/v1",
            workspace_id=WORKSPACE_ID,
        )


@pytest.mark.parametrize(
    "workspace_id",
    [
        None, "", "workspace", "../another-workspace",
        "00000000-0000-0000-0000-000000000000",
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
    ],
)
def test_rest_client_rejects_invalid_or_nil_workspace_bindings(workspace_id):
    transport = _Transport([])
    with pytest.raises(ValueError, match="workspace_id"):
        FabricRESTController(
            _TokenProvider(), workspace_id=workspace_id, transport=transport,
        )
    assert transport.requests == []


@pytest.mark.parametrize("body,message", [
    (b"{", "invalid JSON"), (b"\xff", "invalid JSON"),
    (b"[]", "must be an object"),
    (b'{"value":null}', "list of objects"),
    (b'{"value":[1]}', "list of objects"),
    (b'{"value":[],"continuationUri":1}', "ambiguous"),
])
def test_list_items_rejects_malformed_server_responses(body, message):
    transport = _Transport([HTTPResponse(200, {}, body)])
    client = FabricRESTController(_TokenProvider(), workspace_id=WORKSPACE_ID, transport=transport)
    with pytest.raises(FabricRestError, match=message):
        client.list_items()
    assert len(transport.requests) == 1


@pytest.mark.parametrize("retry_after,expected", [
    ("2.5", 2.5), ("-1", 0.0), ("invalid-date", 1.0),
    ("Thu, 01 Jan 1970 00:00:00 GMT", 0.0),
])
def test_operation_retry_headers_control_poll_delay(retry_after, expected):
    transport = _Transport([
        HTTPResponse(202, {"Location": "/operations/one", "Retry-After": retry_after}),
        HTTPResponse(429, {"Retry-After": "0"}, b'{"status":"Running"}'),
        HTTPResponse(200, {}, b'{"status":"Succeeded"}'),
    ])
    delays = []
    client = FabricRESTController(
        _TokenProvider(), workspace_id=WORKSPACE_ID, transport=transport,
        sleep=delays.append, monotonic=lambda: 0,
    )
    assert client._lro_json("POST", "/items", value=b"raw") == {"status": "Succeeded"}
    assert delays == [expected, 0]
    assert transport.requests[0]["body"] == b"raw"


@pytest.mark.parametrize("body", [b"", b"null"])
def test_synchronous_empty_operation_response_is_allowed(body):
    client = FabricRESTController(
        _TokenProvider(), workspace_id=WORKSPACE_ID,
        transport=_Transport([HTTPResponse(201, {}, body)]),
    )
    assert client._lro_json("POST", "/items") == {}


def test_operation_polling_follows_response_location_and_enforces_deadline():
    transport = _Transport([
        HTTPResponse(202, {"Location": "/operations/one"}),
        HTTPResponse(200, {"Location": "/created"}, b'{"id":"created"}'),
        HTTPResponse(200, {}, b'{"id":"created","type":"Report"}'),
    ])
    client = FabricRESTController(
        _TokenProvider(), workspace_id=WORKSPACE_ID, transport=transport,
        sleep=lambda _: None, monotonic=lambda: 0,
    )
    assert client._lro_json("POST", "/items") == {"id": "created", "type": "Report"}
    assert transport.requests[-1]["url"].endswith("/created")
    client = FabricRESTController(
        _TokenProvider(), workspace_id=WORKSPACE_ID,
        transport=_Transport([HTTPResponse(202, {"Location": "/operations/one"})]),
        lro_timeout=0, monotonic=lambda: 0,
    )
    with pytest.raises(TimeoutError, match="timed out"):
        client._lro_json("POST", "/items")
