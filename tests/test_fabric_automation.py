from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import SimpleNamespace
from urllib.parse import parse_qs
from urllib.request import Request

import pytest

import people_counter.fabric_automation as fabric_automation
import people_counter.fabric_rest as fabric_rest
from people_counter.fabric_automation import (
    AzureCliResourceTokenProvider,
    DeploymentEvidence,
    FabricApiError,
    FabricAutomationClient,
    GitHubOidcResourceTokenProvider,
    HTTPResponse,
    OperationHandle,
    make_resource_token_provider,
    redact_evidence,
)
from people_counter.fabric_rest import AzureCliTokenProvider


@dataclass
class _TokenProvider:
    token: str = "secret-token"
    calls: int = 0

    def get_token(self) -> str:
        self.calls += 1
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


@dataclass
class _Clock:
    sleeps: list[float] = field(default_factory=list)
    now: float = 0.0

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds

    def monotonic(self) -> float:
        return self.now


@dataclass
class _UrlResponse:
    body: bytes

    def read(self) -> bytes:
        return self.body

    def __enter__(self) -> _UrlResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


def _client(
    responses: list[HTTPResponse],
    *,
    token_provider: object | None = None,
    **kwargs,
) -> tuple[FabricAutomationClient, _Transport]:
    transport = _Transport(responses)
    client = FabricAutomationClient(
        token_provider or _TokenProvider(),
        transport=transport,
        api_root="https://fabric.example/v1",
        sleep=lambda _: None,
        monotonic=lambda: 0.0,
        **kwargs,
    )
    return client, transport


# --- construction -----------------------------------------------------------


def test_non_https_api_root_is_rejected() -> None:
    with pytest.raises(ValueError, match="must use HTTPS"):
        FabricAutomationClient(
            _TokenProvider(),
            transport=_Transport([]),
            api_root="http://fabric.example/v1",
        )


def test_negative_max_retries_is_rejected() -> None:
    with pytest.raises(ValueError, match="max_retries"):
        FabricAutomationClient(
            _TokenProvider(),
            transport=_Transport([]),
            max_retries=-1,
        )


def test_default_headers_are_applied_but_cannot_override_authorization() -> None:
    with pytest.raises(ValueError, match="Authorization"):
        FabricAutomationClient(
            _TokenProvider(),
            transport=_Transport([]),
            default_headers={"authorization": "unsafe"},
        )
    client, transport = _client(
        [HTTPResponse(200, {}, b"{}")],
        default_headers={"x-ms-version": "2023-11-03"},
    )

    client.request_json("GET", "/path")

    assert transport.requests[0]["headers"]["x-ms-version"] == "2023-11-03"
    assert transport.requests[0]["headers"]["Authorization"]


def test_client_rejects_cross_origin_urls_before_acquiring_a_token() -> None:
    provider = _TokenProvider()
    client, transport = _client([], token_provider=provider)

    with pytest.raises(FabricApiError, match="different API origin"):
        client.request_json("GET", "https://attacker.example/collect")

    assert provider.calls == 0
    assert transport.requests == []


def test_client_rejects_non_https_absolute_syntax() -> None:
    client, _ = _client([])

    with pytest.raises(FabricApiError, match="relative or HTTPS"):
        client.request_json("GET", "http://fabric.example/v1/items")


def test_evidence_redaction_removes_nested_authentication_material() -> None:
    evidence = DeploymentEvidence(
        action="connection",
        status="failed",
        resource={
            "credentialDetails": {
                "username": "operator",
                "password": "do-not-log",
            },
            "safe": "Bearer abc.def",
        },
        error="Basic Zm9vOmJhcg== was rejected",
    ).to_dict()

    assert evidence["resource"]["credentialDetails"] == "<redacted>"
    assert evidence["resource"]["safe"] == "<redacted>"
    assert evidence["error"] == "<redacted> was rejected"
    assert redact_evidence({"api_token": "value"}) == {"api_token": "<redacted>"}


def test_request_error_redacts_submitted_nested_credentials() -> None:
    client, _ = _client(
        [
            HTTPResponse(
                400,
                {},
                b'{"error":"password super-secret was rejected"}',
            )
        ]
    )

    with pytest.raises(FabricApiError) as captured:
        client.request_json(
            "PATCH",
            "/connections/id",
            value={
                "credentialDetails": {
                    "credentials": {
                        "credentialType": "Basic",
                        "password": "super-secret",
                    }
                }
            },
        )

    assert "super-secret" not in str(captured.value)
    assert "<redacted>" in str(captured.value)


def test_resource_token_provider_validates_resource_and_redacts_repr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="HTTPS origin"):
        AzureCliResourceTokenProvider("not-a-resource")
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def run(*args: object, **kwargs: object) -> SimpleNamespace:
        calls.append((args, kwargs))
        return SimpleNamespace(stdout="access-token\n")

    monkeypatch.setattr(
        fabric_automation.subprocess,
        "run",
        run,
    )
    provider = AzureCliResourceTokenProvider("https://storage.azure.com/")

    assert provider.get_token() == "access-token"
    assert calls[0][0][0] == [
        "az",
        "account",
        "get-access-token",
        "--scope",
        "https://storage.azure.com/.default",
        "--query",
        "accessToken",
        "--output",
        "tsv",
    ]
    assert calls[0][1] == {
        "capture_output": True,
        "check": True,
        "text": True,
        "timeout": 60,
    }
    assert "access-token" not in repr(provider)


def test_resource_token_provider_uses_github_oidc_and_caches_access_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[Request] = []
    responses = [
        _UrlResponse(b'{"value":"github-assertion"}'),
        _UrlResponse(b'{"access_token":"azure-access-token","expires_in":3600}'),
    ]

    def open_url(request: Request, *, timeout: int) -> _UrlResponse:
        assert timeout == 60
        requests.append(request)
        return responses.pop(0)

    monkeypatch.setattr(fabric_automation, "urlopen", open_url)
    monkeypatch.setattr(fabric_automation.time, "time", lambda: 1000.0)
    provider = make_resource_token_provider(
        "https://storage.azure.com/",
        env={
            "ACTIONS_ID_TOKEN_REQUEST_URL": (
                "https://pipelinesghubeus2.actions.githubusercontent.com/"
                "oidc?api-version=2.0"
            ),
            "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "github-request-token",
            "ARM_CLIENT_ID": "88fb936a-6e03-42f2-9ca9-6a0869e1523e",
            "ARM_TENANT_ID": "b8005d83-0703-4f94-8749-5c3e02f07bba",
        },
    )

    assert isinstance(provider, GitHubOidcResourceTokenProvider)
    assert provider.get_token() == "azure-access-token"
    assert provider.get_token() == "azure-access-token"
    assert len(requests) == 2
    assertion_request = requests[0]
    assert assertion_request.get_header("Authorization") == (
        "Bearer github-request-token"
    )
    assert assertion_request.full_url.endswith(
        "audience=api%3A%2F%2FAzureADTokenExchange"
    )
    token_request = requests[1]
    token_form = parse_qs(token_request.data.decode())
    assert token_form == {
        "client_assertion": ["github-assertion"],
        "client_assertion_type": [
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
        ],
        "client_id": ["88fb936a-6e03-42f2-9ca9-6a0869e1523e"],
        "grant_type": ["client_credentials"],
        "scope": ["https://storage.azure.com/.default"],
    }
    assert "github-request-token" not in repr(provider)
    assert "azure-access-token" not in repr(provider)


@pytest.mark.parametrize(
    "request_url",
    [
        "https://token.actions.githubusercontent.com/oidc",
        "https://pipelines.evil.actions.githubusercontent.com/oidc",
        "https://pipelines.actions.githubusercontent.com.evil.example/oidc",
    ],
)
def test_resource_token_provider_rejects_non_pipeline_origins(
    request_url: str,
) -> None:
    with pytest.raises(ValueError, match="unsupported origin"):
        GitHubOidcResourceTokenProvider(
            resource="https://storage.azure.com/",
            client_id="88fb936a-6e03-42f2-9ca9-6a0869e1523e",
            tenant_id="b8005d83-0703-4f94-8749-5c3e02f07bba",
            request_url=request_url,
            request_token="github-request-token",
        )


def test_resource_token_provider_falls_back_and_rejects_partial_oidc() -> None:
    assert isinstance(
        make_resource_token_provider("https://storage.azure.com/", env={}),
        AzureCliResourceTokenProvider,
    )
    with pytest.raises(FabricApiError, match="GitHub OIDC environment"):
        make_resource_token_provider(
            "https://storage.azure.com/",
            env={"ACTIONS_ID_TOKEN_REQUEST_URL": "https://example.invalid"},
        )
    with pytest.raises(FabricApiError, match="Azure workload identity"):
        make_resource_token_provider(
            "https://storage.azure.com/",
            env={
                "ACTIONS_ID_TOKEN_REQUEST_URL": (
                    "https://pipelines.actions.githubusercontent.com/oidc"
                ),
                "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "token",
            },
        )


# --- TokenProvider structural compatibility --------------------------------


def test_azure_cli_token_provider_satisfies_token_provider_protocol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        fabric_rest.subprocess,
        "run",
        lambda *_a, **_kw: SimpleNamespace(stdout="cli-token\n"),
    )
    client, transport = _client(
        [HTTPResponse(200, {}, b"{}")],
        token_provider=AzureCliTokenProvider(),
    )

    client.request_json("GET", "/items")

    assert transport.requests[0]["headers"]["Authorization"] == "Bearer cli-token"


# --- JSON request helper -----------------------------------------------------


def test_request_json_authenticates_and_serializes_canonical_json() -> None:
    client, transport = _client([HTTPResponse(200, {}, b'{"ok":true}')])

    result = client.request_json("POST", "/items", value={"z": 1, "a": 2})

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


def test_request_json_returns_empty_mapping_for_empty_body() -> None:
    client, _ = _client([HTTPResponse(200, {}, b"")])

    assert client.request_json("GET", "/items") == {}


def test_request_json_rejects_non_object_json() -> None:
    client, _ = _client([HTTPResponse(200, {}, b"[1,2]")])

    with pytest.raises(FabricApiError, match="must be an object"):
        client.request_json("GET", "/items")


def test_request_json_rejects_invalid_json() -> None:
    client, _ = _client([HTTPResponse(200, {}, b"not-json")])

    with pytest.raises(FabricApiError, match="invalid JSON"):
        client.request_json("GET", "/items")


def test_request_redacts_token_from_error_body() -> None:
    client, _ = _client([HTTPResponse(403, {}, b"denied secret-token")])

    with pytest.raises(FabricApiError, match="Fabric HTTP 403") as captured:
        client.request_json("GET", "/items")

    assert "secret-token" not in str(captured.value)
    assert "<redacted>" in str(captured.value)


# --- bounded 429/503 retry handling -----------------------------------------


def test_request_retries_numeric_retry_after_on_429_then_succeeds() -> None:
    transport = _Transport(
        [
            HTTPResponse(429, {"Retry-After": "2"}, b""),
            HTTPResponse(200, {}, b'{"ok":true}'),
        ]
    )
    clock = _Clock()
    client = FabricAutomationClient(
        _TokenProvider(),
        transport=transport,
        api_root="https://fabric.example/v1",
        sleep=clock.sleep,
        monotonic=clock.monotonic,
        max_retries=3,
    )

    assert client.request_json("GET", "/items") == {"ok": True}
    assert clock.sleeps == [2.0]
    assert len(transport.requests) == 2


def test_request_retries_on_503_with_default_retry_after() -> None:
    transport = _Transport(
        [
            HTTPResponse(503, {}, b""),
            HTTPResponse(200, {}, b"{}"),
        ]
    )
    clock = _Clock()
    client = FabricAutomationClient(
        _TokenProvider(),
        transport=transport,
        api_root="https://fabric.example/v1",
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )

    assert client.request_json("GET", "/items") == {}
    assert clock.sleeps == [1.0]


def test_request_does_not_retry_non_retryable_status() -> None:
    client, transport = _client([HTTPResponse(404, {}, b"missing")])

    with pytest.raises(FabricApiError, match="Fabric HTTP 404"):
        client.request_json("GET", "/items")

    assert len(transport.requests) == 1


def test_request_raises_after_exhausting_bounded_retries() -> None:
    responses = [HTTPResponse(429, {"Retry-After": "0"}, b"throttled")] * 3
    clock = _Clock()
    client = FabricAutomationClient(
        _TokenProvider(),
        transport=_Transport(responses),
        api_root="https://fabric.example/v1",
        sleep=clock.sleep,
        monotonic=clock.monotonic,
        max_retries=2,
    )

    with pytest.raises(FabricApiError, match="after 3 attempt"):
        client.request_json("GET", "/items")

    assert len(clock.sleeps) == 2


def test_request_fetches_a_fresh_token_for_every_attempt() -> None:
    token_provider = _TokenProvider()
    client, _ = _client(
        [
            HTTPResponse(429, {"Retry-After": "0"}, b""),
            HTTPResponse(200, {}, b"{}"),
        ],
        token_provider=token_provider,
    )

    client.request_json("GET", "/items")

    assert token_provider.calls == 2


# --- _retry_after_seconds parsing -------------------------------------------


def test_retry_after_seconds_defaults_when_header_missing() -> None:
    assert fabric_automation._retry_after_seconds({}) == 1.0


def test_retry_after_seconds_parses_numeric_header() -> None:
    assert fabric_automation._retry_after_seconds({"Retry-After": "2.5"}) == 2.5


def test_retry_after_seconds_clamps_negative_numeric_header() -> None:
    assert fabric_automation._retry_after_seconds({"Retry-After": "-5"}) == 0.0


def test_retry_after_seconds_falls_back_on_unparseable_header() -> None:
    assert (
        fabric_automation._retry_after_seconds({"Retry-After": "not-a-value"}) == 1.0
    )


def test_retry_after_seconds_clamps_past_http_date() -> None:
    assert (
        fabric_automation._retry_after_seconds(
            {"Retry-After": "Mon, 01 Jan 2000 00:00:00 GMT"}
        )
        == 0.0
    )


# --- pagination --------------------------------------------------------------


def test_list_pages_follows_continuation_uri() -> None:
    client, transport = _client(
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

    assert client.list_pages("/items") == [{"id": "one"}, {"id": "two"}]
    assert transport.requests[1]["url"] == "https://fabric.example/next"


def test_list_pages_rejects_non_string_continuation() -> None:
    client, _ = _client(
        [HTTPResponse(200, {}, b'{"value":[],"continuationUri":1}')]
    )

    with pytest.raises(FabricApiError, match="continuation URI"):
        client.list_pages("/items")


def test_list_pages_rejects_non_object_items() -> None:
    client, _ = _client([HTTPResponse(200, {}, b'{"value":[1]}')])

    with pytest.raises(FabricApiError, match="list of objects"):
        client.list_pages("/items")


# --- OperationHandle ----------------------------------------------------------


def test_operation_handle_rejects_empty_id() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        OperationHandle("")


def test_operation_handle_from_headers_prefers_operation_id() -> None:
    handle = OperationHandle.from_headers(
        {"x-ms-operation-id": "op-1", "Location": "/operations/other"}
    )
    assert handle == OperationHandle("op-1")
    assert handle.location() == "/operations/op-1"


def test_operation_handle_from_headers_falls_back_to_location() -> None:
    handle = OperationHandle.from_headers({"Location": "/operations/op-2/"})
    assert handle == OperationHandle("op-2")


def test_operation_handle_from_headers_returns_none_when_synchronous() -> None:
    assert OperationHandle.from_headers({}) is None


# --- submit_operation / run_operation / resume_operation ---------------------


def test_submit_operation_returns_none_handle_for_synchronous_response() -> None:
    client, _ = _client([HTTPResponse(200, {}, b'{"id":"created"}')])

    handle, result = client.submit_operation("POST", "/items", value={})

    assert handle is None
    assert result == {"id": "created"}


def test_submit_operation_returns_handle_for_async_response() -> None:
    client, _ = _client(
        [HTTPResponse(202, {"x-ms-operation-id": "op-9"}, b"")]
    )

    handle, result = client.submit_operation("POST", "/items", value={})

    assert handle == OperationHandle("op-9")
    assert result == {}


def test_run_operation_returns_succeeded_evidence_without_polling() -> None:
    client, _ = _client([HTTPResponse(200, {}, b'{"id":"created"}')])

    evidence = client.run_operation("POST", "/items", action="create-item", value={})

    assert evidence == DeploymentEvidence(
        action="create-item",
        status="succeeded",
        resource={"id": "created"},
    )


def test_list_pages_supports_endpoint_specific_value_key() -> None:
    client, _ = _client(
        [
            HTTPResponse(
                200,
                {},
                b'{"libraries":[{"name":"people_counter.whl"}]}',
            )
        ]
    )

    assert client.list_pages("/libraries", value_key="libraries") == [
        {"name": "people_counter.whl"}
    ]


def test_list_pages_maps_explicit_empty_error_code_to_empty_list() -> None:
    client, transport = _client(
        [
            HTTPResponse(
                404,
                {},
                b'{"errorCode":"EnvironmentLibrariesNotFound"}',
            )
        ]
    )

    assert client.list_pages(
        "/libraries",
        value_key="libraries",
        empty_error_codes=("EnvironmentLibrariesNotFound",),
    ) == []
    assert transport.requests[0]["method"] == "GET"


def test_list_pages_rejects_unexpected_error_code() -> None:
    client, _ = _client(
        [HTTPResponse(404, {}, b'{"errorCode":"WorkspaceNotFound"}')]
    )

    with pytest.raises(
        FabricApiError,
        match=(
            "Fabric list returned HTTP 404 with unexpected error code "
            "'WorkspaceNotFound'"
        ),
    ):
        client.list_pages(
            "/libraries",
            value_key="libraries",
            empty_error_codes=("EnvironmentLibrariesNotFound",),
        )


def test_list_pages_with_empty_error_codes_preserves_success_response() -> None:
    client, transport = _client(
        [HTTPResponse(200, {}, b'{"libraries":[{"name":"current.whl"}]}')]
    )

    assert client.list_pages(
        "/libraries",
        value_key="libraries",
        empty_error_codes=("EnvironmentLibrariesNotFound",),
    ) == [{"name": "current.whl"}]
    assert transport.requests[0]["method"] == "GET"


def test_synchronous_created_location_is_not_treated_as_lro() -> None:
    client, transport = _client(
        [
            HTTPResponse(
                201,
                {"Location": "https://fabric.example/v1/schedules/schedule-1"},
                b'{"id":"schedule-1","enabled":false}',
            )
        ]
    )

    evidence = client.run_operation(
        "POST",
        "/schedules",
        action="create-schedule",
        value={"enabled": False},
    )

    assert evidence.status == "succeeded"
    assert evidence.operation_id is None
    assert evidence.resource == {"id": "schedule-1", "enabled": False}
    assert len(transport.requests) == 1


def test_run_operation_polls_and_reads_resource_location() -> None:
    client, transport = _client(
        [
            HTTPResponse(202, {"x-ms-operation-id": "op-1"}, b""),
            HTTPResponse(
                200,
                {"Retry-After": "0"},
                b'{"status":"Succeeded","resourceLocation":"/result"}',
            ),
            HTTPResponse(200, {}, b'{"id":"created"}'),
        ],
        max_retries=1,
    )

    evidence = client.run_operation("POST", "/items", action="create-item", value={})

    assert evidence.status == "succeeded"
    assert evidence.operation_id == "op-1"
    assert evidence.resource == {"id": "created"}
    assert evidence.attempts == 1
    assert [request["url"] for request in transport.requests] == [
        "https://fabric.example/v1/items",
        "https://fabric.example/v1/operations/op-1",
        "https://fabric.example/v1/result",
    ]


def test_run_operation_prefers_result_location_for_definition_lro() -> None:
    client, transport = _client(
        [
            HTTPResponse(202, {"x-ms-operation-id": "op-definition"}, b""),
            HTTPResponse(
                200,
                {},
                (
                    b'{"status":"Succeeded",'
                    b'"resourceLocation":"/items/item-1",'
                    b'"resultLocation":"/operations/op-definition/result"}'
                ),
            ),
            HTTPResponse(
                200,
                {},
                b'{"definition":{"format":"TMDL","parts":[]}}',
            ),
        ]
    )

    evidence = client.run_operation(
        "POST",
        "/items/item-1/getDefinition",
        action="export-definition",
    )

    assert evidence.resource == {
        "definition": {"format": "TMDL", "parts": []}
    }
    assert transport.requests[-1]["url"].endswith(
        "/operations/op-definition/result"
    )


def test_get_definition_uses_conventional_operation_result_path() -> None:
    client, transport = _client(
        [
            HTTPResponse(202, {"x-ms-operation-id": "op-definition"}, b""),
            HTTPResponse(200, {}, b'{"status":"Succeeded"}'),
            HTTPResponse(
                200,
                {},
                b'{"definition":{"format":"TMDL","parts":[]}}',
            ),
        ]
    )

    evidence = client.run_operation(
        "POST",
        "/items/item-1/getDefinition?format=TMDL",
        action="export-definition",
    )

    assert evidence.resource == {
        "definition": {"format": "TMDL", "parts": []}
    }
    assert transport.requests[-1]["url"].endswith(
        "/operations/op-definition/result"
    )


def test_run_operation_surfaces_terminal_failure_as_evidence_not_exception() -> None:
    client, _ = _client(
        [
            HTTPResponse(202, {"x-ms-operation-id": "op-1"}, b""),
            HTTPResponse(200, {}, b'{"status":"Failed","detail":"boom"}'),
        ]
    )

    evidence = client.run_operation("POST", "/items", action="create-item", value={})

    assert evidence.status == "failed"
    assert evidence.operation_id == "op-1"
    assert evidence.error is not None and "operation failed" in evidence.error
    assert evidence.resource == {"status": "Failed", "detail": "boom"}


def test_run_operation_times_out_as_bounded_evidence() -> None:
    transport = _Transport(
        [
            HTTPResponse(202, {"x-ms-operation-id": "op-1"}, b""),
            HTTPResponse(200, {}, b'{"status":"Running"}'),
        ]
    )
    deadlines = iter([0.0, 0.0, 100.0])
    client = FabricAutomationClient(
        _TokenProvider(),
        transport=transport,
        api_root="https://fabric.example/v1",
        sleep=lambda _: None,
        monotonic=lambda: next(deadlines),
        lro_timeout=10.0,
    )

    evidence = client.run_operation("POST", "/items", action="create-item", value={})

    assert evidence.status == "failed"
    assert evidence.error == "timed out waiting for Fabric operation"
    assert evidence.operation_id == "op-1"


def test_resume_operation_polls_from_bare_operation_id() -> None:
    client, transport = _client(
        [HTTPResponse(200, {}, b'{"status":"Succeeded"}')]
    )

    evidence = client.resume_operation(OperationHandle("op-7"), action="resume-item")

    assert evidence == DeploymentEvidence(
        action="resume-item",
        status="succeeded",
        operation_id="op-7",
        resource={"status": "Succeeded"},
        attempts=1,
    )
    assert transport.requests == [
        {
            "method": "GET",
            "url": "https://fabric.example/v1/operations/op-7",
            "headers": {
                "Accept": "application/json",
                "Authorization": "Bearer secret-token",
            },
            "body": None,
            "timeout": 30.0,
        }
    ]


def test_evidence_operation_handle_round_trips_for_resume() -> None:
    evidence = DeploymentEvidence(
        action="create-item", status="failed", operation_id="op-3"
    )

    handle = evidence.operation_handle()

    assert handle == OperationHandle("op-3")


def test_evidence_operation_handle_is_none_without_operation_id() -> None:
    evidence = DeploymentEvidence(action="create-item", status="succeeded")

    assert evidence.operation_handle() is None


# --- DeploymentEvidence machine-readable JSON --------------------------------


def test_deployment_evidence_to_dict_and_json_are_sorted_and_complete() -> None:
    evidence = DeploymentEvidence(
        action="create-item",
        status="succeeded",
        operation_id="op-1",
        resource={"id": "created"},
        attempts=2,
    )

    assert evidence.to_dict() == {
        "action": "create-item",
        "status": "succeeded",
        "operationId": "op-1",
        "resource": {"id": "created"},
        "error": None,
        "attempts": 2,
    }
    assert evidence.to_json() == (
        '{"action": "create-item", "attempts": 2, "error": null, '
        '"operationId": "op-1", "resource": {"id": "created"}, '
        '"status": "succeeded"}'
    )


def test_deployment_evidence_to_json_handles_missing_resource() -> None:
    evidence = DeploymentEvidence(action="create-item", status="failed", error="boom")

    assert evidence.to_dict()["resource"] is None
    assert '"resource": null' in evidence.to_json()
