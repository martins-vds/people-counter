import copy
import io
import json
import subprocess
import urllib.error
from datetime import datetime, timezone
from unittest.mock import Mock, call
from uuid import UUID

import pytest

from scripts import deploy_sjd_operations as operations
from tests.fabric_fixtures import ROLE_IDS, TEST_TARGET


def api():
    return operations.FabricApi(TEST_TARGET, token="test-token", sleep=Mock())


@pytest.mark.parametrize("headers,expected", [
    ({"Location": "/operations/url-id"}, "url-id"),
    ({"location": "/operations/url-id", "x-ms-operation-id": "header-id"}, "header-id"),
    ({"Location": "/operations/url-id", "X-Ms-Operation-Id": "header-id"}, "header-id"),
])
def test_operation_wait_reads_the_assigned_operation_identifier(monkeypatch, headers, expected):
    client = api()
    request = Mock(return_value=(200, {}, {"status": "Succeeded"}))
    monkeypatch.setattr(client, "request", request)
    assert client.wait(headers) == expected
    request.assert_called_once_with("GET", headers.get("Location", headers.get("location")))
    client.sleep.assert_not_called()


@pytest.mark.parametrize("retry,delay", [({}, 5.0), ({"Retry-After": "0"}, 1.0), ({"retry-after": "2.5"}, 2.5)])
def test_operation_wait_obeys_a_bounded_positive_retry_delay(monkeypatch, retry, delay):
    client = api()
    request = Mock(side_effect=[
        (200, retry, {"status": "Running"}),
        (200, {}, {"status": "Succeeded"}),
    ])
    monkeypatch.setattr(client, "request", request)
    assert client.wait({"Location": "/operations/one"}) == "one"
    client.sleep.assert_called_once_with(delay)
    assert request.call_count == 2


@pytest.mark.parametrize("status", ["Failed", "Cancelled"])
def test_terminal_operation_failure_is_not_an_upload_success(monkeypatch, status):
    client = api()
    request = Mock(return_value=(200, {}, {"status": status, "error": {"message": "failed"}}))
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(operations.OperationsDeploymentError, match="operation failed"):
        client.wait({"Location": "/operations/one"})
    assert request.call_count == 1
    client.sleep.assert_not_called()


@pytest.mark.parametrize("headers", [{}, {"Location": ""}, {"Location": "/"}])
def test_operation_without_a_polling_identity_fails_before_network(monkeypatch, headers):
    client = api()
    request = Mock()
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(operations.OperationsDeploymentError, match="Location|operation ID"):
        client.wait(headers)
    request.assert_not_called()


def test_operation_wait_stops_after_the_polling_limit(monkeypatch):
    client = api()
    request = Mock(return_value=(200, {}, {"status": "Running"}))
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(operations.OperationsDeploymentError, match="timed out"):
        client.wait({"Location": "/operations/one"})
    assert request.call_count == 120
    assert client.sleep.call_args_list == [call(5.0)] * 120


@pytest.mark.parametrize("status", [200, 202])
def test_definition_reads_the_lro_result_instead_of_a_submission_ack(monkeypatch, status):
    client = api()
    definition = {"definition": {"format": "SparkJobDefinitionV2", "parts": []}}
    responses = [(status, {"Location": "/operations/one"}, definition if status == 200 else {})]
    if status == 202:
        responses.append((200, {}, definition))
    request = Mock(side_effect=responses)
    monkeypatch.setattr(client, "request", request)
    wait = Mock(return_value="assigned-operation")
    monkeypatch.setattr(client, "wait", wait)
    assert client.get_definition(ROLE_IDS["process"]) == definition
    assert request.call_args_list[0].args == (
        "POST",
        f"/workspaces/{TEST_TARGET.workspace_id}/items/{ROLE_IDS['process']}/getDefinition?format=SparkJobDefinitionV2",
    )
    if status == 202:
        assert request.call_args_list[1].args == ("GET", "/operations/assigned-operation/result")
    assert wait.call_count == (status == 202)


def test_definition_can_request_an_untyped_notebook_or_pipeline(monkeypatch):
    client = api()
    request = Mock(return_value=(200, {}, {"definition": {}}))
    monkeypatch.setattr(client, "request", request)
    client.get_definition(ROLE_IDS["control"], format_name=None)
    assert request.call_args.args[1].endswith("/getDefinition")


def payload():
    return operations.schedule_payload(
        interval_minutes=5, enabled=False, command_line_arguments="checked",
        now=datetime(2026, 11, 11, tzinfo=timezone.utc),
    )


@pytest.mark.parametrize("existing", [False, True])
def test_schedule_success_requires_an_independent_persisted_readback(monkeypatch, existing):
    client = api()
    schedule_id = str(UUID(int=90))
    expected = payload()
    observed = {"id": schedule_id, **expected}
    request = Mock(side_effect=[
        (200, {}, {"value": [{"id": schedule_id}] if existing else []}),
        (204 if existing else 201, {}, {"id": schedule_id}),
        (200, {}, observed),
    ])
    monkeypatch.setattr(client, "request", request)
    assert client.reconcile_schedule(ROLE_IDS["control"], payload=expected) == observed
    base = f"/workspaces/{TEST_TARGET.workspace_id}/items/{ROLE_IDS['control']}/jobs/sparkjob/schedules"
    assert request.call_args_list == [
        call("GET", base),
        call("PATCH" if existing else "POST", f"{base}/{schedule_id}" if existing else base, expected),
        call("GET", f"{base}/{schedule_id}"),
    ]


def test_schedule_write_ack_does_not_hide_a_different_persisted_value(monkeypatch):
    client = api()
    expected = payload()
    schedule_id = str(UUID(int=90))
    actual = copy.deepcopy(expected)
    actual["enabled"] = True
    request = Mock(side_effect=[
        (200, {}, {"value": [{"id": schedule_id}]}),
        (200, {}, {"id": schedule_id, **expected}),
        (200, {}, {"id": schedule_id, **actual}),
    ])
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(operations.OperationsDeploymentError, match="readback"):
        client.reconcile_schedule(ROLE_IDS["control"], payload=expected)
    assert request.call_count == 3


def test_created_schedule_without_a_returned_id_is_uniquely_discovered(monkeypatch):
    client = api()
    expected = payload()
    schedule_id = str(UUID(int=90))
    request = Mock(side_effect=[
        (200, {}, {"value": []}),
        (201, {}, {}),
        (200, {}, {"value": [{"id": schedule_id}]}),
        (200, {}, {"id": schedule_id, **expected}),
    ])
    monkeypatch.setattr(client, "request", request)
    result = client.reconcile_schedule(ROLE_IDS["control"], payload=expected)
    assert result == {"id": schedule_id, **expected}
    assert request.call_count == 4


def test_disabling_an_existing_schedule_preserves_required_configuration_and_arguments(monkeypatch):
    client = api()
    schedule_id = str(UUID(int=90))
    expected = payload()
    original = {"id": schedule_id, **expected, "enabled": True}
    request = Mock(side_effect=[
        (200, {}, {"value": [original]}),
        (200, {}, {"id": schedule_id, **expected}),
        (200, {}, {"id": schedule_id, **expected}),
    ])
    monkeypatch.setattr(client, "request", request)
    client.disable_schedules(ROLE_IDS["control"])
    assert request.call_args_list[1].args[2] == expected


@pytest.mark.parametrize("corrupt", [None, [], {}, 1, "", {"accessToken": None}, {"accessToken": 1}])
def test_invalid_cli_token_is_rejected_before_it_can_be_used(monkeypatch, corrupt):
    runner = Mock(return_value=json.dumps(corrupt))
    monkeypatch.setattr(operations.subprocess, "check_output", runner)
    with pytest.raises(operations.OperationsDeploymentError, match="token"):
        operations._token()


def test_cli_token_is_obtained_for_the_standard_fabric_audience(monkeypatch):
    runner = Mock(return_value='{"accessToken":"test-token"}')
    monkeypatch.setattr(operations.subprocess, "check_output", runner)
    assert operations._token() == "test-token"
    assert runner.call_args.args == ([
        "az", "account", "get-access-token", "--resource",
        "https://api.fabric.microsoft.com", "--output", "json",
    ],)
    assert runner.call_args.kwargs == {"text": True}


def test_cli_login_failure_has_no_credential_fallback(monkeypatch):
    runner = Mock(side_effect=subprocess.CalledProcessError(1, ["az"]))
    monkeypatch.setattr(operations.subprocess, "check_output", runner)
    with pytest.raises(subprocess.CalledProcessError):
        operations._token()
    runner.assert_called_once()


def test_request_surfaces_http_failures_with_the_target_path(monkeypatch):
    client = api()
    monkeypatch.setattr(
        operations.urllib.request, "urlopen",
        Mock(side_effect=urllib.error.HTTPError(
            "https://api.fabric.microsoft.com/v1/items", 403, "denied", {},
            io.BytesIO(b"not authorized"),
        )),
    )
    with pytest.raises(operations.OperationsDeploymentError, match="403: not authorized"):
        client.request("GET", "/items")


def test_process_upsert_removes_an_existing_recurring_schedule_before_definition_update(monkeypatch):
    client = api()
    item_id = ROLE_IDS["process"]
    schedule_id = str(UUID(int=90))
    monkeypatch.setattr(client, "list_sjds", lambda: [{"id": item_id, "displayName": "process"}])
    request = Mock(side_effect=[
        (200, {}, {"value": [{"id": schedule_id, "enabled": False}]}),
        (204, {}, {}),
        (200, {}, {"value": []}),
        (204, {}, {}),
    ])
    monkeypatch.setattr(client, "request", request)
    assert client.upsert_sjd("process", "process") == item_id
    base = f"/workspaces/{TEST_TARGET.workspace_id}/items/{item_id}/jobs/sparkjob/schedules"
    assert request.call_args_list[:3] == [
        call("GET", base), call("DELETE", f"{base}/{schedule_id}"), call("GET", base),
    ]
    assert request.call_args_list[-1].args[1].endswith("/updateDefinition")


def test_process_schedule_deletion_must_be_verified_before_definition_changes(monkeypatch):
    client = api()
    item_id = ROLE_IDS["process"]
    schedule_id = str(UUID(int=90))
    monkeypatch.setattr(client, "list_sjds", lambda: [{"id": item_id, "displayName": "process"}])
    request = Mock(side_effect=[
        (200, {}, {"value": [{"id": schedule_id, "enabled": False}]}),
        (204, {}, {}),
        (200, {}, {"value": [{"id": schedule_id, "enabled": False}]}),
    ])
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(operations.OperationsDeploymentError, match="on-demand"):
        client.upsert_sjd("process", "process")
    assert not any(call.args[1].endswith("/updateDefinition") for call in request.call_args_list)


@pytest.mark.parametrize("body", [b"null", b"[]", b'"text"', b"7"])
def test_nonempty_api_response_must_be_an_object_not_a_silent_empty_ack(monkeypatch, body):
    class Response:
        status = 200
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return body

    monkeypatch.setattr(operations.urllib.request, "urlopen", Mock(return_value=Response()))
    with pytest.raises(operations.OperationsDeploymentError, match="non-object JSON"):
        api().request("GET", "/items")


@pytest.mark.parametrize("payload", [None, {"z": 1, "a": 2}])
@pytest.mark.parametrize("absolute", [False, True])
def test_operations_request_preserves_method_headers_body_timeout_and_response(
    monkeypatch, payload, absolute,
):
    observed = {}

    class Response:
        status = 201
        headers = {"Location": "/operations/one", "X-Test": "preserve"}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b'{"value":"response"}'

    def request(value, *, timeout):
        observed.update(request=value, timeout=timeout)
        return Response()

    monkeypatch.setattr(operations.urllib.request, "urlopen", request)
    path = "https://api.fabric.microsoft.com/v1/items" if absolute else "/items"
    assert api().request("POST", path, payload) == (
        201, Response.headers, {"value": "response"},
    )
    sent = observed["request"]
    assert sent.get_method() == "POST"
    assert sent.full_url == "https://api.fabric.microsoft.com/v1/items"
    assert observed["timeout"] == 120
    assert sent.get_header("Accept") == "application/json"
    assert sent.get_header("Authorization") == "Bearer test-token"
    assert sent.data == (None if payload is None else b'{"a":2,"z":1}')
    assert sent.get_header("Content-type") == (None if payload is None else "application/json")


def test_empty_api_ack_is_only_a_no_content_response_not_a_json_null(monkeypatch):
    class Response:
        status = 204
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b""

    monkeypatch.setattr(operations.urllib.request, "urlopen", Mock(return_value=Response()))
    assert api().request("DELETE", "/items") == (204, {}, {})
