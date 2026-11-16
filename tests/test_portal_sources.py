from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from azure.core.exceptions import AzureError, HttpResponseError, ServiceRequestError, ServiceResponseError
from azure.core.pipeline.policies import SansIOHTTPPolicy
from azure.core.pipeline.transport import HttpRequest, HttpResponse, HttpTransport
from azure.storage.filedatalake import DataLakeServiceClient as NativeServiceClient

from people_counter.portal_sources import AdlsFootageSource, SelectedFootageInput, source_identity
from people_counter.manifest import CatalogError


@pytest.fixture
def source(monkeypatch):
    filesystem = Mock()
    credential = Mock()
    azure = Mock()
    azure.require_identity.return_value = {"tenant_id": "tenant", "account": "operator@example"}
    azure.credential.return_value = credential
    service = Mock()
    service.get_file_system_client.return_value = filesystem
    factory = Mock(return_value=service)
    monkeypatch.setattr("azure.storage.filedatalake.DataLakeServiceClient", factory)
    result = AdlsFootageSource({
        "kind": "adls", "account": "footage123", "filesystem": "recordings",
        "subscription": "subscription", "root": "entrance",
    }, azure)
    return result, filesystem, azure, factory


def test_adls_source_identity_is_storage_qualified_and_stable_across_browsing():
    first = {"account": "footage123", "filesystem": "recordings", "root": "entrance"}
    assert source_identity(first) == source_identity(dict(first))
    for key, value in (("account", "other123"), ("filesystem", "other"), ("root", "other")):
        assert source_identity(first) != source_identity({**first, key: value})


def test_adls_source_lazily_uses_sdk_credentials_and_original_paths(source):
    adapter, filesystem, azure, factory = source
    azure.credential.assert_not_called()
    assert adapter.remote_path("day/clip.mp4") == "entrance/day/clip.mp4"
    assert adapter.relative_video("day/clip.mp4").endswith("/day/clip.mp4")
    filesystem.get_file_client.return_value.get_file_properties.return_value = SimpleNamespace(size=7, etag='"version"')
    assert adapter.properties("day/clip.mp4") == {"size": 7, "etag": '"version"'}
    azure.credential.assert_called_once_with(azure.require_identity.return_value)
    assert factory.call_args.kwargs["credential"] is azure.credential.return_value
    filesystem.get_file_client.assert_called_with("entrance/day/clip.mp4")
    filesystem.get_file_client.return_value.download_file.assert_not_called()


def test_forked_decoder_uses_its_own_http_session_without_closing_the_parent_pool(source, monkeypatch):
    import multiprocessing
    import os
    import requests

    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("Remote decoding requires fork")
    adapter, _, _, factory = source
    adapter.client
    transport = factory.call_args.kwargs.get("transport")
    assert isinstance(transport, HttpTransport)
    original_session = requests.Session
    sessions = []

    def session():
        created = original_session()
        close = Mock(wraps=created.close)
        response = requests.Response()
        response.status_code = 200
        response.headers["x-owner-pid"] = str(os.getpid())
        response._content = b""
        response.raw = SimpleNamespace(enforce_content_length=False)
        request = Mock(return_value=response)
        monkeypatch.setattr(created, "close", close)
        monkeypatch.setattr(created, "request", request)
        sessions.append((os.getpid(), close, request))
        return created

    monkeypatch.setattr(requests, "Session", session)
    transport.close()
    assert not sessions
    with transport:
        transport.open()
        assert len(sessions) == 1
        response = transport.send(
            HttpRequest("HEAD", "https://footage123.blob.core.windows.net/recordings/clip.mp4"),
            connection_timeout=2, read_timeout=3, stream=True,
        )
        assert response.status_code == 200
        assert sessions[0][2].call_args.kwargs["timeout"] == (2, 3)
        assert sessions[0][2].call_args.kwargs["stream"] is True
        context = multiprocessing.get_context("fork")
        parent, child = context.Pipe(duplex=False)

        def decode():
            with child:
                response = transport.send(HttpRequest("HEAD", "https://footage123.blob.core.windows.net/recordings/clip.mp4"))
                transport.close()
                child.send((os.getpid(), len(sessions), sessions[-1][0],
                            sessions[0][1].call_count, sessions[-1][1].call_count,
                            response.headers["x-owner-pid"], sessions[-1][2].call_args.kwargs["timeout"]))

        process = context.Process(target=decode)
        with parent, child:
            process.start()
            child.close()
            try:
                assert parent.poll(5), "Decoder did not establish its process-local session"
                pid, count, owner, parent_closes, child_closes, response_owner, timeout = parent.recv()
                process.join(5)
                assert process.exitcode == 0
                assert pid != os.getpid()
                assert count == 2 and owner == pid
                assert response_owner == str(pid)
                assert timeout == (10, 10)
                assert parent_closes == 0 and child_closes == 1
            finally:
                if process.is_alive():
                    process.kill()
                process.join()
                process.close()
        transport.open()
        assert len(sessions) == 1
        sessions[0][1].assert_not_called()
    sessions[0][1].assert_called_once()
    with pytest.raises(ValueError, match="already been closed"):
        transport.open()


@pytest.fixture
def metadata_sdk(source, monkeypatch):
    class Response(HttpResponse):
        def __init__(self, request, status):
            super().__init__(request, None)
            self.status_code = status
            self.reason = "OK" if status == 200 else "Storage failure"
            self.content_type = "application/xml"
            self.headers = {
                "Content-Length": "7", "ETag": '"version"',
                "Last-Modified": "Fri, 03 Apr 2026 12:30:00 GMT",
                "x-ms-request-id": "metadata-request-123",
            }
            if status != 200:
                self.headers["x-ms-error-code"] = "ServerBusy" if status == 503 else "AuthorizationPermissionMismatch"

        def body(self):
            return b"<Error><Message>PRIVATE_RESPONSE_SECRET</Message></Error>"

    class Transport(HttpTransport):
        status = 200
        error = None

        def __init__(self):
            self.calls = []
            self.sleeps = []

        def open(self):
            pass

        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

        def send(self, request, **kwargs):
            self.calls.append((request, kwargs))
            if self.error is not None:
                raise self.error
            return Response(request, self.status)

    adapter, _, azure, _ = source
    transport = Transport()

    def fail_backoff(*args, **kwargs):
        transport.sleeps.append("backoff")
        raise AssertionError("Metadata reads must not enter retry backoff")

    monkeypatch.setattr("azure.storage.filedatalake._shared.policies.StorageRetryPolicy.sleep", fail_backoff)
    monkeypatch.setattr("azure.storage.blob._shared.policies.StorageRetryPolicy.sleep", fail_backoff)
    azure.credential.return_value = None
    azure.policy.return_value = SansIOHTTPPolicy()
    monkeypatch.setattr(
        "azure.storage.filedatalake.DataLakeServiceClient",
        lambda *args, **kwargs: NativeServiceClient(*args, **{**kwargs, "transport": transport}),
    )
    yield adapter, transport
    adapter.reset()


def test_native_metadata_read_preserves_result_and_bounded_transport_timeouts(metadata_sdk):
    adapter, transport = metadata_sdk
    assert adapter.properties("day/clip.mp4") == {"size": 7, "etag": '"version"'}
    assert len(transport.calls) == 1
    request, options = transport.calls[0]
    assert request.method == "HEAD"
    assert request.url.endswith("/recordings/entrance/day/clip.mp4")
    assert options["connection_timeout"] == options["read_timeout"] == 10
    assert transport.sleeps == []


@pytest.mark.parametrize("failure", [503, 403, "connect", "read"])
def test_native_metadata_failures_do_not_retry_and_surface_safe_storage_details(
    metadata_sdk, caplog, failure,
):
    adapter, transport = metadata_sdk
    if isinstance(failure, int):
        transport.status = failure
    else:
        kind = ServiceRequestError if failure == "connect" else ServiceResponseError
        transport.error = kind("PRIVATE_TRANSPORT_SECRET")
    with pytest.raises(ValueError) as raised:
        adapter.properties("day/clip.mp4")
    detail = str(raised.value)
    assert "day/clip.mp4" in detail
    assert isinstance(raised.value.__cause__, AzureError)
    assert type(raised.value.__cause__).__name__ in detail
    if isinstance(failure, int):
        assert f"HTTP {failure}" in detail
        assert ("ServerBusy" if failure == 503 else "AuthorizationPermissionMismatch") in detail
        assert "metadata-request-123" in detail
    assert detail in caplog.text
    assert "PRIVATE_" not in detail + caplog.text
    assert len(transport.calls) == 1
    assert transport.sleeps == []


def test_metadata_error_uses_sdk_code_when_response_headers_are_unavailable(source, caplog):
    from azure.storage.blob import StorageErrorCode

    adapter, filesystem, _, _ = source
    error = HttpResponseError("PRIVATE_RESPONSE_SECRET")
    error.error_code = StorageErrorCode.SERVER_BUSY
    filesystem.get_file_client.return_value.get_file_properties.side_effect = error
    with pytest.raises(ValueError) as raised:
        adapter.properties("day/clip.mp4")
    assert "code ServerBusy" in str(raised.value)
    assert raised.value.__cause__ is error
    assert "PRIVATE_" not in str(raised.value) + caplog.text


def test_source_reset_closes_owner_not_borrowed_children_or_credential(source):
    adapter, filesystem, azure, factory = source
    adapter.reset()
    factory.assert_not_called()
    assert adapter.client is filesystem
    adapter.reset()
    adapter.reset()
    factory.return_value.close.assert_called_once()
    filesystem.close.assert_not_called()
    azure.credential.return_value.close.assert_not_called()


def test_source_filesystem_failure_closes_new_service(source):
    adapter, _, _, factory = source
    factory.return_value.get_file_system_client.side_effect = RuntimeError("Filesystem unavailable")
    with pytest.raises(RuntimeError, match="Filesystem unavailable"):
        adapter.client
    factory.return_value.close.assert_called_once()


def test_source_reset_waits_for_active_borrower(source):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    adapter, _, _, factory = source
    entered, release, resetting = Event(), Event(), Event()
    def borrow():
        with adapter.lease():
            assert adapter.client is source[1]
            entered.set()
            assert release.wait(5)
    def reset():
        resetting.set()
        adapter.reset()
    with ThreadPoolExecutor(max_workers=2) as pool:
        borrower = pool.submit(borrow)
        try:
            assert entered.wait(2)
            cleanup = pool.submit(reset)
            assert resetting.wait(2)
            assert not cleanup.done()
            factory.return_value.close.assert_not_called()
        finally:
            release.set()
        borrower.result()
        cleanup.result()
    factory.return_value.close.assert_called_once()


def test_source_reset_closes_native_service_transport(source, monkeypatch):
    from azure.core.pipeline.policies import SansIOHTTPPolicy
    from azure.core.pipeline.transport import HttpTransport

    class Transport(HttpTransport):
        close_calls = 0
        def open(self):
            pass
        def close(self):
            self.close_calls += 1
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.close()
        def send(self, *args, **kwargs):
            raise AssertionError("No network allowed")
    adapter, _, azure, _ = source
    transport = Transport()
    azure.policy.return_value = SansIOHTTPPolicy()
    monkeypatch.setattr(
        "azure.storage.filedatalake.DataLakeServiceClient",
        lambda *args, **kwargs: NativeServiceClient(*args, **{**kwargs, "transport": transport}),
    )
    adapter.client
    adapter.reset()
    assert transport.close_calls == 2
    adapter.reset()
    assert transport.close_calls == 2


@pytest.mark.parametrize("path", ["../clip.mp4", "/clip.mp4", "day/../../clip.mp4", "https://other/clip.mp4"])
def test_adls_source_rejects_traversal_and_urls(source, path):
    with pytest.raises(CatalogError):
        source[0].canonical(path)


def test_adls_recursive_selection_enforces_exact_limit_without_truncation(source):
    adapter, filesystem, _, _ = source
    def path(index):
        return SimpleNamespace(name=f"entrance/day/{index}.mp4", is_directory=False)
    filesystem.get_paths.return_value = iter(path(index) for index in range(10000))
    assert len(adapter.videos("day")) == 10000
    filesystem.get_paths.return_value = iter(path(index) for index in range(10001))
    with pytest.raises(ValueError, match="10,000"):
        adapter.videos("day")


def test_adls_browsing_is_shallow_paged_and_reads_no_video_content(source):
    adapter, filesystem, _, _ = source
    class Pages:
        continuation_token = "next-page"
        def __iter__(self):
            return self
        def __next__(self):
            return iter([SimpleNamespace(
                name="entrance/day/clip.mp4", is_directory=False, content_length=99,
                last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )])
    filesystem.get_paths.return_value.by_page.return_value = Pages()
    rows, token = adapter.page("day", "previous-page")
    assert token == "next-page"
    assert rows[0]["path"] == "day/clip.mp4"
    assert rows[0]["size_bytes"] == 99
    filesystem.get_paths.assert_called_once_with(path="entrance/day", recursive=False, max_results=100)
    filesystem.get_paths.return_value.by_page.assert_called_once_with(continuation_token="previous-page")
    filesystem.get_file_client.assert_not_called()


@pytest.mark.parametrize("change", [{"size": 14}, {"etag": '"changed"'}])
def test_selected_remote_input_rejects_changed_size_or_etag(change):
    adapter = AdlsFootageSource.__new__(AdlsFootageSource)
    adapter.logical_prefix = "adls-test/"
    properties = {"size": 13, "etag": '"original"'}
    adapter.properties = lambda video: dict(properties)
    inputs = SelectedFootageInput(adapter, ["clip.mp4"], {"clip.mp4": dict(properties)})
    observed = inputs.stat("adls-test/clip.mp4")
    assert (observed.size, observed.version) == (13, '"original"')
    assert inputs.local_path("adls-test/clip.mp4") is None
    properties.update(change)
    with pytest.raises(ValueError, match="changed after selection"):
        inputs.stat("adls-test/clip.mp4")


def test_local_input_version_tracks_real_file_changes(tmp_path):
    import os
    from people_counter.portal_service import LocalFootageSource

    root = tmp_path / "videos"
    root.mkdir()
    video = root / "clip.mp4"
    video.write_bytes(b"same size")
    inputs = SelectedFootageInput(LocalFootageSource(root), ["clip.mp4"])
    before = inputs.stat("videos/clip.mp4")
    metadata = video.stat()
    os.utime(video, ns=(metadata.st_atime_ns, metadata.st_mtime_ns + 1000000))
    after = inputs.stat("videos/clip.mp4")
    assert after.size == before.size
    assert after.version != before.version
