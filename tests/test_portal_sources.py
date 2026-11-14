from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

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


def test_adls_source_uses_explicit_cli_credentials_and_original_paths(source):
    adapter, filesystem, azure, factory = source
    azure.credential.assert_called_once_with(azure.require_identity.return_value, subscription="subscription")
    assert factory.call_args.kwargs["credential"] is azure.credential.return_value
    assert adapter.remote_path("day/clip.mp4") == "entrance/day/clip.mp4"
    assert adapter.relative_video("day/clip.mp4").endswith("/day/clip.mp4")
    filesystem.get_file_client.return_value.get_file_properties.return_value = SimpleNamespace(size=7, etag='"version"')
    assert adapter.properties("day/clip.mp4") == {"size": 7, "etag": '"version"'}
    filesystem.get_file_client.assert_called_with("entrance/day/clip.mp4")
    filesystem.get_file_client.return_value.download_file.assert_not_called()


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
