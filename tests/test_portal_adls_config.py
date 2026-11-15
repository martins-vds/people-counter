import pytest

from people_counter.portal_sources import AdlsFootageSource, AdlsPortalConfig


def test_environment_pins_existing_adls_root_and_configurable_upload_limit(monkeypatch):
    monkeypatch.setenv("PC_PORTAL_ADLS_ACCOUNT_URL", "https://footageaccount.dfs.core.windows.net")
    monkeypatch.setenv("PC_PORTAL_ADLS_FILESYSTEM", "videos")
    monkeypatch.setenv("PC_PORTAL_ADLS_ROOT", "intake/north")
    config = AdlsPortalConfig.from_environment()
    assert config.descriptor == {"kind": "adls", "account": "footageaccount", "filesystem": "videos", "root": "intake/north"}
    assert config.max_upload_bytes == 5 * 1024**3
    monkeypatch.setenv("PC_PORTAL_MAX_UPLOAD_BYTES", "12345")
    assert AdlsPortalConfig.from_environment().max_upload_bytes == 12345


@pytest.mark.parametrize("name,value", [
    ("PC_PORTAL_ADLS_ACCOUNT_URL", ""), ("PC_PORTAL_ADLS_ACCOUNT_URL", "https://evil.example"),
    ("PC_PORTAL_ADLS_FILESYSTEM", ""), ("PC_PORTAL_ADLS_ROOT", "../outside"),
    ("PC_PORTAL_MAX_UPLOAD_BYTES", "0"), ("PC_PORTAL_MAX_UPLOAD_BYTES", "invalid"),
])
def test_invalid_configuration_never_falls_back_to_local(monkeypatch, name, value):
    monkeypatch.setenv("PC_PORTAL_ADLS_ACCOUNT_URL", "https://footageaccount.dfs.core.windows.net")
    monkeypatch.setenv("PC_PORTAL_ADLS_FILESYSTEM", "videos")
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        AdlsPortalConfig.from_environment()


def test_portal_uses_only_configured_adls_and_opens_without_local_footage(monkeypatch, tmp_path):
    from people_counter.portal_service import PortalService
    monkeypatch.setenv("PC_PORTAL_ADLS_ACCOUNT_URL", "https://footageaccount.dfs.core.windows.net")
    monkeypatch.setenv("PC_PORTAL_ADLS_FILESYSTEM", "videos")
    service = PortalService.from_environment(tmp_path / "models", tmp_path / "state")
    assert isinstance(service.source, AdlsFootageSource)
    draft = service.new_draft("Morning")
    assert draft["source"] == service.source.descriptor
    assert service.for_source(draft["source"]).source is service.source
    with pytest.raises(ValueError, match="configured"):
        service.for_source({"kind": "local"})
    with pytest.raises(ValueError, match="configured"):
        service.for_source({**draft["source"], "account": "anotheraccount"})


def test_configured_source_reuses_approved_full_preview_for_generation_inspection(monkeypatch, tmp_path):
    from dataclasses import asdict
    from people_counter.manifest import VideoMetadata
    from people_counter.portal_remote_media import PreviewLimitError
    from people_counter.portal_service import PortalService
    from people_counter.portal_sources import SelectedFootageInput

    monkeypatch.setenv("PC_PORTAL_ADLS_ACCOUNT_URL", "https://footageaccount.dfs.core.windows.net")
    monkeypatch.setenv("PC_PORTAL_ADLS_FILESYSTEM", "videos")
    service = PortalService.from_environment(tmp_path / "models", tmp_path / "state")
    assert isinstance(service.source, AdlsFootageSource)
    observations = {"size": 8, "etag": '"approved-version"'}
    monkeypatch.setattr(service.source, "properties", lambda video: observations)
    def bounded_failure(*args, **kwargs):
        raise PreviewLimitError("Bounded decode exhausted")
    monkeypatch.setattr("people_counter.portal_remote_media.remote_probe", bounded_failure)
    monkeypatch.setattr("people_counter.portal_remote_media.remote_frame", bounded_failure)
    metadata = VideoMetadata(1920, 1080, 30)
    identity = "a" * 32
    service.store.put("source_preview", identity, {
        "video": service.source.relative_video("clip.mp4"), "seconds": 0,
        "observations": observations, "metadata": asdict(metadata),
    })
    previews = service.state / "previews"
    previews.mkdir()
    (previews / (identity + ".jpg")).write_bytes(b"approved-jpeg")
    cached = service.source.probe("clip.mp4")
    assert (cached.width, cached.height, cached.duration_seconds) == (1920, 1080, 30)
    assert service.source.frame("clip.mp4") == (b"approved-jpeg", 1920, 1080)
    monkeypatch.setattr(SelectedFootageInput, "chunks", lambda *args: iter([b"original"]))
    inputs = SelectedFootageInput(service.source, ["clip.mp4"], {"clip.mp4": observations})
    inspection = inputs.inspect(service.source.relative_video("clip.mp4"))
    assert (inspection.size_bytes, inspection.width, inspection.height) == (8, 1920, 1080)
