import pytest

from people_counter.portal_service import LocalFootageSource, PortalService, PortalStore
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


def test_store_creates_nested_state_and_reopens_durable_metadata_and_jobs(tmp_path):
    path = tmp_path / "new" / "private" / "portal.sqlite3"
    store = PortalStore(path)
    location = store.add_location("North entrance")
    camera = store.add_camera("Door", location)
    identity = store.enqueue("prepare", {"folder": "north"})
    store.event(identity, "Queued by operator")
    reopened = PortalStore(path)
    assert reopened.locations() == [{
        "id": location, "display_name": "North entrance",
        "camera_ids": [camera], "camera_count": 1,
    }]
    claimed = reopened.claim()
    assert claimed is not None
    assert claimed["payload"] == {"folder": "north"}
    assert reopened.events(identity) == ["Queued by operator"]


@pytest.mark.parametrize("kind", ["missing", "file"])
def test_local_constructor_rejects_unusable_root_with_actionable_error(tmp_path, kind):
    root = tmp_path / "footage"
    if kind == "file":
        root.write_bytes(b"not a directory")
        with pytest.raises(ValueError, match="^The configured footage source root must be a directory$"):
            LocalFootageSource(root)
    else:
        with pytest.raises(FileNotFoundError):
            LocalFootageSource(root)


def test_local_source_resolves_alias_and_preserves_manifest_relative_paths(tmp_path):
    root = tmp_path / "footage"
    (root / "north").mkdir(parents=True)
    (root / "north" / "clip.mp4").write_bytes(b"local footage")
    alias = tmp_path / "alias"
    alias.symlink_to(root, target_is_directory=True)
    source = LocalFootageSource(alias)
    assert source.resolve("north/clip.mp4") == root / "north" / "clip.mp4"
    assert source.prefix("north") == "footage/north/"
    relative = source.relative_video("north/clip.mp4")
    assert (source.video_root / relative).read_bytes() == b"local footage"


@pytest.mark.parametrize("output", ["same", "child", "parent", "alias-child"])
def test_local_constructor_refuses_overlapping_package_output(tmp_path, output):
    root = tmp_path / "footage"
    root.mkdir()
    destinations = {"same": root, "child": root / "packages", "parent": tmp_path}
    alias = tmp_path / "alias"
    alias.symlink_to(root, target_is_directory=True)
    destinations["alias-child"] = alias / "packages"
    with pytest.raises(ValueError, match="^The package output root must be separate from the source root$"):
        PortalService(root, tmp_path / "models", tmp_path / "state", output_root=destinations[output])
    assert list(root.iterdir()) == []


@pytest.mark.parametrize("custom_output", [False, True])
def test_local_constructor_reopens_state_and_preserves_drafts_and_output(tmp_path, custom_output):
    root = tmp_path / "footage"
    root.mkdir()
    state = tmp_path / "new" / "private" / "state"
    output = tmp_path / "new-output" / "private" / "packages" if custom_output else state / "packages"
    kwargs = {"output_root": output} if custom_output else {}
    service = PortalService(root, tmp_path / "models", state, **kwargs)
    draft = service.new_draft("Retained package")
    (output / "retained.txt").write_text("operator artifact")
    reopened = PortalService(root, tmp_path / "models", state, **kwargs)
    assert reopened.draft(draft["id"]) == draft
    assert (output / "retained.txt").read_text() == "operator artifact"
    alias = tmp_path / "alias"
    alias.symlink_to(root, target_is_directory=True)
    assert PortalService(alias, tmp_path / "models", state, **kwargs).draft(draft["id"]) == draft
    other = tmp_path / "different-footage"
    other.mkdir()
    with pytest.raises(ValueError, match=(
        "^This portal state belongs to a different local footage root\\. "
        "Restore that root or use a separate state directory\\.$"
    )):
        PortalService(other, tmp_path / "models", state, **kwargs)
    assert PortalService(root, tmp_path / "models", state, **kwargs).draft(draft["id"]) == draft


def test_local_constructor_reads_existing_database_and_migrates_only_unassigned_footage(tmp_path):
    root = tmp_path / "footage"
    root.mkdir()
    (root / "new.mp4").write_bytes(b"footage")
    state = tmp_path / "state"
    store = PortalStore(state / "portal.sqlite3")
    location = store.add_location("North")
    first = store.add_camera("First", location)
    second = store.add_camera("Second", location)
    for identity, camera, video in [
        ("a", first, "new.mp4"), ("b", second, "new.mp4"),
        ("c", first, "assigned.mp4"),
    ]:
        store.put("camera", identity, {"video": video, "catalog": {"camera_id": camera, "location_id": location}})
    store.assign_footage(second, ["assigned.mp4"])
    service = PortalService(root, tmp_path / "models", state)
    assert service.store.entries("footage") == [
        {"id": "assigned.mp4", "camera_id": second}, {"id": "new.mp4", "camera_id": first},
    ]
    assert service.footage_assignments() == {"new.mp4": first, "assigned.mp4": second}
    assert service.video_key("new.mp4") == "new.mp4"
    reopened = PortalService(root, tmp_path / "models", state)
    assert reopened.store.entries("footage") == service.store.entries("footage")


def test_adls_constructor_preserves_local_history_without_adopting_legacy_assignments(tmp_path):
    from people_counter.portal_azure import PortalAzureSession

    root = tmp_path / "footage"
    root.mkdir()
    state = tmp_path / "state"
    local = PortalService(root, tmp_path / "models", state)
    draft = local.new_draft("Local history")
    location = local.store.add_location("North")
    camera = local.store.add_camera("Door", location)
    local.store.put("camera", "legacy", {"video": "clip.mp4", "catalog": {"camera_id": camera, "location_id": location}})
    azure = PortalAzureSession()
    source = AdlsFootageSource({"kind": "adls", "account": "testing", "filesystem": "videos", "root": "north"}, azure)
    service = PortalService(source, tmp_path / "models", state)
    assert service.store.entries("footage") == []
    assert service.draft(draft["id"]) == draft
    assert service.source_descriptor(source.identity) == {"id": source.identity, **source.descriptor}
    assert service.azure is azure
    with pytest.raises(ValueError, match="configured ADLS source"):
        service.for_source(draft["source"])


@pytest.mark.parametrize("cloud_enabled", [False, True])
def test_constructor_cloud_permission_controls_durable_job_submission(tmp_path, cloud_enabled):
    root = tmp_path / "footage"
    root.mkdir()
    service = (
        PortalService(root, tmp_path / "models", tmp_path / "state", allow_cloud=True)
        if cloud_enabled else PortalService(root, tmp_path / "models", tmp_path / "state")
    )
    for kind in ("publish", "submit"):
        if cloud_enabled:
            identity = service.enqueue(kind, {"package_id": "retained"})
            job = service.store.job(identity)
            assert job is not None
            assert job["status"] == "QUEUED"
        else:
            with pytest.raises(ValueError, match="^Cloud actions are disabled$"):
                service.enqueue(kind, {"package_id": "retained"})
    if not cloud_enabled:
        assert service.store.jobs() == []


def test_initialized_local_portal_is_healthy_and_displays_resolved_source(tmp_path):
    from people_counter.operator_portal import create_app
    from people_counter.portal_azure import PortalAzureSession

    root = tmp_path / "footage"
    root.mkdir()
    service = PortalService(root, tmp_path / "models", tmp_path / "state")
    assert isinstance(service.azure, PortalAzureSession)
    client = create_app(service).test_client()
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json == {"status": "ok"}
    response = client.get("/?folder=")
    assert response.status_code == 200
    assert f"Local folder / {root}".encode() in response.data


@pytest.mark.parametrize("size,accepted", [
    (1, True), (5 * 1024**3, True), (5 * 1024**3 + 1, False),
])
def test_direct_adls_constructor_enforces_upload_boundary_without_environment_override(
    tmp_path, fabric_runtime_context, size, accepted,
):
    from people_counter.portal_azure import PortalAzureSession
    from tests.test_portal_intake import Cloud

    source = AdlsFootageSource({"kind": "adls", "account": "testing", "filesystem": "videos"}, PortalAzureSession())
    source._client = Cloud()
    service = PortalService(source, tmp_path / "models", tmp_path / "state")
    if accepted:
        operation = service.intake.start_upload("", "clip.mp4", size)
        assert operation["state"] == "uploading"
        assert service.intake.record(operation["id"])["size"] == size
    else:
        with pytest.raises(ValueError, match="^Upload size exceeds the configured per-file limit or is empty$"):
            service.intake.start_upload("", "clip.mp4", size)
        assert service.store.entries("intake") == []


def test_local_initialization_supports_real_manifest_generation_and_retained_package_output(tmp_path):
    import json
    import subprocess

    root = tmp_path / "footage"
    root.mkdir()
    video = "20260101T120000Z.mp4"
    subprocess.run([
        "ffmpeg", "-v", "error", "-f", "lavfi", "-i", "color=size=64x48:rate=5",
        "-t", "1", "-c:v", "mpeg4", "-metadata", "creation_time=2026-01-01T12:00:00Z", str(root / video),
    ], check=True, timeout=30)
    service = PortalService(root, tmp_path / "models", tmp_path / "state")
    location = service.store.add_location("North")
    camera = service.store.add_camera("Door", location)
    service.assign_footage(camera, [video])
    service.save_camera_line(camera, {
        "video": video, "counting_line_x1": "0", "counting_line_y1": "20",
        "counting_line_x2": "63", "counting_line_y2": "20",
    })
    identity = service.enqueue_preparation("", name="Morning")
    assert service.run_next_job()
    job = service.store.job(identity)
    assert job is not None
    assert job["status"] == "SUCCEEDED", job["error"]
    package = tmp_path / "state" / "packages" / "morning" / "v1"
    manifest = json.loads((package / "manifest-package.json").read_text())
    assert manifest["entries"][0]["video_relative_path"] == "footage/" + video
    reopened = PortalService(root, tmp_path / "models", tmp_path / "state")
    assert reopened.package_dir(identity) == package
    assert (root / video).is_file()


def test_constructor_uses_deployed_model_bytes_and_retains_approved_catalog(tmp_path):
    import hashlib

    from people_counter.backfill_operator import load_target
    from people_counter.inference_profiles import artifact_paths

    root = tmp_path / "footage"
    root.mkdir()
    models = tmp_path / "models"
    profile = "rtdetr-r18-onnx-cpu"
    hashes = {}
    for relative in artifact_paths(profile):
        artifact = models / relative
        artifact.parent.mkdir(parents=True, exist_ok=True)
        content = f"deployed artifact {relative}".encode()
        artifact.write_bytes(content)
        hashes[relative] = hashlib.sha256(content).hexdigest()
    binding = {
        "workspace_id": "11111111-1111-1111-1111-111111111111",
        "lakehouse_id": "22222222-2222-2222-2222-222222222222",
        "environment_id": "33333333-3333-3333-3333-333333333333",
        "control_sjd_id": "44444444-4444-4444-4444-444444444444",
        "intake_mode": "explicit", "storage_account": "testing",
        "filesystem": "videos", "shortcut_path": "Files/videos",
    }
    service = PortalService(root, models, tmp_path / "state")
    identity = service.enqueue("profiles", {"target": "production", "binding": binding, "profiles": [profile]})
    assert service.run_next_job()
    job = service.store.job(identity)
    assert job is not None
    assert job["status"] == "SUCCEEDED", job["error"]
    catalog = tmp_path / "state" / "backfill-catalog.json"
    assert load_target(catalog, "production", profile).profile["model_artifact_sha256"] == hashes
    original = catalog.read_bytes()
    reopened = PortalService(root, models, tmp_path / "state")
    identity = reopened.enqueue("profiles", {"target": "production", "binding": binding, "profiles": [profile]})
    assert reopened.run_next_job()
    job = reopened.store.job(identity)
    assert job is not None
    assert job["status"] == "SUCCEEDED"
    assert catalog.read_bytes() == original
