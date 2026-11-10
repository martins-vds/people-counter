import json
import os
import sqlite3
import threading
import shutil
import time
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("flask")

from people_counter.operator_portal import create_app
from people_counter.portal_service import PortalService, PortalStore
from people_counter.manifest import CatalogError, build_manifest_plan, load_camera_catalog, load_video_inventory
from tests.test_manifest_publisher import inspection, write_catalog
from people_counter import portal_service as portal_module
from people_counter import operator_portal as portal_web
from people_counter import backfill_operator as backfill_module
from people_counter.backfill_operator import progress_reporter, _progress


@pytest.fixture(autouse=True)
def forbid_live_cloud(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Portal tests must not construct live cloud clients")
    monkeypatch.setattr(backfill_module.OneLakeFiles, "__init__", forbidden)
    monkeypatch.setattr(portal_module.AzureDataLakeStorage, "__init__", forbidden)


@pytest.fixture
def service(tmp_path):
    source = tmp_path / "videos"
    source.mkdir()
    (source / "lobby").mkdir()
    models = tmp_path / "models"
    models.mkdir()
    return PortalService(source, models, tmp_path / "state")


@pytest.fixture
def app(service):
    app = create_app(service)
    app.config["TESTING"] = True
    return app


def csrf(client):
    client.get("/")
    with client.session_transaction() as session:
        return session["csrf"]


def test_root_and_health_need_no_cloud_access(app):
    client = app.test_client()
    assert client.get("/healthz").json == {"status": "ok"}
    response = client.get("/")
    assert response.status_code == 200
    assert b"Operator portal" in response.data
    assert b"Local disk" in response.data
    assert b"Cloud storage" in response.data


def test_post_requires_csrf_and_same_origin(app):
    client = app.test_client()
    assert client.post("/packages", data={"folder": "lobby"}).status_code == 403
    token = csrf(client)
    assert client.post(
        "/packages", data={"csrf": token, "folder": "lobby"},
        headers={"Origin": "https://untrusted.example"},
    ).status_code == 403
    assert client.post(
        "/packages", data={"csrf": token, "folder": "lobby"},
        headers={"Origin": "https://localhost"},
    ).status_code == 403
    response = client.post(
        "/packages", data={"csrf": token, "folder": "lobby"},
        headers={"Origin": "http://localhost"},
    )
    assert response.status_code == 302
    assert response.headers["Referrer-Policy"] == "same-origin"


def test_cloud_is_disabled_even_with_confirmation(app):
    client = app.test_client()
    response = client.post("/cloud", data={"csrf": csrf(client), "confirm": "yes"})
    assert response.status_code == 403
    assert b"disabled" in response.data


def test_profile_form_posts_the_existing_background_job_contract(app, service):
    client = app.test_client()
    binding = {
        "workspace_id": "11111111-1111-1111-1111-111111111111",
        "lakehouse_id": "22222222-2222-2222-2222-222222222222",
        "environment_id": "33333333-3333-3333-3333-333333333333",
        "control_sjd_id": "44444444-4444-4444-4444-444444444444",
        "storage_account": "testaccount", "filesystem": "footage",
        "shortcut_path": "Files/videos", "table_directory": "Tables/dbo",
    }
    form = {
        **binding, "target": "dialog-ci", "profiles": ["rtdetr-r18-onnx-cpu"],
        "batch_size": "2", "sample_fps": "2.5",
    }
    assert client.post("/profiles", data=form).status_code == 403
    assert service.store.jobs() == []
    response = client.post("/profiles", data={**form, "csrf": csrf(client)})
    assert response.status_code == 302
    jobs = service.store.jobs()
    assert len(jobs) == 1 and jobs[0]["status"] == "QUEUED" and jobs[0]["kind"] == "profiles"
    assert service.store.job(jobs[0]["id"])["payload"] == {
        "target": "dialog-ci", "binding": {**binding, "intake_mode": "explicit"},
        "profiles": ["rtdetr-r18-onnx-cpu"], "overrides": {"batch_size": 2, "sample_fps": 2.5},
    }
    assert not service.catalog.exists()


@pytest.mark.parametrize("relative", ["../outside", "/etc/passwd", "lobby/../../outside"])
def test_source_rejects_traversal(service, relative):
    with pytest.raises(ValueError, match="source root"):
        service.source.resolve(relative)


def test_source_rejects_symlink_escape(service, tmp_path):
    outside = tmp_path / "outside.mp4"
    outside.write_bytes(b"private")
    (service.source.root / "escape.mp4").symlink_to(outside)
    with pytest.raises(ValueError, match="source root"):
        service.source.resolve("escape.mp4")
    assert next(entry for entry in service.source.entries("") if entry["name"] == "escape.mp4")["blocked"] is True


def test_explorer_metadata_is_read_only_and_folders_sort_first(service, monkeypatch):
    video = service.source.root / "Alpha.MP4"
    video.write_bytes(b"x" * 2048)
    os.utime(video, (1767268800, 1767268800))
    (service.source.root / "notes.txt").write_text("notes")
    monkeypatch.setattr(service.source, "frame", lambda *args: pytest.fail("Browsing must not decode video"))
    entries = service.source.entries("")
    assert [entry["name"] for entry in entries] == ["lobby", "Alpha.MP4", "notes.txt"]
    row = entries[1]
    assert row["video"] is True and row["directory"] is False
    assert row["size_bytes"] == 2048
    assert row["modified_utc"] == "2026-01-01 12:00 UTC"
    assert video.read_bytes() == b"x" * 2048


@pytest.mark.skipif(not hasattr(time, "tzset"), reason="Requires POSIX timezone switching")
def test_explorer_modified_times_are_utc_regardless_of_server_timezone(service, monkeypatch):
    video = service.source.root / "clip.mp4"
    video.write_bytes(b"video")
    os.utime(video, (1767268800, 1767268800))
    try:
        with monkeypatch.context() as environment:
            environment.setenv("TZ", "America/Denver")
            time.tzset()
            row = next(entry for entry in service.source.entries("") if entry["name"] == "clip.mp4")
            assert row["modified_utc"] == "2026-01-01 12:00 UTC"
    finally:
        time.tzset()


def test_explorer_tree_only_loads_the_current_ancestor_branch(service, monkeypatch):
    (service.source.root / "site/east/nested").mkdir(parents=True)
    (service.source.root / "unopened/deep").mkdir(parents=True)
    calls = []
    original = service.source.folders

    def folders(folder):
        calls.append(folder)
        return original(folder)

    monkeypatch.setattr(service.source, "folders", folders)
    tree = service.source.folder_tree("site/east")
    assert calls == ["", "site", "site/east"]
    site = next(node for node in tree if node["path"] == "site")
    east = site["children"][0]
    assert east["path"] == "site/east"
    assert east["children"] == [{"name": "nested", "path": "site/east/nested", "children": None}]
    assert next(node for node in tree if node["path"] == "unopened")["children"] is None


def test_folder_endpoint_is_shallow_and_excludes_blocked_links(app, service, tmp_path):
    (service.source.root / "lobby/child/deep").mkdir(parents=True)
    (service.source.root / "lobby/clip.mp4").write_bytes(b"video")
    outside = tmp_path / "outside"
    outside.mkdir()
    (service.source.root / "lobby/outside").symlink_to(outside, target_is_directory=True)
    response = app.test_client().get("/folders", query_string={"folder": "lobby"})
    assert response.status_code == 200
    assert response.json == {"folders": [
        {"name": "child", "path": "lobby/child", "children": None},
    ]}
    blocked = next(entry for entry in service.source.entries("lobby") if entry["blocked"])
    assert blocked["size_bytes"] is None and blocked["modified_utc"] is None


@pytest.mark.parametrize("folder", ["../outside", "/etc", "escape"])
def test_folder_endpoint_enforces_the_same_source_boundary(app, service, tmp_path, folder):
    (service.source.root / "escape").symlink_to(tmp_path, target_is_directory=True)
    response = app.test_client().get("/folders", query_string={"folder": folder})
    assert response.status_code == 400
    assert b"source root" in response.data


@pytest.mark.parametrize("query,kind,expected", [
    ("CLIP", "videos", ["lobby/clip.MP4"]),
    ("", "folders", ["lobby/clips"]),
    ("", "all", ["lobby/clips", "lobby/clip.MP4", "lobby/readme.txt"]),
    ("missing", "all", []),
])
def test_explorer_filters_current_folder_without_filtering_navigation(app, service, query, kind, expected):
    (service.source.root / "lobby/clips").mkdir()
    (service.source.root / "lobby/clip.MP4").write_bytes(b"video")
    (service.source.root / "lobby/readme.txt").write_text("notes")
    response = app.test_client().get("/", query_string={"folder": "lobby", "q": query, "kind": kind})
    assert response.status_code == 200
    for path in ["lobby/clips", "lobby/clip.MP4", "lobby/readme.txt"]:
        assert (f'data-entry-path="{path}"'.encode() in response.data) == (path in expected)
    assert response.data.count(b"data-entry-path=") == len(expected)
    assert b'aria-label="Folders"' in response.data
    if not expected:
        assert b"No matching items" in response.data
        assert b"This folder is empty" not in response.data


def test_explorer_rejects_unknown_file_filters(app):
    assert app.test_client().get("/?kind=unknown").status_code == 400


def test_explorer_distinguishes_empty_folders_from_no_filter_matches(app):
    response = app.test_client().get("/?folder=lobby&q=anything")
    assert response.status_code == 200
    assert b"This folder is empty" in response.data
    assert b"No matching items" not in response.data


def test_explorer_reports_unavailable_file_metadata(service, monkeypatch, caplog):
    path = service.source.root / "unavailable.mp4"
    path.write_bytes(b"video")

    def denied():
        raise PermissionError("permission denied")

    monkeypatch.setattr(service.source, "_children", lambda folder: [
        SimpleNamespace(name=path.name, path=str(path), stat=denied),
    ])
    row = service.source.entries("")[0]
    assert row["error"] == "Metadata unavailable: permission denied"
    assert row["video"] is False and row["size_bytes"] is None and row["modified_utc"] is None
    assert "Cannot inspect footage entry" in caplog.text


def test_explorer_reports_entry_resolution_errors_without_losing_row_shape(service, monkeypatch, caplog):
    path = service.source.root / "bad-link"
    original = Path.resolve

    def resolve(candidate, *args, **kwargs):
        if candidate == path:
            raise RuntimeError("symlink loop")
        return original(candidate, *args, **kwargs)

    monkeypatch.setattr(service.source, "_children", lambda folder: [
        SimpleNamespace(name=path.name, path=str(path)),
    ])
    monkeypatch.setattr(Path, "resolve", resolve)
    row = service.source.entries("")[0]
    assert row == {
        "name": "bad-link", "path": "bad-link", "blocked": False, "directory": False,
        "video": False, "size_bytes": None, "modified_utc": None,
        "error": "Metadata unavailable: symlink loop",
    }
    assert "Cannot inspect footage entry" in caplog.text


def test_lazy_folder_inspection_errors_are_logged_and_reported(service, monkeypatch, caplog):
    path = service.source.root / "unavailable"
    path.mkdir()

    def denied():
        raise PermissionError("permission denied")

    monkeypatch.setattr(service.source, "_children", lambda folder: [
        SimpleNamespace(name=path.name, path=str(path), is_dir=denied),
    ])
    with pytest.raises(ValueError, match="Cannot load folders: permission denied"):
        service.source.folders("")
    assert "Cannot inspect folder" in caplog.text


def test_explorer_does_not_inspect_metadata_outside_the_source(service, tmp_path, monkeypatch):
    outside = tmp_path / "outside"
    outside.mkdir()
    link = service.source.root / "outside"
    link.symlink_to(outside, target_is_directory=True)

    def forbidden():
        pytest.fail("Blocked entries must not expose external metadata")

    monkeypatch.setattr(service.source, "_children", lambda folder: [
        SimpleNamespace(name=link.name, path=str(link), stat=forbidden, is_dir=forbidden),
    ])
    row = service.source.entries("")[0]
    assert row["blocked"] is True and row["size_bytes"] is None and row["modified_utc"] is None
    assert service.source.folders("") == []


def test_explorer_surfaces_directory_read_errors(app, caplog):
    with patch.object(portal_module.os, "scandir", side_effect=PermissionError(13, "permission denied")):
        response = app.test_client().get("/?folder=lobby")
    assert response.status_code == 400
    assert b"Cannot read folder" in response.data
    assert "Cannot read footage folder" in caplog.text


def test_folder_endpoint_cannot_list_a_video_file(app, service):
    (service.source.root / "clip.mp4").write_bytes(b"video")
    response = app.test_client().get("/folders?folder=clip.mp4")
    assert response.status_code == 400
    assert b"Select a footage folder" in response.data


def test_explorer_reports_a_folder_changed_during_navigation(service, monkeypatch):
    monkeypatch.setattr(service.source, "folders", lambda folder: [])
    with pytest.raises(ValueError, match="changed while browsing"):
        service.source.folder_tree("lobby")


def test_jobs_persist_and_interrupted_work_is_not_reported_success(service):
    job = service.enqueue("prepare", {"folder": "lobby", "cameras": [], "exceptions": []})
    assert service.store.job(job)["status"] == "QUEUED"
    assert service.run_next_job() is True
    assert service.store.job(job)["status"] == "FAILED"
    assert "camera" in service.store.job(job)["error"].lower()
    restored = PortalService(service.source.root, service.models_root, service.state)
    assert restored.store.job(job)["status"] == "FAILED"
    assert restored.store.events(job)


def test_unknown_job_and_bad_download_are_not_success(app):
    client = app.test_client()
    assert client.get("/jobs/not-a-job").status_code == 404
    assert client.get("/jobs/not-a-job/files/secret").status_code == 404
    assert client.get("/", headers={"Host": "untrusted.example"}).status_code == 400


def test_inventory_supports_line_only_video_exceptions_without_faking_capture_time(tmp_path):
    video = tmp_path / "north/camera-17/20260101T120000Z.mp4"
    video.parent.mkdir(parents=True)
    video.write_bytes(b"video")
    catalog = tmp_path / "catalog.csv"
    write_catalog(catalog)
    inventory = tmp_path / "exceptions.csv"
    inventory.write_text(
        "video_relative_path,captured_at_utc,frame_width,frame_height,counting_line_x1,"
        "counting_line_y1,counting_line_x2,counting_line_y2\n"
        "north/camera-17/20260101T120000Z.mp4,,1920,1080,10,20,1900,20\n"
    )
    entries = load_video_inventory(inventory)
    assert entries[video.relative_to(tmp_path).as_posix()].captured_at_utc is None
    plan = build_manifest_plan(
        video, tmp_path, load_camera_catalog(catalog), entries,
        staging_prefix="staging", incoming_prefix="incoming", inspector=inspection,
    )
    assert plan.catalog.counting_line == (10, 20, 1900, 20)
    assert plan.capture_time_source == "filename"
    assert plan.captured_at_utc.isoformat() == "2026-01-01T12:00:00+00:00"


def test_partial_calibration_override_is_rejected(tmp_path):
    path = tmp_path / "exceptions.csv"
    path.write_text("video_relative_path,captured_at_utc,counting_line_x1\nclip.mp4,,10\n")
    with pytest.raises(CatalogError):
        load_video_inventory(path)


def save_camera(service, monkeypatch):
    service.store.add_location("north-entrance")
    camera_id = service.store.add_camera("Camera 17", "north-entrance")
    video = service.source.root / "lobby/20260101T120000Z.mp4"
    video.write_bytes(b"video")
    monkeypatch.setattr(service.source, "frame", lambda *args: (b"jpeg", 1920, 1080))
    form = {
        "video": "lobby/20260101T120000Z.mp4", "folder": "lobby", "scope": "camera",
        "camera_id": camera_id, "location_id": "north-entrance", "camera_timezone": "America/Denver",
        "effective_from_utc": "2025-01-01T00:00:00Z", "effective_to_utc": "2027-01-01T00:00:00Z",
        "capture_time_source": "filename_utc",
        "capture_time_regex": r"(?P<captured_at_utc>\d{8}T\d{6}Z)",
        "capture_time_format": "%Y%m%dT%H%M%SZ",
        "counting_line_x1": "10", "counting_line_y1": "20",
        "counting_line_x2": "1900", "counting_line_y2": "900",
    }
    service.save_calibration(form)
    return form


def test_preparation_snapshots_camera_metadata_and_archive_has_no_footage(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    monkeypatch.setattr(portal_module, "inspect_video", inspection)
    identity = service.enqueue_preparation("lobby")
    camera = service.store.entries("camera")[0]
    assert camera["folder"] == "lobby"
    assert camera["video"] == form["video"]
    for name in (
        "camera_id", "location_id", "camera_timezone", "capture_time_source",
        "capture_time_regex", "capture_time_format", "effective_from_utc", "effective_to_utc",
        "counting_line_x1", "counting_line_y1", "counting_line_x2", "counting_line_y2",
    ):
        assert camera["catalog"][name] == form[name]
    replacement = service.store.add_camera("Later Edit", "north-entrance")
    changed = {**camera["catalog"], "camera_id": replacement}
    service.store.put("camera", camera["id"], {**camera, "catalog": changed})
    assert service.run_next_job()
    assert service.store.job(identity)["status"] == "SUCCEEDED"
    index = json.loads(service.report(identity, "manifest-package.json").read_text())
    prepared = json.loads((
        service.job_dir(identity) / "package" / index["entries"][0]["prepared_manifest_path"]
    ).read_text())
    assert prepared["camera_id"] == form["camera_id"]
    assert prepared["camera_timezone"] == "America/Denver"
    assert prepared["counting_line"] == [10, 20, 1900, 900]
    import zipfile
    with zipfile.ZipFile(service.archive(identity)) as archive:
        assert "manifest-package.json" in archive.namelist()
        assert not any(name.endswith(".mp4") for name in archive.namelist())
    assert service.archive(identity).exists()


def test_video_exception_and_camera_edit_are_validated(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    identity = service.store.entries("camera")[0]["id"]
    service.save_calibration({**form, "config_id": identity, "effective_to_utc": "2028-01-01T00:00:00Z"})
    assert len(service.store.entries("camera")) == 1
    assert service.store.entries("camera")[0]["catalog"]["effective_to_utc"] == "2028-01-01T00:00:00Z"
    service.save_calibration({**form, "scope": "video", "counting_line_y1": "100", "counting_line_y2": "100"})
    assert service.store.entries("exception")[0]["counting_line_y1"] == "100"
    assert service.store.entries("camera")[0]["catalog"]["counting_line_y1"] == "20"
    with pytest.raises(CatalogError, match="outside"):
        service.save_calibration({**form, "counting_line_x2": "1920"})
    with pytest.raises(ValueError, match="Unknown"):
        service.save_calibration({**form, "config_id": "nonexistent"})
    with pytest.raises(ValueError, match="Select"):
        service.save_calibration({**form, "scope": "invalid"})


def test_recovery_and_retry_preserve_original_job_identity(service):
    identity = service.enqueue("prepare", {"folder": "lobby", "cameras": [], "exceptions": []})
    service.enqueue("prepare", {"folder": "lobby", "cameras": [], "exceptions": []})
    claimed = service.store.claim()
    assert claimed["id"] == identity
    assert service.store.claim() is None
    service.store.recover()
    assert service.store.job(identity)["status"] == "INTERRUPTED"
    service.store.retry(identity)
    assert service.store.job(identity)["status"] == "QUEUED"
    assert service.store.job(identity)["payload"] == claimed["payload"]
    with pytest.raises(ValueError):
        service.store.retry(identity)


def test_fatal_worker_error_is_unhealthy_and_visible_in_ui(service, app, monkeypatch):
    def fail():
        raise sqlite3.OperationalError("database unavailable")
    monkeypatch.setattr(service.store, "claim", fail)
    service.work(threading.Event())
    client = app.test_client()
    assert client.get("/healthz").status_code == 503
    assert b"Job worker stopped" in client.get("/").data


def test_progress_reporters_are_scoped_and_restore_cli_output(capsys):
    outer, inner = [], []
    with progress_reporter(outer.append):
        _progress("outer")
        with progress_reporter(inner.append):
            _progress("inner")
        _progress("restored")
    _progress("cli")
    assert outer == ["outer", "restored"]
    assert inner == ["inner"]
    assert capsys.readouterr().err == "cli\n"


def test_preview_rejects_bad_times_and_releases_decoder_on_failure(service):
    video = service.source.root / "lobby/broken.mp4"
    video.write_bytes(b"not a video")
    for seconds in (-1, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="finite and nonnegative"):
            service.source.frame("lobby/broken.mp4", seconds)
    with patch("cv2.VideoCapture") as capture:
        capture.return_value.read.return_value = (False, None)
        with pytest.raises(ValueError, match="Cannot decode"):
            service.source.frame("lobby/broken.mp4")
        capture.return_value.release.assert_called_once_with()


@pytest.mark.parametrize("changes", [
    {"counting_line_x1": "1920"}, {"counting_line_x2": "1920"},
    {"counting_line_y1": "1080"}, {"counting_line_y2": "1080"},
    {"counting_line_x1": "-1"},
    {"counting_line_x2": "10", "counting_line_y2": "20"},
])
def test_every_line_boundary_and_degenerate_line_is_rejected(service, monkeypatch, changes):
    form = save_camera(service, monkeypatch)
    with pytest.raises(CatalogError):
        service.save_calibration({
            **form, **changes, "config_id": service.store.entries("camera")[0]["id"],
        })


def test_worker_polls_only_when_idle_and_observes_stop(service, monkeypatch):
    stop = threading.Event()
    calls = []
    results = iter([True, False])

    def run():
        calls.append("run")
        return next(results)

    def idle(timeout):
        assert timeout == 0.5
        calls.append("idle")
        stop.set()
        return True

    monkeypatch.setattr(service, "run_next_job", run)
    monkeypatch.setattr(stop, "wait", idle)
    service.work(stop)
    assert calls == ["run", "run", "idle"]
    assert service.worker_error is None


def test_empty_timestamp_without_calibration_is_not_silently_ignored(tmp_path):
    path = tmp_path / "exceptions.csv"
    path.write_text("video_relative_path,captured_at_utc\nclip.mp4,\n")
    with pytest.raises(CatalogError, match="timestamp or calibration"):
        load_video_inventory(path)


def test_invalid_video_capture_override_does_not_save_exception(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    with pytest.raises(CatalogError):
        service.save_calibration({**form, "scope": "video", "captured_at_utc": "not-a-UTC-time"})
    assert service.store.entries("exception") == []


def camera_record(camera, location):
    return {"catalog": {"camera_id": camera, "location_id": location}, "folder": "lobby", "video": "lobby/clip.mp4"}


def test_named_directory_registers_cameras_without_footage(service):
    with patch.object(service.source, "frame", side_effect=AssertionError("No footage is required")):
        location = service.store.add_location("  S\u00e3o Paulo North  ")
        assert location == "sao-paulo-north"
        camera = service.store.add_camera("Main Entrance", location)
        assert camera == "sao-paulo-north-main-entrance"
        second = service.store.add_location("South Store")
        assert service.store.add_camera("Main Entrance", second) == "south-store-main-entrance"
    assert service.store.entries("camera_identity") == [
        {"id": camera, "display_name": "Main Entrance", "location_id": location},
        {"id": "south-store-main-entrance", "display_name": "Main Entrance", "location_id": second},
    ]
    assert service.store.entries("camera") == []
    assert service.store.locations() == [
        {"id": location, "display_name": "S\u00e3o Paulo North", "camera_ids": [camera], "camera_count": 1},
        {"id": second, "display_name": "South Store", "camera_ids": ["south-store-main-entrance"], "camera_count": 1},
    ]
    assert PortalStore(service.store.path).entries("camera_identity") == service.store.entries("camera_identity")


def test_named_directory_rejects_empty_slugs_duplicates_and_unknown_locations(service):
    for name in ("", "  ", "!!!"):
        with pytest.raises(ValueError):
            service.store.add_location(name)
    north = service.store.add_location("North Store")
    with pytest.raises(ValueError, match="already exists"):
        service.store.add_location("North---Store")
    service.store.add_camera("Entrance", north)
    with pytest.raises(ValueError, match="already exists"):
        service.store.add_camera("Entrance!", north)
    with pytest.raises(ValueError, match="existing location"):
        service.store.add_camera("Entrance", "unknown")
    assert len(service.store.entries("camera_identity")) == 1


def test_calibration_rejects_unregistered_camera_and_preserves_existing_data(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    before = service.store.entries("camera")
    with pytest.raises(ValueError, match="registered camera"):
        service.save_calibration({**form, "camera_id": "unregistered", "config_id": before[0]["id"]})
    assert service.store.entries("camera") == before


def test_named_directory_migration_keeps_legacy_ids_and_job_snapshots(service):
    record = camera_record("Legacy Camera 17", "Legacy Site")
    insert_legacy_camera(service.store, "legacy", record)
    job = service.store.enqueue("prepare", {"cameras": [record["catalog"]]})
    restored = PortalStore(service.store.path)
    assert restored.entries("camera_identity") == [
        {"id": "Legacy Camera 17", "display_name": "Legacy Camera 17", "location_id": "Legacy Site"},
    ]
    assert restored.locations()[0]["display_name"] == "Legacy Site"
    assert restored.job(job)["payload"]["cameras"] == [record["catalog"]]
    assert PortalStore(service.store.path).entries("camera_identity") == restored.entries("camera_identity")


def test_camera_registration_form_requires_csrf_and_never_opens_footage(app, service):
    client = app.test_client()
    location = service.store.add_location("North Store")
    form = {"display_name": "Main Entrance", "location_id": location, "camera_id": "forged-id"}
    assert client.post("/cameras", data=form).status_code == 403
    with patch.object(service.source, "frame", side_effect=AssertionError("No footage is required")):
        response = client.post("/cameras", data={**form, "csrf": csrf(client)})
    assert response.status_code == 302
    assert service.store.entries("camera_identity") == [{
        "id": "north-store-main-entrance", "display_name": "Main Entrance", "location_id": location,
    }]
    assert service.store.entries("camera") == []
    assert b"Main Entrance" in client.get("/cameras").data
    assert client.post("/cameras", data={**form, "csrf": csrf(client)}).status_code == 400
    assert len(service.store.entries("camera_identity")) == 1


def test_global_camera_slug_collision_does_not_change_ownership(service):
    first = service.store.add_location("A B")
    second = service.store.add_location("A")
    camera = service.store.add_camera("C", first)
    with pytest.raises(ValueError, match="already exists"):
        service.store.add_camera("B C", second)
    assert service.store.entries("camera_identity") == [
        {"id": camera, "display_name": "C", "location_id": first},
    ]


def test_concurrent_camera_registration_cannot_duplicate_ids(service):
    location = service.store.add_location("North Store")
    ready = threading.Barrier(2)

    def register(name):
        ready.wait(timeout=5)
        try:
            return service.store.add_camera(name, location)
        except ValueError as error:
            assert "already exists" in str(error)
            return "duplicate"

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert sorted(executor.map(register, ["Main Entrance", "Main---Entrance"])) == [
            "duplicate", "north-store-main-entrance",
        ]
    assert len(service.store.entries("camera_identity")) == 1


def test_empty_legacy_location_migration_preserves_ids_and_new_display_names(service):
    service.store.put("location", "Existing Legacy ID", {})
    service.store.add_location("Named Location")
    restored = PortalStore(service.store.path)
    assert restored.entries("location") == [
        {"id": "Existing Legacy ID", "display_name": "Existing Legacy ID"},
        {"id": "named-location", "display_name": "Named Location"},
    ]


def test_legacy_configuration_cannot_override_registered_camera_location(service):
    north = service.store.add_location("North")
    camera = service.store.add_camera("Entrance", north)
    insert_legacy_camera(service.store, "corrupt", camera_record(camera, "south"))
    before = service.store.entries("camera_identity")
    with pytest.raises(ValueError, match="one location"):
        PortalStore(service.store.path)
    assert service.store.entries("camera_identity") == before
    assert service.store.entries("location") == [{"id": north, "display_name": "North"}]


def test_camera_timezone_is_validated_even_when_a_request_bypasses_the_picker(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    camera = service.store.entries("camera")[0]
    with pytest.raises(CatalogError, match="timezone"):
        service.save_calibration({**form, "config_id": camera["id"], "camera_timezone": "Not/A/Timezone"})
    assert service.store.entries("camera") == [camera]


def test_locations_exist_without_cameras_and_persist(service):
    service.store.add_location("empty-warehouse")
    assert service.store.locations() == [
        {"id": "empty-warehouse", "display_name": "empty-warehouse", "camera_ids": [], "camera_count": 0},
    ]
    restored = PortalStore(service.store.path)
    assert restored.locations() == service.store.locations()
    assert restored.entries("camera") == []


def test_location_has_many_cameras_but_counts_ids_not_configurations(service):
    service.store.add_location("north")
    service.store.add_location("empty")
    first = service.store.add_camera("Cam 1", "north")
    second = service.store.add_camera("Cam 2", "north")
    for config, camera in (("config-1", first), ("config-2", second), ("config-3", first)):
        service.store.put("camera", config, camera_record(camera, "north"))
    assert service.store.locations() == [
        {"id": "empty", "display_name": "empty", "camera_ids": [], "camera_count": 0},
        {"id": "north", "display_name": "north", "camera_ids": [first, second], "camera_count": 2},
    ]


@pytest.mark.parametrize("value", ["", "  ", None, 123])
def test_invalid_or_duplicate_locations_are_rejected(service, value):
    with pytest.raises(ValueError):
        service.store.add_location(value)
    service.store.add_location(" north ")
    with pytest.raises(ValueError, match="already exists"):
        service.store.add_location("north")
    assert len(service.store.locations()) == 1


def test_camera_requires_an_existing_location(service):
    with pytest.raises(ValueError, match="existing location"):
        service.store.put("camera", "config-1", camera_record("cam-1", "missing"))
    assert service.store.entries("camera") == []
    assert service.store.locations() == []


def test_camera_cannot_be_assigned_to_two_locations_and_failed_edit_is_atomic(service):
    service.store.add_location("north")
    service.store.add_location("south")
    camera = service.store.add_camera("Cam 1", "north")
    original = camera_record(camera, "north")
    service.store.put("camera", "early", original)
    service.store.put("camera", "late", original)
    for config in ("new", "late"):
        with pytest.raises(ValueError, match="one location"):
            service.store.put("camera", config, camera_record(f" {camera} ", "south"))
    assert len(service.store.entries("camera")) == 2
    assert all(row["catalog"]["location_id"] == "north" for row in service.store.entries("camera"))
    assert service.store.locations()[0]["camera_count"] == 1
    assert service.store.locations()[1]["camera_count"] == 0


def test_concurrent_camera_saves_cannot_split_ownership(service):
    service.store.add_location("north")
    service.store.add_location("south")
    camera = service.store.add_camera("Cam 1", "north")
    ready = threading.Barrier(2)

    def save(location):
        ready.wait(timeout=5)
        try:
            service.store.put("camera", location, camera_record(camera, location))
            return "saved"
        except ValueError:
            return "rejected"

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert sorted(executor.map(save, ["north", "south"])) == ["rejected", "saved"]
    assert len(service.store.entries("camera")) == 1
    assert sum(row["camera_count"] for row in service.store.locations()) == 1


def test_registered_camera_cannot_move_through_calibration_or_rewrite_package_snapshot(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    identity = service.enqueue_preparation("lobby")
    config = service.store.entries("camera")[0]
    service.store.add_location("south")
    with pytest.raises(ValueError, match="one location"):
        service.save_calibration({**form, "config_id": config["id"], "location_id": "south"})
    assert service.store.job(identity)["payload"]["cameras"][0]["location_id"] == "north-entrance"
    assert service.store.locations() == [
        {
            "id": "north-entrance", "display_name": "north-entrance",
            "camera_ids": [form["camera_id"]], "camera_count": 1,
        },
        {"id": "south", "display_name": "south", "camera_ids": [], "camera_count": 0},
    ]


def insert_legacy_camera(store, identity, record):
    with store.connection() as db:
        db.execute("INSERT INTO entries VALUES ('camera',?,?)", (identity, json.dumps(record)))


def test_legacy_locations_migrate_idempotently_without_changing_jobs(service):
    legacy = camera_record(" cam-1 ", " north ")
    insert_legacy_camera(service.store, "legacy-1", legacy)
    insert_legacy_camera(service.store, "legacy-2", legacy)
    job = service.store.enqueue("prepare", {"cameras": [legacy["catalog"]]})
    restored = PortalStore(service.store.path)
    assert restored.locations() == [
        {"id": "north", "display_name": "north", "camera_ids": ["cam-1"], "camera_count": 1},
    ]
    assert restored.job(job)["payload"]["cameras"] == [legacy["catalog"]]
    assert all(row["catalog"]["camera_id"] == "cam-1" for row in restored.entries("camera"))
    assert PortalStore(service.store.path).locations() == restored.locations()


def test_conflicting_legacy_camera_locations_fail_without_partial_migration(service):
    insert_legacy_camera(service.store, "legacy-1", camera_record("cam-1", "north"))
    insert_legacy_camera(service.store, "legacy-2", camera_record("cam-1", "south"))
    before = service.store.entries("camera")
    with pytest.raises(ValueError, match="one location"):
        PortalStore(service.store.path)
    assert service.store.entries("camera") == before
    assert service.store.entries("location") == []


def test_locations_form_requires_csrf_and_creates_empty_location(app, service, monkeypatch):
    client = app.test_client()
    assert client.post("/locations", data={"display_name": "Warehouse"}).status_code == 403
    with patch.object(service.source, "frame", side_effect=AssertionError("No footage is required")):
        response = client.post("/locations", data={"csrf": csrf(client), "display_name": "Warehouse"})
    assert response.status_code == 302
    page = client.get("/locations")
    assert page.status_code == 200 and b"warehouse" in page.data
    assert b'data-camera-count="0"' in page.data
    assert service.store.entries("camera") == []


def test_tailwind_stylesheet_is_compiled_and_served_locally(app):
    response = app.test_client().get("/static/build/portal.css")
    assert response.status_code == 200
    assert "text/css" in response.content_type
    assert b"tailwindcss v4.3.0" in response.data
    assert b"@apply" not in response.data
    assert b'@import "tailwindcss"' not in response.data


def test_compose_live_assets_reload_templates_without_restarting_jobs(service, tmp_path, monkeypatch):
    source = Path(portal_web.__file__).parent
    templates = tmp_path / "live-templates"
    static = tmp_path / "live-static"
    shutil.copytree(source / "portal_templates", templates)
    shutil.copytree(source / "portal_static", static)
    monkeypatch.setenv("PC_PORTAL_TEMPLATE_DIR", str(templates))
    monkeypatch.setenv("PC_PORTAL_STATIC_DIR", str(static))
    monkeypatch.setenv("PC_PORTAL_LIVE_TEMPLATES", "1")
    app = create_app(service)
    assert app.template_folder == str(templates)
    assert app.static_folder == str(static)
    assert app.config["TEMPLATES_AUTO_RELOAD"] is True
    client = app.test_client()
    assert client.get("/").status_code == 200
    page = templates / "footage.html"
    page.write_text(page.read_text().replace("Operator portal", "Live template marker"))
    assert b"Live template marker" in client.get("/").data


def test_codespaces_exact_private_forwarded_host_and_origin_work(service, monkeypatch):
    monkeypatch.setenv("CODESPACES", "true")
    monkeypatch.setenv("CODESPACE_NAME", "test-portal-abc")
    monkeypatch.setenv("GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN", "app.github.dev")
    host = "test-portal-abc-8088.app.github.dev"
    app = create_app(service)
    client = app.test_client()
    response = client.get("/", base_url=f"https://{host}")
    assert response.status_code == 200
    assert "Secure" in response.headers["Set-Cookie"]
    with client.session_transaction(base_url=f"https://{host}") as stored:
        token = stored["csrf"]
    response = client.post(
        "/locations", base_url=f"http://{host}",
        data={"csrf": token, "display_name": "codespaces-location"},
        headers={"Origin": f"https://{host}"},
    )
    assert response.status_code == 302
    assert client.post(
        "/locations", base_url=f"http://{host}",
        data={"csrf": token, "display_name": "forbidden"},
        headers={"Origin": "https://another-codespace-8088.app.github.dev"},
    ).status_code == 403
    assert client.get("/", base_url="https://another-codespace-8088.app.github.dev").status_code == 400
    assert service.store.locations() == [
        {"id": "codespaces-location", "display_name": "codespaces-location", "camera_ids": [], "camera_count": 0},
    ]


@pytest.mark.parametrize("variable,value", [
    ("CODESPACE_NAME", None),
    ("CODESPACE_NAME", "-invalid"),
    ("CODESPACE_NAME", "name/path"),
    ("GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN", None),
    ("GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN", "*.app.github.dev"),
    ("GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN", "app.github.dev/path"),
    ("PC_PORTAL_PORT", "0"),
    ("PC_PORTAL_PORT", "65536"),
])
def test_codespaces_rejects_invalid_forwarding_configuration(monkeypatch, variable, value):
    monkeypatch.setenv("CODESPACES", "true")
    monkeypatch.setenv("CODESPACE_NAME", "test-portal-abc")
    monkeypatch.setenv("GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN", "app.github.dev")
    monkeypatch.setenv("PC_PORTAL_PORT", "8088")
    if value is None:
        monkeypatch.delenv(variable)
    else:
        monkeypatch.setenv(variable, value)
    with pytest.raises(ValueError, match="Invalid Codespaces port-forwarding environment"):
        portal_web._codespaces_origin()


@pytest.mark.parametrize("port", [None, "1", "65535"])
def test_codespaces_default_and_valid_boundary_ports(monkeypatch, port):
    monkeypatch.setenv("CODESPACES", "true")
    monkeypatch.setenv("CODESPACE_NAME", "test-portal-abc")
    monkeypatch.setenv("GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN", "app.github.dev")
    if port is None:
        monkeypatch.delenv("PC_PORTAL_PORT", raising=False)
    else:
        monkeypatch.setenv("PC_PORTAL_PORT", port)
    assert portal_web._codespaces_origin() == f"https://test-portal-abc-{port or '8088'}.app.github.dev"
