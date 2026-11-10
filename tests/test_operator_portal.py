import json
import sqlite3
import threading
from unittest.mock import patch

import pytest

pytest.importorskip("flask")

from people_counter.operator_portal import create_app
from people_counter.portal_service import PortalService
from people_counter.manifest import CatalogError, build_manifest_plan, load_camera_catalog, load_video_inventory
from tests.test_manifest_publisher import inspection, write_catalog
from people_counter import portal_service as portal_module
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
    assert service.source.entries("")[0]["blocked"] is True


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
    video = service.source.root / "lobby/20260101T120000Z.mp4"
    video.write_bytes(b"video")
    monkeypatch.setattr(service.source, "frame", lambda *args: (b"jpeg", 1920, 1080))
    form = {
        "video": "lobby/20260101T120000Z.mp4", "folder": "lobby", "scope": "camera",
        "camera_id": "camera-17", "location_id": "north-entrance", "camera_timezone": "America/Denver",
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
    changed = {**camera["catalog"], "camera_id": "later-edit"}
    service.store.put("camera", camera["id"], {**camera, "catalog": changed})
    assert service.run_next_job()
    assert service.store.job(identity)["status"] == "SUCCEEDED"
    index = json.loads(service.report(identity, "manifest-package.json").read_text())
    prepared = json.loads((
        service.job_dir(identity) / "package" / index["entries"][0]["prepared_manifest_path"]
    ).read_text())
    assert prepared["camera_id"] == "camera-17"
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
    service.save_calibration({**form, "config_id": identity, "location_id": "updated"})
    assert len(service.store.entries("camera")) == 1
    assert service.store.entries("camera")[0]["catalog"]["location_id"] == "updated"
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
