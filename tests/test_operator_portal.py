import json
import sqlite3
import threading
import shutil
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

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
    service.store.add_location("north-entrance")
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
    service.store.add_location("updated")
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


def camera_record(camera, location):
    return {"catalog": {"camera_id": camera, "location_id": location}, "folder": "lobby", "video": "lobby/clip.mp4"}


def test_locations_exist_without_cameras_and_persist(service):
    service.store.add_location("empty-warehouse")
    assert service.store.locations() == [{"id": "empty-warehouse", "camera_ids": [], "camera_count": 0}]
    restored = PortalStore(service.store.path)
    assert restored.locations() == service.store.locations()
    assert restored.entries("camera") == []


def test_location_has_many_cameras_but_counts_ids_not_configurations(service):
    service.store.add_location("north")
    service.store.add_location("empty")
    for config, camera in (("config-1", "cam-1"), ("config-2", "cam-2"), ("config-3", "cam-1")):
        service.store.put("camera", config, camera_record(camera, "north"))
    assert service.store.locations() == [
        {"id": "empty", "camera_ids": [], "camera_count": 0},
        {"id": "north", "camera_ids": ["cam-1", "cam-2"], "camera_count": 2},
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
    original = camera_record("cam-1", "north")
    service.store.put("camera", "early", original)
    service.store.put("camera", "late", original)
    for config in ("new", "late"):
        with pytest.raises(ValueError, match="one location"):
            service.store.put("camera", config, camera_record(" cam-1 ", "south"))
    assert len(service.store.entries("camera")) == 2
    assert all(row["catalog"]["location_id"] == "north" for row in service.store.entries("camera"))
    assert service.store.locations()[0]["camera_count"] == 1
    assert service.store.locations()[1]["camera_count"] == 0


def test_concurrent_camera_saves_cannot_split_ownership(service):
    service.store.add_location("north")
    service.store.add_location("south")
    ready = threading.Barrier(2)

    def save(location):
        ready.wait(timeout=5)
        try:
            service.store.put("camera", location, camera_record("cam-1", location))
            return "saved"
        except ValueError:
            return "rejected"

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert sorted(executor.map(save, ["north", "south"])) == ["rejected", "saved"]
    assert len(service.store.entries("camera")) == 1
    assert sum(row["camera_count"] for row in service.store.locations()) == 1


def test_single_configuration_can_move_without_rewriting_package_snapshot(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    identity = service.enqueue_preparation("lobby")
    config = service.store.entries("camera")[0]
    service.store.add_location("south")
    service.save_calibration({**form, "config_id": config["id"], "location_id": "south"})
    assert service.store.job(identity)["payload"]["cameras"][0]["location_id"] == "north-entrance"
    assert service.store.locations() == [
        {"id": "north-entrance", "camera_ids": [], "camera_count": 0},
        {"id": "south", "camera_ids": ["camera-17"], "camera_count": 1},
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
    assert restored.locations() == [{"id": "north", "camera_ids": ["cam-1"], "camera_count": 1}]
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
    assert client.post("/locations", data={"location_id": "warehouse"}).status_code == 403
    with patch.object(service.source, "frame", side_effect=AssertionError("No footage is required")):
        response = client.post("/locations", data={"csrf": csrf(client), "location_id": "warehouse"})
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
        data={"csrf": token, "location_id": "codespaces-location"},
        headers={"Origin": f"https://{host}"},
    )
    assert response.status_code == 302
    assert client.post(
        "/locations", base_url=f"http://{host}",
        data={"csrf": token, "location_id": "forbidden"},
        headers={"Origin": "https://another-codespace-8088.app.github.dev"},
    ).status_code == 403
    assert client.get("/", base_url="https://another-codespace-8088.app.github.dev").status_code == 400
    assert service.store.locations() == [
        {"id": "codespaces-location", "camera_ids": [], "camera_count": 0},
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
