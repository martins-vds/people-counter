import csv
import json
from dataclasses import replace
from unittest.mock import patch

import pytest

from people_counter.operator_portal import create_app
from people_counter.portal_service import PortalService
from people_counter.manifest import CatalogError, VideoValidationError, load_camera_catalog, match_catalog_entry
from tests.test_manifest_publisher import inspection
from tests.test_operator_portal import csrf, save_camera


@pytest.fixture
def service(tmp_path):
    source = tmp_path / "videos"
    (source / "lobby").mkdir(parents=True)
    return PortalService(source, tmp_path / "models", tmp_path / "state")


def test_assign_many_videos_without_counting_line_and_restore(service):
    location = service.store.add_location("Entrance")
    camera = service.store.add_camera("Main", location)
    for name in ("today.mp4", "tomorrow.mp4"):
        (service.source.root / "lobby" / name).write_bytes(b"video")
    service.assign_footage(camera, ["lobby/today.mp4", "lobby/tomorrow.mp4"])
    restored = PortalService(service.source.root, service.models_root, service.state)
    assert restored.footage_assignments() == {
        "lobby/today.mp4": camera, "lobby/tomorrow.mp4": camera,
    }
    assert not restored.store.entries("camera")
    client = create_app(restored).test_client()
    page = client.get("/?folder=lobby").data
    assert b"Main" in page
    assert b"Camera / calibration" not in page
    assert b"Not calibrated" not in page
    assert b"Calibrate counting line" not in page
    assert b"Not calibrated" in client.get(f"/cameras/{camera}/footage").data
    identity = restored.enqueue_preparation("lobby")
    assert restored.run_next_job()
    assert restored.store.job(identity)["status"] == "FAILED"
    assert "counting line" in restored.store.job(identity)["error"]


def test_assignment_validates_entire_selection_before_writing(service):
    location = service.store.add_location("Entrance")
    camera = service.store.add_camera("Main", location)
    (service.source.root / "lobby/today.mp4").write_bytes(b"video")
    with pytest.raises(ValueError):
        service.assign_footage(camera, ["lobby/today.mp4", "../outside.mp4"])
    assert service.footage_assignments() == {}
    with pytest.raises(ValueError, match="camera"):
        service.assign_footage("missing", ["lobby/today.mp4"])
    with pytest.raises(ValueError, match="video"):
        service.assign_footage(camera, ["lobby"])
    (service.source.root / "not-a-video.mp4").mkdir()
    (service.source.root / "notes.txt").write_text("notes")
    for path in ("not-a-video.mp4", "notes.txt"):
        with pytest.raises(ValueError, match="video"):
            service.assign_footage(camera, [path])
    with pytest.raises(ValueError, match="Select"):
        service.assign_footage(camera, [])


def test_bulk_assignment_form_and_camera_page_show_all_footage(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    second = service.source.root / "lobby/20260102T120000Z.mp4"
    second.write_bytes(b"video")
    app = create_app(service)
    client = app.test_client()
    videos = [form["video"], second.relative_to(service.source.root).as_posix()]
    assert client.post("/footage/assign", data={"camera_id": form["camera_id"], "videos": videos}).status_code == 403
    response = client.post("/footage/assign", data={
        "csrf": csrf(client), "camera_id": form["camera_id"], "videos": videos, "folder": "lobby",
    })
    assert response.status_code == 302
    page = client.get(f"/cameras/{form['camera_id']}/footage").data
    assert all(video.encode() in page for video in videos)
    assert b"Calibration example" in page
    assert b"Edit metadata" in client.get("/cameras").data
    existing = service.store.entries("camera")[0]
    page = client.get("/calibrate", query_string={"video": videos[1], "camera_id": form["camera_id"]}).data
    assert f'name="config_id" value="{existing["id"]}"'.encode() in page
    assert b'value="1900"' in page


def test_same_folder_different_cameras_keep_exact_assignments_in_package(service, monkeypatch):
    first = save_camera(service, monkeypatch)
    second_camera = service.store.add_camera("Camera 18", first["location_id"])
    second_video = "lobby/20260102T120000Z.mp4"
    (service.source.root / second_video).write_bytes(b"video")
    service.assign_footage(second_camera, [second_video])
    service.save_calibration({**first, "camera_id": second_camera, "video": second_video})
    monkeypatch.setattr("people_counter.portal_service.inspect_video", inspection)
    identity = service.enqueue_preparation("lobby")
    service.assign_footage(first["camera_id"], [second_video])
    assert service.run_next_job()
    assert service.store.job(identity)["status"] == "SUCCEEDED"
    package = service.job_dir(identity) / "package"
    index = json.loads((package / "manifest-package.json").read_text())
    manifests = {
        item["video_relative_path"]: json.loads((package / item["prepared_manifest_path"]).read_text())
        for item in index["entries"]
    }
    assert manifests["videos/" + first["video"]]["camera_id"] == first["camera_id"]
    assert manifests["videos/" + second_video]["camera_id"] == second_camera
    assert all(item["counting_line"] == [10, 20, 1900, 900] for item in manifests.values())
    exported = list(csv.DictReader(client_get_csv(service).splitlines()))
    assert all(row["video_relative_path"] for row in exported)


def client_get_csv(service):
    return create_app(service).test_client().get("/cameras.csv").get_data(as_text=True)


def test_metadata_edit_does_not_change_geometry_or_require_frame_decode(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    config = service.store.entries("camera")[0]
    client = create_app(service).test_client()
    url = f"/cameras/{form['camera_id']}/metadata/{config['id']}"
    page = client.get(url)
    assert page.status_code == 200 and b"Counting line editor" not in page.data
    with patch.object(service.source, "frame", side_effect=AssertionError("Must not decode")):
        response = client.post(url, data={
            "csrf": csrf(client), "camera_timezone": "UTC", "capture_time_source": "embedded",
            "capture_time_regex": "", "capture_time_format": "",
            "effective_from_utc": "2025-01-01T00:00:00Z", "effective_to_utc": "",
        })
    assert response.status_code == 302
    changed = service.store.entries("camera")[0]
    assert changed["catalog"]["camera_timezone"] == "UTC"
    assert changed["catalog"]["counting_line_x1"] == config["catalog"]["counting_line_x1"]
    assert changed["video"] == config["video"]


@pytest.mark.parametrize("name", ["summary.json", "rejection-report.csv"])
def test_reports_view_inline_with_escaping_and_download_retained(service, name):
    identity = service.enqueue("prepare", {"folder": "lobby", "cameras": [], "exceptions": []})
    root = service.job_dir(identity) / "package"
    root.mkdir(parents=True)
    dangerous = "<script>alert(1)</script>"
    if name.endswith(".json"):
        (root / name).write_text(json.dumps({"planned": 2, "rejected": 1, "details": {"reason": dangerous}}))
    else:
        (root / name).write_text(f"reason_code,message\nMISSING,{dangerous}\n")
    client = create_app(service).test_client()
    response = client.get(f"/jobs/{identity}/reports/{name}")
    assert response.status_code == 200
    assert b"attachment" not in response.headers.get("Content-Disposition", "").encode()
    assert b"<script>" not in response.data
    assert b"&lt;script&gt;" in response.data
    assert b"Download" in response.data
    page = client.get(f"/jobs/{identity}").data
    assert f"/jobs/{identity}/reports/{name}".encode() in page
    assert b"does not upload" in page
    assert "attachment" in client.get(f"/jobs/{identity}/files/{name}").headers["Content-Disposition"]
    assert client.get(f"/jobs/{identity}/reports/not-allowed.json").status_code == 404


def test_csv_report_pagination_and_empty_state(service):
    identity = service.enqueue("prepare", {})
    root = service.job_dir(identity)
    root.mkdir(parents=True)
    path = root / "rejection-report.csv"
    path.write_text("reason_code,message\n" + "".join(f"ERROR,video-{i}\n" for i in range(52)))
    client = create_app(service).test_client()
    first = client.get(f"/jobs/{identity}/reports/rejection-report.csv").data
    assert b"video-0<" in first and b"video-50<" not in first
    assert b"Next" in first
    second = client.get(f"/jobs/{identity}/reports/rejection-report.csv?page=2").data
    assert b"video-50<" in second and b"video-0<" not in second
    assert b">Next<" not in second
    assert client.get(f"/jobs/{identity}/reports/rejection-report.csv?page=0").status_code == 400
    path.write_text("reason_code,message\n")
    assert b"No rows" in client.get(f"/jobs/{identity}/reports/rejection-report.csv").data


def test_exact_catalog_assignment_precedes_folder_without_matching_other_video(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    path = service.state / "catalog.csv"
    path.write_text(client_get_csv(service))
    original = load_camera_catalog(path)[0]
    specific = replace(
        original, camera_id="different-camera",
        video_relative_path="videos/lobby/20260102T120000Z.mp4",
    )
    selected, _, _ = match_catalog_entry(
        "videos/lobby/20260102T120000Z.mp4", [original, specific], {},
    )
    assert selected.camera_id == "different-camera"
    selected, _, _ = match_catalog_entry(
        "videos/lobby/20260101T120000Z.mp4", [original, specific], {},
    )
    assert selected.camera_id == form["camera_id"]
    with pytest.raises(VideoValidationError, match="No camera"):
        match_catalog_entry("videos/other/20260101T120000Z.mp4", [specific], {})


def test_repeated_exact_catalog_rows_with_overlapping_dates_are_rejected(service, monkeypatch):
    save_camera(service, monkeypatch)
    text = client_get_csv(service)
    rows = list(csv.DictReader(text.splitlines()))
    exact = next(row for row in rows if row["video_relative_path"])
    fields = rows[0].keys()
    with pytest.raises(CatalogError, match="Overlapping"):
        service.validate_csv(tuple(fields), [exact, exact])
    with pytest.raises(CatalogError):
        service.validate_csv(tuple(fields), [{**exact, "video_relative_path": "../outside.mp4"}])


def test_changing_calibration_example_keeps_previous_footage_assigned(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    second = "lobby/20260102T120000Z.mp4"
    (service.source.root / second).write_bytes(b"video")
    service.assign_footage(form["camera_id"], [second])
    config = service.store.entries("camera")[0]
    service.save_calibration({**form, "video": second, "config_id": config["id"]})
    assert service.footage_assignments() == {
        form["video"]: form["camera_id"], second: form["camera_id"],
    }


def test_report_view_explains_size_limit_with_download_link(service):
    identity = service.enqueue("prepare", {})
    root = service.job_dir(identity)
    root.mkdir(parents=True)
    (root / "summary.json").write_text(" " * (5 * 1024 * 1024 + 1))
    client = create_app(service).test_client()
    response = client.get(f"/jobs/{identity}/reports/summary.json")
    assert response.status_code == 400
    assert b"inline limit" in response.data
    assert f"/jobs/{identity}/files/summary.json".encode() in response.data


def test_existing_calibration_cannot_be_moved_to_another_camera(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    config = service.store.entries("camera")[0]
    other = service.store.add_camera("Another", form["location_id"])
    service.assign_footage(other, [form["video"]])
    with pytest.raises(ValueError, match="different camera"):
        service.save_calibration({**form, "camera_id": other, "config_id": config["id"]})
    assert service.store.entries("camera")[0] == config


def test_catalog_rejects_missing_exact_path_field_value(service, monkeypatch):
    save_camera(service, monkeypatch)
    path = service.state / "short.csv"
    text = client_get_csv(service).splitlines()
    path.write_text(text[0] + "\n" + text[-1].rsplit(",", 1)[0] + "\n")
    with pytest.raises(CatalogError, match="malformed"):
        load_camera_catalog(path)


def test_json_report_collections_are_paginated_inline(service):
    identity = service.enqueue("prepare", {})
    root = service.job_dir(identity)
    root.mkdir(parents=True)
    (root / "manifest-package.json").write_text(json.dumps({
        "complete": True, "entries": [{"video": f"clip-{i}"} for i in range(52)],
    }))
    client = create_app(service).test_client()
    url = f"/jobs/{identity}/reports/manifest-package.json"
    first = client.get(url).data
    assert b"clip-0" in first and b"clip-50" not in first
    assert b"Next" in first
    second = client.get(url + "?page=2&section=entries").data
    assert b"clip-50" in second and b"clip-0" not in second
    assert b">Next<" not in second
    assert client.get(url + "?section=missing").status_code == 400


def test_invalid_csv_report_is_visible_error_not_server_failure(service):
    identity = service.enqueue("prepare", {})
    root = service.job_dir(identity)
    root.mkdir(parents=True)
    (root / "rejection-report.csv").write_bytes(b"reason\n\xff\n")
    response = create_app(service).test_client().get(
        f"/jobs/{identity}/reports/rejection-report.csv",
    )
    assert response.status_code == 400
    assert b"Cannot display CSV report" in response.data


def test_camera_assignment_works_outside_calibration_example_folder(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    (service.source.root / "archive").mkdir()
    (service.source.root / "archive/20260102T120000Z.mp4").write_bytes(b"video")
    service.assign_footage(form["camera_id"], ["archive/20260102T120000Z.mp4"])
    exact = [
        row for row in service.camera_catalog()
        if row.get("video_relative_path") == "videos/archive/20260102T120000Z.mp4"
    ]
    assert len(exact) == 1
    assert exact[0]["source_path_prefix"] == "videos/archive/"
    assert exact[0]["camera_id"] == form["camera_id"]


def test_legacy_folder_specific_configurations_keep_their_own_geometry(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    (service.source.root / "side").mkdir()
    second = "side/20260102T120000Z.mp4"
    (service.source.root / second).write_bytes(b"video")
    service.save_calibration({
        **form, "folder": "side", "video": second,
        "counting_line_y1": "100", "counting_line_y2": "100",
    })
    exact = {
        row["video_relative_path"]: row for row in service.camera_catalog()
        if row.get("video_relative_path")
    }
    assert exact["videos/" + form["video"]]["counting_line_y1"] == "20"
    assert exact["videos/" + second]["counting_line_y1"] == "100"
    assert len(exact) == 2
    assert {
        row["source_path_prefix"] for row in service.camera_catalog()
        if not row.get("video_relative_path")
    } == {"videos/lobby/", "videos/side/"}


def test_metadata_cannot_update_another_camera_or_an_unknown_configuration(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    config = service.store.entries("camera")[0]
    other = service.store.add_camera("Another", form["location_id"])
    for camera_id, config_id in ((other, config["id"]), (form["camera_id"], "unknown")):
        with pytest.raises(ValueError, match="Unknown camera configuration"):
            service.update_camera_metadata(camera_id, config_id, {})
    assert service.store.entries("camera")[0] == config


def test_json_report_inline_size_limit_includes_exact_boundary(service):
    identity = service.enqueue("prepare", {})
    root = service.job_dir(identity)
    root.mkdir(parents=True)
    path = root / "summary.json"
    prefix, suffix = '{"detail":"', '"}'
    size = 5 * 1024 * 1024
    path.write_text(prefix + "x" * (size - len(prefix) - len(suffix)) + suffix)
    assert path.stat().st_size == size
    assert service.report_preview(identity, "summary.json", 1)["format"] == "json"
    with path.open("a") as handle:
        handle.write(" ")
    with pytest.raises(ValueError, match="5 MiB"):
        service.report_preview(identity, "summary.json", 1)


def test_json_report_sections_and_root_array_are_navigable(service):
    identity = service.enqueue("prepare", {})
    root = service.job_dir(identity)
    root.mkdir(parents=True)
    path = root / "summary.json"
    path.write_text(json.dumps({"first": [1], "second": [2, 3], "count": 3}))
    report = service.report_preview(identity, "summary.json", 1, "second")
    assert report["data"] == {"count": 3}
    assert report["sections"] == {"first": 1, "second": 2}
    assert report["section"] == "second" and report["records"] == [2, 3]
    path.write_text(json.dumps([{"item": 1}]))
    report = service.report_preview(identity, "summary.json", 1)
    assert report["records"] == [{"item": 1}]
    assert report["section"] == "records"


def test_preparation_normalizes_folder_before_checking_uncalibrated_assignments(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    other = service.store.add_camera("Uncalibrated", form["location_id"])
    service.assign_footage(other, [form["video"]])
    identity = service.enqueue_preparation("lobby/../lobby")
    assert service.store.job(identity)["payload"]["folder"] == "lobby"
    assert service.run_next_job()
    assert "counting line" in service.store.job(identity)["error"]


def test_nested_folders_with_different_cameras_require_explicit_assignments(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    config = service.store.entries("camera")[0]
    service.save_calibration({**form, "folder": "", "config_id": config["id"]})
    other = service.store.add_camera("Another", form["location_id"])
    second = "lobby/20260102T120000Z.mp4"
    (service.source.root / second).write_bytes(b"video")
    service.save_calibration({**form, "video": second, "camera_id": other})
    rows = service.camera_catalog()
    assert len(rows) == 2
    assert all(row.get("video_relative_path") for row in rows)


def test_metadata_validation_rejects_bad_timezone_without_changing_saved_line(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    config = service.store.entries("camera")[0]
    with pytest.raises(CatalogError, match="timezone"):
        service.update_camera_metadata(form["camera_id"], config["id"], {
            **form, "camera_timezone": "Not/A/Timezone",
        })
    assert service.store.entries("camera")[0] == config


def test_legacy_calibration_examples_migrate_without_losing_assignment(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    with service.store.connection() as db:
        db.execute("DELETE FROM entries WHERE kind='footage'")
    assert service.footage_assignments() == {form["video"]: form["camera_id"]}
    restored = PortalService(service.source.root, service.models_root, service.state)
    assert restored.footage_assignments() == {form["video"]: form["camera_id"]}
    assert restored.store.entries("footage") == [{
        "id": form["video"], "camera_id": form["camera_id"],
    }]


def test_new_effective_dated_calibration_does_not_edit_existing_configuration(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    client = create_app(service).test_client()
    page = client.get("/calibrate", query_string={
        "video": form["video"], "camera_id": form["camera_id"], "new": "1",
    })
    assert page.status_code == 200
    assert b'name="config_id" value=""' in page.data
    assert b'name="new" value="1"' in page.data
    assert b'value="1900"' not in page.data


@pytest.mark.parametrize("count,has_next", [(0, False), (50, False), (51, True)])
def test_json_report_pagination_has_exact_fifty_record_boundary(service, count, has_next):
    identity = service.enqueue("prepare", {})
    root = service.job_dir(identity)
    root.mkdir(parents=True)
    (root / "summary.json").write_text(json.dumps({"entries": list(range(count))}))
    report = service.report_preview(identity, "summary.json", 1)
    assert report["has_next"] is has_next
    assert report["records"] == list(range(min(count, 50)))


def test_editing_one_camera_configuration_keeps_other_configurations_valid(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    first = service.store.entries("camera")[0]
    (service.source.root / "side").mkdir()
    (service.source.root / "side/20260102T120000Z.mp4").write_bytes(b"video")
    service.save_calibration({**form, "folder": "side", "video": "side/20260102T120000Z.mp4"})
    service.update_camera_metadata(form["camera_id"], first["id"], {
        **form, "effective_to_utc": "2028-01-01T00:00:00Z",
    })
    configs = {item["id"]: item for item in service.store.entries("camera")}
    assert configs[first["id"]]["catalog"]["effective_to_utc"] == "2028-01-01T00:00:00Z"
    assert next(item for item in configs.values() if item["id"] != first["id"])["catalog"]["effective_to_utc"] == form["effective_to_utc"]
