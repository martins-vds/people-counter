import csv
import json
from dataclasses import replace
from unittest.mock import patch

import pytest

from people_counter.operator_portal import create_app
from people_counter.portal_service import PortalService
from people_counter.manifest import CatalogError, VideoMetadata, VideoValidationError, load_camera_catalog, match_catalog_entry
from tests.test_manifest_publisher import inspection
from tests.test_operator_portal import csrf, save_camera


@pytest.fixture
def service(tmp_path, monkeypatch):
    source = tmp_path / "videos"
    (source / "lobby").mkdir(parents=True)
    service = PortalService(source, tmp_path / "models", tmp_path / "state")
    monkeypatch.setattr("people_counter.portal_service.probe_video", lambda path: VideoMetadata(1920, 1080, 30))
    monkeypatch.setattr(service.source, "frame", lambda *args: (b"jpeg", 1920, 1080))
    return service


def test_camera_calibration_is_scoped_and_replaces_only_camera_geometry(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    service.save_calibration({**form, "scope": "video", "captured_at_utc": "2026-01-01T12:00:00Z"})
    before_assignments = service.footage_assignments()
    before_exception = service.store.entries("exception")
    config = service.store.entries("camera")[0]
    client = create_app(service).test_client()
    url = f"/cameras/{form['camera_id']}/calibrate"
    page = client.get(url).data
    assert b"Reference footage" in page and b"Save camera calibration" in page
    assert b'name="camera_id"' not in page and b'name="scope"' not in page
    response = client.post(url, data={
        "csrf": csrf(client), "video": form["video"], "config_id": config["id"],
        **{name: form[name] for name in ("counting_line_x1", "counting_line_y1", "counting_line_x2", "counting_line_y2")},
        "counting_line_x1": "42",
    })
    assert response.status_code == 302
    assert service.footage_assignments() == before_assignments
    assert service.store.entries("exception") == before_exception
    updated = service.store.entries("camera")
    assert len(updated) == 1 and updated[0]["id"] == config["id"]
    assert updated[0]["catalog"] == {**config["catalog"], "counting_line_x1": "42"}
    camera_page = client.get(f"/cameras/{form['camera_id']}/footage").data
    assert b"Saved counting-line configurations" not in camera_page
    assert b"Recalibrate camera" in camera_page
    assert b"/review?video=" in camera_page


@pytest.mark.parametrize("change", [
    {"video": "other.mp4"}, {"camera_id": "forged"}, {"location_id": "forged"}, {"config_id": "forged"},
])
def test_camera_calibration_rejects_unassociated_reference_without_mutation(service, monkeypatch, change):
    form = save_camera(service, monkeypatch)
    (service.source.root / "other.mp4").write_bytes(b"video")
    before = service.store.entries("camera")
    client = create_app(service).test_client()
    response = client.post(f"/cameras/{form['camera_id']}/calibrate", data={
        **form, "csrf": csrf(client), "config_id": before[0]["id"], **change,
    })
    assert response.status_code == 400
    assert service.store.entries("camera") == before
    assert "other.mp4" not in service.footage_assignments()


def test_first_camera_calibration_creates_valid_defaults_and_recalibration_replaces_them(service, monkeypatch):
    location = service.store.add_location("Entrance")
    camera = service.store.add_camera("Main", location)
    video = "lobby/first.mp4"
    (service.source.root / video).write_bytes(b"video")
    monkeypatch.setattr("people_counter.portal_service.probe_video", lambda path: VideoMetadata(
        1920, 1080, 30, ("2026-01-01T12:00:00Z",),
    ))
    unassigned = service.footage_readiness(video)
    assert unassigned["issues"][0]["code"] == "CAMERA_UNASSIGNED"
    assert "Camera association" in unassigned["issues"][0]["action"]
    service.assign_footage(camera, [video])
    line = dict(zip(("counting_line_x1", "counting_line_y1", "counting_line_x2", "counting_line_y2"),
                    ("10", "20", "1900", "900")))
    service.save_camera_line(camera, {"video": video, **line})
    first = service.store.entries("camera")[0]
    assert service.footage_readiness(video)["issues"] == []
    assert service.footage_assignments() == {video: camera}
    service.save_camera_line(camera, {"video": video, **line, "counting_line_x1": "30"})
    after = service.store.entries("camera")
    assert len(after) == 1 and after[0]["id"] == first["id"]
    assert after[0]["catalog"] == {**first["catalog"], "counting_line_x1": "30"}


def test_readiness_uses_manifest_rules_and_reports_timestamp_and_dimensions(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    with patch.object(service.source, "frame", wraps=service.source.frame) as decoded:
        ready = service.footage_readiness(form["video"])
    decoded.assert_called_once_with(form["video"])
    assert ready["issues"] == [] and ready["media_ok"]
    assert ready["config_id"] == service.store.entries("camera")[0]["id"]
    assert ready["camera_values"] == service.store.entries("camera")[0]["catalog"]
    assert ready["captured_at_utc"] == "2026-01-01T12:00:00Z"
    monkeypatch.setattr("people_counter.portal_service.probe_video", lambda path: VideoMetadata(1280, 720, 30))
    issue = service.footage_readiness(form["video"])["issues"][0]
    assert issue["code"] == "VIDEO_DIMENSION_MISMATCH"
    assert issue["scope"] == "package"
    config = service.store.entries("camera")[0]
    service.update_camera_metadata(form["camera_id"], config["id"], {
        **config["catalog"], "capture_time_source": "inventory",
        "capture_time_regex": "", "capture_time_format": "",
    })
    assert service.footage_readiness(form["video"])["issues"][0]["code"] == "CAPTURE_TIME_MISSING"


@pytest.mark.parametrize("line_source", ["camera", "video"])
def test_dimension_mismatch_explains_reference_and_links_to_video_confirmation(service, monkeypatch, line_source):
    form = save_camera(service, monkeypatch)
    if line_source == "video":
        service.update_video_exception(form["video"], {
            "captured_at_utc": "2026-01-01T12:00:00Z",
            "frame_width": "2560", "frame_height": "1440",
            **{name: form[name] for name in ("counting_line_x1", "counting_line_y1", "counting_line_x2", "counting_line_y2")},
        })
    camera = service.store.entries("camera")
    exceptions = service.store.entries("exception")
    monkeypatch.setattr("people_counter.portal_service.probe_video", lambda path: VideoMetadata(3840, 2160, 30))
    monkeypatch.setattr(service.source, "frame", lambda *args: (b"jpeg", 3840, 2160))
    client = create_app(service).test_client()
    page = client.get("/review", query_string={"video": form["video"]}).get_data(as_text=True)
    assert "3840 &times; 2160 px" in page
    expected = "2560 &times; 1440 px" if line_source == "video" else "1920 &times; 1080 px"
    assert expected in page
    assert ("Saved video line reference" if line_source == "video" else "Camera calibration reference") in page
    assert 'href="#video-line-editor"' in page and 'id="video-line-editor"' in page
    assert "The line can look right" in page
    assert "Correct the catalog dimensions" not in page
    assert service.store.entries("camera") == camera
    assert service.store.entries("exception") == exceptions
    response = client.post("/footage/line", data={
        **form, "csrf": csrf(client), "scope": "video",
    }, follow_redirects=True)
    assert response.status_code == 200 and b"VIDEO_DIMENSION_MISMATCH" not in response.data
    saved = service.store.entries("exception")[0]
    assert (saved["frame_width"], saved["frame_height"]) == ("3840", "2160")
    assert [saved[name] for name in ("counting_line_x1", "counting_line_y1", "counting_line_x2", "counting_line_y2")] == ["10", "20", "1900", "900"]
    assert service.store.entries("camera") == camera
    if line_source == "video":
        assert saved["captured_at_utc"] == "2026-01-01T12:00:00Z"


def test_invalid_media_remains_reviewable_but_assignment_is_blocked(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    other = service.store.add_camera("Other", form["location_id"])
    def broken(path):
        raise VideoValidationError("Unreadable video", reason_code="VIDEO_PROBE_FAILED",
                                   suggested_action="Replace the damaged file")
    monkeypatch.setattr("people_counter.portal_service.probe_video", broken)
    client = create_app(service).test_client()
    page = client.get("/review", query_string={"video": form["video"]})
    assert page.status_code == 200 and b"Unreadable video" in page.data
    assert b"Readiness" in page.data and b'id="line-canvas"' not in page.data
    result = service.footage_readiness(form["video"])
    assert not result["media_ok"]
    assert result["issues"][0]["action"] == "Replace the damaged file"
    with pytest.raises(VideoValidationError, match="Unreadable"):
        service.assign_footage(other, [form["video"]])
    assert service.footage_assignments()[form["video"]] == form["camera_id"]


def test_reassignment_requires_geometry_choice_and_preserves_capture_time(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    service.save_calibration({**form, "scope": "video", "captured_at_utc": "2026-01-01T12:00:00Z"})
    other = service.store.add_camera("Other", form["location_id"])
    before = service.store.entries("exception")
    with pytest.raises(ValueError, match="keep or clear"):
        service.assign_footage(other, [form["video"]])
    assert service.footage_assignments()[form["video"]] == form["camera_id"]
    service.assign_footage(other, [form["video"]], geometry_action="keep")
    assert service.store.entries("exception") == before
    assert service.footage_readiness(form["video"])["issues"][0]["code"] == "CAMERA_NOT_CALIBRATED"
    service.assign_footage(form["camera_id"], [form["video"]], geometry_action="clear")
    exception = service.store.entries("exception")[0]
    assert exception["captured_at_utc"] == "2026-01-01T12:00:00Z"
    assert "frame_width" not in exception


def test_bad_media_blocks_entire_assignment_and_keeps_exception_choices_atomic(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    other = service.store.add_camera("Other", form["location_id"])
    service.save_calibration({**form, "scope": "video", "captured_at_utc": "2026-01-01T12:00:00Z"})
    (service.source.root / "bad.mp4").write_bytes(b"bad")
    before = service.store.entries("exception")
    def probe(path):
        if path.name == "bad.mp4":
            raise VideoValidationError("Broken media")
        return VideoMetadata(1920, 1080, 30)
    monkeypatch.setattr("people_counter.portal_service.probe_video", probe)
    with pytest.raises(VideoValidationError):
        service.assign_footage(other, [form["video"], "bad.mp4"], geometry_action="clear")
    assert service.store.entries("exception") == before
    assert service.footage_assignments() == {form["video"]: form["camera_id"]}


def test_unreadable_media_has_actionable_assignment_error(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    with patch("people_counter.portal_service.probe_video", side_effect=PermissionError("permission denied")):
        client = create_app(service).test_client()
        response = client.post("/footage/assign", data={
            "csrf": csrf(client), "videos": [form["video"]], "camera_id": form["camera_id"],
        })
    assert response.status_code == 400
    assert b"permission denied" in response.data


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
    page = client.get("/calibrate", query_string={"video": videos[1], "camera_id": form["camera_id"]}, follow_redirects=True).data
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


def test_footage_review_has_independent_geometry_and_recording_forms(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    client = create_app(service).test_client()
    library = client.get("/?folder=lobby").get_data(as_text=True)
    assert library.index(">Locations</a>") < library.index(">Cameras</a>") < library.index(">Footage</a>")
    assert "Review footage" in library
    response = client.get("/review", query_string={"video": form["video"], "folder": "lobby"})
    assert response.status_code == 200
    assert b"Save video line exception" in response.data
    assert b"Camera association" in response.data and b"Readiness" in response.data
    assert b"Save recording details" in response.data
    assert b'action="/footage/recording"' in response.data
    assert b"Effective dates select" in response.data


def test_recording_exception_saves_without_decoding_or_changing_geometry(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    service.save_calibration({**form, "scope": "video", "counting_line_x1": "42"})
    client = create_app(service).test_client()
    token = csrf(client)
    with patch.object(service.source, "frame", side_effect=AssertionError("Must not decode")):
        response = client.post("/footage/recording", data={
            "csrf": token, "video": form["video"], "folder": "lobby",
            "captured_at_utc": "2026-01-01T12:34:56",
        })
    assert response.status_code == 302
    saved = service.store.entries("exception")[0]
    assert saved["captured_at_utc"] == "2026-01-01T12:34:56Z"
    assert saved["counting_line_x1"] == "42"
    camera = service.store.entries("camera")[0]
    with patch.object(service.source, "frame", wraps=service.source.frame) as decoded:
        response = client.post("/footage/line", data={
            **form, "csrf": token, "scope": "video", "counting_line_x1": "70",
        })
    decoded.assert_called_once_with(form["video"])
    assert response.status_code == 302
    saved = service.store.entries("exception")[0]
    assert saved["captured_at_utc"] == "2026-01-01T12:34:56Z"
    assert saved["counting_line_x1"] == "70"
    assert service.store.entries("camera")[0] == camera
    response = client.get("/review", query_string={"video": form["video"], "scope": "video"})
    assert b'value="2026-01-01T12:34:56"' in response.data
    assert b'value="70"' in response.data


def test_timestamp_only_exception_and_independent_override_removal(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    client = create_app(service).test_client()
    token = csrf(client)
    assert client.post("/footage/recording", data={
        "csrf": token, "video": form["video"], "captured_at_utc": "2026-01-01T12:00",
    }).status_code == 302
    assert "frame_width" not in service.store.entries("exception")[0]
    assert client.post("/footage/line", data={
        **form, "csrf": token, "scope": "video",
    }).status_code == 302
    assert client.post("/footage/line", data={
        "csrf": token, "video": form["video"], "use_camera": "yes",
    }).status_code == 302
    saved = service.store.entries("exception")[0]
    assert saved["captured_at_utc"] == "2026-01-01T12:00Z"
    assert "frame_width" not in saved
    assert client.post("/footage/recording", data={
        "csrf": token, "video": form["video"], "captured_at_utc": "",
    }).status_code == 302
    assert service.store.entries("exception") == []


def test_camera_line_save_retains_recording_rules(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    config = service.store.entries("camera")[0]
    service.update_camera_metadata(form["camera_id"], config["id"], {
        **config["catalog"], "camera_timezone": "America/Chicago", "capture_time_source": "embedded",
        "capture_time_regex": "", "capture_time_format": "",
    })
    client = create_app(service).test_client()
    before = service.store.entries("camera")
    assert client.post("/footage/line", data={
        **form, "csrf": csrf(client), "config_id": config["id"], "counting_line_x1": "77",
    }).status_code == 400
    assert service.store.entries("camera") == before
    assert client.post(f"/cameras/{form['camera_id']}/calibrate", data={
        **form, "csrf": csrf(client), "config_id": config["id"], "counting_line_x1": "77",
    }).status_code == 302
    updated = service.store.entries("camera")[0]["catalog"]
    assert updated["camera_timezone"] == "America/Chicago"
    assert updated["capture_time_source"] == "embedded"
    assert updated["counting_line_x1"] == "77"


@pytest.mark.parametrize("folder", ["lobby", ""])
def test_named_package_revisions_use_confined_snapshotted_destinations(service, monkeypatch, tmp_path, folder):
    save_camera(service, monkeypatch)
    monkeypatch.setattr("people_counter.portal_service.inspect_video", inspection)
    first = service.enqueue_preparation(folder, name="Morning", destination="north")
    assert service.run_next_job()
    original = service.package_dir(first)
    assert original.is_relative_to(service.output_root)
    before = (original / "manifest-package.json").read_bytes()
    second = service.enqueue_preparation(folder, name="Morning", destination="north")
    assert service.run_next_job()
    packages = service.packages()
    assert [(p["name"], p["revision"]) for p in packages] == [("Morning", 2), ("Morning", 1)]
    assert (original / "manifest-package.json").read_bytes() == before
    restored = PortalService(service.source.root, service.models_root, service.state, output_root=tmp_path / "new-output")
    assert restored.package_dir(first) == original
    assert restored.package_dir(second) != original
    assert restored.archive(first).is_file()
    for destination in ("../outside", "/tmp/outside"):
        with pytest.raises(ValueError, match="output root"):
            service.enqueue_preparation("lobby", name="Unsafe", destination=destination)
    (service.output_root / "escape").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError, match="output root"):
        service.enqueue_preparation("lobby", name="Unsafe", destination="escape")


def test_package_deletion_preserves_reports_history_and_active_references(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    monkeypatch.setattr("people_counter.portal_service.inspect_video", inspection)
    identity = service.enqueue_preparation("lobby", name="Morning")
    assert service.run_next_job()
    service.archive(identity)
    dependent = service.store.enqueue("submit", {"package_id": identity})
    with pytest.raises(ValueError, match="referenced"):
        service.delete_package(identity)
    service.store.finish(dependent, error="Test job stopped")
    service.delete_package(identity)
    assert service.store.job(identity)["status"] == "SUCCEEDED"
    assert service.report(identity, "summary.json").is_file()
    assert (service.source.root / form["video"]).read_bytes() == b"video"
    assert service.packages()[0]["deleted"]
    assert not (service.job_dir(identity) / "manifest-package.zip").exists()
    with pytest.raises(ValueError, match="deleted"):
        service.archive(identity)
    with pytest.raises(ValueError, match="deleted"):
        service.package_dir(identity)
    client = create_app(service).test_client()
    assert b"Artifacts deleted" in client.get("/packages").data


def test_failed_revisions_keep_numbers_and_legacy_packages_remain_readable(service):
    first = service.enqueue_preparation("lobby", name="Morning")
    assert service.run_next_job()
    assert service.store.job(first)["status"] == "FAILED"
    service.enqueue_preparation("lobby", name="Morning")
    assert [p["revision"] for p in service.packages()] == [2, 1]
    legacy = service.enqueue("prepare", {})
    folder = service.job_dir(legacy) / "package"
    folder.mkdir(parents=True)
    (folder / "summary.json").write_text('{"planned":1}')
    assert service.package_dir(legacy) == folder
    assert service.report(legacy, "summary.json").read_text() == '{"planned":1}'
    assert service.packages()[0]["revision"] is None


def test_output_root_overlap_and_existing_revision_are_rejected(service):
    with pytest.raises(ValueError, match="separate"):
        PortalService(
            service.source.root, service.models_root, service.state,
            output_root=service.source.root / "output",
        )
    existing = service.output_root / "morning/v1"
    existing.mkdir(parents=True)
    (existing / "keep.txt").write_text("unchanged")
    with pytest.raises(ValueError, match="destination"):
        service.enqueue_preparation("lobby", name="Morning")
    assert (existing / "keep.txt").read_text() == "unchanged"
    assert service.store.jobs() == []


def test_recording_exception_rejects_bad_timestamp_without_losing_geometry(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    service.save_calibration({**form, "scope": "video"})
    before = service.store.entries("exception")
    client = create_app(service).test_client()
    assert client.post("/footage/recording", data={
        "csrf": csrf(client), "video": form["video"], "captured_at_utc": "not-a-time",
    }).status_code == 400
    assert service.store.entries("exception") == before
    assert client.post("/footage/line", data={
        **form, "csrf": csrf(client), "scope": "video", "counting_line_x1": "-1",
    }).status_code == 400
    assert service.store.entries("exception") == before


def test_review_keeps_recording_details_available_with_multiple_camera_configurations(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    (service.source.root / "side").mkdir()
    (service.source.root / "side/clip.mp4").write_bytes(b"video")
    service.save_calibration({**form, "folder": "side", "video": "side/clip.mp4"})
    client = create_app(service).test_client()
    response = client.get("/review", query_string={"video": form["video"], "folder": "lobby"})
    assert response.status_code == 200
    assert b"Save recording details" in response.data
    assert b"Save video line exception" in response.data
    assert b"Save counting line" not in response.data
    assert b"Choose the recording period" in client.get(f"/cameras/{form['camera_id']}/calibrate").data


def test_package_path_rejects_replaced_revision_symlink(service):
    first = service.enqueue_preparation("lobby", name="Morning")
    second = service.enqueue_preparation("lobby", name="Morning")
    original = service.package_dir(first)
    original.rmdir()
    original.symlink_to(service.package_dir(second), target_is_directory=True)
    with pytest.raises(ValueError, match="destination"):
        service.package_dir(first)


def test_failed_artifact_cleanup_remains_deleted_and_reports_are_retained(service, monkeypatch):
    identity = service.enqueue_preparation("lobby", name="Morning")
    service.store.finish(identity, result={"planned": 1})
    (service.package_dir(identity) / "summary.json").write_text('{"planned":1}')
    with patch("people_counter.portal_service.shutil.rmtree", side_effect=PermissionError("read-only")):
        with pytest.raises(ValueError, match="cleanup"):
            service.delete_package(identity)
    assert service.packages()[0]["deleted"]
    assert service.report(identity, "summary").read_text() == '{"planned":1}'
    with pytest.raises(ValueError, match="deleted"):
        service.archive(identity)


def test_job_reports_use_stable_semantic_ids_and_readable_summary(service):
    identity = service.enqueue("prepare", {})
    root = service.job_dir(identity) / "package"
    root.mkdir(parents=True)
    (root / "summary.json").write_text('{"planned":12,"rejected":1}')
    (root / "rejection-report.csv").write_text("reason,message\nTIME,Missing timestamp\n")
    client = create_app(service).test_client()
    page = client.get(f"/jobs/{identity}").data
    assert b"Download summary JSON" in page
    assert b"View summary.json" not in page
    assert b"planned" in page and b"12" in page
    response = client.get(f"/jobs/{identity}/reports/summary")
    assert response.status_code == 200 and b"<h1>Summary</h1>" in response.data
    assert b"Download summary JSON" in response.data
    assert client.get(f"/jobs/{identity}/files/summary").status_code == 200
    assert client.get(f"/jobs/{identity}/files/../../secret").status_code == 404


def test_processing_overrides_are_snapshotted_without_changing_approved_defaults(service, monkeypatch):
    from people_counter import backfill_operator as backfill
    from people_counter.inference_profiles import artifact_paths, resolve_profile

    identity = service.enqueue("prepare", {})
    service.store.finish(identity, result={"planned": 1})
    preset = "rtdetr-r18-onnx-cpu"
    profile = resolve_profile(preset, {path: "a" * 64 for path in artifact_paths(preset)})
    binding = {
        "workspace_id": "11111111-1111-1111-1111-111111111111",
        "lakehouse_id": "22222222-2222-2222-2222-222222222222",
        "environment_id": "33333333-3333-3333-3333-333333333333",
        "control_sjd_id": "44444444-4444-4444-4444-444444444444",
        "storage_account": "testaccount", "filesystem": "footage",
        "shortcut_path": "Files/videos", "table_directory": "Tables/dbo", "intake_mode": "explicit",
    }
    service.catalog.write_text(json.dumps({
        "schema": backfill.CATALOG_SCHEMA,
        "targets": {"prod": {"binding": binding, "profiles": {preset: profile}}},
    }))
    before = service.catalog.read_bytes()
    service.allow_cloud = True
    client = create_app(service).test_client()
    response = client.post("/cloud", data={
        "csrf": csrf(client), "confirm": "yes", "package_id": identity,
        "selection": "prod::" + preset, "action": "submit", "batch_size": "4",
        "detection_threshold": "0.35", "sample_fps": "2", "capacity": "F64",
    })
    assert response.status_code == 302
    job = service.store.job(response.headers["Location"].rsplit("/", 1)[1])
    assert job["kind"] == "submit" and job["status"] == "QUEUED"
    assert job["payload"]["package_id"] == identity
    assert job["payload"]["wait"] is False
    assert job["payload"]["config"]["profile"]["detection_threshold"] == 0.35
    assert job["payload"]["config"]["profile"]["batch_size"] == 4
    assert job["payload"]["config"]["profile"]["sample_fps"] == 2.0
    assert job["payload"]["capacity"] == {"sku": "F64", "source": "operator-declared"}
    assert job["payload"]["config"]["profile"]["runtime_key"] != profile["runtime_key"]
    assert service.catalog.read_bytes() == before
    assert b"Effective inference settings" in client.get(response.headers["Location"]).data
    default_job = service.store.job(service.enqueue_cloud(identity, "prod", preset, "submit", True))
    assert default_job["payload"]["wait"] is True
    assert "capacity" not in default_job["payload"]
    assert default_job["payload"]["config"]["profile"] == profile
    for capacity in ("F1", "F4096"):
        with pytest.raises(ValueError, match="capacity"):
            service.enqueue_cloud(identity, "prod", preset, "submit", False, capacity=capacity)
    invalid = client.post("/cloud", data={
        "csrf": csrf(client), "confirm": "yes", "package_id": identity,
        "selection": "prod::" + preset, "action": "publish", "detection_threshold": "0.35",
    })
    assert invalid.status_code == 400


def test_direct_cloud_queueing_still_obeys_disabled_cloud_gate(service):
    identity = service.enqueue("prepare", {})
    service.store.finish(identity, result={"planned": 1})
    with patch("people_counter.portal_service.backfill.load_target", side_effect=AssertionError("Must not resolve")):
        with pytest.raises(ValueError, match="disabled"):
            service.enqueue_cloud(identity, "prod", "profile", "submit", False)


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
    semantic = "summary" if name.endswith(".json") else "rejections"
    assert f"/jobs/{identity}/reports/{semantic}".encode() in page
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
    path.write_text("")
    empty = client.get(f"/jobs/{identity}/reports/rejection-report.csv")
    assert empty.status_code == 200 and b"No rows" in empty.data


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


def test_legacy_new_flag_does_not_create_another_calibration(service, monkeypatch):
    form = save_camera(service, monkeypatch)
    client = create_app(service).test_client()
    page = client.get("/calibrate", query_string={
        "video": form["video"], "camera_id": form["camera_id"], "new": "1",
    }, follow_redirects=True)
    assert page.status_code == 200
    assert b'name="new"' not in page.data
    assert b'value="1900"' in page.data
    assert len(service.store.entries("camera")) == 1


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
