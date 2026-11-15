from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from people_counter.operator_portal import create_app
from people_counter.portal_service import PortalService
from people_counter.portal_sources import AdlsFootageSource
from tests.test_operator_portal import FakePortalAzure, csrf


@pytest.fixture
def explorer(tmp_path):
    source = AdlsFootageSource({"kind": "adls", "account": "footageaccount", "filesystem": "videos", "root": ""}, FakePortalAzure())
    service = PortalService(source, tmp_path / "models", tmp_path / "state")
    service._intake = SimpleNamespace(
        page=Mock(return_value=([{"name": "clip.mp4", "path": "clip.mp4", "directory": False, "video": True,
                                 "size_bytes": 10, "modified_utc": "", "blocked": False, "error": None}], "next-page")),
        start_upload=Mock(return_value={"id": "a" * 32, "state": "uploading", "received": 0}),
        append_upload=Mock(return_value={"id": "a" * 32, "state": "uploading", "received": 3}),
        assert_available=Mock(),
    )
    source.page = service._intake.page
    app = create_app(service)
    app.config["TESTING"] = True
    return app, service


def test_explorer_is_fixed_adls_and_new_packages_skip_source_selection(explorer):
    app, service = explorer
    client = app.test_client()
    response = client.get("/footage")
    assert response.status_code == 200
    assert b"Upload" in response.data and b"next-page" in response.data
    response = client.post("/packages/new", data={"csrf": csrf(client), "package_name": "Morning"})
    assert "step=footage" in response.location
    assert service.store.entries("draft")[0]["source"] == service.source.descriptor
    assert client.get("/identity/login").status_code == 404


def test_upload_chunks_need_header_csrf_and_never_parse_multipart(explorer):
    app, service = explorer
    client = app.test_client()
    token = csrf(client)
    response = client.post("/intake/uploads", data={"csrf": token, "folder": "", "name": "clip.mp4", "size": "3"})
    assert response.status_code == 200
    assert service._intake.start_upload.call_args.args == ("", "clip.mp4", 3)
    url = "/intake/uploads/" + "a" * 32 + "/chunks?offset=0"
    assert client.put(url, data=b"abc", content_type="application/octet-stream").status_code == 403
    response = client.put(url, data=b"abc", content_type="application/octet-stream", headers={"X-CSRF-Token": token})
    assert response.status_code == 200
    assert service._intake.append_upload.call_args.args == ("a" * 32, 0, b"abc")
    assert client.put(url, data=b"x", headers={"X-CSRF-Token": token, "Origin": "https://evil.example"}).status_code == 403


def test_reserved_paths_cannot_be_selected_or_generated(explorer):
    _, service = explorer
    service.source.properties = lambda video: {"size": 10, "etag": '"version"'}
    draft = service.new_draft("Reserved")
    service._intake.assert_available.side_effect = ValueError("reserved for a storage mutation")
    with pytest.raises(ValueError, match="reserved"):
        service.update_draft(draft["id"], {"videos": ["clip.mp4"]})
    service.store.put("draft", draft["id"], {**draft, "videos": ["clip.mp4"]})
    with pytest.raises(ValueError, match="reserved"):
        service.generate_draft(draft["id"])


def test_legacy_metadata_deletion_keeps_independent_catalog_and_job_history(explorer):
    app, service = explorer
    location = service.store.add_location("North")
    camera = service.store.add_camera("Entrance", location)
    identity = "d" * 32
    service.store.put("draft", identity, {"name": "Legacy", "source": {"kind": "local", "root": "/old-root"},
                                        "videos": ["clip.mp4"], "folder": "", "observations": {}})
    service.store.put("footage", "clip.mp4", {"camera_id": camera})
    job = service.store.enqueue("prepare", {"source": {"kind": "local", "root": "/old-root"}, "videos": ["clip.mp4"]})
    client = app.test_client()
    assert b"read-only history" in client.get("/packages/drafts/" + identity).data
    url = "/packages/drafts/" + identity + "/delete-local"
    assert client.post(url, data={"csrf": csrf(client), "confirm": "yes"}).status_code == 400
    service.store.finish(job, {})
    response = client.post(url, data={"csrf": csrf(client), "confirm": "yes"})
    assert response.status_code == 302
    assert service.store.entries("draft") == []
    assert service.store.entries("footage") == []
    assert service.store.locations()[0]["id"] == location
    assert service.store.entries("camera_identity")[0]["id"] == camera
    assert service.store.job(job)["status"] == "SUCCEEDED"


def test_an_intake_recovery_outcome_is_not_a_successful_job(explorer):
    _, service = explorer
    service._intake.complete = Mock(return_value={"state": "recovery", "message": "Native move failed"})
    job = service.enqueue("intake", {"operation_id": "a" * 32})
    service.run_next_job()
    assert service.store.job(job)["status"] == "FAILED"
    assert "Native move failed" in service.store.job(job)["error"]


def test_fresh_review_updates_only_reviewed_versions_and_keeps_other_replacements_blocked(explorer):
    _, service = explorer
    service.media_metadata = Mock()
    service.source.properties = lambda video: {"size": 10, "etag": "new-" + video}
    draft = service.new_draft("Replacement")
    service.store.put("draft", draft["id"], {**draft, "videos": ["clip.mp4", "second.mp4"], "review_required": True,
                                           "observations": {"clip.mp4": {"size": 1, "etag": "old"}, "second.mp4": {"size": 1, "etag": "old"}}})
    for video in ("clip.mp4", "second.mp4"):
        service.store.put("source_review", service.source.relative_video(video), {"required": True})
    service.clear_intake_review("clip.mp4")
    saved = service.draft(draft["id"])
    assert saved["observations"]["clip.mp4"]["etag"] == "new-clip.mp4"
    assert saved["observations"]["second.mp4"]["etag"] == "old"
    assert saved["review_required"] is True
    service.clear_intake_review("second.mp4")
    assert service.store.entries("source_review") == []
    assert service.draft(draft["id"])["review_required"] is False
    assert service.draft(draft["id"])["videos"] == ["clip.mp4", "second.mp4"]


def test_unreadable_replacement_cannot_clear_required_review(explorer):
    _, service = explorer
    relative = service.source.relative_video("clip.mp4")
    service.store.put("source_review", relative, {"required": True})
    service.media_metadata = Mock(side_effect=ValueError("Unreadable replacement"))
    with pytest.raises(ValueError, match="Unreadable"):
        service.clear_intake_review("clip.mp4")
    assert service.store.entries("source_review")[0]["id"] == relative
