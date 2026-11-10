import json
import zipfile

import pytest

pytest.importorskip("flask")
pytest.importorskip("playwright")

from playwright.sync_api import expect

pytestmark = pytest.mark.ui


def calibrate_camera(page, portal):
    page.goto(portal.url + "/?folder=lobby")
    page.get_by_role("link", name="Calibrate camera using clip.mp4", exact=True).click()
    page.wait_for_load_state("load")
    expect(page.locator("#preview-image")).to_have_js_property("naturalWidth", 320)
    editor = page.get_by_label("Counting line editor")
    editor.scroll_into_view_if_needed()
    bounds = editor.bounding_box()
    assert bounds is not None
    page.mouse.click(bounds["x"] + bounds["width"] * .1, bounds["y"] + bounds["height"] * .5)
    page.mouse.click(bounds["x"] + bounds["width"] * .9, bounds["y"] + bounds["height"] * .5)
    expect(page.get_by_label("X1", exact=True)).to_have_value("32")
    expect(page.get_by_label("X2", exact=True)).to_have_value("288")
    expect(page.get_by_label("Y1", exact=True)).to_have_value("90")
    page.get_by_label("Camera ID", exact=True).fill("camera-17")
    page.get_by_label("Location ID", exact=True).fill("north-entrance")
    page.get_by_label("Camera timezone", exact=True).fill("America/Denver")
    page.get_by_label("Preview time in seconds", exact=True).fill("0.2")
    page.get_by_role("button", name="Show frame", exact=True).click()
    expect(page.get_by_label("Camera ID", exact=True)).to_have_value("camera-17")
    expect(page.get_by_label("X1", exact=True)).to_have_value("32")
    page.get_by_role("button", name="Save calibration", exact=True).click()
    expect(page.get_by_role("heading", name="Camera metadata", exact=True)).to_be_visible()
    expect(page.get_by_role("cell", name="camera-17 / north-entrance America/Denver")).to_be_visible()


def prepare_package(page, portal):
    page.goto(portal.url + "/packages?folder=lobby")
    page.get_by_role("button", name="Generate manifest package", exact=True).click()
    expect(page.locator("[data-job-status]")).to_have_text("SUCCEEDED", timeout=30000)
    return page.url.rsplit("/", 1)[-1]


@pytest.mark.parametrize("viewport", [{"width": 1280, "height": 900}, {"width": 390, "height": 844}])
def test_draw_camera_line_prepare_and_download_metadata_only_package(page, portal, tmp_path, viewport):
    page.set_viewport_size(viewport)
    calibrate_camera(page, portal)
    identity = prepare_package(page, portal)
    with page.expect_download() as downloaded:
        page.get_by_role("link", name="Download manifest package ZIP").click()
    archive = tmp_path / "package.zip"
    downloaded.value.save_as(archive)
    with zipfile.ZipFile(archive) as package:
        index = json.loads(package.read("manifest-package.json"))
        assert index["complete"] is True and index["prepared"] == 1
        manifest = json.loads(package.read(index["entries"][0]["prepared_manifest_path"]))
        assert manifest["counting_line"] == [32, 90, 288, 90]
        assert manifest["camera_id"] == "camera-17"
        assert manifest["camera_timezone"] == "America/Denver"
        assert not any(name.endswith(".mp4") for name in package.namelist())
    assert portal.service.store.job(identity)["status"] == "SUCCEEDED"
    assert portal.calls == []


def test_per_video_exception_preserves_camera_defaults_and_original_timestamp(page, portal):
    calibrate_camera(page, portal)
    portal.make_video("lobby/wide.mp4", width=640, height=360)
    page.goto(portal.url + "/calibrate?video=lobby/wide.mp4&folder=lobby&scope=video")
    page.get_by_label("X1", exact=True).fill("12")
    page.get_by_label("Y1", exact=True).fill("30")
    page.get_by_label("X2", exact=True).fill("620")
    page.get_by_label("Y2", exact=True).fill("30")
    page.get_by_role("button", name="Save calibration").click()
    expect(page.get_by_text("videos/lobby/wide.mp4:", exact=False)).to_be_visible()
    identity = prepare_package(page, portal)
    index = json.loads((portal.service.job_dir(identity) / "package/manifest-package.json").read_text())
    entry = next(item for item in index["entries"] if item["video_relative_path"].endswith("wide.mp4"))
    manifest = json.loads((
        portal.service.job_dir(identity) / "package" / entry["prepared_manifest_path"]
    ).read_text())
    assert manifest["counting_line"] == [12, 30, 620, 30]
    assert manifest["frame_width"] == 640 and manifest["frame_height"] == 360
    assert manifest["captured_at_utc"] == "2026-01-01T12:00:00Z"
    assert portal.service.store.entries("camera")[0]["catalog"]["counting_line_x1"] == "32"


def test_background_job_continues_after_browser_leaves_and_errors_are_visible(page, portal):
    calibrate_camera(page, portal)
    (portal.source / "lobby/broken.mp4").write_bytes(b"not a video")
    page.goto(portal.url + "/packages?folder=lobby")
    page.get_by_role("button", name="Generate manifest package").click()
    identity = page.url.rsplit("/", 1)[-1]
    page.goto(portal.url + "/")
    page.goto(portal.url + "/jobs/" + identity)
    expect(page.locator("[data-job-status]")).to_have_text("FAILED", timeout=15000)
    expect(page.get_by_role("alert")).to_contain_text("rejected")
    expect(page.get_by_role("link", name="rejection-report.csv")).to_be_visible()
    assert portal.service.store.job(identity)["status"] == "FAILED"


def test_cloud_gate_csrf_and_source_boundaries_are_enforced_in_browser(page, portal):
    page.goto(portal.url + "/cloud")
    expect(page.get_by_role("button", name="Start cloud action")).to_be_disabled()
    token = page.locator("[name=csrf]").input_value()
    assert page.request.post(portal.url + "/cloud", form={"csrf": token, "confirm": "yes"}).status == 403
    assert page.request.post(portal.url + "/packages", form={"folder": "lobby"}).status == 403
    response = page.goto(portal.url + "/?folder=../../")
    assert response.status == 400
    expect(page.get_by_role("alert")).to_contain_text("source root")
    assert portal.calls == []


@pytest.mark.portal_cloud
def test_profile_generation_publication_and_submit_wait_use_shared_backends(page, portal):
    calibrate_camera(page, portal)
    package_id = prepare_package(page, portal)
    page.get_by_role("link", name="Profiles", exact=True).click()
    fields = {
        "Target name": "ci",
        "Workspace ID": "11111111-1111-1111-1111-111111111111",
        "Lakehouse ID": "22222222-2222-2222-2222-222222222222",
        "Environment ID": "33333333-3333-3333-3333-333333333333",
        "Control SJD ID": "44444444-4444-4444-4444-444444444444",
        "Storage account": "testaccount", "Filesystem": "footage",
    }
    for label, value in fields.items():
        page.get_by_label(label, exact=True).fill(value)
    page.get_by_label("rtdetr-r18-onnx-cpu", exact=True).check()
    page.get_by_text("Advanced inference overrides (optional)", exact=True).click()
    page.get_by_label("Batch size override", exact=True).select_option("2")
    page.get_by_label("Sampling FPS override", exact=True).fill("2")
    page.get_by_role("button", name="Generate profiles", exact=True).click()
    expect(page.locator("[data-job-status]")).to_have_text("SUCCEEDED", timeout=30000)
    profile = json.loads(portal.service.catalog.read_text())["targets"]["ci"]["profiles"]["rtdetr-r18-onnx-cpu"]
    assert profile["batch_size"] == 2 and profile["sample_fps"] == 2.0
    for action, wait in (("publish", False), ("submit", False), ("submit", True)):
        page.goto(portal.url + "/cloud")
        page.get_by_label("Prepared package", exact=True).select_option(package_id)
        page.get_by_label("Target and profile", exact=True).select_option("ci::rtdetr-r18-onnx-cpu")
        page.get_by_label("Action", exact=True).select_option(action)
        if wait:
            page.get_by_label("Wait for processing after submission").check()
        page.get_by_label("I reviewed the target and intake policy and approve this cloud action.").check()
        page.get_by_role("button", name="Start cloud action", exact=True).click()
        expect(page.locator("[data-job-status]")).to_have_text("SUCCEEDED", timeout=30000)
    expect(page.get_by_role("heading", name="Result", exact=True)).to_be_visible()
    expect(page.locator("#job-log")).to_contain_text("1/1 committed")
    assert len(portal.calls) == 2
