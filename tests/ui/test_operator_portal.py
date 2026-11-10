import json
import zipfile
from urllib.parse import parse_qs, urlsplit

import pytest

pytest.importorskip("flask")
pytest.importorskip("playwright")

from playwright.sync_api import expect

pytestmark = pytest.mark.ui


@pytest.mark.parametrize("viewport", [{"width": 1440, "height": 1000}, {"width": 390, "height": 844}])
def test_folder_explorer_lazy_navigation_filters_metadata_and_context(page, portal, viewport):
    page.set_viewport_size(viewport)
    portal.make_video("site/east/clip.mp4")
    (portal.source / "site/east/nested").mkdir()
    (portal.source / "site/east/readme.txt").write_text("notes")
    (portal.source / "unopened/deep").mkdir(parents=True)
    requested = []
    page.on("request", lambda request: requested.append(request.url) if urlsplit(request.url).path == "/folders" else None)
    page.goto(portal.url + "/")
    assert requested == []
    if viewport["width"] < 1024:
        page.get_by_role("button", name="Folders", exact=True).click()
    folders = page.get_by_role("navigation", name="Folders", exact=True)
    site = folders.locator('details[data-folder="site"] > summary')
    site.focus()
    with page.expect_response(lambda response: urlsplit(response.url).path == "/folders"):
        site.press("Enter")
    expect(folders.get_by_role("link", name="east", exact=True)).to_be_visible()
    east = folders.locator('details[data-folder="site/east"] > summary')
    east.focus()
    with page.expect_response(lambda response: urlsplit(response.url).path == "/folders"):
        east.press("Enter")
    expect(folders.get_by_role("link", name="nested", exact=True)).to_be_visible()
    east.press("Enter")
    east.press("Enter")
    assert [parse_qs(urlsplit(url).query)["folder"][0] for url in requested] == ["site", "site/east"]
    folders.get_by_role("link", name="east", exact=True).click()
    expect(page.get_by_role("navigation", name="Folder path")).to_contain_text("site")
    expect(page.get_by_role("navigation", name="Folder path")).to_contain_text("east")
    page.get_by_label("Filter files by name", exact=True).fill("CLIP")
    page.get_by_label("File type", exact=True).select_option("videos")
    page.get_by_role("button", name="Apply filters", exact=True).click()
    table = page.get_by_role("table", name="Folder contents", exact=True)
    expect(table.locator("tbody tr[data-entry-path]")).to_have_count(1)
    row = table.locator('tr[data-entry-path="site/east/clip.mp4"]')
    expect(row).to_contain_text("Video")
    expect(row).to_contain_text("UTC")
    expect(row).not_to_contain_text("Unavailable")
    expect(page.get_by_role("link", name="Calibrate counting line for clip.mp4", exact=True)).to_have_attribute(
        "href", "/calibrate?video=site/east/clip.mp4&folder=site/east",
    )
    expect(page.get_by_role("link", name="Generate manifest package", exact=True)).to_have_attribute(
        "href", "/packages?folder=site/east",
    )
    page.get_by_label("Filter files by name", exact=True).fill("no-such-file")
    page.get_by_role("button", name="Apply filters", exact=True).click()
    expect(page.get_by_text("No matching items", exact=True)).to_be_visible()
    page.get_by_role("link", name="Clear filters", exact=True).click()
    expect(table.locator("tbody tr[data-entry-path]")).to_have_count(3)
    page.get_by_role("link", name="Open folder nested", exact=True).click()
    expect(page.get_by_text("This folder is empty.", exact=True)).to_be_visible()
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
    assert portal.calls == []


@pytest.mark.parametrize("failure", [503, 200])
def test_folder_explorer_reports_lazy_load_failures_and_retries(page, portal, failure):
    (portal.source / "unopened/child").mkdir(parents=True)
    unsafe_name = '<img src=x onerror="alert(1)">'
    (portal.source / "unopened" / unsafe_name).mkdir()
    attempts = []

    def folders_response(route):
        attempts.append(route.request.url)
        if len(attempts) == 1:
            route.fulfill(status=failure, content_type="application/json", body='{"unexpected": []}')
        else:
            route.continue_()

    page.route("**/folders?folder=unopened", folders_response)
    page.goto(portal.url + "/")
    folders = page.get_by_role("navigation", name="Folders", exact=True)
    node = folders.locator('details[data-folder="unopened"] > summary')
    node.focus()
    node.press("Enter")
    expect(folders.get_by_role("alert")).to_contain_text("Unable to load folders")
    folders.get_by_role("button", name="Retry folder unopened", exact=True).click()
    expect(folders.get_by_role("link", name="child", exact=True)).to_be_visible()
    expect(folders.get_by_role("link", name=unsafe_name, exact=True)).to_be_visible()
    expect(folders.locator("img")).to_have_count(0)
    expect(folders.get_by_role("alert")).to_have_count(0)
    assert len(attempts) == 2


def ensure_location(page, portal, location):
    page.goto(portal.url + "/locations")
    row = page.locator(f'tr[data-location-id="{location}"]')
    if row.count() == 0:
        page.get_by_role("button", name="Add location", exact=True).click()
        page.get_by_label("Location name", exact=True).fill(location)
        page.get_by_role("dialog", name="Add a location", exact=True).get_by_role("button", name="Add location", exact=True).click()
    return row


def calibrate_camera(page, portal, *, camera_id="camera-17", location="north-entrance", folder="lobby"):
    ensure_location(page, portal, location)
    registered_id = f"{location}-{camera_id}"
    page.goto(portal.url + "/cameras")
    if page.locator(f'tr[data-camera-id="{registered_id}"]').count() == 0:
        page.get_by_role("button", name="Add camera", exact=True).click()
        page.get_by_label("Location", exact=True).select_option(location)
        page.get_by_label("Camera name", exact=True).fill(camera_id)
        page.get_by_role("dialog", name="Add a camera", exact=True).get_by_role("button", name="Add camera", exact=True).click()
    page.goto(portal.url + "/?folder=" + folder)
    page.get_by_role("link", name="Calibrate counting line for clip.mp4", exact=True).click()
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
    page.get_by_label("Location", exact=True).select_option(location)
    page.get_by_label("Camera", exact=True).select_option(registered_id)
    page.get_by_label("Camera timezone", exact=True).select_option("America/Denver")
    page.get_by_label("Preview time in seconds", exact=True).fill("0.2")
    page.get_by_role("button", name="Show frame", exact=True).click()
    expect(page.get_by_label("Camera", exact=True)).to_have_value(registered_id)
    expect(page.get_by_label("X1", exact=True)).to_have_value("32")
    page.get_by_role("button", name="Save calibration", exact=True).click()
    expect(page.get_by_role("heading", name="Camera metadata", exact=True)).to_be_visible()
    row = page.get_by_role("row").filter(has=page.get_by_role("cell", name=folder, exact=True))
    expect(row).to_contain_text(registered_id)
    expect(row).to_contain_text("America/Denver")


def prepare_package(page, portal):
    page.goto(portal.url + "/packages?folder=lobby")
    expect(page.get_by_role("dialog", name="Generate a package", exact=True)).to_be_visible()
    expect(page.get_by_label("Folder within source root", exact=True)).to_have_value("lobby")
    page.get_by_role("dialog", name="Generate a package", exact=True).get_by_role("button", name="Generate manifest package", exact=True).click()
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
        assert manifest["camera_id"] == "north-entrance-camera-17"
        assert manifest["camera_timezone"] == "America/Denver"
        assert not any(name.endswith(".mp4") for name in package.namelist())
    assert portal.service.store.job(identity)["status"] == "SUCCEEDED"
    assert portal.calls == []


def test_per_video_exception_preserves_camera_defaults_and_original_timestamp(page, portal):
    calibrate_camera(page, portal)
    portal.make_video("lobby/wide.mp4", width=640, height=360)
    page.goto(portal.url + "/?folder=lobby")
    page.get_by_role("link", name="Calibrate counting line for wide.mp4", exact=True).click()
    page.get_by_label("Apply calibration to", exact=True).select_option("video")
    expect(page.get_by_label("Location", exact=True)).to_be_disabled()
    expect(page.get_by_label("Camera timezone", exact=True)).to_be_disabled()
    expect(page.get_by_label("Authoritative capture time override (UTC, optional)", exact=True)).to_be_enabled()
    page.get_by_text("Manual line coordinates", exact=True).click()
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
    page.get_by_role("dialog", name="Generate a package", exact=True).get_by_role("button", name="Generate manifest package", exact=True).click()
    identity = page.url.rsplit("/", 1)[-1]
    page.goto(portal.url + "/")
    page.goto(portal.url + "/jobs/" + identity)
    expect(page.locator("[data-job-status]")).to_have_text("FAILED", timeout=15000)
    expect(page.get_by_role("alert")).to_contain_text("rejected")
    expect(page.locator("#job-log")).to_be_visible()
    expect(page.get_by_role("link", name="rejection-report.csv")).to_be_visible()
    assert portal.service.store.job(identity)["status"] == "FAILED"
    original = portal.service.store.job(identity)["payload"]
    page.get_by_role("button", name="Resume with retained state", exact=True).click()
    expect(page.get_by_role("dialog", name="Resume safely", exact=True)).to_be_visible()
    with page.expect_response(
        lambda response: response.request.method == "POST" and response.url.endswith("/resume"),
    ) as resumed:
        page.get_by_role("dialog", name="Resume safely", exact=True).get_by_role("button", name="Resume with retained state", exact=True).click()
    assert resumed.value.status == 302
    assert page.url.endswith("/jobs/" + identity)
    assert portal.service.store.job(identity)["payload"] == original
    assert len(portal.service.store.jobs()) == 1


def test_cloud_gate_csrf_and_source_boundaries_are_enforced_in_browser(page, portal):
    for route, label in (("/cameras", "Add camera"), ("/packages", "Generate manifest package")):
        page.goto(portal.url + route)
        page.wait_for_load_state("load")
        expect(page.get_by_role("button", name=label, exact=True)).to_be_disabled()
    page.goto(portal.url + "/cloud")
    expect(page.get_by_role("button", name="Start cloud action")).to_be_disabled()
    token = page.locator("[name=csrf]").input_value()
    assert page.request.post(portal.url + "/cloud", form={"csrf": token, "confirm": "yes"}).status == 403
    assert page.request.post(portal.url + "/packages", form={"folder": "lobby"}).status == 403
    response = page.goto(portal.url + "/?folder=../../")
    assert response.status == 400
    expect(page.get_by_role("alert")).to_contain_text("source root")
    assert portal.calls == []


def test_locations_support_zero_many_cameras_and_reject_split_assignments(page, portal):
    empty = ensure_location(page, portal, "empty-warehouse")
    expect(empty).to_have_attribute("data-camera-count", "0")
    expect(empty.get_by_role("cell", name="No cameras yet", exact=True)).to_be_visible()
    calibrate_camera(page, portal)
    portal.make_video("side/clip.mp4")
    calibrate_camera(page, portal, camera_id="camera-18", folder="side")
    portal.make_video("later/clip.mp4")
    calibrate_camera(page, portal, folder="later")
    page.get_by_role("link", name="Locations", exact=True).click()
    north = page.locator('tr[data-location-id="north-entrance"]')
    expect(north).to_have_attribute("data-camera-count", "2")
    expect(north.get_by_role("cell", name="2", exact=True)).to_be_visible()
    expect(north).to_contain_text("north-entrance-camera-17")
    expect(north).to_contain_text("north-entrance-camera-18")
    expect(page.locator('tr[data-location-id="empty-warehouse"]')).to_have_attribute("data-camera-count", "0")
    page.get_by_role("link", name="Cameras", exact=True).click()
    page.get_by_role("link", name="Edit north-entrance-camera-17", exact=True).first.click()
    page.get_by_label("Location", exact=True).select_option("empty-warehouse")
    expect(page.get_by_label("Camera", exact=True)).to_be_disabled()
    page.get_by_label("Location", exact=True).select_option("north-entrance")
    page.get_by_label("Camera", exact=True).select_option("north-entrance-camera-17")
    page.locator("#camera-location").evaluate("element => element.value = 'empty-warehouse'")
    page.get_by_role("button", name="Save calibration", exact=True).click()
    expect(page.get_by_role("alert")).to_contain_text("location")
    assert all(
        item["catalog"]["location_id"] == "north-entrance"
        for item in portal.service.store.entries("camera")
    )
    page.get_by_role("link", name="Locations", exact=True).click()
    expect(page.locator('tr[data-location-id="north-entrance"]')).to_have_attribute("data-camera-count", "2")


@pytest.mark.parametrize("viewport", [{"width": 1440, "height": 1000}, {"width": 390, "height": 844}])
def test_tailwind_design_covers_every_page_and_accessible_controls(page, portal, viewport):
    page.set_viewport_size(viewport)
    routes = [
        "/", "/locations", "/cameras", "/packages", "/profiles", "/cloud", "/jobs",
        "/calibrate?video=lobby/clip.mp4&folder=lobby", "/not-found",
    ]
    for route in routes:
        response = page.goto(portal.url + route)
        assert response.status == (404 if route == "/not-found" else 200)
        stylesheet = page.locator('link[rel="stylesheet"]')
        expect(stylesheet).to_have_attribute("href", "/static/build/portal.css")
        heading = page.get_by_role("heading", level=1)
        expect(heading).to_be_visible()
        assert heading.evaluate("(element) => parseFloat(getComputedStyle(element).fontSize)") >= 24
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        expect(page.get_by_role("navigation", name="Main navigation").get_by_role("link")).to_have_count(7)
        headers = page.get_by_role("columnheader")
        if headers.count():
            assert headers.first.evaluate("(element) => parseFloat(getComputedStyle(element).paddingLeft)") >= 12
    page.goto(portal.url + "/profiles")
    creation = page.get_by_role("button", name="Add target and profiles", exact=True).bounding_box()
    catalog = page.get_by_role("heading", name="Approved target catalog", exact=True).bounding_box()
    assert creation is not None and catalog is not None and creation["y"] < catalog["y"]
    page.get_by_role("button", name="Add target and profiles", exact=True).click()
    field = page.get_by_label("Workspace ID", exact=True)
    assert field.evaluate("(element) => parseFloat(getComputedStyle(element).borderRadius)") >= 8
    initial_shadow = field.evaluate("(element) => getComputedStyle(element).boxShadow")
    field.focus()
    page.wait_for_function(
        "args => getComputedStyle(args[0]).boxShadow !== args[1]",
        arg=[field.element_handle(), initial_shadow],
    )
    assert field.evaluate("(element) => getComputedStyle(element).boxShadow") != initial_shadow
    page.get_by_text("Advanced inference overrides (optional)", exact=True).click()
    combo = page.get_by_label("Batch size override", exact=True)
    assert combo.evaluate("(element) => parseFloat(getComputedStyle(element).borderRadius)") >= 8
    expect(page.get_by_role("button", name="Generate profiles", exact=True)).to_be_visible()
    page.goto(portal.url + "/")
    page.keyboard.press("Tab")
    expect(page.get_by_role("link", name="Skip to main content", exact=True)).to_be_focused()


@pytest.mark.parametrize("viewport", [{"width": 1440, "height": 1000}, {"width": 390, "height": 844}])
def test_named_camera_registration_and_location_filtered_calibration(page, portal, viewport):
    page.set_viewport_size(viewport)
    for location, identity in (("North Store", "north-store"), ("South Store", "south-store")):
        page.goto(portal.url + "/locations")
        page.get_by_role("button", name="Add location", exact=True).click()
        page.get_by_label("Location name", exact=True).fill("S\u00e3o Paulo North")
        expect(page.locator("[data-generated-id]")).to_have_text("sao-paulo-north")
        page.get_by_label("Location name", exact=True).fill(location)
        expect(page.locator("[data-generated-id]")).to_have_text(identity)
        page.get_by_role("dialog", name="Add a location", exact=True).get_by_role("button", name="Add location", exact=True).click()
        row = page.locator(f'tr[data-location-id="{identity}"]')
        expect(row).to_have_attribute("data-camera-count", "0")
        row.get_by_role("link", name="Add camera", exact=True).click()
        expect(page.get_by_label("Location", exact=True)).to_have_value(identity)
        page.get_by_role("button", name="Cancel", exact=True).click()
        expect(page.locator("#open-camera-dialog")).to_be_focused()
        page.get_by_role("button", name="Add camera", exact=True).click()
        expect(page.get_by_label("Location", exact=True)).to_have_value(identity)
        page.get_by_label("Camera name", exact=True).fill("Main Entrance")
        expect(page.locator("[data-generated-id]")).to_have_text(f"{identity}-main-entrance")
        page.get_by_role("dialog", name="Add a camera", exact=True).get_by_role("button", name="Add camera", exact=True).click()
        registered = page.locator(f'tr[data-camera-id="{identity}-main-entrance"]')
        expect(registered).to_contain_text("Main Entrance")
        expect(registered).to_contain_text("Not calibrated")
    assert portal.service.store.entries("camera") == []
    page.goto(portal.url + "/?folder=lobby")
    expect(page.get_by_role("link", name="Calibrate counting line for clip.mp4", exact=True)).to_have_count(1)
    expect(page.get_by_role("link", name="Video exception for clip.mp4", exact=True)).to_have_count(0)
    page.get_by_role("link", name="Calibrate counting line for clip.mp4", exact=True).click()
    location = page.get_by_label("Location", exact=True)
    camera = page.get_by_label("Camera", exact=True)
    expect(camera).to_be_disabled()
    location.select_option("north-store")
    expect(camera.locator('option:not([value=""])')).to_have_count(1)
    camera.select_option("north-store-main-entrance")
    location.select_option("south-store")
    expect(camera).to_have_value("")
    expect(camera.locator('option[value="north-store-main-entrance"]')).to_have_count(0)
    camera.select_option("south-store-main-entrance")
    timezone = page.get_by_label("Camera timezone", exact=True)
    expect(timezone).to_have_js_property("tagName", "SELECT")
    timezone.select_option("America/Denver")
    page.get_by_role("button", name="Save calibration", exact=True).click()
    expect(page.get_by_role("heading", name="Camera metadata", exact=True)).to_be_visible()
    saved = portal.service.store.entries("camera")[0]["catalog"]
    assert saved["camera_id"] == "south-store-main-entrance"
    assert saved["location_id"] == "south-store"
    assert saved["camera_timezone"] == "America/Denver"
    page.get_by_role("link", name="Edit south-store-main-entrance", exact=True).click()
    expect(page.get_by_label("Location", exact=True)).to_have_value("south-store")
    expect(page.get_by_label("Camera", exact=True)).to_have_value("south-store-main-entrance")
    expect(page.get_by_label("Camera timezone", exact=True)).to_have_value("America/Denver")


@pytest.mark.portal_cloud
@pytest.mark.parametrize("viewport", [{"width": 1440, "height": 1000}, {"width": 390, "height": 844}])
def test_progressive_disclosure_keeps_primary_tasks_and_safety_visible(page, portal, viewport):
    page.set_viewport_size(viewport)
    page.goto(portal.url + "/")
    expect(page.get_by_role("navigation", name="Folder path", exact=True)).to_be_visible()
    expect(page.get_by_label("Filter files by name", exact=True)).to_be_visible()
    expect(page.get_by_label("Source implementation", exact=True)).not_to_be_visible()
    page.get_by_text("Source configuration", exact=True).click()
    expect(page.get_by_label("Source implementation", exact=True)).to_be_visible()
    expect(page.get_by_label("Folder within source root", exact=True)).to_be_visible()
    calibrate_camera(page, portal)
    expect(page.get_by_text("Frame", exact=True)).not_to_be_visible()
    page.get_by_text("Geometry and effective dates", exact=True).click()
    expect(page.get_by_text("Frame", exact=True)).to_be_visible()
    page.get_by_role("link", name="Edit north-entrance-camera-17", exact=True).click()
    expect(page.get_by_label("Location", exact=True)).to_be_visible()
    expect(page.get_by_label("Camera timezone", exact=True)).to_be_visible()
    expect(page.get_by_label("X2", exact=True)).not_to_be_visible()
    expect(page.get_by_label("Effective from (UTC)", exact=True)).not_to_be_visible()
    coordinates = page.get_by_text("Manual line coordinates", exact=True)
    coordinates.click()
    page.get_by_label("X2", exact=True).fill("320")
    coordinates.click()
    page.get_by_role("button", name="Save calibration", exact=True).click()
    expect(page.get_by_label("X2", exact=True)).to_be_visible()
    expect(page.get_by_label("X2", exact=True)).to_be_focused()
    page.get_by_label("X2", exact=True).fill("288")
    recording = page.get_by_text("Recording time and effective dates", exact=True)
    recording.click()
    page.get_by_label("Effective from (UTC)", exact=True).fill("2025-01-01T00:00:00Z")
    recording.click()
    page.get_by_role("button", name="Save calibration", exact=True).click()
    page.get_by_role("link", name="Edit north-entrance-camera-17", exact=True).click()
    expect(page.get_by_label("Effective from (UTC)", exact=True)).to_be_visible()
    expect(page.get_by_label("Effective from (UTC)", exact=True)).to_have_value("2025-01-01T00:00:00Z")
    page.get_by_label("Apply calibration to", exact=True).select_option("video")
    expect(page.get_by_label("Location", exact=True)).not_to_be_visible()
    expect(page.get_by_label("Authoritative capture time override (UTC, optional)", exact=True)).to_be_visible()
    page.get_by_label("Apply calibration to", exact=True).select_option("camera")
    expect(page.get_by_label("Camera timezone", exact=True)).to_have_value("America/Denver")
    expect(page.get_by_label("Authoritative capture time override (UTC, optional)", exact=True)).not_to_be_visible()
    page.goto(portal.url + "/profiles")
    page.get_by_role("button", name="Add target and profiles", exact=True).click()
    expect(page.get_by_label("Target name", exact=True)).to_be_visible()
    expect(page.get_by_label("Batch size override", exact=True)).not_to_be_visible()
    advanced = page.get_by_text("Advanced inference overrides (optional)", exact=True)
    advanced.focus()
    advanced.press("Enter")
    expect(page.get_by_label("Batch size override", exact=True)).to_be_visible()
    page.goto(portal.url + "/cloud")
    page.get_by_role("button", name="Start cloud action", exact=True).click()
    action = page.get_by_label("Action", exact=True)
    wait = page.get_by_label("Wait for processing after submission", exact=True)
    expect(wait).not_to_be_visible()
    expect(page.get_by_label("I reviewed the target and intake policy and approve this cloud action.")).to_be_visible()
    action.select_option("submit")
    expect(wait).to_be_visible()
    wait.check()
    action.select_option("publish")
    expect(wait).not_to_be_visible()
    expect(wait).not_to_be_checked()
    expect(wait).to_be_disabled()
    page.goto(portal.url + "/packages")
    expect(page.get_by_role("button", name="Generate manifest package", exact=True)).to_be_visible()
    expect(page.get_by_text("1. Inspect and validate", exact=True)).not_to_be_visible()
    page.get_by_text("How package preparation works", exact=True).click()
    expect(page.get_by_text("1. Inspect and validate", exact=True)).to_be_visible()
    prepare_package(page, portal)
    expect(page.locator("#job-log")).not_to_be_visible()
    expect(page.get_by_role("link", name="Download manifest package ZIP", exact=True)).to_be_visible()
    page.get_by_text("Completed job log", exact=False).click()
    expect(page.locator("#job-log")).to_be_visible()


@pytest.mark.portal_cloud
@pytest.mark.parametrize("viewport", [{"width": 1440, "height": 1000}, {"width": 390, "height": 844}])
def test_profile_dialog_is_compact_accessible_and_preserves_drafts_without_submitting(page, portal, viewport):
    page.set_viewport_size(viewport)
    pending_scripts = []
    page.route("**/portal.js", lambda route: pending_scripts.append(route))
    page.goto(portal.url + "/profiles", wait_until="commit")
    trigger = page.get_by_role("button", name="Add target and profiles", exact=True)
    dialog = page.locator("#profile-dialog")
    expect(page.get_by_role("heading", name="Approved target catalog", exact=True)).to_be_visible()
    expect(page.get_by_label("Target name", exact=True)).not_to_be_visible()
    expect(trigger).to_be_disabled()
    assert len(pending_scripts) == 1
    pending_scripts[0].continue_()
    page.wait_for_load_state("load")
    expect(trigger).to_be_enabled()
    trigger.click()
    expect(page.get_by_role("dialog", name="Generate approved profiles", exact=True)).to_be_visible()
    target = page.get_by_label("Target name", exact=True)
    expect(target).to_be_focused()
    target.fill("draft-target")
    bounds = dialog.bounding_box()
    assert bounds is not None
    assert bounds["x"] >= 0 and bounds["x"] + bounds["width"] <= viewport["width"]
    assert bounds["y"] >= 0 and bounds["y"] + bounds["height"] <= viewport["height"]
    assert dialog.evaluate("element => element.scrollWidth <= element.clientWidth")
    page.keyboard.press("Escape")
    expect(dialog).not_to_be_visible()
    expect(trigger).to_be_focused()
    trigger.click()
    expect(target).to_have_value("draft-target")
    submit = page.get_by_role("button", name="Generate profiles", exact=True)
    close = page.get_by_role("button", name="Close dialog", exact=True)
    close.focus()
    page.keyboard.press("Tab")
    expect(target).to_be_focused()
    page.keyboard.press("Shift+Tab")
    expect(close).to_be_focused()
    assert dialog.evaluate("element => element.matches(':modal')")
    trigger.evaluate("element => element.focus()")
    expect(close).to_be_focused()
    dialog.dispatch_event("keydown", {"key": "Escape", "isComposing": True})
    expect(dialog).to_be_visible()
    # Embedded browsers can deliver Escape without invoking the native close action.
    dialog.dispatch_event("keydown", {"key": "Escape"})
    expect(dialog).not_to_be_visible()
    expect(trigger).to_be_focused()
    trigger.click()
    close.click()
    expect(dialog).not_to_be_visible()
    expect(trigger).to_be_focused()
    trigger.click()
    page.get_by_role("button", name="Cancel", exact=True).click()
    expect(dialog).not_to_be_visible()
    expect(trigger).to_be_focused()
    trigger.click()
    target.fill("")
    submit.click()
    expect(dialog).to_be_visible()
    expect(target).to_be_focused()
    assert portal.service.store.jobs() == []
    assert not portal.service.catalog.exists()


@pytest.mark.portal_cloud
@pytest.mark.parametrize("viewport", [{"width": 1440, "height": 1000}, {"width": 390, "height": 844}])
def test_shared_form_dialogs_preserve_context_and_require_fresh_cloud_approval(page, portal, viewport):
    page.set_viewport_size(viewport)
    calibrate_camera(page, portal)
    cases = [
        ("/locations", "Add location", "Add a location", "Location name"),
        ("/cameras", "Add camera", "Add a camera", "Camera name"),
        ("/packages", "Generate manifest package", "Generate a package", "Folder within source root"),
        ("/cloud", "Start cloud action", "Start a cloud job", "Prepared package"),
        ("/profiles", "Add target and profiles", "Generate approved profiles", "Target name"),
    ]
    for route, label, title, field_label in cases:
        page.goto(portal.url + route)
        field = page.get_by_label(field_label, exact=True)
        expect(field).not_to_be_visible()
        trigger = page.get_by_role("button", name=label, exact=True)
        trigger.click()
        dialog = page.get_by_role("dialog", name=title, exact=True)
        expect(dialog).to_be_visible()
        assert dialog.evaluate("element => element.matches(':modal')")
        bounds = dialog.bounding_box()
        assert bounds is not None
        assert 0 <= bounds["x"] and bounds["x"] + bounds["width"] <= viewport["width"]
        assert 0 <= bounds["y"] and bounds["y"] + bounds["height"] <= viewport["height"]
        assert dialog.evaluate("element => element.scrollWidth <= element.clientWidth")
        if route != "/cloud":
            field.fill("unsaved-draft")
        page.get_by_role("button", name="Cancel", exact=True).click()
        expect(dialog).not_to_be_visible()
        expect(trigger).to_be_focused()
        trigger.click()
        if route != "/cloud":
            expect(field).to_have_value("unsaved-draft")
        else:
            expect(dialog).to_contain_text("Publishing can trigger arrival automation.")
            approval = page.get_by_label("I reviewed the target and intake policy and approve this cloud action.")
            approval.check()
        page.keyboard.press("Escape")
        expect(dialog).not_to_be_visible()
        expect(trigger).to_be_focused()
        trigger.click()
        if route == "/cloud":
            expect(approval).not_to_be_checked()
        page.get_by_role("button", name="Close dialog", exact=True).click()
        expect(dialog).not_to_be_visible()
    assert portal.service.store.jobs() == []
    assert portal.calls == []


@pytest.mark.portal_cloud
def test_resume_dialog_requires_new_approval_and_keeps_retained_work_unchanged(page, portal):
    identity = "d" * 32
    with portal.service.store.connection() as db:
        db.execute(
            "INSERT INTO jobs(id,kind,payload,status,error) VALUES (?,'submit',?,'FAILED',?)",
            (identity, json.dumps({"retained": "original-inputs"}), "Interrupted registration"),
        )
    page.goto(portal.url + "/jobs/" + identity)
    expect(page.get_by_role("alert")).to_contain_text("Interrupted registration")
    trigger = page.get_by_role("button", name="Resume with retained state", exact=True)
    trigger.click()
    dialog = page.get_by_role("dialog", name="Resume safely", exact=True)
    expect(dialog).to_be_visible()
    approval = page.get_by_label("I approve resuming this cloud action with its retained state.")
    expect(approval).not_to_be_checked()
    dialog.get_by_role("button", name="Resume with retained state", exact=True).click()
    expect(approval).to_be_focused()
    approval.check()
    page.get_by_role("button", name="Cancel", exact=True).click()
    trigger.click()
    expect(approval).not_to_be_checked()
    page.keyboard.press("Escape")
    expect(dialog).not_to_be_visible()
    retained = portal.service.store.job(identity)
    assert retained["status"] == "FAILED"
    assert retained["payload"] == {"retained": "original-inputs"}
    assert len(portal.service.store.jobs()) == 1
    assert portal.calls == []


@pytest.mark.portal_cloud
def test_profile_generation_publication_and_submit_wait_use_shared_backends(page, portal):
    calibrate_camera(page, portal)
    package_id = prepare_package(page, portal)
    page.get_by_role("link", name="Profiles", exact=True).click()
    page.get_by_role("button", name="Add target and profiles", exact=True).click()
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
        page.get_by_role("button", name="Start cloud action", exact=True).click()
        page.get_by_label("Prepared package", exact=True).select_option(package_id)
        page.get_by_label("Target and profile", exact=True).select_option("ci::rtdetr-r18-onnx-cpu")
        page.get_by_label("Action", exact=True).select_option(action)
        if wait:
            page.get_by_label("Wait for processing after submission").check()
        page.get_by_label("I reviewed the target and intake policy and approve this cloud action.").check()
        page.get_by_role("dialog", name="Start a cloud job", exact=True).get_by_role("button", name="Start cloud action", exact=True).click()
        expect(page.locator("[data-job-status]")).to_have_text("SUCCEEDED", timeout=30000)
    expect(page.get_by_role("heading", name="Result", exact=True)).to_be_visible()
    page.get_by_text("Completed job log", exact=False).click()
    expect(page.locator("#job-log")).to_contain_text("1/1 committed")
    assert len(portal.calls) == 2
