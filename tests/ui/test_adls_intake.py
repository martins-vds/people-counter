import hashlib
import zipfile
import pytest
from playwright.sync_api import expect

from people_counter.portal_sources import AdlsFootageSource
from tests.test_operator_portal import FakePortalAzure
from tests.test_portal_intake import Cloud

pytestmark = pytest.mark.ui


def configure(portal):
    source = AdlsFootageSource({"kind": "adls", "account": "testing", "filesystem": "videos", "root": "root"},
                               FakePortalAzure(), portal.service.store)
    cloud = Cloud()
    cloud.files["root"] = {"size": 0, "etag": "root", "metadata": {"hdi_isfolder": "true"}, "data": b"", "is_directory": True}
    source._client = cloud
    service = portal.service
    service.source, service.local_source, service._azure, service._intake = source, None, source.azure, None
    service.input_root = service.state / "inputs"
    service.store.put("source", source.identity, source.descriptor)
    return cloud


def test_draft_folder_navigation_storage_dialog_and_safe_camera_assignment(page, portal):
    cloud = configure(portal)
    original = (portal.source / "lobby/clip.mp4").read_bytes()
    cloud.files["root/day"] = {"size": 0, "etag": "day", "metadata": {"hdi_isfolder": "true"}, "data": b"", "is_directory": True}
    for path in ("day/first.mp4", "day/second.mp4"):
        cloud.files["root/" + path] = {"size": len(original), "etag": path, "metadata": {}, "data": original, "is_directory": False}
    location = portal.service.store.add_location("North")
    camera = portal.service.store.add_camera("Entrance", location)
    draft = portal.service.new_draft("Draft UX")
    url = portal.url + "/packages/drafts/" + draft["id"]
    page.goto(url)
    page.get_by_label("Select day", exact=True).check()
    expect(page.locator('[name="mode"][value="recursive"]')).to_be_checked()
    expect(page.get_by_role("button", name="Manage videos", exact=True)).to_have_count(0)
    for action in ("Create folder", "Move selected", "Rename selected", "Download selected"):
        button = page.get_by_role("button", name=action, exact=True)
        expect(button).to_be_visible()
        assert button.evaluate("(button) => !button.closest('dialog')")
    page.get_by_role("button", name="Create folder", exact=True).click()
    expect(page.locator("[data-intake-dialog]")).to_be_visible()
    page.locator("[data-intake-dialog]").get_by_role("button", name="Close dialog", exact=True).click()
    expect(page.locator("[data-intake-dialog]")).not_to_be_visible()
    expect(page.get_by_role("button", name="Create folder", exact=True)).to_be_focused()
    page.get_by_role("button", name="Upload videos", exact=True).click()
    storage = page.get_by_role("dialog", name="Upload videos", exact=True)
    expect(storage).to_be_visible()
    expect(storage.get_by_role("button", name="Create folder", exact=True)).to_have_count(0)
    page.keyboard.press("Escape")
    expect(storage).not_to_be_visible()
    expect(page.get_by_role("button", name="Upload videos", exact=True)).to_be_focused()
    page.get_by_role("link", name="day/", exact=True).click()
    crumbs = page.get_by_role("navigation", name="Folder path", exact=True)
    expect(crumbs.get_by_text("day", exact=True)).to_be_visible()
    page.get_by_role("button", name="Upload videos", exact=True).click()
    expect(storage.locator("[data-upload-destination]")).to_have_text("Uploading to: day")
    expect(storage.get_by_role("textbox")).to_have_count(0)
    expect(storage.get_by_role("button", name="Confirm destination", exact=True)).to_have_count(0)
    storage.get_by_role("button", name="Close dialog", exact=True).click()
    crumbs.get_by_role("link", name="Source root", exact=True).click()
    page.get_by_label("Select day", exact=True).check()
    page.get_by_role("button", name="Save selection and assign cameras", exact=True).click()
    page.get_by_label("Select day/first.mp4 for batch assignment", exact=True).check()
    page.get_by_role("button", name="Assign camera to day/second.mp4", exact=True).click()
    assignment = page.get_by_role("dialog", name="Assign camera", exact=True)
    expect(assignment.locator("[data-assignment-summary]")).to_contain_text("day/second.mp4")
    expect(assignment.locator("[data-assignment-summary]")).not_to_contain_text("day/first.mp4")
    assignment.get_by_label("Camera", exact=True).select_option(camera)
    assignment.get_by_role("button", name="Save camera assignments", exact=True).click()
    expect(assignment).not_to_be_visible(timeout=30000)
    assert portal.service.footage_assignments() == {"day/second.mp4": camera}
    expect(page.get_by_label("Select day/first.mp4 for batch assignment", exact=True)).not_to_be_checked()
    page.get_by_label("Select day/first.mp4 for batch assignment", exact=True).check()
    page.get_by_role("button", name="Assign selected recordings", exact=True).click()
    expect(assignment.get_by_label("Camera", exact=True)).to_have_value("")
    assignment.get_by_label("Camera", exact=True).select_option(camera)
    assignment.get_by_role("button", name="Save camera assignments", exact=True).click()
    expect(assignment).not_to_be_visible(timeout=30000)
    assert portal.service.footage_assignments() == {"day/first.mp4": camera, "day/second.mp4": camera}
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")


@pytest.mark.parametrize("view", ["review", "calibration"])
def test_remote_review_and_calibration_navigate_before_media_is_ready(page, portal, view):
    cloud = configure(portal)
    original = (portal.source / "lobby/clip.mp4").read_bytes()
    cloud.files["root/clip.mp4"] = {"size": len(original), "etag": "clip", "metadata": {}, "data": original, "is_directory": False}
    location = portal.service.store.add_location("North")
    camera = portal.service.store.add_camera("Entrance", location)
    portal.service.assign_footage(camera, ["clip.mp4"])
    draft = portal.service.new_draft("Preview UX")
    pending = []
    page.route("**/*media=1", lambda route: pending.append(route))
    path = "/review" if view == "review" else f"/cameras/{camera}/calibrate"
    page.goto(portal.url + path + "?video=clip.mp4&draft=" + draft["id"])
    expect(page.get_by_role("heading", level=1)).to_have_text("Review footage" if view == "review" else "Calibrate camera")
    expect(page.locator("[data-media-status]")).to_contain_text("Loading footage")
    expect(page.get_by_role("link", name="Return to package draft", exact=True)).to_be_visible()
    expect(page.locator("#line-canvas")).to_have_count(0)
    assert len(pending) == 1
    pending[0].continue_()
    expect(page.locator("#line-canvas")).to_be_visible(timeout=30000)
    expect(page.locator("[data-media-content]")).to_have_attribute("aria-busy", "false")
    if view == "review":
        page.get_by_label("Capture time override (UTC, optional)", exact=True).fill("2026-01-01T12:00")
        expect(page.locator("#readiness-update-status")).to_contain_text("Readiness updated", timeout=30000)


@pytest.mark.parametrize("view", ["draft", "library"])
def test_assignment_shows_feedback_while_post_is_pending(page, portal, view):
    cloud = configure(portal)
    original = (portal.source / "lobby/clip.mp4").read_bytes()
    videos = ["first.mp4", "second.mp4"]
    for name in videos:
        cloud.files["root/" + name] = {
            "size": len(original), "etag": name, "metadata": {},
            "data": original, "is_directory": False,
        }
    location = portal.service.store.add_location("North")
    camera = portal.service.store.add_camera("Entrance", location)
    if view == "draft":
        draft = portal.service.new_draft("Pending assignment")
        portal.service.update_draft(draft["id"], {"videos": videos})
        page.goto(portal.url + "/packages/drafts/" + draft["id"] + "?step=cameras")
        page.get_by_role("button", name="Select all videos on this page", exact=True).click()
        page.get_by_role("button", name="Assign selected recordings", exact=True).click()
        page.get_by_label("Camera", exact=True).select_option(camera)
        button = page.get_by_role("button", name="Save camera assignments", exact=True)
        success = "Camera assignment saved. Package draft retained."
    else:
        page.goto(portal.url + "/footage")
        for video in videos:
            page.get_by_label("Select " + video, exact=True).check()
        page.get_by_label("Assign selected footage to camera", exact=True).select_option(camera)
        button = page.get_by_role("button", name="Assign selected footage", exact=True)
        success = "Selected footage assigned to the camera. Its saved counting line is unchanged."
    pending = []

    def hold_assignment(route):
        if route.request.method == "POST":
            pending.append(route)
        else:
            route.continue_()

    page.route("**/packages/drafts/**" if view == "draft" else "**/footage/assign*", hold_assignment)
    with page.expect_request(lambda request: request.method == "POST"):
        button.click()
    feedback = page.locator("[data-assignment-feedback]")
    expect(feedback).to_be_visible()
    expect(feedback).to_contain_text("Assigning camera to 2 recording(s)")
    expect(page.get_by_role("progressbar", name="Camera assignment progress", exact=True)).to_be_visible()
    form = page.locator("[data-assignment-submit]")
    expect(form).to_have_attribute("aria-busy", "true")
    expect(form).to_have_attribute("inert", "")
    expect(page.get_by_role("button", name="Assigning...", exact=True, include_hidden=True)).to_be_disabled()
    assert len(pending) == 1
    for video in videos:
        assert f'name="videos"\r\n\r\n{video}\r\n' in pending[0].request.post_data
    if view == "draft":
        expect(page.get_by_role("button", name="Close dialog", exact=True)).to_be_disabled()
        page.keyboard.press("Escape")
        expect(page.get_by_role("dialog", name="Assign camera", exact=True)).to_be_visible()
    form.evaluate("(element) => element.requestSubmit()")
    assert len(pending) == 1
    pending[0].continue_()
    expect(page.get_by_text(success, exact=True)).to_be_visible(timeout=30000)
    assert portal.service.footage_assignments() == {video: camera for video in videos}
    expect(page.locator("[data-assignment-feedback]:visible")).to_have_count(0)


@pytest.mark.parametrize("width", [390, 1440])
def test_ten_row_pagination_and_page_scoped_camera_association_keep_next_steps_above_tables(page, portal, width):
    cloud = configure(portal)
    original = (portal.source / "lobby/clip.mp4").read_bytes()
    videos = [f"clip-{index:02}.mp4" for index in range(12)]
    for name in videos:
        cloud.files["root/" + name] = {
            "size": len(original), "etag": name, "metadata": {},
            "data": original, "is_directory": False,
        }
    location = portal.service.store.add_location("North")
    camera = portal.service.store.add_camera("Entrance", location)
    draft = portal.service.new_draft("Paged camera assignment")
    portal.service.update_draft(draft["id"], {"videos": videos})
    page.set_viewport_size({"width": width, "height": 1000})
    url = portal.url + "/packages/drafts/" + draft["id"]
    page.goto(url)
    source = page.get_by_role("table", name="Source recordings", exact=True)
    expect(source.locator("tbody > tr:visible")).to_have_count(10)
    save = page.get_by_role("button", name="Save selection and assign cameras", exact=True)
    assert save.bounding_box()["y"] < source.bounding_box()["y"]
    source_pager = page.get_by_role("navigation", name="Source recordings pagination", exact=True)
    source_pager.get_by_role("button", name="Next page", exact=True).click()
    expect(source.locator("tbody > tr:visible")).to_have_count(2)
    expect(page.locator("[data-selection-status]")).to_contain_text("2 of 2 videos selected on this page")
    page.goto(url + "?step=cameras")
    table = page.get_by_role("table", name="Selected recordings", exact=True)
    expect(table.locator("tbody > tr:visible")).to_have_count(10)
    next_step = page.get_by_role("link", name="Review generation", exact=True)
    assert next_step.bounding_box()["y"] < table.bounding_box()["y"]
    page.get_by_role("button", name="Select all videos on this page", exact=True).click()
    pager = page.get_by_role("navigation", name="Selected recordings pagination", exact=True)
    pager.get_by_role("button", name="Next page", exact=True).click()
    expect(table.locator("tbody > tr:visible")).to_have_count(2)
    assign = page.get_by_role("button", name="Assign selected recordings", exact=True)
    expect(assign).to_be_disabled()
    page.get_by_role("button", name="Select all videos on this page", exact=True).click()
    page.get_by_role("button", name="Unselect all videos on this page", exact=True).click()
    expect(assign).to_be_disabled()
    page.get_by_role("button", name="Select all videos on this page", exact=True).click()
    assign.click()
    dialog = page.get_by_role("dialog", name="Assign camera", exact=True)
    expect(dialog.locator("[data-assignment-summary]")).to_contain_text("Assign 2 recording(s)")
    dialog.get_by_label("Camera", exact=True).select_option(camera)
    dialog.get_by_role("button", name="Save camera assignments", exact=True).click()
    expect(dialog).not_to_be_visible(timeout=30000)
    assert portal.service.footage_assignments() == {video: camera for video in videos[10:]}
    for index in range(11):
        portal.service.store.add_location(f"Store {index}")
    page.goto(portal.url + "/locations")
    expect(page.locator("table tbody > tr:visible")).to_have_count(10)
    page.get_by_role("navigation", name="Registered locations pagination", exact=True).get_by_role(
        "button", name="Next page", exact=True,
    ).click()
    expect(page.locator("table tbody > tr:visible")).to_have_count(2)
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")


@pytest.mark.parametrize("width", [320, 1440])
def test_compact_bulk_selection_feedback_refresh_and_centered_storage_dialogs(page, portal, width):
    cloud = configure(portal)
    original = (portal.source / "lobby/clip.mp4").read_bytes()
    for name in ("first.mp4", "second.mp4"):
        cloud.files["root/" + name] = {
            "size": len(original), "etag": name, "metadata": {},
            "data": original, "is_directory": False,
        }
    draft = portal.service.new_draft("Bulk selection")
    page.set_viewport_size({"width": width, "height": 1000})
    page.goto(portal.url + "/packages/drafts/" + draft["id"])
    expect(page.locator("[data-selection-options]")).not_to_have_attribute("open", "")
    page.get_by_role("button", name="Select all videos on this page", exact=True).click()
    expect(page.get_by_label("Select first.mp4", exact=True)).to_be_checked()
    expect(page.get_by_label("Select second.mp4", exact=True)).to_be_checked()
    expect(page.locator("[data-selection-status]")).to_contain_text("2 of 2 videos selected on this page")
    page.get_by_role("button", name="Rename selected", exact=True).click()
    message = page.locator("[data-intake-toolbar] [data-intake-message]")
    expect(message).to_contain_text("renaming requires exactly one item")
    expect(message).to_be_in_viewport()
    refresh = page.get_by_role("link", name="Refresh folder", exact=True)
    expect(refresh).to_have_text("")
    assert refresh.bounding_box()["height"] == page.get_by_role("button", name="Create folder", exact=True).bounding_box()["height"]
    for action in ("Create folder", "Move selected"):
        page.get_by_role("button", name=action, exact=True).click()
        dialog = page.locator("[data-intake-dialog]")
        expect(dialog).to_be_visible()
        box = dialog.bounding_box()
        assert abs(box["x"] + box["width"] / 2 - width / 2) <= 2
        assert abs(box["y"] + box["height"] / 2 - 500) <= 2
        dialog.get_by_role("button", name="Close dialog", exact=True).click()
    page.get_by_role("button", name="Unselect all videos on this page", exact=True).click()
    expect(page.get_by_label("Select first.mp4", exact=True)).not_to_be_checked()
    expect(page.get_by_label("Select second.mp4", exact=True)).not_to_be_checked()
    page.get_by_text("Selection options", exact=True).click()
    expect(page.get_by_role("radio", name="Select individual videos across folders", exact=True)).to_be_visible()
    assert not portal.service.store.entries("draft")[0]["videos"]
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")


@pytest.mark.parametrize("width", [320, 1440])
def test_current_folder_upload_hash_move_download_and_package_flow(page, portal, width):
    cloud = configure(portal)
    original = (portal.source / "lobby/clip.mp4").read_bytes()
    cloud.files["root/day"] = {"size": 0, "etag": "day", "metadata": {"hdi_isfolder": "true"}, "data": b"", "is_directory": True}
    page.set_viewport_size({"width": width, "height": 1000})
    page.goto(portal.url + "/footage?folder=day")
    page.get_by_role("button", name="Upload videos", exact=True).click()
    upload = page.get_by_role("dialog", name="Upload videos", exact=True)
    expect(upload.locator("[data-upload-destination]")).to_have_text("Uploading to: day")
    expect(upload.get_by_role("textbox")).to_have_count(0)
    expect(upload.get_by_role("button", name="Confirm destination", exact=True)).to_have_count(0)
    picker = page.get_by_label("Choose local videos", exact=True)
    expect(picker).to_be_enabled()
    picker.set_input_files({"name": "uploaded.mp4", "mimeType": "video/mp4", "buffer": original})
    page.get_by_role("button", name="Upload selected videos", exact=True).click()
    checksum = hashlib.sha256(original).hexdigest()
    expect(page.get_by_text("Uploaded. SHA-256: " + checksum)).to_be_visible(timeout=30000)
    assert cloud.files["root/day/uploaded.mp4"]["data"] == original
    assert "root/uploaded.mp4" not in cloud.files
    assert not list(portal.service.state.rglob("*.mp4"))
    upload.get_by_role("button", name="Close dialog", exact=True).click()
    page.get_by_role("link", name="Refresh folder", exact=True).click()
    page.get_by_label("Select uploaded.mp4", exact=True).check()
    page.get_by_role("button", name="Rename selected", exact=True).click()
    modal = page.locator("[data-intake-dialog]")
    modal.get_by_label("Folder or new name", exact=True).fill("renamed.mp4")
    modal.get_by_role("button", name="Confirm", exact=True).click()
    expect(page.locator("[data-intake-toolbar] [data-intake-message]")).to_contain_text("Move completed.", timeout=30000)
    assert "root/day/uploaded.mp4" not in cloud.files
    page.get_by_role("link", name="Refresh folder", exact=True).click()
    with page.expect_download() as download:
        page.get_by_role("link", name="Download", exact=True).click()
    assert open(download.value.path(), "rb").read() == original
    location = portal.service.store.add_location("North")
    camera = portal.service.store.add_camera("Entrance", location)
    portal.service.assign_footage(camera, ["day/renamed.mp4"])
    portal.service.save_camera_line(camera, {
        "video": "day/renamed.mp4", "counting_line_x1": "32", "counting_line_y1": "90",
        "counting_line_x2": "288", "counting_line_y2": "90",
    })
    page.goto(portal.url + "/packages")
    page.get_by_role("button", name="New package", exact=True).click()
    modal = page.get_by_role("dialog", name="New package", exact=True)
    modal.get_by_label("Package name", exact=True).fill("Uploaded footage")
    modal.get_by_role("button", name="Choose footage", exact=True).click()
    page.get_by_role("link", name="day/", exact=True).click()
    page.get_by_label("Select renamed.mp4", exact=True).check()
    page.get_by_role("button", name="Save selection and assign cameras", exact=True).click()
    page.get_by_role("link", name="Review generation", exact=True).click()
    expect(page.locator('[name="destination"]')).to_have_count(0)
    expect(page.get_by_text("application-managed storage", exact=False)).to_be_visible()
    page.get_by_role("button", name="Generate manifest package", exact=True).click()
    expect(page.locator("[data-job-status]")).to_have_text("SUCCEEDED", timeout=30000)
    expect(page.get_by_role("heading", name="Package ready to download", exact=True)).to_be_visible()
    with page.expect_download() as package_download:
        page.locator(".page-actions").get_by_role("link", name="Download manifest package ZIP", exact=True).click()
    with zipfile.ZipFile(package_download.value.path()) as package:
        assert "manifest-package.json" in package.namelist()
        assert not any(name.endswith(".mp4") for name in package.namelist())
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
