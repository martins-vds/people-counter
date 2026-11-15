import hashlib
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


@pytest.mark.parametrize("width", [320, 1440])
def test_destination_first_upload_hash_move_download_and_package_flow(page, portal, width):
    cloud = configure(portal)
    original = (portal.source / "lobby/clip.mp4").read_bytes()
    page.set_viewport_size({"width": width, "height": 1000})
    page.goto(portal.url + "/footage")
    picker = page.get_by_label("Choose local videos", exact=True)
    expect(picker).to_be_disabled()
    page.get_by_role("button", name="Confirm destination", exact=True).click()
    expect(picker).to_be_enabled()
    picker.set_input_files({"name": "uploaded.mp4", "mimeType": "video/mp4", "buffer": original})
    page.get_by_role("button", name="Upload selected videos", exact=True).click()
    checksum = hashlib.sha256(original).hexdigest()
    expect(page.get_by_text("Uploaded. SHA-256: " + checksum)).to_be_visible(timeout=30000)
    assert cloud.files["root/uploaded.mp4"]["data"] == original
    assert not list(portal.service.state.rglob("*.mp4"))
    page.get_by_role("link", name="Refresh folder", exact=True).click()
    page.get_by_label("Select uploaded.mp4", exact=True).check()
    page.get_by_role("button", name="Rename selected", exact=True).click()
    modal = page.locator("[data-intake-dialog]")
    modal.get_by_label("Folder or new name", exact=True).fill("renamed.mp4")
    modal.get_by_role("button", name="Confirm", exact=True).click()
    expect(page.get_by_text("Move completed.", exact=False)).to_be_visible(timeout=30000)
    assert "root/uploaded.mp4" not in cloud.files
    page.get_by_role("link", name="Refresh folder", exact=True).click()
    with page.expect_download() as download:
        page.get_by_role("link", name="Download", exact=True).click()
    assert open(download.value.path(), "rb").read() == original
    location = portal.service.store.add_location("North")
    camera = portal.service.store.add_camera("Entrance", location)
    portal.service.assign_footage(camera, ["renamed.mp4"])
    portal.service.save_camera_line(camera, {
        "video": "renamed.mp4", "counting_line_x1": "32", "counting_line_y1": "90",
        "counting_line_x2": "288", "counting_line_y2": "90",
    })
    page.goto(portal.url + "/packages")
    page.get_by_role("button", name="New package", exact=True).click()
    modal = page.get_by_role("dialog", name="New package", exact=True)
    modal.get_by_label("Package name", exact=True).fill("Uploaded footage")
    modal.get_by_role("button", name="Choose footage", exact=True).click()
    page.get_by_label("Select renamed.mp4", exact=True).check()
    page.get_by_role("button", name="Save selection and assign cameras", exact=True).click()
    page.get_by_role("link", name="Review generation", exact=True).click()
    page.get_by_role("button", name="Generate manifest package", exact=True).click()
    expect(page.locator("[data-job-status]")).to_have_text("SUCCEEDED", timeout=30000)
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
