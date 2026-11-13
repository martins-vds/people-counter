"""Capture-time regex safety and compatibility."""

import re
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from people_counter import manifest


def catalog_entry(pattern_text, time_format="%Y%m%dT%H%M%SZ"):
    pattern, time_format = manifest._compile_capture_pattern(
        {"capture_time_regex": pattern_text, "capture_time_format": time_format},
        2,
        "filename_utc",
    )
    return manifest.CameraCatalogEntry(
        source_path_prefix="north/camera-17/",
        camera_id="camera-17",
        location_id="north",
        camera_timezone="UTC",
        frame_width=1920,
        frame_height=1080,
        counting_line=(0, 0, 100, 100),
        capture_time_source="filename_utc",
        capture_time_regex=pattern,
        capture_time_format=time_format,
        effective_from_utc=datetime(2020, 1, 1, tzinfo=timezone.utc),
        effective_to_utc=None,
    )


@pytest.mark.parametrize("legacy_pattern", [False, True])
def test_capture_regex_timeout_is_actionable_video_rejection(legacy_pattern):
    entry = catalog_entry(r"(?P<captured_at_utc>)(a|aa)+$", "%Y")
    if legacy_pattern:
        entry = replace(entry, capture_time_regex=re.compile(r"(?P<captured_at_utc>)(a|aa)+$"))
    filename = "a" * 32 + "!.mp4"
    with pytest.raises(manifest.VideoValidationError) as caught:
        manifest.match_catalog_entry(entry.source_path_prefix + filename, [entry], {})

    error = caught.value
    assert error.reason_code == "CAPTURE_TIME_REGEX_TIMEOUT"
    assert error.field == "capture_time_regex"
    assert error.observed_value == filename
    assert error.camera_id == entry.camera_id
    assert error.source_path_prefix == entry.source_path_prefix
    assert error.retryable is False
    assert "0.1 seconds" in str(error)
    assert error.suggested_action == (
        "Simplify capture_time_regex to avoid excessive backtracking, "
        "or use embedded capture time or an inventory override."
    )
    assert isinstance(error.__cause__, TimeoutError)


@pytest.mark.parametrize("legacy_pattern", [False, True])
def test_custom_capture_extraction_preserves_flags_and_utc_offset(legacy_pattern):
    pattern_text = r"clip_(?P<captured_at_utc>\d{8}T\d{6}[+-]\d{4})"
    entry = catalog_entry("(?i)" + pattern_text, "%Y%m%dT%H%M%S%z")
    if legacy_pattern:
        entry = replace(entry, capture_time_regex=re.compile(pattern_text, re.IGNORECASE | re.ASCII))
    _, captured_at, source = manifest.match_catalog_entry(
        entry.source_path_prefix + "prefix_CLIP_20260203T060506+0200.mp4", [entry], {}
    )
    assert captured_at == datetime(2026, 2, 3, 4, 5, 6, tzinfo=timezone.utc)
    assert source == "filename"


@pytest.mark.parametrize(
    ("pattern_text", "message"),
    [("(", "invalid capture_time_regex"), (r"(?P<wrong>\d+)", "must define captured_at_utc")],
)
def test_capture_regex_catalog_errors_are_preserved(pattern_text, message):
    with pytest.raises(manifest.CatalogError, match=f"Catalog row 2: .*{message}") as caught:
        catalog_entry(pattern_text)
    assert caught.value.reason_code == "CATALOG_INVALID"


@pytest.mark.parametrize(
    ("filename", "reason_code", "observed_value"),
    [
        ("undated.mp4", "CAPTURE_TIME_FILENAME_MISMATCH", "undated.mp4"),
        ("clip_20261303T040506Z.mp4", "CAPTURE_TIME_FILENAME_INVALID", "20261303T040506Z"),
    ],
)
def test_filename_errors_remain_distinct_from_timeout(filename, reason_code, observed_value):
    entry = catalog_entry(r"(?P<captured_at_utc>\d{8}T\d{6}Z)")
    with pytest.raises(manifest.VideoValidationError) as caught:
        manifest._filename_capture_time(entry, entry.source_path_prefix + filename)
    assert caught.value.reason_code == reason_code
    assert caught.value.field == "captured_at_utc"
    assert caught.value.observed_value == observed_value


def test_capture_without_offset_still_defaults_to_utc():
    entry = catalog_entry(r"(?P<captured_at_utc>\d{8}T\d{6}Z)")
    assert manifest._filename_capture_time(
        entry, "clip_20260203T040506Z.mp4"
    ) == datetime(2026, 2, 3, 4, 5, 6, tzinfo=timezone.utc)


def test_legacy_capture_regex_keeps_ascii_with_ignorecase():
    entry = catalog_entry(r"(?P<captured_at_utc>\d{8}T\d{6}Z)")
    entry = replace(
        entry,
        capture_time_regex=re.compile(
            r"^\w+_(?P<captured_at_utc>\d{8}T\d{6}Z)",
            re.ASCII | re.IGNORECASE,
        ),
    )
    assert manifest._filename_capture_time(
        entry, "CLIP_20260203t040506z.mp4"
    ) == datetime(2026, 2, 3, 4, 5, 6, tzinfo=timezone.utc)
    with pytest.raises(manifest.VideoValidationError) as caught:
        manifest._filename_capture_time(entry, "é_20260203T040506Z.mp4")
    assert caught.value.reason_code == "CAPTURE_TIME_FILENAME_MISMATCH"
