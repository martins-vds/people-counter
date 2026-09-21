"""Catalog validation and deterministic video-manifest planning."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from collections.abc import Callable
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

CATALOG_COLUMNS = {
    "catalog_version",
    "source_path_prefix",
    "camera_id",
    "location_id",
    "camera_timezone",
    "frame_width",
    "frame_height",
    "counting_line_x1",
    "counting_line_y1",
    "counting_line_x2",
    "counting_line_y2",
    "capture_time_source",
    "capture_time_regex",
    "capture_time_format",
    "effective_from_utc",
    "effective_to_utc",
}
INVENTORY_COLUMNS = {"video_relative_path", "captured_at_utc"}
CONTENT_TYPES = {
    ".avi": "video/x-msvideo",
    ".mkv": "video/x-matroska",
    ".mov": "video/quicktime",
    ".mp4": "video/mp4",
}


class ManifestPublisherError(RuntimeError):
    """Base class for expected manifest-publication failures."""


class CatalogError(ManifestPublisherError):
    """Raised when catalog or inventory data violates its contract."""


class VideoValidationError(ManifestPublisherError):
    """Raised when one video cannot produce a valid manifest."""


@dataclass(frozen=True)
class CameraCatalogEntry:
    source_path_prefix: str
    camera_id: str
    location_id: str
    camera_timezone: str
    frame_width: int
    frame_height: int
    counting_line: tuple[int, int, int, int]
    capture_time_source: str
    capture_time_regex: re.Pattern[str] | None
    capture_time_format: str | None
    effective_from_utc: datetime
    effective_to_utc: datetime | None


@dataclass(frozen=True)
class InventoryEntry:
    captured_at_utc: datetime
    asset_id: str | None
    asset_version: str | None


@dataclass(frozen=True)
class VideoInspection:
    size_bytes: int
    sha256: str
    width: int
    height: int
    duration_seconds: float


@dataclass(frozen=True)
class ManifestPlan:
    local_path: Path
    relative_path: str
    catalog: CameraCatalogEntry
    captured_at_utc: datetime
    inspection: VideoInspection
    asset_id: str
    asset_version: str
    staging_video_path: str
    incoming_video_path: str
    staging_manifest_path: str
    incoming_manifest_path: str
    content_type: str

    @property
    def fingerprint(self) -> str:
        payload = "\n".join(
            (
                self.relative_path,
                self.inspection.sha256,
                self.asset_id,
                self.asset_version,
                self.captured_at_utc.isoformat(),
                self.catalog.camera_id,
                self.catalog.location_id,
                ",".join(str(value) for value in self.catalog.counting_line),
            )
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_relative_path(value: str, *, directory: bool = False) -> str:
    stripped = value.strip()
    if (
        not stripped
        or stripped != value
        or "\\" in stripped
        or "%" in stripped
        or "\x00" in stripped
        or stripped.startswith("/")
    ):
        raise CatalogError(f"Invalid relative path: {value!r}")
    raw_parts = stripped.rstrip("/").split("/")
    if any(part in {"", ".", ".."} for part in raw_parts):
        raise CatalogError(f"Invalid relative path: {value!r}")
    normalized = PurePosixPath(*raw_parts).as_posix()
    return f"{normalized}/" if directory else normalized


def parse_utc_timestamp(value: str, field: str) -> datetime:
    stripped = value.strip()
    if not stripped:
        raise CatalogError(f"{field} must not be blank")
    iso_value = f"{stripped[:-1]}+00:00" if stripped.endswith("Z") else stripped
    try:
        parsed = datetime.fromisoformat(iso_value)
    except ValueError as error:
        raise CatalogError(f"{field} is not a valid ISO-8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CatalogError(f"{field} must include UTC Z or a numeric offset")
    return parsed.astimezone(timezone.utc)


def _positive_int(row: dict[str, str], column: str, line: int) -> int:
    try:
        value = int(row[column])
    except (TypeError, ValueError) as error:
        raise CatalogError(f"Catalog row {line}: {column} must be an integer") from error
    if value <= 0:
        raise CatalogError(f"Catalog row {line}: {column} must be positive")
    return value


def _coordinate(row: dict[str, str], column: str, line: int) -> int:
    try:
        value = int(row[column])
    except (TypeError, ValueError) as error:
        raise CatalogError(f"Catalog row {line}: {column} must be an integer") from error
    if value < 0:
        raise CatalogError(f"Catalog row {line}: {column} must not be negative")
    return value


def _required_text(row: dict[str, str], column: str, line: int) -> str:
    value = row[column].strip()
    if not value:
        raise CatalogError(f"Catalog row {line}: {column} must not be blank")
    return value


def _compile_capture_pattern(
    row: dict[str, str],
    line: int,
    source: str,
) -> tuple[re.Pattern[str] | None, str | None]:
    pattern_text = row["capture_time_regex"].strip()
    format_text = row["capture_time_format"].strip()
    if source == "inventory":
        if pattern_text or format_text:
            raise CatalogError(
                f"Catalog row {line}: inventory capture time must not define "
                "a regex or format"
            )
        return None, None
    if source != "filename_utc":
        raise CatalogError(
            f"Catalog row {line}: capture_time_source must be "
            "filename_utc or inventory"
        )
    if not pattern_text or not format_text:
        raise CatalogError(
            f"Catalog row {line}: filename_utc requires a regex and format"
        )
    try:
        pattern = re.compile(pattern_text)
    except re.error as error:
        raise CatalogError(
            f"Catalog row {line}: invalid capture_time_regex"
        ) from error
    if "captured_at_utc" not in pattern.groupindex:
        raise CatalogError(
            f"Catalog row {line}: capture_time_regex must define "
            "captured_at_utc"
        )
    return pattern, format_text


def _parse_catalog_row(row: dict[str, str], line: int) -> CameraCatalogEntry:
    if _positive_int(row, "catalog_version", line) != 1:
        raise CatalogError(f"Catalog row {line}: unsupported catalog_version")
    width = _positive_int(row, "frame_width", line)
    height = _positive_int(row, "frame_height", line)
    counting_line = tuple(
        _coordinate(row, column, line)
        for column in (
            "counting_line_x1",
            "counting_line_y1",
            "counting_line_x2",
            "counting_line_y2",
        )
    )
    x1, y1, x2, y2 = counting_line
    if x1 >= width or x2 >= width or y1 >= height or y2 >= height:
        raise CatalogError(f"Catalog row {line}: counting line is outside the frame")
    if (x1, y1) == (x2, y2):
        raise CatalogError(f"Catalog row {line}: counting-line endpoints must differ")

    camera_timezone = _required_text(row, "camera_timezone", line)
    try:
        ZoneInfo(camera_timezone)
    except (ValueError, ZoneInfoNotFoundError) as error:
        raise CatalogError(
            f"Catalog row {line}: unknown camera_timezone {camera_timezone!r}"
        ) from error

    source = _required_text(row, "capture_time_source", line)
    pattern, time_format = _compile_capture_pattern(row, line, source)
    effective_from = parse_utc_timestamp(
        row["effective_from_utc"],
        f"Catalog row {line} effective_from_utc",
    )
    effective_to_text = row["effective_to_utc"].strip()
    effective_to = (
        parse_utc_timestamp(
            effective_to_text,
            f"Catalog row {line} effective_to_utc",
        )
        if effective_to_text
        else None
    )
    if effective_to is not None and effective_to <= effective_from:
        raise CatalogError(
            f"Catalog row {line}: effective_to_utc must follow effective_from_utc"
        )

    return CameraCatalogEntry(
        source_path_prefix=normalize_relative_path(
            _required_text(row, "source_path_prefix", line),
            directory=True,
        ),
        camera_id=_required_text(row, "camera_id", line),
        location_id=_required_text(row, "location_id", line),
        camera_timezone=camera_timezone,
        frame_width=width,
        frame_height=height,
        counting_line=counting_line,
        capture_time_source=source,
        capture_time_regex=pattern,
        capture_time_format=time_format,
        effective_from_utc=effective_from,
        effective_to_utc=effective_to,
    )


def _intervals_overlap(
    left: CameraCatalogEntry,
    right: CameraCatalogEntry,
) -> bool:
    left_end = left.effective_to_utc or datetime.max.replace(tzinfo=timezone.utc)
    right_end = right.effective_to_utc or datetime.max.replace(tzinfo=timezone.utc)
    return left.effective_from_utc < right_end and right.effective_from_utc < left_end


def _validate_catalog(entries: list[CameraCatalogEntry]) -> None:
    identities: dict[str, tuple[str, str]] = {}
    for entry in entries:
        identity = (entry.location_id, entry.camera_timezone)
        previous = identities.setdefault(entry.camera_id, identity)
        if previous != identity:
            raise CatalogError(
                f"camera_id {entry.camera_id!r} maps to multiple "
                "locations or timezones"
            )

    for index, left in enumerate(entries):
        for right in entries[index + 1 :]:
            prefixes_overlap = (
                left.source_path_prefix.startswith(right.source_path_prefix)
                or right.source_path_prefix.startswith(left.source_path_prefix)
            )
            if prefixes_overlap and _intervals_overlap(left, right):
                raise CatalogError(
                    "Overlapping source prefixes have overlapping effective "
                    f"ranges: {left.source_path_prefix!r} and "
                    f"{right.source_path_prefix!r}"
                )


def load_camera_catalog(path: Path) -> list[CameraCatalogEntry]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or ()
        missing = CATALOG_COLUMNS - set(fieldnames)
        if missing:
            raise CatalogError(
                f"Camera catalog is missing columns: {', '.join(sorted(missing))}"
            )
        if len(fieldnames) != len(set(fieldnames)):
            raise CatalogError("Camera catalog contains duplicate column names")
        entries = []
        for line, row in enumerate(reader, start=2):
            missing_values = [
                column
                for column in CATALOG_COLUMNS
                if row.get(column) is None
            ]
            if None in row or missing_values:
                raise CatalogError(f"Camera catalog row {line} is malformed")
            entries.append(_parse_catalog_row(row, line))
    if not entries:
        raise CatalogError("Camera catalog contains no data rows")
    _validate_catalog(entries)
    return entries


def load_video_inventory(path: Path | None) -> dict[str, InventoryEntry]:
    if path is None:
        return {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or ()
        missing = INVENTORY_COLUMNS - set(fieldnames)
        if missing:
            raise CatalogError(
                f"Video inventory is missing columns: {', '.join(sorted(missing))}"
            )
        if len(fieldnames) != len(set(fieldnames)):
            raise CatalogError("Video inventory contains duplicate column names")
        inventory: dict[str, InventoryEntry] = {}
        for line, row in enumerate(reader, start=2):
            if None in row or any(row.get(column) is None for column in INVENTORY_COLUMNS):
                raise CatalogError(f"Video inventory row {line} is malformed")
            relative_path = normalize_relative_path(row["video_relative_path"])
            if relative_path in inventory:
                raise CatalogError(
                    f"Video inventory row {line}: duplicate path {relative_path!r}"
                )
            asset_id = (row.get("asset_id") or "").strip() or None
            asset_version = (row.get("asset_version") or "").strip() or None
            if asset_version is not None and not re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9._-]*",
                asset_version,
            ):
                raise CatalogError(
                    f"Video inventory row {line}: asset_version is not path-safe"
                )
            inventory[relative_path] = InventoryEntry(
                captured_at_utc=parse_utc_timestamp(
                    row["captured_at_utc"],
                    f"Video inventory row {line} captured_at_utc",
                ),
                asset_id=asset_id,
                asset_version=asset_version,
            )
    return inventory


def _filename_capture_time(
    entry: CameraCatalogEntry,
    relative_path: str,
) -> datetime:
    assert entry.capture_time_regex is not None
    assert entry.capture_time_format is not None
    match = entry.capture_time_regex.search(PurePosixPath(relative_path).name)
    if match is None:
        raise VideoValidationError(
            f"Filename does not match the capture-time rule: {relative_path}"
        )
    value = match.group("captured_at_utc")
    try:
        captured_at = datetime.strptime(value, entry.capture_time_format)
    except ValueError as error:
        raise VideoValidationError(
            f"Filename capture time is invalid: {relative_path}"
        ) from error
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    return captured_at.astimezone(timezone.utc)


def match_catalog_entry(
    relative_path: str,
    entries: list[CameraCatalogEntry],
    inventory: dict[str, InventoryEntry],
) -> tuple[CameraCatalogEntry, datetime, InventoryEntry | None]:
    path_matches = [
        entry
        for entry in entries
        if relative_path.startswith(entry.source_path_prefix)
    ]
    if not path_matches:
        raise VideoValidationError(
            f"No camera catalog row matches {relative_path}"
        )

    inventory_entry = inventory.get(relative_path)
    active_matches: list[tuple[CameraCatalogEntry, datetime]] = []
    errors: list[VideoValidationError] = []
    for entry in path_matches:
        try:
            if entry.capture_time_source == "inventory":
                if inventory_entry is None:
                    raise VideoValidationError(
                        f"Video inventory has no row for {relative_path}"
                    )
                captured_at = inventory_entry.captured_at_utc
            else:
                captured_at = _filename_capture_time(entry, relative_path)
        except VideoValidationError as error:
            errors.append(error)
            continue
        before_end = (
            entry.effective_to_utc is None
            or captured_at < entry.effective_to_utc
        )
        if entry.effective_from_utc <= captured_at and before_end:
            active_matches.append((entry, captured_at))

    if len(active_matches) == 1:
        entry, captured_at = active_matches[0]
        return entry, captured_at, inventory_entry
    if not active_matches and len(path_matches) == 1 and errors:
        raise errors[0]
    details = f"; first error: {errors[0]}" if errors else ""
    raise VideoValidationError(
        f"Expected one active camera catalog row for {relative_path}; "
        f"found {len(active_matches)}{details}"
    )


def inspect_video(path: Path, chunk_size: int = 8 * 1024 * 1024) -> VideoInspection:
    digest = hashlib.sha256()
    size_bytes = 0
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
            size_bytes += len(chunk)
    if size_bytes <= 0:
        raise VideoValidationError(f"Video is empty: {path}")

    try:
        completed = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=width,height,duration:format=duration",
                "-of",
                "json",
                str(path),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except FileNotFoundError as error:
        raise VideoValidationError(
            "ffprobe is required to inspect source videos"
        ) from error
    except subprocess.TimeoutExpired as error:
        raise VideoValidationError(f"ffprobe timed out for video: {path}") from error
    except subprocess.CalledProcessError as error:
        detail = error.stderr.strip() or "unknown ffprobe error"
        raise VideoValidationError(
            f"ffprobe rejected video {path}: {detail}"
        ) from error

    try:
        probe = json.loads(completed.stdout)
        stream = probe["streams"][0]
        width = int(stream["width"])
        height = int(stream["height"])
    except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise VideoValidationError(
            f"ffprobe returned incomplete metadata for video: {path}"
        ) from error
    duration_seconds = _probe_duration(probe, path)
    if (
        width <= 0
        or height <= 0
        or not math.isfinite(duration_seconds)
        or duration_seconds <= 0
    ):
        raise VideoValidationError(
            f"ffprobe returned invalid metadata for video: {path}"
        )

    return VideoInspection(
        size_bytes=size_bytes,
        sha256=digest.hexdigest(),
        width=width,
        height=height,
        duration_seconds=duration_seconds,
    )


def _probe_duration(probe: dict[str, object], path: Path) -> float:
    streams = probe.get("streams")
    file_format = probe.get("format")
    candidates: list[object] = []
    if isinstance(streams, list) and streams and isinstance(streams[0], dict):
        candidates.append(streams[0].get("duration"))
    if isinstance(file_format, dict):
        candidates.append(file_format.get("duration"))
    for candidate in candidates:
        try:
            duration = float(candidate)
        except (TypeError, ValueError):
            continue
        if math.isfinite(duration) and duration > 0:
            return duration
    raise VideoValidationError(
        f"ffprobe returned invalid duration for video: {path}"
    )


def build_manifest_plan(
    local_path: Path,
    video_root: Path,
    catalog: list[CameraCatalogEntry],
    inventory: dict[str, InventoryEntry],
    *,
    staging_prefix: str,
    incoming_prefix: str,
    inspector: Callable[[Path], VideoInspection] = inspect_video,
) -> ManifestPlan:
    relative_path = normalize_relative_path(
        local_path.relative_to(video_root).as_posix()
    )
    entry, captured_at, inventory_entry = match_catalog_entry(
        relative_path,
        catalog,
        inventory,
    )
    inspection = inspector(local_path)
    if (inspection.width, inspection.height) != (
        entry.frame_width,
        entry.frame_height,
    ):
        raise VideoValidationError(
            f"Video dimensions {inspection.width}x{inspection.height} do not "
            f"match catalog dimensions {entry.frame_width}x{entry.frame_height}: "
            f"{relative_path}"
        )

    asset_id = (
        inventory_entry.asset_id
        if inventory_entry is not None and inventory_entry.asset_id is not None
        else relative_path
    )
    derived_version = hashlib.sha256(
        f"{relative_path}\n{inspection.sha256}".encode("utf-8")
    ).hexdigest()
    asset_version = (
        inventory_entry.asset_version
        if inventory_entry is not None
        and inventory_entry.asset_version is not None
        else derived_version
    )
    date_path = captured_at.strftime("%Y/%m/%d")
    filename = PurePosixPath(relative_path).name
    manifest_name = f"{PurePosixPath(filename).stem}.json"
    staging_root = normalize_relative_path(staging_prefix, directory=True)
    incoming_root = normalize_relative_path(incoming_prefix, directory=True)
    staging_base = (
        f"{staging_root}{inspection.sha256}/{asset_version}"
    )
    incoming_base = f"{incoming_root}{date_path}/{asset_version}"
    suffix = local_path.suffix.lower()
    if suffix not in CONTENT_TYPES:
        raise VideoValidationError(f"Unsupported video extension: {relative_path}")

    return ManifestPlan(
        local_path=local_path,
        relative_path=relative_path,
        catalog=entry,
        captured_at_utc=captured_at,
        inspection=inspection,
        asset_id=asset_id,
        asset_version=asset_version,
        staging_video_path=f"{staging_base}/{filename}",
        incoming_video_path=f"{incoming_base}/{filename}",
        staging_manifest_path=f"{staging_base}/{manifest_name}",
        incoming_manifest_path=f"{incoming_base}/{manifest_name}",
        content_type=CONTENT_TYPES[suffix],
    )


def manifest_bytes(
    plan: ManifestPlan,
    *,
    storage_account: str,
    filesystem: str,
    source_etag: str,
) -> bytes:
    video_uri = (
        f"abfss://{filesystem}@{storage_account}.dfs.core.windows.net/"
        f"{quote(plan.incoming_video_path, safe='/-._~')}"
    )
    payload = {
        "schema_version": 1,
        "asset_id": plan.asset_id,
        "asset_version": plan.asset_version,
        "video_uri": video_uri,
        "source_etag": source_etag,
        "expected_size_bytes": plan.inspection.size_bytes,
        "expected_sha256": plan.inspection.sha256,
        "camera_id": plan.catalog.camera_id,
        "location_id": plan.catalog.location_id,
        "captured_at_utc": plan.captured_at_utc.isoformat().replace(
            "+00:00",
            "Z",
        ),
        "camera_timezone": plan.catalog.camera_timezone,
        "counting_line": list(plan.catalog.counting_line),
        "content_type": plan.content_type,
        "duration_seconds": plan.inspection.duration_seconds,
    }
    return (
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    ).encode("utf-8")
