import csv
import json
import sqlite3
import subprocess
import tempfile
import unittest
import io
import contextlib
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from people_counter.adls import (
    AzureDataLakeStorage,
    PublicationConflictError,
    RemoteObject,
)
from people_counter.manifest import (
    CatalogError,
    ManifestPublisherError,
    VideoInspection,
    build_manifest_plan,
    inspect_video,
    load_camera_catalog,
    load_video_inventory,
    manifest_bytes,
    match_catalog_entry,
)
from people_counter.manifest_publisher import (
    PublisherConfig,
    PublishSummary,
    build_parser,
    discover_videos,
    main,
    run_publisher,
)


CATALOG_FIELDS = (
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
)


class FakeStorage:
    def __init__(self):
        self.objects = {}
        self.operations = []

    def stat(self, path):
        value = self.objects.get(path)
        if value is None:
            return None
        content, etag = value
        return RemoteObject(size=len(content), etag=etag)

    def read_bytes(self, path):
        return self.objects[path][0]

    def upload_file(
        self,
        local_path,
        remote_path,
        content_type,
        expected_size,
        expected_sha256,
    ):
        self.operations.append(("upload_file", remote_path, content_type))
        content = local_path.read_bytes()
        self.assert_upload_matches = (
            len(content) == expected_size
            and expected_sha256
            == "a" * 64
        )
        existing = self.objects.get(remote_path)
        if existing is not None and existing[0] != content:
            raise PublicationConflictError("staged video conflict")
        self.objects[remote_path] = (content, '"video-etag"')

    def upload_bytes(self, content, remote_path, content_type):
        self.operations.append(
            ("upload_bytes", remote_path, content_type)
        )
        existing = self.objects.get(remote_path)
        if existing is not None and existing[0] != content:
            raise PublicationConflictError("staged manifest conflict")
        self.objects[remote_path] = (content, '"manifest-etag"')

    def rename(self, source_path, destination_path):
        self.operations.append(("rename", source_path, destination_path))
        if destination_path in self.objects:
            raise PublicationConflictError("destination conflict")
        self.objects[destination_path] = self.objects.pop(source_path)


def write_catalog(
    path,
    *,
    capture_time_source="filename_utc",
    prefix="north/camera-17/",
    effective_to="",
):
    row = {
        "catalog_version": "1",
        "source_path_prefix": prefix,
        "camera_id": "camera-17",
        "location_id": "north-entrance",
        "camera_timezone": "America/Denver",
        "frame_width": "1920",
        "frame_height": "1080",
        "counting_line_x1": "0",
        "counting_line_y1": "540",
        "counting_line_x2": "1919",
        "counting_line_y2": "540",
        "capture_time_source": capture_time_source,
        "capture_time_regex": (
            r"(?P<captured_at_utc>\d{8}T\d{6}Z)"
            if capture_time_source == "filename_utc"
            else ""
        ),
        "capture_time_format": (
            "%Y%m%dT%H%M%SZ"
            if capture_time_source == "filename_utc"
            else ""
        ),
        "effective_from_utc": "2026-01-01T00:00:00Z",
        "effective_to_utc": effective_to,
    }
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CATALOG_FIELDS)
        writer.writeheader()
        writer.writerow(row)


def write_inventory(path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "video_relative_path",
                "captured_at_utc",
                "asset_id",
                "asset_version",
            ),
        )
        writer.writeheader()
        writer.writerows(rows)


def inspection(path):
    del path
    return VideoInspection(
        size_bytes=5,
        sha256="a" * 64,
        width=1920,
        height=1080,
        duration_seconds=30.0,
    )


class ManifestCatalogTests(unittest.TestCase):
    def test_catalog_and_inventory_build_deterministic_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            catalog_path = root / "catalog.csv"
            inventory_path = root / "inventory.csv"
            video = root / "north/camera-17/clip.mp4"
            video.parent.mkdir(parents=True)
            video.write_bytes(b"video")
            write_catalog(
                catalog_path,
                capture_time_source="inventory",
            )
            write_inventory(
                inventory_path,
                [
                    {
                        "video_relative_path": "north/camera-17/clip.mp4",
                        "captured_at_utc": "2026-02-03T04:05:06Z",
                        "asset_id": "source-asset-1",
                        "asset_version": "source-version-2",
                    }
                ],
            )

            plan = build_manifest_plan(
                video,
                root,
                load_camera_catalog(catalog_path),
                load_video_inventory(inventory_path),
                staging_prefix="staging",
                incoming_prefix="incoming",
                inspector=inspection,
            )
            payload = json.loads(
                manifest_bytes(
                    plan,
                    storage_account="account",
                    filesystem="footage",
                    source_etag='"etag"',
                )
            )

        self.assertEqual(plan.asset_id, "source-asset-1")
        self.assertEqual(plan.asset_version, "source-version-2")
        self.assertEqual(
            plan.incoming_video_path,
            "incoming/2026/02/03/source-version-2/clip.mp4",
        )
        self.assertEqual(payload["source_etag"], '"etag"')
        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(payload["asset_id"], "source-asset-1")
        self.assertEqual(payload["asset_version"], "source-version-2")
        self.assertEqual(payload["expected_sha256"], "a" * 64)
        self.assertEqual(payload["expected_size_bytes"], 5)
        self.assertEqual(payload["camera_id"], "camera-17")
        self.assertEqual(payload["location_id"], "north-entrance")
        self.assertEqual(payload["camera_timezone"], "America/Denver")
        self.assertEqual(payload["counting_line"], [0, 540, 1919, 540])
        self.assertEqual(payload["content_type"], "video/mp4")
        self.assertEqual(payload["duration_seconds"], 30.0)
        self.assertEqual(payload["captured_at_utc"], "2026-02-03T04:05:06Z")
        self.assertEqual(
            payload["video_uri"],
            (
                "abfss://footage@account.dfs.core.windows.net/"
                "incoming/2026/02/03/source-version-2/clip.mp4"
            ),
        )

    def test_filename_time_derives_stable_asset_version(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            catalog_path = root / "catalog.csv"
            video = root / "north/camera-17/20260203T040506Z.mp4"
            video.parent.mkdir(parents=True)
            video.write_bytes(b"video")
            write_catalog(catalog_path)

            first = build_manifest_plan(
                video,
                root,
                load_camera_catalog(catalog_path),
                {},
                staging_prefix="staging",
                incoming_prefix="incoming",
                inspector=inspection,
            )
            second = build_manifest_plan(
                video,
                root,
                load_camera_catalog(catalog_path),
                {},
                staging_prefix="staging",
                incoming_prefix="incoming",
                inspector=inspection,
            )

        self.assertEqual(first.asset_id, first.relative_path)
        self.assertEqual(first.asset_version, second.asset_version)
        self.assertEqual(len(first.asset_version), 64)
        self.assertEqual(
            first.captured_at_utc,
            datetime(2026, 2, 3, 4, 5, 6, tzinfo=timezone.utc),
        )

    def test_catalog_rejects_out_of_frame_counting_line(self):
        with tempfile.TemporaryDirectory() as directory:
            catalog_path = Path(directory) / "catalog.csv"
            write_catalog(catalog_path)
            with catalog_path.open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            rows[0]["counting_line_x2"] = "1920"
            with catalog_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=CATALOG_FIELDS)
                writer.writeheader()
                writer.writerows(rows)

            with self.assertRaisesRegex(
                CatalogError,
                "counting line is outside the frame",
            ):
                load_camera_catalog(catalog_path)

    def test_inventory_rejects_duplicate_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            inventory_path = Path(directory) / "inventory.csv"
            row = {
                "video_relative_path": "north/camera-17/clip.mp4",
                "captured_at_utc": "2026-02-03T04:05:06Z",
                "asset_id": "",
                "asset_version": "",
            }
            write_inventory(inventory_path, [row, row])

            with self.assertRaisesRegex(CatalogError, "duplicate path"):
                load_video_inventory(inventory_path)

    def test_inventory_rejects_path_traversal_asset_version(self):
        with tempfile.TemporaryDirectory() as directory:
            inventory_path = Path(directory) / "inventory.csv"
            write_inventory(
                inventory_path,
                [
                    {
                        "video_relative_path": "north/camera-17/clip.mp4",
                        "captured_at_utc": "2026-02-03T04:05:06Z",
                        "asset_id": "",
                        "asset_version": "..",
                    }
                ],
            )

            with self.assertRaisesRegex(CatalogError, "not path-safe"):
                load_video_inventory(inventory_path)

    def test_catalog_matching_reports_missing_prefix_and_filename_time(self):
        with tempfile.TemporaryDirectory() as directory:
            catalog_path = Path(directory) / "catalog.csv"
            write_catalog(catalog_path)
            entry = load_camera_catalog(catalog_path)[0]

            with self.assertRaisesRegex(
                ManifestPublisherError,
                "No camera catalog row",
            ):
                match_catalog_entry("other/clip.mp4", [entry], {})
            with self.assertRaisesRegex(
                ManifestPublisherError,
                "Filename does not match",
            ):
                match_catalog_entry(
                    "north/camera-17/clip.mp4",
                    [entry],
                    {},
                )

    def test_catalog_matching_enforces_effective_interval(self):
        with tempfile.TemporaryDirectory() as directory:
            catalog_path = Path(directory) / "catalog.csv"
            write_catalog(catalog_path)
            entry = replace(
                load_camera_catalog(catalog_path)[0],
                effective_to_utc=datetime(
                    2026,
                    2,
                    1,
                    tzinfo=timezone.utc,
                ),
            )

            with self.assertRaisesRegex(
                ManifestPublisherError,
                "found 0",
            ):
                match_catalog_entry(
                    "north/camera-17/20260203T040506Z.mp4",
                    [entry],
                    {},
                )


class ManifestPublisherTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.catalog_path = self.root / "catalog.csv"
        self.video = self.root / "north/camera-17/20260203T040506Z.mp4"
        self.video.parent.mkdir(parents=True)
        self.video.write_bytes(b"video")
        write_catalog(self.catalog_path)

    def tearDown(self):
        self.temporary_directory.cleanup()

    def config(self, *, dry_run=False, inventory_path=None):
        return PublisherConfig(
            catalog_path=self.catalog_path,
            inventory_path=inventory_path,
            video_root=self.root,
            partition_prefix="north/",
            storage_account="account",
            filesystem="footage",
            staging_prefix="staging",
            incoming_prefix="incoming",
            checkpoint_path=self.root / "checkpoint.sqlite3",
            rejection_report_path=self.root / "rejections.csv",
            max_files=1000,
            dry_run=dry_run,
        )

    def test_publisher_moves_video_before_manifest_and_is_idempotent(self):
        storage = FakeStorage()
        config = self.config()

        first = run_publisher(
            config,
            storage=storage,
            inspector=inspection,
        )
        operations_after_first = list(storage.operations)
        second = run_publisher(
            config,
            storage=storage,
            inspector=lambda _: self.fail(
                "completed checkpoint should skip inspection"
            ),
        )

        self.assertEqual(first.published, 1)
        self.assertEqual(first.already_published, 0)
        self.assertEqual(first.rejected, 0)
        self.assertEqual(
            [operation[0] for operation in operations_after_first],
            ["upload_file", "rename", "upload_bytes", "rename"],
        )
        video_upload, video_rename, manifest_upload, manifest_rename = (
            operations_after_first
        )
        self.assertTrue(video_upload[1].startswith("staging/"))
        self.assertEqual(video_upload[2], "video/mp4")
        self.assertTrue(storage.assert_upload_matches)
        self.assertEqual(video_rename[1], video_upload[1])
        self.assertTrue(video_rename[2].startswith("incoming/2026/02/03/"))
        self.assertTrue(manifest_upload[1].startswith("staging/"))
        self.assertEqual(manifest_upload[2], "application/json")
        self.assertEqual(manifest_rename[1], manifest_upload[1])
        self.assertTrue(
            manifest_rename[2].startswith("incoming/2026/02/03/")
        )
        published_manifest = json.loads(
            storage.objects[manifest_rename[2]][0]
        )
        self.assertEqual(published_manifest["source_etag"], '"video-etag"')
        self.assertTrue(
            published_manifest["video_uri"].startswith(
                "abfss://footage@account.dfs.core.windows.net/incoming/"
            )
        )
        self.assertEqual(second.published, 0)
        self.assertEqual(second.already_published, 1)
        self.assertEqual(storage.operations, operations_after_first)
        with sqlite3.connect(config.checkpoint_path) as connection:
            state, attempts, manifest_uri = connection.execute(
                "SELECT state, attempt_count, manifest_uri FROM publications"
            ).fetchone()
        self.assertEqual(state, "MANIFEST_PUBLISHED")
        self.assertEqual(attempts, 1)
        self.assertTrue(manifest_uri.endswith(".json"))

        rehashed = run_publisher(
            replace(config, rehash=True),
            storage=storage,
            inspector=inspection,
        )
        self.assertEqual(rehashed.already_published, 1)

    def test_conflicting_manifest_is_rejected_without_overwrite(self):
        storage = FakeStorage()
        config = self.config()
        plan = build_manifest_plan(
            self.video,
            self.root,
            load_camera_catalog(self.catalog_path),
            {},
            staging_prefix="staging",
            incoming_prefix="incoming",
            inspector=inspection,
        )
        storage.objects[plan.incoming_video_path] = (
            self.video.read_bytes(),
            '"video-etag"',
        )
        storage.objects[plan.incoming_manifest_path] = (
            b"conflict",
            '"manifest-etag"',
        )

        summary = run_publisher(
            config,
            storage=storage,
            inspector=inspection,
        )

        self.assertEqual(summary.rejected, 1)
        self.assertEqual(
            storage.objects[plan.incoming_manifest_path][0],
            b"conflict",
        )
        report = list(
            csv.DictReader(
                config.rejection_report_path.open(encoding="utf-8")
            )
        )
        self.assertEqual(len(report), 1)
        self.assertIn("Published manifest conflicts", report[0]["error"])

    def test_retry_after_video_rename_publishes_only_manifest(self):
        storage = FakeStorage()
        config = self.config()
        plan = build_manifest_plan(
            self.video,
            self.root,
            load_camera_catalog(self.catalog_path),
            {},
            staging_prefix="staging",
            incoming_prefix="incoming",
            inspector=inspection,
        )
        storage.objects[plan.incoming_video_path] = (
            self.video.read_bytes(),
            '"video-etag"',
        )

        summary = run_publisher(
            config,
            storage=storage,
            inspector=inspection,
        )

        self.assertEqual(summary.published, 1)
        self.assertEqual(
            [operation[0] for operation in storage.operations],
            ["upload_bytes", "rename"],
        )
        self.assertIn(plan.incoming_manifest_path, storage.objects)

    def test_missing_video_after_rename_is_rejected(self):
        class VanishingVideoStorage(FakeStorage):
            def rename(self, source_path, destination_path):
                if destination_path.endswith(".mp4"):
                    self.objects.pop(source_path)
                    return
                super().rename(source_path, destination_path)

        storage = VanishingVideoStorage()
        config = self.config()

        summary = run_publisher(
            config,
            storage=storage,
            inspector=inspection,
        )

        self.assertEqual(summary.rejected, 1)
        report = config.rejection_report_path.read_text(encoding="utf-8")
        self.assertIn("Published video size conflicts", report)

    def test_dry_run_plans_without_storage(self):
        summary = run_publisher(
            self.config(dry_run=True),
            storage=None,
            inspector=inspection,
        )

        self.assertEqual(summary.discovered, 1)
        self.assertEqual(summary.planned, 1)
        self.assertEqual(summary.published, 0)
        self.assertEqual(summary.rejected, 0)
        self.assertEqual(summary.total_video_duration_seconds, 30.0)

    def test_global_inventory_allows_rows_outside_partition(self):
        inventory_path = self.root / "inventory.csv"
        write_inventory(
            inventory_path,
            [
                {
                    "video_relative_path": "other/missing.mp4",
                    "captured_at_utc": "2026-02-03T04:05:06Z",
                    "asset_id": "",
                    "asset_version": "",
                }
            ],
        )
        config = self.config(dry_run=True, inventory_path=inventory_path)

        summary = run_publisher(
            config,
            storage=None,
            inspector=inspection,
        )

        self.assertEqual(summary.planned, 1)

    def test_missing_inventory_file_inside_partition_fails_before_checkpoint(self):
        inventory_path = self.root / "inventory.csv"
        write_inventory(
            inventory_path,
            [
                {
                    "video_relative_path": "north/camera-17/missing.mp4",
                    "captured_at_utc": "2026-02-03T04:05:06Z",
                    "asset_id": "",
                    "asset_version": "",
                }
            ],
        )
        config = self.config(inventory_path=inventory_path)

        with self.assertRaisesRegex(
            ManifestPublisherError,
            "outside the discovered partition",
        ):
            run_publisher(config, storage=None, inspector=inspection)

        self.assertFalse(config.checkpoint_path.exists())

    def test_discovery_rejects_unbounded_partition(self):
        second = self.video.with_name("20260203T050506Z.mp4")
        second.write_bytes(b"video")

        with self.assertRaisesRegex(
            ManifestPublisherError,
            "exceeding --max-files=1",
        ):
            discover_videos(self.root, "north/", 1)

    def test_source_filename_rejects_percent_encoding(self):
        encoded = self.video.with_name("20260203T050506Z%2fclip.mp4")
        encoded.write_bytes(b"video")

        summary = run_publisher(
            self.config(dry_run=True),
            storage=None,
            inspector=inspection,
        )

        self.assertEqual(summary.discovered, 2)
        self.assertEqual(summary.planned, 1)
        self.assertEqual(summary.rejected, 1)

    def test_parser_requires_adls_destination_only_for_publication(self):
        parser = build_parser()
        dry_run = parser.parse_args(
            [
                "--catalog",
                str(self.catalog_path),
                "--video-root",
                str(self.root),
                "--dry-run",
            ]
        )

        self.assertTrue(dry_run.dry_run)
        self.assertIsNone(dry_run.storage_account)
        self.assertIsNone(dry_run.filesystem)

    def test_parser_validates_storage_names(self):
        parser = build_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args(
                [
                    "--catalog",
                    str(self.catalog_path),
                    "--video-root",
                    str(self.root),
                    "--storage-account",
                    "INVALID",
                    "--filesystem",
                    "footage",
                ]
            )

    def test_main_runs_dry_run_without_loading_azure(self):
        summary = PublishSummary(
            generator_version="0.1.0",
            catalog_sha256="a" * 64,
            discovered=1,
            planned=1,
        )
        with (
            patch(
                "people_counter.manifest_publisher.run_publisher",
                return_value=summary,
            ) as run,
            patch(
                "people_counter.manifest_publisher.AzureDataLakeStorage"
            ) as azure_storage,
            contextlib.redirect_stdout(io.StringIO()) as output,
        ):
            exit_code = main(
                [
                    "--catalog",
                    str(self.catalog_path),
                    "--video-root",
                    str(self.root),
                    "--dry-run",
                ]
            )

        self.assertEqual(exit_code, 0)
        azure_storage.assert_not_called()
        self.assertEqual(
            json.loads(output.getvalue())["catalog_sha256"],
            "a" * 64,
        )
        self.assertIsNone(run.call_args.kwargs["storage"])

    def test_main_builds_azure_storage_and_returns_two_for_rejections(self):
        summary = PublishSummary(rejected=1)
        with (
            patch(
                "people_counter.manifest_publisher.run_publisher",
                return_value=summary,
            ) as run,
            patch(
                "people_counter.manifest_publisher.AzureDataLakeStorage"
            ) as azure_storage,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            exit_code = main(
                [
                    "--catalog",
                    str(self.catalog_path),
                    "--video-root",
                    str(self.root),
                    "--storage-account",
                    "account123",
                    "--filesystem",
                    "footage",
                    "--chunk-size-mib",
                    "4",
                    "--rehash",
                ]
            )

        self.assertEqual(exit_code, 2)
        azure_storage.assert_called_once_with(
            "account123",
            "footage",
            chunk_size=4 * 1024 * 1024,
        )
        config = run.call_args.args[0]
        self.assertTrue(config.rehash)

    def test_main_requires_destination_for_publication(self):
        with (
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit) as raised,
        ):
            main(
                [
                    "--catalog",
                    str(self.catalog_path),
                    "--video-root",
                    str(self.root),
                ]
            )

        self.assertEqual(raised.exception.code, 2)


class VideoInspectionTests(unittest.TestCase):
    def test_inspection_streams_hash_and_uses_ffprobe_metadata(self):
        with tempfile.NamedTemporaryFile(suffix=".mp4") as video:
            Path(video.name).write_bytes(b"video")
            completed = subprocess.CompletedProcess(
                args=["ffprobe"],
                returncode=0,
                stdout=json.dumps(
                    {
                        "streams": [{"width": 1920, "height": 1080}],
                        "format": {"duration": "30.5"},
                    }
                ),
                stderr="",
            )
            with patch(
                "people_counter.manifest.subprocess.run",
                return_value=completed,
            ) as run:
                result = inspect_video(Path(video.name), chunk_size=2)

        self.assertEqual(result.size_bytes, 5)
        self.assertEqual(result.width, 1920)
        self.assertEqual(result.height, 1080)
        self.assertEqual(result.duration_seconds, 30.5)
        self.assertEqual(
            result.sha256,
            "0cab1c9617404faf2b24e221e189ca5945813e14d3f766345b09ca13bbe28ffc",
        )
        run.assert_called_once()

    def test_inspection_surfaces_ffprobe_failure_as_video_rejection(self):
        with tempfile.NamedTemporaryFile(suffix=".mp4") as video:
            Path(video.name).write_bytes(b"video")
            with (
                patch(
                    "people_counter.manifest.subprocess.run",
                    side_effect=subprocess.CalledProcessError(
                        1,
                        ["ffprobe"],
                        stderr="corrupt stream",
                    ),
                ),
                self.assertRaisesRegex(
                    ManifestPublisherError,
                    "corrupt stream",
                ),
            ):
                inspect_video(Path(video.name))

    def test_inspection_falls_back_from_na_stream_duration(self):
        with tempfile.NamedTemporaryFile(suffix=".mp4") as video:
            Path(video.name).write_bytes(b"video")
            completed = subprocess.CompletedProcess(
                args=["ffprobe"],
                returncode=0,
                stdout=json.dumps(
                    {
                        "streams": [
                            {
                                "width": 1920,
                                "height": 1080,
                                "duration": "N/A",
                            }
                        ],
                        "format": {"duration": "31.25"},
                    }
                ),
                stderr="",
            )
            with patch(
                "people_counter.manifest.subprocess.run",
                return_value=completed,
            ):
                result = inspect_video(Path(video.name))

        self.assertEqual(result.duration_seconds, 31.25)

    def test_inspection_prefers_valid_stream_duration(self):
        with tempfile.NamedTemporaryFile(suffix=".mp4") as video:
            Path(video.name).write_bytes(b"video")
            completed = subprocess.CompletedProcess(
                args=["ffprobe"],
                returncode=0,
                stdout=json.dumps(
                    {
                        "streams": [
                            {
                                "width": 1920,
                                "height": 1080,
                                "duration": "29.75",
                            }
                        ],
                        "format": {"duration": "30.0"},
                    }
                ),
                stderr="",
            )
            with patch(
                "people_counter.manifest.subprocess.run",
                return_value=completed,
            ):
                result = inspect_video(Path(video.name))

        self.assertEqual(result.duration_seconds, 29.75)


class FakeFileClient:
    def __init__(self, filesystem, path):
        self.filesystem = filesystem
        self.path = path

    def exists(self):
        return self.path in self.filesystem.objects

    def get_file_properties(self):
        content, etag = self.filesystem.objects[self.path]
        return SimpleNamespace(size=len(content), etag=etag)

    def create_file(self, **kwargs):
        self.filesystem.create_settings[self.path] = kwargs
        self.filesystem.objects[self.path] = (b"", '"etag"')

    def append_data(self, data, offset, length):
        content, etag = self.filesystem.objects[self.path]
        self.filesystem.appends.append((self.path, offset, length))
        self.filesystem.objects[self.path] = (
            content[:offset] + bytes(data),
            etag,
        )

    def flush_data(self, offset, **kwargs):
        self.filesystem.flushes.append((self.path, offset, kwargs))

    def download_file(self):
        content = self.filesystem.objects[self.path][0]
        return SimpleNamespace(readall=lambda: content)

    def rename_file(self, destination, **kwargs):
        self.filesystem.renames.append((self.path, destination, kwargs))
        if self.filesystem.rename_error is not None:
            raise self.filesystem.rename_error
        destination_path = destination.split("/", 1)[1]
        self.filesystem.objects[destination_path] = self.filesystem.objects.pop(
            self.path
        )


class FakeDirectoryClient:
    def __init__(self, filesystem, path):
        self.filesystem = filesystem
        self.path = path

    def exists(self):
        return self.path in self.filesystem.directories

    def create_directory(self):
        self.filesystem.directories.add(self.path)


class FakeFileSystem:
    def __init__(self):
        self.objects = {}
        self.directories = set()
        self.appends = []
        self.flushes = []
        self.renames = []
        self.create_settings = {}
        self.rename_error = None

    def get_file_client(self, path):
        return FakeFileClient(self, path)

    def get_directory_client(self, path):
        return FakeDirectoryClient(self, path)


class AzureDataLakeStorageTests(unittest.TestCase):
    def storage(self, filesystem, chunk_size=2):
        storage = AzureDataLakeStorage.__new__(AzureDataLakeStorage)
        storage._filesystem = "footage"
        storage._client = filesystem
        storage._chunk_size = chunk_size
        storage._known_directories = set()
        return storage

    def test_upload_file_resumes_from_remote_size_and_sets_content_type(self):
        filesystem = FakeFileSystem()
        filesystem.objects["staging/video.mp4"] = (b"vi", '"etag"')
        storage = self.storage(filesystem)
        with tempfile.NamedTemporaryFile(suffix=".mp4") as video:
            Path(video.name).write_bytes(b"video")

            storage.upload_file(
                Path(video.name),
                "staging/video.mp4",
                "video/mp4",
                5,
                (
                    "0cab1c9617404faf2b24e221e189ca5945813e14"
                    "d3f766345b09ca13bbe28ffc"
                ),
            )

        self.assertEqual(
            filesystem.objects["staging/video.mp4"][0],
            b"video",
        )
        self.assertEqual(
            filesystem.appends,
            [
                ("staging/video.mp4", 2, 2),
                ("staging/video.mp4", 4, 1),
            ],
        )
        self.assertEqual(
            filesystem.flushes[-1][2]["content_settings"].content_type,
            "video/mp4",
        )
        self.assertEqual(
            [flush[1] for flush in filesystem.flushes],
            [4, 5],
        )

    def test_rename_uses_server_side_if_missing_condition(self):
        from azure.core import MatchConditions

        filesystem = FakeFileSystem()
        filesystem.objects["staging/video.mp4"] = (b"video", '"etag"')
        storage = self.storage(filesystem)

        storage.rename("staging/video.mp4", "incoming/video.mp4")

        _, destination, kwargs = filesystem.renames[0]
        self.assertEqual(destination, "footage/incoming/video.mp4")
        self.assertEqual(kwargs["etag"], "*")
        self.assertIs(
            kwargs["match_condition"],
            MatchConditions.IfMissing,
        )

    def test_upload_and_read_manifest_bytes(self):
        filesystem = FakeFileSystem()
        storage = self.storage(filesystem)

        storage.upload_bytes(
            b'{"schema_version":1}\n',
            "staging/video.json",
            "application/json",
        )

        self.assertEqual(
            storage.read_bytes("staging/video.json"),
            b'{"schema_version":1}\n',
        )
        self.assertEqual(storage.stat("staging/video.json").size, 21)
        self.assertEqual(
            filesystem.create_settings["staging/video.json"][
                "content_settings"
            ].content_type,
            "application/json",
        )

    def test_rename_translates_server_destination_race(self):
        from azure.core.exceptions import ResourceExistsError

        filesystem = FakeFileSystem()
        filesystem.objects["staging/video.mp4"] = (b"video", '"etag"')
        filesystem.rename_error = ResourceExistsError("race")
        storage = self.storage(filesystem)

        with self.assertRaisesRegex(
            PublicationConflictError,
            "Destination already exists: incoming/video.mp4",
        ):
            storage.rename("staging/video.mp4", "incoming/video.mp4")


if __name__ == "__main__":
    unittest.main()
