import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from people_counter.local_spark import (
    ManifestValidationError,
    StagingValidationError,
    _attempt_staging_path,
    _bounded_partition_count,
    _localized_models_dir,
    _process_partition,
    _require_primitive_tree,
    read_claimed_manifest,
    validate_delta_staging,
)


def manifest(item_updates=None):
    item = {
        "work_id": "work-a",
        "attempt_id": "attempt-a",
        "source_video": "/data/samples/video.mp4",
        "pipeline": "rtdetr-osnet",
        "probe_delay_seconds": 0,
        "batch_id": "batch-a",
        "batch_attempt_id": "batch-attempt-a",
        "manifest_sha256": "a" * 64,
        "expected_staging_path": "/staging",
    }
    item.update(item_updates or {})
    return {
        "schema_version": 1,
        "batch_id": "batch-a",
        "batch_attempt_id": "batch-attempt-a",
        "correlation_id": "correlation-a",
        "expected_staging_path": "/staging",
        "items": [item],
    }


class LocalSparkTests(unittest.TestCase):
    def test_localized_models_are_confined_copied_verified_and_reused(self):
        with tempfile.TemporaryDirectory() as temporary:
            source_root = Path(temporary) / "source"
            source_root.mkdir()
            source = source_root / "model.bin"
            source.write_bytes(b"model")
            spark_root = Path(temporary) / "spark"
            spark_files = SimpleNamespace(
                get=lambda name: str(source_root / name),
                getRootDirectory=lambda: str(spark_root),
            )
            work = {
                "model_identity": "identity-1",
                "spark_localized_models": {
                    "pipeline/model.bin": {
                        "localized_name": "model.bin",
                        "sha256": hashlib.sha256(b"model").hexdigest(),
                    }
                },
            }
            with patch.dict(
                "sys.modules",
                {"pyspark": SimpleNamespace(SparkFiles=spark_files)},
            ):
                first = _localized_models_dir(work)
                second = _localized_models_dir(work)
                self.assertEqual(first, second)
                self.assertEqual(
                    first,
                    spark_root / "people-counter-models" / "identity-1",
                )
                self.assertEqual(
                    (first / "pipeline/model.bin").read_bytes(), b"model"
                )
                with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                    _localized_models_dir(
                        {
                            **work,
                            "spark_localized_models": {
                                "pipeline/model.bin": {
                                    "localized_name": "model.bin",
                                    "sha256": "0" * 64,
                                }
                            },
                        }
                    )

    def test_localized_models_reject_empty_and_unsafe_mappings(self):
        self.assertIsNone(_localized_models_dir({}))
        spark_files = SimpleNamespace(getRootDirectory=lambda: "/safe")
        with patch.dict(
            "sys.modules",
            {"pyspark": SimpleNamespace(SparkFiles=spark_files)},
        ):
            for value in (
                {},
                {"/absolute": {}},
                {"../escape": {}},
                {"safe": "not-a-mapping"},
            ):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    _localized_models_dir(
                        {
                            "model_identity": "identity-1",
                            "spark_localized_models": value,
                        }
                    )

    def test_manifest_is_hash_verified_and_primitive_only(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "manifest.json"
            content = json.dumps(manifest()).encode()
            path.write_bytes(content)
            digest = hashlib.sha256(content).hexdigest()

            loaded, actual = read_claimed_manifest(
                path,
                expected_sha256=digest,
            )
            self.assertEqual(loaded["batch_id"], "batch-a")
            self.assertEqual(actual, digest)
            with self.assertRaisesRegex(
                ManifestValidationError,
                "SHA-256 mismatch",
            ):
                read_claimed_manifest(path, expected_sha256="0" * 64)
            path.write_text("{")
            with self.assertRaisesRegex(
                ManifestValidationError,
                "manifest is not valid JSON",
            ):
                read_claimed_manifest(path)
            path.write_text("[]")
            with self.assertRaisesRegex(
                ManifestValidationError,
                "manifest root must be an object",
            ):
                read_claimed_manifest(path)

    def test_manifest_rejects_duplicates_non_primitives_and_empty_items(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "manifest.json"
            invalid = (
                {**manifest(), "items": []},
                {**manifest(), "items": manifest()["items"] * 2},
                manifest(),
            )
            for index, value in enumerate(invalid[:2]):
                with self.subTest(index=index):
                    path.write_text(json.dumps(value))
                    with self.assertRaises(ManifestValidationError):
                        read_claimed_manifest(path)
            with self.assertRaises(ManifestValidationError):
                _require_primitive_tree(object())

    def test_probe_partition_reuses_executor_primitive_and_emits_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            video = Path(temporary) / "video.mp4"
            video.write_bytes(b"sample")
            item = manifest(
                {
                    "source_video": str(video),
                    "source_sha256": hashlib.sha256(b"sample").hexdigest(),
                }
            )["items"][0]
            with (
                patch.dict(
                    os.environ,
                    {
                        "SPARK_EXECUTOR_ID": "executor-2",
                        "PEOPLE_COUNTER_RELEASE_DIGEST": "release-a",
                    },
                ),
                patch("people_counter.local_spark.socket.gethostname", return_value="worker-2"),
            ):
                records = list(_process_partition([item], "probe"))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["record_type"], "video_result")
        self.assertEqual(records[0]["executor_identity"], "executor-2@worker-2")
        self.assertEqual(records[0]["release_digest"], "release-a")

    def test_probe_partition_emits_error_for_source_hash_mismatch(self):
        with tempfile.TemporaryDirectory() as temporary:
            video = Path(temporary) / "video.mp4"
            video.write_bytes(b"sample")
            item = manifest(
                {
                    "source_video": str(video),
                    "source_sha256": "0" * 64,
                }
            )["items"][0]

            records = list(_process_partition([item], "probe"))

        self.assertEqual(records[0]["record_type"], "error")
        self.assertEqual(records[0]["error_type"], "ValueError")

    def test_staging_path_is_attempt_scoped_and_confined(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.assertEqual(
                _attempt_staging_path(root, "batch", "attempt"),
                root / "batch" / "attempt",
            )
            for unsafe in ("../escape", "/escape", "a/b"):
                with self.subTest(unsafe=unsafe):
                    with self.assertRaises(ValueError):
                        _attempt_staging_path(root, unsafe, "attempt")

    def _staged_row(self, **updates):
        row = {
            "batch_id": "batch-a",
            "batch_attempt_id": "batch-attempt-a",
            "manifest_sha256": "a" * 64,
            "expected_staging_path": "/staging",
            "work_id": "work-a",
            "attempt_id": "attempt-a",
            "record_type": "video_result",
            "status": "SUCCEEDED",
            "executor_id": "1",
            "executor_host": "worker-1",
            "executor_identity": "1@worker-1",
            "release_digest": "release-a",
        }
        row.update(updates)
        return row

    def _spark_with_rows(self, rows):
        spark = MagicMock()
        spark.read.format.return_value.load.return_value.select.return_value.collect.return_value = [
            *rows
        ]
        return spark

    def test_validate_staging_requires_verified_terminal_and_worker_count(self):
        spark = self._spark_with_rows([self._staged_row()])
        item = manifest()["items"]

        result = validate_delta_staging(
            spark,
            Path("/staging"),
            item,
            batch_id="batch-a",
            batch_attempt_id="batch-attempt-a",
            manifest_sha256="a" * 64,
        )
        self.assertEqual(result.record_count, 1)
        self.assertEqual(result.batch_id, "batch-a")
        self.assertEqual(result.batch_attempt_id, "batch-attempt-a")
        self.assertEqual(result.manifest_sha256, "a" * 64)
        self.assertEqual(result.path, Path("/staging"))
        self.assertEqual(result.executor_identities, ("1@worker-1",))
        self.assertEqual(result.release_digests, ("release-a",))
        spark.read.format.assert_called_with("delta")
        spark.read.format.return_value.load.assert_called_with("/staging")
        spark.read.format.return_value.load.return_value.select.assert_called_with(
            "batch_id",
            "batch_attempt_id",
            "manifest_sha256",
            "expected_staging_path",
            "work_id",
            "attempt_id",
            "record_type",
            "status",
            "executor_id",
            "executor_host",
            "executor_identity",
            "release_digest",
        )
        with self.assertRaisesRegex(StagingValidationError, "at least 2"):
            validate_delta_staging(
                spark,
                Path("/staging"),
                item,
                batch_id="batch-a",
                batch_attempt_id="batch-attempt-a",
                manifest_sha256="a" * 64,
                minimum_executor_identities=2,
            )

    def test_validate_staging_rejects_missing_duplicate_and_foreign_terminals(self):
        cases = {
            "missing": [],
            "duplicate": [self._staged_row(), self._staged_row()],
            "foreign": [self._staged_row(work_id="foreign")],
        }
        for name, rows in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(StagingValidationError):
                    validate_delta_staging(
                        self._spark_with_rows(rows),
                        Path("/staging"),
                        manifest()["items"],
                        batch_id="batch-a",
                        batch_attempt_id="batch-attempt-a",
                        manifest_sha256="a" * 64,
                    )

        duplicate_items = manifest()["items"] * 2
        with self.assertRaisesRegex(
            StagingValidationError,
            "duplicate work identities",
        ):
            validate_delta_staging(
                self._spark_with_rows([self._staged_row()]),
                Path("/staging"),
                duplicate_items,
                batch_id="batch-a",
                batch_attempt_id="batch-attempt-a",
                manifest_sha256="a" * 64,
            )

    def test_validate_staging_surfaces_failed_terminal(self):
        result = validate_delta_staging(
            self._spark_with_rows(
                [
                    self._staged_row(
                        record_type="error",
                        status="FAILED",
                    )
                ]
            ),
            Path("/staging"),
            manifest()["items"],
            batch_id="batch-a",
            batch_attempt_id="batch-attempt-a",
            manifest_sha256="a" * 64,
        )

        self.assertEqual(result.failed_work_ids, ("work-a",))

    def test_validate_staging_rejects_attempt_manifest_path_and_host_mismatch(self):
        cases = {
            "attempt": {"attempt_id": "foreign-attempt"},
            "manifest": {"manifest_sha256": "b" * 64},
            "path": {"expected_staging_path": "/foreign"},
            "host": {"executor_host": "worker-2"},
        }
        for name, updates in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(StagingValidationError):
                    validate_delta_staging(
                        self._spark_with_rows([self._staged_row(**updates)]),
                        Path("/staging"),
                        manifest()["items"],
                        batch_id="batch-a",
                        batch_attempt_id="batch-attempt-a",
                        manifest_sha256="a" * 64,
                    )

    def test_non_terminal_rows_do_not_count_as_worker_placement(self):
        rows = [
            self._staged_row(
                record_type="telemetry",
                executor_id="2",
                executor_host="worker-2",
                executor_identity="2@worker-2",
            ),
            self._staged_row(),
        ]

        with self.assertRaisesRegex(StagingValidationError, "at least 2"):
            validate_delta_staging(
                self._spark_with_rows(rows),
                Path("/staging"),
                manifest()["items"],
                batch_id="batch-a",
                batch_attempt_id="batch-attempt-a",
                manifest_sha256="a" * 64,
                minimum_executor_identities=2,
            )

    def test_sdk_partition_count_reuses_runtime_but_preserves_canary_width(self):
        self.assertEqual(_bounded_partition_count(4, 1), 2)
        self.assertEqual(_bounded_partition_count(2, 1), 1)
        self.assertEqual(_bounded_partition_count(2, 2), 2)
        with self.assertRaisesRegex(ValueError, "cannot exceed"):
            _bounded_partition_count(1, 2)
        with self.assertRaisesRegex(ValueError, "item_count must be positive"):
            _bounded_partition_count(0, 1)
        with self.assertRaisesRegex(ValueError, "must be at least one"):
            _bounded_partition_count(1, 0)


if __name__ == "__main__":
    unittest.main()
