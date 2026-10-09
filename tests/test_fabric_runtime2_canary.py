from __future__ import annotations

import importlib
import json
import sys
import tempfile
import unittest
import uuid
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

from people_counter.fabric_runtime2_canary import (
    FILES_PREFIX,
    LAKEHOUSE_ID,
    SAFETY_TOKEN,
    WORKSPACE_ID,
    WRITE_SCOPE,
    CanaryValidationError,
    LocalCanaryStorage,
    RunRootExistsError,
    RuntimeObservation,
    _executor_local_probe_rows,
    _probe_row,
    build_parser,
    installed_package_identity,
    run_local_analogue,
    safe_child,
    validate_arguments,
    validate_artifacts,
    validate_default_lakehouse,
    validate_probe_records,
    validate_runtime,
    sha256_bytes,
    sha256_json,
)


RUN_ID = "68dcae03-e70c-4d74-b635-73fc26f93141"
RELEASE = "a" * 64
DEFINITION = "c" * 64
SOURCE_ARCHIVE = "d" * 64
PACKAGE_IDENTITY = installed_package_identity()


def arguments(run_id: str = RUN_ID):
    return validate_arguments(
        WORKSPACE_ID,
        LAKEHOUSE_ID,
        run_id,
        WRITE_SCOPE,
        SAFETY_TOKEN,
        RELEASE,
        "0.6.0",
        PACKAGE_IDENTITY,
        DEFINITION,
        SOURCE_ARCHIVE,
    )


def runtime(**updates: str | None) -> RuntimeObservation:
    values = {
        "python": "3.13.7",
        "spark": "4.1.1",
        "java": "openjdk version 21.0.8",
        "scala": "2.13.16",
        "delta": "4.2.0",
        "package_version": "0.6.0",
        "release_digest": RELEASE,
    }
    values.update(updates)
    return RuntimeObservation(**values)


class ArgumentAndRuntimeTests(unittest.TestCase):
    def test_exact_argument_contract_and_run_path(self) -> None:
        validated = arguments()
        self.assertEqual(
            validated.relative_run_root,
            f"{FILES_PREFIX}/run={RUN_ID}",
        )
        self.assertEqual(
            safe_child(validated.relative_run_root, "input/manifest.json"),
            f"{validated.relative_run_root}/input/manifest.json",
        )

    def test_rejects_identity_scope_token_digest_and_paths(self) -> None:
        cases = (
            ("workspace", (str(uuid.uuid4()), LAKEHOUSE_ID, RUN_ID, WRITE_SCOPE, SAFETY_TOKEN, RELEASE, "0.6.0", PACKAGE_IDENTITY, DEFINITION, SOURCE_ARCHIVE)),
            ("lakehouse", (WORKSPACE_ID, str(uuid.uuid4()), RUN_ID, WRITE_SCOPE, SAFETY_TOKEN, RELEASE, "0.6.0", PACKAGE_IDENTITY, DEFINITION, SOURCE_ARCHIVE)),
            ("run", (WORKSPACE_ID, LAKEHOUSE_ID, "__REQUIRED__", WRITE_SCOPE, SAFETY_TOKEN, RELEASE, "0.6.0", PACKAGE_IDENTITY, DEFINITION, SOURCE_ARCHIVE)),
            ("scope", (WORKSPACE_ID, LAKEHOUSE_ID, RUN_ID, "../Tables", SAFETY_TOKEN, RELEASE, "0.6.0", PACKAGE_IDENTITY, DEFINITION, SOURCE_ARCHIVE)),
            ("token", (WORKSPACE_ID, LAKEHOUSE_ID, RUN_ID, WRITE_SCOPE, "wrong", RELEASE, "0.6.0", PACKAGE_IDENTITY, DEFINITION, SOURCE_ARCHIVE)),
            ("release", (WORKSPACE_ID, LAKEHOUSE_ID, RUN_ID, WRITE_SCOPE, SAFETY_TOKEN, "abc", "0.6.0", PACKAGE_IDENTITY, DEFINITION, SOURCE_ARCHIVE)),
            ("version", (WORKSPACE_ID, LAKEHOUSE_ID, RUN_ID, WRITE_SCOPE, SAFETY_TOKEN, RELEASE, "", PACKAGE_IDENTITY, DEFINITION, SOURCE_ARCHIVE)),
            ("package", (WORKSPACE_ID, LAKEHOUSE_ID, RUN_ID, WRITE_SCOPE, SAFETY_TOKEN, RELEASE, "0.6.0", "bad", DEFINITION, SOURCE_ARCHIVE)),
            ("definition", (WORKSPACE_ID, LAKEHOUSE_ID, RUN_ID, WRITE_SCOPE, SAFETY_TOKEN, RELEASE, "0.6.0", PACKAGE_IDENTITY, "bad", SOURCE_ARCHIVE)),
            ("archive", (WORKSPACE_ID, LAKEHOUSE_ID, RUN_ID, WRITE_SCOPE, SAFETY_TOKEN, RELEASE, "0.6.0", PACKAGE_IDENTITY, DEFINITION, "bad")),
        )
        for name, values in cases:
            with self.subTest(name=name):
                with self.assertRaises(CanaryValidationError):
                    validate_arguments(*values)
        for unsafe in ("", "../escape", "/Tables/x", "abfss://other/path", "a\\b"):
            with self.subTest(path=unsafe):
                with self.assertRaises(CanaryValidationError):
                    safe_child(arguments().relative_run_root, unsafe)
        with self.assertRaises(CanaryValidationError):
            safe_child("Files/unmanaged/run=value", "child")

    def test_runtime_parser_requires_reviewed_versions_and_observable_versions(self) -> None:
        self.assertEqual(validate_runtime(runtime()).spark, "4.1.1")
        self.assertIsNone(validate_runtime(runtime(scala=None, delta=None)).delta)
        for name, value in (
            ("python", "3.12.10"),
            ("spark", "3.5.5"),
            ("java", "openjdk 17"),
            ("scala", "2.12.20"),
            ("delta", "4.1.0"),
            ("package_version", ""),
            ("release_digest", "b"),
        ):
            with self.subTest(name=name):
                with self.assertRaises(CanaryValidationError):
                    validate_runtime(runtime(**{name: value}))

    def test_default_lakehouse_requires_positive_fixed_identity(self) -> None:
        spark = MagicMock()
        values = {
            "trident.lakehouse.id": LAKEHOUSE_ID,
            "trident.workspace.id": WORKSPACE_ID,
        }
        spark.conf.get.side_effect = lambda key: values[key]
        observed = validate_default_lakehouse(spark, MagicMock(), arguments())
        self.assertEqual(observed, values)

        spark.conf.get.side_effect = KeyError
        with self.assertRaisesRegex(CanaryValidationError, "not observable"):
            validate_default_lakehouse(spark, MagicMock(), arguments())

        values["trident.lakehouse.id"] = str(uuid.uuid4())
        spark.conf.get.side_effect = lambda key: values[key]
        with self.assertRaisesRegex(CanaryValidationError, "conflicting"):
            validate_default_lakehouse(spark, MagicMock(), arguments())

    def test_observed_fabric_bindings_normalize_and_ignore_false_positives(self) -> None:
        spark = MagicMock()
        spark.conf.getAll = {
            "spark.hadoop.trident.lakehouse.id": f"{{{LAKEHOUSE_ID.upper()}}}",
            "spark.hadoop.trident.workspace.id": f'"{WORKSPACE_ID}"',
            "spark.hadoop.trident.artifact.workspace.id": WORKSPACE_ID,
            "unrelated.lakehouse.id": str(uuid.uuid4()),
            "spark.synapse.workspace.name": "not-an-id",
        }
        spark.conf.get.side_effect = KeyError
        observed = validate_default_lakehouse(
            spark, MagicMock(), arguments()
        )
        self.assertEqual(
            observed["spark.hadoop.trident.lakehouse.id"], LAKEHOUSE_ID
        )
        self.assertNotIn("unrelated.lakehouse.id", observed)

        spark.conf.getAll = {
            "unrelated.lakehouse.id": LAKEHOUSE_ID,
            "unrelated.workspace.id": WORKSPACE_ID,
        }
        with self.assertRaisesRegex(CanaryValidationError, "not observable"):
            validate_default_lakehouse(spark, MagicMock(), arguments())

    def test_conflicting_authoritative_binding_is_rejected(self) -> None:
        spark = MagicMock()
        spark.conf.getAll = {
            "spark.hadoop.trident.lakehouse.id": LAKEHOUSE_ID,
            "spark.hadoop.trident.workspace.id": WORKSPACE_ID,
            "spark.hadoop.trident.artifact.workspace.id": str(uuid.uuid4()),
        }
        spark.conf.get.side_effect = KeyError
        with self.assertRaisesRegex(CanaryValidationError, "conflicting"):
            validate_default_lakehouse(spark, MagicMock(), arguments())

    def test_help_and_import_do_not_load_pyspark(self) -> None:
        sys.modules.pop("pyspark", None)
        importlib.import_module("people_counter.fabric_runtime2_canary")
        self.assertNotIn("pyspark", sys.modules)

    def test_executor_probe_source_is_localized_without_changing_identity(self) -> None:
        sys.modules.pop("pyspark", None)
        row = _probe_row(
            arguments(),
            0,
            f"{arguments().relative_run_root}/input/probe-{RUN_ID}.txt",
            sha256_bytes(b"probe"),
            "b" * 64,
        )
        row["distributed_source_name"] = f"probe-{RUN_ID}.txt"
        spark_files = MagicMock()
        spark_files.get.return_value = "/executor/spark-files/probe.txt"

        localized = _executor_local_probe_rows([row], spark_files)

        spark_files.get.assert_called_once_with(f"probe-{RUN_ID}.txt")
        self.assertEqual(
            localized[0]["source_video"], "/executor/spark-files/probe.txt"
        )
        self.assertEqual(localized[0]["work_id"], row["work_id"])
        self.assertEqual(localized[0]["source_sha256"], row["source_sha256"])
        with self.assertRaises(CanaryValidationError) as raised:
            _executor_local_probe_rows(
                [{**row, "distributed_source_name": "../escape"}],
                spark_files,
            )
        self.assertEqual(
            str(raised.exception), "invalid distributed probe source name"
        )
        for invalid in (None, "", 7):
            with self.subTest(distributed_source_name=invalid):
                with self.assertRaises(CanaryValidationError):
                    _executor_local_probe_rows(
                        [{**row, "distributed_source_name": invalid}],
                        spark_files,
                    )
        with self.assertRaises(SystemExit) as raised:
            build_parser().parse_args(["--help"])
        self.assertEqual(raised.exception.code, 0)
        self.assertNotIn("pyspark", sys.modules)


class LocalAnalogueTests(unittest.TestCase):
    def test_local_analogue_is_immutable_pointer_backed_and_rerun_fails(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
            writes: list[str] = []

            class RecordingStorage(LocalCanaryStorage):
                def write_exclusive(self, relative, content):
                    writes.append(relative)
                    super().write_exclusive(relative, content)

            storage = RecordingStorage(Path(temporary))
            first = run_local_analogue(storage, arguments(), runtime=runtime())
            root = Path(temporary) / arguments().relative_run_root
            self.assertEqual(first["status"], "SUCCEEDED")
            self.assertTrue((root / "_SUCCESS").is_file())
            self.assertTrue((root / "attempts/records.json").is_file())
            self.assertTrue((root / "pointer_delta/row.json").is_file())
            self.assertTrue(writes[-1].endswith("/_SUCCESS"))
            with self.assertRaises(RunRootExistsError):
                run_local_analogue(storage, arguments(), runtime=runtime())

            second_id = "3cc6f7ad-853a-47ea-bcec-ea7a158002f2"
            second = run_local_analogue(
                storage, arguments(second_id), runtime=runtime()
            )
            self.assertEqual(second["runId"], second_id)
            self.assertTrue(
                (Path(temporary) / arguments(second_id).relative_run_root).is_dir()
            )

    def test_marker_pointer_result_tampering_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
            storage = LocalCanaryStorage(Path(temporary))
            run_local_analogue(storage, arguments(), runtime=runtime())
            root = Path(temporary) / arguments().relative_run_root
            records = json.loads((root / "attempts/records.json").read_text())
            input_file = root / "input/probe.txt"
            manifest_hash = sha256_json(
                {"runId": RUN_ID, "partitions": arguments().partitions}
            )
            expected_rows = [
                _probe_row(
                    arguments(),
                    index,
                    str(input_file),
                    sha256_bytes(input_file.read_bytes()),
                    manifest_hash,
                )
                for index in range(arguments().partitions)
            ]
            pointer = json.loads((root / "pointer.json").read_text())
            pointer["recordsSha256"] = "0" * 64
            (root / "pointer.json").write_text(json.dumps(pointer))
            with self.assertRaises(CanaryValidationError):
                validate_artifacts(storage, arguments(), records, expected_rows)

    def test_each_chain_artifact_tamper_and_missing_artifact_is_rejected(self) -> None:
        cases = ("success", "result", "pointer", "records", "pointer_delta", "missing")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory(
                dir=Path.cwd()
            ) as temporary:
                storage = LocalCanaryStorage(Path(temporary))
                run_local_analogue(storage, arguments(), runtime=runtime())
                root = Path(temporary) / arguments().relative_run_root
                records_path = root / "attempts/records.json"
                records = json.loads(records_path.read_text())
                input_file = root / "input/probe.txt"
                manifest_hash = sha256_json(
                    {"runId": RUN_ID, "partitions": arguments().partitions}
                )
                expected_rows = [
                    _probe_row(
                        arguments(),
                        index,
                        str(input_file),
                        sha256_bytes(input_file.read_bytes()),
                        manifest_hash,
                    )
                    for index in range(arguments().partitions)
                ]
                paths = {
                    "success": root / "_SUCCESS",
                    "result": root / "result.json",
                    "pointer": root / "pointer.json",
                    "pointer_delta": root / "pointer_delta/row.json",
                }
                if case == "records":
                    records[0]["status"] = "FAILED"
                elif case == "missing":
                    paths["result"].unlink()
                else:
                    document = json.loads(paths[case].read_text())
                    document["recordCount"] = 999
                    paths[case].write_text(json.dumps(document))
                with self.assertRaises(CanaryValidationError):
                    validate_artifacts(
                        storage, arguments(), records, expected_rows
                    )

    def test_exact_executor_provenance_rejects_tampering(self) -> None:
        row = {
            "work_id": "canary-partition-0000",
            "attempt_id": "attempt",
            "manifest_sha256": "m",
            "input_payload_sha256": "i",
            "config_sha256": "c",
            "model_identity": "model",
            "release_digest": RELEASE,
            "project_version": "0.6.0",
            "package_identity": PACKAGE_IDENTITY,
            "physical_partition": 0,
        }
        record = {
            **row,
            "payload_json": "{}",
            "record_payload_sha256": sha256_json({}),
            "partition_id": 0,
            "cpu_threads": 1,
            "status": "SUCCEEDED",
            "executor_host": "worker",
            "executor_identity": "1@worker",
            "stage_id": 2,
            "task_attempt_id": 3,
            "thread_id": 4,
            "record_type": "video_result",
        }
        summary = validate_probe_records([record], [row], arguments())
        self.assertEqual(summary["terminalCount"], 1)
        for name, value in (
            ("partition_id", 1),
            ("executor_identity", "1@other"),
            ("task_attempt_id", -1),
            ("release_digest", "b" * 64),
        ):
            with self.subTest(name=name):
                with self.assertRaises(CanaryValidationError):
                    validate_probe_records(
                        [{**record, name: value}], [row], arguments()
                    )

    def test_storage_rejects_escape_and_create_only_write(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
            storage = LocalCanaryStorage(Path(temporary))
            with self.assertRaises(CanaryValidationError):
                storage.write_exclusive("../escape", b"x")
            storage.write_exclusive("Files/x", b"one")
            with self.assertRaises(CanaryValidationError):
                storage.write_exclusive("Files/x", b"two")


if __name__ == "__main__":
    unittest.main()
