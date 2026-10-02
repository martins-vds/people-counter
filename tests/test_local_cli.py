import contextlib
import io
import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, patch

from people_counter.local_cli import _submit, build_parser, main
from people_counter.local_control import ControlRun
from people_counter.local_spark import BatchStaging
from people_counter.local_storage import StoredObject
from people_counter.local_storage import ContentAddressedStore


class LocalCliTests(unittest.TestCase):
    def test_parser_exposes_control_commands(self):
        parser = build_parser()
        action = next(
            item
            for item in parser._actions
            if item.__class__.__name__ == "_SubParsersAction"
        )
        self.assertEqual(
            set(action.choices),
            {"init", "status", "recover", "seed", "submit"},
        )

    def test_init_seed_and_status_round_trip(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = root / "control" / "queue.sqlite3"
            content = root / "content"
            video = root / "video.mp4"
            video.write_bytes(b"video")

            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    main(
                        [
                            "--database",
                            str(database),
                            "--content-root",
                            str(content),
                            "init",
                        ]
                    ),
                    0,
                )
                self.assertEqual(
                    main(
                        [
                            "--database",
                            str(database),
                            "--content-root",
                            str(content),
                            "seed",
                            "--video",
                            str(video),
                            "--idempotency-prefix",
                            "smoke",
                            "--copies",
                            "2",
                        ]
                    ),
                    0,
                )
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(
                    main(
                        [
                            "--database",
                            str(database),
                            "--content-root",
                            str(content),
                            "status",
                        ]
                    ),
                    0,
                )

        messages = json.loads(output.getvalue())["messages"]
        self.assertEqual(len(messages), 2)
        self.assertEqual({message["state"] for message in messages}, {"ready"})

    def test_submit_wires_manifest_runtime_staging_and_shutdown(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path = root / "manifest.json"
            manifest_path.write_text("{}")
            staging = BatchStaging(
                batch_id="batch-a",
                batch_attempt_id="attempt-a",
                manifest_sha256="d" * 64,
                path=root / "staging",
                record_count=2,
                executor_identities=("1@worker-1", "2@worker-2"),
                release_digests=("release-a",),
                failed_work_ids=(),
            )
            stored = StoredObject("d" * 64, 2, manifest_path)
            spark = MagicMock(version="4.1.1")
            logger = MagicMock()

            def control_side_effect(actor, store, runner, **kwargs):
                self.assertEqual(kwargs["owner"].split(":")[0], "test-host")
                self.assertIsInstance(store, ContentAddressedStore)
                self.assertEqual(kwargs["minimum_items"], 2)
                self.assertEqual(kwargs["staging_root"], root / "staging")
                self.assertEqual(
                    runner(manifest_path, stored.sha256, "correlation-a"),
                    staging,
                )
                return ControlRun(
                    "batch-a",
                    "correlation-a",
                    stored,
                    staging,
                    2,
                )

            args = Namespace(
                database=root / "queue.sqlite3",
                content_root=root / "content",
                master="spark://master:7077",
                staging_root=root / "staging",
                mode="probe",
                minimum_workers=2,
                max_items=2,
                lock_seconds=120,
                heartbeat_seconds=30,
            )
            output = io.StringIO()
            with (
                patch(
                    "people_counter.local_cli.socket.gethostname",
                    return_value="test-host",
                ),
                patch(
                    "people_counter.local_cli.logging.getLogger",
                    return_value=logger,
                ),
                patch(
                    "people_counter.local_cli.SerializedQueueActor",
                ),
                patch(
                    "people_counter.local_cli.run_claimed_batch",
                    side_effect=control_side_effect,
                ),
                patch(
                    "people_counter.local_cli.create_local_spark_session",
                    return_value=spark,
                ) as create_session,
                patch(
                    "people_counter.local_cli.runtime_identity",
                    return_value={"spark": "4.1.1"},
                ) as identity,
                patch(
                    "people_counter.local_cli.process_claimed_batch",
                    return_value=staging,
                ) as process_batch,
                contextlib.redirect_stdout(output),
            ):
                exit_code = _submit(args)

        self.assertEqual(exit_code, 0)
        create_session.assert_called_once_with(
            master="spark://master:7077",
            app_name="people-counter-local-correlation-a",
            correlation_id="correlation-a",
        )
        logger.info.assert_called_once_with(
            "local_runtime_identity",
            extra={
                "event": "local_runtime_identity",
                "correlation_id": "correlation-a",
                "runtime_identity": {"spark": "4.1.1"},
            },
        )
        identity.assert_called_once_with(spark)
        process_batch.assert_called_once_with(
            spark,
            manifest_path,
            args.staging_root,
            expected_manifest_sha256="d" * 64,
            processor_mode="probe",
            minimum_executor_identities=2,
        )
        spark.stop.assert_called_once_with()
        result = json.loads(output.getvalue())
        self.assertEqual(result["executor_identities"], ["1@worker-1", "2@worker-2"])
        self.assertEqual(result["release_digests"], ["release-a"])

    def test_submit_defaults_to_one_worker_and_rejects_impossible_canary(self):
        parser = build_parser()
        args = parser.parse_args(["submit"])
        self.assertEqual(args.minimum_workers, 1)

        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            main(["submit", "--max-items", "1", "--minimum-workers", "2"])

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            impossible = Namespace(
                database=root / "queue.sqlite3",
                content_root=root / "content",
                master="spark://master:7077",
                staging_root=root / "staging",
                mode="probe",
                minimum_workers=2,
                max_items=1,
                lock_seconds=120,
                heartbeat_seconds=30,
            )
            with self.assertRaisesRegex(
                ValueError,
                "minimum_workers cannot exceed max_items",
            ):
                _submit(impossible)


if __name__ == "__main__":
    unittest.main()
