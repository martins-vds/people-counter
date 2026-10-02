import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from uuid import UUID

from people_counter.local_control import _verify_staging_fence, run_claimed_batch
from people_counter.local_queue import LeaseLostError, SQLiteQueue, SerializedQueueActor
from people_counter.local_spark import BatchStaging, StagingValidationError
from people_counter.local_storage import ContentAddressedStore


class LocalControlTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.database = root / "queue.sqlite3"
        self.store = ContentAddressedStore(root / "content")

    def tearDown(self):
        self.temporary.cleanup()

    def test_successful_batch_materializes_manifest_and_completes_claim(self):
        queue = SQLiteQueue(self.database)
        message_id = queue.enqueue(
            "work",
            {"source_video": "/data/samples/video.mp4"},
        )
        observed_batch_id = [None]
        observed_attempt_id = [None]

        def runner(path, digest, correlation_id):
            self.assertTrue(path.is_file())
            self.assertEqual(self.store.read_json(digest)["correlation_id"], correlation_id)
            manifest = self.store.read_json(digest)
            self.assertEqual(manifest["schema_version"], 1)
            self.assertEqual(
                manifest["items"][0]["source_video"],
                "/data/samples/video.mp4",
            )
            observed_batch_id[0] = manifest["batch_id"]
            observed_attempt_id[0] = manifest["batch_attempt_id"]
            return BatchStaging(
                batch_id=manifest["batch_id"],
                batch_attempt_id=manifest["batch_attempt_id"],
                manifest_sha256=digest,
                record_count=1,
                executor_identities=("1@worker-1",),
                release_digests=("release-a",),
                failed_work_ids=(),
                path=Path(manifest["expected_staging_path"]),
            )

        with SerializedQueueActor(self.database) as actor:
            result = run_claimed_batch(
                actor,
                self.store,
                runner,
                owner="driver",
                max_items=1,
                minimum_items=1,
                lock_seconds=10,
                heartbeat_seconds=1,
                staging_root=Path(self.temporary.name) / "staging",
            )

        self.assertEqual(result.claimed_count, 1)
        self.assertEqual(observed_batch_id[0], result.batch_id)
        self.assertEqual(UUID(result.batch_id).version, 4)
        self.assertEqual(UUID(result.correlation_id).version, 4)
        self.assertEqual(UUID(observed_attempt_id[0]).version, 4)
        self.assertEqual(SQLiteQueue(self.database).get(message_id).state, "completed")

    def test_empty_queue_does_not_materialize_an_invalid_manifest(self):
        with SerializedQueueActor(self.database) as actor:
            result = run_claimed_batch(
                actor,
                self.store,
                lambda *_: self.fail("runner must not be called"),
                owner="driver",
                max_items=1,
                minimum_items=1,
                lock_seconds=10,
                heartbeat_seconds=1,
                staging_root=Path(self.temporary.name) / "staging",
            )

        self.assertEqual(result.claimed_count, 0)
        self.assertEqual(UUID(result.batch_id).version, 4)
        self.assertEqual(UUID(result.correlation_id).version, 4)
        self.assertIsNone(result.manifest)
        self.assertIsNone(result.staging)

    def test_failed_staging_abandons_for_retry(self):
        queue = SQLiteQueue(self.database)
        message_id = queue.enqueue(
            "work",
            {"source_video": "/data/samples/video.mp4"},
        )

        def runner(path, digest, correlation_id):
            manifest = self.store.read_json(digest)
            return BatchStaging(
                batch_id=manifest["batch_id"],
                batch_attempt_id=manifest["batch_attempt_id"],
                manifest_sha256=digest,
                record_count=1,
                executor_identities=("1@worker-1",),
                release_digests=("release-a",),
                failed_work_ids=(message_id,),
                path=Path(manifest["expected_staging_path"]),
            )

        with SerializedQueueActor(self.database) as actor:
            result = run_claimed_batch(
                actor,
                self.store,
                runner,
                owner="driver",
                max_items=1,
                minimum_items=1,
                lock_seconds=10,
                heartbeat_seconds=1,
                staging_root=Path(self.temporary.name) / "staging",
            )

        self.assertEqual(result.staging.failed_work_ids, (message_id,))
        message = SQLiteQueue(self.database).get(message_id)
        self.assertEqual(message.state, "ready")
        self.assertEqual(
            message.dead_letter_reason,
            "Spark staging contains a failed terminal record",
        )

    def test_runner_exception_abandons_live_claim(self):
        queue = SQLiteQueue(self.database)
        message_id = queue.enqueue(
            "work",
            {"source_video": "/data/samples/video.mp4"},
        )

        with SerializedQueueActor(self.database) as actor:
            with self.assertRaisesRegex(RuntimeError, "spark failed"):
                run_claimed_batch(
                    actor,
                    self.store,
                    lambda *_: (_ for _ in ()).throw(RuntimeError("spark failed")),
                    owner="driver",
                    max_items=1,
                    minimum_items=1,
                    lock_seconds=10,
                    heartbeat_seconds=1,
                    staging_root=Path(self.temporary.name) / "staging",
                )

        self.assertEqual(SQLiteQueue(self.database).get(message_id).state, "ready")

    def test_minimum_items_does_not_claim_partial_canary(self):
        queue = SQLiteQueue(self.database)
        message_id = queue.enqueue(
            "only",
            {"source_video": "/data/samples/video.mp4"},
        )

        with SerializedQueueActor(self.database) as actor:
            result = run_claimed_batch(
                actor,
                self.store,
                lambda *_: self.fail("runner must not be called"),
                owner="driver",
                max_items=2,
                minimum_items=2,
                lock_seconds=10,
                heartbeat_seconds=1,
                staging_root=Path(self.temporary.name) / "staging",
            )

        self.assertEqual(result.claimed_count, 0)
        message = SQLiteQueue(self.database).get(message_id)
        self.assertEqual(message.state, "ready")
        self.assertEqual(message.delivery_count, 0)

    def test_heartbeat_renews_while_runner_blocks(self):
        queue = SQLiteQueue(self.database)
        queue.enqueue("heartbeat", {"source_video": "/data/video.mp4"})
        deadlines = []

        def runner(path, digest, correlation_id):
            with sqlite3.connect(self.database) as connection:
                deadlines.append(
                    connection.execute(
                        "SELECT locked_until FROM queue_messages"
                    ).fetchone()[0]
                )
            time.sleep(0.2)
            with sqlite3.connect(self.database) as connection:
                deadlines.append(
                    connection.execute(
                        "SELECT locked_until FROM queue_messages"
                    ).fetchone()[0]
                )
            value = self.store.read_json(digest)
            return BatchStaging(
                batch_id=value["batch_id"],
                batch_attempt_id=value["batch_attempt_id"],
                manifest_sha256=digest,
                path=Path(value["expected_staging_path"]),
                record_count=1,
                executor_identities=("1@worker-1",),
                release_digests=("release-a",),
                failed_work_ids=(),
            )

        with SerializedQueueActor(self.database) as actor:
            run_claimed_batch(
                actor,
                self.store,
                runner,
                owner="driver",
                max_items=1,
                minimum_items=1,
                lock_seconds=1,
                heartbeat_seconds=0.05,
                staging_root=Path(self.temporary.name) / "staging",
            )

        self.assertGreater(deadlines[1], deadlines[0])

    def test_lost_fence_prevents_finalization(self):
        queue = SQLiteQueue(self.database)
        message_id = queue.enqueue(
            "lost",
            {"source_video": "/data/video.mp4"},
            max_delivery_count=3,
        )

        def runner(path, digest, correlation_id):
            SQLiteQueue(self.database).recover_expired(now=time.time() + 10)
            value = self.store.read_json(digest)
            return BatchStaging(
                batch_id=value["batch_id"],
                batch_attempt_id=value["batch_attempt_id"],
                manifest_sha256=digest,
                path=Path(value["expected_staging_path"]),
                record_count=1,
                executor_identities=("1@worker-1",),
                release_digests=("release-a",),
                failed_work_ids=(),
            )

        with SerializedQueueActor(self.database) as actor:
            with self.assertRaises(LeaseLostError):
                run_claimed_batch(
                    actor,
                    self.store,
                    runner,
                    owner="driver",
                    max_items=1,
                    minimum_items=1,
                    lock_seconds=1,
                    heartbeat_seconds=0.05,
                    staging_root=Path(self.temporary.name) / "staging",
                )

        self.assertEqual(SQLiteQueue(self.database).get(message_id).state, "ready")

    def test_foreign_staging_fence_prevents_completion(self):
        queue = SQLiteQueue(self.database)
        message_id = queue.enqueue(
            "foreign",
            {"source_video": "/data/video.mp4"},
        )

        def runner(path, digest, correlation_id):
            value = self.store.read_json(digest)
            return BatchStaging(
                batch_id="foreign-batch",
                batch_attempt_id=value["batch_attempt_id"],
                manifest_sha256=digest,
                path=Path(value["expected_staging_path"]),
                record_count=1,
                executor_identities=("1@worker-1",),
                release_digests=("release-a",),
                failed_work_ids=(),
            )

        with SerializedQueueActor(self.database) as actor:
            with self.assertRaisesRegex(
                StagingValidationError,
                "staging fence mismatch",
            ):
                run_claimed_batch(
                    actor,
                    self.store,
                    runner,
                    owner="driver",
                    max_items=1,
                    minimum_items=1,
                    lock_seconds=10,
                    heartbeat_seconds=1,
                    staging_root=Path(self.temporary.name) / "staging",
                )

        self.assertEqual(SQLiteQueue(self.database).get(message_id).state, "ready")

    def test_every_staging_fence_identity_is_compared(self):
        expected_path = Path(self.temporary.name) / "staging" / "batch" / "attempt"
        valid = {
            "batch_id": "batch",
            "batch_attempt_id": "attempt",
            "manifest_sha256": "a" * 64,
            "path": expected_path,
            "record_count": 1,
            "executor_identities": ("1@worker-1",),
            "release_digests": ("release-a",),
            "failed_work_ids": (),
        }
        cases = {
            "batch_id": {"batch_id": "foreign"},
            "batch_attempt_id": {"batch_attempt_id": "foreign"},
            "manifest_sha256": {"manifest_sha256": "b" * 64},
            "path": {"path": Path("/foreign")},
        }
        for name, updates in cases.items():
            with self.subTest(name=name):
                staging = BatchStaging(**{**valid, **updates})
                with self.assertRaisesRegex(StagingValidationError, name):
                    _verify_staging_fence(
                        staging,
                        batch_id="batch",
                        batch_attempt_id="attempt",
                        manifest_sha256="a" * 64,
                        expected_path=expected_path,
                    )


if __name__ == "__main__":
    unittest.main()
