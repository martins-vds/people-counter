import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from people_counter.local_queue import (
    ActorClosedError,
    ActorDiedError,
    LeaseLostError,
    QueueConflictError,
    SQLiteQueue,
    SerializedQueueActor,
)


class SQLiteQueueTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "queue.sqlite3"
        self.queue = SQLiteQueue(self.database)

    def tearDown(self):
        self.temporary.cleanup()

    def test_enqueue_is_idempotent_only_for_identical_content(self):
        first = self.queue.enqueue("camera:a", {"video": "a.mp4"})
        second = self.queue.enqueue("camera:a", {"video": "a.mp4"})

        self.assertEqual(first, second)
        with self.assertRaises(QueueConflictError):
            self.queue.enqueue("camera:a", {"video": "different.mp4"})
        with self.assertRaises(QueueConflictError):
            self.queue.enqueue(
                "camera:a",
                {"video": "a.mp4"},
                max_delivery_count=6,
            )

    def test_claim_renew_complete_and_fencing(self):
        message_id = self.queue.enqueue("work", {"value": 1}, available_at=0)
        claim = self.queue.claim(
            1,
            owner="driver-a",
            lock_seconds=10,
            now=100,
        )[0]

        self.assertEqual(claim.message_id, message_id)
        self.assertEqual(claim.idempotency_key, "work")
        self.assertEqual(claim.payload, {"value": 1})
        self.assertEqual(claim.delivery_count, 1)
        self.assertEqual(claim.max_delivery_count, 5)
        self.assertEqual(
            self.queue.renew_many(
                [claim.lock_token],
                lock_seconds=10,
                now=105,
            ),
            115,
        )
        self.queue.complete(claim.lock_token, now=106)
        self.assertEqual(self.queue.get(message_id).state, "completed")
        with self.assertRaises(LeaseLostError):
            self.queue.complete(claim.lock_token, now=107)

    def test_claim_enforces_limit_boundaries(self):
        for limit in (0, 101):
            with self.subTest(limit=limit):
                with self.assertRaisesRegex(ValueError, "between 1 and 100"):
                    self.queue.claim(
                        limit,
                        owner="driver",
                        lock_seconds=10,
                        now=100,
                    )
        self.assertEqual(
            self.queue.claim(
                100,
                owner="driver",
                lock_seconds=10,
                now=100,
            ),
            [],
        )
        with self.assertRaisesRegex(ValueError, "owner must be a non-empty string"):
            self.queue.claim(1, owner="", lock_seconds=10, now=100)
        with self.assertRaisesRegex(ValueError, "between 1 and limit"):
            self.queue.claim(
                1,
                owner="driver",
                lock_seconds=10,
                minimum_count=2,
                now=100,
            )

    def test_claim_minimum_is_atomic_and_does_not_consume_delivery(self):
        message_id = self.queue.enqueue("only", {}, available_at=0)

        self.assertEqual(
            self.queue.claim(
                2,
                owner="driver",
                lock_seconds=10,
                minimum_count=2,
                now=100,
            ),
            [],
        )
        message = self.queue.get(message_id)
        self.assertEqual(message.state, "ready")
        self.assertEqual(message.delivery_count, 0)

    def test_claim_records_owner_and_exact_lock_deadline(self):
        self.queue.enqueue("owned", {}, available_at=0)

        claim = self.queue.claim(
            1,
            owner="driver-a",
            lock_seconds=10,
            now=100,
        )[0]
        with sqlite3.connect(self.database) as connection:
            row = connection.execute(
                "SELECT locked_by, locked_until FROM queue_messages"
            ).fetchone()

        self.assertEqual(row, ("driver-a", 110))
        self.assertEqual(claim.locked_until, 110)

    def test_abandon_retries_then_dead_letters_at_delivery_limit(self):
        message_id = self.queue.enqueue(
            "work",
            {"value": 1},
            max_delivery_count=2,
            available_at=0,
        )
        first = self.queue.claim(
            1,
            owner="driver",
            lock_seconds=10,
            now=100,
        )[0]
        self.queue.abandon(
            first.lock_token,
            delay_seconds=5,
            reason="transient",
            now=101,
        )
        self.assertEqual(self.queue.claim(1, owner="driver", lock_seconds=10, now=105), [])
        second = self.queue.claim(
            1,
            owner="driver",
            lock_seconds=10,
            now=106,
        )[0]
        self.queue.abandon(second.lock_token, reason="again", now=107)

        message = self.queue.get(message_id)
        self.assertEqual(message.state, "dead")
        self.assertEqual(message.delivery_count, 2)
        self.assertEqual(message.dead_letter_reason, "again")

    def test_expired_lease_recovers_or_dead_letters(self):
        retry_id = self.queue.enqueue(
            "retry",
            {},
            max_delivery_count=2,
            available_at=0,
        )
        dead_id = self.queue.enqueue(
            "dead",
            {},
            max_delivery_count=1,
            available_at=0,
        )
        claims = self.queue.claim(
            2,
            owner="driver",
            lock_seconds=10,
            now=100,
        )

        self.assertEqual(self.queue.recover_expired(now=110), 2)
        self.assertEqual(self.queue.get(retry_id).state, "ready")
        self.assertEqual(self.queue.get(dead_id).state, "dead")
        for claim in claims:
            with self.assertRaises(LeaseLostError):
                self.queue.renew(claim.lock_token, lock_seconds=10, now=111)

    def test_claim_automatically_recovers_expired_work_and_rejects_old_token(self):
        message_id = self.queue.enqueue(
            "retry",
            {},
            max_delivery_count=3,
            available_at=0,
        )
        first = self.queue.claim(
            1,
            owner="driver-a",
            lock_seconds=10,
            now=100,
        )[0]

        second = self.queue.claim(
            1,
            owner="driver-b",
            lock_seconds=10,
            now=110,
        )[0]

        self.assertEqual(second.message_id, message_id)
        self.assertEqual(second.delivery_count, 2)
        self.assertNotEqual(second.lock_token, first.lock_token)
        with self.assertRaises(LeaseLostError):
            self.queue.complete(first.lock_token, now=111)

    def test_renew_many_is_atomic_when_one_fence_is_lost(self):
        self.queue.enqueue("first-renew", {}, available_at=0)
        self.queue.enqueue("second-renew", {}, available_at=0)
        claims = self.queue.claim(
            2,
            owner="driver",
            lock_seconds=20,
            now=100,
        )
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "UPDATE queue_messages SET locked_until = 104 WHERE lock_token = ?",
                (claims[1].lock_token,),
            )

        with self.assertRaises(LeaseLostError):
            self.queue.renew_many(
                [claim.lock_token for claim in claims],
                lock_seconds=30,
                now=105,
            )

        self.assertEqual(self.queue.get(claims[0].message_id).locked_until, 120)

    def test_finalize_is_atomic_when_any_fence_is_invalid(self):
        first_id = self.queue.enqueue("first", {}, available_at=0)
        self.queue.enqueue("second", {}, available_at=0)
        claims = self.queue.claim(
            2,
            owner="driver",
            lock_seconds=10,
            now=100,
        )

        with self.assertRaises(LeaseLostError):
            self.queue.finalize(
                [
                    (claims[0].lock_token, "complete", None),
                    ("missing-token", "complete", None),
                ],
                now=101,
            )

        self.assertEqual(self.queue.get(first_id).state, "locked")

    def test_database_uses_wal_and_full_synchronous_mode(self):
        with sqlite3.connect(self.database) as connection:
            journal = connection.execute("PRAGMA journal_mode").fetchone()[0]
            synchronous = connection.execute("PRAGMA synchronous").fetchone()[0]

        self.assertEqual(journal.lower(), "wal")
        self.assertEqual(synchronous, 2)

    def test_serialized_actor_runs_operations_on_its_control_thread(self):
        with SerializedQueueActor(self.database) as actor:
            message_id = actor.call(lambda queue: queue.enqueue("actor", {"ok": True}))
            state = actor.call(lambda queue: queue.get(message_id).state)

        self.assertEqual(state, "ready")
        with self.assertRaises(ActorClosedError):
            actor.call(lambda queue: queue.list())

    def test_serialized_actor_propagates_startup_and_operation_failures(self):
        with self.assertRaisesRegex(ValueError, "busy_timeout_ms"):
            SerializedQueueActor(self.database, busy_timeout_ms=0)

        with patch(
            "people_counter.local_queue.SQLiteQueue",
            side_effect=sqlite3.OperationalError("cannot initialize"),
        ):
            with self.assertRaisesRegex(sqlite3.OperationalError, "cannot initialize"):
                SerializedQueueActor(self.database)

        with SerializedQueueActor(self.database) as actor:
            with self.assertRaisesRegex(ValueError, "operation failed"):
                actor.call(
                    lambda queue: (_ for _ in ()).throw(
                        ValueError("operation failed")
                    )
                )
            self.assertEqual(actor.call(lambda queue: queue.list()), [])

    def test_serialized_actor_propagates_unexpected_death(self):
        actor = SerializedQueueActor(self.database)
        with self.assertRaises(ActorDiedError):
            actor.call(
                lambda queue: (_ for _ in ()).throw(
                    KeyboardInterrupt("fatal operation")
                )
            )
        with self.assertRaises(ActorDiedError):
            actor.call(lambda queue: queue.list())
        actor.close()

    def test_serialized_actor_close_fails_pending_call_without_hanging(self):
        actor = SerializedQueueActor(
            self.database,
            call_timeout_seconds=2,
            close_timeout_seconds=2,
        )
        entered = threading.Event()
        release = threading.Event()
        outcomes = {}

        def blocking(queue):
            entered.set()
            release.wait(timeout=1)
            return "finished"

        def invoke(name, operation):
            try:
                outcomes[name] = actor.call(operation)
            except BaseException as error:
                outcomes[name] = error

        running = threading.Thread(target=invoke, args=("running", blocking))
        running.start()
        self.assertTrue(entered.wait(timeout=1))
        pending = threading.Thread(
            target=invoke,
            args=("pending", lambda queue: "must not run"),
        )
        pending.start()
        time.sleep(0.05)
        closing = threading.Thread(target=actor.close)
        closing.start()
        time.sleep(0.05)
        release.set()
        for thread in (running, pending, closing):
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())

        self.assertEqual(outcomes["running"], "finished")
        self.assertIsInstance(outcomes["pending"], ActorClosedError)

    def test_serialized_actor_call_timeout_is_bounded(self):
        release = threading.Event()
        actor = SerializedQueueActor(
            self.database,
            call_timeout_seconds=0.05,
            close_timeout_seconds=1,
        )

        with self.assertRaisesRegex(
            TimeoutError,
            "operation timed out",
        ):
            actor.call(lambda queue: release.wait(timeout=0.5))

        release.set()
        actor.close()

    def test_serialized_actor_continues_after_queued_call_times_out(self):
        entered = threading.Event()
        release = threading.Event()
        actor = SerializedQueueActor(
            self.database,
            call_timeout_seconds=1,
            close_timeout_seconds=1,
        )
        first_outcome = []

        def run_first():
            first_outcome.append(
                actor.call(
                    lambda queue: (
                        entered.set(),
                        release.wait(timeout=0.5),
                    )
                )
            )

        first = threading.Thread(target=run_first)
        first.start()
        self.assertTrue(entered.wait(timeout=1))
        actor._call_timeout_seconds = 0.05
        try:
            with self.assertRaises(TimeoutError):
                actor.call(lambda queue: "cancelled before execution")
        finally:
            actor._call_timeout_seconds = 1
        release.set()
        first.join(timeout=1)
        self.assertFalse(first.is_alive())
        self.assertTrue(first_outcome)
        self.assertEqual(actor.call(lambda queue: "still running"), "still running")
        actor.close()

    def test_serialized_actor_startup_and_close_timeouts_are_bounded(self):
        original_queue = SQLiteQueue
        startup_release = threading.Event()

        def delayed_startup(*args, **kwargs):
            startup_release.wait(timeout=0.5)
            return original_queue(*args, **kwargs)

        with patch(
            "people_counter.local_queue.SQLiteQueue",
            side_effect=delayed_startup,
        ):
            with self.assertRaisesRegex(TimeoutError, "startup timed out"):
                SerializedQueueActor(
                    self.database,
                    startup_timeout_seconds=0.05,
                    close_timeout_seconds=0.05,
                )
            startup_release.set()

        operation_release = threading.Event()
        actor = SerializedQueueActor(
            self.database,
            call_timeout_seconds=1,
            close_timeout_seconds=0.05,
        )
        entered = threading.Event()
        caller = threading.Thread(
            target=lambda: actor.call(
                lambda queue: (
                    entered.set(),
                    operation_release.wait(timeout=0.5),
                )
            )
        )
        caller.start()
        self.assertTrue(entered.wait(timeout=1))
        with self.assertRaisesRegex(TimeoutError, "shutdown timed out"):
            actor.close()
        operation_release.set()
        caller.join(timeout=1)
        self.assertFalse(caller.is_alive())
        actor.close()


if __name__ == "__main__":
    unittest.main()
