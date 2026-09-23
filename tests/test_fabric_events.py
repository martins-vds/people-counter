import copy
import json
import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from itertools import count
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, call, patch

from people_counter import fabric_events as events


NOW = datetime(2026, 9, 23, 12, 0, tzinfo=timezone.utc)
DAY = date(2026, 9, 23)


def bootstrapped_spark(spark):
    schemas = {}
    for suffix, columns in events._REQUIRED_SCHEMAS.items():
        fields = []
        for name, data_type in columns.items():
            dtype = MagicMock()
            dtype.simpleString.return_value = data_type
            fields.append(SimpleNamespace(name=name, dataType=dtype))
        schema = MagicMock()
        schema.fields = fields
        schema.__getitem__.side_effect = {field.name: field for field in fields}.__getitem__
        schemas[suffix] = schema

    def table(name):
        suffix = next(suffix for suffix in schemas if name.endswith("_" + suffix))
        frame = spark.table.return_value
        frame.schema = schemas[suffix]
        return frame

    spark.catalog.tableExists.return_value = True
    spark.catalog.getTable.return_value = SimpleNamespace(isTemporary=False, tableType="MANAGED")
    spark.table.side_effect = table
    return schemas


def state(status="LEASED", worker="execution"):
    work = {
        "work_id": "work", "capture_date": DAY, "config_sha256": "config",
        "status": status, "lease_owner_attempt_id": "attempt",
        "lease_dispatcher_id": "dispatcher", "lease_acquired_at": NOW - timedelta(minutes=5),
        "lease_expires_at": NOW + timedelta(minutes=20),
        "last_heartbeat_at": NOW - timedelta(minutes=1), "committed_attempt_id": None,
        "attempt_count": 1, "max_attempts": 3, "not_before_at": None,
        "completed_at": None, "queue_entered_at": NOW - timedelta(minutes=10),
        "last_error_category": None, "last_error_type": None, "last_error_message": None,
    }
    attempt = {
        **dict.fromkeys(events._UPDATE_FIELDS),
        "attempt_id": "attempt", "work_id": "work", "capture_date": DAY,
        "dispatcher_id": "dispatcher", "config_sha256": "config",
        "worker_execution_id": worker, "status": status,
        "last_heartbeat_at": work["last_heartbeat_at"],
    }
    return work, attempt


def failure(**changes):
    return {
        "error_category": "RUNTIME", "error_type": "RuntimeError", "error_message": "failed",
        "retryable": True, "lease_lost": False, "processed_frames": 7,
        "processing_seconds": 2.5, **changes,
    }


def event(kind="claim_execution", payload=None, sequence=1, **changes):
    return {
        "event_id": f"event-{sequence}", "work_id": "work", "attempt_id": "attempt",
        "worker_execution_id": "execution", "capture_date": DAY, "sequence": sequence,
        "event_kind": kind, "created_at": NOW,
        "payload_json": json.dumps(
            {"payload": payload if payload is not None else {}, "lease_minutes": 30},
            default=events._json_default,
        ),
        **changes,
    }


def plan(row, work, attempt, now=NOW):
    return events._plan(events._Command.from_row(row), work, attempt, now)


class Predicate:
    def __init__(self, evaluate):
        self.evaluate = evaluate

    def __and__(self, other):
        return Predicate(lambda row: self.evaluate(row) and other.evaluate(row))

    def __or__(self, other):
        return Predicate(lambda row: self.evaluate(row) or other.evaluate(row))

    def __eq__(self, value):
        return Predicate(lambda row: self.evaluate(row) == value)

    def __ne__(self, value):
        return Predicate(lambda row: self.evaluate(row) != value)

    def __gt__(self, other):
        return Predicate(
            lambda row: self.evaluate(row) is not None
            and other.evaluate(row) is not None
            and self.evaluate(row) > other.evaluate(row)
        )

    def eqNullSafe(self, other):
        return Predicate(lambda row: self.evaluate(row) == other.evaluate(row))

    def cast(self, data_type):
        return self


class PredicateFunctions:
    @staticmethod
    def col(name):
        return Predicate(lambda row: row[name])

    @staticmethod
    def lit(value):
        return Predicate(lambda row: value)

    @staticmethod
    def current_timestamp():
        return PredicateFunctions.lit(NOW)

    @staticmethod
    def max(name):
        return SimpleNamespace(alias=lambda alias: (name, alias))


class QueryRow(dict):
    def asDict(self, recursive=False):
        return dict(self)


class QueryRows:
    def __init__(self, rows):
        self.rows = rows

    def where(self, condition):
        return QueryRows([row for row in self.rows if condition.evaluate(row)])

    def limit(self, size):
        if type(size) is not int:
            raise TypeError("Spark limit requires an integer")
        return QueryRows(self.rows[:size])

    def collect(self):
        return [QueryRow(row) for row in self.rows]

    def agg(self, aggregate):
        name, alias = aggregate
        values = [row[name] for row in self.rows if row[name] is not None]
        return QueryRows([{alias: max(values) if values else None}])


class Writer:
    def __init__(self):
        self.active = False
        self.retained = False
        self.calls = 0

    def run(self, callback):
        if self.active or self.retained:
            raise RuntimeError("Writer is already locked")
        self.calls += 1
        self.active = True
        try:
            result = callback()
        except Exception:
            self.retained = True
            raise
        finally:
            self.active = False
        return result


class MemoryStore:
    """Storage fault injector; production decisions still use the real planner."""

    def __init__(self, writer, rows, work=None, attempt=None):
        self.writer = writer
        self.rows = rows
        default_work, default_attempt = state()
        self.work = copy.deepcopy(work if work is not None else default_work)
        self.attempt = copy.deepcopy(attempt if attempt is not None else default_attempt)
        self.receipts = {}
        self.writes = []
        self.receipt_batches = []
        self.fail_after = None

    def assert_locked(self):
        if not self.writer.active:
            raise AssertionError("Metadata access outside writer")

    def pending(self, limit):
        self.assert_locked()
        return sorted(
            [row for row in self.rows if row["event_id"] not in self.receipts],
            key=lambda row: (
                row["created_at"], row["worker_execution_id"], row["sequence"], row["event_id"],
            ),
        )[:limit]

    def unique(self, table, key, value):
        self.assert_locked()
        row = self.work if table == "video_work" else self.attempt
        events._require(row[key] == value, "Row not found")
        return copy.deepcopy(row)

    def receipt(self, event_id):
        self.assert_locked()
        return self.receipts.get(event_id)

    def highest_sequence(self, row):
        self.assert_locked()
        return max((
            receipt["sequence"] for receipt in self.receipts.values()
            if all(receipt[key] == row[key] for key in ("work_id", "attempt_id", "worker_execution_id"))
        ), default=0)

    def update(self, table, before, updates):
        self.assert_locked()
        if not updates:
            return
        target = self.work if table == "video_work" else self.attempt
        if target != before:
            raise AssertionError("Mutation used a stale snapshot")
        target.update(updates)
        self.writes.append((table, copy.deepcopy(updates)))
        if self.fail_after == table:
            self.fail_after = None
            raise OSError("Ambiguous Delta commit")

    def append_receipts(self, receipts):
        self.assert_locked()
        self.receipt_batches.append(copy.deepcopy(receipts))
        for receipt in receipts:
            if receipt["event_id"] in self.receipts:
                raise AssertionError("Duplicate receipt")
            self.receipts[receipt["event_id"]] = receipt
        if self.fail_after == "receipts":
            self.fail_after = None
            raise OSError("Ambiguous receipt append")


class EventBusinessTests(unittest.TestCase):
    def assertRejected(self, row, work, attempt, message, now=NOW):
        with self.assertRaisesRegex(events._Rejected, message):
            plan(row, work, attempt, now)

    def test_claim_checks_live_lease_and_worker(self):
        work, attempt = state(worker=None)
        self.assertEqual(plan(event(), work, attempt).attempt, {"worker_execution_id": "execution"})
        attempt["worker_execution_id"] = "execution"
        self.assertEqual(plan(event(), work, attempt).attempt, {"worker_execution_id": "execution"})
        attempt["worker_execution_id"] = "other"
        self.assertRejected(event(), work, attempt, "Another execution")

    def test_identity_and_configuration_fences(self):
        for table, key, value in (
            ("work", "work_id", "other"), ("attempt", "work_id", "other"),
            ("attempt", "attempt_id", "other"), ("work", "capture_date", date(2026, 9, 22)),
            ("attempt", "capture_date", date(2026, 9, 22)),
            ("attempt", "config_sha256", "other"), ("work", "config_sha256", ""),
            ("work", "lease_dispatcher_id", "other"),
        ):
            with self.subTest(table=table, key=key):
                work, attempt = state()
                (work if table == "work" else attempt)[key] = value
                self.assertRejected(event(), work, attempt, "match")

    def test_expired_missing_old_and_future_leases_reject_claim(self):
        for expires in (None, NOW, NOW - timedelta(seconds=1)):
            work, attempt = state()
            work["lease_expires_at"] = expires
            self.assertRejected(event(), work, attempt, "expired")
        work, attempt = state()
        self.assertRejected(event(created_at=NOW - timedelta(minutes=6)), work, attempt, "predates")
        self.assertRejected(event(created_at=NOW + timedelta(seconds=1)), work, attempt, "future")
        work["lease_acquired_at"] = None
        self.assertRejected(event(), work, attempt, "predates")

    def test_claim_never_reclaims_later_or_terminal_state(self):
        for status in ("STAGING", "RUNNING", "WRITING", "SUCCEEDED", "RECOVERING", "DEAD_LETTERED"):
            work, attempt = state(status)
            self.assertRejected(event(), work, attempt, "requires LEASED|not active")

    def test_heartbeat_all_state_pairs_are_fenced(self):
        for old in events._ACTIVE:
            for new in events._ACTIVE:
                with self.subTest(old=old, new=new):
                    work, attempt = state(old)
                    row = event("heartbeat", {"status": new, "updates": {"processed_frames": 12}})
                    if events._ACTIVE.index(new) - events._ACTIVE.index(old) in (0, 1):
                        result = plan(row, work, attempt)
                        self.assertEqual(result.work["status"], new)
                        self.assertEqual(result.attempt, {
                            "processed_frames": 12, "status": new, "last_heartbeat_at": NOW,
                        })
                        self.assertEqual(result.work["lease_expires_at"], NOW + timedelta(minutes=30))
                    else:
                        self.assertRejected(row, work, attempt, "Invalid transition")

    def test_heartbeat_timestamp_is_event_time_not_payload_or_replay_time(self):
        work, attempt = state("STAGING")
        row = event("heartbeat", {
            "status": "RUNNING", "updates": {
                "last_heartbeat_at": NOW + timedelta(days=1),
                "inference_started_at": NOW.isoformat(),
            },
        })
        first = plan(row, work, attempt, NOW + timedelta(seconds=2))
        self.assertEqual(first.attempt["last_heartbeat_at"], NOW)
        self.assertEqual(first.attempt["inference_started_at"], NOW)
        self.assertEqual(first.work["lease_expires_at"], NOW + timedelta(minutes=30))
        attempt.update(first.attempt)
        replay = plan(row, work, attempt, NOW + timedelta(seconds=4))
        self.assertEqual(replay.work, first.work)
        work.update(replay.work)
        completed = plan(row, work, attempt, NOW + timedelta(hours=1))
        self.assertEqual((completed.work, completed.attempt), ({}, {}))

    def test_heartbeat_does_not_shorten_lease(self):
        work, attempt = state()
        work["lease_expires_at"] = NOW + timedelta(minutes=50)
        result = plan(event("heartbeat", {"status": "LEASED", "updates": {}}), work, attempt)
        self.assertEqual(result.work["lease_expires_at"], work["lease_expires_at"])

    def test_partial_heartbeat_cannot_resurrect_expired_work(self):
        work, attempt = state()
        row = event("heartbeat", {"status": "STAGING", "updates": {}})
        attempt.update(plan(row, work, attempt).attempt)
        self.assertRejected(row, work, attempt, "expired", NOW + timedelta(minutes=20))

    def test_heartbeat_rejects_old_timestamp_or_finished_attempt(self):
        work, attempt = state()
        work["last_heartbeat_at"] = NOW + timedelta(seconds=1)
        row = event("heartbeat", {"status": "STAGING", "updates": {}})
        self.assertRejected(row, work, attempt, "older")
        work["last_heartbeat_at"] = NOW
        attempt["status"] = "SUCCEEDED"
        self.assertRejected(row, work, attempt, "not active")

    def test_attempt_update_only_changes_provided_fields(self):
        work, attempt = state("RUNNING")
        attempt["pipeline_run_id"] = "preserve"
        result = plan(event("attempt_update", {"updates": {"processed_frames": 15}}), work, attempt)
        self.assertEqual(result.work, {})
        self.assertEqual(result.attempt, {"processed_frames": 15})
        self.assertEqual(attempt["pipeline_run_id"], "preserve")

    def test_expired_attempt_metadata_cannot_be_changed(self):
        work, attempt = state("RUNNING")
        work["lease_expires_at"] = NOW
        self.assertRejected(
            event("attempt_update", {"updates": {"processed_frames": 15}}),
            work, attempt, "expired",
        )

    def test_attempt_update_cannot_advance_or_regress_work_state(self):
        work, attempt = state("RUNNING")
        for target in ("LEASED", "WRITING", "SUCCEEDED", "TERMINAL_FAILED"):
            self.assertRejected(event("attempt_update", {"updates": {"status": target}}), work, attempt, "transitions")
        self.assertRejected(event("attempt_update", {"updates": {"completed_at": NOW}}), work, attempt, "completed_at")
        attempt["status"] = "STAGING"
        self.assertRejected(event("attempt_update", {"updates": {}}), work, attempt, "differs")

    def test_final_attempt_update_replays_but_other_terminal_updates_reject(self):
        work, attempt = state("WRITING")
        row = event("attempt_update", {"updates": {"status": "SUCCEEDED", "distinct_people": 4}})
        result = plan(row, work, attempt)
        self.assertEqual(result.attempt["completed_at"], NOW)
        attempt.update(result.attempt)
        self.assertEqual(plan(row, work, attempt).attempt, {})
        self.assertRejected(event("attempt_update", {"updates": {"distinct_people": 99}}), work, attempt, "Completed")
        other = event("attempt_update", {"updates": {"status": "SUCCEEDED", "distinct_people": 4}},
                      created_at=NOW + timedelta(seconds=1))
        self.assertRejected(other, work, attempt, "Completed", NOW + timedelta(seconds=1))

    def test_final_notebook_payload_accepts_nullable_metrics_and_error_fields(self):
        work, attempt = state("WRITING")
        updates = {
            "status": "SUCCEEDED", "completed_at": NOW.isoformat(),
            "source_duration_seconds": None, "source_fps": 25.0,
            "total_source_frames": 100, "processed_frames": 20,
            "effective_sample_fps": 5.0, "processing_seconds": 2.0,
            "distinct_people": 3, "line_in_count": 1, "line_out_count": 2,
            "retryable": None, "error_category": None, "error_type": None, "error_message": None,
        }
        result = plan(event("attempt_update", {"updates": updates}), work, attempt)
        self.assertEqual(result.attempt, {**updates, "completed_at": NOW})
        self.assertEqual(work["config_sha256"], attempt["config_sha256"])
        self.assertNotIn("config_sha256", result.attempt)

    def test_commit_requires_writing_success_and_live_lease(self):
        work, attempt = state("WRITING")
        self.assertRejected(event("commit"), work, attempt, "succeeded attempt")
        attempt["status"] = "SUCCEEDED"
        work["status"] = "RUNNING"
        self.assertRejected(event("commit"), work, attempt, "WRITING")
        work["status"] = "WRITING"
        work["lease_expires_at"] = NOW
        self.assertRejected(event("commit"), work, attempt, "expired")

    def test_commit_and_ambiguous_commit_replay(self):
        work, attempt = state("WRITING")
        attempt["status"] = "SUCCEEDED"
        result = plan(event("commit"), work, attempt)
        self.assertEqual(result.work["committed_attempt_id"], "attempt")
        self.assertEqual(result.work["status"], "SUCCEEDED")
        self.assertEqual(result.work["completed_at"], NOW)
        for key in events._LEASE_CLEAR:
            self.assertIsNone(result.work[key])
        work.update(result.work)
        replay = plan(event("commit"), work, attempt, NOW + timedelta(days=1))
        self.assertEqual((replay.work, replay.attempt), ({}, {}))
        attempt["worker_execution_id"] = "another"
        self.assertRejected(event("commit"), work, attempt, "Another execution")

    def test_commit_replay_requires_both_success_status_and_own_pointer(self):
        for status, pointer in (
            ("SUCCEEDED", None),
            ("SUCCEEDED", "another-attempt"),
            ("WRITING", "attempt"),
            ("RETRY_WAIT", "attempt"),
        ):
            with self.subTest(status=status, pointer=pointer):
                work, attempt = state(status)
                work["committed_attempt_id"] = pointer
                attempt["status"] = "SUCCEEDED"
                with self.assertRaises(events._Rejected):
                    plan(event("commit", {}), work, attempt)

    def test_failure_uses_current_retry_budget_and_retryability(self):
        for count, retryable, lease_lost, expected, expected_attempt in (
            (1, True, False, "RETRY_WAIT", "RETRY_WAIT"),
            (1, True, True, "RETRY_WAIT", "LEASE_LOST"),
            (1, False, False, "TERMINAL_FAILED", "TERMINAL_FAILED"),
            (3, True, False, "DEAD_LETTERED", "DEAD_LETTERED"),
            (4, False, True, "DEAD_LETTERED", "DEAD_LETTERED"),
        ):
            with self.subTest(count=count, retryable=retryable, lease_lost=lease_lost):
                work, attempt = state("RUNNING")
                work["attempt_count"] = count
                result = plan(event("failure", failure(retryable=retryable, lease_lost=lease_lost)), work, attempt)
                self.assertEqual(result.work["status"], expected)
                self.assertEqual(result.attempt["status"], expected_attempt)
                self.assertEqual(result.attempt["processed_frames"], 7)
                self.assertEqual(result.attempt["completed_at"], NOW)
                self.assertEqual(result.work["last_error_message"], "failed")
                self.assertEqual(result.work["lease_owner_attempt_id"], None)
                if expected == "RETRY_WAIT":
                    self.assertEqual(result.work["not_before_at"], NOW + timedelta(minutes=1))
                    self.assertEqual(result.work["queue_entered_at"], NOW)
                else:
                    self.assertIsNone(result.work["not_before_at"])
                    self.assertNotIn("queue_entered_at", result.work)

    def test_failure_delay_is_capped(self):
        work, attempt = state("RUNNING")
        work.update(attempt_count=1000, max_attempts=1001)
        result = plan(event("failure", failure()), work, attempt)
        self.assertEqual(result.work["not_before_at"], NOW + timedelta(minutes=60))

    def test_failure_can_release_its_expired_lease_but_cannot_renew_it(self):
        work, attempt = state("RUNNING")
        work["lease_expires_at"] = NOW - timedelta(minutes=1)
        result = plan(event("failure", failure(lease_lost=True)), work, attempt)
        self.assertEqual(result.attempt["status"], "LEASE_LOST")
        self.assertEqual(result.work["status"], "RETRY_WAIT")
        self.assertIsNone(result.work["lease_expires_at"])
        self.assertIsNone(result.work["lease_owner_attempt_id"])

    def test_partial_failure_replays_after_expiry_without_changing_timestamp(self):
        work, attempt = state("RUNNING")
        row = event("failure", failure())
        result = plan(row, work, attempt)
        attempt.update(result.attempt)
        replay = plan(row, work, attempt, NOW + timedelta(hours=1))
        self.assertEqual(replay, result)
        work.update(result.work)
        self.assertEqual(plan(row, work, attempt).work, {})
        self.assertEqual(plan(row, work, attempt).attempt, {})

    def test_failure_does_not_overwrite_an_unpublished_succeeded_attempt(self):
        work, attempt = state("WRITING")
        attempt["status"] = "SUCCEEDED"
        self.assertRejected(event("failure", failure()), work, attempt, "Completed")

    def test_failure_cannot_clobber_new_lease_or_watchdog_or_terminal_work(self):
        row = event("failure", failure())
        for change, message in (
            ({"lease_owner_attempt_id": "new"}, "does not own"),
            ({"status": "RECOVERING"}, "not active"),
            ({"status": "DEAD_LETTERED"}, "not active"),
            ({"status": "SUCCEEDED", "committed_attempt_id": "attempt"}, "Committed"),
        ):
            work, attempt = state("RUNNING")
            work.update(change)
            self.assertRejected(row, work, attempt, message)
        work, attempt = state("RUNNING")
        attempt["status"] = "RETRY_WAIT"
        self.assertRejected(row, work, attempt, "Completed")

    def test_every_mutating_command_rejects_stale_worker(self):
        for kind, payload in (
            ("heartbeat", {"status": "WRITING", "updates": {}}),
            ("attempt_update", {"updates": {}}),
            ("commit", {}), ("failure", failure()),
        ):
            work, attempt = state("WRITING", worker="new")
            self.assertRejected(event(kind, payload), work, attempt, "Another execution")


class EventDrainTests(unittest.TestCase):
    def drain(self, store, limit=1000):
        with patch.object(events, "_DeltaStore", return_value=store), patch.object(events, "_utc_now", return_value=NOW):
            return events.process_worker_events(None, store.writer, table_prefix="pc", limit=limit)

    def test_duplicates_are_receipted_once_inside_one_writer_run(self):
        writer = Writer()
        row = event()
        store = MemoryStore(writer, [row, copy.deepcopy(row)])
        self.assertEqual(self.drain(store), {"processed": 1, "applied": 1, "rejected": 0})
        self.assertEqual(writer.calls, 1)
        self.assertEqual(len(store.receipts), 1)
        self.assertEqual(len(store.writes), 1)
        self.assertEqual(self.drain(store)["processed"], 0)

    def test_reordered_delivery_is_sorted_and_receipts_fence_next_event(self):
        rows = [
            event("heartbeat", {"status": "RUNNING", "updates": {}}, 3),
            event("heartbeat", {"status": "STAGING", "updates": {}}, 2),
            event(),
        ]
        store = MemoryStore(Writer(), rows)
        self.assertEqual(self.drain(store)["applied"], 3)
        self.assertEqual(store.work["status"], "RUNNING")
        self.assertEqual([batch[0]["sequence"] for batch in store.receipt_batches], [1, 2, 3])

    def test_late_sequence_is_rejected_without_any_mutation(self):
        store = MemoryStore(Writer(), [event(sequence=4)])
        self.drain(store)
        store.rows.append(event("heartbeat", {"status": "STAGING", "updates": {}}, 2))
        count = len(store.writes)
        self.assertEqual(self.drain(store)["rejected"], 1)
        self.assertEqual(len(store.writes), count)
        self.assertIn("Stale event sequence", store.receipts["event-2"]["message"])

    def test_equal_sequence_different_event_is_rejected(self):
        store = MemoryStore(Writer(), [event(), event(event_id="duplicate-sequence")])
        self.assertEqual(self.drain(store)["rejected"], 1)
        self.assertEqual(len(store.writes), 1)

    def test_unrelated_execution_sequences_do_not_fence_valid_owner(self):
        store = MemoryStore(Writer(), [event()])
        store.receipts["old"] = {
            **event(sequence=99), "worker_execution_id": "other", "outcome": "REJECTED",
        }
        self.assertEqual(self.drain(store)["applied"], 1)

    def test_limit_and_empty_drain(self):
        store = MemoryStore(Writer(), [event(), event(sequence=2)])
        self.assertEqual(self.drain(store, limit=1)["processed"], 1)
        self.assertEqual(len(store.receipts), 1)
        self.assertEqual(self.drain(store, limit=1)["processed"], 1)
        self.assertEqual(self.drain(store), {"processed": 0, "applied": 0, "rejected": 0})

    def test_invalid_durable_payload_becomes_rejected_receipt(self):
        store = MemoryStore(Writer(), [event(payload_json="{")])
        self.assertEqual(self.drain(store)["rejected"], 1)
        self.assertIn("Invalid event", store.receipts["event-1"]["message"])
        self.assertFalse(store.writer.retained)
        self.assertEqual(store.writes, [])

    def test_missing_or_duplicate_state_is_rejected(self):
        store = MemoryStore(Writer(), [event()])
        store.unique = MagicMock(side_effect=events._Rejected("Expected exactly one row"))
        self.assertEqual(self.drain(store)["rejected"], 1)
        self.assertEqual(store.writes, [])

    def test_partial_failure_recovery_finishes_work_then_receipt(self):
        work, attempt = state("RUNNING")
        store = MemoryStore(Writer(), [event("failure", failure())], work, attempt)
        store.fail_after = "video_attempts"
        with self.assertRaisesRegex(OSError, "Ambiguous"):
            self.drain(store)
        self.assertTrue(store.writer.retained)
        self.assertEqual(store.receipts, {})
        self.assertEqual(store.work["status"], "RUNNING")
        self.assertEqual(store.attempt["status"], "RETRY_WAIT")
        store.writer.retained = False  # Operator has fenced the failed writer.
        self.assertEqual(self.drain(store)["applied"], 1)
        self.assertEqual(store.work["status"], "RETRY_WAIT")
        self.assertEqual(store.attempt["completed_at"], NOW)

    def test_ambiguous_work_commit_replays_without_regression(self):
        work, attempt = state("WRITING")
        attempt["status"] = "SUCCEEDED"
        store = MemoryStore(Writer(), [event("commit")], work, attempt)
        store.fail_after = "video_work"
        with self.assertRaises(OSError):
            self.drain(store)
        self.assertEqual(store.work["committed_attempt_id"], "attempt")
        self.assertEqual(store.receipts, {})
        store.writer.retained = False
        self.assertEqual(self.drain(store)["applied"], 1)
        self.assertEqual(len(store.writes), 1)

    def test_ambiguous_receipt_append_never_duplicates_receipt(self):
        store = MemoryStore(Writer(), [event()])
        store.fail_after = "receipts"
        with self.assertRaises(OSError):
            self.drain(store)
        store.writer.retained = False
        self.assertEqual(self.drain(store)["processed"], 0)
        self.assertEqual(len(store.receipt_batches), 1)

    def test_unexpected_storage_errors_escape_without_receipt(self):
        store = MemoryStore(Writer(), [event()])
        store.unique = MagicMock(side_effect=OSError("Spark unavailable"))
        with self.assertRaisesRegex(OSError, "Spark unavailable"):
            self.drain(store)
        self.assertTrue(store.writer.retained)
        self.assertEqual(store.receipts, {})

    def test_failures_during_mutation_retain_lock_without_rejection_receipt(self):
        store = MemoryStore(Writer(), [event()])
        store.update = MagicMock(side_effect=events._Rejected("Lease expired while applying"))
        with self.assertRaisesRegex(events._Rejected, "Lease expired while applying"):
            self.drain(store)
        self.assertTrue(store.writer.retained)
        self.assertEqual(store.receipts, {})

    def test_lease_expiry_after_attempt_write_requires_safe_recovery(self):
        store = MemoryStore(Writer(), [event("heartbeat", {"status": "STAGING", "updates": {}})])
        apply_update = store.update

        def update(table, before, updates):
            if table == "video_work":
                raise RuntimeError("Lease expired while applying the event")
            apply_update(table, before, updates)

        store.update = update
        with self.assertRaisesRegex(RuntimeError, "Lease expired"):
            self.drain(store)
        self.assertTrue(store.writer.retained)
        self.assertEqual(store.attempt["status"], "STAGING")
        self.assertEqual(store.work["status"], "LEASED")
        self.assertEqual(store.receipts, {})

    def test_independent_work_receipts_are_batched(self):
        store = MemoryStore(Writer(), [
            event(work_id="work-1", attempt_id="attempt-1"),
            event(sequence=2, work_id="work-2", attempt_id="attempt-2"),
        ])
        store.unique = MagicMock(side_effect=events._Rejected("Missing work"))
        self.assertEqual(self.drain(store)["rejected"], 2)
        self.assertEqual(len(store.receipt_batches), 1)
        self.assertEqual(len(store.receipt_batches[0]), 2)

    def test_receipt_recheck_handles_a_stale_pending_snapshot(self):
        row = event()
        store = MemoryStore(Writer(), [row])
        store.receipts[row["event_id"]] = row
        store.pending = MagicMock(return_value=[row])
        self.assertEqual(self.drain(store)["processed"], 0)
        self.assertEqual(store.writes, [])


class EventValidationTests(unittest.TestCase):
    def test_delta_naive_utc_timestamps_match_durable_event_timestamps(self):
        self.assertTrue(events._matches(
            {"completed_at": NOW.replace(tzinfo=None)},
            {"completed_at": NOW},
        ))
        self.assertFalse(events._matches({"completed_at": None}, {"completed_at": NOW}))
        self.assertFalse(events._matches(
            {"completed_at": NOW.replace(tzinfo=None) - timedelta(seconds=1)},
            {"completed_at": NOW},
        ))

    def test_all_allowed_columns_and_nullable_prefix_are_validated(self):
        updates = dict.fromkeys(events._UPDATE_FIELDS)
        updates["status"] = "SUCCEEDED"
        events._validate_updates(updates)
        for invalid in (
            {"source_fps": None, "processed_frames": -1},
            {"source_fps": None, "status": None},
        ):
            with self.subTest(updates=invalid), self.assertRaises(ValueError):
                events._validate_updates(invalid)

    def test_invalid_identifiers_and_limits_fail_before_writer(self):
        writer = MagicMock()
        for prefix, database in (("x;DROP", ""), ("x.y", ""), ("", ""), ("pc", "db.x"), (None, "")):
            with self.assertRaises(ValueError):
                events.process_worker_events(None, writer, table_prefix=prefix, database=database)
        for limit in (0, -1, True, 1.5, "1"):
            with self.assertRaises(ValueError):
                events.process_worker_events(None, writer, table_prefix="pc", limit=limit)
        writer.run.assert_not_called()
        self.assertEqual(events._tables("pc", "db")["worker_events"], "db.pc_worker_events")

    def test_payload_keys_kinds_and_columns_are_allowlisted(self):
        for kind, payload in (
            ("delete", {}), ("commit", {"status": "SUCCEEDED"}), ("claim_execution", []),
            ("heartbeat", {"status": "RUNNING", "updates": {"status": "WRITING"}}),
            ("heartbeat", {"status": "SUCCEEDED", "updates": {}}),
            ("heartbeat", {"status": "RUNNING", "updates": {"completed_at": NOW}}),
            ("attempt_update", {"updates": {"worker_execution_id": "hijack"}}),
            ("attempt_update", {"updates": {"config_sha256": "hijack"}}),
            ("attempt_update", {"updates": {"retryable": True}}),
            ("attempt_update", {"updates": {"error_message": "wrong channel"}}),
            ("attempt_update", {"updates": {"lease_owner_attempt_id": "hijack"}}),
            ("attempt_update", {"updates": []}),
            ("failure", failure(retryable="true")), ("failure", failure(lease_lost=None)),
            ("failure", failure(error_type="")), ("failure", failure(error_message=None)),
        ):
            with self.subTest(kind=kind, payload=payload), self.assertRaises(ValueError):
                events._validate_payload(kind, payload)

    def test_update_value_types_are_validated_before_casts(self):
        for name, value in (
            ("status", None), ("status", 1), ("processed_frames", -1), ("processed_frames", 2**63),
            ("processed_frames", True), ("processing_seconds", float("inf")),
            ("processing_seconds", float("nan")), ("processing_seconds", "1"),
            ("pipeline_run_id", []), ("completed_at", "yesterday"),
            ("completed_at", 123), ("retryable", "yes"),
        ):
            with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                events._validate_updates({name: value})
        events._validate_updates({
            "processed_frames": 0, "processing_seconds": 0.0, "retryable": False,
            "input_sha256": None, "completed_at": NOW, "status": "RUNNING",
        })

    def test_envelope_validates_identity_date_sequence_and_duration(self):
        for changes in (
            {"work_id": ""}, {"attempt_id": 42}, {"worker_execution_id": ""},
            {"capture_date": "2026-09-23"}, {"capture_date": NOW}, {"sequence": 0},
            {"payload_json": "{}"},
            {"payload_json": json.dumps({"payload": {}, "lease_minutes": 0})},
            {"created_at": None},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                events._Command.from_row(event(**changes))

    def test_timestamp_normalization_and_json_dates(self):
        self.assertEqual(events._timestamp(NOW.replace(tzinfo=None)), NOW)
        self.assertEqual(events._timestamp("2026-09-23T14:00:00+02:00"), NOW)
        self.assertEqual(events._json_default(DAY), "2026-09-23")
        with self.assertRaises(TypeError):
            events._json_default(object())


class WorkerEventClientTests(unittest.TestCase):
    def setUp(self):
        clock = SimpleNamespace(
            monotonic=MagicMock(side_effect=count(0, 100)),
            sleep=MagicMock(),
        )
        time_patch = patch.object(events, "time", clock)
        time_patch.start()
        self.addCleanup(time_patch.stop)

    def client(self, **changes):
        values = {
            "table_prefix": "pc", "database": "db", "work_id": "work", "attempt_id": "attempt",
            "worker_execution_id": "execution", "capture_date": DAY, "lease_minutes": 30,
            **changes,
        }
        spark = MagicMock()
        bootstrapped_spark(spark)
        output = spark.createDataFrame.return_value.write
        output.format.return_value = output
        output.mode.return_value = output
        output.option.return_value = output
        return events.WorkerEventClient(spark, MagicMock(), **values), spark, output

    def test_submit_durable_transaction_then_drain_then_receipt(self):
        client, spark, output = self.client()
        order = []
        output.saveAsTable.side_effect = lambda name: order.append("append")
        expected = {"outcome": "APPLIED", "message": "ok"}
        with (
            patch.object(events, "uuid4", return_value="stable-id") as uuid,
            patch.object(events, "_utc_now", return_value=NOW),
            patch.object(events, "process_worker_events", side_effect=lambda *a, **kw: order.append("drain") or {"processed": 1}) as drain,
            patch.object(events, "_read_receipt", side_effect=lambda *a: order.append("receipt") or expected),
        ):
            self.assertEqual(client.submit("heartbeat", {"status": "STAGING", "updates": {"staging_started_at": NOW}}), expected)
        self.assertEqual(order, ["append", "drain", "receipt"])
        uuid.assert_called_once_with()
        output.option.assert_has_calls([call("txnAppId", "stable-id"), call("txnVersion", 0)])
        output.saveAsTable.assert_called_once_with("db.pc_worker_events")
        row = spark.createDataFrame.call_args.args[0][0]
        self.assertEqual(row["event_id"], "stable-id")
        self.assertEqual(row["sequence"], 1)
        self.assertEqual(row["created_at"], NOW)
        self.assertEqual(row["capture_date"], DAY)
        self.assertEqual(json.loads(row["payload_json"]), {
            "lease_minutes": 30, "payload": {
                "status": "STAGING", "updates": {"staging_started_at": NOW.isoformat()},
            },
        })
        drain.assert_called_once_with(spark, client._writer, table_prefix="pc", database="db")

    def test_sequence_and_created_at_are_monotonic(self):
        client, spark, _ = self.client()
        with (
            patch.object(events, "_utc_now", side_effect=[NOW, NOW - timedelta(seconds=1)]),
            patch.object(events, "process_worker_events", return_value={"processed": 1}),
            patch.object(events, "_read_receipt", return_value={"outcome": "APPLIED"}),
        ):
            client.submit("claim_execution", {})
            client.submit("claim_execution", {})
        rows = [item.args[0][0] for item in spark.createDataFrame.call_args_list]
        self.assertEqual([row["sequence"] for row in rows], [1, 2])
        self.assertEqual([row["created_at"] for row in rows], [NOW, NOW])
        self.assertNotEqual(rows[0]["event_id"], rows[1]["event_id"])

    def test_client_drains_backlog_until_own_receipt(self):
        client, _, output = self.client()
        with (
            patch.object(events, "process_worker_events", return_value={"processed": 1000}) as drain,
            patch.object(events, "_read_receipt", side_effect=[None, {"outcome": "APPLIED"}]),
        ):
            client.submit("claim_execution", {})
        self.assertEqual(drain.call_count, 2)
        output.saveAsTable.assert_called_once()

    def test_rejected_receipt_is_lease_lost(self):
        client, _, _ = self.client()
        with (
            patch.object(events, "process_worker_events", return_value={"processed": 1}),
            patch.object(events, "_read_receipt", return_value={"outcome": "REJECTED", "message": "expired"}),
            self.assertRaisesRegex(events.LeaseLostError, "expired"),
        ):
            client.submit("claim_execution", {})

    def test_unknown_receipt_is_not_success(self):
        client, _, _ = self.client()
        with (
            patch.object(events, "process_worker_events", return_value={"processed": 0}),
            patch.object(events, "_read_receipt", return_value={"outcome": "UNKNOWN"}),
            self.assertRaisesRegex(RuntimeError, "Unknown"),
        ):
            client.submit("claim_execution", {})

    def test_empty_drain_waits_until_deadline_without_resubmission(self):
        client, _, output = self.client()
        with (
            patch.object(events.time, "monotonic", side_effect=[100, 100, 700]),
            patch.object(events.time, "sleep") as sleep,
            patch.object(events, "process_worker_events", return_value={"processed": 0}) as drain,
            patch.object(events, "_read_receipt", return_value=None),
            self.assertRaisesRegex(TimeoutError, "durable event .*remains queued"),
        ):
            client.submit("claim_execution", {})
        drain.assert_called_once()
        sleep.assert_called_once_with(0.1)
        output.saveAsTable.assert_called_once()

    def test_continuous_backlog_is_bounded_without_fabricated_success(self):
        client, _, output = self.client()
        with (
            patch.object(events.time, "monotonic", side_effect=[0, 0, 1, 600]),
            patch.object(events, "uuid4", return_value="stable-id") as uuid,
            patch.object(events, "process_worker_events", return_value={"processed": 1000}) as drain,
            patch.object(events, "_read_receipt", return_value=None),
            self.assertRaisesRegex(TimeoutError, "stable-id"),
        ):
            client.submit("claim_execution", {})
        self.assertEqual(drain.call_count, 2)
        uuid.assert_called_once_with()
        output.saveAsTable.assert_called_once()

    def test_storage_failures_propagate_without_automatic_resubmission(self):
        for failure_point in ("append", "drain", "receipt"):
            client, _, output = self.client()
            with (
                patch.object(events, "process_worker_events", return_value={"processed": 1}) as drain,
                patch.object(events, "_read_receipt", return_value={"outcome": "APPLIED"}) as receipt,
            ):
                target = {"append": output.saveAsTable, "drain": drain, "receipt": receipt}[failure_point]
                target.side_effect = OSError(failure_point)
                with self.assertRaisesRegex(OSError, failure_point):
                    client.submit("claim_execution", {})
            output.saveAsTable.assert_called_once()

    def test_invalid_client_or_payload_never_writes(self):
        for changes in ({"work_id": ""}, {"capture_date": NOW}, {"lease_minutes": 0}):
            with self.assertRaises(ValueError):
                self.client(**changes)
        client, spark, _ = self.client()
        with self.assertRaises(ValueError):
            client.submit("commit", {"sql": "DELETE"})
        spark.createDataFrame.assert_not_called()

    def test_missing_bootstrap_tables_fail_before_any_inbox_write(self):
        for suffix in events._REQUIRED_SCHEMAS:
            with self.subTest(suffix=suffix):
                client, spark, output = self.client()
                missing = f"db.pc_{suffix}"
                spark.catalog.tableExists.side_effect = lambda name: name != missing
                with self.assertRaisesRegex(RuntimeError, f"{missing}.*offline bootstrap"):
                    client.submit("claim_execution", {})
                spark.createDataFrame.assert_not_called()
                output.saveAsTable.assert_not_called()
                client._writer.run.assert_not_called()
                self.assertEqual(client._sequence, 0)

    def test_incompatible_bootstrap_schema_fails_before_inbox_write(self):
        for suffix, column, invalid_type in (
            ("worker_events", "sequence", "int"),
            ("worker_event_receipts", "applied_at", "string"),
            ("video_work", "config_sha256", "binary"),
            ("video_attempts", "processed_frames", "string"),
        ):
            with self.subTest(suffix=suffix, column=column):
                client, spark, output = self.client()
                schemas = bootstrapped_spark(spark)
                schemas[suffix][column].dataType.simpleString.return_value = invalid_type
                with self.assertRaisesRegex(RuntimeError, f"requires {column}"):
                    client.submit("claim_execution", {})
                spark.createDataFrame.assert_not_called()
                output.saveAsTable.assert_not_called()

    def test_catalog_failures_preserve_original_exception(self):
        client, spark, output = self.client()
        original = OSError("Catalog permission denied")
        spark.catalog.tableExists.side_effect = original
        with self.assertRaises(OSError) as raised:
            client.submit("claim_execution", {})
        self.assertIs(raised.exception, original)
        output.saveAsTable.assert_not_called()

    def test_views_cannot_shadow_missing_bootstrap_tables(self):
        for temporary, table_type in ((True, "TEMPORARY"), (False, "VIEW")):
            client, spark, output = self.client()
            spark.catalog.getTable.return_value = SimpleNamespace(
                isTemporary=temporary, tableType=table_type,
            )
            with self.assertRaisesRegex(RuntimeError, "permanent bootstrap table"):
                client.submit("claim_execution", {})
            spark.createDataFrame.assert_not_called()
            output.saveAsTable.assert_not_called()


class DeltaBoundaryTests(unittest.TestCase):
    def setUp(self):
        sql = ModuleType("pyspark.sql")
        sql.functions = MagicMock()
        delta_tables = ModuleType("delta.tables")
        delta_tables.DeltaTable = MagicMock()
        self.modules = patch.dict(sys.modules, {
            "pyspark": ModuleType("pyspark"), "pyspark.sql": sql,
            "delta": ModuleType("delta"), "delta.tables": delta_tables,
        })
        self.modules.start()
        self.addCleanup(self.modules.stop)
        self.f = sql.functions
        self.f.col.return_value.__gt__.return_value = MagicMock()
        self.f.lit.return_value.__gt__.return_value = MagicMock()
        self.delta = delta_tables.DeltaTable
        self.spark = MagicMock()
        self.schemas = bootstrapped_spark(self.spark)
        self.store = events._DeltaStore(self.spark, events._tables("pc", "db"))

    def test_pending_uses_left_anti_receipts_and_deterministic_order(self):
        frame = self.spark.table.return_value.join.return_value
        frame.orderBy.return_value.limit.return_value.collect.return_value = []
        self.assertEqual(self.store.pending(17), [])
        self.spark.table.return_value.join.assert_called_once_with(
            self.spark.table.return_value.select.return_value, "event_id", "leftanti",
        )
        frame.orderBy.assert_called_once_with("created_at", "worker_execution_id", "sequence", "event_id")
        frame.orderBy.return_value.limit.assert_called_once_with(17)

    def test_duplicate_rows_and_receipts_are_not_accepted(self):
        query = self.spark.table.return_value.where.return_value.limit.return_value
        for count in (0, 2):
            query.collect.return_value = [MagicMock()] * count
            with self.assertRaises(events._Rejected):
                self.store.unique("video_work", "work_id", "work")
        query.collect.return_value = [MagicMock(), MagicMock()]
        with self.assertRaisesRegex(RuntimeError, "Duplicate receipts"):
            self.store.receipt("event")
        query.collect.return_value = []
        self.assertIsNone(self.store.receipt("event"))

    def test_unique_and_receipt_return_row_dictionary(self):
        row = MagicMock()
        row.asDict.return_value = {"event_id": "event"}
        self.spark.table.return_value.where.return_value.limit.return_value.collect.return_value = [row]
        self.assertEqual(self.store.receipt("event"), {"event_id": "event"})
        self.assertEqual(self.store.unique("video_work", "work_id", "work"), {"event_id": "event"})

    def use_query_rows(self, rows):
        self.store.f = PredicateFunctions
        sys.modules["pyspark.sql"].functions = PredicateFunctions
        self.spark.table.side_effect = None
        self.spark.table.return_value = QueryRows(rows)

    def test_receipt_query_filters_event_before_limiting_or_detecting_duplicates(self):
        target = {"event_id": "requested", "outcome": "APPLIED"}
        unrelated = [{"event_id": f"other-{i}", "outcome": "REJECTED"} for i in range(3)]
        self.use_query_rows([*unrelated, target])
        self.assertEqual(self.store.receipt("requested"), target)
        self.assertIsNone(self.store.receipt("missing"))
        self.use_query_rows([*unrelated, target, target])
        with self.assertRaisesRegex(RuntimeError, "Duplicate receipts for event requested"):
            self.store.receipt("requested")

    def test_sequence_query_cannot_use_receipts_from_other_work_or_execution(self):
        identity = event()
        receipts = [{**identity, "sequence": 3}, {**identity, "sequence": 7}]
        for key in ("work_id", "attempt_id", "worker_execution_id"):
            receipts.append({**identity, key: "another", "sequence": 99})
        self.use_query_rows(receipts)
        self.assertEqual(self.store.highest_sequence(identity), 7)
        self.assertEqual(
            self.store.highest_sequence({**identity, "work_id": "missing"}),
            0,
        )

    def test_highest_sequence_is_scoped_to_all_execution_keys(self):
        frame = self.spark.table.return_value
        frame.where.return_value = frame
        for number, expected in ((None, 0), (17, 17)):
            frame.agg.return_value.collect.return_value = [{"sequence": number}]
            self.assertEqual(self.store.highest_sequence(event()), expected)
        self.assertEqual(
            [item.args[0] for item in self.f.col.call_args_list],
            ["work_id", "attempt_id", "worker_execution_id"] * 2,
        )
        self.assertEqual(self.f.max.call_args_list, [call("sequence"), call("sequence")])

    def test_updates_use_real_delta_api_partial_columns_and_target_schema_casts(self):
        _, attempt = state("RUNNING")
        after = {**attempt, "processed_frames": 42}
        with patch.object(self.store, "unique", return_value=after):
            self.store.update("video_attempts", attempt, {"processed_frames": 42})
        self.delta.forName.assert_called_once_with(self.spark, "db.pc_video_attempts")
        update = self.delta.forName.return_value.update
        self.assertEqual(set(update.call_args.kwargs["set"]), {"processed_frames"})
        self.f.lit.return_value.cast.assert_called_once_with(
            self.spark.table.return_value.schema["processed_frames"].dataType,
        )
        self.assertEqual(
            [item.args[0] for item in self.f.col.call_args_list],
            ["work_id", "capture_date", "config_sha256", "status", "attempt_id", "worker_execution_id", "dispatcher_id"],
        )

    def test_work_publication_is_guarded_by_owner_state_and_database_time(self):
        work, _ = state("WRITING")
        with patch.object(self.store, "unique", return_value={**work, "status": "SUCCEEDED"}):
            self.store.update("video_work", work, {"status": "SUCCEEDED"})
        columns = [item.args[0] for item in self.f.col.call_args_list]
        for field in ("lease_owner_attempt_id", "lease_dispatcher_id", "lease_expires_at",
                      "committed_attempt_id", "attempt_count", "max_attempts"):
            self.assertIn(field, columns)
        self.f.current_timestamp.assert_called_once_with()

    def test_renewal_also_guards_new_expiry_against_database_time(self):
        work, _ = state("WRITING")
        updates = {"status": "WRITING", "lease_expires_at": NOW + timedelta(minutes=30)}
        with patch.object(self.store, "unique", return_value={**work, **updates}):
            self.store.update("video_work", work, updates)
        self.assertEqual(self.f.current_timestamp.call_count, 2)

    def apply_to_rows(self, table, before, rows, updates):
        self.store.f = PredicateFunctions

        def update(*, condition, set):
            for row in rows:
                if condition.evaluate(row):
                    row.update({key: expression.evaluate(row) for key, expression in set.items()})

        self.delta.forName.return_value.update.side_effect = update
        with (
            patch.object(self.store, "unique", return_value=rows[0]),
            patch.object(events, "_utc_now", return_value=NOW),
        ):
            self.store.update(table, before, updates)

    def test_control_updates_match_every_identity_and_version_key(self):
        work, attempt = state("WRITING")
        cases = (
            (
                "video_work", work, {"status": "SUCCEEDED"},
                (
                    "work_id", "capture_date", "config_sha256", "status",
                    "lease_owner_attempt_id", "lease_dispatcher_id",
                    "lease_acquired_at", "lease_expires_at", "committed_attempt_id",
                    "attempt_count", "max_attempts",
                ),
            ),
            (
                "video_attempts", attempt, {"processed_frames": 42},
                (
                    "work_id", "capture_date", "config_sha256", "status",
                    "attempt_id", "worker_execution_id", "dispatcher_id",
                ),
            ),
        )
        for table, before, updates, keys in cases:
            for key in keys:
                with self.subTest(table=table, key=key):
                    original = before[key]
                    if isinstance(original, datetime):
                        different = original + timedelta(seconds=1)
                    elif isinstance(original, date):
                        different = original + timedelta(days=1)
                    elif isinstance(original, int):
                        different = original + 1
                    else:
                        different = "another-value"
                    target = copy.deepcopy(before)
                    unrelated = {**before, key: different}
                    expected_unrelated = copy.deepcopy(unrelated)
                    self.apply_to_rows(table, before, [target, unrelated], updates)
                    self.assertEqual(target, {**before, **updates})
                    self.assertEqual(unrelated, expected_unrelated)

    def test_active_and_commit_writes_require_strictly_live_database_lease(self):
        for status in ("LEASED", "STAGING", "RUNNING", "WRITING", "SUCCEEDED"):
            for expiry in (None, NOW - timedelta(microseconds=1), NOW):
                with self.subTest(status=status, expiry=expiry):
                    before, _ = state("WRITING")
                    before["lease_expires_at"] = expiry
                    target = copy.deepcopy(before)
                    updates = {"status": status, "last_heartbeat_at": NOW}
                    with (
                        patch.object(events, "_utc_now", return_value=NOW),
                        self.assertRaisesRegex(RuntimeError, "expired"),
                    ):
                        self.apply_to_rows("video_work", before, [target], updates)
                    self.assertEqual(target, before)

    def test_renewal_cannot_write_an_already_expired_new_lease(self):
        for expiry in (NOW - timedelta(microseconds=1), NOW):
            with self.subTest(expiry=expiry):
                before, _ = state("RUNNING")
                target = copy.deepcopy(before)
                with self.assertRaisesRegex(RuntimeError, "did not persist"):
                    self.apply_to_rows(
                        "video_work", before, [target],
                        {"status": "RUNNING", "lease_expires_at": expiry},
                    )
                self.assertEqual(target, before)

    def test_live_lease_renewal_and_expired_failure_release_are_allowed(self):
        before, _ = state("RUNNING")
        renewed = {"status": "RUNNING", "lease_expires_at": NOW + timedelta(minutes=30)}
        target = copy.deepcopy(before)
        self.apply_to_rows("video_work", before, [target], renewed)
        self.assertEqual(target, {**before, **renewed})
        before["lease_expires_at"] = NOW - timedelta(seconds=1)
        target = copy.deepcopy(before)
        released = {"status": "RETRY_WAIT", **events._LEASE_CLEAR}
        self.apply_to_rows("video_work", before, [target], released)
        self.assertEqual(target, {**before, **released})

    def test_silent_update_mismatch_and_mid_write_expiry_are_errors(self):
        work, attempt = state("RUNNING")
        with patch.object(self.store, "unique", return_value=attempt), self.assertRaisesRegex(RuntimeError, "did not persist"):
            self.store.update("video_attempts", attempt, {"processed_frames": 99})
        work["lease_expires_at"] = NOW
        with (
            patch.object(self.store, "unique", return_value=work),
            patch.object(events, "_utc_now", return_value=NOW),
            self.assertRaisesRegex(RuntimeError, "expired"),
        ):
            self.store.update("video_work", work, {"status": "WRITING"})

    def test_empty_update_does_not_touch_delta(self):
        self.store.update("video_work", {}, {})
        self.delta.forName.assert_not_called()

    def test_writer_validates_all_bootstrap_tables_inside_lock_before_processing(self):
        for suffix in events._REQUIRED_SCHEMAS:
            with self.subTest(suffix=suffix):
                writer = Writer()
                missing = f"db.pc_{suffix}"

                def exists(name):
                    self.assertTrue(writer.active)
                    return name != missing

                self.spark.catalog.tableExists.side_effect = exists
                with self.assertRaisesRegex(RuntimeError, "missing.*offline bootstrap"):
                    events.process_worker_events(self.spark, writer, table_prefix="pc", database="db")
                self.assertTrue(writer.retained)
                self.assertEqual(writer.calls, 1)
                self.spark.createDataFrame.assert_not_called()
                self.delta.forName.assert_not_called()

    def test_missing_receipt_table_cannot_be_recreated_by_append(self):
        self.spark.catalog.tableExists.return_value = False
        with self.assertRaisesRegex(RuntimeError, "worker_event_receipts.*missing"):
            self.store.append_receipts([{"event_id": "event"}])
        self.spark.createDataFrame.assert_not_called()

    def test_missing_target_table_cannot_be_created_by_update(self):
        self.spark.catalog.tableExists.return_value = False
        _, attempt = state("RUNNING")
        with self.assertRaisesRegex(RuntimeError, "video_attempts.*missing"):
            self.store.update("video_attempts", attempt, {"processed_frames": 10})
        self.delta.forName.assert_not_called()

    def test_missing_and_ambiguous_columns_fail_schema_validation(self):
        schema = self.schemas["video_work"]
        schema.fields = [field for field in schema.fields if field.name != "config_sha256"]
        with self.assertRaisesRegex(RuntimeError, "config_sha256 string; found missing"):
            events._table_schema(self.spark, "db.pc_video_work", "video_work")
        schema.fields.append(SimpleNamespace(name="WORK_ID", dataType=MagicMock()))
        with self.assertRaisesRegex(RuntimeError, "ambiguous column names"):
            events._table_schema(self.spark, "db.pc_video_work", "video_work")

    def test_schema_validation_permits_unrelated_extra_columns(self):
        schema = self.schemas["video_work"]
        dtype = MagicMock()
        dtype.simpleString.return_value = "string"
        schema.fields.append(SimpleNamespace(name="asset_id", dataType=dtype))
        self.assertIs(events._table_schema(self.spark, "db.pc_video_work", "video_work"), schema)
        self.spark.catalog.getTable.return_value.tableType = "EXTERNAL"
        self.assertIs(events._table_schema(self.spark, "db.pc_video_work", "video_work"), schema)

    def test_receipts_are_appended_not_merged_or_inbox_mutated(self):
        receipts = [{"event_id": "event"}]
        self.store.append_receipts(receipts)
        self.spark.createDataFrame.assert_called_once_with(receipts, self.spark.table.return_value.schema)
        self.spark.createDataFrame.return_value.write.format.assert_called_once_with("delta")
        self.spark.createDataFrame.return_value.write.format.return_value.mode.assert_called_once_with("append")
        self.spark.createDataFrame.return_value.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
            "db.pc_worker_event_receipts",
        )
        self.delta.forName.assert_not_called()


if __name__ == "__main__":
    unittest.main()
