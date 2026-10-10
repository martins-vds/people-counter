import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from people_counter.sjd_control import (
    BatchValidationError,
    FabricControlStore,
    ImmutableConflictError,
    LeaseBudgetError,
    LeaseLostError,
    SQLiteControlStore,
    UnsupportedControlStoreError,
    main,
    normalize_registration_requests,
)
from people_counter.sjd_process import LOCAL_TWO_WORKERS


class MutableClock:
    def __init__(self, value=100.0):
        self.value = value

    def __call__(self):
        return self.value


class SJDControlTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=Path.cwd())
        root = Path(self.temporary.name)
        self.clock = MutableClock()
        self.identities = iter(f"id-{index}" for index in range(100))
        self.store = SQLiteControlStore(
            root / "control.sqlite3",
            root / "content",
            clock=self.clock,
            id_factory=lambda: next(self.identities),
        )

    def tearDown(self):
        self.temporary.cleanup()

    def register(self, work_id, *, runtime="runtime-a", max_attempts=3, duration=10):
        return self.store.register(
            work_id,
            {"source_video": f"{work_id}.mp4"},
            runtime_key=runtime,
            duration_seconds=duration,
            config_sha256="config-a",
            release_digest="release-a",
            max_attempts=max_attempts,
            available_at=0,
        )

    def claim(self, **overrides):
        values = {
            "max_items": 10,
            "minimum_items": 1,
            "lease_seconds": 100,
            "minimum_speed_x": 1,
            "safety_factor": 1,
            "margin_seconds": 10,
        }
        values.update(overrides)
        return self.store.claim("driver-a", **values)

    @staticmethod
    def outputs(batch):
        return [
            {
                "work_id": item.work_id,
                "attempt_id": item.attempt_id,
                "output_path": f"/staging/{item.attempt_id}",
                "output_sha256": hashlib.sha256(
                    item.attempt_id.encode()
                ).hexdigest(),
                "records": [
                    {
                        "executor_identity": f"executor-{ordinal}",
                        "partition_id": ordinal,
                        "task_attempt_id": 0,
                        "record_sequence": 0,
                    }
                ],
            }
            for ordinal, item in enumerate(batch.items)
        ]

    def test_register_is_idempotent_but_rejects_immutable_conflicts(self):
        first = self.register("work-1")
        second = self.register("work-1")

        self.assertEqual(first, second)
        with self.assertRaises(ImmutableConflictError):
            self.store.register(
                "work-1",
                {"source_video": "different.mp4"},
                runtime_key="runtime-a",
                duration_seconds=10,
                config_sha256="config-a",
                release_digest="release-a",
            )

    def test_register_many_is_atomic_idempotent_and_rejects_partition_conflicts(self):
        def request(work_id, source):
            return {
                "work_id": work_id,
                "payload": {"source_video": source},
                "runtime_key": "runtime-a",
                "duration_seconds": 10,
                "config_sha256": "config-a",
                "release_digest": "release-a",
                "max_attempts": 3,
                "available_at": 25,
            }

        partition = [
            request("work-1", "one.mp4"),
            request("work-1", "one.mp4"),
            request("work-2", "two.mp4"),
        ]
        first = self.store.register_many(partition)
        second = self.store.register_many(partition)

        self.assertEqual(first, second)
        self.assertEqual([item.work_id for item in first], ["work-1", "work-2"])
        with sqlite3.connect(self.store.database) as connection:
            available = connection.execute(
                "SELECT work_id, available_at FROM work ORDER BY work_id"
            ).fetchall()
        self.assertEqual(available, [("work-1", 25.0), ("work-2", 25.0)])
        with self.assertRaisesRegex(
            ImmutableConflictError,
            "duplicated with different content",
        ):
            self.store.register_many(
                [
                    request("work-3", "three.mp4"),
                    request("work-3", "different.mp4"),
                ]
            )
        with self.assertRaises(KeyError):
            self.store.get_work("work-3")

    def test_registration_request_normalization_rejects_non_sequences(self):
        for invalid in ({}, "items", b"items", bytearray(b"items")):
            with self.subTest(value=invalid):
                with self.assertRaisesRegex(
                    ValueError,
                    "registration requests must be a sequence",
                ):
                    normalize_registration_requests(invalid)

    def test_claim_is_homogeneous_bounded_and_materializes_verified_envelope(self):
        self.register("a-1", runtime="runtime-a")
        self.register("b-1", runtime="runtime-b")
        self.register("a-2", runtime="runtime-a")

        batch = self.claim(max_items=2, minimum_items=2)

        self.assertIsNotNone(batch)
        self.assertEqual(batch.batch_id, "id-0")
        self.assertEqual(batch.runtime_key, "runtime-a")
        self.assertEqual([item.work_id for item in batch.items], ["a-1", "a-2"])
        envelope = self.store.load_claim_envelope(
            batch.batch_id,
            envelope_path=batch.envelope_path,
            envelope_sha256=batch.envelope_sha256,
        )
        self.assertEqual(envelope["schema_version"], 1)
        self.assertEqual(
            [item["work_id"] for item in envelope["items"]], ["a-1", "a-2"]
        )
        self.assertEqual(self.store.get_work("b-1").status, "READY")

    def test_claim_scope_atomically_excludes_unrelated_ready_work(self):
        self.register("unrelated", runtime="runtime-a")
        self.register("requested-1", runtime="runtime-a")
        self.register("requested-2", runtime="runtime-a")

        batch = self.claim(
            max_items=3,
            minimum_items=2,
            allowed_work_ids={"requested-1", "requested-2"},
        )

        self.assertEqual(
            [item.work_id for item in batch.items],
            ["requested-1", "requested-2"],
        )
        self.assertEqual(self.store.get_work("unrelated").status, "READY")
        self.assertIsNone(
            self.claim(
                minimum_items=1,
                allowed_work_ids={"requested-1", "requested-2"},
            )
        )

    def test_claim_respects_atomic_active_batch_limit(self):
        self.register("work-1")
        self.register("work-2")

        first = self.claim(max_items=1, maximum_active_batches=1)
        blocked = self.claim(max_items=1, maximum_active_batches=1)

        self.assertIsNotNone(first)
        self.assertIsNone(blocked)
        self.assertEqual(self.store.get_work("work-2").status, "READY")
        with self.assertRaisesRegex(
            ValueError,
            "maximum_active_batches must be between 1 and 100",
        ):
            self.claim(maximum_active_batches=0)

    def test_claim_fails_closed_for_cardinality_and_lease_budget(self):
        self.register("slow", duration=90)

        self.assertIsNone(self.claim(max_items=2, minimum_items=2))
        self.assertEqual(self.store.get_work("slow").attempt_count, 0)
        with self.assertRaises(LeaseBudgetError):
            self.claim(lease_seconds=100, margin_seconds=10)
        self.assertEqual(self.store.get_work("slow").status, "READY")

    def test_claim_uses_process_profile_worker_count_and_exact_lpt_makespan(self):
        for work_id, duration in (
            ("work-0", 8),
            ("work-1", 7),
            ("work-2", 6),
            ("work-3", 5),
        ):
            self.register(work_id, duration=duration)

        with self.assertRaisesRegex(LeaseBudgetError, "makespan=13"):
            self.store.claim(
                "driver-a",
                max_items=4,
                minimum_items=4,
                lease_seconds=46.25,
                process_profile=LOCAL_TWO_WORKERS,
            )
        batch = self.store.claim(
            "driver-a",
            max_items=4,
            minimum_items=4,
            lease_seconds=46.3,
            process_profile=LOCAL_TWO_WORKERS,
        )
        envelope = self.store.load_claim_envelope(batch.batch_id)
        self.assertEqual(envelope["admission"]["worker_count"], 2)
        self.assertEqual(envelope["admission"]["projected_makespan_seconds"], 13)

    def test_claim_uses_largest_lease_safe_prefix_without_consuming_remainder(self):
        for work_id in ("work-0", "work-1", "work-2"):
            self.register(work_id, duration=40)

        batch = self.store.claim(
            "driver-a",
            max_items=3,
            minimum_items=2,
            lease_seconds=81,
            process_profile=LOCAL_TWO_WORKERS,
        )

        self.assertEqual([item.work_id for item in batch.items], ["work-0", "work-1"])
        self.assertEqual(self.store.get_work("work-2").status, "READY")
        self.assertEqual(self.store.get_work("work-2").attempt_count, 0)
        envelope = self.store.load_claim_envelope(batch.batch_id)
        self.assertEqual(envelope["admission"]["worker_count"], 2)
        self.assertEqual(envelope["admission"]["projected_makespan_seconds"], 40)

    def test_adaptive_claim_honors_single_and_maximum_boundaries(self):
        self.register("single", duration=40)
        single = self.store.claim(
            "driver-a",
            max_items=1,
            minimum_items=1,
            lease_seconds=81,
            process_profile=LOCAL_TWO_WORKERS,
        )
        self.assertEqual([item.work_id for item in single.items], ["single"])

        for work_id in ("short-0", "short-1", "short-2"):
            self.register(work_id, duration=1)
        maximum = self.store.claim(
            "driver-a",
            max_items=3,
            minimum_items=2,
            lease_seconds=33,
            process_profile=LOCAL_TWO_WORKERS,
        )
        self.assertEqual(
            [item.work_id for item in maximum.items],
            ["short-0", "short-1", "short-2"],
        )

    def test_adaptive_claim_applies_speed_safety_margin_and_strict_boundary(self):
        self.register("speed-adjusted", duration=10)
        accelerated = self.claim(
            max_items=1,
            lease_seconds=7,
            minimum_speed_x=2,
            safety_factor=1,
            margin_seconds=1,
        )
        self.assertEqual(
            [item.work_id for item in accelerated.items],
            ["speed-adjusted"],
        )

        for work_id in ("safe-0", "safe-1", "safe-2"):
            self.register(work_id, duration=40)
        safe = self.store.claim(
            "driver-a",
            max_items=3,
            minimum_items=1,
            lease_seconds=100,
            process_profile=LOCAL_TWO_WORKERS,
        )
        self.assertEqual([item.work_id for item in safe.items], ["safe-0", "safe-1"])
        self.assertEqual(self.store.get_work("safe-2").attempt_count, 0)

        with self.assertRaises(LeaseBudgetError):
            self.store.claim(
                "driver-a",
                max_items=1,
                minimum_items=1,
                lease_seconds=80,
                process_profile=LOCAL_TWO_WORKERS,
            )
        self.assertEqual(self.store.get_work("safe-2").attempt_count, 0)

    def test_adaptive_claim_checks_every_intermediate_prefix_size(self):
        for work_id, duration in (
            ("work-0", 40),
            ("work-1", 30),
            ("work-2", 20),
            ("work-3", 20),
        ):
            self.register(work_id, duration=duration)

        batch = self.store.claim(
            "driver-a",
            max_items=4,
            minimum_items=1,
            lease_seconds=100,
            process_profile=LOCAL_TWO_WORKERS,
        )
        self.assertEqual(
            [item.work_id for item in batch.items],
            ["work-0", "work-1", "work-2"],
        )
        self.assertEqual(self.store.get_work("work-3").attempt_count, 0)

    def test_adaptive_claim_does_not_admit_a_maximum_at_exact_lease_boundary(self):
        for work_id in ("work-0", "work-1", "work-2"):
            self.register(work_id, duration=40)

        batch = self.store.claim(
            "driver-a",
            max_items=3,
            minimum_items=2,
            lease_seconds=130,
            process_profile=LOCAL_TWO_WORKERS,
        )
        self.assertEqual([item.work_id for item in batch.items], ["work-0", "work-1"])
        self.assertEqual(self.store.get_work("work-2").attempt_count, 0)

    def test_envelope_requires_matching_batch_path_and_digest(self):
        self.register("work")
        batch = self.claim()

        with self.assertRaises(BatchValidationError):
            self.store.load_claim_envelope(
                batch.batch_id,
                envelope_path=batch.envelope_path,
                envelope_sha256="0" * 64,
            )

    def test_seal_and_commit_advance_atomic_publication_sequence(self):
        self.register("work-1")
        self.register("work-2")
        batch = self.claim(max_items=2)
        envelope = self.store.load_claim_envelope(batch.batch_id)

        self.store.seal_batch(
            batch.batch_id,
            self.outputs(batch),
            envelope_sha256=batch.envelope_sha256,
            membership_sha256=envelope["membership_sha256"],
        )
        sequences = self.store.commit_batch(batch.batch_id)

        self.assertEqual(sequences, (1, 2))
        self.assertEqual(self.store.commit_batch(batch.batch_id), (1, 2))
        work = self.store.get_work("work-1")
        self.assertEqual(work.status, "SUCCEEDED")
        self.assertIn(work.publication_sequence, sequences)
        self.assertEqual(self.store.reconcile(), [])

    def test_commit_rejects_a_live_but_unsealed_batch(self):
        self.register("work")
        batch = self.claim()

        with self.assertRaises(LeaseLostError):
            self.store.commit_batch(batch.batch_id)

    def test_seal_rejects_missing_members_and_expired_fences(self):
        self.register("work-1")
        self.register("work-2")
        batch = self.claim(max_items=2)
        envelope = self.store.load_claim_envelope(batch.batch_id)

        with self.assertRaises(BatchValidationError):
            self.store.seal_batch(
                batch.batch_id,
                self.outputs(batch)[:1],
                envelope_sha256=batch.envelope_sha256,
                membership_sha256=envelope["membership_sha256"],
            )
        self.clock.value = batch.lease_expires_at
        with self.assertRaises(LeaseLostError):
            self.store.seal_batch(
                batch.batch_id,
                self.outputs(batch),
                envelope_sha256=batch.envelope_sha256,
                membership_sha256=envelope["membership_sha256"],
            )

    def test_seal_requires_explicit_unique_provenance_records(self):
        self.register("work")
        batch = self.claim()
        envelope = self.store.load_claim_envelope(batch.batch_id)

        missing = self.outputs(batch)
        missing[0].pop("records")
        with self.assertRaises(BatchValidationError):
            self.store.seal_batch(
                batch.batch_id,
                missing,
                envelope_sha256=batch.envelope_sha256,
                membership_sha256=envelope["membership_sha256"],
            )
        renamed = self.outputs(batch)
        renamed[0]["process_records"] = renamed[0].pop("records")
        with self.assertRaises(BatchValidationError):
            self.store.seal_batch(
                batch.batch_id,
                renamed,
                envelope_sha256=batch.envelope_sha256,
                membership_sha256=envelope["membership_sha256"],
            )
        duplicate = self.outputs(batch)
        duplicate[0]["records"].append(dict(duplicate[0]["records"][0]))
        with self.assertRaisesRegex(BatchValidationError, "duplicate"):
            self.store.seal_batch(
                batch.batch_id,
                duplicate,
                envelope_sha256=batch.envelope_sha256,
                membership_sha256=envelope["membership_sha256"],
            )

    def test_recover_retries_then_dead_letters_and_fences_old_attempt(self):
        self.register("work", max_attempts=2)
        first = self.claim(lease_seconds=20, margin_seconds=1)
        self.clock.value = first.lease_expires_at

        report = self.store.recover()
        self.assertEqual((report.recovered, report.retried, report.dead), (1, 1, 0))
        self.assertEqual(self.store.get_work("work").status, "READY")

        second = self.claim(lease_seconds=20, margin_seconds=1)
        self.assertNotEqual(first.items[0].attempt_id, second.items[0].attempt_id)
        envelope = self.store.load_claim_envelope(first.batch_id)
        with self.assertRaises(LeaseLostError):
            self.store.seal_batch(
                first.batch_id,
                self.outputs(first),
                envelope_sha256=first.envelope_sha256,
                membership_sha256=envelope["membership_sha256"],
            )
        self.clock.value = second.lease_expires_at
        report = self.store.recover()
        self.assertEqual((report.recovered, report.retried, report.dead), (1, 0, 1))
        self.assertEqual(self.store.get_work("work").status, "DEAD")

    def test_recover_expires_sealed_attempt_but_retains_audit_metadata(self):
        self.register("work", max_attempts=2)
        first = self.claim(lease_seconds=20, margin_seconds=1)
        envelope = self.store.load_claim_envelope(first.batch_id)
        self.store.seal_batch(
            first.batch_id,
            self.outputs(first),
            envelope_sha256=first.envelope_sha256,
            membership_sha256=envelope["membership_sha256"],
        )
        self.clock.value = first.lease_expires_at

        self.store.recover()

        with sqlite3.connect(self.store.database) as connection:
            attempt = connection.execute(
                """
                SELECT status, output_path, output_sha256, terminal_succeeded,
                       sealed_at, recovery_outcome
                FROM attempts WHERE attempt_id = ?
                """,
                (first.items[0].attempt_id,),
            ).fetchone()
            record_count = connection.execute(
                "SELECT COUNT(*) FROM output_records WHERE attempt_id = ?",
                (first.items[0].attempt_id,),
            ).fetchone()[0]
        self.assertEqual(attempt[0], "EXPIRED")
        self.assertEqual(attempt[1], f"/staging/{first.items[0].attempt_id}")
        self.assertIsNotNone(attempt[2])
        self.assertEqual(attempt[3], 1)
        self.assertIsNotNone(attempt[4])
        self.assertEqual(attempt[5], "READY")
        self.assertEqual(record_count, 1)

        second = self.claim()
        envelope = self.store.load_claim_envelope(second.batch_id)
        self.store.seal_batch(
            second.batch_id,
            self.outputs(second),
            envelope_sha256=second.envelope_sha256,
            membership_sha256=envelope["membership_sha256"],
        )
        self.store.commit_batch(second.batch_id)
        self.assertEqual(self.store.reconcile(), [])

    def test_replay_is_stable_operator_attributed_and_only_for_dead_work(self):
        self.register("work", max_attempts=1)
        batch = self.claim(lease_seconds=20, margin_seconds=1)
        self.clock.value = batch.lease_expires_at
        self.store.recover()

        first = self.store.replay(
            "work", operator="alice@example.com", reason="reviewed transient failure"
        )
        second = self.store.replay(
            "work", operator="alice@example.com", reason="reviewed transient failure"
        )

        self.assertEqual(first, second)
        self.assertTrue(first.replay_id.startswith("replay-"))
        self.assertEqual(first.operator, "alice@example.com")
        self.assertEqual(self.store.get_work("work").last_replay_id, first.replay_id)
        with self.assertRaisesRegex(Exception, "not replay-eligible"):
            self.store.replay("work", operator="bob", reason="different request")

    def test_replay_changes_only_effective_retry_budget(self):
        originally_registered = self.register("work", max_attempts=1)
        batch = self.claim(lease_seconds=20, margin_seconds=1)
        self.clock.value = batch.lease_expires_at
        self.store.recover()
        self.store.replay(
            "work",
            operator="alice@example.com",
            reason="reviewed transient failure",
            additional_attempts=2,
        )

        after_replay = self.register("work", max_attempts=1)

        self.assertEqual(originally_registered.original_max_attempts, 1)
        self.assertEqual(after_replay.original_max_attempts, 1)
        self.assertEqual(after_replay.max_attempts, 3)
        with self.assertRaises(ImmutableConflictError):
            self.register("work", max_attempts=2)

    def test_quarantine_terminalizes_only_exact_ready_release_work(self):
        digest = "a" * 64
        self.store.register(
            "obsolete-work",
            {"source_video": "obsolete.mp4"},
            runtime_key="runtime-a",
            duration_seconds=10,
            config_sha256="config-a",
            release_digest=digest,
            available_at=0,
        )

        first = self.store.quarantine(
            {"obsolete-work": digest},
            operator="release-manager",
            reason="release evidence is no longer executable",
        )
        second = self.store.quarantine(
            {"obsolete-work": digest},
            operator="release-manager",
            reason="release evidence is no longer executable",
        )

        self.assertEqual(first, second)
        self.assertEqual(first.work_ids, ("obsolete-work",))
        self.assertEqual(self.store.get_work("obsolete-work").status, "DEAD")
        with self.assertRaises(ImmutableConflictError):
            self.store.quarantine(
                {"obsolete-work": "b" * 64},
                operator="release-manager",
                reason="release evidence is no longer executable",
            )

    def test_reconcile_uses_stable_ids_for_all_integrity_classes(self):
        self.register("expired", max_attempts=2)
        batch = self.claim(lease_seconds=20, margin_seconds=1)
        envelope = self.store.load_claim_envelope(batch.batch_id)
        self.store.seal_batch(
            batch.batch_id,
            self.outputs(batch),
            envelope_sha256=batch.envelope_sha256,
            membership_sha256=envelope["membership_sha256"],
        )
        self.clock.value = batch.lease_expires_at
        first = self.store.reconcile()
        first_ids = {finding.finding_id for finding in first}
        self.clock.value += 1
        second_ids = {finding.finding_id for finding in self.store.reconcile()}

        self.assertEqual(first_ids, second_ids)
        self.assertEqual(
            {finding.finding_type for finding in first},
            {"EXPIRED_LEASE", "SEALED_UNCOMMITTED_ATTEMPT"},
        )

    def test_reconcile_detects_pointer_orphan_and_membership_tampering(self):
        self.register("work")
        batch = self.claim()
        output = self.outputs(batch)[0]
        with sqlite3.connect(self.store.database) as connection:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute(
                """
                UPDATE batches SET membership_sha256 = ? WHERE batch_id = ?
                """,
                ("0" * 64, batch.batch_id),
            )
            connection.execute(
                """
                UPDATE work SET status = 'SUCCEEDED',
                    lease_owner = NULL, lease_attempt_id = NULL,
                    lease_expires_at = NULL
                WHERE work_id = 'work'
                """
            )
            values = (
                "missing-attempt",
                "orphan-work",
                output["output_path"],
                output["output_sha256"],
                "executor-x",
                1,
                2,
                3,
                self.clock.value,
            )
            connection.execute(
                """
                INSERT INTO output_records (
                    attempt_id, work_id, output_path, output_sha256,
                    executor_identity, partition_id, task_attempt_id,
                    record_sequence, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO output_records (
                        attempt_id, work_id, output_path, output_sha256,
                        executor_identity, partition_id, task_attempt_id,
                        record_sequence, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    values,
                )

        kinds = {finding.finding_type for finding in self.store.reconcile()}
        self.assertTrue(
            {
                "MISSING_COMMITTED_POINTER",
                "ORPHAN_OUTPUT",
                "MEMBERSHIP_HASH_MISMATCH",
            }
            <= kinds
        )

    def test_module_never_imports_pyspark_and_fabric_placeholder_fails(self):
        import sys

        self.assertNotIn("pyspark", sys.modules)
        with self.assertRaises(UnsupportedControlStoreError):
            FabricControlStore()

    def test_cli_bootstrap_register_claim_recover_reconcile_replay_status(self):
        root = Path(self.temporary.name)
        database = root / "cli.sqlite3"
        common = ["--database", str(database)]

        with patch("builtins.print") as output:
            self.assertEqual(main([*common, "bootstrap"]), 0)
            self.assertEqual(
                main(
                    [
                        *common,
                        "register",
                        "--work-id",
                        "cli-work",
                        "--payload-json",
                        '{"source_video":"a.mp4"}',
                        "--runtime-key",
                        "runtime-a",
                        "--duration-seconds",
                        "1",
                        "--config-sha256",
                        "config-a",
                        "--release-digest",
                        "release-a",
                        "--max-attempts",
                        "1",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        *common,
                        "claim",
                        "--owner",
                        "driver",
                        "--max-items",
                        "1",
                        "--lease-seconds",
                        "1",
                        "--minimum-speed-x",
                        "100",
                        "--margin-seconds",
                        "0.1",
                    ]
                ),
                0,
            )
            claimed = json.loads(output.call_args.args[0])
            self.assertEqual(claimed["items"][0]["work_id"], "cli-work")
            self.assertEqual(main([*common, "status"]), 0)
            self.assertEqual(main([*common, "reconcile"]), 0)
            self.assertEqual(main([*common, "recover"]), 0)

        with sqlite3.connect(database) as connection:
            connection.execute(
                "UPDATE work SET lease_expires_at = 0 WHERE work_id = 'cli-work'"
            )
            connection.execute(
                "UPDATE batches SET lease_expires_at = 0 WHERE batch_id = ?",
                (claimed["batch_id"],),
            )
        main([*common, "recover"])
        self.assertEqual(
            main(
                [
                    *common,
                    "replay",
                    "--work-id",
                    "cli-work",
                    "--operator",
                    "operator",
                    "--reason",
                    "reviewed",
                ]
            ),
            0,
        )


if __name__ == "__main__":
    unittest.main()
