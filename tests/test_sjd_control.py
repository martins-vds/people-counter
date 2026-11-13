import hashlib
import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from people_counter.sjd_control import (
    BatchValidationError,
    ControlError,
    FabricControlStore,
    ImmutableConflictError,
    LeaseBudgetError,
    LeaseLostError,
    SQLiteControlStore,
    UnsupportedControlStoreError,
    main,
    normalize_registration_requests,
    parse_registration_manifest,
    read_registration_manifest,
    require_release_compatibility,
)
from people_counter.sjd_process import LOCAL_TWO_WORKERS, verify_envelope


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

    @staticmethod
    def request(**overrides):
        return {
            "work_id": "work",
            "payload": {"source_video": "video.mp4"},
            "runtime_key": "runtime-a",
            "duration_seconds": 10,
            "config_sha256": "config-a",
            "release_digest": "release-a",
            **overrides,
        }

    def test_registration_manifests_accept_json_and_jsonl_without_losing_defaults(self):
        request = self.request()
        for text in (
            json.dumps(request),
            json.dumps([request]),
            json.dumps({"items": [request]}),
            "\n" + json.dumps(request) + "\n\n" + json.dumps(self.request(work_id="other")),
        ):
            with self.subTest(text=text):
                normalized = parse_registration_manifest(text)
                self.assertEqual(normalized[0]["work_id"], "work")
                self.assertEqual(normalized[0]["max_attempts"], 3)
                self.assertIsNone(normalized[0]["available_at"])
        manifest = self.store.database.parent / "manifest.jsonl"
        manifest.write_text(json.dumps(request) + "\n", encoding="utf-8")
        works = self.store.register_manifest(manifest)
        self.assertEqual([work.work_id for work in works], ["work"])
        self.assertEqual(self.store.get_work("work"), works[0])
        self.assertEqual(self.store.register_many([request]), works)
        self.assertEqual(
            read_registration_manifest(manifest),
            parse_registration_manifest(manifest.read_text(encoding="utf-8")),
        )

    def test_invalid_registration_is_rejected_before_writing_work(self):
        class IntegerSubclass(int):
            pass

        class FloatSubclass(float):
            pass

        invalid = (
            ("work_id", " "),
            ("payload", []),
            ("payload", {"invalid": float("nan")}),
            ("duration_seconds", 0),
            ("duration_seconds", True),
            ("duration_seconds", float("inf")),
            ("duration_seconds", IntegerSubclass(10)),
            ("duration_seconds", FloatSubclass(10)),
            ("max_attempts", 0),
            ("max_attempts", 101),
            ("max_attempts", True),
            ("available_at", "tomorrow"),
        )
        for field, value in invalid:
            request = self.request(**{field: value})
            with self.subTest(field=field, value=value):
                with self.assertRaises(ValueError):
                    self.store.register(**request)
                with self.assertRaises(ValueError):
                    self.store.register_many([request])
        self.assertEqual(self.store.status()["work"], {})
        for text in ("", "[]", "null", "3", '{"items": []}', "[4]", "[{}]"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                parse_registration_manifest(text)
        with self.assertRaisesRegex(ValueError, "source.jsonl:3"):
            parse_registration_manifest(
                json.dumps(self.request()) + "\n\nnot-json", source="source.jsonl"
            )
        with self.assertRaisesRegex(ValueError, "cannot read"):
            read_registration_manifest(self.store.database.parent / "missing.json")

    def test_claim_rejects_invalid_admission_without_leasing(self):
        self.register("work")
        for overrides in (
            {"max_items": True},
            {"minimum_items": 0},
            {"minimum_items": 11},
            {"maximum_active_batches": 0},
            {"maximum_active_batches": True},
            {"safety_factor": 0.9},
            {"allowed_work_ids": "work"},
            {"allowed_work_ids": iter(["work"])},
            {"allowed_work_ids": [""]},
            {"peak_rss_bytes": 1},
            {"minimum_speed_x": None},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                self.claim(**overrides)
        self.assertIsNone(self.claim(allowed_work_ids=[]))
        require_release_compatibility([], None)
        self.assertEqual(self.store.get_work("work").attempt_count, 0)

    def test_profile_admission_checks_memory_and_legacy_conflicts(self):
        self.register("work")
        arguments = {
            "process_profile": LOCAL_TWO_WORKERS,
            "minimum_speed_x": None,
            "safety_factor": None,
            "margin_seconds": None,
        }
        cases = (
            ({"peak_rss_bytes": 2**30}, LeaseBudgetError),
            ({"peak_rss_bytes": True}, ValueError),
            ({"minimum_speed_x": 2}, ValueError),
            ({"process_profile": replace(LOCAL_TWO_WORKERS, task_cpus=3)}, ValueError),
            ({"process_profile": replace(LOCAL_TWO_WORKERS, memory_reserve_bytes=-1),
              "peak_rss_bytes": 1}, ValueError),
            ({"process_profile": replace(LOCAL_TWO_WORKERS, name="")}, ValueError),
        )
        for overrides, error in cases:
            with self.subTest(overrides=overrides), self.assertRaises(error):
                self.claim(**(arguments | overrides))
        batch = self.claim(**arguments, peak_rss_bytes=512 * 2**20)
        self.assertEqual([item.work_id for item in batch.items], ["work"])

    def test_missing_work_and_batch_interfaces_fail_explicitly(self):
        for operation in (
            lambda: self.store.get_work("missing"),
            lambda: self.store.load_claim_envelope("missing"),
            lambda: self.store.load_claim_envelope_with_digest("missing"),
            lambda: self.store.process_batch_state(SimpleNamespace(batch_id="missing")),
            lambda: self.store.commit_batch("missing"),
            lambda: self.store.replay("missing", operator="ops", reason="retry"),
            lambda: self.store.seal_batch(
                "missing", [], envelope_sha256="a" * 64, membership_sha256="b" * 64
            ),
        ):
            with self.subTest(operation=operation), self.assertRaises(KeyError):
                operation()

    def test_envelope_read_detects_missing_tampered_and_invalid_content(self):
        self.register("work")
        batch = self.claim()
        path = Path(batch.envelope_path)
        original = path.read_bytes()
        for content, message in (
            (b"tampered", "digest mismatch"),
            (None, "missing"),
        ):
            if content is None:
                path.unlink()
            else:
                path.write_bytes(content)
            with self.assertRaisesRegex(BatchValidationError, message):
                self.store.load_claim_envelope(batch.batch_id)
        path.write_bytes(original)
        for content in (b"{", b'{"schema_version":2}', b"null", b"[]"):
            path.write_bytes(content)
            digest = hashlib.sha256(content).hexdigest()
            with sqlite3.connect(self.store.database) as connection:
                connection.execute(
                    "UPDATE batches SET envelope_sha256=? WHERE batch_id=?",
                    (digest, batch.batch_id),
                )
            with self.subTest(content=content), self.assertRaises(BatchValidationError):
                self.store.load_claim_envelope(batch.batch_id)

    def test_process_heartbeat_extends_all_fences_and_never_shortens_them(self):
        self.register("work")
        batch = self.claim()
        raw = self.store.load_claim_envelope(batch.batch_id)
        envelope = verify_envelope(
            raw, batch_id=batch.batch_id, envelope_sha256=batch.envelope_sha256
        )
        self.store.heartbeat_process(envelope, 1)
        self.assertEqual(self.store.process_batch_state(envelope).lease_expires_at, 200)
        self.clock.value = 150
        self.store.heartbeat_process(envelope, 100)
        self.assertEqual(self.store.process_batch_state(envelope).lease_expires_at, 250)
        with sqlite3.connect(self.store.database) as connection:
            for table in ("work", "attempts", "batches"):
                self.assertEqual(
                    connection.execute(f"SELECT lease_expires_at FROM {table}").fetchone()[0],
                    250,
                )
        with self.assertRaisesRegex(BatchValidationError, "identity changed"):
            self.store.assert_process_fence(replace(envelope, envelope_sha256="a" * 64))
        with self.assertRaisesRegex(LeaseLostError, "membership changed"):
            self.store.assert_process_fence(replace(envelope, items=()))
        with sqlite3.connect(self.store.database) as connection:
            connection.execute("UPDATE work SET fence=fence+1")
        with self.assertRaisesRegex(LeaseLostError, "lost its work fence"):
            self.store.heartbeat_process(envelope, 100)

    def test_seal_validation_is_atomic_and_repeated_seals_are_immutable(self):
        self.register("work")
        batch = self.claim()
        envelope = self.store.load_claim_envelope(batch.batch_id)
        hashes = {
            "envelope_sha256": batch.envelope_sha256,
            "membership_sha256": envelope["membership_sha256"],
        }
        output = self.outputs(batch)[0]
        for outputs, error in (
            ("invalid", ValueError),
            ([None], ValueError),
            ([output, output], BatchValidationError),
            ([output | {"records": "invalid"}], BatchValidationError),
            ([output | {"records": [None]}], BatchValidationError),
            ([output | {"records": []}], BatchValidationError),
            ([output | {"succeeded": 1}], ValueError),
            ([output | {"output_sha256": "invalid"}], ValueError),
            ([output | {"records": [output["records"][0] | {"partition_id": -1}]}], ValueError),
        ):
            with self.subTest(outputs=outputs), self.assertRaises(error):
                self.store.seal_batch(batch.batch_id, outputs, **hashes)
        with self.assertRaisesRegex(BatchValidationError, "hash mismatch"):
            self.store.seal_batch(
                batch.batch_id, [output], **(hashes | {"membership_sha256": "a" * 64})
            )
        self.store.seal_batch(batch.batch_id, [output], **hashes)
        self.store.seal_batch(batch.batch_id, [output], **hashes)
        for outputs, override in (
            ([output], {"envelope_sha256": "b" * 64}),
            ([output | {"output_path": "/different"}], {}),
            ([output | {"records": [output["records"][0] | {"executor_identity": "other"}]}], {}),
        ):
            with self.subTest(outputs=outputs), self.assertRaises(ImmutableConflictError):
                self.store.seal_batch(batch.batch_id, outputs, **(hashes | override))
        self.assertEqual(self.store.commit_batch(batch.batch_id), (1,))

    def test_failed_attempts_retry_then_die_without_publication(self):
        self.register("work", max_attempts=2)
        for status in ("READY", "DEAD"):
            batch = self.claim()
            envelope = self.store.load_claim_envelope(batch.batch_id)
            outputs = [output | {"succeeded": False} for output in self.outputs(batch)]
            self.store.seal_batch(
                batch.batch_id, outputs, envelope_sha256=batch.envelope_sha256,
                membership_sha256=envelope["membership_sha256"],
            )
            self.assertEqual(self.store.commit_batch(batch.batch_id), ())
            self.assertEqual(self.store.commit_batch(batch.batch_id), ())
            self.assertEqual(self.store.get_work("work").status, status)
        self.assertIsNone(self.store.status()["last_publication_sequence"])

    def test_bootstrap_migrates_retry_budget_and_rejects_incompatible_metadata(self):
        self.register("work", max_attempts=7)
        with sqlite3.connect(self.store.database) as connection:
            connection.execute("ALTER TABLE work DROP COLUMN original_max_attempts")
        self.store.bootstrap()
        self.assertEqual(self.store.get_work("work").original_max_attempts, 7)
        with sqlite3.connect(self.store.database) as connection:
            connection.execute("PRAGMA user_version=999")
        with self.assertRaisesRegex(ControlError, "unsupported control schema"):
            self.store.bootstrap()
        with sqlite3.connect(self.store.database) as connection:
            connection.execute("PRAGMA user_version=1")
            connection.execute("UPDATE control_metadata SET schema_version=999")
        with self.assertRaisesRegex(ControlError, "metadata conflicts"):
            self.store.bootstrap()
        for timeout in (0, 60_001):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                SQLiteControlStore(self.store.database, busy_timeout_ms=timeout)

    def test_reconciliation_tracks_tampering_and_resolves_restored_artifacts(self):
        self.register("work")
        batch = self.claim()
        path = Path(batch.envelope_path)
        original = path.read_bytes()
        for content in (None, b"tampered"):
            if content is None:
                path.unlink()
            else:
                path.write_bytes(content)
            findings = self.store.reconcile()
            self.assertEqual([finding.finding_type for finding in findings], ["ENVELOPE_HASH_MISMATCH"])
        path.write_bytes(original)
        self.assertEqual(self.store.reconcile(), [])
        for content in (b"{", b"{}", b'{"items":[],"membership_sha256":"wrong"}'):
            path.write_bytes(content)
            with sqlite3.connect(self.store.database) as connection:
                connection.execute(
                    "UPDATE batches SET envelope_sha256=?",
                    (hashlib.sha256(content).hexdigest(),),
                )
            self.assertEqual(
                [finding.finding_type for finding in self.store.reconcile()],
                ["MEMBERSHIP_HASH_MISMATCH"],
            )
        with sqlite3.connect(self.store.database) as connection:
            path.write_bytes(original)
            connection.execute(
                "UPDATE batches SET envelope_sha256=?", (batch.envelope_sha256,)
            )
            connection.execute("UPDATE attempts SET fence=fence+1")
        findings = self.store.reconcile()
        self.assertEqual(findings[0].details["reason"], "attempt rows differ from batch membership")

    def test_manifest_cli_registers_atomically_and_rejects_mixed_arguments(self):
        manifest = self.store.database.parent / "requests.json"
        manifest.write_text(json.dumps([self.request()]), encoding="utf-8")
        base = ["--database", str(self.store.database), "register"]
        with patch("sys.stdout"):
            self.assertEqual(main(base + ["--manifest", str(manifest)]), 0)
        self.assertEqual(self.store.get_work("work").status, "READY")
        with self.assertRaisesRegex(ValueError, "cannot be combined"):
            main(base + ["--manifest", str(manifest), "--work-id", "other"])
        with self.assertRaisesRegex(ValueError, "single-item registration requires"):
            main(base)

    def test_replay_rejects_changed_requests_and_committed_work(self):
        self.register("work", max_attempts=1)
        batch = self.claim()
        self.clock.value = batch.lease_expires_at
        self.store.recover()
        for attempts in (0, True, 101):
            with self.subTest(attempts=attempts), self.assertRaises(ValueError):
                self.store.replay("work", operator="ops", reason="retry", additional_attempts=attempts)
        replay = self.store.replay("work", operator="ops", reason="retry")
        with self.assertRaisesRegex(ControlError, "not replay-eligible"):
            self.store.replay("work", operator="other", reason="retry")
        self.assertEqual(self.store.get_work("work").last_replay_id, replay.replay_id)
        with sqlite3.connect(self.store.database) as connection:
            connection.execute("UPDATE work SET status='DEAD', committed_attempt_id='corrupt'")
        with self.assertRaisesRegex(ControlError, "committed work"):
            self.store.replay("work", operator="ops", reason="retry")

    def test_quarantine_rejects_missing_mismatched_and_already_leased_work(self):
        digest = "a" * 64
        self.store.register(**self.request(release_digest=digest))
        for values, error in (
            ({}, ValueError),
            ([], ValueError),
            ({"missing": digest}, KeyError),
            ({"work": "b" * 64}, ImmutableConflictError),
        ):
            with self.subTest(values=values), self.assertRaises(error):
                self.store.quarantine(values, operator="ops", reason="obsolete")
        self.claim()
        with self.assertRaisesRegex(ControlError, "not quarantine-eligible"):
            self.store.quarantine({"work": digest}, operator="ops", reason="obsolete")

    def test_seal_and_commit_reject_corrupt_authoritative_attempts(self):
        self.register("work")
        batch = self.claim()
        envelope = self.store.load_claim_envelope(batch.batch_id)
        hashes = {
            "envelope_sha256": batch.envelope_sha256,
            "membership_sha256": envelope["membership_sha256"],
        }
        with sqlite3.connect(self.store.database) as connection:
            connection.execute("UPDATE work SET fence=fence+1")
        with self.assertRaisesRegex(LeaseLostError, "lost its work fence"):
            self.store.seal_batch(batch.batch_id, self.outputs(batch), **hashes)
        with sqlite3.connect(self.store.database) as connection:
            connection.execute("UPDATE work SET fence=fence-1")
        self.store.seal_batch(batch.batch_id, self.outputs(batch), **hashes)
        with sqlite3.connect(self.store.database) as connection:
            connection.execute("UPDATE attempts SET output_path=NULL")
        with self.assertRaisesRegex(LeaseLostError, "cannot advance its pointer"):
            self.store.commit_batch(batch.batch_id)
        self.assertEqual(self.store.get_work("work").status, "LEASED")
        with sqlite3.connect(self.store.database) as connection:
            connection.execute("DELETE FROM attempts")
        with self.assertRaisesRegex(BatchValidationError, "cardinality"):
            self.store.commit_batch(batch.batch_id)

    def test_legacy_duplicate_output_corruption_is_detected_before_bootstrap(self):
        self.register("work")
        batch = self.claim()
        envelope = self.store.load_claim_envelope(batch.batch_id)
        self.store.seal_batch(
            batch.batch_id, self.outputs(batch), envelope_sha256=batch.envelope_sha256,
            membership_sha256=envelope["membership_sha256"],
        )
        with sqlite3.connect(self.store.database) as connection:
            connection.execute("ALTER TABLE output_records RENAME TO modern_output_records")
            connection.execute("CREATE TABLE output_records AS SELECT * FROM modern_output_records")
            connection.execute("INSERT INTO output_records SELECT * FROM modern_output_records")
            connection.execute("DROP TABLE modern_output_records")
        duplicate = next(
            finding for finding in self.store.reconcile()
            if finding.finding_type == "DUPLICATE_OUTPUT_IDENTITY"
        )
        self.assertEqual(duplicate.details, {"count": 2})
        with self.assertRaisesRegex(ControlError, "duplicate immutable identities"):
            self.store.bootstrap()

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

    def test_release_difference_does_not_prevent_a_supported_claim(self):
        self.register("old-release-work")
        with self.assertWarns(FutureWarning):
            batch = self.claim(expected_release_digest="release-b")
        self.assertEqual([item.work_id for item in batch.items], ["old-release-work"])
        with self.store._connect() as connection:
            row = connection.execute(
                "SELECT status,attempt_count FROM work WHERE work_id = ?",
                ("old-release-work",),
            ).fetchone()
            self.assertEqual(tuple(row), ("LEASED", 1))
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM attempts").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM batches").fetchone()[0], 1)

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
        import subprocess
        import sys

        probe = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import sys; import people_counter.sjd_control; "
                    "raise SystemExit('pyspark' in sys.modules)"
                ),
            ],
            check=False,
        )
        self.assertEqual(probe.returncode, 0)
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
