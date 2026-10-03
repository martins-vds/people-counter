import argparse
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from people_counter.models import LineCountRecord, RunResult
from people_counter.sjd_control import LeaseLostError, SQLiteControlStore
from people_counter.sjd_process import (
    LOCAL_TWO_WORKERS,
    DirectExecutionHarness,
    FabricAttemptAdapter,
    LeaseAdmissionError,
    LocalJsonAttemptAdapter,
    ProcessValidationError,
    StagingConflictError,
    UnsupportedAttemptStoreError,
    _model_identity,
    _parser,
    _sha256,
    admit_lease,
    conservative_concurrency,
    execute_sjd_bucket_partition,
    execute_sjd_partition,
    plan_duration_lpt,
    resolve_profile,
    run_process_batch,
    sparse_line_records,
    validate_staged_records,
    verify_envelope,
)


class MutableClock:
    def __init__(self, value=100.0):
        self.value = value

    def __call__(self):
        return self.value


class SyntheticHarness:
    def __init__(self, failed=()):
        self.failed = set(failed)
        self.calls = 0

    def execute(self, envelope, plan, profile, mode):
        self.calls += 1
        records = []
        for bucket in plan.buckets:
            for planned in bucket.items:
                item = planned.item
                failed = item.work_id in self.failed
                payload = (
                    {
                        "error_category": "TEST",
                        "error_type": "SyntheticError",
                        "error_message": "independent failure",
                    }
                    if failed
                    else {"processed_frames": 1}
                )
                records.append(
                    {
                        "record_type": "error" if failed else "video_result",
                        "work_id": item.work_id,
                        "attempt_id": item.attempt_id,
                        "source_video": item.payload["source_video"],
                        "status": "FAILED" if failed else "SUCCEEDED",
                        "payload_json": json.dumps(
                            payload, separators=(",", ":"), sort_keys=True
                        ),
                        "error_type": "SyntheticError" if failed else None,
                        "error_message": "independent failure" if failed else None,
                        "retryable": True if failed else None,
                        "processed_frames": None if failed else 1,
                        "processing_seconds": None if failed else 0.01,
                        "emitted_at_utc": "2026-01-01T00:00:00+00:00",
                        "batch_id": envelope.batch_id,
                        "process_attempt_id": envelope.execution_attempt_id,
                        "envelope_sha256": envelope.envelope_sha256,
                        "manifest_sha256": envelope.envelope_sha256,
                        "membership_sha256": envelope.membership_sha256,
                        "fence": item.fence,
                        "input_payload_sha256": item.payload_sha256,
                        "record_payload_sha256": _sha256(payload),
                        "config_sha256": item.config_sha256,
                        "model_identity": _model_identity(item.payload),
                        "release_digest": item.release_digest,
                        "runtime_key": item.runtime_key,
                        "duration_seconds": item.duration_seconds,
                        "planned_cost_seconds": planned.planned_cost_seconds,
                        "planned_concurrency": plan.concurrency.concurrency,
                        "peak_rss_bytes": plan.concurrency.peak_rss_bytes,
                        "duration_only_fallback": (
                            plan.concurrency.duration_only_fallback
                        ),
                        "bucket_id": planned.bucket_id,
                        "wave_index": planned.wave_index,
                        "physical_partition": planned.physical_partition,
                        "physical_partition_id": planned.physical_partition,
                        "stage_id": 4,
                        "partition_id": planned.physical_partition,
                        "task_attempt_id": planned.bucket_id,
                        "task_attempt_number": 0,
                        "executor_identity": f"executor-{planned.bucket_id}@worker",
                        "executor_host": "worker",
                        "record_sequence": 0,
                        "detector_batch_size": item.payload.get("batch_size", 1),
                        "cpu_threads": 1,
                        "runtime_loads": 0,
                        "runtime_cache_hits": 0,
                    }
                )
        return records


class SjdProcessTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.root = Path(self.temporary.name)
        self.clock = MutableClock()
        identities = iter(f"id-{index}" for index in range(100))
        self.store = SQLiteControlStore(
            self.root / "control.sqlite3",
            self.root / "content",
            clock=self.clock,
            id_factory=lambda: next(identities),
        )
        self.attempts = LocalJsonAttemptAdapter(self.root / "staging")

    def tearDown(self):
        self.temporary.cleanup()

    def claim(self, durations=(10.0,), *, batch_sizes=None, lease=1000.0):
        sizes = batch_sizes or [1] * len(durations)
        for index, (duration, batch_size) in enumerate(
            zip(durations, sizes, strict=True)
        ):
            self.store.register(
                f"work-{index}",
                {
                    "source_video": str(self.root / f"video-{index}.mp4"),
                    "pipeline": "rtdetr-osnet",
                    "batch_size": batch_size,
                },
                runtime_key="runtime-a",
                duration_seconds=duration,
                config_sha256="config-a",
                release_digest="release-a",
            )
        return self.store.claim(
            "process-owner",
            max_items=len(durations),
            minimum_items=len(durations),
            lease_seconds=lease,
            minimum_speed_x=1,
            safety_factor=1,
            margin_seconds=1,
        )

    def verified(self, batch):
        envelope = self.store.load_claim_envelope(batch.batch_id)
        return verify_envelope(
            envelope,
            batch_id=batch.batch_id,
            envelope_sha256=batch.envelope_sha256,
        )

    def test_cli_help_and_required_run_contract_do_not_import_spark(self):
        parser = _parser()
        with self.assertRaises(SystemExit) as raised:
            parser.parse_args(["--help"])
        self.assertEqual(raised.exception.code, 0)
        arguments = parser.parse_args(
            [
                "run",
                "--batch-id",
                "batch-a",
                "--profile",
                "local-two-workers",
                "--mode",
                "probe",
                "--harness",
                "direct",
            ]
        )
        self.assertIsInstance(arguments, argparse.Namespace)
        self.assertEqual(arguments.mode, "probe")

    def test_profile_is_fixed_and_rejects_incompatible_overrides(self):
        profile = resolve_profile(
            "candidate-a-local",
            spark_overrides={
                "spark.executor.instances": 2,
                "spark.executor.cores": 1,
                "spark.task.cpus": 1,
                "spark.dynamicAllocation.enabled": False,
                "spark.speculation": False,
            },
        )
        self.assertEqual(profile, LOCAL_TWO_WORKERS)
        with self.assertRaises(ProcessValidationError):
            resolve_profile(
                "local-two-workers",
                spark_overrides={"spark.speculation": True},
            )
        with self.assertRaises(ProcessValidationError):
            resolve_profile("elastic")

    def test_concurrency_uses_items_profile_cpu_memory_and_explicit_fallback(self):
        fallback = conservative_concurrency(
            5, LOCAL_TWO_WORKERS, peak_rss_bytes=None
        )
        self.assertEqual(fallback.concurrency, 2)
        self.assertTrue(fallback.duration_only_fallback)
        measured = conservative_concurrency(
            5,
            LOCAL_TWO_WORKERS,
            peak_rss_bytes=700 * 1024 * 1024,
        )
        self.assertEqual(measured.memory_limit, 2)
        self.assertFalse(measured.duration_only_fallback)
        with self.assertRaises(ProcessValidationError):
            conservative_concurrency(
                2,
                LOCAL_TWO_WORKERS,
                peak_rss_bytes=900 * 1024 * 1024,
            )

    def test_lpt_waves_are_deterministic_and_map_buckets_to_physical_partitions(self):
        batch = self.claim((8, 7, 6, 5))
        envelope = self.verified(batch)
        plan = plan_duration_lpt(
            envelope.items, LOCAL_TWO_WORKERS, peak_rss_bytes=None
        )
        self.assertEqual(
            [[entry.item.work_id for entry in bucket.items] for bucket in plan.buckets],
            [["work-0", "work-3"], ["work-1", "work-2"]],
        )
        self.assertEqual(
            [bucket.physical_partition for bucket in plan.buckets], [0, 1]
        )
        self.assertEqual([bucket.bucket_id for bucket in plan.buckets], [0, 1])
        self.assertEqual(plan.concurrency.concurrency, 2)
        self.assertEqual([wave.index for wave in plan.waves], [0, 1])
        self.assertEqual(plan.projected_makespan_seconds, 13)

    def test_lease_admission_fails_closed_at_margin(self):
        batch = self.claim((10, 9))
        plan = plan_duration_lpt(
            self.verified(batch).items,
            LOCAL_TWO_WORKERS,
            peak_rss_bytes=None,
        )
        with self.assertRaises(LeaseAdmissionError):
            admit_lease(
                plan,
                LOCAL_TWO_WORKERS,
                lease_expires_at=142.5,
                now=100,
            )
        self.assertEqual(
            admit_lease(
                plan,
                LOCAL_TWO_WORKERS,
                lease_expires_at=143,
                now=100,
            ),
            12.5,
        )

    def test_envelope_rejects_payload_tampering_and_detector_batch_drift(self):
        batch = self.claim((10,))
        envelope = self.store.load_claim_envelope(batch.batch_id)
        envelope["items"][0]["payload"]["source_video"] = "tampered.mp4"
        with self.assertRaisesRegex(Exception, "payload SHA"):
            verify_envelope(
                envelope,
                batch_id=batch.batch_id,
                envelope_sha256=batch.envelope_sha256,
            )
        self.store.register(
            "invalid-batch",
            {
                "source_video": str(self.root / "invalid.mp4"),
                "pipeline": "rtdetr-osnet",
                "batch_size": 3,
            },
            runtime_key="runtime-a",
            duration_seconds=9,
            config_sha256="config-a",
            release_digest="release-a",
        )
        invalid = self.store.claim(
            "process-owner",
            max_items=1,
            lease_seconds=1000,
            minimum_speed_x=1,
            safety_factor=1,
            margin_seconds=1,
        )
        with self.assertRaisesRegex(ProcessValidationError, "batch_size"):
            self.verified(invalid)

    def test_sparse_line_records_keep_crossings_and_final_cumulative(self):
        rows = [
            {"frame": 1, "frame_in_count": 0, "frame_out_count": 0},
            {"frame": 2, "frame_in_count": 1, "frame_out_count": 0},
            {"frame": 3, "frame_in_count": 0, "frame_out_count": 0},
        ]
        self.assertEqual(
            [row["frame"] for row in sparse_line_records(rows)], [2, 3]
        )
        self.assertEqual(sparse_line_records([]), [])

    def test_executor_asserts_task_partition_and_configures_cpu_before_runtime(self):
        events = []

        runtime = object()

        def load(config):
            events.append("runtime")
            return runtime

        def run(config, installed):
            self.assertIs(installed, runtime)
            result = RunResult()
            result.started = True
            result.initialized = True
            result.fps = 30
            result.processed_frames = 1
            result.line_counts = [
                LineCountRecord(1, "0", "0", 0, 0, 0, 0, 0, 0, 1, 1)
            ]
            return result

        row = {
            "work_id": "work",
            "attempt_id": "attempt",
            "source_video": "video.mp4",
            "batch_id": "batch",
            "process_attempt_id": "process",
            "envelope_sha256": "a" * 64,
            "manifest_sha256": "a" * 64,
            "membership_sha256": "b" * 64,
            "fence": 1,
            "input_payload_sha256": "c" * 64,
            "config_sha256": "config",
            "model_identity": "model",
            "release_digest": "release",
            "runtime_key": "runtime",
            "duration_seconds": 1,
            "planned_cost_seconds": 1,
            "planned_concurrency": 1,
            "peak_rss_bytes": None,
            "duration_only_fallback": True,
            "bucket_id": 0,
            "wave_index": 0,
            "physical_partition": 0,
            "executor_cores": 1,
            "task_cpus": 1,
            "mode": "sdk",
            "_task_identity": {
                "partition_id": 0,
                "executor_identity": "executor@worker",
            },
        }

        def configure(*args, **kwargs):
            self.assertTrue(kwargs["apply_native_limits"])
            events.append("cpu")
            return type("Budget", (), {"threads_per_worker": 1})()

        with (
            patch("people_counter.sjd_process.configure_cpu_runtime", side_effect=configure),
            patch("people_counter.sjd_process._config_from_work", return_value=object()),
            patch(
                "people_counter.fabric_executor_partition._runtime_cache_key",
                return_value=("runtime",),
            ),
            patch(
                "people_counter.fabric_executor_partition.load_runtime",
                side_effect=load,
            ),
            patch(
                "people_counter.fabric_executor_partition.run_with_runtime",
                side_effect=run,
            ),
        ):
            records = list(execute_sjd_partition([row, row]))
        self.assertEqual(events[:2], ["cpu", "runtime"])
        self.assertEqual(records[-1]["runtime_loads"], 1)
        self.assertEqual(records[-1]["runtime_cache_hits"], 1)
        self.assertEqual(records[-1]["partition_id"], 0)
        bad = {**row, "physical_partition": 1}
        with self.assertRaisesRegex(ProcessValidationError, "partition mismatch"):
            list(execute_sjd_partition([bad]))

    def test_direct_probe_harness_uses_executor_partition_mapping(self):
        batch = self.claim((10, 8))
        envelope = self.verified(batch)
        plan = plan_duration_lpt(
            envelope.items, LOCAL_TWO_WORKERS, peak_rss_bytes=None
        )
        budget = type("Budget", (), {"threads_per_worker": 1})()
        with patch(
            "people_counter.sjd_process.configure_cpu_runtime",
            return_value=budget,
        ) as configure:
            records = DirectExecutionHarness().execute(
                envelope, plan, LOCAL_TWO_WORKERS, "probe"
            )
        configure.assert_called_with(1, 1, apply_native_limits=False)
        self.assertEqual(len(records), 2)
        self.assertEqual(
            {record["partition_id"] for record in records}, {0, 1}
        )
        self.assertEqual(
            {record["manifest_sha256"] for record in records},
            {envelope.envelope_sha256},
        )

    def test_top_level_bucket_adapter_requires_one_physical_bucket(self):
        self.assertEqual(list(execute_sjd_bucket_partition([])), [])
        self.assertEqual(list(execute_sjd_bucket_partition([None])), [])
        with self.assertRaisesRegex(ProcessValidationError, "exactly one bucket"):
            list(execute_sjd_bucket_partition([{"rows": []}, {"rows": []}]))
        with patch(
            "people_counter.sjd_process.execute_sjd_partition",
            return_value=[{"record": "ok"}],
        ) as executor:
            self.assertEqual(
                list(execute_sjd_bucket_partition([{"rows": [{"work": 1}]}])),
                [{"record": "ok"}],
            )
        executor.assert_called_once_with([{"work": 1}])

    def test_process_commits_pointer_and_is_idempotent_from_complete_marker(self):
        batch = self.claim((10, 8))
        harness = SyntheticHarness()
        first = run_process_batch(
            self.store,
            batch.batch_id,
            LOCAL_TWO_WORKERS,
            "probe",
            harness,
            self.attempts,
        )
        second = run_process_batch(
            self.store,
            batch.batch_id,
            LOCAL_TWO_WORKERS,
            "probe",
            harness,
            self.attempts,
        )
        self.assertEqual(first.publication_sequences, second.publication_sequences)
        self.assertFalse(first.resumed)
        self.assertTrue(second.resumed)
        self.assertEqual(harness.calls, 1)
        self.assertEqual(self.store.get_work("work-0").status, "SUCCEEDED")

    def test_sdk_mode_uses_the_same_process_protocol(self):
        batch = self.claim((10,))
        harness = SyntheticHarness()
        result = run_process_batch(
            self.store,
            batch.batch_id,
            LOCAL_TWO_WORKERS,
            "sdk",
            harness,
            self.attempts,
        )
        self.assertEqual(result.record_count, 1)
        self.assertEqual(harness.calls, 1)
        with self.assertRaisesRegex(ProcessValidationError, "unsupported"):
            run_process_batch(
                self.store,
                batch.batch_id,
                LOCAL_TWO_WORKERS,
                "invalid",
                harness,
                self.attempts,
            )

    def test_independent_video_failure_is_validated_and_published(self):
        batch = self.claim((10, 8))
        result = run_process_batch(
            self.store,
            batch.batch_id,
            LOCAL_TWO_WORKERS,
            "probe",
            SyntheticHarness({"work-1"}),
            self.attempts,
        )
        self.assertEqual(result.failed_work_ids, ("work-1",))
        self.assertEqual(len(result.publication_sequences), 1)
        self.assertEqual(self.store.get_work("work-0").status, "SUCCEEDED")
        self.assertEqual(self.store.get_work("work-1").status, "READY")
        self.assertIsNone(self.store.get_work("work-1").committed_attempt_id)

    def test_crash_after_marker_resumes_without_reexecution(self):
        batch = self.claim((10,))
        harness = SyntheticHarness()

        def crash(point):
            if point == "after_marker":
                raise RuntimeError("crash")

        with self.assertRaisesRegex(RuntimeError, "crash"):
            run_process_batch(
                self.store,
                batch.batch_id,
                LOCAL_TWO_WORKERS,
                "probe",
                harness,
                self.attempts,
                crash_hook=crash,
            )
        result = run_process_batch(
            self.store,
            batch.batch_id,
            LOCAL_TWO_WORKERS,
            "probe",
            harness,
            self.attempts,
        )
        self.assertTrue(result.resumed)
        self.assertEqual(harness.calls, 1)

    def test_crash_after_seal_resumes_before_pointer_without_reexecution(self):
        batch = self.claim((10,))
        harness = SyntheticHarness()

        def crash(point):
            if point == "after_seal":
                raise RuntimeError("crash after seal")

        with self.assertRaisesRegex(RuntimeError, "crash after seal"):
            run_process_batch(
                self.store,
                batch.batch_id,
                LOCAL_TWO_WORKERS,
                "probe",
                harness,
                self.attempts,
                crash_hook=crash,
            )
        result = run_process_batch(
            self.store,
            batch.batch_id,
            LOCAL_TWO_WORKERS,
            "probe",
            harness,
            self.attempts,
        )
        self.assertTrue(result.resumed)
        self.assertEqual(harness.calls, 1)
        self.assertEqual(result.publication_sequences, (1,))

    def test_partial_staging_after_crash_is_rejected(self):
        batch = self.claim((10,))

        def crash(point):
            if point == "after_staging":
                raise RuntimeError("crash")

        with self.assertRaises(RuntimeError):
            run_process_batch(
                self.store,
                batch.batch_id,
                LOCAL_TWO_WORKERS,
                "probe",
                SyntheticHarness(),
                self.attempts,
                crash_hook=crash,
            )
        with self.assertRaisesRegex(StagingConflictError, "partial"):
            run_process_batch(
                self.store,
                batch.batch_id,
                LOCAL_TWO_WORKERS,
                "probe",
                SyntheticHarness(),
                self.attempts,
            )

    def test_readback_payload_tampering_is_rejected_before_success_marker(self):
        batch = self.claim((10,))

        class TamperingAdapter(LocalJsonAttemptAdapter):
            def read_records(self, batch_id, process_attempt_id):
                records = super().read_records(batch_id, process_attempt_id)
                records[0]["record_payload_sha256"] = "0" * 64
                return records

        adapter = TamperingAdapter(self.root / "tampered-staging")
        with self.assertRaisesRegex(ProcessValidationError, "payload hash"):
            run_process_batch(
                self.store,
                batch.batch_id,
                LOCAL_TWO_WORKERS,
                "probe",
                SyntheticHarness(),
                adapter,
            )
        envelope = self.verified(batch)
        self.assertFalse(
            (
                adapter.attempt_path(batch.batch_id, envelope.execution_attempt_id)
                / "_SUCCESS"
            ).exists()
        )
        self.assertEqual(self.store.get_work("work-0").status, "LEASED")

    def test_required_provenance_schema_and_types_are_validated(self):
        batch = self.claim((10,), batch_sizes=(2,))
        envelope = self.verified(batch)
        plan = plan_duration_lpt(
            envelope.items,
            LOCAL_TWO_WORKERS,
            peak_rss_bytes=128 * 1024 * 1024,
        )
        record = SyntheticHarness().execute(
            envelope, plan, LOCAL_TWO_WORKERS, "probe"
        )[0]
        validate_staged_records([record], envelope, plan)

        required = (
            "stage_id",
            "physical_partition_id",
            "task_attempt_id",
            "task_attempt_number",
            "executor_identity",
            "executor_host",
            "release_digest",
            "config_sha256",
            "model_identity",
            "manifest_sha256",
            "attempt_id",
            "fence",
            "input_payload_sha256",
            "record_payload_sha256",
            "record_sequence",
            "bucket_id",
            "wave_index",
        )
        for name in required:
            with self.subTest(missing=name):
                malformed = dict(record)
                malformed.pop(name)
                with self.assertRaisesRegex(
                    ProcessValidationError, f"required {name}"
                ):
                    validate_staged_records([malformed], envelope, plan)

        malformed_types = {
            "stage_id": True,
            "task_attempt_id": "0",
            "task_attempt_number": 0.0,
            "executor_host": 1,
            "fence": False,
            "record_sequence": "0",
            "duration_only_fallback": 0,
        }
        for name, value in malformed_types.items():
            with self.subTest(malformed=name):
                malformed = {**record, name: value}
                with self.assertRaises(ProcessValidationError):
                    validate_staged_records([malformed], envelope, plan)

        failed = SyntheticHarness({"work-0"}).execute(
            envelope, plan, LOCAL_TWO_WORKERS, "probe"
        )[0]
        validate_staged_records([failed], envelope, plan)
        for name in ("error_type", "error_message", "retryable"):
            with self.subTest(missing_error_value=name):
                malformed = {**failed, name: None}
                with self.assertRaises(ProcessValidationError):
                    validate_staged_records([malformed], envelope, plan)

        telemetry = {
            **record,
            "record_type": "telemetry",
            "record_sequence": 1,
            "processed_frames": None,
            "processing_seconds": None,
        }
        validate_staged_records([record, telemetry], envelope, plan)
        with self.assertRaisesRegex(
            ProcessValidationError, "exactly one terminal"
        ):
            validate_staged_records([telemetry], envelope, plan)
        second_terminal = {**record, "record_sequence": 1}
        with self.assertRaisesRegex(
            ProcessValidationError, "exactly one terminal"
        ):
            validate_staged_records([record, second_terminal], envelope, plan)

        for name in ("processed_frames", "processing_seconds"):
            with self.subTest(missing_video_result_value=name):
                malformed = {**record, name: None}
                with self.assertRaises(ProcessValidationError):
                    validate_staged_records([malformed], envelope, plan)
        with self.assertRaisesRegex(
            ProcessValidationError, "detector_batch_size"
        ):
            validate_staged_records(
                [{**record, "detector_batch_size": 3}],
                envelope,
                plan,
            )
        for identity in (
            "executor@other",
            "@worker",
            "executor-worker",
            "executor@alias@worker",
        ):
            with self.subTest(executor_identity=identity):
                with self.assertRaisesRegex(
                    ProcessValidationError, "executor_identity"
                ):
                    validate_staged_records(
                        [{**record, "executor_identity": identity}],
                        envelope,
                        plan,
                    )
        for payload_json in ("{", "[]"):
            with self.subTest(payload_json=payload_json):
                with self.assertRaisesRegex(ProcessValidationError, "payload_json"):
                    validate_staged_records(
                        [{**record, "payload_json": payload_json}],
                        envelope,
                        plan,
                    )

    def test_inconsistent_plan_provenance_is_rejected_before_success_marker(self):
        batch = self.claim((10,))

        class TamperingAdapter(LocalJsonAttemptAdapter):
            def read_records(self, batch_id, process_attempt_id):
                records = super().read_records(batch_id, process_attempt_id)
                records[0]["physical_partition_id"] += 1
                return records

        adapter = TamperingAdapter(self.root / "partition-tampered-staging")
        with self.assertRaisesRegex(
            ProcessValidationError, "physical_partition_id mismatch"
        ):
            run_process_batch(
                self.store,
                batch.batch_id,
                LOCAL_TWO_WORKERS,
                "probe",
                SyntheticHarness(),
                adapter,
            )
        envelope = self.verified(batch)
        self.assertFalse(
            (
                adapter.attempt_path(batch.batch_id, envelope.execution_attempt_id)
                / "_SUCCESS"
            ).exists()
        )

    def test_missing_provenance_is_rejected_before_success_marker(self):
        batch = self.claim((10,))

        class MissingProvenanceAdapter(LocalJsonAttemptAdapter):
            def read_records(self, batch_id, process_attempt_id):
                records = super().read_records(batch_id, process_attempt_id)
                records[0].pop("stage_id")
                return records

        adapter = MissingProvenanceAdapter(self.root / "missing-provenance-staging")
        with self.assertRaisesRegex(ProcessValidationError, "required stage_id"):
            run_process_batch(
                self.store,
                batch.batch_id,
                LOCAL_TWO_WORKERS,
                "probe",
                SyntheticHarness(),
                adapter,
            )
        envelope = self.verified(batch)
        self.assertFalse(
            (
                adapter.attempt_path(batch.batch_id, envelope.execution_attempt_id)
                / "_SUCCESS"
            ).exists()
        )

    def test_cross_type_record_sequence_collision_is_rejected_before_marker(self):
        batch = self.claim((10,))

        class CollidingHarness(SyntheticHarness):
            def execute(self, envelope, plan, profile, mode):
                records = super().execute(envelope, plan, profile, mode)
                records.append(
                    {
                        **records[0],
                        "record_type": "telemetry",
                    }
                )
                return records

        with self.assertRaisesRegex(
            ProcessValidationError, "duplicate staged logical record"
        ):
            run_process_batch(
                self.store,
                batch.batch_id,
                LOCAL_TWO_WORKERS,
                "probe",
                CollidingHarness(),
                self.attempts,
            )
        envelope = self.verified(batch)
        self.assertFalse(
            (
                self.attempts.attempt_path(
                    batch.batch_id, envelope.execution_attempt_id
                )
                / "_SUCCESS"
            ).exists()
        )

    def test_recovery_revalidates_provenance_before_accepting_success_marker(self):
        batch = self.claim((10,))

        def crash(point):
            if point == "after_marker":
                raise RuntimeError("crash")

        with self.assertRaisesRegex(RuntimeError, "crash"):
            run_process_batch(
                self.store,
                batch.batch_id,
                LOCAL_TWO_WORKERS,
                "probe",
                SyntheticHarness(),
                self.attempts,
                crash_hook=crash,
            )
        envelope = self.verified(batch)
        records_path = (
            self.attempts.attempt_path(
                batch.batch_id, envelope.execution_attempt_id
            )
            / "records.json"
        )
        records = json.loads(records_path.read_text(encoding="utf-8"))
        records[0].pop("executor_host")
        records_path.write_text(json.dumps(records), encoding="utf-8")

        with self.assertRaisesRegex(
            ProcessValidationError, "required executor_host"
        ):
            run_process_batch(
                self.store,
                batch.batch_id,
                LOCAL_TWO_WORKERS,
                "probe",
                SyntheticHarness(),
                self.attempts,
            )
        self.assertEqual(self.store.get_work("work-0").status, "LEASED")

    def test_stale_fence_after_marker_never_seals_or_settles(self):
        batch = self.claim((10,))

        def stale(point):
            if point == "after_marker":
                with sqlite3.connect(self.store.database) as connection:
                    connection.execute(
                        "UPDATE work SET fence = fence + 1 WHERE work_id = 'work-0'"
                    )

        with self.assertRaises(LeaseLostError):
            run_process_batch(
                self.store,
                batch.batch_id,
                LOCAL_TWO_WORKERS,
                "probe",
                SyntheticHarness(),
                self.attempts,
                crash_hook=stale,
            )
        self.assertNotEqual(self.store.get_work("work-0").status, "SUCCEEDED")

    def test_success_marker_is_create_only_identical_or_conflict(self):
        batch = self.claim((10,))
        envelope = self.verified(batch)
        path = self.attempts.write_records(
            batch.batch_id, envelope.execution_attempt_id, []
        )
        marker = {"identity": "same"}
        self.attempts.create_success(
            batch.batch_id, envelope.execution_attempt_id, marker
        )
        self.attempts.create_success(
            batch.batch_id, envelope.execution_attempt_id, marker
        )
        self.assertTrue((path / "_SUCCESS").is_file())
        with self.assertRaises(Exception):
            self.attempts.create_success(
                batch.batch_id,
                envelope.execution_attempt_id,
                {"identity": "different"},
            )

    def test_recovered_claim_uses_a_different_attempt_scoped_path(self):
        first = self.claim((10,), lease=100)
        first_envelope = self.verified(first)
        first_path = self.attempts.attempt_path(
            first.batch_id, first_envelope.execution_attempt_id
        )
        self.clock.value = first.lease_expires_at
        self.store.recover()
        second = self.store.claim(
            "process-owner",
            max_items=1,
            lease_seconds=100,
            minimum_speed_x=1,
            safety_factor=1,
            margin_seconds=1,
        )
        second_envelope = self.verified(second)
        second_path = self.attempts.attempt_path(
            second.batch_id, second_envelope.execution_attempt_id
        )
        self.assertNotEqual(
            first_envelope.execution_attempt_id,
            second_envelope.execution_attempt_id,
        )
        self.assertNotEqual(first_path, second_path)

    def test_fabric_attempt_adapter_is_explicitly_unsupported(self):
        with self.assertRaises(UnsupportedAttemptStoreError):
            FabricAttemptAdapter()


if __name__ == "__main__":
    unittest.main()
