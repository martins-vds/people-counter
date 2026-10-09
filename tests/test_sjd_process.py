import argparse
import hashlib
import importlib.metadata
import json
import sqlite3
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import people_counter.sjd_process as process_module
from people_counter.models import LineCountRecord, RunResult
from people_counter.fabric_executor_inventory import ExecutorRecord
from people_counter.sjd_control import LeaseLostError, SQLiteControlStore
from people_counter.sjd_process import (
    LOCAL_TWO_WORKERS,
    DirectExecutionHarness,
    EnvelopeItem,
    FabricAttemptAdapter,
    LeaseAdmissionError,
    LocalJsonAttemptAdapter,
    PartitionStagingReceipt,
    ProcessValidationError,
    SparkExecutionHarness,
    StagingConflictError,
    StreamingSparkExecutionHarness,
    TaskIdentity,
    UnsupportedAttemptStoreError,
    _committed_plan,
    _model_identity,
    _package_version,
    _parser,
    _sha256,
    admit_lease,
    build_profile_from_inventory,
    conservative_concurrency,
    execute_sjd_bucket_partition,
    execute_sjd_bucket_partition_delta,
    execute_sjd_bucket_partition_streaming,
    execute_sjd_partition,
    plan_duration_lpt,
    read_staged_partition_records,
    resolve_profile,
    resume_committed_process_batch,
    run_process_batch,
    select_one_receipt_per_partition,
    sparse_line_records,
    stage_partition_records,
    validate_staged_records,
    verify_envelope,
)


def test_warm_executor_derives_model_identity_when_caller_omits_it() -> None:
    captured: dict[str, object] = {}
    budget = SimpleNamespace()

    def config_from_work(row):
        captured.update(row)
        return "config"

    with (
        patch.object(
            process_module,
            "configure_placement_safe_cpu_runtime",
            return_value=budget,
        ) as configure,
        patch.object(
            process_module,
            "verify_effective_thread_settings",
        ) as verify_settings,
        patch.object(
            process_module,
            "_config_from_work",
            side_effect=config_from_work,
        ),
        patch("people_counter.api.load_runtime", return_value="runtime"),
        patch(
            "people_counter.api.run_with_runtime",
            return_value=SimpleNamespace(processed_frames=1),
        ),
    ):
        process_module.warm_executor_for_work(
            {
                "executor_cores": 8,
                "task_cpus": 2,
                "planned_concurrency": 1,
                "pipeline": "rtdetr-osnet",
                "detector_model": "r18",
                "model_format": "pytorch",
                "device_variant": "cpu",
            }
        )

    configure.assert_called_once_with([8], 2, 4, apply_native_limits=True)
    verify_settings.assert_called_once_with(budget)
    assert captured["model_identity"] == _model_identity(captured)


def test_warm_executor_for_works_runs_every_distinct_runtime() -> None:
    works = ({"runtime": "r18"}, {"runtime": "r50"})
    with patch.object(process_module, "warm_executor_for_work") as warm:
        process_module.warm_executor_for_works(works)
    assert warm.call_args_list == [
        unittest.mock.call(works[0]),
        unittest.mock.call(works[1]),
    ]
    with unittest.TestCase().assertRaisesRegex(
        ProcessValidationError, "must not be empty"
    ):
        process_module.warm_executor_for_works(())


@pytest.mark.parametrize(
    ("executor_cores", "task_cpus", "planned_concurrency", "expected"),
    (
        (8, 1, 1, 8),
        (8, 2, 1, 4),
        (8, 4, 1, 2),
        (8, 4, 8, 8),
        (1, 2, 1, 1),
    ),
)
def test_native_thread_budget_concurrency_covers_every_scheduler_slot(
    executor_cores: int,
    task_cpus: int,
    planned_concurrency: int,
    expected: int,
) -> None:
    observed = process_module._native_thread_budget_concurrency(
        executor_cores,
        task_cpus,
        planned_concurrency,
    )
    assert type(observed) is int
    assert observed == expected


@pytest.mark.parametrize(
    "error_class",
    ("DELTA_TABLE_NOT_FOUND", "PATH_NOT_FOUND"),
)
def test_delta_stage_missing_accepts_spark_missing_path_classes(
    error_class: str,
) -> None:
    error = RuntimeError("missing")
    error.getErrorClass = lambda: error_class
    assert process_module._delta_stage_missing(error)
    assert process_module._delta_stage_missing(
        RuntimeError(f"[{error_class}] missing")
    )


def test_delta_stage_missing_rejects_other_delta_errors() -> None:
    error = RuntimeError("[DELTA_SCHEMA_MISMATCH] incompatible")
    error.getErrorClass = lambda: "DELTA_SCHEMA_MISMATCH"
    assert not process_module._delta_stage_missing(error)


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
                        "package_version": _package_version(),
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

    def test_package_version_uses_distribution_metadata_and_fails_closed(self):
        with patch(
            "people_counter.sjd_process.importlib.metadata.version",
            return_value="0.7.1",
        ) as version:
            self.assertEqual(_package_version(), "0.7.1")
        version.assert_called_once_with("people-counter")

        with patch(
            "people_counter.sjd_process.importlib.metadata.version",
            side_effect=importlib.metadata.PackageNotFoundError,
        ):
            with self.assertRaises(ProcessValidationError) as raised:
                _package_version()
        self.assertEqual(
            str(raised.exception),
            "people-counter distribution metadata is unavailable",
        )

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

    def test_build_profile_from_inventory_uses_live_discovered_executor_count(self):
        executors = (
            ExecutorRecord(
                executor_id="1",
                host="h1",
                total_cores=1,
                max_memory_bytes=1024,
            ),
            ExecutorRecord(
                executor_id="2",
                host="h2",
                total_cores=1,
                max_memory_bytes=1024,
            ),
            ExecutorRecord(
                executor_id="3",
                host="h3",
                total_cores=1,
                max_memory_bytes=1024,
            ),
        )
        profile = build_profile_from_inventory(
            "fabric-live-discovered",
            executors,
            task_cpus=1,
            expected_executor_cores=1,
            executor_memory_bytes=1024 * 1024 * 1024,
            memory_reserve_bytes=256 * 1024 * 1024,
            heartbeat_seconds=10.0,
            minimum_speed_x=1.0,
            lease_safety_factor=1.25,
            lease_margin_seconds=30.0,
        )
        self.assertEqual(profile.executor_instances, 3)
        self.assertEqual(profile.executor_cores, 1)
        self.assertEqual(profile.task_cpus, 1)
        self.assertFalse(profile.speculation)
        self.assertTrue(profile.fixed_allocation)

    def test_build_profile_from_inventory_fails_closed_on_mismatched_resources(self):
        executors = (
            ExecutorRecord(
                executor_id="1",
                host="h1",
                total_cores=2,
                max_memory_bytes=1024,
            ),
        )
        with self.assertRaises(ProcessValidationError):
            build_profile_from_inventory(
                "fabric-live-discovered",
                executors,
                task_cpus=1,
                expected_executor_cores=1,
                executor_memory_bytes=1024 * 1024 * 1024,
                memory_reserve_bytes=256 * 1024 * 1024,
                heartbeat_seconds=10.0,
                minimum_speed_x=1.0,
                lease_safety_factor=1.25,
                lease_margin_seconds=30.0,
            )

    def test_build_profile_from_inventory_rejects_empty_executors(self):
        with self.assertRaises(ProcessValidationError):
            build_profile_from_inventory(
                "fabric-live-discovered",
                (),
                task_cpus=1,
                expected_executor_cores=1,
                executor_memory_bytes=1024 * 1024 * 1024,
                memory_reserve_bytes=256 * 1024 * 1024,
                heartbeat_seconds=10.0,
                minimum_speed_x=1.0,
                lease_safety_factor=1.25,
                lease_margin_seconds=30.0,
            )

    def test_build_profile_from_inventory_rejects_empty_executors_with_the_exact_message(
        self,
    ):
        with self.assertRaises(ProcessValidationError) as error:
            build_profile_from_inventory(
                "fabric-live-discovered",
                (),
                task_cpus=1,
                expected_executor_cores=1,
                executor_memory_bytes=1024 * 1024 * 1024,
                memory_reserve_bytes=256 * 1024 * 1024,
                heartbeat_seconds=10.0,
                minimum_speed_x=1.0,
                lease_safety_factor=1.25,
                lease_margin_seconds=30.0,
            )
        self.assertEqual(str(error.exception), "executors must not be empty")

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

    def test_concurrency_profile_limit_uses_planned_task_count_not_executor_count(
        self,
    ):
        """Regression: discovered executor/core capacity must be consumable.

        A reported five executors x eight cores plans 40 one-CPU-wide
        physical partitions via `build_profile_from_inventory`; concurrency
        must be bounded by that `planned_task_count`, not collapsed back
        down to the bare `executor_instances` of 5.
        """
        executors = tuple(
            ExecutorRecord(str(i), f"h{i}", 8, 10_000_000_000) for i in range(5)
        )
        profile = build_profile_from_inventory(
            "wide-profile",
            executors,
            task_cpus=1,
            expected_executor_cores=8,
            executor_memory_bytes=10_000_000_000,
            memory_reserve_bytes=0,
            heartbeat_seconds=10.0,
            minimum_speed_x=1.0,
            lease_safety_factor=1.25,
            lease_margin_seconds=30.0,
            peak_rss_bytes=100_000_000,
        )
        self.assertEqual(profile.planned_task_count, 40)
        decision = conservative_concurrency(1000, profile, peak_rss_bytes=None)
        self.assertEqual(decision.profile_limit, 40)
        self.assertEqual(decision.cpu_limit, 40)
        self.assertEqual(decision.concurrency, 40)

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

    def test_lpt_accepts_a_workload_that_meets_the_minimum_videos_per_group(self):
        batch = self.claim((8, 7, 6, 5))
        envelope = self.verified(batch)
        plan = plan_duration_lpt(
            envelope.items,
            LOCAL_TWO_WORKERS,
            peak_rss_bytes=None,
            minimum_videos_per_group=2,
        )
        self.assertEqual(plan.concurrency.concurrency, 2)

    def test_lpt_fails_closed_when_a_group_falls_short_of_the_minimum(self):
        # One large video dominates a bucket while five small videos fill the
        # other bucket: six total videos across two buckets could amortize
        # three videos per bucket, but the dominated bucket only gets one.
        batch = self.claim((100, 1, 1, 1, 1, 1))
        envelope = self.verified(batch)
        with self.assertRaises(ProcessValidationError):
            plan_duration_lpt(
                envelope.items,
                LOCAL_TWO_WORKERS,
                peak_rss_bytes=None,
                minimum_videos_per_group=3,
            )

    def test_lpt_minimum_videos_per_group_failure_forwards_the_real_message(self):
        batch = self.claim((100, 1, 1, 1, 1, 1))
        envelope = self.verified(batch)
        with self.assertRaises(ProcessValidationError) as ctx:
            plan_duration_lpt(
                envelope.items,
                LOCAL_TWO_WORKERS,
                peak_rss_bytes=None,
                minimum_videos_per_group=3,
            )
        self.assertNotEqual(str(ctx.exception), "None")
        self.assertIn("group sizes", str(ctx.exception))

    def test_lpt_rejects_items_with_mixed_runtime_keys(self):
        items = (
            EnvelopeItem(
                ordinal=0,
                work_id="work-0",
                attempt_id="attempt-0",
                fence=1,
                payload_sha256="sha-0",
                payload={"source_video": "video-0.mp4"},
                config_sha256="config-a",
                release_digest="release-a",
                duration_seconds=10.0,
                runtime_key="runtime-a",
            ),
            EnvelopeItem(
                ordinal=1,
                work_id="work-1",
                attempt_id="attempt-1",
                fence=1,
                payload_sha256="sha-1",
                payload={"source_video": "video-1.mp4"},
                config_sha256="config-a",
                release_digest="release-a",
                duration_seconds=10.0,
                runtime_key="runtime-b",
            ),
        )
        with self.assertRaises(ProcessValidationError) as error:
            plan_duration_lpt(items, LOCAL_TWO_WORKERS, peak_rss_bytes=None)
        self.assertEqual(
            str(error.exception), "one process SJD may contain one runtime affinity"
        )

    def test_lpt_rejects_an_empty_claim_with_the_exact_message(self):
        with self.assertRaises(ProcessValidationError) as error:
            plan_duration_lpt((), LOCAL_TWO_WORKERS, peak_rss_bytes=None)
        self.assertEqual(str(error.exception), "cannot plan an empty claim")

    def test_lpt_bucket_and_wave_fields_are_populated_exactly(self):
        batch = self.claim((8, 7, 6, 5))
        envelope = self.verified(batch)
        plan = plan_duration_lpt(
            envelope.items, LOCAL_TWO_WORKERS, peak_rss_bytes=None
        )
        for bucket in plan.buckets:
            self.assertEqual(bucket.runtime_key, "runtime-a")
            self.assertEqual(
                bucket.total_cost_seconds,
                sum(entry.item.duration_seconds for entry in bucket.items),
            )
        for wave in plan.waves:
            self.assertEqual(
                wave.projected_cost_seconds,
                max(entry.planned_cost_seconds for entry in wave.items),
            )

    def test_lpt_respects_the_peak_rss_bytes_memory_constraint(self):
        batch = self.claim((8, 7, 6, 5))
        envelope = self.verified(batch)
        unconstrained = plan_duration_lpt(
            envelope.items, LOCAL_TWO_WORKERS, peak_rss_bytes=None
        )
        self.assertTrue(unconstrained.concurrency.duration_only_fallback)
        self.assertIsNone(unconstrained.concurrency.memory_limit)
        constrained = plan_duration_lpt(
            envelope.items,
            LOCAL_TWO_WORKERS,
            peak_rss_bytes=700 * 1024 * 1024,
        )
        self.assertFalse(constrained.concurrency.duration_only_fallback)
        self.assertEqual(constrained.concurrency.memory_limit, 2)
        self.assertEqual(constrained.concurrency.peak_rss_bytes, 700 * 1024 * 1024)

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
            "package_version": _package_version(),
            "runtime_key": "runtime",
            "duration_seconds": 1,
            "planned_cost_seconds": 1,
            "planned_concurrency": 1,
            "peak_rss_bytes": None,
            "duration_only_fallback": True,
            "bucket_id": 0,
            "wave_index": 0,
            "physical_partition": 0,
            "executor_cores": 8,
            "task_cpus": 1,
            "mode": "sdk",
            "_task_identity": {
                "partition_id": 0,
                "executor_identity": "executor@worker",
            },
        }

        cpu_budget = type("Budget", (), {"threads_per_worker": 1})()

        def configure(*args, **kwargs):
            self.assertTrue(kwargs["apply_native_limits"])
            events.append("cpu")
            return cpu_budget

        with (
            patch(
                "people_counter.sjd_process.configure_placement_safe_cpu_runtime",
                side_effect=configure,
            ) as configure_runtime,
            patch(
                "people_counter.sjd_process.verify_effective_thread_settings",
                return_value=None,
            ) as verify_settings,
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
        self.assertEqual(
            configure_runtime.call_args,
            unittest.mock.call([8], 1, 8, apply_native_limits=True),
        )
        verify_settings.assert_called_once_with(cpu_budget)
        self.assertEqual(records[-1]["runtime_loads"], 1)
        self.assertEqual(records[-1]["runtime_cache_hits"], 1)
        self.assertEqual(records[-1]["partition_id"], 0)
        self.assertEqual(
            {record["package_version"] for record in records},
            {_package_version()},
        )
        bad = {**row, "physical_partition": 1}
        with self.assertRaisesRegex(ProcessValidationError, "partition mismatch"):
            list(execute_sjd_partition([bad]))

    def test_execute_sjd_partition_returns_empty_for_empty_rows(self):
        self.assertEqual(list(execute_sjd_partition([])), [])

    def test_execute_sjd_partition_rejects_unreviewed_task_cpus_width(self):
        row = {
            "physical_partition": 0,
            "executor_cores": 8,
            "task_cpus": 3,
            "_task_identity": {
                "partition_id": 0,
                "executor_identity": "executor@worker",
            },
        }
        with self.assertRaisesRegex(
            ProcessValidationError, "reviewed task_cpus width"
        ):
            list(execute_sjd_partition([row]))

    def test_execute_sjd_partition_rejects_task_cpus_wider_than_executor_cores(self):
        row = {
            "physical_partition": 0,
            "executor_cores": 2,
            "task_cpus": 4,
            "_task_identity": {
                "partition_id": 0,
                "executor_identity": "executor@worker",
            },
        }
        with self.assertRaisesRegex(
            ProcessValidationError, "cannot exceed executor_cores"
        ):
            list(execute_sjd_partition([row]))

    def test_execute_sjd_partition_rejects_unsupported_mode(self):
        row = {
            "physical_partition": 0,
            "executor_cores": 4,
            "task_cpus": 1,
            "planned_concurrency": 1,
            "mode": "bogus",
            "_task_identity": {
                "partition_id": 0,
                "executor_identity": "executor@worker",
            },
        }
        with self.assertRaisesRegex(
            ProcessValidationError, "unsupported processor mode"
        ):
            list(execute_sjd_partition([row]))

    def test_direct_probe_harness_uses_executor_partition_mapping(self):
        batch = self.claim((10, 8))
        envelope = self.verified(batch)
        plan = plan_duration_lpt(
            envelope.items, LOCAL_TWO_WORKERS, peak_rss_bytes=None
        )
        budget = type("Budget", (), {"threads_per_worker": 1})()
        with patch(
            "people_counter.sjd_process.configure_placement_safe_cpu_runtime",
            return_value=budget,
        ) as configure:
            records = DirectExecutionHarness().execute(
                envelope, plan, LOCAL_TWO_WORKERS, "probe"
            )
        configure.assert_called_with([1], 1, 2, apply_native_limits=False)
        self.assertEqual(len(records), 2)
        self.assertEqual(
            {record["partition_id"] for record in records}, {0, 1}
        )
        self.assertEqual(
            {record["manifest_sha256"] for record in records},
            {envelope.envelope_sha256},
        )

    def test_spark_harness_enriches_rows_by_envelope_work_id(self):
        class FakeRDD:
            def __init__(self, values):
                self.values = values
                self.result = []

            def mapPartitions(self, function):
                for value in self.values:
                    self.result.extend(function(iter([value])))
                return self

            def collect(self):
                return self.result

        class FakeContext:
            @staticmethod
            def parallelize(values, partitions):
                self.assertEqual(partitions, 2)
                return FakeRDD(values)

        batch = self.claim((10,))
        envelope = self.verified(batch)
        plan = plan_duration_lpt(
            envelope.items, LOCAL_TWO_WORKERS, peak_rss_bytes=None
        )
        spark = type("FakeSpark", (), {"sparkContext": FakeContext()})()
        harness = SparkExecutionHarness(
            spark,
            verify_settings=False,
            row_enrichment={"work-0": {"localized_video_name": "video.mp4"}},
        )
        with patch(
            "people_counter.sjd_process.execute_sjd_partition",
            side_effect=lambda rows: rows,
        ):
            records = harness.execute(
                envelope, plan, LOCAL_TWO_WORKERS, "probe"
            )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["work_id"], "work-0")
        self.assertEqual(records[0]["localized_video_name"], "video.mp4")

    def test_stage_partition_records_writes_once_and_is_idempotent_on_retry(self):
        staging_root = self.root / "streaming-staging"
        records = [{"work_id": "work-0", "value": 1}]
        receipt = stage_partition_records(
            staging_root, 0, records, task_attempt_id=1, attempt_number=0
        )
        self.assertEqual(receipt.physical_partition, 0)
        self.assertEqual(receipt.task_attempt_id, 1)
        self.assertEqual(receipt.attempt_number, 0)
        self.assertEqual(receipt.record_count, 1)
        self.assertEqual(receipt.work_ids, ("work-0",))

        # A retried task reproducing byte-identical output must not error
        # and must return an equal receipt (idempotent rerun).
        retried = stage_partition_records(
            staging_root, 0, records, task_attempt_id=1, attempt_number=0
        )
        self.assertEqual(retried, receipt)

    def test_delta_partition_emits_attempt_qualified_records_and_receipt(self):
        rows = [
            {
                "physical_partition": 3,
                "_task_identity": {
                    "stage_id": 1,
                    "partition_id": 3,
                    "task_attempt_id": 41,
                    "attempt_number": 2,
                    "executor_identity": "executor@host",
                    "executor_host": "host",
                },
            }
        ]
        with patch.object(
            process_module,
            "execute_sjd_partition",
            return_value=[{"work_id": "work"}],
        ):
            output = list(
                execute_sjd_bucket_partition_delta(iter([{"rows": rows}]))
            )
        self.assertEqual(len(output), 2)
        self.assertEqual(output[0]["row_kind"], "record")
        self.assertEqual(output[0]["record_ordinal"], 0)
        self.assertEqual(output[0]["record_json"], '{"work_id":"work"}')
        self.assertEqual(output[1]["row_kind"], "receipt")
        self.assertEqual(output[1]["physical_partition"], 3)
        self.assertEqual(output[1]["task_attempt_id"], 41)
        self.assertEqual(output[1]["attempt_number"], 2)
        self.assertEqual(output[1]["record_count"], 1)
        self.assertEqual(output[1]["work_ids_json"], '["work"]')

        with patch.object(
            process_module,
            "execute_sjd_partition",
            return_value=[{"work_id": "work"}],
        ):
            with self.assertRaisesRegex(
                ProcessValidationError,
                "unsupported streaming staging backend: 'invalid'",
            ):
                list(
                    execute_sjd_bucket_partition_streaming(
                        "Files/stage", staging_backend="invalid"
                    )(iter([{"rows": rows}]))
                )

    def test_streaming_delta_recovers_after_stage_before_receipt(self):
        pytest.importorskip("pyspark.sql")
        pytest.importorskip("delta")
        from people_counter.local_spark import create_local_spark_session

        spark = create_local_spark_session(
            master="local[2]",
            app_name="people-counter-delta-receipt-recovery",
            correlation_id="delta-receipt-recovery",
        )
        try:
            batch = self.claim((10,))
            envelope = self.verified(batch)
            plan = plan_duration_lpt(
                envelope.items, LOCAL_TWO_WORKERS, peak_rss_bytes=None
            )
            staged = self.root / "spark-delta-streaming"
            failures = 0

            def fail_once():
                nonlocal failures
                failures += 1
                if failures == 1:
                    raise RuntimeError(
                        "injected failure after Delta stage before receipt"
                    )

            harness = StreamingSparkExecutionHarness(
                spark,
                staged,
                verify_settings=False,
                staging_backend="spark_delta",
                after_stage_hook=fail_once,
            )
            with self.assertRaisesRegex(
                RuntimeError, "after Delta stage before receipt"
            ):
                harness.execute(envelope, plan, LOCAL_TWO_WORKERS, "probe")

            history_before = (
                spark.sql(f"DESCRIBE HISTORY delta.`{staged}`").collect()
            )
            self.assertEqual(len(history_before), 1)
            records = harness.execute(
                envelope, plan, LOCAL_TWO_WORKERS, "probe"
            )
            history_after = (
                spark.sql(f"DESCRIBE HISTORY delta.`{staged}`").collect()
            )
            self.assertEqual(len(history_after), 1)
            self.assertEqual(failures, 2)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["work_id"], "work-0")
            validate_staged_records(records, envelope, plan)
        finally:
            spark.stop()

    def test_stage_partition_records_rejects_conflicting_content_for_same_partition(
        self,
    ):
        staging_root = self.root / "streaming-staging"
        stage_partition_records(
            staging_root,
            0,
            [{"work_id": "work-0", "value": 1}],
            task_attempt_id=1,
            attempt_number=0,
        )
        with self.assertRaises(StagingConflictError) as ctx:
            stage_partition_records(
                staging_root,
                0,
                [{"work_id": "work-0", "value": 2}],
                task_attempt_id=1,
                attempt_number=0,
            )
        self.assertIn("partition 0", str(ctx.exception))
        self.assertIn("different content", str(ctx.exception))

    def test_stage_partition_records_different_attempts_never_conflict(self):
        # Two distinct task attempts of the *same* physical partition (a
        # genuine retry producing non-byte-identical, but both valid,
        # output) must never raise a staging conflict: each attempt is
        # qualified by its own task_attempt_id/attempt_number path.
        staging_root = self.root / "streaming-staging"
        first = stage_partition_records(
            staging_root,
            0,
            [{"work_id": "work-0", "value": 1}],
            task_attempt_id=1,
            attempt_number=0,
        )
        second = stage_partition_records(
            staging_root,
            0,
            [{"work_id": "work-0", "value": 2}],
            task_attempt_id=2,
            attempt_number=1,
        )
        self.assertNotEqual(first.content_sha256, second.content_sha256)
        self.assertEqual(
            read_staged_partition_records(staging_root, first),
            [{"work_id": "work-0", "value": 1}],
        )
        self.assertEqual(
            read_staged_partition_records(staging_root, second),
            [{"work_id": "work-0", "value": 2}],
        )

    def test_stage_partition_records_creates_missing_nested_staging_root(self):
        # staging_root may be several directory levels below any existing
        # path on first use (e.g. a batch/attempt-scoped nested staging
        # tree); mkdir must create every missing intermediate level.
        staging_root = self.root / "nested" / "levels" / "deep" / "streaming-staging"
        self.assertFalse(staging_root.exists())
        receipt = stage_partition_records(
            staging_root,
            0,
            [{"work_id": "work-0", "value": 1}],
            task_attempt_id=1,
            attempt_number=0,
        )
        self.assertEqual(receipt.physical_partition, 0)
        self.assertTrue(staging_root.is_dir())

    def test_stage_partition_records_writes_new_content_with_exactly_utf8(self):
        # The new-content write path must pass `encoding="utf-8"` to
        # `Path.open`, not `None` (locale-dependent) or an omitted default.
        staging_root = self.root / "utf8-write"
        seen_kwargs = []
        real_open = Path.open

        def spy_open(self, *args, **kwargs):
            if args and args[0] == "x":
                seen_kwargs.append(kwargs.get("encoding"))
            return real_open(self, *args, **kwargs)

        with patch.object(Path, "open", spy_open):
            stage_partition_records(
                staging_root,
                0,
                [{"work_id": "work-0"}],
                task_attempt_id=1,
                attempt_number=0,
            )
        self.assertEqual(seen_kwargs, ["utf-8"])

    def test_stage_partition_records_reads_existing_content_with_exactly_utf8(self):
        # The idempotent-retry path reads the pre-existing file back to
        # compare content; it must use `encoding="utf-8"`, not `None`.
        staging_root = self.root / "utf8-read"
        records = [{"work_id": "work-0"}]
        stage_partition_records(
            staging_root, 0, records, task_attempt_id=1, attempt_number=0
        )
        seen_kwargs = []
        real_read_text = Path.read_text

        def spy_read_text(self, *args, **kwargs):
            seen_kwargs.append(kwargs.get("encoding"))
            return real_read_text(self, *args, **kwargs)

        with patch.object(Path, "read_text", spy_read_text):
            retried = stage_partition_records(
                staging_root, 0, records, task_attempt_id=1, attempt_number=0
            )
        self.assertEqual(seen_kwargs, ["utf-8"])
        self.assertEqual(retried.record_count, 1)

    def test_read_staged_partition_records_round_trips_and_fails_closed(self):
        staging_root = self.root / "streaming-staging"
        records = [{"work_id": "work-0", "value": 1}]
        receipt = stage_partition_records(
            staging_root, 0, records, task_attempt_id=1, attempt_number=0
        )
        read_back = read_staged_partition_records(staging_root, receipt)
        self.assertEqual(read_back, records)

        missing_receipt = PartitionStagingReceipt(
            physical_partition=1,
            task_attempt_id=1,
            attempt_number=0,
            record_count=1,
            work_ids=("work-0",),
            content_sha256=receipt.content_sha256,
        )
        with self.assertRaises(StagingConflictError) as missing_ctx:
            read_staged_partition_records(staging_root, missing_receipt)
        self.assertIn("partition 1", str(missing_ctx.exception))
        self.assertIn("missing", str(missing_ctx.exception))

        tampered_receipt = PartitionStagingReceipt(
            physical_partition=0,
            task_attempt_id=1,
            attempt_number=0,
            record_count=1,
            work_ids=("work-0",),
            content_sha256="0" * 64,
        )
        with self.assertRaises(StagingConflictError) as tampered_ctx:
            read_staged_partition_records(staging_root, tampered_receipt)
        self.assertIn("does not", str(tampered_ctx.exception))
        self.assertIn("match its receipt", str(tampered_ctx.exception))

        drifted_receipt = PartitionStagingReceipt(
            physical_partition=0,
            task_attempt_id=1,
            attempt_number=0,
            record_count=2,
            work_ids=("work-0",),
            content_sha256=receipt.content_sha256,
        )
        with self.assertRaises(StagingConflictError) as drifted_ctx:
            read_staged_partition_records(staging_root, drifted_receipt)
        self.assertIn("record count drift", str(drifted_ctx.exception))

    def test_read_staged_partition_records_rejects_tampering_with_the_exact_message(
        self,
    ):
        staging_root = self.root / "streaming-staging"
        receipt = stage_partition_records(
            staging_root,
            0,
            [{"work_id": "work-0", "value": 1}],
            task_attempt_id=1,
            attempt_number=0,
        )
        tampered_receipt = PartitionStagingReceipt(
            physical_partition=0,
            task_attempt_id=1,
            attempt_number=0,
            record_count=1,
            work_ids=("work-0",),
            content_sha256="0" * 64,
        )
        with self.assertRaises(StagingConflictError) as error:
            read_staged_partition_records(staging_root, tampered_receipt)
        self.assertEqual(
            str(error.exception),
            "partition 0 staged content does not match its receipt",
        )

    def test_read_staged_partition_records_reads_with_exactly_utf8(self):
        staging_root = self.root / "streaming-staging"
        receipt = stage_partition_records(
            staging_root,
            0,
            [{"work_id": "work-0", "value": 1}],
            task_attempt_id=1,
            attempt_number=0,
        )
        seen_kwargs = []
        real_read_text = Path.read_text

        def spy_read_text(self, *args, **kwargs):
            seen_kwargs.append(kwargs.get("encoding"))
            return real_read_text(self, *args, **kwargs)

        with patch.object(Path, "read_text", spy_read_text):
            read_staged_partition_records(staging_root, receipt)
        self.assertEqual(seen_kwargs, ["utf-8"])

    def test_execute_sjd_bucket_partition_streaming_stages_and_yields_one_receipt(
        self,
    ):
        staging_root = self.root / "streaming-staging"
        rows = [
            {
                "physical_partition": 0,
                "work_id": "work-0",
                "_task_identity": {
                    "partition_id": 0,
                    "task_attempt_id": 7,
                    "attempt_number": 0,
                    "executor_identity": "executor@worker",
                },
            }
        ]
        with patch(
            "people_counter.sjd_process.execute_sjd_partition",
            return_value=[{"work_id": "work-0", "record": "ok"}],
        ) as executor:
            run = execute_sjd_bucket_partition_streaming(str(staging_root))
            receipts = list(run([{"rows": rows}]))
        executor.assert_called_once_with(rows)
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]["physical_partition"], 0)
        self.assertEqual(receipts[0]["task_attempt_id"], 7)
        self.assertEqual(receipts[0]["attempt_number"], 0)
        self.assertEqual(receipts[0]["record_count"], 1)
        self.assertEqual(receipts[0]["work_ids"], ("work-0",))
        staged = read_staged_partition_records(
            staging_root,
            PartitionStagingReceipt(**receipts[0]),
        )
        self.assertEqual(staged, [{"work_id": "work-0", "record": "ok"}])

    def test_execute_sjd_bucket_partition_streaming_handles_empty_and_none(self):
        run = execute_sjd_bucket_partition_streaming(str(self.root / "unused"))
        self.assertEqual(list(run([])), [])
        self.assertEqual(list(run([None])), [])
        with self.assertRaisesRegex(ProcessValidationError, "exactly one bucket"):
            list(run([{"rows": []}, {"rows": []}]))

    def test_execute_sjd_bucket_partition_streaming_rejects_multi_bucket_with_the_exact_message(
        self,
    ):
        run = execute_sjd_bucket_partition_streaming(str(self.root / "unused"))
        with self.assertRaises(ProcessValidationError) as error:
            list(run([{"rows": []}, {"rows": []}]))
        self.assertEqual(
            str(error.exception),
            "each physical Spark partition must contain exactly one bucket",
        )

    def test_select_one_receipt_per_partition_picks_latest_equivalent_attempt(self):
        earlier = PartitionStagingReceipt(
            physical_partition=0,
            task_attempt_id=5,
            attempt_number=0,
            record_count=1,
            work_ids=("work-0",),
            content_sha256="a" * 64,
        )
        winner = PartitionStagingReceipt(
            physical_partition=0,
            task_attempt_id=9,
            attempt_number=1,
            record_count=1,
            work_ids=("work-0",),
            content_sha256="a" * 64,
        )
        other_partition = PartitionStagingReceipt(
            physical_partition=1,
            task_attempt_id=3,
            attempt_number=0,
            record_count=1,
            work_ids=("work-1",),
            content_sha256="c" * 64,
        )
        selected = select_one_receipt_per_partition(
            [winner, earlier, other_partition]
        )
        self.assertEqual(selected, [winner, other_partition])

    def test_select_one_receipt_per_partition_breaks_ties_by_latest_task_attempt_id(
        self,
    ):
        lower_task_attempt = PartitionStagingReceipt(
            physical_partition=0,
            task_attempt_id=1,
            attempt_number=0,
            record_count=1,
            work_ids=("work-0",),
            content_sha256="a" * 64,
        )
        higher_task_attempt = PartitionStagingReceipt(
            physical_partition=0,
            task_attempt_id=2,
            attempt_number=0,
            record_count=1,
            work_ids=("work-0",),
            content_sha256="a" * 64,
        )
        selected = select_one_receipt_per_partition(
            [higher_task_attempt, lower_task_attempt]
        )
        self.assertEqual(selected, [higher_task_attempt])

    def test_select_one_receipt_per_partition_rejects_conflicting_attempts(self):
        first = PartitionStagingReceipt(
            physical_partition=0,
            task_attempt_id=1,
            attempt_number=0,
            record_count=1,
            work_ids=("work-0",),
            content_sha256="a" * 64,
        )
        conflicting = PartitionStagingReceipt(
            physical_partition=0,
            task_attempt_id=2,
            attempt_number=1,
            record_count=1,
            work_ids=("work-0",),
            content_sha256="b" * 64,
        )
        with self.assertRaises(StagingConflictError) as error:
            select_one_receipt_per_partition([first, conflicting])
        self.assertEqual(
            str(error.exception),
            "successful attempts produced conflicting output for physical "
            "partition 0",
        )

    def test_streaming_spark_harness_rejects_conflicting_duplicate_receipt(
        self,
    ):
        # With speculation disabled, more than one receipt for the same
        # physical partition should never occur in ordinary operation, but
        # correctness cannot depend on that: simulate a zombie retried
        # task's receipt arriving alongside a successor with different
        # output and prove the driver fails closed before reading either.
        batch = self.claim((10,))
        envelope = self.verified(batch)
        plan = plan_duration_lpt(
            envelope.items, LOCAL_TWO_WORKERS, peak_rss_bytes=None
        )
        staging_root = self.root / "streaming-duplicate"

        # Pre-stage a zombie earlier attempt's receipt directly, as if an
        # earlier, since-retried task had already written its own staged
        # output for the same physical partition before being killed.
        zombie_receipt = stage_partition_records(
            staging_root,
            0,
            [{"work_id": "work-0", "record": "zombie-should-be-ignored"}],
            task_attempt_id=1,
            attempt_number=0,
        )
        real_receipt = stage_partition_records(
            staging_root,
            0,
            [{"work_id": "work-0", "record": "winner"}],
            task_attempt_id=2,
            attempt_number=1,
        )

        class FakeRDD:
            def __init__(self, rows):
                self.rows = rows

            def mapPartitions(self, function):
                return self

            def collect(self):
                # Both the real winning attempt's receipt and the zombie's
                # arrive together, exactly as Spark's own `collect()` could
                # surface them if a retried task's late output were ever
                # observed alongside its successor.
                return [asdict(real_receipt), asdict(zombie_receipt)]

        class FakeContext:
            @staticmethod
            def parallelize(values, partitions):
                return FakeRDD(values)

        spark = type("FakeSpark", (), {"sparkContext": FakeContext()})()
        harness = StreamingSparkExecutionHarness(
            spark, staging_root, verify_settings=False
        )
        real_read = read_staged_partition_records
        read_calls = []

        def spy_read(root, receipt):
            read_calls.append(receipt)
            return real_read(root, receipt)

        with patch(
            "people_counter.sjd_process.read_staged_partition_records",
            side_effect=spy_read,
        ), self.assertRaisesRegex(
            StagingConflictError,
            "successful attempts produced conflicting output",
        ):
            harness.execute(envelope, plan, LOCAL_TWO_WORKERS, "probe")

        self.assertEqual(read_calls, [])

    def test_streaming_spark_harness_different_attempts_of_the_same_partition_never_conflict(
        self,
    ):
        # Stage a stray/orphaned earlier attempt's output directly (as if a
        # task crashed after staging but before its receipt was collected),
        # then run the harness for a *later* attempt of the same partition
        # producing different content: the attempt-qualified staging paths
        # must mean this never raises a staging conflict, and the harness's
        # own (later) attempt's receipt is the one actually returned.
        class FakeRDD:
            def __init__(self, values):
                self.values = values
                self.result: list[dict] = []

            def mapPartitions(self, function):
                for value in self.values:
                    self.result.extend(function(iter([value])))
                return self

            def collect(self):
                return self.result

        class FakeContext:
            @staticmethod
            def parallelize(values, partitions):
                return FakeRDD(values)

        batch = self.claim((10,))
        envelope = self.verified(batch)
        plan = plan_duration_lpt(
            envelope.items, LOCAL_TWO_WORKERS, peak_rss_bytes=None
        )
        spark = type("FakeSpark", (), {"sparkContext": FakeContext()})()
        staging_root = self.root / "streaming-stray-retry"
        harness = StreamingSparkExecutionHarness(
            spark, staging_root, verify_settings=False
        )

        # The stray, orphaned attempt-0 output: never read back by the
        # harness below, since Spark's own retry gave this partition a new
        # (attempt_number=1) identity for its real, collected receipt.
        stage_partition_records(
            staging_root,
            0,
            [{"work_id": "work-0", "record": "stray-orphan"}],
            task_attempt_id=1,
            attempt_number=0,
        )

        with patch(
            "people_counter.sjd_process.execute_sjd_partition",
            side_effect=lambda rows: [
                {**row, "record_type": "video_result"} for row in rows
            ],
        ), patch(
            "people_counter.sjd_process._task_identity",
            return_value=TaskIdentity(
                stage_id=0,
                partition_id=0,
                task_attempt_id=2,
                attempt_number=1,
                executor_identity="executor@worker",
                executor_host="worker",
            ),
        ):
            records = harness.execute(envelope, plan, LOCAL_TWO_WORKERS, "probe")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["work_id"], "work-0")
        self.assertNotEqual(records[0].get("record"), "stray-orphan")

    def test_streaming_spark_harness_missing_receipt_preserves_a_valid_sibling(
        self,
    ):
        # When one partition in a group never produces a receipt (its
        # executor died mid-task) while a sibling partition's receipt was
        # already staged successfully, the aggregate failure must not
        # destroy or invalidate the sibling's already-staged, valid output:
        # it must remain intact and independently re-readable afterwards.
        class FakeRDD:
            def __init__(self, values):
                self.values = values
                self.result: list[dict] = []

            def mapPartitions(self, function):
                for value in self.values:
                    if value is None:
                        continue
                    self.result.extend(function(iter([value])))
                return self

            def collect(self):
                return self.result

        class FakeContext:
            @staticmethod
            def parallelize(values, partitions):
                return FakeRDD(values)

        batch = self.claim((10, 8))
        envelope = self.verified(batch)
        plan = plan_duration_lpt(
            envelope.items, LOCAL_TWO_WORKERS, peak_rss_bytes=None
        )
        spark = type("FakeSpark", (), {"sparkContext": FakeContext()})()
        staging_root = self.root / "streaming-partial-sibling"
        harness = StreamingSparkExecutionHarness(
            spark, staging_root, verify_settings=False
        )

        def fake_execute_sjd_partition(rows):
            if rows[0]["physical_partition"] == 1:
                raise RuntimeError("executor died mid-task for partition 1")
            return [{**row, "record_type": "video_result"} for row in rows]

        with patch(
            "people_counter.sjd_process.execute_sjd_partition",
            side_effect=fake_execute_sjd_partition,
        ), patch(
            "people_counter.sjd_process._task_identity",
            side_effect=lambda row: TaskIdentity(
                stage_id=0,
                partition_id=row["physical_partition"],
                task_attempt_id=row["physical_partition"],
                attempt_number=0,
                executor_identity="executor@worker",
                executor_host="worker",
            ),
        ), self.assertRaises(RuntimeError):
            harness.execute(envelope, plan, LOCAL_TWO_WORKERS, "probe")

        # Partition 0's receipt was staged before partition 1's task died;
        # its content must still be present and verifiable on disk.
        staged_files = sorted(
            staging_root.glob("partition-0000/**/records.jsonl")
        )
        self.assertEqual(len(staged_files), 1)
        self.assertTrue(staged_files[0].is_file())
        # And it must still read back cleanly -- the partial batch
        # failure did not corrupt or truncate the sibling's valid output.
        staged_rows = json.loads(staged_files[0].read_text(encoding="utf-8"))
        self.assertEqual(len(staged_rows), 1)
        self.assertEqual(staged_rows[0]["work_id"], "work-0")
        self.assertEqual(staged_rows[0]["record_type"], "video_result")

    def test_streaming_spark_harness_stages_rows_and_returns_all_records(self):
        class FakeRDD:
            def __init__(self, values):

                self.values = values
                self.result = []

            def mapPartitions(self, function):
                for value in self.values:
                    self.result.extend(function(iter([value])))
                return self

            def collect(self):
                return self.result

        class FakeContext:
            def __init__(self):
                self.parallelize_calls = []

            def parallelize(self, values, partitions):
                self.parallelize_calls.append(partitions)
                return FakeRDD(values)

        batch = self.claim((10, 8))
        envelope = self.verified(batch)
        plan = plan_duration_lpt(
            envelope.items, LOCAL_TWO_WORKERS, peak_rss_bytes=None
        )
        fake_context = FakeContext()
        spark = type("FakeSpark", (), {"sparkContext": fake_context})()
        staging_root = self.root / "streaming-harness"
        harness = StreamingSparkExecutionHarness(
            spark,
            staging_root,
            verify_settings=False,
            row_enrichment={"work-0": {"enrichment_flag": "present"}},
        )
        with patch(
            "people_counter.sjd_process.execute_sjd_partition",
            side_effect=lambda rows: [
                {**row, "record_type": "video_result"} for row in rows
            ],
        ), patch(
            "people_counter.sjd_process._task_identity",
            side_effect=lambda row: TaskIdentity(
                stage_id=0,
                partition_id=row["physical_partition"],
                task_attempt_id=row["physical_partition"],
                attempt_number=0,
                executor_identity="executor@worker",
                executor_host="worker",
            ),
        ):
            records = harness.execute(envelope, plan, LOCAL_TWO_WORKERS, "probe")
        self.assertEqual(
            {record["work_id"] for record in records}, {"work-0", "work-1"}
        )
        # The full row payload must have actually been written to immutable
        # staging rather than only existing in the collected receipt list.
        staged_files = sorted(
            (staging_root).glob("partition-*/task-attempt-*/attempt-*/records.jsonl")
        )
        self.assertEqual(len(staged_files), 2)
        # Every row must carry the actual requested mode, not a dropped one.
        self.assertTrue(all(record["mode"] == "probe" for record in records))
        # row_enrichment is applied per-work_id, not to a fixed/None key.
        by_work_id = {record["work_id"]: record for record in records}
        self.assertEqual(
            by_work_id["work-0"]["enrichment_flag"], "present"
        )
        self.assertNotIn("enrichment_flag", by_work_id["work-1"])
        # The Spark partition count passed to parallelize must reflect the
        # profile's actual physical partition count, not a dropped None.
        self.assertEqual(
            fake_context.parallelize_calls,
            [len(LOCAL_TWO_WORKERS.physical_partitions)],
        )

    def test_streaming_spark_harness_verifies_live_settings_by_default(self):
        class FakeRDD:
            def mapPartitions(self, function):
                return self

            def collect(self):
                return []

        class FakeContext:
            @staticmethod
            def parallelize(values, partitions):
                return FakeRDD()

        class FakeConf:
            def __init__(self, overrides):
                self.overrides = overrides

            def get(self, key, default):
                return self.overrides.get(key, default)

        batch = self.claim((10,))
        envelope = self.verified(batch)
        plan = plan_duration_lpt(
            envelope.items, LOCAL_TWO_WORKERS, peak_rss_bytes=None
        )

        # A live Spark config that matches the fixed profile: verification
        # must run (by default, with no verify_settings kwarg at all) and
        # must not raise.
        matching_spark = type(
            "FakeSpark",
            (),
            {"sparkContext": FakeContext(), "conf": FakeConf({})},
        )()
        harness = StreamingSparkExecutionHarness(
            matching_spark, self.root / "streaming-verify-match"
        )
        with self.assertRaises(StagingConflictError):
            # Fails on the (expected, empty-receipt) staging check, proving
            # settings verification itself passed without raising first.
            harness.execute(envelope, plan, LOCAL_TWO_WORKERS, "probe")

        # A live Spark config that conflicts with the fixed profile: default
        # verification (no explicit verify_settings kwarg) must detect it.
        conflicting_key = next(iter(LOCAL_TWO_WORKERS.spark_settings))
        conflicting_spark = type(
            "FakeSpark",
            (),
            {
                "sparkContext": FakeContext(),
                "conf": FakeConf({conflicting_key: "definitely-not-a-real-value"}),
            },
        )()
        harness_default = StreamingSparkExecutionHarness(
            conflicting_spark, self.root / "streaming-verify-default"
        )
        # NOTE: StagingConflictError is itself a ProcessValidationError
        # subclass, so asserting the base class alone would not distinguish
        # "verification ran and caught the conflict" from "verification was
        # skipped and the (unrelated) empty-receipt staging check tripped
        # instead". Assert the exact exception type and its distinguishing
        # message content to really prove settings verification executed.
        with self.assertRaises(ProcessValidationError) as default_ctx:
            harness_default.execute(envelope, plan, LOCAL_TWO_WORKERS, "probe")
        self.assertIs(type(default_ctx.exception), ProcessValidationError)
        self.assertIn(
            "live Spark configuration conflicts", str(default_ctx.exception)
        )

        # Same conflicting config with verify_settings explicitly True must
        # also detect it (proving the ctor stores the real argument, not an
        # always-None/falsy placeholder).
        harness_explicit = StreamingSparkExecutionHarness(
            conflicting_spark,
            self.root / "streaming-verify-explicit",
            verify_settings=True,
        )
        with self.assertRaises(ProcessValidationError) as explicit_ctx:
            harness_explicit.execute(envelope, plan, LOCAL_TWO_WORKERS, "probe")
        self.assertIs(type(explicit_ctx.exception), ProcessValidationError)
        self.assertIn(
            "live Spark configuration conflicts", str(explicit_ctx.exception)
        )


    def test_streaming_spark_harness_fails_closed_on_missing_receipt(self):
        class FakeRDD:
            def __init__(self, values):
                self.values = values

            def mapPartitions(self, function):
                return self

            def collect(self):
                return []

        class FakeContext:
            @staticmethod
            def parallelize(values, partitions):
                return FakeRDD(values)

        batch = self.claim((10,))
        envelope = self.verified(batch)
        plan = plan_duration_lpt(
            envelope.items, LOCAL_TWO_WORKERS, peak_rss_bytes=None
        )
        spark = type("FakeSpark", (), {"sparkContext": FakeContext()})()
        harness = StreamingSparkExecutionHarness(
            spark, self.root / "streaming-missing", verify_settings=False
        )
        with self.assertRaises(StagingConflictError) as ctx:
            harness.execute(envelope, plan, LOCAL_TWO_WORKERS, "probe")
        self.assertIn("did not receive a receipt", str(ctx.exception))
        self.assertIn("expected", str(ctx.exception))

    def test_streaming_spark_harness_missing_receipt_reports_the_exact_message(self):
        class FakeRDD:
            def __init__(self, values):
                self.values = values

            def mapPartitions(self, function):
                return self

            def collect(self):
                return []

        class FakeContext:
            @staticmethod
            def parallelize(values, partitions):
                return FakeRDD(values)

        batch = self.claim((10,))
        envelope = self.verified(batch)
        plan = plan_duration_lpt(
            envelope.items, LOCAL_TWO_WORKERS, peak_rss_bytes=None
        )
        spark = type("FakeSpark", (), {"sparkContext": FakeContext()})()
        harness = StreamingSparkExecutionHarness(
            spark, self.root / "streaming-missing-exact", verify_settings=False
        )
        with self.assertRaises(StagingConflictError) as ctx:
            harness.execute(envelope, plan, LOCAL_TWO_WORKERS, "probe")
        self.assertEqual(
            str(ctx.exception),
            "streaming harness did not receive a receipt for every planned "
            "bucket: expected [0], observed []",
        )

    def test_streaming_spark_harness_preserves_the_receipt_work_ids(self):
        # The receipt's `work_ids` field must be reconstructed from the
        # executor row's actual `work_ids`, not dropped to `None`: verify
        # by capturing the receipt object actually handed to the
        # read-back step.
        class FakeRDD:
            def __init__(self, values):
                self.values = values
                self.result: list[dict] = []

            def mapPartitions(self, function):
                for value in self.values:
                    self.result.extend(function(iter([value])))
                return self

            def collect(self):
                return self.result

        class FakeContext:
            @staticmethod
            def parallelize(values, partitions):
                return FakeRDD(values)

        batch = self.claim((10,))
        envelope = self.verified(batch)
        plan = plan_duration_lpt(
            envelope.items, LOCAL_TWO_WORKERS, peak_rss_bytes=None
        )
        spark = type("FakeSpark", (), {"sparkContext": FakeContext()})()
        harness = StreamingSparkExecutionHarness(
            spark, self.root / "streaming-work-ids", verify_settings=False
        )
        captured_receipts = []
        real_read = read_staged_partition_records

        def spy_read(staging_root, receipt):
            captured_receipts.append(receipt)
            return real_read(staging_root, receipt)

        with patch(
            "people_counter.sjd_process.execute_sjd_partition",
            side_effect=lambda rows: [
                {**row, "record_type": "video_result"} for row in rows
            ],
        ), patch(
            "people_counter.sjd_process.read_staged_partition_records",
            side_effect=spy_read,
        ), patch(
            "people_counter.sjd_process._task_identity",
            side_effect=lambda row: TaskIdentity(
                stage_id=0,
                partition_id=row["physical_partition"],
                task_attempt_id=row["physical_partition"],
                attempt_number=0,
                executor_identity="executor@worker",
                executor_host="worker",
            ),
        ):
            harness.execute(envelope, plan, LOCAL_TWO_WORKERS, "probe")
        self.assertTrue(captured_receipts)
        self.assertTrue(
            all(
                isinstance(receipt.work_ids, tuple) and receipt.work_ids
                for receipt in captured_receipts
            )
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

    def test_committed_resume_reconstructs_plan_without_live_profile(self):
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

        resumed = resume_committed_process_batch(
            self.store,
            batch.batch_id,
            self.attempts,
        )

        self.assertIsNotNone(resumed)
        assert resumed is not None
        self.assertEqual(first.publication_sequences, resumed.publication_sequences)
        self.assertTrue(resumed.resumed)
        self.assertEqual(harness.calls, 1)

    def test_committed_resume_returns_none_for_uncommitted_batch(self):
        batch = self.claim((10,))

        result = resume_committed_process_batch(
            self.store,
            batch.batch_id,
            self.attempts,
        )

        self.assertIsNone(result)

    def test_committed_preflight_rejects_expired_lease(self):
        batch = self.claim((10,), lease=20)
        self.clock.value += 21

        with self.assertRaisesRegex(
            LeaseLostError,
            "does not own a live fence",
        ):
            resume_committed_process_batch(
                self.store,
                batch.batch_id,
                self.attempts,
            )

    def test_committed_resume_preserves_measured_rss_plan(self):
        batch = self.claim((10, 8))
        peak_rss_bytes = 768 * 1024 * 1024
        first = run_process_batch(
            self.store,
            batch.batch_id,
            LOCAL_TWO_WORKERS,
            "probe",
            SyntheticHarness(),
            self.attempts,
            peak_rss_bytes=peak_rss_bytes,
        )

        resumed = resume_committed_process_batch(
            self.store,
            batch.batch_id,
            self.attempts,
        )

        self.assertIsNotNone(resumed)
        assert resumed is not None
        self.assertEqual(first.publication_sequences, resumed.publication_sequences)

    def test_committed_plan_rejects_missing_staged_records(self):
        batch = self.claim((10,))

        with self.assertRaisesRegex(
            StagingConflictError,
            "^committed batch has no staged records$",
        ):
            _committed_plan([], self.verified(batch))

    def test_committed_plan_rejects_inconsistent_concurrency_evidence(self):
        batch = self.claim((10, 8))
        run_process_batch(
            self.store,
            batch.batch_id,
            LOCAL_TWO_WORKERS,
            "probe",
            SyntheticHarness(),
            self.attempts,
        )
        complete = self.attempts.load_complete(
            batch.batch_id,
            self.verified(batch).execution_attempt_id,
        )
        assert complete is not None
        records, _ = complete
        records[0]["planned_concurrency"] = 1

        with self.assertRaisesRegex(
            StagingConflictError,
            "^committed staging has inconsistent concurrency evidence$",
        ):
            _committed_plan(records, self.verified(batch))

    def test_committed_plan_rejects_inconsistent_rss_fallback_evidence(self):
        batch = self.claim((10,))
        run_process_batch(
            self.store,
            batch.batch_id,
            LOCAL_TWO_WORKERS,
            "probe",
            SyntheticHarness(),
            self.attempts,
        )
        complete = self.attempts.load_complete(
            batch.batch_id,
            self.verified(batch).execution_attempt_id,
        )
        assert complete is not None
        records, _ = complete
        records[0]["duration_only_fallback"] = False

        with self.assertRaisesRegex(
            StagingConflictError,
            "^committed staging has inconsistent RSS fallback evidence$",
        ):
            _committed_plan(records, self.verified(batch))

    def test_committed_resume_requires_complete_staging(self):
        batch = self.claim((10,))
        run_process_batch(
            self.store,
            batch.batch_id,
            LOCAL_TWO_WORKERS,
            "probe",
            SyntheticHarness(),
            self.attempts,
        )
        attempts = unittest.mock.MagicMock()
        attempts.load_complete.return_value = None

        with self.assertRaisesRegex(
            StagingConflictError,
            "^committed batch is missing complete staging$",
        ):
            resume_committed_process_batch(
                self.store,
                batch.batch_id,
                attempts,
            )

    def test_committed_preflight_validates_release_before_state_short_circuit(self):
        batch = self.claim((10,))
        evidence = SimpleNamespace(
            manifest=SimpleNamespace(package_version="release-version"),
            manifest_sha256="release-a",
        )

        with patch(
            "people_counter.fabric_release_provenance."
            "validate_installed_package_version"
        ) as validate_version:
            result = resume_committed_process_batch(
                self.store,
                batch.batch_id,
                self.attempts,
                release_evidence=evidence,
            )

        self.assertIsNone(result)
        validate_version.assert_called_once_with(
            expected_version="release-version",
        )

    def test_committed_preflight_rejects_release_digest_mismatch(self):
        batch = self.claim((10,))
        evidence = SimpleNamespace(
            manifest=SimpleNamespace(package_version="release-version"),
            manifest_sha256="different-release",
        )

        with (
            patch(
                "people_counter.fabric_release_provenance."
                "validate_installed_package_version"
            ),
            self.assertRaisesRegex(
                ProcessValidationError,
                "^claim release digest differs from detached manifest "
                "for \\['work-0'\\]$",
            ),
        ):
            resume_committed_process_batch(
                self.store,
                batch.batch_id,
                self.attempts,
                release_evidence=evidence,
            )

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
        self.assertEqual(result.driver_package_version, _package_version())
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
        for name in ("work_id", "attempt_id"):
            with self.subTest(invalid_identity_field=name):
                with self.assertRaises(ProcessValidationError) as raised:
                    validate_staged_records(
                        [{**record, name: None}],
                        envelope,
                        plan,
                    )
                self.assertEqual(
                    str(raised.exception),
                    f"{name} must be a non-empty string",
                )
        with self.assertRaises(ProcessValidationError) as raised:
            validate_staged_records(
                [{**record, "work_id": "unexpected"}],
                envelope,
                plan,
            )
        self.assertEqual(
            str(raised.exception),
            "unexpected staged identity: "
            f"('unexpected', {record['attempt_id']!r})",
        )

        required = (
            "stage_id",
            "physical_partition_id",
            "task_attempt_id",
            "task_attempt_number",
            "executor_identity",
            "executor_host",
            "release_digest",
            "package_version",
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
        with self.assertRaisesRegex(
            ProcessValidationError, "package_version mismatch"
        ):
            validate_staged_records(
                [{**record, "package_version": "0.0.0"}],
                envelope,
                plan,
            )

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
