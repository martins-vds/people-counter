import hashlib
import json
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from people_counter.config import RFDetrBotsortConfig, RTDetrOsnetConfig
from people_counter.fabric_dispatch import (
    EXECUTOR_PARTITION,
    NOTEBOOK_04,
    compatible_queue_engine,
    normalize_processing_engine,
    validate_immutable_processing_engine,
)
from people_counter.fabric_executor_partition import (
    ExecutorRuntimeCache,
    SdkRuntimeProcessor,
    _runtime_cache_key,
    executor_partition_schema,
    process_video_partition,
)
from people_counter.fabric_executor_production import (
    DriverAttemptStates,
    ExecutorPreflightError,
    LeaseSafetyControls,
    StagingConflictError,
    parse_claimed_items,
    plan_largest_cost_first,
    process_production_partition,
    require_configured_lease_budget,
    require_live_wave_budget,
    select_production_line_counts,
    staged_executor_source,
    staging_transaction,
    validate_claimed_rows,
    validate_staging_records,
)
from people_counter.models import LineCountRecord, PersonTelemetry, RunResult


class FabricDispatchTests(unittest.TestCase):
    def test_processing_engine_defaults_and_normalizes(self):
        self.assertEqual(normalize_processing_engine(None), NOTEBOOK_04)
        self.assertEqual(
            normalize_processing_engine(" executor_partition "),
            EXECUTOR_PARTITION,
        )
        self.assertEqual(compatible_queue_engine(None), NOTEBOOK_04)

    def test_processing_engine_rejects_blank_unknown_and_non_string_values(self):
        for value in ("", "other", 4):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "processing_engine"):
                    normalize_processing_engine(value)
        with self.assertRaisesRegex(ValueError, "processing_engine is required"):
            normalize_processing_engine(None, default=None)

    def test_existing_work_cannot_change_processing_engine(self):
        self.assertEqual(
            validate_immutable_processing_engine(None, "notebook_04"),
            NOTEBOOK_04,
        )
        with self.assertRaisesRegex(ValueError, "conflicting processing_engine"):
            validate_immutable_processing_engine(
                NOTEBOOK_04,
                EXECUTOR_PARTITION,
            )


class FabricExecutorProductionTests(unittest.TestCase):
    @staticmethod
    def queue_row(work_id="work-a", attempt_id="attempt-a", **updates):
        row = {
            "work_id": work_id,
            "lease_owner_attempt_id": attempt_id,
            "lease_dispatcher_id": "dispatcher-a",
            "lease_expires_at": datetime.now(timezone.utc) + timedelta(hours=1),
            "status": "LEASED",
            "processing_engine": EXECUTOR_PARTITION,
            "duration_seconds": 120.0,
            "runtime_sha256": "runtime-a",
            "config_sha256": "config-a",
        }
        row.update(updates)
        return row

    def test_parse_claimed_items_bounds_and_rejects_duplicates(self):
        items = parse_claimed_items(
            '[{"work_id":"work-a","attempt_id":"attempt-a"}]',
            1,
        )
        self.assertEqual(
            items,
            [{"work_id": "work-a", "attempt_id": "attempt-a"}],
        )
        self.assertEqual(parse_claimed_items("[]", 1), [])
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            parse_claimed_items(
                [
                    {"work_id": "work-a", "attempt_id": "attempt-a"},
                    {"work_id": "work-a", "attempt_id": "attempt-a"},
                ],
                2,
            )
        with self.assertRaisesRegex(ValueError, "maximum is 1"):
            parse_claimed_items(
                [
                    {"work_id": "work-a", "attempt_id": "attempt-a"},
                    {"work_id": "work-b", "attempt_id": "attempt-b"},
                ],
                1,
            )

    def test_validate_claimed_rows_enforces_engine_lease_and_runtime(self):
        items = [{"work_id": "work-a", "attempt_id": "attempt-a"}]
        rows = validate_claimed_rows(
            items,
            [self.queue_row()],
            dispatcher_id="dispatcher-a",
        )
        self.assertEqual(rows[0]["duration_seconds"], 120.0)

        invalid_rows = (
            self.queue_row(processing_engine=NOTEBOOK_04),
            self.queue_row(lease_dispatcher_id="dispatcher-b"),
            self.queue_row(duration_seconds=None),
        )
        for row in invalid_rows:
            with self.subTest(row=row):
                with self.assertRaises((ValueError, ExecutorPreflightError)):
                    validate_claimed_rows(
                        items,
                        [row],
                        dispatcher_id="dispatcher-a",
                    )

        with self.assertRaisesRegex(
            ExecutorPreflightError,
            "exactly one compatible model runtime",
        ):
            validate_claimed_rows(
                [
                    {"work_id": "work-a", "attempt_id": "attempt-a"},
                    {"work_id": "work-b", "attempt_id": "attempt-b"},
                ],
                [
                    self.queue_row(),
                    self.queue_row(
                        "work-b",
                        "attempt-b",
                        runtime_sha256="runtime-b",
                    ),
                ],
                dispatcher_id="dispatcher-a",
            )

    def test_lease_safety_rejects_unsafe_configured_and_live_budgets(self):
        controls = LeaseSafetyControls.create(2.0, 1.5, 120, 60)
        self.assertEqual(controls.projected_wall_seconds(120), 90.0)
        self.assertEqual(
            require_configured_lease_budget(
                [{"duration_seconds": 120}],
                controls,
                5,
            ),
            [90.0],
        )
        with self.assertRaisesRegex(
            ExecutorPreflightError,
            "single-video wall time",
        ):
            require_configured_lease_budget(
                [{"duration_seconds": 240}],
                controls,
                5,
            )

        now = datetime(2026, 10, 1, tzinfo=timezone.utc)
        self.assertEqual(
            require_live_wave_budget(
                60,
                [now + timedelta(minutes=5)],
                margin_seconds=120,
                now=now,
            ),
            now + timedelta(seconds=60),
        )
        with self.assertRaisesRegex(ExecutorPreflightError, "live lease margin"):
            require_live_wave_budget(
                180,
                [now + timedelta(minutes=5)],
                margin_seconds=120,
                now=now,
            )

    def test_lease_safety_requires_conservative_values(self):
        with self.assertRaisesRegex(ValueError, "at least 1.0"):
            LeaseSafetyControls.create(2.0, 0.9, 120, 60)
        with self.assertRaisesRegex(ValueError, "two heartbeat"):
            LeaseSafetyControls.create(2.0, 1.0, 119, 60)

    def test_attempt_states_do_not_advance_pending_attempts(self):
        states = DriverAttemptStates(["attempt-a", "attempt-b"])
        states.transition("attempt-a", "STAGING")
        states.transition("attempt-a", "RUNNING")
        self.assertEqual(states.status("attempt-a"), "RUNNING")
        self.assertEqual(states.status("attempt-b"), "LEASED")
        with self.assertRaisesRegex(ValueError, "Invalid attempt transition"):
            states.transition("attempt-b", "RUNNING")

    def test_largest_cost_first_plan_is_bounded_and_deterministic(self):
        rows = [
            {"work_id": "small", "duration_seconds": 1},
            {"work_id": "large", "duration_seconds": 10},
            {"work_id": "medium", "duration_seconds": 5},
        ]
        plan = plan_largest_cost_first(rows, 2)
        self.assertEqual(
            [[row["work_id"] for row in partition] for partition in plan],
            [["large"], ["medium", "small"]],
        )
        self.assertEqual(plan_largest_cost_first([], 2), [])

    def test_line_count_selection_matches_notebook_04(self):
        records = [
            {"frame": 1, "frame_in_count": 0, "frame_out_count": 0},
            {"frame": 2, "frame_in_count": 1, "frame_out_count": 0},
            {"frame": 3, "frame_in_count": 0, "frame_out_count": 0},
        ]
        self.assertEqual(
            [row["frame"] for row in select_production_line_counts(records)],
            [2, 3],
        )
        self.assertEqual(select_production_line_counts([]), [])

    def test_process_production_partition_emits_valid_success_records(self):
        source_bytes = b"executor-canary-video"
        result = RunResult(
            initialized=True,
            fps=2.0,
            total_source_frames=20,
            total_sampled_frames=10,
            source_frames_read=20,
            processed_frames=10,
            effective_sample_fps=2.0,
            processing_seconds=3.5,
            line_in_count=1,
            line_out_count=0,
            telemetry={1: PersonTelemetry(entry_frame=0, last_seen_frame=2)},
            line_counts=[
                LineCountRecord(
                    frame=2,
                    video_seconds="1.000",
                    video_timestamp="00:00:01.000",
                    frame_in_count=1,
                    frame_out_count=0,
                    cumulative_in_count=1,
                    cumulative_out_count=0,
                    line_start_x=0,
                    line_start_y=10,
                    line_end_x=100,
                    line_end_y=10,
                )
            ],
        )
        processor = MagicMock()
        processor.run.return_value = result
        transaction = staging_transaction("worker-a", 0)
        config_builder = MagicMock(side_effect=lambda row, staged: staged)

        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source.mp4"
            source.write_bytes(source_bytes)
            work = {
                "work_id": "work-a",
                "attempt_id": "attempt-a",
                "expected_size_bytes": len(source_bytes),
                "expected_sha256": hashlib.sha256(source_bytes).hexdigest(),
                "duration_seconds": 10.0,
                "capture_date": datetime(2026, 10, 1).date(),
            }

            records = list(
                process_production_partition(
                    [work],
                    source_resolver=lambda row: source,
                    config_builder=config_builder,
                    worker_execution_id="worker-a",
                    transaction=transaction,
                    processor=processor,
                )
            )

        self.assertEqual(
            [record["record_type"] for record in records],
            ["video_result", "telemetry", "line_count"],
        )
        self.assertTrue(all(record["status"] == "SUCCEEDED" for record in records))
        self.assertTrue(
            all(record["txn_app_id"] == transaction.app_id for record in records)
        )
        self.assertTrue(
            all(record["worker_execution_id"] == "worker-a" for record in records)
        )
        self.assertEqual(records[0]["input_sha256"], work["expected_sha256"])
        self.assertEqual(records[0]["processed_frames"], 10)
        self.assertEqual(records[0]["distinct_people"], 1)
        config_path = config_builder.call_args.args[1]
        self.assertFalse(config_path.exists())
        validate_staging_records(
            records,
            [{"work_id": "work-a", "attempt_id": "attempt-a"}],
            transaction,
        )

    def test_process_production_partition_classifies_one_error_record(self):
        transaction = staging_transaction("worker-a", 1)
        processor = MagicMock()
        classifier = MagicMock(return_value=(False, "INPUT"))

        records = list(
            process_production_partition(
                [
                    {
                        "work_id": "work-a",
                        "attempt_id": "attempt-a",
                        "capture_date": datetime(2026, 10, 1).date(),
                    }
                ],
                source_resolver=MagicMock(side_effect=FileNotFoundError("missing")),
                config_builder=MagicMock(),
                worker_execution_id="worker-a",
                transaction=transaction,
                processor=processor,
                error_classifier=classifier,
            )
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["record_type"], "error")
        self.assertEqual(records[0]["status"], "FAILED")
        self.assertEqual(records[0]["error_type"], "FileNotFoundError")
        self.assertFalse(records[0]["retryable"])
        self.assertEqual(
            json.loads(records[0]["payload_json"])["error_category"],
            "INPUT",
        )
        classifier.assert_called_once()
        processor.run.assert_not_called()
        validate_staging_records(
            records,
            [{"work_id": "work-a", "attempt_id": "attempt-a"}],
            transaction,
        )

    def test_staging_identity_and_cardinality_are_stable(self):
        transaction = staging_transaction("execution-a", 0)
        self.assertEqual(transaction, staging_transaction("execution-a", 0))
        items = [{"work_id": "work-a", "attempt_id": "attempt-a"}]
        records = [
            {
                "work_id": "work-a",
                "attempt_id": "attempt-a",
                "record_type": "video_result",
                "record_sequence": 0,
                "txn_app_id": transaction.app_id,
                "txn_version": transaction.version,
            },
            {
                "work_id": "work-a",
                "attempt_id": "attempt-a",
                "record_type": "line_count",
                "record_sequence": 0,
                "txn_app_id": transaction.app_id,
                "txn_version": transaction.version,
            },
        ]
        validate_staging_records(records, items, transaction)
        with self.assertRaisesRegex(StagingConflictError, "Duplicate staging"):
            validate_staging_records([records[0], records[0]], items, transaction)
        with self.assertRaisesRegex(StagingConflictError, "one terminal"):
            validate_staging_records(records[1:], items, transaction)

    def test_executor_source_staging_validates_and_cleans_attempt_file(self):
        payload = b"video-bytes"
        digest = hashlib.sha256(payload).hexdigest()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.mp4"
            source.write_bytes(payload)
            temporary_root = root / "staging"
            temporary_root.mkdir()

            with staged_executor_source(
                source,
                attempt_id="attempt-a",
                expected_size_bytes=len(payload),
                expected_sha256=digest,
                temporary_root=temporary_root,
            ) as (staged, actual_digest, actual_size):
                self.assertTrue(staged.is_file())
                self.assertEqual(actual_digest, digest)
                self.assertEqual(actual_size, len(payload))
                staged_parent = staged.parent

            self.assertFalse(staged.exists())
            self.assertFalse(staged_parent.exists())

            with self.assertRaisesRegex(ValueError, "SHA-256"):
                with staged_executor_source(
                    source,
                    attempt_id="attempt-a",
                    expected_size_bytes=len(payload),
                    expected_sha256="0" * 64,
                    temporary_root=temporary_root,
                ):
                    self.fail("Hash mismatch must fail before yielding")


class FabricExecutorPartitionTests(unittest.TestCase):
    def test_executor_partition_schema_is_explicit(self):
        fake_types = types.SimpleNamespace()

        class StructField:
            def __init__(self, name, data_type, nullable):
                self.name = name
                self.dataType = data_type
                self.nullable = nullable

        class StructType:
            def __init__(self, fields):
                self.fields = fields

        for type_name in (
            "StringType",
            "BooleanType",
            "LongType",
            "DoubleType",
            "TimestampType",
        ):
            setattr(fake_types, type_name, lambda name=type_name: name)
        fake_types.StructField = StructField
        fake_types.StructType = StructType
        pyspark = types.ModuleType("pyspark")
        pyspark_sql = types.ModuleType("pyspark.sql")
        pyspark_sql.types = fake_types

        with patch.dict(
            sys.modules,
            {"pyspark": pyspark, "pyspark.sql": pyspark_sql},
        ):
            schema = executor_partition_schema()

        fields = [(field.name, field.nullable) for field in schema.fields]
        self.assertEqual(
            fields,
            [
                ("record_type", False),
                ("work_id", False),
                ("attempt_id", True),
                ("source_video", False),
                ("status", False),
                ("payload_json", False),
                ("error_type", True),
                ("error_message", True),
                ("retryable", True),
                ("processed_frames", True),
                ("processing_seconds", True),
                ("emitted_at_utc", False),
            ],
        )

    def test_runtime_cache_loads_once_per_key(self):
        cache = ExecutorRuntimeCache()
        loader = MagicMock(side_effect=[object(), object()])

        first = cache.get_or_load(("rtdetr", "cpu"), loader)
        second = cache.get_or_load(("rtdetr", "cpu"), loader)
        third = cache.get_or_load(("rtdetr", "gpu"), loader)

        self.assertIs(first, second)
        self.assertIsNot(first, third)
        self.assertEqual(loader.call_count, 2)

    def test_process_video_partition_emits_summary_records_and_errors(self):
        result = RunResult(
            initialized=True,
            fps=2.0,
            total_source_frames=20,
            total_sampled_frames=10,
            source_frames_read=20,
            processed_frames=10,
            effective_sample_fps=2.0,
            processing_seconds=3.5,
            line_in_count=1,
            line_out_count=0,
            telemetry={1: PersonTelemetry(entry_frame=0, last_seen_frame=2)},
            line_counts=[
                LineCountRecord(
                    frame=2,
                    video_seconds="1.000",
                    video_timestamp="00:00:01.000",
                    frame_in_count=1,
                    frame_out_count=0,
                    cumulative_in_count=1,
                    cumulative_out_count=0,
                    line_start_x=0,
                    line_start_y=10,
                    line_end_x=100,
                    line_end_y=10,
                )
            ],
        )
        processor = MagicMock()
        processor.run.side_effect = [result, OSError("cannot read video")]
        config_builder = MagicMock(side_effect=lambda row: row["work_id"])

        records = list(
            process_video_partition(
                [
                    {"work_id": "work-a", "source_video": "a.mp4"},
                    {"work_id": "work-b", "source_video": "b.mp4"},
                ],
                config_builder=config_builder,
                processor=processor,
            )
        )

        self.assertEqual(
            config_builder.call_args_list,
            [
                unittest.mock.call(
                    {"work_id": "work-a", "source_video": "a.mp4"}
                ),
                unittest.mock.call(
                    {"work_id": "work-b", "source_video": "b.mp4"}
                ),
            ],
        )
        self.assertEqual(
            processor.run.call_args_list,
            [unittest.mock.call("work-a"), unittest.mock.call("work-b")],
        )
        self.assertEqual(
            [record["record_type"] for record in records],
            ["video_result", "telemetry", "line_count", "error"],
        )
        self.assertEqual(records[0]["status"], "SUCCEEDED")
        self.assertEqual(records[0]["processed_frames"], 10)
        self.assertEqual(records[0]["processing_seconds"], 3.5)
        self.assertEqual(
            json.loads(records[0]["payload_json"]),
            {
                "effective_sample_fps": 2.0,
                "ended_early": False,
                "line_in_count": 1,
                "line_out_count": 0,
                "source_frames_read": 20,
                "total_sampled_frames": 10,
                "total_source_frames": 20,
            },
        )
        self.assertEqual(records[1]["status"], "SUCCEEDED")
        self.assertEqual(json.loads(records[1]["payload_json"])["person_id"], 1)
        self.assertEqual(records[2]["status"], "SUCCEEDED")
        self.assertEqual(
            json.loads(records[2]["payload_json"]),
            {
                "cumulative_in_count": 1,
                "cumulative_out_count": 0,
                "frame": 2,
                "frame_in_count": 1,
                "frame_out_count": 0,
                "line_end_x": 100,
                "line_end_y": 10,
                "line_start_x": 0,
                "line_start_y": 10,
                "video_seconds": "1.000",
                "video_timestamp": "00:00:01.000",
            },
        )
        self.assertEqual(records[3]["status"], "FAILED")
        self.assertEqual(records[3]["error_message"], "cannot read video")
        self.assertEqual(records[3]["error_type"], "OSError")
        self.assertTrue(records[3]["retryable"])
        self.assertEqual(
            json.loads(records[3]["payload_json"]),
            {
                "error_category": "RUNTIME",
                "error_message": "cannot read video",
                "error_type": "OSError",
            },
        )

    def test_process_video_partition_classifies_error_and_continues(self):
        error = OSError("temporary read failure")
        result = RunResult(initialized=True, fps=1.0)
        processor = MagicMock()
        processor.run.side_effect = [error, result]
        classifier = MagicMock(return_value=(False, "INPUT"))

        records = list(
            process_video_partition(
                [
                    {"work_id": "work-a", "source_video": "a.mp4"},
                    {"work_id": "work-b", "source_video": "b.mp4"},
                ],
                config_builder=lambda row: row,
                processor=processor,
                error_classifier=classifier,
            )
        )

        classifier.assert_called_once_with(error)
        self.assertEqual(
            [record["record_type"] for record in records],
            ["error", "video_result"],
        )
        self.assertFalse(records[0]["retryable"])
        self.assertEqual(
            json.loads(records[0]["payload_json"])["error_category"],
            "INPUT",
        )

    def test_process_video_partition_constructs_default_processor(self):
        result = RunResult(initialized=True, fps=1.0)
        processor = MagicMock()
        processor.run.return_value = result

        with patch(
            "people_counter.fabric_executor_partition.SdkRuntimeProcessor",
            return_value=processor,
        ) as processor_type:
            records = list(
                process_video_partition(
                    [{"work_id": "work-a", "source_video": "a.mp4"}],
                    config_builder=lambda row: row,
                )
            )

        processor_type.assert_called_once_with()
        processor.run.assert_called_once()
        self.assertEqual([record["record_type"] for record in records], ["video_result"])

    def test_sdk_runtime_processor_reuses_loaded_rtdetr_runtime(self):
        config_a = RTDetrOsnetConfig(
            video=Path("a.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
            detector_model="r18",
        )
        config_b = RTDetrOsnetConfig(
            video=Path("b.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
            detector_model="r18",
        )
        runtime = MagicMock()
        result_a = RunResult(initialized=True)
        result_b = RunResult(initialized=True)

        with (
            patch(
                "people_counter.pipelines.rtdetr_osnet.load_runtime",
                return_value=runtime,
            ) as load_runtime,
            patch(
                "people_counter.pipelines.rtdetr_osnet.run_with_runtime",
                side_effect=[result_a, result_b],
            ) as run_with_runtime,
            patch(
                "people_counter.pipelines.rtdetr_osnet.RTDetrRuntime",
                new=MagicMock,
            ),
        ):
            processor = SdkRuntimeProcessor()
            self.assertIs(processor.run(config_a), result_a)
            self.assertIs(processor.run(config_b), result_b)

        load_runtime.assert_called_once_with(config_a)
        self.assertEqual(
            run_with_runtime.call_args_list[0].args,
            (config_a, runtime),
        )
        self.assertEqual(
            run_with_runtime.call_args_list[1].args,
            (config_b, runtime),
        )

    def test_sdk_runtime_processor_separates_rtdetr_model_cache_keys(self):
        configs = [
            RTDetrOsnetConfig(
                video=Path(f"{model}.mp4"),
                device_variant="cpu",
                device="cpu",
                batch_size=1,
                detector_model=model,
            )
            for model in ("r18", "r50")
        ]
        runtimes = [MagicMock(), MagicMock()]

        with (
            patch(
                "people_counter.pipelines.rtdetr_osnet.load_runtime",
                side_effect=runtimes,
            ) as load_runtime,
            patch(
                "people_counter.pipelines.rtdetr_osnet.run_with_runtime",
                side_effect=[RunResult(initialized=True), RunResult(initialized=True)],
            ),
            patch(
                "people_counter.pipelines.rtdetr_osnet.RTDetrRuntime",
                new=MagicMock,
            ),
        ):
            processor = SdkRuntimeProcessor()
            processor.run(configs[0])
            processor.run(configs[1])

        self.assertEqual(
            load_runtime.call_args_list,
            [unittest.mock.call(configs[0]), unittest.mock.call(configs[1])],
        )

    def test_sdk_runtime_processor_uses_explicit_pipeline_cache_keys(self):
        models_dir = Path("models")
        rtdetr = RTDetrOsnetConfig(
            video=Path("a.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
            detector_model="r18",
            models_dir=models_dir,
        )
        rfdetr = RFDetrBotsortConfig(
            video=Path("b.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=2,
            use_fp16=False,
            models_dir=models_dir,
        )
        cache = MagicMock()
        runtimes = [MagicMock(), MagicMock()]
        cache.get_or_load.side_effect = runtimes

        with (
            patch(
                "people_counter.pipelines.rtdetr_osnet.run_with_runtime",
                return_value=RunResult(initialized=True),
            ),
            patch(
                "people_counter.pipelines.rtdetr_osnet.RTDetrRuntime",
                new=MagicMock,
            ),
            patch(
                "people_counter.pipelines.rfdetr_botsort.run_with_runtime",
                return_value=RunResult(initialized=True),
            ),
            patch(
                "people_counter.pipelines.rfdetr_botsort.RFDetrRuntime",
                new=MagicMock,
            ),
        ):
            processor = SdkRuntimeProcessor(cache=cache)
            processor.run(rtdetr)
            processor.run(rfdetr)

        resolved_models = str(models_dir.resolve())
        self.assertEqual(
            cache.get_or_load.call_args_list[0].args[0],
            (
                "rtdetr-osnet",
                "cpu",
                "cpu",
                "r18",
                "pytorch",
                resolved_models,
            ),
        )
        self.assertEqual(
            cache.get_or_load.call_args_list[1].args[0],
            (
                "rfdetr-botsort",
                "cpu",
                "cpu",
                2,
                False,
                "pytorch",
                resolved_models,
            ),
        )

    def test_sdk_runtime_processor_uses_distinct_rfdetr_cache_keys(self):
        config_a = RFDetrBotsortConfig(
            video=Path("a.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=1,
        )
        config_b = RFDetrBotsortConfig(
            video=Path("b.mp4"),
            device_variant="cpu",
            device="cpu",
            batch_size=2,
        )
        runtime_a = MagicMock()
        runtime_b = MagicMock()
        result_a = RunResult(initialized=True)
        result_b = RunResult(initialized=True)

        with (
            patch(
                "people_counter.pipelines.rfdetr_botsort.load_runtime",
                side_effect=[runtime_a, runtime_b],
            ) as load_runtime,
            patch(
                "people_counter.pipelines.rfdetr_botsort.run_with_runtime",
                side_effect=[result_a, result_b],
            ) as run_with_runtime,
            patch(
                "people_counter.pipelines.rfdetr_botsort.RFDetrRuntime",
                new=MagicMock,
            ),
        ):
            processor = SdkRuntimeProcessor()
            self.assertIs(processor.run(config_a), result_a)
            self.assertIs(processor.run(config_b), result_b)

        self.assertEqual(load_runtime.call_args_list[0].args, (config_a,))
        self.assertEqual(load_runtime.call_args_list[1].args, (config_b,))
        self.assertEqual(
            run_with_runtime.call_args_list[0].args,
            (config_a, runtime_a),
        )
        self.assertEqual(
            run_with_runtime.call_args_list[1].args,
            (config_b, runtime_b),
        )

    def test_sdk_runtime_processor_rejects_unknown_config(self):
        with self.assertRaises(TypeError) as raised:
            SdkRuntimeProcessor().run(MagicMock())
        self.assertEqual(
            str(raised.exception),
            "config must be RTDetrOsnetConfig or RFDetrBotsortConfig; "
            "got MagicMock",
        )

    def test_runtime_cache_key_rejects_unknown_config(self):
        with self.assertRaises(TypeError) as raised:
            _runtime_cache_key(MagicMock())
        self.assertEqual(
            str(raised.exception),
            "config must be RTDetrOsnetConfig or RFDetrBotsortConfig; "
            "got MagicMock",
        )


if __name__ == "__main__":
    unittest.main()
