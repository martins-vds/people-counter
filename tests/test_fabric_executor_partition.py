import json
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from people_counter.config import RFDetrBotsortConfig, RTDetrOsnetConfig
from people_counter.fabric_executor_partition import (
    ExecutorRuntimeCache,
    SdkRuntimeProcessor,
    _runtime_cache_key,
    calculate_thread_budget,
    configure_cpu_runtime,
    executor_partition_schema,
    process_video_partition,
)
from people_counter.models import LineCountRecord, PersonTelemetry, RunResult


class FabricExecutorPartitionTests(unittest.TestCase):
    def test_thread_budget_rejects_invalid_values(self):
        for kwargs, message in (
            (
                {"driver_cores": 0, "active_workers": 1},
                "driver_cores must be a positive integer",
            ),
            (
                {"driver_cores": 4, "active_workers": 0},
                "active_workers must be a positive integer",
            ),
            (
                {"driver_cores": 4.0, "active_workers": 1},
                "driver_cores must be a positive integer",
            ),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError) as raised:
                    calculate_thread_budget(**kwargs)
                self.assertEqual(str(raised.exception), message)

    def test_thread_budget_preserves_inputs_and_warns_only_for_oversubscription(self):
        with self.assertNoLogs(
            "people_counter.fabric_executor_partition",
            level="WARNING",
        ):
            balanced = calculate_thread_budget(4, 4)
        self.assertEqual(balanced.driver_cores, 4)
        self.assertEqual(balanced.active_workers, 4)
        self.assertEqual(balanced.threads_per_worker, 1)

        with self.assertLogs(
            "people_counter.fabric_executor_partition",
            level="WARNING",
        ) as logs:
            oversubscribed = calculate_thread_budget(2, 3)
        self.assertEqual(oversubscribed.driver_cores, 2)
        self.assertEqual(oversubscribed.active_workers, 3)
        self.assertEqual(oversubscribed.threads_per_worker, 1)
        self.assertEqual(
            logs.output,
            [
                "WARNING:people_counter.fabric_executor_partition:"
                "active_workers=3 exceeds driver_cores=2; assigning one "
                "thread per worker"
            ],
        )

    def test_configure_cpu_runtime_sets_environment_budget(self):
        environ = {}

        with self.assertLogs(
            "people_counter.fabric_executor_partition",
            level="INFO",
        ) as logs:
            budget = configure_cpu_runtime(
                4,
                3,
                environment=environ,
                apply_native_limits=False,
            )

        self.assertEqual(budget.driver_cores, 4)
        self.assertEqual(budget.active_workers, 3)
        self.assertEqual(budget.threads_per_worker, 1)
        self.assertEqual(environ["OMP_NUM_THREADS"], "1")
        self.assertEqual(environ["MKL_NUM_THREADS"], "1")
        self.assertEqual(
            logs.output,
            [
                "INFO:people_counter.fabric_executor_partition:"
                "Configured CPU executor worker: driver_cores=4 "
                "active_workers=3 threads_per_worker=1"
            ],
        )

    def test_configure_cpu_runtime_can_update_process_environment(self):
        with patch.dict(os.environ, {}, clear=True):
            budget = configure_cpu_runtime(
                1,
                1,
                apply_native_limits=False,
            )

            self.assertEqual(budget.threads_per_worker, 1)
            self.assertEqual(os.environ["OMP_NUM_THREADS"], "1")

    def test_configure_cpu_runtime_rejects_contradictory_existing_environment(self):
        with self.assertRaises(ValueError) as raised:
            configure_cpu_runtime(
                4,
                2,
                environment={"OMP_NUM_THREADS": "4"},
                apply_native_limits=False,
            )
        self.assertEqual(
            str(raised.exception),
            "OMP_NUM_THREADS is already set to 4; expected 2. "
            "Configure CPU thread limits before native runtime import.",
        )

    def test_configure_cpu_runtime_applies_native_limits_once_per_process(self):
        environ = {}
        cv2 = MagicMock()
        torch = MagicMock()

        with (
            patch(
                "people_counter.fabric_executor_partition._NATIVE_THREAD_BUDGET",
                None,
            ),
            patch.dict(sys.modules, {"cv2": cv2, "torch": torch}),
        ):
            first = configure_cpu_runtime(4, 2, environment=environ)
            second = configure_cpu_runtime(4, 2, environment=environ)
            with self.assertRaisesRegex(
                ValueError,
                "Native CPU runtime is already configured",
            ) as raised:
                configure_cpu_runtime(6, 3, environment=environ)

        self.assertEqual(first, second)
        self.assertEqual(
            str(raised.exception),
            "Native CPU runtime is already configured with "
            "ThreadBudget(driver_cores=4, active_workers=2, "
            "threads_per_worker=2); requested "
            "ThreadBudget(driver_cores=6, active_workers=3, "
            "threads_per_worker=2)",
        )
        torch.set_num_threads.assert_called_once_with(2)
        torch.set_num_interop_threads.assert_called_once_with(1)
        cv2.setNumThreads.assert_called_once_with(1)

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
            ("rtdetr-osnet", "cpu", "cpu", "r18", resolved_models),
        )
        self.assertEqual(
            cache.get_or_load.call_args_list[1].args[0],
            ("rfdetr-botsort", "cpu", "cpu", 2, False, resolved_models),
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
