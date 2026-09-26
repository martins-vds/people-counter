import ast
import json
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call

from people_counter.runtime import RuntimeCompatibilityError


NOTEBOOKS = Path(__file__).resolve().parents[1] / "notebooks" / "fabric"
WORKER = NOTEBOOKS / "04_process_video.ipynb"
BOOTSTRAP = NOTEBOOKS / "00_bootstrap_lakehouse.ipynb"
BENCHMARK = NOTEBOOKS / "08_capacity_benchmark.ipynb"
EXECUTOR_PROTOTYPE = NOTEBOOKS / "15_executor_partition_inference.ipynb"


def cell_source(path, cell_id):
    notebook = json.loads(path.read_text(encoding="utf-8"))
    return "".join(next(cell["source"] for cell in notebook["cells"] if cell["id"] == cell_id))


def code_source(path):
    notebook = json.loads(path.read_text(encoding="utf-8"))
    return "\n".join(
        "".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"
    )


def worker_functions(*names, **namespace):
    tree = ast.parse(cell_source(WORKER, "worker-helpers"))
    functions = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    if {node.name for node in functions} != set(names):
        raise AssertionError("Requested worker helper was not found")
    namespace.update(Any=Any, DataFrame=object)
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(WORKER), "exec"), namespace)
    return namespace


def cell_functions(path, cell_id, *names, **namespace):
    tree = ast.parse(cell_source(path, cell_id))
    functions = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    if {node.name for node in functions} != set(names):
        raise AssertionError("Requested notebook helper was not found")
    exec(
        compile(ast.Module(body=functions, type_ignores=[]), str(path), "exec"),
        namespace,
    )
    return namespace


class WorkerLeaseError(RuntimeError):
    pass


class FabricNotebookTests(unittest.TestCase):
    def worker_run_namespace(self):
        client = MagicMock()
        staged, temporary = MagicMock(), MagicMock()
        local_models, models_temporary = MagicMock(), MagicMock()
        result = MagicMock(
            fps=25.0,
            total_source_frames=250,
            processed_frames=30,
            effective_sample_fps=3.0,
            processing_seconds=4.0,
            line_in_count=2,
            line_out_count=1,
        )
        config = MagicMock()
        config.result.initialized = False
        work = {
            "capture_date": date(2023, 9, 29),
            "status": "LEASED",
            "committed_attempt_id": None,
        }
        published = {"status": "SUCCEEDED", "committed_attempt_id": "attempt-a"}
        return worker_functions(
            "merge_attempt", "claim_worker_execution",
            worker_items=[{"work_id": "work-a", "attempt_id": "attempt-a"}],
            worker_execution_id="execution-a",
            lease_minutes=30,
            max_worker_lifetime_seconds=3600,
            prefix="pc",
            database="",
            driver_cores=4,
            active_workers=2,
            spark_application_id="application-1",
            pending_contexts=[],
            PIPELINE_RUN_ID="pipeline-a",
            ACTIVITY_RUN_ID="activity-a",
            FABRIC_JOB_INSTANCE_ID="",
            BUNDLE_MANIFEST_SHA256="bundle-a",
            version=MagicMock(return_value="0.3.0"),
            spark_session=MagicMock(),
            control_writer=MagicMock(),
            WorkerEventClient=MagicMock(return_value=client),
            load_work=MagicMock(side_effect=[work, work, published]),
            verify_lease=MagicMock(),
            stage_models=MagicMock(
                return_value=(local_models, models_temporary)
            ),
            stage_source=MagicMock(return_value=(staged, temporary)),
            build_config=MagicMock(return_value=config),
            heartbeat=MagicMock(),
            load_runtime=MagicMock(return_value=MagicMock()),
            run_with_runtime=MagicMock(return_value=result),
            utc_now=lambda: datetime(2026, 9, 23, tzinfo=timezone.utc),
            time=MagicMock(monotonic=MagicMock(return_value=0.0)),
            shutil=MagicMock(),
            thread_budget=MagicMock(
                driver_cores=4,
                active_workers=2,
                threads_per_worker=2,
                interop_threads_configured=True,
            ),
            result_frames=MagicMock(return_value=(
                MagicMock(count=MagicMock(return_value=2)),
                MagicMock(count=MagicMock(return_value=3)),
            )),
            append_attempt_rows=MagicMock(),
            telemetry_table="pc_telemetry_attempts",
            line_counts_table="pc_line_count_attempts",
            classify_error=MagicMock(return_value=(True, "RUNTIME")),
            LeaseLostError=WorkerLeaseError,
            RuntimeCompatibilityError=RuntimeCompatibilityError,
            json=json,
            print=MagicMock(),
        )

    def execute_worker(self, namespace):
        exec(compile(cell_source(WORKER, "worker-run"), str(WORKER), "exec"), namespace)

    def test_all_fabric_code_cells_compile(self):
        for path in sorted(NOTEBOOKS.glob("*.ipynb")):
            notebook = json.loads(path.read_text(encoding="utf-8"))
            for cell in notebook["cells"]:
                if cell["cell_type"] == "code":
                    with self.subTest(notebook=path.name, cell=cell["id"]):
                        compile("".join(cell["source"]), f"{path}:{cell['id']}", "exec")

    def test_executor_partition_prototype_configures_executor_cpu_limits(self):
        parameters = cell_source(EXECUTOR_PROTOTYPE, "parameters")
        spark_config = cell_source(EXECUTOR_PROTOTYPE, "spark-config")
        partition = cell_source(EXECUTOR_PROTOTYPE, "map-partitions")

        self.assertIn('OUTPUT_TXN_APP_ID = "UNSET"', parameters)
        self.assertIn("OUTPUT_TXN_VERSION = -1", parameters)
        self.assertIn(
            "calculate_thread_budget(EXECUTOR_CORES, ACTIVE_TASKS_PER_EXECUTOR)",
            spark_config,
        )
        self.assertNotIn("configure_cpu_runtime(", spark_config)
        self.assertIn(
            "configure_cpu_runtime(EXECUTOR_CORES, ACTIVE_TASKS_PER_EXECUTOR)",
            partition,
        )
        self.assertIn("models_dir is required for offline executor inference", partition)
        self.assertIn("supports only CPU inference", partition)

    def test_benchmark_rejects_placeholder_runtime_labels(self):
        required_runtime_label = cell_functions(
            BENCHMARK,
            "benchmark-run",
            "required_runtime_label",
        )["required_runtime_label"]

        for value in (None, "", " ", "UNSET", "none", "NULL"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError,
                    "exact non-empty Fabric runtime label",
                ):
                    required_runtime_label(value)
        self.assertEqual(required_runtime_label(" Runtime 2.0 "), "Runtime 2.0")

    def test_benchmark_parses_bounded_multi_video_items(self):
        namespace = cell_functions(
            BENCHMARK,
            "benchmark-run",
            "parse_benchmark_items",
            json=json,
            VIDEO_URI="fallback.mp4",
            SAMPLE_NAME="fallback",
            EXPECTED_VIDEO_DURATION_SECONDS=60.0,
        )
        parse_benchmark_items = namespace["parse_benchmark_items"]
        items = parse_benchmark_items(
            json.dumps(
                [
                    {
                        "video_uri": "a.mp4",
                        "sample_name": "a",
                        "duration_seconds": 10,
                    },
                    {
                        "video_uri": "b.mp4",
                        "sample_name": "b",
                        "duration_seconds": 20,
                    },
                ]
            ),
            2,
        )

        self.assertEqual([item["sample_name"] for item in items], ["a", "b"])
        with self.assertRaisesRegex(ValueError, "maximum is 1"):
            parse_benchmark_items(items, 1)
        with self.assertRaisesRegex(ValueError, "positive duration_seconds"):
            parse_benchmark_items(
                '[{"video_uri":"a.mp4","sample_name":"a","duration_seconds":0}]',
                1,
            )

    def test_benchmark_reuses_one_runtime_per_worker(self):
        source = cell_source(BENCHMARK, "benchmark-run")

        self.assertIn("for item in benchmark_items:", source)
        self.assertIn("if runtime is None:", source)
        self.assertIn("result = run_with_runtime(config, runtime)", source)
        self.assertEqual(source.count("runtime = load_runtime(config)"), 1)

    def test_benchmark_worker_and_gate_share_validated_grouping_keys(self):
        run_source = cell_source(BENCHMARK, "benchmark-run")
        summary_source = cell_source(BENCHMARK, "benchmark-summary")

        self.assertIn('"runtime_version": runtime_version', run_source)
        self.assertIn(
            'F.col("runtime_version") == runtime_version',
            summary_source,
        )
        for key in (
            "benchmark_batch_id",
            "capacity_sku",
            "runtime_version",
            "sdk_version",
            "config_sha256",
            "concurrent_workers",
        ):
            self.assertIn(f'"{key}"', summary_source)

    def test_benchmark_schema_and_worker_include_phase_metrics(self):
        bootstrap_create = cell_source(BOOTSTRAP, "bootstrap-tables")
        bootstrap_evolution = cell_source(
            BOOTSTRAP,
            "bootstrap-schema-evolution",
        )
        benchmark_parameters = cell_source(BENCHMARK, "benchmark-parameters")
        benchmark_run = cell_source(BENCHMARK, "benchmark-run")
        columns = {
            "source_stage_seconds": "DOUBLE",
            "runtime_load_seconds": "DOUBLE",
            "video_processing_seconds": "DOUBLE",
            "result_persist_seconds": "DOUBLE",
            "sampled_frames": "BIGINT",
            "artifact_mode": "STRING",
            "driver_cores": "INT",
            "active_workers_per_driver": "INT",
            "threads_per_worker": "INT",
            "interop_threads_configured": "BOOLEAN",
            "spark_application_id": "STRING",
        }

        for name, data_type in columns.items():
            self.assertIn(f"{name} {data_type}", bootstrap_create)
            self.assertIn(f'"{name}": "{data_type}"', bootstrap_evolution)
            self.assertIn(f'"{name}"', benchmark_run)
        self.assertIn('MODELS_DIR = ""', benchmark_parameters)
        self.assertIn("shutil.copytree(model_source, local_models)", benchmark_run)
        self.assertIn('"models_dir": local_models', benchmark_run)
        self.assertIn("runtime = load_runtime(config)", benchmark_run)
        self.assertIn("run_with_runtime(config, runtime)", benchmark_run)
        self.assertIn("time.perf_counter()", benchmark_run)
        self.assertNotIn("DeltaTable.forName", benchmark_run)
        self.assertIn('"result_persist_seconds": None', benchmark_run)
        self.assertIn('"artifact_mode": "offline"', benchmark_run)
        self.assertIn(
            "spark_application_id = spark_session.sparkContext.applicationId",
            benchmark_run,
        )
        summary = cell_source(BENCHMARK, "benchmark-summary")
        self.assertIn("invalid_interop_benchmarks", summary)
        self.assertIn("and invalid_interop_benchmarks == 0", summary)

    def test_worker_reloads_after_runtime_compatibility_mismatch(self):
        namespace = self.worker_run_namespace()
        first_runtime = MagicMock(name="first-runtime")
        replacement_runtime = MagicMock(name="replacement-runtime")
        result = namespace["run_with_runtime"].return_value
        namespace["load_runtime"].side_effect = [
            first_runtime,
            replacement_runtime,
        ]
        namespace["run_with_runtime"].side_effect = [
            RuntimeCompatibilityError("runtime is incompatible with config"),
            result,
        ]

        self.execute_worker(namespace)

        self.assertEqual(namespace["load_runtime"].call_count, 2)
        self.assertEqual(
            namespace["run_with_runtime"].call_args_list,
            [
                call(namespace["build_config"].return_value, first_runtime),
                call(namespace["build_config"].return_value, replacement_runtime),
            ],
        )
        self.assertEqual(namespace["outcome"]["status"], "SUCCEEDED")

    def test_operations_schema_and_aggregate_track_deferred_attempts(self):
        bootstrap_create = cell_source(BOOTSTRAP, "bootstrap-tables")
        bootstrap_evolution = cell_source(
            BOOTSTRAP,
            "bootstrap-schema-evolution",
        )
        aggregate = cell_source(
            NOTEBOOKS / "07_build_gold_aggregates.ipynb",
            "gold-build",
        )

        self.assertIn("deferred BIGINT NOT NULL", bootstrap_create)
        self.assertIn('"deferred": "BIGINT"', bootstrap_evolution)
        self.assertIn('F.col("status") == "RELEASED"', aggregate)
        self.assertIn('"failed", "deferred"', aggregate)

    def test_worker_has_no_direct_delta_mutations(self):
        source = code_source(WORKER)
        tree = ast.parse(source)
        self.assertNotIn("from delta.tables", source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                self.assertNotIn(node.func.attr, {"merge", "delete", "whenMatchedUpdateAll"})

    def test_worker_batch_parameters_require_bounded_unique_items(self):
        parameters = cell_source(WORKER, "worker-parameters")
        namespace = worker_functions(
            "require_text",
            "parse_worker_items",
            json=json,
            WORK_ID="legacy-work",
            ATTEMPT_ID="legacy-attempt",
            max_items_per_worker=2,
        )
        parse_worker_items = namespace["parse_worker_items"]

        self.assertIn('MODELS_DIR = ""', parameters)
        self.assertIn("MAX_ITEMS_PER_WORKER = 4", parameters)
        self.assertIn("MAX_WORKER_LIFETIME_SECONDS = 19800", parameters)
        self.assertEqual(
            parse_worker_items("[]"),
            [{"work_id": "legacy-work", "attempt_id": "legacy-attempt"}],
        )
        no_work = worker_functions(
            "require_text",
            "parse_worker_items",
            json=json,
            WORK_ID="",
            ATTEMPT_ID="",
            max_items_per_worker=2,
        )["parse_worker_items"]
        self.assertEqual(no_work("[]"), [])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            parse_worker_items(
                '[{"work_id":"a","attempt_id":"1"},'
                '{"work_id":"a","attempt_id":"1"}]'
            )
        with self.assertRaisesRegex(ValueError, "maximum is 2"):
            parse_worker_items(
                '[{"work_id":"a","attempt_id":"1"},'
                '{"work_id":"b","attempt_id":"2"},'
                '{"work_id":"c","attempt_id":"3"}]'
            )

    def test_claimed_worker_batches_are_configuration_homogeneous(self):
        source = cell_source(NOTEBOOKS / "03_claim_work.ipynb", "claim-work")
        self.assertIn(
            '.select("lease_dispatcher_id")\n.distinct()\n.count()',
            source.replace("        ", ""),
        )
        self.assertIn(
            'F.coalesce("runtime_sha256", "config_sha256")',
            source,
        )
        self.assertIn(
            "Claimed worker batch must contain exactly one compatible model runtime",
            source,
        )
        self.assertIn('F.col("status").isin(active_states)', source)
        self.assertIn('"attempt_count": "greatest(t.attempt_count - 1, 0)"', source)

    def test_registration_persists_runtime_compatibility_hash(self):
        bootstrap_create = cell_source(BOOTSTRAP, "bootstrap-tables")
        bootstrap_evolution = cell_source(BOOTSTRAP, "bootstrap-schema-evolution")
        event_source = code_source(NOTEBOOKS / "01_register_event.ipynb")
        backfill_source = code_source(NOTEBOOKS / "02_register_backfill.ipynb")

        self.assertIn("runtime_sha256 STRING", bootstrap_create)
        self.assertIn('"runtime_sha256": "STRING"', bootstrap_evolution)
        self.assertIn('"runtime_sha256": runtime_sha256', event_source)
        self.assertIn('F.lit(runtime_sha256).alias("runtime_sha256")', backfill_source)
        self.assertIn(
            'if field == "runtime_sha256" and actual is None:',
            event_source,
        )
        self.assertIn('condition="t.runtime_sha256 IS NULL"', event_source)
        self.assertIn(
            'check = F.col("t.runtime_sha256").isNull() | check',
            backfill_source,
        )
        self.assertIn("persisted_same_immutable", backfill_source)
        runtime_block = event_source.split("runtime_value = {", 1)[1].split(
            "runtime_encoded =",
            1,
        )[0]
        self.assertNotIn('"line"', runtime_block)

    def test_worker_reuses_one_runtime_for_multiple_items(self):
        namespace = self.worker_run_namespace()
        namespace["worker_items"] = [
            {"work_id": "work-a", "attempt_id": "attempt-a"},
            {"work_id": "work-b", "attempt_id": "attempt-b"},
        ]
        work_a = {
            "capture_date": date(2023, 9, 29),
            "status": "LEASED",
            "committed_attempt_id": None,
        }
        work_b = {
            "capture_date": date(2023, 9, 30),
            "status": "LEASED",
            "committed_attempt_id": None,
        }
        namespace["load_work"].side_effect = [
            work_a,
            work_b,
            work_a,
            {"status": "SUCCEEDED", "committed_attempt_id": "attempt-a"},
            work_b,
            {"status": "SUCCEEDED", "committed_attempt_id": "attempt-b"},
        ]
        config_a, config_b = MagicMock(), MagicMock()
        config_a.result.initialized = False
        config_b.result.initialized = False
        namespace["build_config"].side_effect = [config_a, config_b]
        runtime = MagicMock()
        namespace["load_runtime"].return_value = runtime
        result_a = namespace["run_with_runtime"].return_value
        result_b = MagicMock(
            fps=25.0,
            total_source_frames=500,
            processed_frames=60,
            effective_sample_fps=3.0,
            processing_seconds=8.0,
            line_in_count=4,
            line_out_count=2,
        )
        namespace["run_with_runtime"].side_effect = [result_a, result_b]
        namespace["stage_source"].side_effect = [
            (MagicMock(), MagicMock()),
            (MagicMock(), MagicMock()),
        ]

        self.execute_worker(namespace)

        namespace["load_runtime"].assert_called_once_with(config_a)
        self.assertEqual(
            namespace["run_with_runtime"].call_args_list,
            [call(config_a, runtime), call(config_b, runtime)],
        )
        self.assertEqual(namespace["outcome"]["processed_items"], 2)
        self.assertEqual(namespace["outcome"]["failed_items"], 0)
        self.assertEqual(namespace["append_attempt_rows"].call_count, 4)

    def test_worker_keeps_first_commit_and_continues_after_second_failure(self):
        namespace = self.worker_run_namespace()
        namespace["worker_items"] = [
            {"work_id": "work-a", "attempt_id": "attempt-a"},
            {"work_id": "work-b", "attempt_id": "attempt-b"},
        ]
        work_a = {
            "capture_date": date(2023, 9, 29),
            "status": "LEASED",
            "committed_attempt_id": None,
        }
        work_b = {
            "capture_date": date(2023, 9, 30),
            "status": "LEASED",
            "committed_attempt_id": None,
        }
        namespace["load_work"].side_effect = [
            work_a,
            work_b,
            work_a,
            {"status": "SUCCEEDED", "committed_attempt_id": "attempt-a"},
            work_b,
            {"status": "RUNNING", "committed_attempt_id": None},
        ]
        config_a, config_b = MagicMock(), MagicMock()
        config_a.result.initialized = False
        config_b.result.initialized = False
        namespace["build_config"].side_effect = [config_a, config_b]
        namespace["stage_source"].side_effect = [
            (MagicMock(), MagicMock()),
            (MagicMock(), MagicMock()),
        ]
        namespace["run_with_runtime"].side_effect = [
            namespace["run_with_runtime"].return_value,
            RuntimeError("second item failed"),
        ]

        with self.assertRaisesRegex(
            RuntimeError,
            'completed with 1 failed item.*"status": "COMPLETED_WITH_FAILURES"',
        ):
            self.execute_worker(namespace)

        submitted_events = [
            event.args[0]
            for client_call in namespace["WorkerEventClient"].return_value.mock_calls
            if client_call[0] == "submit"
            for event in [client_call]
        ]
        self.assertIn("commit", submitted_events)
        self.assertIn("failure", submitted_events)
        self.assertEqual(namespace["run_with_runtime"].call_count, 2)
        self.assertEqual(namespace["outcome"]["failed_items"], 1)
        self.assertEqual(namespace["outcome"]["spark_application_id"], "application-1")

    def test_documented_cpu_and_grouped_benchmark_mappings_are_safe(self):
        readme = (NOTEBOOKS / "README.md").read_text(encoding="utf-8")

        self.assertIn(
            "| `ACTIVE_WORKERS` | `Int` | Dynamic | "
            "`@pipeline().parameters.MAX_CONCURRENT_WORKERS` |",
            readme,
        )
        self.assertIn(
            "| `BENCHMARK_ITEMS_JSON` | `@string(item())` |",
            readme,
        )
        self.assertIn(
            "`@pipeline().parameters.EXPECTED_BATCH_MEMBERS`",
            readme,
        )
        self.assertIn("interop_threads_configured=false", readme)

    def test_worker_releases_lifetime_deferred_item_without_failure(self):
        namespace = self.worker_run_namespace()
        namespace["time"].monotonic.side_effect = [0.0, 0.0, 3600.0]
        work = {
            "capture_date": date(2023, 9, 29),
            "status": "LEASED",
            "committed_attempt_id": None,
        }
        namespace["load_work"].side_effect = [
            work,
            work,
        ]

        self.execute_worker(namespace)

        namespace["stage_models"].assert_not_called()
        namespace["run_with_runtime"].assert_not_called()
        namespace["WorkerEventClient"].return_value.submit.assert_called_with(
            "release",
            {"reason": "Worker lifetime limit reached before starting item"},
        )
        self.assertEqual(namespace["outcome"]["failed_items"], 0)
        self.assertEqual(namespace["outcome"]["items"][0]["status"], "DEFERRED")

    def test_empty_worker_batch_is_successful_no_work(self):
        namespace = self.worker_run_namespace()
        namespace.update(worker_items=[], thread_budget=None)
        namespace["load_work"].reset_mock()

        self.execute_worker(namespace)

        self.assertEqual(namespace["outcome"]["status"], "NO_WORK")
        self.assertEqual(namespace["outcome"]["processed_items"], 0)
        namespace["WorkerEventClient"].assert_not_called()
        namespace["stage_models"].assert_not_called()

    def test_pending_items_receive_leased_heartbeats(self):
        client = MagicMock()
        context = {
            "work_id": "work-b",
            "attempt_id": "attempt-b",
            "client": client,
            "last_heartbeat": 0.0,
            "renewal_error": None,
        }
        now = datetime(2026, 9, 23, tzinfo=timezone.utc)
        namespace = worker_functions(
            "renew_pending_leases",
            pending_contexts=[context],
            heartbeat_seconds=600,
            utc_now=lambda: now,
            PIPELINE_RUN_ID="pipeline",
            ACTIVITY_RUN_ID="activity",
            FABRIC_JOB_INSTANCE_ID="job",
            BUNDLE_MANIFEST_SHA256="bundle",
            version=lambda name: "test-sdk",
            json=json,
            print=MagicMock(),
        )

        namespace["renew_pending_leases"](700.0)

        client.submit.assert_called_once_with(
            "heartbeat",
            {
                "status": "LEASED",
                "updates": {
                    "status": "LEASED",
                    "last_heartbeat_at": now,
                    "pipeline_run_id": "pipeline",
                    "activity_run_id": "activity",
                    "fabric_job_instance_id": "job",
                    "sdk_version": "test-sdk",
                    "bundle_manifest_sha256": "bundle",
                },
            },
        )
        self.assertEqual(context["last_heartbeat"], 700.0)

    def test_other_control_writers_use_the_same_mutation_authority(self):
        for name in (
            "00_bootstrap_lakehouse", "01_register_event", "02_register_backfill",
            "03_claim_work", "05_watchdog_recovery", "06_reconcile_publication",
            "09_maintain_delta", "10_replay_work", "14_reset_test_data",
        ):
            with self.subTest(notebook=name):
                source = code_source(NOTEBOOKS / f"{name}.ipynb")
                self.assertIn("from people_counter.fabric_control import ControlWriter", source)
                self.assertIn('writer = ControlWriter(spark_session,', source)
                self.assertIn('"control_writer"', source)
                self.assertIn("DeltaTable = writer.tables", source)
                self.assertNotIn("from delta.tables import", source)
                if "DeltaTable.forName" in source:
                    self.assertLess(source.index("DeltaTable = writer.tables"), source.index("DeltaTable.forName"))

    def test_claim_and_watchdog_drain_events_before_candidate_reads(self):
        for name, candidate in (
            ("03_claim_work", 'owned_active = ('),
            ("05_watchdog_recovery", 'candidates = ('),
        ):
            with self.subTest(notebook=name):
                source = code_source(NOTEBOOKS / f"{name}.ipynb")
                drain = "process_worker_events(spark_session, writer, table_prefix=prefix, database=database)"
                self.assertIn(drain, source)
                self.assertLess(source.index(drain), source.index(candidate))

    def test_administrative_notebooks_default_to_closed_stop_gate(self):
        for name in ("00_bootstrap_lakehouse", "09_maintain_delta", "14_reset_test_data"):
            with self.subTest(notebook=name):
                source = code_source(NOTEBOOKS / f"{name}.ipynb")
                self.assertIn("CONFIRM_WRITERS_STOPPED = False", source)
                self.assertIn('parameter_bool(CONFIRM_WRITERS_STOPPED, "CONFIRM_WRITERS_STOPPED")', source)

    def test_attempt_metadata_is_submitted_as_partial_event(self):
        client = MagicMock()
        namespace = worker_functions("merge_attempt", event_client=client)
        updates = {"processed_frames": 42}
        namespace["merge_attempt"](updates)
        client.submit.assert_called_once_with("attempt_update", {"updates": updates})

    def test_worker_claim_waits_for_authoritative_acknowledgement(self):
        client = MagicMock()
        verify = MagicMock()
        namespace = worker_functions(
            "claim_worker_execution", event_client=client, verify_lease=verify,
        )
        work = {"status": "LEASED"}
        namespace["claim_worker_execution"](work)
        verify.assert_called_once_with(work)
        client.submit.assert_called_once_with("claim_execution", {})

    def test_claim_rejection_prevents_execution(self):
        client = MagicMock()
        error = RuntimeError("Another execution owns the attempt")
        client.submit.side_effect = error
        namespace = worker_functions(
            "claim_worker_execution", event_client=client, verify_lease=MagicMock(),
        )
        with self.assertRaises(RuntimeError) as raised:
            namespace["claim_worker_execution"]({})
        self.assertIs(raised.exception, error)

    def test_heartbeat_is_throttled_but_forced_updates_wait_for_writer(self):
        client = MagicMock()
        clock = MagicMock()
        clock.monotonic.return_value = 105.0
        now = datetime(2026, 9, 23, tzinfo=timezone.utc)
        verify = MagicMock()
        current = {"status": "RUNNING"}
        namespace = worker_functions(
            "heartbeat",
            event_client=client,
            time=clock,
            heartbeat_seconds=600,
            utc_now=lambda: now,
            PIPELINE_RUN_ID="pipeline",
            ACTIVITY_RUN_ID="activity",
            FABRIC_JOB_INSTANCE_ID="job",
            BUNDLE_MANIFEST_SHA256="bundle",
            version=lambda name: "test-sdk",
            load_work=lambda: current,
            verify_lease=verify,
            renew_pending_leases=MagicMock(),
        )
        heartbeat = namespace["heartbeat"]
        heartbeat.last_sent = 100.0
        result = MagicMock(processed_frames=7, processing_seconds=2.0)
        heartbeat("RUNNING", result)
        client.submit.assert_not_called()
        heartbeat(
            "RUNNING", result, force=True,
            updates={"inference_started_at": now, "status": "INVALID_OVERRIDE"},
        )
        client.submit.assert_called_once_with(
            "heartbeat",
            {
                "status": "RUNNING",
                "updates": {
                    "inference_started_at": now,
                    "status": "RUNNING",
                    "last_heartbeat_at": now,
                    "pipeline_run_id": "pipeline",
                    "activity_run_id": "activity",
                    "fabric_job_instance_id": "job",
                    "sdk_version": "test-sdk",
                    "bundle_manifest_sha256": "bundle",
                    "processed_frames": 7,
                    "processing_seconds": 2.0,
                },
            },
        )
        verify.assert_called_once_with(current)
        self.assertEqual(heartbeat.last_sent, 105.0)

    def test_failed_heartbeat_does_not_advance_throttle(self):
        client = MagicMock()
        client.submit.side_effect = RuntimeError("Writer unavailable")
        clock = MagicMock()
        clock.monotonic.return_value = 1000.0
        namespace = worker_functions(
            "heartbeat",
            event_client=client,
            time=clock,
            heartbeat_seconds=600,
            utc_now=MagicMock(),
            PIPELINE_RUN_ID="",
            ACTIVITY_RUN_ID="",
            FABRIC_JOB_INSTANCE_ID="",
            BUNDLE_MANIFEST_SHA256="",
            version=MagicMock(),
            load_work=MagicMock(),
            verify_lease=MagicMock(),
            renew_pending_leases=MagicMock(),
        )
        heartbeat = namespace["heartbeat"]
        heartbeat.last_sent = 0.0
        with self.assertRaisesRegex(RuntimeError, "Writer unavailable"):
            heartbeat("STAGING", force=True)
        self.assertEqual(heartbeat.last_sent, 0.0)
        namespace["load_work"].assert_not_called()

    def test_output_and_failure_snapshot_use_the_same_idempotent_append(self):
        frame = MagicMock()
        frame.take.return_value = [object()]
        writer = frame.write
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.option.return_value = writer
        namespace = worker_functions("append_attempt_rows", attempt_id="attempt-a")
        append = namespace["append_attempt_rows"]
        append("pc_telemetry_attempts", frame)
        append("pc_telemetry_attempts", frame)
        self.assertEqual(writer.mode.call_args_list, [call("append"), call("append")])
        self.assertEqual(
            writer.option.call_args_list,
            [
                call("txnAppId", "pc_telemetry_attempts:attempt-a"),
                call("txnVersion", 0),
            ] * 2,
        )
        self.assertEqual(
            writer.saveAsTable.call_args_list,
            [call("pc_telemetry_attempts"), call("pc_telemetry_attempts")],
        )

    def test_empty_output_does_not_create_a_transaction(self):
        frame = MagicMock()
        frame.take.return_value = []
        namespace = worker_functions("append_attempt_rows", attempt_id="attempt-a")
        namespace["append_attempt_rows"]("pc_telemetry_attempts", frame)
        frame.write.format.assert_not_called()

    def test_output_error_propagates_without_delete_or_overwrite(self):
        frame = MagicMock()
        frame.take.return_value = [object()]
        writer = frame.write
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.option.return_value = writer
        writer.saveAsTable.side_effect = OSError("OneLake unavailable")
        namespace = worker_functions("append_attempt_rows", attempt_id="attempt-a")
        with self.assertRaisesRegex(OSError, "OneLake unavailable"):
            namespace["append_attempt_rows"]("pc_telemetry_attempts", frame)
        writer.mode.assert_called_once_with("append")

    def test_success_waits_for_commit_and_cleans_staging(self):
        namespace = self.worker_run_namespace()
        self.execute_worker(namespace)
        client = namespace["event_client"]
        client.submit.assert_any_call("claim_execution", {})
        self.assertEqual(client.submit.call_args, call("commit", {}))
        self.assertEqual(namespace["outcome"]["status"], "SUCCEEDED")
        self.assertEqual(namespace["outcome"]["items"][0]["status"], "SUCCEEDED")
        self.assertEqual(namespace["append_attempt_rows"].call_count, 2)
        namespace["verify_lease"].assert_called_with(namespace["work"])
        namespace["staged"].unlink.assert_called_once_with(missing_ok=True)
        namespace["shutil"].rmtree.assert_any_call(
            namespace["temporary"],
            ignore_errors=True,
        )

    def test_inference_failure_is_submitted_before_batch_failure(self):
        namespace = self.worker_run_namespace()
        work = {
            "capture_date": date(2023, 9, 29),
            "status": "LEASED",
            "committed_attempt_id": None,
        }
        namespace["load_work"].side_effect = [work, {"status": "RUNNING", "committed_attempt_id": None}]
        error = OSError("Inference failed")
        namespace["run_with_runtime"].side_effect = error
        with self.assertRaisesRegex(
            RuntimeError,
            "Worker batch completed with 1 failed item",
        ):
            self.execute_worker(namespace)
        kind, payload = namespace["event_client"].submit.call_args.args
        self.assertEqual(kind, "failure")
        self.assertEqual(payload["error_message"], "Inference failed")
        self.assertEqual(payload["error_type"], "OSError")
        self.assertTrue(payload["retryable"])
        self.assertFalse(payload["lease_lost"])
        self.assertNotIn(call("commit", {}), namespace["event_client"].submit.call_args_list)
        namespace["append_attempt_rows"].assert_not_called()
        namespace["staged"].unlink.assert_called_once_with(missing_ok=True)
        self.assertEqual(
            namespace["outcome"]["items"][0]["error_message"],
            "Inference failed",
        )

    def test_ambiguous_commit_does_not_turn_published_work_into_failure(self):
        namespace = self.worker_run_namespace()

        def submit(kind, payload):
            if kind == "commit":
                raise OSError("Receipt response lost after publication")

        namespace["WorkerEventClient"].return_value.submit.side_effect = submit
        self.execute_worker(namespace)
        self.assertEqual(namespace["outcome"]["status"], "SUCCEEDED")
        self.assertEqual(
            namespace["outcome"]["items"][0]["status"],
            "SUCCEEDED_AFTER_AMBIGUOUS_COMMIT",
        )
        self.assertFalse(
            any(item.args[0] == "failure" for item in namespace["event_client"].submit.call_args_list)
        )


if __name__ == "__main__":
    unittest.main()
