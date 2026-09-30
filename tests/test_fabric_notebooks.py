import ast
import hashlib
import io
import json
import math
import re
import unittest
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from people_counter.cpu_runtime import calculate_placement_safe_thread_budget
from people_counter.runtime import RuntimeCompatibilityError


NOTEBOOKS = Path(__file__).resolve().parents[1] / "notebooks" / "fabric"
WORKER = NOTEBOOKS / "04_process_video.ipynb"
BOOTSTRAP = NOTEBOOKS / "00_bootstrap_lakehouse.ipynb"
BENCHMARK = NOTEBOOKS / "08_capacity_benchmark.ipynb"
EXECUTOR_PROTOTYPE = NOTEBOOKS / "15_executor_partition_inference.ipynb"
EXECUTOR_BENCHMARK_CONTROL = NOTEBOOKS / "16_executor_partition_benchmark_control.ipynb"
EXECUTOR_BENCHMARK_PIPELINE = (
    NOTEBOOKS / "exports" / "pc-executor-partition-benchmark.json"
)


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
                    source = "".join(cell["source"])
                    if source.lstrip().startswith("%%configure"):
                        continue
                    with self.subTest(notebook=path.name, cell=cell["id"]):
                        compile(source, f"{path}:{cell['id']}", "exec")

    def test_executor_partition_configures_spark_before_parameter_cell(self):
        notebook = json.loads(EXECUTOR_PROTOTYPE.read_text(encoding="utf-8"))
        code_cells = [
            cell for cell in notebook["cells"] if cell["cell_type"] == "code"
        ]

        self.assertEqual(code_cells[0]["id"], "spark-session-config")
        self.assertEqual(code_cells[1]["id"], "parameters")
        self.assertIn("parameters", code_cells[1]["metadata"]["tags"])

        magic, payload = "".join(code_cells[0]["source"]).split("\n", 1)
        self.assertEqual(magic, "%%configure")
        configuration = json.loads(payload)
        self.assertEqual(
            configuration["conf"]["spark.task.cpus"],
            {
                "parameterName": "CPUS_PER_TASK",
                "defaultValue": "1",
            },
        )
        self.assertEqual(configuration["conf"]["spark.speculation"], "false")
        self.assertEqual(configuration["executorCores"]["defaultValue"], 4)
        self.assertEqual(configuration["executorMemory"]["defaultValue"], "28g")

    def test_executor_partition_prototype_is_capacity_aware(self):
        parameters = cell_source(EXECUTOR_PROTOTYPE, "parameters")
        spark_config = cell_source(EXECUTOR_PROTOTYPE, "spark-config")
        planning = cell_source(EXECUTOR_PROTOTYPE, "prepared-delta-input")
        partition = cell_source(EXECUTOR_PROTOTYPE, "map-partitions")

        self.assertIn('OUTPUT_TXN_APP_ID = "UNSET"', parameters)
        self.assertIn("OUTPUT_TXN_VERSION = -1", parameters)
        self.assertIn("CPUS_PER_TASK = 1", parameters)
        self.assertIn('PARALLEL_TASKS = "auto"', parameters)
        self.assertIn("PARTITION_WAVES = 3", parameters)
        self.assertIn("PEAK_WORKER_MEMORY_GIB = 0.0", parameters)
        self.assertIn("USABLE_EXECUTOR_MEMORY_GIB = 0.0", parameters)
        self.assertNotIn("\nEXECUTOR_CORES =", parameters)
        self.assertNotIn("\nACTIVE_TASKS_PER_EXECUTOR =", parameters)
        self.assertNotIn("\nTARGET_PARTITIONS =", parameters)
        self.assertNotIn('conf.set("spark.task.cpus"', spark_config)
        self.assertNotIn("configure_cpu_runtime(", spark_config)
        self.assertIn(
            "spark-driver-status-store",
            spark_config,
        )
        self.assertIn(
            "sum(item[\"slots\"] for item in slot_details)",
            spark_config,
        )
        self.assertIn("executor_cgroup_memory_sample()", spark_config)
        self.assertIn("normalize_executor_id(raw_executor_id)", spark_config)
        self.assertIn("calculate_auto_usable_executor_memory_bytes(", spark_config)
        self.assertIn("memory_characterization_mode = PEAK_WORKER_MEMORY_GIB == 0", spark_config)
        self.assertIn("planned_concurrency = planned_worker_concurrency(", planning)
        self.assertIn("memory_concurrency_cap", planning)
        self.assertIn("memory_cap_mode", planning)
        self.assertIn(
            "native_thread_budget = calculate_placement_safe_thread_budget(",
            planning,
        )
        self.assertIn(
            "(executor.total_cores for executor in resource_snapshot_before.executors)",
            planning,
        )
        self.assertIn("PARALLEL_TASKS exceeds the placement-safe global memory cap", planning)
        self.assertIn("if memory_characterization_mode", planning)
        self.assertIn("choose_bucket(", planning)
        self.assertIn("toLocalIterator()", planning)
        self.assertIn(".partitionBy(", partition)
        self.assertIn("task_cpus = normalize_task_cpus(context.cpus())", partition)
        self.assertIn(
            "driver_cores=native_thread_budget_cores",
            partition,
        )
        self.assertIn(
            "active_workers=native_thread_budget_workers",
            partition,
        )
        self.assertNotIn("driver_cores=task_cpus", partition)
        self.assertIn("thread_budget.threads_per_worker != native_threads_per_worker", partition)
        self.assertIn("worker_lifetime_peak_rss_bytes", partition)
        self.assertIn("linux-proc-vmhwm", partition)
        self.assertIn("models_dir is required for offline executor inference", partition)
        self.assertIn("supports only CPU inference", partition)

    def test_executor_partition_normalizes_integral_task_cpu_allocations(self):
        normalize_task_cpus = cell_functions(
            EXECUTOR_PROTOTYPE,
            "map-partitions",
            "normalize_task_cpus",
            math=math,
        )["normalize_task_cpus"]

        for value in (1, 1.0, "1", "1.0", 4.0):
            with self.subTest(value=value):
                self.assertEqual(normalize_task_cpus(value), int(float(value)))
        for value in (True, None, 0, 0.0, 1.5, float("inf"), "invalid"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "Invalid Spark task CPU allocation",
                ):
                    normalize_task_cpus(value)

    def test_executor_partition_slot_details_preserve_executor_boundaries(self):
        helpers = cell_functions(
            EXECUTOR_PROTOTYPE,
            "spark-config",
            "executor_slot_details",
            "positive_int_parameter",
            re=re,
        )
        executor_slot_details = helpers["executor_slot_details"]
        positive_int_parameter = helpers["positive_int_parameter"]
        executors = [
            MagicMock(executor_id="executor-a", total_cores=8),
            MagicMock(executor_id="executor-b", total_cores=5),
        ]

        details = executor_slot_details(executors, 3)

        self.assertEqual([item["slots"] for item in details], [2, 1])
        self.assertEqual([item["fragment_cores"] for item in details], [2, 2])
        self.assertEqual(sum(item["slots"] for item in details), 3)
        self.assertEqual(positive_int_parameter(5, "CPUS_PER_TASK"), 5)
        self.assertEqual(positive_int_parameter("5", "CPUS_PER_TASK"), 5)
        for invalid in (0, True, 5.0, "0", "5.0", "auto", None):
            with self.subTest(invalid_parameter=invalid):
                with self.assertRaisesRegex(ValueError, "positive integer"):
                    positive_int_parameter(invalid, "CPUS_PER_TASK")

        spark_config = cell_source(EXECUTOR_PROTOTYPE, "spark-config")
        self.assertIn("memory_concurrency_cap", spark_config)
        self.assertNotIn("minimum_safe_cpus_per_task=", spark_config)

    def test_executor_partition_memory_parser_uses_binary_units(self):
        parse_spark_memory_bytes = cell_functions(
            EXECUTOR_PROTOTYPE,
            "spark-config",
            "parse_spark_memory_bytes",
            re=re,
        )["parse_spark_memory_bytes"]

        self.assertEqual(parse_spark_memory_bytes("1.5g"), int(1.5 * 1024**3))
        self.assertEqual(parse_spark_memory_bytes("512 MiB"), 512 * 1024**2)
        self.assertEqual(
            parse_spark_memory_bytes("4096", default_unit="m"),
            4 * 1024**3,
        )
        with self.assertRaisesRegex(ValueError, "Unsupported Spark memory value"):
            parse_spark_memory_bytes("unbounded")

    def test_executor_partition_derives_spark_worker_memory_envelope(self):
        namespace = cell_functions(
            EXECUTOR_PROTOTYPE,
            "spark-config",
            "parse_spark_memory_bytes",
            "optional_spark_memory_bytes",
            "configured_executor_memory_envelope",
            math=math,
            re=re,
        )
        gib = 1024**3
        derive = namespace["configured_executor_memory_envelope"]

        explicit = derive(
            {
                "spark.executor.memoryOverhead": "6144",
                "spark.memory.offHeap.enabled": "true",
                "spark.memory.offHeap.size": "2g",
            },
            28 * gib,
        )
        self.assertEqual(
            explicit["configured_worker_budget_bytes"],
            int(4.5 * gib),
        )
        self.assertEqual(explicit["executor_native_reserve_bytes"], int(1.5 * gib))
        self.assertEqual(explicit["reserved_outside_workers_bytes"], 30 * gib)
        self.assertEqual(
            explicit["source"],
            "spark.executor.memoryOverhead",
        )

        pyspark = derive(
            {
                "spark.executor.memoryOverhead": "4g",
                "spark.executor.pyspark.memory": "8g",
                "spark.memory.offHeap.enabled": "true",
                "spark.memory.offHeap.size": "2g",
            },
            28 * gib,
        )
        self.assertEqual(pyspark["configured_worker_budget_bytes"], 8 * gib)
        self.assertEqual(pyspark["reserved_outside_workers_bytes"], 34 * gib)
        self.assertEqual(pyspark["source"], "spark.executor.pyspark.memory")

        derived = derive({}, 28 * gib)
        derived_overhead = int((28 * gib // 1024**2) * 0.10) * 1024**2
        self.assertEqual(
            derived["configured_worker_budget_bytes"],
            derived_overhead - int(derived_overhead * 0.25),
        )
        self.assertEqual(derived["source"], "derived-spark-memory-overhead")
        managed_minimum = derive(
            {"spark.executor.memoryOverhead": "384m"}, 28 * gib
        )
        self.assertEqual(managed_minimum["configured_worker_budget_bytes"], 0)
        self.assertEqual(
            managed_minimum["source"],
            "fabric-node-memory-minus-spark-reservations",
        )

    def test_executor_partition_reports_unbounded_fabric_cgroup(self):
        sample_memory = cell_functions(
            EXECUTOR_PROTOTYPE,
            "spark-config",
            "executor_cgroup_memory_sample",
        )["executor_cgroup_memory_sample"]

        def fake_open(path, *args, **kwargs):
            path = str(path)
            if path == "/proc/self/statm":
                return io.StringIO("100 10 0 0 0 0 0\n")
            if path == "/proc/self/cgroup":
                return io.StringIO("0::/system.slice/yarn-nm.service\n")
            if path.endswith("/memory.max"):
                return io.StringIO("max\n")
            raise FileNotFoundError(path)

        def fake_sysconf(name):
            return 4_194_304 if name == "SC_PHYS_PAGES" else 4096

        with patch("builtins.open", side_effect=fake_open), patch(
            "os.sysconf", side_effect=fake_sysconf
        ):
            sample = sample_memory()

        self.assertIsNone(sample["cgroup_limit_bytes"])
        self.assertIsNone(sample["cgroup_current_bytes"])
        self.assertEqual(sample["python_worker_rss_bytes"], 10 * 4096)
        self.assertEqual(sample["physical_memory_bytes"], 16 * 1024**3)
        self.assertIn(
            "/sys/fs/cgroup/system.slice/yarn-nm.service/memory.max",
            sample["cgroup_limit_paths_checked"],
        )
        self.assertIn("no finite readable limit", sample["cgroup_error"])

    def test_executor_partition_ignores_fabric_none_executor_id(self):
        namespace = cell_functions(
            EXECUTOR_PROTOTYPE,
            "spark-config",
            "normalize_executor_id",
            "executor_memory_probe_coverage",
        )
        normalize = namespace["normalize_executor_id"]
        coverage = namespace["executor_memory_probe_coverage"]
        probes = [
            {
                "probe_key": "vm-c7249084",
                "executor_id": "None",
                "executor_id_raw": "None",
            }
        ]

        self.assertEqual(normalize(" 1 "), "1")
        self.assertEqual(normalize("None"), "")
        complete, details = coverage(probes, {"1"})

        self.assertTrue(complete)
        self.assertEqual(details["identity_mode"], "host-fallback")
        self.assertEqual(details["observed_probe_keys"], ["vm-c7249084"])
        self.assertEqual(details["ignored_executor_id_values"], ["None"])
        self.assertEqual(details["missing_executor_count"], 0)

    def test_executor_partition_auto_memory_uses_smallest_executor_budget(self):
        namespace = cell_functions(
            EXECUTOR_PROTOTYPE,
            "spark-config",
            "normalize_executor_id",
            "executor_memory_probe_coverage",
            "calculate_auto_usable_executor_memory_bytes",
        )
        calculate = namespace["calculate_auto_usable_executor_memory_bytes"]
        gib = 1024**3
        envelope = {
            "configured_worker_budget_bytes": 6 * gib,
            "reserved_outside_workers_bytes": 30 * gib,
        }
        probes = [
            {
                "probe_key": "executor-1",
                "executor_id": "1",
                "hostname": "worker-1",
                "cgroup_limit_bytes": 40 * gib,
                "cgroup_current_bytes": 10 * gib,
                "python_worker_rss_bytes": 1 * gib,
                "physical_memory_bytes": 64 * gib,
                "cgroup_limit_path": "/sys/fs/cgroup/memory.max",
            },
            {
                "probe_key": "executor-2",
                "executor_id": "2",
                "hostname": "worker-2",
                "cgroup_limit_bytes": 40 * gib,
                "cgroup_current_bytes": 38 * gib,
                "python_worker_rss_bytes": 2 * gib,
                "physical_memory_bytes": 64 * gib,
                "cgroup_limit_path": "/sys/fs/cgroup/memory.max",
            },
        ]

        usable, details = calculate(probes, {"1", "2"}, envelope)

        self.assertEqual(usable, 2 * gib)
        self.assertEqual(
            [item["usable_memory_bytes"] for item in details],
            [6 * gib, 2 * gib],
        )
        self.assertEqual(
            {item["budget_source"] for item in details},
            {"spark-config+fabric-node-envelope+cgroup"},
        )
        with self.assertRaisesRegex(RuntimeError, "did not cover every runnable executor"):
            calculate(probes[:1], {"1", "2"}, envelope)

    def test_executor_partition_auto_memory_falls_back_to_spark_envelope(self):
        namespace = cell_functions(
            EXECUTOR_PROTOTYPE,
            "spark-config",
            "normalize_executor_id",
            "executor_memory_probe_coverage",
            "calculate_auto_usable_executor_memory_bytes",
        )
        calculate = namespace["calculate_auto_usable_executor_memory_bytes"]
        gib = 1024**3
        envelope = {
            "configured_worker_budget_bytes": 6 * gib,
            "reserved_outside_workers_bytes": 30 * gib,
        }
        checked_path = "/sys/fs/cgroup/system.slice/yarn-nm.service/memory.max"
        probes = [
            {
                "probe_key": "executor-1",
                "executor_id": "1",
                "hostname": "worker-1",
                "cgroup_limit_bytes": None,
                "cgroup_current_bytes": None,
                "cgroup_reclaimable_cache_bytes": None,
                "cgroup_working_set_bytes": None,
                "python_worker_rss_bytes": 1 * gib,
                "physical_memory_bytes": 64 * gib,
                "cgroup_limit_path": None,
                "cgroup_limit_paths_checked": [checked_path],
                "cgroup_error": "Executor memory cgroup has no finite readable limit",
            }
        ]

        usable, details = calculate(probes, {"1"}, envelope)

        self.assertEqual(usable, 6 * gib)
        self.assertEqual(len(details), 1)
        self.assertEqual(details[0]["usable_memory_bytes"], 6 * gib)
        self.assertEqual(
            details[0]["budget_source"],
            "spark-config+fabric-node-envelope",
        )
        self.assertEqual(details[0]["cgroup_limit_paths"], [checked_path])
        self.assertIsNone(details[0]["cgroup_limit_bytes"])
        self.assertEqual(
            details[0]["cgroup_errors"],
            ["Executor memory cgroup has no finite readable limit"],
        )

    def test_executor_partition_uses_fabric_node_memory_for_managed_overhead(self):
        namespace = cell_functions(
            EXECUTOR_PROTOTYPE,
            "spark-config",
            "normalize_executor_id",
            "executor_memory_probe_coverage",
            "calculate_auto_usable_executor_memory_bytes",
        )
        calculate = namespace["calculate_auto_usable_executor_memory_bytes"]
        gib = 1024**3
        envelope = {
            "configured_worker_budget_bytes": 0,
            "reserved_outside_workers_bytes": 28 * gib,
            "executor_native_reserve_bytes": 384 * 1024**2,
            "executor_pyspark_memory_bytes": None,
        }
        probes = [
            {
                "probe_key": "worker-1",
                "executor_id": None,
                "executor_id_raw": "None",
                "hostname": "worker-1",
                "cgroup_limit_bytes": None,
                "cgroup_current_bytes": None,
                "python_worker_rss_bytes": 1 * gib,
                "physical_memory_bytes": 32 * gib,
                "cgroup_limit_paths_checked": ["/sys/fs/cgroup/memory.max"],
                "cgroup_error": "no finite readable limit",
            }
        ]

        usable, details = calculate(probes, {"1"}, envelope)

        self.assertEqual(usable, 3 * gib)
        self.assertEqual(details[0]["fabric_node_capacity_bytes"], 4 * gib)
        self.assertEqual(details[0]["fabric_node_native_reserve_bytes"], 1 * gib)
        self.assertEqual(details[0]["fabric_node_worker_budget_bytes"], 3 * gib)
        self.assertEqual(details[0]["budget_source"], "fabric-node-envelope")

    def test_executor_partition_parses_and_rounds_worker_peak_memory(self):
        parse_status = cell_functions(
            EXECUTOR_PROTOTYPE,
            "map-partitions",
            "parse_proc_status_memory",
        )["parse_proc_status_memory"]
        rounded_peak = cell_functions(
            EXECUTOR_PROTOTYPE,
            "controlled-delta-persistence",
            "rounded_peak_memory_gib",
            math=math,
        )["rounded_peak_memory_gib"]

        self.assertEqual(
            parse_status("Name:\tpython\nVmHWM:\t2097152 kB\nVmRSS:\t1048576 kB\n"),
            {
                "rss_bytes": 1 * 1024**3,
                "peak_rss_bytes": 2 * 1024**3,
            },
        )
        self.assertEqual(
            rounded_peak(int(2.01 * 1024**3)),
            2.25,
        )
        self.assertEqual(
            rounded_peak(2 * 1024**3),
            2.25,
        )
        with self.assertRaisesRegex(RuntimeError, "positive VmRSS and VmHWM"):
            parse_status("VmRSS:\t1024 kB\n")

    def test_executor_partition_bucket_choice_is_balanced_and_stable(self):
        helpers = cell_functions(
            EXECUTOR_PROTOTYPE,
            "prepared-delta-input",
            "choose_bucket",
            "planned_worker_concurrency",
        )
        choose_bucket = helpers["choose_bucket"]
        planned_worker_concurrency = helpers["planned_worker_concurrency"]

        self.assertEqual(choose_bucket(4.0, "new", [2.0, 2.0], {}, 0.1), 0)
        self.assertEqual(
            choose_bucket(1.0, "shared", [2.0, 2.1], {"shared": 1}, 0.1),
            1,
        )
        self.assertEqual(
            choose_bucket(4.0, "shared", [1.0, 8.0], {"shared": 1}, 0.1),
            0,
        )
        self.assertEqual(
            planned_worker_concurrency(16, 3, 10, "auto", False),
            3,
        )
        self.assertEqual(
            planned_worker_concurrency(16, 3, 10, 2, False),
            2,
        )
        self.assertEqual(
            planned_worker_concurrency(16, 0, 10, "auto", True),
            1,
        )
        with self.assertRaisesRegex(
            RuntimeError,
            "placement-safe global memory cap",
        ):
            planned_worker_concurrency(16, 3, 10, 4, False)

    def test_executor_partition_preserves_fabric_spark_ui_query_parameters(self):
        endpoint = cell_functions(
            EXECUTOR_PROTOTYPE,
            "spark-config",
            "spark_monitoring_executors_endpoint",
            quote=quote,
            urlsplit=urlsplit,
            urlunsplit=urlunsplit,
        )["spark_monitoring_executors_endpoint"]

        self.assertEqual(
            endpoint(
                "https://example.test/sparkui/activity/?artifactId=item&workspace=dev",
                "application/1",
            ),
            "https://example.test/sparkui/activity/api/v1/applications/"
            "application%2F1/executors?artifactId=item&workspace=dev",
        )
        self.assertEqual(
            endpoint("http://localhost:4040/", "application-1"),
            "http://localhost:4040/api/v1/applications/application-1/executors",
        )
        with self.assertRaisesRegex(ValueError, "Invalid Spark UI URL"):
            endpoint("sparkui/activity?artifactId=item", "application-1")

    def test_executor_partition_reads_active_executor_status_summaries(self):
        @dataclass
        class Resource:
            executor_id: str
            host_port: str
            total_cores: int
            max_storage_memory_bytes: int
            active_tasks: int

        class Summary:
            def __init__(self, identifier, active, cores):
                self.identifier = identifier
                self.active = active
                self.cores = cores

            def id(self):
                return self.identifier

            def isActive(self):
                return self.active

            def totalCores(self):
                return self.cores

            def hostPort(self):
                return f"{self.identifier}:1234"

            def maxMemory(self):
                return 1024

            def activeTasks(self):
                return 1

        class Iterator:
            def __init__(self, values):
                self.values = iter(values)
                self.current = None

            def hasNext(self):
                if self.current is None:
                    self.current = next(self.values, None)
                return self.current is not None

            def next(self):
                value, self.current = self.current, None
                return value

        converter = cell_functions(
            EXECUTOR_PROTOTYPE,
            "spark-config",
            "executor_resources_from_summaries",
            ExecutorResource=Resource,
        )["executor_resources_from_summaries"]
        resources = converter(
            Iterator(
                [
                    Summary("2", True, 4),
                    Summary("driver", True, 4),
                    Summary("1", True, 2),
                    Summary("removed", False, 4),
                ]
            )
        )

        self.assertEqual([item.executor_id for item in resources], ["1", "2"])
        self.assertEqual([item.total_cores for item in resources], [2, 4])
        with self.assertRaisesRegex(ValueError, "Invalid active executor summary"):
            converter(Iterator([Summary("bad", True, 0)]))

    def test_executor_partition_scopes_and_persists_benchmark_identity(self):
        parameters = cell_source(EXECUTOR_PROTOTYPE, "parameters")
        spark_config = cell_source(EXECUTOR_PROTOTYPE, "spark-config")
        planning = cell_source(EXECUTOR_PROTOTYPE, "prepared-delta-input")
        execution = cell_source(EXECUTOR_PROTOTYPE, "map-partitions")
        persistence = cell_source(
            EXECUTOR_PROTOTYPE,
            "controlled-delta-persistence",
        )

        for name in (
            "INPUT_BATCH_ID",
            "CAPACITY_SKU",
            "RUNTIME_VERSION",
            "CONFIG_SHA256",
        ):
            self.assertIn(f'{name} = ""', parameters)
        self.assertIn(
            "if any(provided_identity) and not all(provided_identity)",
            spark_config,
        )
        self.assertIn('F.col("benchmark_batch_id") == benchmark_batch_id', planning)
        self.assertIn('F.col("capacity_sku") == capacity_sku', planning)
        self.assertIn('F.col("runtime_version") == runtime_version', planning)
        self.assertIn('F.col("config_sha256") == config_sha256', planning)
        for source in (planning, execution, persistence):
            for field in (
                "benchmark_batch_id",
                "capacity_sku",
                "runtime_version",
                "config_sha256",
            ):
                self.assertIn(f'"{field}"', source)

    def test_executor_benchmark_control_has_parameterized_modes(self):
        notebook = json.loads(
            EXECUTOR_BENCHMARK_CONTROL.read_text(encoding="utf-8")
        )
        code_cells = {
            cell["id"]: cell
            for cell in notebook["cells"]
            if cell["cell_type"] == "code"
        }

        self.assertIn("parameters", code_cells["control-parameters"]["metadata"]["tags"])
        self.assertIn('MODE = "PREPARE"', cell_source(
            EXECUTOR_BENCHMARK_CONTROL,
            "control-parameters",
        ))
        self.assertIn("def prepare_benchmark():", cell_source(
            EXECUTOR_BENCHMARK_CONTROL,
            "prepare-benchmark",
        ))
        self.assertIn("def evaluate_benchmark():", cell_source(
            EXECUTOR_BENCHMARK_CONTROL,
            "evaluate-benchmark",
        ))

    def test_executor_benchmark_parses_items_and_rejects_metadata_drift(self):
        namespace = cell_functions(
            EXECUTOR_BENCHMARK_CONTROL,
            "control-helpers",
            "positive_finite",
            "optional_positive_finite",
            "optional_nonnegative_int",
            "parse_benchmark_items",
            "relative_difference",
            "resolve_media_metadata",
            json=json,
            math=math,
        )
        items = namespace["parse_benchmark_items"](
            json.dumps(
                [
                    {
                        "video_uri": "abfss://container/video.mp4",
                        "sample_name": "sample-a",
                    }
                ]
            ),
            2,
        )

        self.assertEqual(items[0]["ordinal"], 0)
        self.assertIsNone(items[0]["duration_seconds"])
        metadata = namespace["resolve_media_metadata"](
            items[0],
            {
                "duration_seconds": 60.0,
                "source_width": 1920,
                "source_height": 1080,
                "source_fps": 30.0,
                "source_frame_count": 1800,
                "codec": "avc1",
            },
            0.02,
        )
        self.assertEqual(metadata["duration_seconds"], 60.0)
        supplied = dict(items[0], duration_seconds=55.0)
        with self.assertRaisesRegex(ValueError, "duration_seconds mismatch"):
            namespace["resolve_media_metadata"](
                supplied,
                {
                    "duration_seconds": 60.0,
                    "source_width": 1920,
                    "source_height": 1080,
                    "source_fps": 30.0,
                    "source_frame_count": 1800,
                    "codec": "avc1",
                },
                0.02,
            )

    def test_executor_benchmark_hashes_and_work_ids_are_stable(self):
        namespace = cell_functions(
            EXECUTOR_BENCHMARK_CONTROL,
            "control-helpers",
            "stable_hash",
            "deterministic_work_id",
            hashlib=hashlib,
            json=json,
        )
        stable_hash = namespace["stable_hash"]
        deterministic_work_id = namespace["deterministic_work_id"]

        self.assertEqual(
            stable_hash({"b": 2, "a": 1}),
            stable_hash({"a": 1, "b": 2}),
        )
        self.assertEqual(
            deterministic_work_id("batch", 0, "video.mp4"),
            deterministic_work_id("batch", 0, "video.mp4"),
        )
        self.assertNotEqual(
            deterministic_work_id("batch", 0, "video.mp4"),
            deterministic_work_id("batch", 1, "video.mp4"),
        )

    def test_executor_benchmark_reports_incomplete_characterization_without_none_value(self):
        helpers = cell_functions(
            EXECUTOR_BENCHMARK_CONTROL,
            "control-helpers",
            "characterization_rejection_message",
            "characterization_capacity_recommendation",
            "optional_text",
            calculate_placement_safe_thread_budget=calculate_placement_safe_thread_budget,
            json=json,
            math=math,
        )
        rejection_message = helpers["characterization_rejection_message"]
        capacity_recommendation = helpers[
            "characterization_capacity_recommendation"
        ]
        intentional_failure = (
            "characterization-only run cannot pass benchmark approval; rerun "
            "with a new batch ID and PEAK_WORKER_MEMORY_GIB=2.25"
        )

        self.assertEqual(helpers["optional_text"](None), "")
        self.assertEqual(helpers["optional_text"](" batch-a "), "batch-a")
        recommendation = capacity_recommendation(
            8.0,
            0.25,
            2.0,
            json.dumps([{"executor_id": "1", "total_cores": 16}]),
            1,
        )
        self.assertEqual(
            recommendation,
            {
                "memory_safe_workers_per_executor": 3,
                "placement_safe_parallel_tasks": 3,
                "executor_cores": [16],
                "native_threads_per_worker_at_cap": 5,
            },
        )
        self.assertEqual(
            rejection_message(2.25, 3, 5, 1, [intentional_failure]),
            "Executor peak-memory characterization completed but cannot be "
            "approved; use a new benchmark batch ID with "
            "PEAK_WORKER_MEMORY_GIB=2.25 and CPUS_PER_TASK=1; "
            "PARALLEL_TASKS=auto will apply the placement-safe global cap of 3 "
            "and derive 5 native threads per worker at that cap",
        )
        incomplete = rejection_message(
            None,
            None,
            None,
            1,
            [
                "expected exactly one run metric row, found 0",
                intentional_failure,
            ],
        )
        self.assertIn("characterization did not complete", incomplete)
        self.assertIn("expected exactly one run metric row, found 0", incomplete)
        self.assertNotIn("PEAK_WORKER_MEMORY_GIB=None", incomplete)

    def test_executor_benchmark_resolves_attached_lakehouse_file_api_paths(self):
        namespace = cell_functions(
            EXECUTOR_BENCHMARK_CONTROL,
            "control-helpers",
            "normalized_text_parameter",
            "onelake_file_api_path",
            json=json,
            unquote=unquote,
            urlsplit=urlsplit,
        )
        normalize = namespace["normalized_text_parameter"]
        resolver = namespace["onelake_file_api_path"]
        lakehouse_id = "883cff91-eaa8-40be-870f-6e9716303cb2"
        uri = (
            "abfss://workspace@onelake.dfs.fabric.microsoft.com/"
            f"{lakehouse_id}/Files/videos/sample%20clip.mp4"
        )

        self.assertEqual(normalize(lakehouse_id, "ID"), lakehouse_id)
        self.assertEqual(normalize(json.dumps(lakehouse_id), "ID"), lakehouse_id)
        with self.assertRaisesRegex(ValueError, "invalid JSON string quoting"):
            normalize('"unterminated', "ID")
        self.assertEqual(
            resolver(uri, lakehouse_id, "/lakehouse/default/"),
            "/lakehouse/default/Files/videos/sample clip.mp4",
        )
        for invalid_uri in (
            uri.replace(lakehouse_id, "different-lakehouse"),
            uri.replace("sample%20clip.mp4", "%2E%2E/clip.mp4"),
            f"{uri}?version=1",
            "https://onelake.dfs.fabric.microsoft.com/video.mp4",
        ):
            with self.subTest(invalid_uri=invalid_uri):
                with self.assertRaises(ValueError):
                    resolver(invalid_uri, lakehouse_id, "/lakehouse/default")

    def test_executor_benchmark_prepares_direct_paths_with_stage_progress(self):
        parameters = cell_source(
            EXECUTOR_BENCHMARK_CONTROL,
            "control-parameters",
        )
        prepare = cell_source(
            EXECUTOR_BENCHMARK_CONTROL,
            "prepare-benchmark",
        )

        self.assertIn('ATTACHED_LAKEHOUSE_ID = ""', parameters)
        self.assertIn(
            'LAKEHOUSE_FILE_API_ROOT = "/lakehouse/default"',
            parameters,
        )
        self.assertNotIn("STAGING_ROOT", parameters)
        self.assertNotIn("notebookutils.fs.cp", prepare)
        self.assertIn("onelake_file_api_path(", prepare)
        self.assertIn("Path(local_video_path).is_file()", prepare)
        self.assertIn('"source_access_mode": "DIRECT_FILE_API"', prepare)
        self.assertIn('"EXECUTOR_BENCHMARK_PREPARE_PROGRESS"', prepare)
        for stage in (
            "VALIDATE",
            "SOURCE_PROBE",
            "EXECUTOR_PREFLIGHT",
            "INPUT_WRITE",
            "EVENT_WRITE",
        ):
            self.assertIn(f'"{stage}"', prepare)

    def test_executor_benchmark_bootstrap_and_pipeline_contract(self):
        bootstrap_parameters = cell_source(BOOTSTRAP, "bootstrap-parameters")
        inference_parameters = cell_source(EXECUTOR_PROTOTYPE, "parameters")
        control_parameters = cell_source(
            EXECUTOR_BENCHMARK_CONTROL,
            "control-parameters",
        )
        bootstrap_create = cell_source(BOOTSTRAP, "bootstrap-tables")
        bootstrap_evolution = cell_source(
            BOOTSTRAP,
            "bootstrap-schema-evolution",
        )
        evaluation = cell_source(
            EXECUTOR_BENCHMARK_CONTROL,
            "evaluate-benchmark",
        )
        for suffix in (
            "executor_partition_input",
            "executor_partition_records",
            "executor_resource_snapshots",
            "executor_partition_plans",
            "executor_inference_runs",
            "executor_benchmark_events",
        ):
            self.assertIn(f'"{suffix}":', bootstrap_create)
        for suffix in (
            "executor_partition_records",
            "executor_resource_snapshots",
            "executor_partition_plans",
            "executor_inference_runs",
        ):
            self.assertIn(f'"{suffix}":', bootstrap_evolution)
        self.assertIn("usable_executor_memory_source STRING NOT NULL", bootstrap_create)
        self.assertIn("memory_budget_details_json STRING NOT NULL", bootstrap_create)
        self.assertIn("memory_characterization_mode BOOLEAN NOT NULL", bootstrap_create)
        self.assertIn("suggested_peak_worker_memory_gib DOUBLE", bootstrap_create)
        self.assertIn("native_threads_per_worker INT NOT NULL", bootstrap_create)
        self.assertIn("peak_worker_memory_details_json STRING NOT NULL", bootstrap_create)
        self.assertIn('"usable_executor_memory_source": "STRING"', bootstrap_evolution)
        self.assertIn('"memory_budget_details_json": "STRING"', bootstrap_evolution)
        self.assertIn('"memory_characterization_mode": "BOOLEAN"', bootstrap_evolution)
        self.assertIn('"suggested_peak_worker_memory_gib": "DOUBLE"', bootstrap_evolution)
        self.assertIn('"native_threads_per_worker": "INT"', bootstrap_evolution)
        self.assertIn('"peak_worker_memory_details_json": "STRING"', bootstrap_evolution)
        self.assertIn(
            "characterization-only run cannot pass benchmark approval",
            evaluation,
        )
        self.assertIn(
            "configured peak worker memory changed during execution",
            evaluation,
        )
        self.assertIn(
            'if float(PEAK_WORKER_MEMORY_GIB) == 0:',
            evaluation,
        )
        self.assertIn(
            'metric["suggested_peak_worker_memory_gib"]',
            evaluation,
        )
        self.assertIn(
            '"suggested_parallel_tasks": suggested_parallel_tasks',
            evaluation,
        )
        self.assertIn(
            '"suggested_native_threads_per_worker": suggested_native_threads',
            evaluation,
        )
        self.assertIn(
            'int(metric["native_threads_per_worker"]) != expected_native_threads',
            evaluation,
        )
        pipeline = json.loads(
            EXECUTOR_BENCHMARK_PIPELINE.read_text(encoding="utf-8")
        )
        expected_prefix_assignment = 'TABLE_PREFIX = "people_counter"'
        self.assertIn(expected_prefix_assignment, bootstrap_parameters)
        self.assertIn(expected_prefix_assignment, inference_parameters)
        self.assertIn(expected_prefix_assignment, control_parameters)
        activities = {
            activity["name"]: activity
            for activity in pipeline["properties"]["activities"]
        }
        self.assertNotIn("ForEach", {item["type"] for item in activities.values()})
        self.assertEqual(
            activities["RunExecutorPartitionInference"]["dependsOn"][0],
            {
                "activity": "PrepareExecutorBenchmark",
                "dependencyConditions": ["Succeeded"],
            },
        )
        self.assertEqual(
            activities["EvaluateExecutorBenchmark"]["dependsOn"][0],
            {
                "activity": "RunExecutorPartitionInference",
                "dependencyConditions": ["Completed"],
            },
        )
        run_parameters = activities["RunExecutorPartitionInference"][
            "typeProperties"
        ]["parameters"]
        self.assertIn("CPUS_PER_TASK", run_parameters)
        self.assertIn("CONFIG_SHA256", run_parameters)
        self.assertEqual(
            activities["PrepareExecutorBenchmark"]["typeProperties"]["notebookId"],
            "efd68f39-e6f6-42fb-9797-9e7ced47a8a0",
        )
        prepare_parameters = activities["PrepareExecutorBenchmark"][
            "typeProperties"
        ]["parameters"]
        self.assertEqual(
            prepare_parameters["ATTACHED_LAKEHOUSE_ID"]["value"]["value"],
            "@concat('\"', pipeline().parameters.ATTACHED_LAKEHOUSE_ID, '\"')",
        )
        self.assertIn("LAKEHOUSE_FILE_API_ROOT", prepare_parameters)
        self.assertNotIn("STAGING_ROOT", prepare_parameters)
        pipeline_parameters = pipeline["properties"]["parameters"]
        self.assertEqual(
            pipeline_parameters["TABLE_PREFIX"]["defaultValue"],
            "people_counter",
        )
        self.assertEqual(pipeline_parameters["DATABASE"]["defaultValue"], "")
        for activity_name in (
            "PrepareExecutorBenchmark",
            "RunExecutorPartitionInference",
            "EvaluateExecutorBenchmark",
        ):
            activity_parameters = activities[activity_name]["typeProperties"][
                "parameters"
            ]
            for table_parameter in ("DATABASE", "TABLE_PREFIX"):
                self.assertEqual(
                    activity_parameters[table_parameter]["value"]["value"],
                    f"@pipeline().parameters.{table_parameter}",
                )
        self.assertEqual(
            pipeline_parameters["USABLE_EXECUTOR_MEMORY_GIB"]["defaultValue"],
            0.0,
        )
        self.assertEqual(
            pipeline_parameters["PEAK_WORKER_MEMORY_GIB"]["defaultValue"],
            0.0,
        )
        for optional_parameter in (
            "BASELINE_BENCHMARK_BATCH_ID",
            "BASELINE_CONFIG_SHA256",
            "CAMERA_MOTION_COMPENSATION",
        ):
            self.assertEqual(
                pipeline_parameters[optional_parameter]["defaultValue"],
                "",
            )
        self.assertEqual(
            pipeline_parameters["ATTACHED_LAKEHOUSE_ID"]["defaultValue"],
            "883cff91-eaa8-40be-870f-6e9716303cb2",
        )
        self.assertEqual(
            pipeline_parameters["LAKEHOUSE_FILE_API_ROOT"]["defaultValue"],
            "/lakehouse/default",
        )

    def test_executor_benchmark_treats_fabric_null_database_as_unqualified(self):
        control_identifier = cell_functions(
            EXECUTOR_BENCHMARK_CONTROL,
            "control-helpers",
            "identifier",
            IDENTIFIER=re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$"),
        )["identifier"]
        inference_identifier = cell_functions(
            EXECUTOR_PROTOTYPE,
            "spark-config",
            "identifier",
            re=re,
        )["identifier"]

        for identifier in (control_identifier, inference_identifier):
            for empty_value in (None, "", " ", "None", "none", "NULL", " null "):
                with self.subTest(
                    helper=identifier.__module__,
                    empty_value=empty_value,
                ):
                    self.assertEqual(
                        identifier(empty_value, "DATABASE", allow_empty=True),
                        "",
                    )
            self.assertEqual(
                identifier("analytics", "DATABASE", allow_empty=True),
                "analytics",
            )
            with self.assertRaisesRegex(ValueError, "valid SQL identifier"):
                identifier("invalid.database", "DATABASE", allow_empty=True)

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
