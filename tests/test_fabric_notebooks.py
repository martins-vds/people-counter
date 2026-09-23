import ast
import json
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call


NOTEBOOKS = Path(__file__).resolve().parents[1] / "notebooks" / "fabric"
WORKER = NOTEBOOKS / "04_process_video.ipynb"


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


class WorkerLeaseError(RuntimeError):
    pass


class FabricNotebookTests(unittest.TestCase):
    def worker_run_namespace(self):
        client = MagicMock()
        staged, temporary = MagicMock(), MagicMock()
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
            work_id="work-a",
            attempt_id="attempt-a",
            worker_execution_id="execution-a",
            lease_minutes=30,
            prefix="pc",
            database="",
            spark_session=MagicMock(),
            control_writer=MagicMock(),
            WorkerEventClient=MagicMock(return_value=client),
            load_work=MagicMock(side_effect=[work, published]),
            verify_lease=MagicMock(),
            stage_source=MagicMock(return_value=(staged, temporary)),
            build_config=MagicMock(return_value=config),
            heartbeat=MagicMock(),
            run=MagicMock(return_value=result),
            utc_now=lambda: datetime(2026, 9, 23, tzinfo=timezone.utc),
            result_frames=MagicMock(return_value=(
                MagicMock(count=MagicMock(return_value=2)),
                MagicMock(count=MagicMock(return_value=3)),
            )),
            append_attempt_rows=MagicMock(),
            telemetry_table="pc_telemetry_attempts",
            line_counts_table="pc_line_count_attempts",
            classify_error=MagicMock(return_value=(True, "RUNTIME")),
            LeaseLostError=WorkerLeaseError,
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

    def test_worker_has_no_direct_delta_mutations(self):
        source = code_source(WORKER)
        tree = ast.parse(source)
        self.assertNotIn("from delta.tables", source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                self.assertNotIn(node.func.attr, {"merge", "delete", "whenMatchedUpdateAll"})

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
            ("03_claim_work", 'owned = ('),
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
        self.assertEqual(namespace["append_attempt_rows"].call_count, 2)
        namespace["verify_lease"].assert_called_with(namespace["work"])
        namespace["staged"].unlink.assert_called_once_with(missing_ok=True)
        namespace["temporary"].rmdir.assert_called_once_with()

    def test_inference_failure_is_submitted_and_original_error_rethrown(self):
        namespace = self.worker_run_namespace()
        work = {
            "capture_date": date(2023, 9, 29),
            "status": "LEASED",
            "committed_attempt_id": None,
        }
        namespace["load_work"].side_effect = [work, {"status": "RUNNING", "committed_attempt_id": None}]
        error = OSError("Inference failed")
        namespace["run"].side_effect = error
        with self.assertRaises(OSError) as raised:
            self.execute_worker(namespace)
        self.assertIs(raised.exception, error)
        kind, payload = namespace["event_client"].submit.call_args.args
        self.assertEqual(kind, "failure")
        self.assertEqual(payload["error_message"], "Inference failed")
        self.assertEqual(payload["error_type"], "OSError")
        self.assertTrue(payload["retryable"])
        self.assertFalse(payload["lease_lost"])
        self.assertNotIn(call("commit", {}), namespace["event_client"].submit.call_args_list)
        namespace["append_attempt_rows"].assert_not_called()
        namespace["staged"].unlink.assert_called_once_with(missing_ok=True)

    def test_ambiguous_commit_does_not_turn_published_work_into_failure(self):
        namespace = self.worker_run_namespace()

        def submit(kind, payload):
            if kind == "commit":
                raise OSError("Receipt response lost after publication")

        namespace["WorkerEventClient"].return_value.submit.side_effect = submit
        self.execute_worker(namespace)
        self.assertEqual(namespace["outcome"]["status"], "SUCCEEDED_AFTER_AMBIGUOUS_COMMIT")
        self.assertFalse(
            any(item.args[0] == "failure" for item in namespace["event_client"].submit.call_args_list)
        )


if __name__ == "__main__":
    unittest.main()
