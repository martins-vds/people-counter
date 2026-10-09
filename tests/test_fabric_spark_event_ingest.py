"""Tests for the Spark event-log parser and task-overlap calculator."""

from __future__ import annotations

import dataclasses
import json
import unittest

from people_counter.fabric_spark_event_ingest import (
    SparkEventLogError,
    compute_task_overlap,
    parse_spark_event_log_lines,
    require_complete_event_evidence,
)


def _executor_added(executor_id: str, host: str, cores: int, timestamp: int) -> str:
    return json.dumps(
        {
            "Event": "SparkListenerExecutorAdded",
            "Timestamp": timestamp,
            "Executor ID": executor_id,
            "Executor Info": {"Host": host, "Total Cores": cores},
        }
    )


def _executor_removed(executor_id: str, reason: str, timestamp: int) -> str:
    return json.dumps(
        {
            "Event": "SparkListenerExecutorRemoved",
            "Timestamp": timestamp,
            "Executor ID": executor_id,
            "Removed Reason": reason,
        }
    )


def _task_end(
    *,
    stage_id: int,
    task_id: int,
    executor_id: str,
    launch_ms: int,
    finish_ms: int,
    failed: bool = False,
    speculative: bool = False,
    include_gc: bool = True,
) -> str:
    metrics = {
        "Executor Run Time": finish_ms - launch_ms,
        "Memory Bytes Spilled": 0,
        "Disk Bytes Spilled": 0,
        "Input Metrics": {"Bytes Read": 1024},
        "Output Metrics": {"Bytes Written": 512},
    }
    if include_gc:
        metrics["JVM GC Time"] = 5
    return json.dumps(
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": stage_id,
            "Task Info": {
                "Task ID": task_id,
                "Executor ID": executor_id,
                "Launch Time": launch_ms,
                "Finish Time": finish_ms,
                "Failed": failed,
                "Speculative": speculative,
            },
            "Task Metrics": metrics,
        }
    )


class ParseSparkEventLogLinesTests(unittest.TestCase):
    def test_parses_executor_added_and_removed_events(self) -> None:
        lines = [
            _executor_added("1", "host-a", 4, 1000),
            _executor_removed("1", "Executor heartbeat timed out", 5000),
        ]
        summary = parse_spark_event_log_lines(lines)
        self.assertEqual(len(summary.executors), 2)
        added, removed = summary.executors
        self.assertEqual(added.event_type, "added")
        self.assertEqual(added.host, "host-a")
        self.assertEqual(added.total_cores, 4)
        self.assertEqual(removed.event_type, "removed")
        self.assertEqual(removed.removed_reason, "Executor heartbeat timed out")

    def test_complete_gate_binds_task_cpus_executor_ids_and_metrics(self) -> None:
        resource_profile = json.dumps(
            {
                "Event": "SparkListenerResourceProfileAdded",
                "Task Resource Requests": {
                    "cpus": {"Resource Name": "cpus", "Amount": 2.0}
                },
            }
        )
        summary = parse_spark_event_log_lines(
            [
                resource_profile,
                _executor_added("1", "host-a", 4, 1000),
                _task_end(
                    stage_id=1,
                    task_id=7,
                    executor_id="1",
                    launch_ms=1100,
                    finish_ms=1200,
                ),
            ]
        )
        overlap = require_complete_event_evidence(
            summary,
            expected_executor_ids=["1"],
            expected_task_cpus=2,
            expected_task_count=1,
        )
        self.assertEqual(summary.task_cpu_values, (2,))
        self.assertEqual(overlap.max_concurrent_tasks, 1)
        one_cpu = dataclasses.replace(summary, task_cpu_values=(1,))
        self.assertEqual(
            require_complete_event_evidence(
                one_cpu,
                expected_executor_ids=["1"],
                expected_task_cpus=1,
                expected_task_count=1,
            ).max_concurrent_tasks,
            1,
        )

    def test_complete_gate_rejects_absent_task_cpu_evidence(self) -> None:
        summary = parse_spark_event_log_lines(
            [
                _executor_added("1", "host-a", 4, 1000),
                _task_end(
                    stage_id=1,
                    task_id=7,
                    executor_id="1",
                    launch_ms=1100,
                    finish_ms=1200,
                ),
            ]
        )
        with self.assertRaisesRegex(
            SparkEventLogError,
            "task CPU evidence is absent",
        ):
            require_complete_event_evidence(
                summary,
                expected_executor_ids=["1"],
                expected_task_cpus=2,
                expected_task_count=1,
            )
        mismatched = dataclasses.replace(summary, task_cpu_values=(1,))
        with self.assertRaisesRegex(
            SparkEventLogError,
            "task CPU evidence is absent or inconsistent",
        ):
            require_complete_event_evidence(
                mismatched,
                expected_executor_ids=["1"],
                expected_task_cpus=2,
                expected_task_count=1,
            )

    def test_complete_gate_rejects_every_incomplete_dimension(self) -> None:
        resource_profile = json.dumps(
            {
                "Event": "SparkListenerResourceProfileAdded",
                "Task Resource Requests": {"cpus": {"Amount": 2}},
            }
        )
        complete = parse_spark_event_log_lines(
            [
                resource_profile,
                _executor_added("1", "host-a", 4, 1000),
                _task_end(
                    stage_id=1,
                    task_id=7,
                    executor_id="1",
                    launch_ms=1100,
                    finish_ms=1200,
                ),
            ]
        )
        with self.assertRaisesRegex(SparkEventLogError, "must not be empty"):
            require_complete_event_evidence(
                complete,
                expected_executor_ids=[],
                expected_task_cpus=2,
                expected_task_count=1,
            )
        for task_cpus, task_count in ((0, 1), (1, 0)):
            with self.subTest(task_cpus=task_cpus, task_count=task_count):
                with self.assertRaisesRegex(SparkEventLogError, "positive"):
                    require_complete_event_evidence(
                        complete,
                        expected_executor_ids=["1"],
                        expected_task_cpus=task_cpus,
                        expected_task_count=task_count,
                    )
        with self.assertRaisesRegex(SparkEventLogError, "executor IDs differ"):
            require_complete_event_evidence(
                complete,
                expected_executor_ids=["2"],
                expected_task_cpus=2,
                expected_task_count=1,
            )
        incomplete_executor = dataclasses.replace(
            complete,
            executors=(
                dataclasses.replace(
                    complete.executors[0],
                    missing_field_reasons=(("Host", "missing"),),
                ),
            ),
        )
        with self.assertRaisesRegex(SparkEventLogError, "executor event"):
            require_complete_event_evidence(
                incomplete_executor,
                expected_executor_ids=["1"],
                expected_task_cpus=2,
                expected_task_count=1,
            )
        with self.assertRaisesRegex(SparkEventLogError, "task count differs"):
            require_complete_event_evidence(
                complete,
                expected_executor_ids=["1"],
                expected_task_cpus=2,
                expected_task_count=2,
            )
        for changes in (
            {"missing_field_reasons": (("Task ID", "missing"),)},
            {"missing_metric_reasons": (("JVM GC Time", "missing"),)},
            {"executor_id": "2"},
            {"failed": True},
            {"speculative": True},
        ):
            with self.subTest(changes=changes):
                unsafe = dataclasses.replace(
                    complete,
                    tasks=(dataclasses.replace(complete.tasks[0], **changes),),
                )
                with self.assertRaisesRegex(
                    SparkEventLogError,
                    "incomplete or unsafe",
                ):
                    require_complete_event_evidence(
                        unsafe,
                        expected_executor_ids=["1"],
                        expected_task_cpus=2,
                        expected_task_count=1,
                    )

    def test_parses_environment_task_cpu_and_rejects_invalid_values(self) -> None:
        environment = lambda value: json.dumps(
            {
                "Event": "SparkListenerEnvironmentUpdate",
                "Spark Properties": {"spark.task.cpus": value},
            }
        )
        summary = parse_spark_event_log_lines([environment("4")])
        self.assertEqual(summary.task_cpu_values, (4,))
        without_cpu = parse_spark_event_log_lines(
            [
                json.dumps(
                    {
                        "Event": "SparkListenerEnvironmentUpdate",
                        "Spark Properties": {"spark.executor.cores": "8"},
                    }
                )
            ]
        )
        self.assertEqual(without_cpu.task_cpu_values, ())
        for value, message in (("x", "not an integer"), ("0", "positive")):
            with self.subTest(value=value):
                with self.assertRaisesRegex(SparkEventLogError, message):
                    parse_spark_event_log_lines([environment(value)])

    def test_resource_profile_rejects_missing_or_invalid_task_cpu(self) -> None:
        def resource(cpu: object) -> str:
            return json.dumps(
                {
                    "Event": "SparkListenerResourceProfileAdded",
                    "Task Resource Requests": {"cpus": cpu},
                }
            )

        with self.assertRaisesRegex(SparkEventLogError, "no task cpus"):
            parse_spark_event_log_lines([resource(None)])
        for amount in (True, 0, -1, 1.5, "2"):
            with self.subTest(amount=amount):
                with self.assertRaisesRegex(SparkEventLogError, "positive integer"):
                    parse_spark_event_log_lines(
                        [resource({"Amount": amount})]
                    )

    def test_executor_added_captures_every_field_exactly(self) -> None:
        summary = parse_spark_event_log_lines([_executor_added("42", "host-a", 4, 1000)])
        (added,) = summary.executors
        self.assertEqual(added.executor_id, "42")
        self.assertEqual(added.timestamp_ms, 1000)

    def test_executor_removed_captures_every_field_exactly(self) -> None:
        summary = parse_spark_event_log_lines(
            [_executor_removed("42", "lost heartbeat", 5000)]
        )
        (removed,) = summary.executors
        self.assertEqual(removed.executor_id, "42")
        self.assertEqual(removed.timestamp_ms, 5000)
        self.assertIsNone(removed.host)
        self.assertIsNone(removed.total_cores)

    def test_parses_task_end_with_full_metrics(self) -> None:
        lines = [
            _task_end(
                stage_id=0, task_id=1, executor_id="1", launch_ms=1000, finish_ms=1500
            )
        ]
        summary = parse_spark_event_log_lines(lines)
        self.assertEqual(len(summary.tasks), 1)
        task = summary.tasks[0]
        self.assertEqual(task.duration_ms, 500)
        self.assertEqual(task.executor_run_time_ms, 500)
        self.assertEqual(task.jvm_gc_time_ms, 5)
        self.assertEqual(task.input_bytes_read, 1024)
        self.assertEqual(task.output_bytes_written, 512)

    def test_task_end_captures_every_identity_and_flag_field_exactly(self) -> None:
        lines = [
            _task_end(
                stage_id=7,
                task_id=99,
                executor_id="exec-3",
                launch_ms=2_000,
                finish_ms=2_750,
                failed=True,
                speculative=True,
            )
        ]
        (task,) = parse_spark_event_log_lines(lines).tasks
        self.assertEqual(task.stage_id, 7)
        self.assertEqual(task.task_id, 99)
        self.assertEqual(task.executor_id, "exec-3")
        self.assertEqual(task.launch_time_ms, 2_000)
        self.assertEqual(task.finish_time_ms, 2_750)
        self.assertTrue(task.failed)
        self.assertTrue(task.speculative)

    def test_task_end_defaults_failed_and_speculative_to_false(self) -> None:
        (task,) = parse_spark_event_log_lines(
            [
                _task_end(
                    stage_id=0,
                    task_id=1,
                    executor_id="1",
                    launch_ms=1_000,
                    finish_ms=1_100,
                )
            ]
        ).tasks
        self.assertFalse(task.failed)
        self.assertFalse(task.speculative)
        self.assertEqual(task.memory_bytes_spilled, 0)
        self.assertEqual(task.disk_bytes_spilled, 0)
        self.assertEqual(task.missing_metric_reasons, ())

    def test_missing_metric_is_recorded_with_an_explicit_reason_not_fabricated(
        self,
    ) -> None:
        lines = [
            _task_end(
                stage_id=0,
                task_id=1,
                executor_id="1",
                launch_ms=1000,
                finish_ms=1500,
                include_gc=False,
            )
        ]
        summary = parse_spark_event_log_lines(lines)
        task = summary.tasks[0]
        self.assertIsNone(task.jvm_gc_time_ms)
        reasons = dict(task.missing_metric_reasons)
        self.assertIn("JVM GC Time", reasons)
        self.assertIn("absent", reasons["JVM GC Time"])

    def test_wrong_typed_present_metric_is_rejected_not_silently_accepted(self) -> None:
        # `_optional_int` must reject a present-but-non-int value (e.g. a
        # string) via an OR of the two failure conditions; an AND would let
        # a non-int, non-bool value like a string slip through unrejected.
        lines = [
            json.dumps(
                {
                    "Event": "SparkListenerTaskEnd",
                    "Stage ID": 0,
                    "Task Info": {
                        "Task ID": 1,
                        "Executor ID": "1",
                        "Launch Time": 1_000,
                        "Finish Time": 1_100,
                    },
                    "Task Metrics": {"Executor Run Time": "not-a-number"},
                }
            )
        ]
        (task,) = parse_spark_event_log_lines(lines).tasks
        self.assertIsNone(task.executor_run_time_ms)
        reasons = dict(task.missing_metric_reasons)
        self.assertIn("present but not an integer", reasons["Executor Run Time"])

    def test_skips_blank_lines(self) -> None:
        lines = ["", "   ", _executor_added("1", "host-a", 2, 1_000)]
        summary = parse_spark_event_log_lines(lines)
        self.assertEqual(len(summary.executors), 1)

    def test_fails_closed_on_invalid_json(self) -> None:
        with self.assertRaises(SparkEventLogError) as error:
            parse_spark_event_log_lines(["{not json"])
        self.assertTrue(str(error.exception).startswith("invalid JSON event line: "))

    def test_fails_closed_on_non_object_json(self) -> None:
        with self.assertRaises(SparkEventLogError) as error:
            parse_spark_event_log_lines(["[1, 2, 3]"])
        self.assertEqual(
            str(error.exception),
            "event log line did not decode to a JSON object",
        )

    def test_missing_executor_id_is_reported_not_fabricated_for_added_event(
        self,
    ) -> None:
        summary = parse_spark_event_log_lines(
            [
                json.dumps(
                    {
                        "Event": "SparkListenerExecutorAdded",
                        "Timestamp": 1_000,
                        "Executor Info": {"Host": "host-a", "Total Cores": 2},
                    }
                )
            ]
        )
        (added,) = summary.executors
        self.assertIsNone(added.executor_id)
        self.assertEqual(
            dict(added.missing_field_reasons),
            {"Executor ID": "missing-required-field"},
        )

    def test_missing_executor_id_is_reported_not_fabricated_for_removed_event(
        self,
    ) -> None:
        summary = parse_spark_event_log_lines(
            [
                json.dumps(
                    {
                        "Event": "SparkListenerExecutorRemoved",
                        "Timestamp": 1_000,
                        "Removed Reason": "lost",
                    }
                )
            ]
        )
        (removed,) = summary.executors
        self.assertIsNone(removed.executor_id)
        self.assertEqual(
            dict(removed.missing_field_reasons),
            {"Executor ID": "missing-required-field"},
        )

    def test_task_end_missing_required_fields_are_explicitly_recorded_not_fabricated(
        self,
    ) -> None:
        lines = [
            json.dumps(
                {
                    "Event": "SparkListenerTaskEnd",
                    "Task Info": {},
                    "Task Metrics": {},
                }
            )
        ]
        (task,) = parse_spark_event_log_lines(lines).tasks
        self.assertIsNone(task.stage_id)
        self.assertIsNone(task.task_id)
        self.assertIsNone(task.executor_id)
        self.assertIsNone(task.launch_time_ms)
        self.assertIsNone(task.finish_time_ms)
        self.assertFalse(task.failed)
        self.assertFalse(task.speculative)
        self.assertEqual(
            dict(task.missing_field_reasons),
            {
                "Stage ID": "missing-required-field",
                "Task ID": "missing-required-field",
                "Executor ID": "missing-required-field",
                "Launch Time": "missing-required-field",
                "Finish Time": "missing-required-field",
            },
        )

    def test_zero_timestamp_is_treated_as_missing_not_as_real_epoch_zero(self) -> None:
        (task,) = parse_spark_event_log_lines(
            [
                json.dumps(
                    {
                        "Event": "SparkListenerTaskEnd",
                        "Stage ID": 0,
                        "Task Info": {
                            "Task ID": 1,
                            "Executor ID": "1",
                            "Launch Time": 0,
                            "Finish Time": 1_100,
                        },
                        "Task Metrics": {},
                    }
                )
            ]
        ).tasks
        self.assertIsNone(task.launch_time_ms)
        self.assertEqual(
            dict(task.missing_field_reasons)["Launch Time"],
            "zero-placeholder-timestamp",
        )
        with self.assertRaises(SparkEventLogError) as error:
            compute_task_overlap((task,))
        self.assertEqual(
            str(error.exception),
            "cannot compute task overlap with missing required task timestamps",
        )

    def test_negative_duration_is_rejected(self) -> None:
        with self.assertRaises(SparkEventLogError) as error:
            parse_spark_event_log_lines(
                [
                    _task_end(
                        stage_id=0,
                        task_id=1,
                        executor_id="1",
                        launch_ms=1_200,
                        finish_ms=1_100,
                    )
                ]
            )
        self.assertEqual(
            str(error.exception),
            "SparkListenerTaskEnd has negative duration: finish time 1100 precedes launch time 1200",
        )

    def test_malformed_nested_metrics_are_rejected(self) -> None:
        with self.assertRaises(SparkEventLogError) as error:
            parse_spark_event_log_lines(
                [
                    json.dumps(
                        {
                            "Event": "SparkListenerTaskEnd",
                            "Stage ID": 0,
                            "Task Info": {
                                "Task ID": 1,
                                "Executor ID": "1",
                                "Launch Time": 1_000,
                                "Finish Time": 1_100,
                            },
                            "Task Metrics": {"Input Metrics": []},
                        }
                    )
                ]
            )
        self.assertEqual(
            str(error.exception),
            "SparkListenerTaskEnd field 'Input Metrics' must be a JSON object when present",
        )

    def test_missing_event_key_defaults_to_an_empty_string_not_the_word_none(
        self,
    ) -> None:
        # `str(event.get("Event", None))` and the bare `event.get("Event")`
        # both stringify a missing key to the literal text "None"; the
        # production default must be `""` so an event log entry with no
        # "Event" key is recorded as an empty, not a "None", event type.
        summary = parse_spark_event_log_lines([json.dumps({"no-event-key": True})])
        self.assertEqual(summary.unrecognized_event_types, ("",))

    def test_records_unrecognized_event_types_without_dropping_the_rest(self) -> None:
        lines = [
            json.dumps({"Event": "SparkListenerApplicationStart"}),
            _executor_added("1", "host-a", 2, 1_000),
        ]
        summary = parse_spark_event_log_lines(lines)
        self.assertEqual(summary.unrecognized_event_types, ("SparkListenerApplicationStart",))
        self.assertEqual(len(summary.executors), 1)

    def test_unsupported_executor_event_type_reports_the_exact_message(self) -> None:
        from people_counter.fabric_spark_event_ingest import ExecutorEventRecord

        with self.assertRaises(SparkEventLogError) as error:
            ExecutorEventRecord(
                executor_id="1",
                event_type="unsupported",
                host=None,
                total_cores=None,
                timestamp_ms=None,
            )
        self.assertEqual(
            str(error.exception),
            "unsupported executor event type: 'unsupported'",
        )


class ComputeTaskOverlapTests(unittest.TestCase):
    def test_empty_tasks_report_zero_overlap(self) -> None:
        overlap = compute_task_overlap(())
        self.assertEqual(overlap.max_concurrent_tasks, 0)
        self.assertEqual(overlap.total_wall_ms, 0)
        self.assertEqual(overlap.total_busy_task_ms, 0)
        self.assertEqual(overlap.observed_parallelism, 0.0)

    def test_total_wall_ms_uses_subtraction_not_addition_of_min_and_max(self) -> None:
        # Using a nonzero launch-time offset distinguishes `max - min` from
        # a mutated `max + min`, which a zero-offset launch time cannot.
        summary = parse_spark_event_log_lines(
            [
                _task_end(
                    stage_id=0,
                    task_id=1,
                    executor_id="1",
                    launch_ms=1_000,
                    finish_ms=1_100,
                ),
                _task_end(
                    stage_id=0,
                    task_id=2,
                    executor_id="2",
                    launch_ms=1_050,
                    finish_ms=1_200,
                ),
            ]
        )
        overlap = compute_task_overlap(summary.tasks)
        self.assertEqual(overlap.total_wall_ms, 200)

    def test_zero_duration_single_task_wall_and_busy_time_stay_at_zero(self) -> None:
        summary = parse_spark_event_log_lines(
            [
                _task_end(
                    stage_id=0,
                    task_id=1,
                    executor_id="1",
                    launch_ms=500,
                    finish_ms=500,
                )
            ]
        )
        overlap = compute_task_overlap(summary.tasks)
        self.assertEqual(overlap.total_wall_ms, 0)
        self.assertEqual(overlap.total_busy_task_ms, 0)

    def test_finish_event_decrement_is_exactly_one_not_two(self) -> None:
        # A short-lived task finishing mid-stream must subtract exactly one
        # from the running concurrency count; subtracting two would
        # under-report a later, genuinely higher overlap.
        lines = [
            _task_end(
                stage_id=0,
                task_id=1,
                executor_id="1",
                launch_ms=1_000,
                finish_ms=1_100,
            ),
            _task_end(
                stage_id=0,
                task_id=2,
                executor_id="2",
                launch_ms=1_001,
                finish_ms=1_002,
            ),
            _task_end(
                stage_id=0,
                task_id=3,
                executor_id="3",
                launch_ms=1_005,
                finish_ms=1_100,
            ),
            _task_end(
                stage_id=0,
                task_id=4,
                executor_id="4",
                launch_ms=1_006,
                finish_ms=1_100,
            ),
            _task_end(
                stage_id=0,
                task_id=5,
                executor_id="5",
                launch_ms=1_007,
                finish_ms=1_100,
            ),
        ]
        summary = parse_spark_event_log_lines(lines)
        overlap = compute_task_overlap(summary.tasks)
        self.assertEqual(overlap.max_concurrent_tasks, 4)

    def test_two_sequential_tasks_have_no_overlap(self) -> None:
        summary = parse_spark_event_log_lines(
            [
                _task_end(
                    stage_id=0,
                    task_id=1,
                    executor_id="1",
                    launch_ms=1_000,
                    finish_ms=1_100,
                ),
                _task_end(
                    stage_id=0,
                    task_id=2,
                    executor_id="1",
                    launch_ms=1_100,
                    finish_ms=1_200,
                ),
            ]
        )
        overlap = compute_task_overlap(summary.tasks)
        self.assertEqual(overlap.max_concurrent_tasks, 1)
        self.assertEqual(overlap.total_wall_ms, 200)
        self.assertEqual(overlap.total_busy_task_ms, 200)
        self.assertAlmostEqual(overlap.observed_parallelism, 1.0)

    def test_two_fully_concurrent_tasks_show_two_x_observed_parallelism(self) -> None:
        summary = parse_spark_event_log_lines(
            [
                _task_end(
                    stage_id=0,
                    task_id=1,
                    executor_id="1",
                    launch_ms=1_000,
                    finish_ms=1_100,
                ),
                _task_end(
                    stage_id=0,
                    task_id=2,
                    executor_id="2",
                    launch_ms=1_000,
                    finish_ms=1_100,
                ),
            ]
        )
        overlap = compute_task_overlap(summary.tasks)
        self.assertEqual(overlap.max_concurrent_tasks, 2)
        self.assertEqual(overlap.total_wall_ms, 100)
        self.assertEqual(overlap.total_busy_task_ms, 200)
        self.assertAlmostEqual(overlap.observed_parallelism, 2.0)

    def test_speculative_duplicates_are_excluded_from_overlap(self) -> None:
        summary = parse_spark_event_log_lines(
            [
                _task_end(
                    stage_id=0,
                    task_id=1,
                    executor_id="1",
                    launch_ms=1_000,
                    finish_ms=1_100,
                ),
                _task_end(
                    stage_id=0,
                    task_id=1,
                    executor_id="2",
                    launch_ms=1_000,
                    finish_ms=1_100,
                    speculative=True,
                ),
            ]
        )
        overlap = compute_task_overlap(summary.tasks)
        self.assertEqual(overlap.max_concurrent_tasks, 1)
        self.assertEqual(overlap.total_busy_task_ms, 100)


if __name__ == "__main__":
    unittest.main()
