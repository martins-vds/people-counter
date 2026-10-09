"""Tests for fail-closed Fabric-platform capability probes."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, call, patch

from people_counter.fabric_capability_probe import (
    CapabilityProbeError,
    CapabilityProbeResult,
    CapabilityStatus,
    ConsumerArtifact,
    ConsumerCapabilityReport,
    ConsumerProbeProfile,
    _host_to_executor_id_map,
    _resolve_executor_id,
    probe_capability,
    probe_concurrent_executor_reads,
    probe_direct_mount_consumer_capability,
    probe_direct_mounted_lakehouse_path,
    probe_executor_peak_rss_bytes,
    probe_gpu_spark_runtime,
    probe_model_file_consumer,
    probe_onnx_runtime_consumer,
    probe_opencv_video_consumer,
    probe_rss_high_water_mark,
    probe_spark_event_log_accessible,
    probe_stream_hash_artifacts,
    persist_consumer_capability_evidence,
    redacted_consumer_capability_evidence,
)
from people_counter.fabric_executor_inventory import ExecutorRecord


class CapabilityProbeResultTests(unittest.TestCase):
    def test_rejects_empty_capability_name(self) -> None:
        with self.assertRaises(CapabilityProbeError) as context:
            CapabilityProbeResult(
                capability="",
                status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
                evidence="blocked",
            )
        self.assertEqual(str(context.exception), "capability name is required")

    def test_rejects_empty_evidence(self) -> None:
        with self.assertRaises(CapabilityProbeError) as context:
            CapabilityProbeResult(
                capability="x",
                status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
                evidence="",
            )
        self.assertEqual(
            str(context.exception),
            "evidence is required for every probe result, pass or fail",
        )

    def test_rejects_available_without_a_value(self) -> None:
        with self.assertRaises(CapabilityProbeError) as context:
            CapabilityProbeResult(
                capability="x",
                status=CapabilityStatus.AVAILABLE,
                evidence="proven",
                value=None,
            )
        self.assertEqual(
            str(context.exception),
            "AVAILABLE probes must record a non-null measured value",
        )


class ProbeCapabilityTests(unittest.TestCase):
    def test_successful_check_is_reported_available_with_evidence(self) -> None:
        result = probe_capability("widget", lambda: (42, "measured 42"))
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertEqual(result.value, 42)
        self.assertEqual(result.evidence, "measured 42")
        self.assertEqual(result.capability, "widget")

    def test_exception_is_normalized_to_fabric_platform_blocked_with_exact_evidence(
        self,
    ) -> None:
        def _check():
            raise RuntimeError("not exposed by this runtime")

        result = probe_capability("widget", _check)
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("not exposed by this runtime", result.evidence)
        self.assertIn("RuntimeError", result.evidence)
        self.assertIsNone(result.value)

    def test_never_fabricates_a_value_for_a_blocked_capability(self) -> None:
        def _check():
            raise ValueError("boom")

        result = probe_capability("widget", _check)
        self.assertIsNone(result.value)


class ProbeGpuSparkRuntimeTests(unittest.TestCase):
    def test_reports_blocked_when_gpu_flag_is_not_enabled(self) -> None:
        spark = MagicMock()
        spark.conf.get.side_effect = lambda key, default=None: {
            "spark.fabric.pool.runtimeType": "Standard",
        }.get(key, default)
        result = probe_gpu_spark_runtime(spark)
        self.assertEqual(result.capability, "fabric_gpu_spark_runtime")
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("spark.fabric.resourceProfile.gpu.enabled", result.evidence)
        self.assertEqual(
            result.evidence,
            "RuntimeError: spark.fabric.resourceProfile.gpu.enabled is not "
            "'true' (observed 'false', runtime='Standard')",
        )

    def test_reports_blocked_with_the_real_default_runtime_and_flag_when_both_keys_are_absent(
        self,
    ) -> None:
        """Proves the exact default literals ("unknown" runtime, "false" gpu
        flag) used when Fabric's conf does not expose either key at all --
        not merely that *some* blocked status is reported."""
        spark = MagicMock()
        spark.conf.get.side_effect = lambda key, default=None: default
        result = probe_gpu_spark_runtime(spark)
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertEqual(
            result.evidence,
            "RuntimeError: spark.fabric.resourceProfile.gpu.enabled is not "
            "'true' (observed 'false', runtime='unknown')",
        )

    def test_reports_available_when_gpu_flag_is_true(self) -> None:
        spark = MagicMock()
        spark.conf.get.side_effect = lambda key, default=None: {
            "spark.fabric.pool.runtimeType": "GPU",
            "spark.fabric.resourceProfile.gpu.enabled": "true",
        }.get(key, default)
        result = probe_gpu_spark_runtime(spark)
        self.assertEqual(result.capability, "fabric_gpu_spark_runtime")
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertTrue(result.value)
        self.assertEqual(
            result.evidence,
            "spark.fabric.resourceProfile.gpu.enabled=true (runtime='GPU')",
        )

    def test_reports_blocked_when_conf_get_raises(self) -> None:
        spark = MagicMock()
        spark.conf.get.side_effect = RuntimeError("conf unavailable")
        result = probe_gpu_spark_runtime(spark)
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("conf unavailable", result.evidence)


class ProbeRssHighWaterMarkTests(unittest.TestCase):
    def test_reports_available_with_a_positive_measured_value_on_this_platform(
        self,
    ) -> None:
        result = probe_rss_high_water_mark()
        self.assertEqual(result.capability, "rss_high_water_mark")
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertIsInstance(result.value, int)
        self.assertGreater(result.value, 0)
        self.assertIn("ru_maxrss", result.evidence)

    def test_computes_the_exact_byte_value_from_the_measured_kib(self) -> None:
        with patch(
            "people_counter.fabric_capability_probe.resource.getrusage",
            return_value=type("Usage", (), {"ru_maxrss": 10})(),
        ):
            result = probe_rss_high_water_mark()
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertEqual(result.value, 10 * 1024)
        self.assertIn("10KiB", result.evidence)

    def test_reports_available_when_ru_maxrss_is_exactly_one(self) -> None:
        """A measured value of 1 KiB is still a genuine positive reading
        and must not be treated as the non-positive/blocked boundary."""
        with patch(
            "people_counter.fabric_capability_probe.resource.getrusage",
            return_value=type("Usage", (), {"ru_maxrss": 1})(),
        ):
            result = probe_rss_high_water_mark()
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertEqual(result.value, 1024)

    def test_reports_blocked_when_ru_maxrss_is_non_positive(self) -> None:
        with patch(
            "people_counter.fabric_capability_probe.resource.getrusage",
            return_value=type("Usage", (), {"ru_maxrss": 0})(),
        ):
            result = probe_rss_high_water_mark()
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("non-positive", result.evidence)


class ProbeSparkEventLogAccessibleTests(unittest.TestCase):
    def test_reports_blocked_when_directory_does_not_exist(self) -> None:
        result = probe_spark_event_log_accessible("/nonexistent/path/for/test")
        self.assertEqual(result.capability, "spark_event_log_accessible")
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("does not exist", result.evidence)

    def test_reports_blocked_when_directory_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = probe_spark_event_log_accessible(directory)
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("empty", result.evidence)

    def test_reports_available_with_entry_count_when_directory_has_entries(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "app-0001").write_text("{}")
            result = probe_spark_event_log_accessible(directory)
        self.assertEqual(result.capability, "spark_event_log_accessible")
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertEqual(result.value, ["app-0001"])


class HostToExecutorIdMapAndResolveTests(unittest.TestCase):
    """Direct unit tests for the host->executor-id resolution helpers.

    These exercise ``_host_to_executor_id_map``/``_resolve_executor_id`` in
    isolation (not only indirectly through the probe functions) to pin down
    the unknown-host fallback behavior that backward compatibility depends
    on.
    """

    @staticmethod
    def _executors() -> list[ExecutorRecord]:
        return [
            ExecutorRecord(
                executor_id="1", host="vm-aaa", total_cores=4, max_memory_bytes=1
            ),
            ExecutorRecord(
                executor_id="2", host="vm-bbb", total_cores=4, max_memory_bytes=1
            ),
        ]

    def test_builds_host_to_executor_id_map_from_executor_records(self) -> None:
        mapping = _host_to_executor_id_map(self._executors())
        self.assertEqual(mapping, {"vm-aaa": "1", "vm-bbb": "2"})

    def test_raises_on_ambiguous_host_shared_by_two_executor_ids(self) -> None:
        executors = [
            ExecutorRecord(
                executor_id="1", host="vm-shared", total_cores=4, max_memory_bytes=1
            ),
            ExecutorRecord(
                executor_id="2", host="vm-shared", total_cores=4, max_memory_bytes=1
            ),
        ]
        with self.assertRaisesRegex(RuntimeError, "cannot reliably resolve"):
            _host_to_executor_id_map(executors)

    def test_resolve_prefers_host_resolved_id_over_reported_id(self) -> None:
        mapping = _host_to_executor_id_map(self._executors())
        resolved = _resolve_executor_id(mapping, "vm-bbb", reported_executor_id="vm-bbb")
        self.assertEqual(resolved, "2")

    def test_resolve_falls_back_to_reported_id_when_host_is_unknown(self) -> None:
        mapping = _host_to_executor_id_map(self._executors())
        resolved = _resolve_executor_id(
            mapping, "vm-unknown-host", reported_executor_id="vm-unknown-host"
        )
        self.assertEqual(resolved, "vm-unknown-host")


class ProbeExecutorPeakRssBytesTests(unittest.TestCase):
    @staticmethod
    def _executors(*executor_ids: str) -> tuple[ExecutorRecord, ...]:
        return tuple(
            ExecutorRecord(
                executor_id=executor_id,
                host=f"host-{executor_id}",
                total_cores=4,
                max_memory_bytes=1024,
            )
            for executor_id in executor_ids
        )

    def test_reports_available_with_the_max_reading_per_executor(self) -> None:
        executors = self._executors("1", "2")
        rows = [
            ("1", "host-1", 100),
            ("1", "host-1", 300),
            ("2", "host-2", 200),
        ]

        def fake_probe_partitions(spark_session, partitions, probe):
            self.assertEqual(partitions, len(executors) * 4)
            return rows

        result = probe_executor_peak_rss_bytes(
            MagicMock(),
            executors,
            warm_up=lambda: None,
            probe_partitions=fake_probe_partitions,
        )
        self.assertEqual(result.capability, "executor_peak_rss_bytes")
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertEqual(result.value, 300)
        self.assertIn("'1': 300", result.evidence)
        self.assertIn("'2': 200", result.evidence)

    def test_resolves_readings_to_the_discovered_executor_id_by_host(self) -> None:
        """Live-verified regression: on the deployed Fabric Spark 4.1.1
        runtime, ``SPARK_EXECUTOR_ID`` is never set inside task processes,
        so every reading's reported id is really just the observed
        hostname (e.g. ``vm-2de69869``), never the numeric
        ``discover_active_executors`` id (e.g. ``"1"``). The probe must
        still recognize these as the discovered executors by resolving
        through ``ExecutorRecord.host``, not report every executor as
        unobserved.
        """
        executors = (
            ExecutorRecord(
                executor_id="1", host="vm-aaa", total_cores=4, max_memory_bytes=1024
            ),
            ExecutorRecord(
                executor_id="2", host="vm-bbb", total_cores=4, max_memory_bytes=1024
            ),
        )
        # Reported executor_id equals the observed hostname (the real
        # fallback behavior), never the discovered numeric id.
        rows = [
            ("vm-aaa", "vm-aaa", 100),
            ("vm-bbb", "vm-bbb", 200),
        ]

        result = probe_executor_peak_rss_bytes(
            MagicMock(),
            executors,
            warm_up=lambda: None,
            probe_partitions=lambda spark_session, partitions, probe: rows,
        )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertEqual(result.value, 200)
        self.assertIn("'1': 100", result.evidence)
        self.assertIn("'2': 200", result.evidence)

    def test_rejects_ambiguous_host_shared_by_two_discovered_executor_ids(
        self,
    ) -> None:
        executors = (
            ExecutorRecord(
                executor_id="1", host="vm-shared", total_cores=4, max_memory_bytes=1024
            ),
            ExecutorRecord(
                executor_id="2", host="vm-shared", total_cores=4, max_memory_bytes=1024
            ),
        )

        result = probe_executor_peak_rss_bytes(
            MagicMock(),
            executors,
            warm_up=lambda: None,
            probe_partitions=lambda spark_session, partitions, probe: [
                ("vm-shared", "vm-shared", 100)
            ],
        )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("reported by multiple executor ids", result.evidence)

    def test_reports_blocked_when_an_executor_is_never_observed(self) -> None:
        executors = self._executors("1", "2")

        def fake_probe_partitions(spark_session, partitions, probe):
            return [("1", "host-1", 100)]

        result = probe_executor_peak_rss_bytes(
            MagicMock(),
            executors,
            warm_up=lambda: None,
            probe_partitions=fake_probe_partitions,
        )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("no RSS reading observed", result.evidence)
        self.assertIn("'2'", result.evidence)

    def test_reports_blocked_when_a_reading_is_non_positive(self) -> None:
        executors = self._executors("1")

        def fake_probe_partitions(spark_session, partitions, probe):
            return [("1", "host-1", 0)]

        result = probe_executor_peak_rss_bytes(
            MagicMock(),
            executors,
            warm_up=lambda: None,
            probe_partitions=fake_probe_partitions,
        )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("non-positive RSS", result.evidence)

    def test_rejects_empty_executor_inventory(self) -> None:
        result = probe_executor_peak_rss_bytes(
            MagicMock(),
            (),
            probe_partitions=lambda spark_session, partitions, probe: [],
        )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("executors must not be empty", result.evidence)

    def test_default_probe_partitions_runs_warm_up_once_per_task_on_executors(
        self,
    ) -> None:
        executors = self._executors("1")
        spark = MagicMock()

        class FakeRDD:
            def __init__(self, values):
                self.values = list(values)

            def mapPartitionsWithIndex(self, function):
                results = []
                for index in range(len(self.values)):
                    results.extend(function(index, iter([self.values[index]])))
                return type("Result", (), {"collect": lambda s: results})()

        spark.sparkContext.parallelize.side_effect = (
            lambda values, partitions: FakeRDD(values)
        )
        calls = []
        with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
            result = probe_executor_peak_rss_bytes(
                spark,
                executors,
                warm_up=lambda: calls.append("warm"),
            )
        spark.sparkContext.parallelize.assert_called_once()
        args, _ = spark.sparkContext.parallelize.call_args
        self.assertEqual(args[1], len(executors) * 4)
        self.assertEqual(len(calls), len(executors) * 4)
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertGreater(result.value, 0)


class ProbeDirectMountedLakehousePathTests(unittest.TestCase):
    @staticmethod
    def _executors(*executor_ids: str) -> tuple[ExecutorRecord, ...]:
        return tuple(
            ExecutorRecord(
                executor_id=executor_id,
                host=f"host-{executor_id}",
                total_cores=4,
                max_memory_bytes=1024,
            )
            for executor_id in executor_ids
        )

    def test_reports_available_when_every_executor_round_trips_successfully(
        self,
    ) -> None:
        import itertools
        import os
        import socket as real_socket

        executors = self._executors("host-1", "host-2")

        class FakeRDD:
            def __init__(self, probe):
                self.probe = probe

            def collect(self):
                rows = []
                for index in range(len(executors) * 4):
                    rows.extend(self.probe(index))
                return rows

        def fake_probe_partitions(spark_session, partitions, probe):
            self.assertEqual(partitions, len(executors) * 4)
            return FakeRDD(probe).collect()

        ids = itertools.cycle(["host-1", "host-2"])

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.object(
                real_socket, "gethostname", lambda: next(ids)
            ), patch.dict(os.environ):
                # The real probe falls back to the observed hostname as the
                # executor identity when SPARK_EXECUTOR_ID is unset, exactly
                # as it would on a bare-metal/local Spark executor.
                os.environ.pop("SPARK_EXECUTOR_ID", None)
                result = probe_direct_mounted_lakehouse_path(
                    MagicMock(),
                    executors,
                    mount_root=tmp_dir,
                    probe_partitions=fake_probe_partitions,
                )
        self.assertEqual(result.capability, "direct_mounted_lakehouse_path")
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertEqual(result.value, tmp_dir)
        self.assertIn("proven read/write/atomic-rename/fsync-capable", result.evidence)
        # The probe must actually have removed its own marker files, not
        # left a litter of test artifacts under the mounted root.
        marker_dir = Path(tmp_dir) / "_capability_probe" / "direct_mount"
        self.assertEqual(list(marker_dir.glob("probe-*")), [])

    def test_reports_available_when_numeric_executor_ids_differ_from_hostnames(
        self,
    ) -> None:
        """Live-verified regression: the deployed Fabric Spark 4.1.1 runtime
        discovers numeric executor ids (``"1"``, ``"2"``) that never equal
        the observed hostname (``vm-aaa``, ``vm-bbb``), and never sets
        ``SPARK_EXECUTOR_ID``. The probe must still prove the mount on
        every discovered executor instead of reporting them all missing.
        """
        import itertools
        import os
        import socket as real_socket

        executors = (
            ExecutorRecord(
                executor_id="1", host="vm-aaa", total_cores=4, max_memory_bytes=1024
            ),
            ExecutorRecord(
                executor_id="2", host="vm-bbb", total_cores=4, max_memory_bytes=1024
            ),
        )

        class FakeRDD:
            def __init__(self, probe):
                self.probe = probe

            def collect(self):
                rows = []
                for index in range(len(executors) * 4):
                    rows.extend(self.probe(index))
                return rows

        def fake_probe_partitions(spark_session, partitions, probe):
            return FakeRDD(probe).collect()

        hosts = itertools.cycle(["vm-aaa", "vm-bbb"])

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.object(
                real_socket, "gethostname", lambda: next(hosts)
            ), patch.dict(os.environ):
                os.environ.pop("SPARK_EXECUTOR_ID", None)
                result = probe_direct_mounted_lakehouse_path(
                    MagicMock(),
                    executors,
                    mount_root=tmp_dir,
                    probe_partitions=fake_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertIn("2 executors", result.evidence)

    def test_reports_blocked_when_one_executor_reading_fails(self) -> None:
        executors = self._executors("1", "2")
        rows = [
            ("1", "host-1", True, "ok"),
            ("2", "host-2", False, "PermissionError: denied"),
        ]

        def fake_probe_partitions(spark_session, partitions, probe):
            return rows

        result = probe_direct_mounted_lakehouse_path(
            MagicMock(),
            executors,
            mount_root="/lakehouse/default",
            probe_partitions=fake_probe_partitions,
        )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("not reliably usable", result.evidence)
        self.assertIn("'2'", result.evidence)
        self.assertIn("PermissionError", result.evidence)

    def test_requires_every_reading_for_an_executor_to_succeed_not_just_one(
        self,
    ) -> None:
        # An executor with two oversampled readings, one success and one
        # failure, must fail the whole probe -- a single lucky success must
        # never mask a genuine intermittent failure.
        executors = self._executors("1")
        rows = [
            ("1", "host-1", True, "ok on first task"),
            ("1", "host-1", False, "OSError: stale mount on second task"),
        ]

        def fake_probe_partitions(spark_session, partitions, probe):
            return rows

        result = probe_direct_mounted_lakehouse_path(
            MagicMock(),
            executors,
            probe_partitions=fake_probe_partitions,
        )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("'1'", result.evidence)

    def test_reports_blocked_when_an_executor_is_never_observed(self) -> None:
        executors = self._executors("1", "2")

        def fake_probe_partitions(spark_session, partitions, probe):
            return [("1", "host-1", True, "ok")]

        result = probe_direct_mounted_lakehouse_path(
            MagicMock(),
            executors,
            probe_partitions=fake_probe_partitions,
        )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("no direct-mount reading observed", result.evidence)
        self.assertIn("'2'", result.evidence)

    def test_rejects_empty_executor_inventory(self) -> None:
        result = probe_direct_mounted_lakehouse_path(
            MagicMock(),
            (),
            probe_partitions=lambda spark_session, partitions, probe: [],
        )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("executors must not be empty", result.evidence)

    def test_real_probe_partition_logic_reports_read_back_mismatch(self) -> None:
        # Exercise the actual internal _probe_partition closure (not a
        # stand-in) against a mount path that does not support real file
        # semantics, proving the read-back-mismatch branch fires rather
        # than only being reachable in theory.
        executors = self._executors("1")

        class FakeRDD:
            def __init__(self, probe):
                self.probe = probe

            def collect(self):
                return list(self.probe(0))

        def fake_probe_partitions(spark_session, partitions, probe):
            return FakeRDD(probe).collect()

        with tempfile.TemporaryDirectory() as tmp_dir:
            real_read_text = Path.read_text

            def tampering_read_text(self, *args, **kwargs):
                # Simulate a mount that silently returns stale/different
                # content on read-back (e.g. a caching proxy mount).
                return real_read_text(self, *args, **kwargs) + "-tampered"

            with patch.object(Path, "read_text", tampering_read_text), patch.dict(
                "os.environ", {"SPARK_EXECUTOR_ID": "1"}
            ):
                result = probe_direct_mounted_lakehouse_path(
                    MagicMock(),
                    executors,
                    mount_root=tmp_dir,
                    probe_partitions=fake_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("read-back mismatch", result.evidence)

    def test_uses_the_documented_default_mount_root_and_probe_dir(self) -> None:
        # Exercise the real internal closure without overriding mount_root
        # or relative_probe_dir, forcing a failure inside it (rather than
        # touching the real, possibly-absent, /lakehouse/default on this
        # host) and proving the failure evidence reflects exactly the
        # documented defaults, not some other accidental path.
        executors = self._executors("1")

        class FakeRDD:
            def __init__(self, probe):
                self.probe = probe

            def collect(self):
                return list(self.probe(0))

        def fake_probe_partitions(spark_session, partitions, probe):
            return FakeRDD(probe).collect()

        with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}), patch.object(
            Path, "mkdir", side_effect=OSError("simulated disk full")
        ):
            result = probe_direct_mounted_lakehouse_path(
                MagicMock(),
                executors,
                probe_partitions=fake_probe_partitions,
            )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn(
            "OSError: simulated disk full "
            "(root=/lakehouse/default/_capability_probe/direct_mount)",
            result.evidence,
        )

    def test_forwards_the_real_spark_session_to_probe_partitions(self) -> None:
        executors = self._executors("1")
        spark = MagicMock()
        seen_sessions = []

        def fake_probe_partitions(spark_session, partitions, probe):
            seen_sessions.append(spark_session)
            return [("1", "host-1", True, "ok")]

        probe_direct_mounted_lakehouse_path(
            spark, executors, probe_partitions=fake_probe_partitions
        )
        self.assertEqual(seen_sessions, [spark])

    def test_accumulates_every_reading_for_an_executor_not_just_the_last(
        self,
    ) -> None:
        # A failing first reading followed by a succeeding second reading
        # for the same executor must still fail the whole probe closed --
        # every observed reading for an executor must be retained and
        # checked, never overwritten/narrowed to only the most recent one.
        executors = self._executors("1")
        rows = [
            ("1", "host-1", False, "OSError: stale mount on first task"),
            ("1", "host-1", True, "ok on second task"),
        ]

        def fake_probe_partitions(spark_session, partitions, probe):
            return rows

        result = probe_direct_mounted_lakehouse_path(
            MagicMock(), executors, probe_partitions=fake_probe_partitions
        )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("stale mount on first task", result.evidence)
        self.assertIn("ok on second task", result.evidence)

    def test_per_executor_evidence_reports_the_most_recent_reading(self) -> None:
        executors = self._executors("1")
        rows = [
            ("1", "host-1", True, "e0"),
            ("1", "host-1", True, "e1"),
            ("1", "host-1", True, "e2"),
        ]

        def fake_probe_partitions(spark_session, partitions, probe):
            return rows

        result = probe_direct_mounted_lakehouse_path(
            MagicMock(), executors, probe_partitions=fake_probe_partitions
        )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertIn("'1': 'e2'", result.evidence)



def _flatten_probe_partitions(spark_session, partitions, probe):
    """Shared fake ``probe_partitions`` that really calls ``probe(index)``.

    Mirrors how the real ``_default_probe_partitions``/Spark
    ``mapPartitionsWithIndex().collect()`` pipeline flattens each task's
    returned rows, but without needing a real ``SparkContext`` -- this lets
    tests exercise the real per-executor probe closures (including real
    cv2/safetensors/onnxruntime calls) against a fake executor topology.
    """
    rows = []
    for index in range(partitions):
        rows.extend(probe(index))
    return rows


def _single_executor() -> tuple[ExecutorRecord, ...]:
    return (
        ExecutorRecord(
            executor_id="1", host="host-1", total_cores=4, max_memory_bytes=1024
        ),
    )


def _two_executors() -> tuple[ExecutorRecord, ...]:
    return (
        ExecutorRecord(
            executor_id="1", host="host-1", total_cores=4, max_memory_bytes=1024
        ),
        ExecutorRecord(
            executor_id="2", host="host-2", total_cores=4, max_memory_bytes=1024
        ),
    )


class ConsumerArtifactTests(unittest.TestCase):
    def test_rejects_an_empty_relative_path(self) -> None:
        with self.assertRaises(CapabilityProbeError) as ctx:
            ConsumerArtifact(relative_path="")
        self.assertEqual(str(ctx.exception), "relative_path is required")

    def test_accepts_an_optional_expected_hash(self) -> None:
        artifact = ConsumerArtifact(relative_path="models/a.bin")
        self.assertIsNone(artifact.expected_sha256)
        hashed = ConsumerArtifact(relative_path="models/a.bin", expected_sha256="abc")
        self.assertEqual(hashed.expected_sha256, "abc")


class StreamSha256Tests(unittest.TestCase):
    """Direct proof that the module-private hasher never performs a
    whole-file ``read()``/``read(None)`` -- it must always request the
    fixed, bounded chunk size, exactly like ``fabric_input_resolver``'s own
    ``stream_sha256``."""

    def test_reads_in_bounded_chunks_never_the_whole_file(self) -> None:
        import hashlib

        from people_counter import fabric_capability_probe

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "video.bin"
            content = b"z" * 55
            path.write_bytes(content)

            with patch.object(
                fabric_capability_probe, "_STREAM_CHUNK_BYTES", 10
            ):
                read_sizes: list[object] = []
                real_open = Path.open

                def recording_open(self: Path, *args: object, **kwargs: object):
                    handle = real_open(self, *args, **kwargs)
                    original_read = handle.read

                    def recording_read(size: object = -1) -> bytes:
                        read_sizes.append(size)
                        return original_read(size)

                    handle.read = recording_read
                    return handle

                with patch.object(Path, "open", recording_open):
                    digest = fabric_capability_probe._stream_sha256(path)

            self.assertEqual(digest, hashlib.sha256(content).hexdigest())
            # Every read must request the bounded chunk size explicitly --
            # never a single unbounded ``read()``/``read(None)`` of the
            # whole file (that would defeat the purpose of streaming).
            self.assertTrue(read_sizes)
            self.assertTrue(all(size == 10 for size in read_sizes))
            self.assertGreater(len(read_sizes), 1)


class RunPerExecutorTests(unittest.TestCase):
    """Direct proof of :func:`_run_per_executor`'s fan-out/aggregation
    contract, independent of any one composed probe that happens to call
    it."""

    @staticmethod
    def _executors(*ids: str):
        return [
            SimpleNamespace(executor_id=executor_id, host=f"host-{executor_id}")
            for executor_id in ids
        ]

    def test_raises_when_executors_is_empty(self) -> None:
        from people_counter.fabric_capability_probe import _run_per_executor

        with self.assertRaises(RuntimeError) as ctx:
            _run_per_executor(
                "spark",
                [],
                oversample_per_executor=4,
                probe_partitions=lambda session, n, fn: [],
                probe_partition=lambda i: None,
            )
        self.assertEqual(str(ctx.exception), "executors must not be empty")

    def test_forwards_the_real_spark_session_to_probe_partitions(self) -> None:
        from people_counter.fabric_capability_probe import _run_per_executor

        recorded = {}

        def fake_probe_partitions(session, n, fn):
            recorded["session"] = session
            recorded["n"] = n
            return [("1", "host-1", True, "ok")]

        sentinel_session = object()
        _run_per_executor(
            sentinel_session,
            self._executors("1"),
            oversample_per_executor=4,
            probe_partitions=fake_probe_partitions,
            probe_partition=lambda i: None,
        )
        self.assertIs(recorded["session"], sentinel_session)
        self.assertEqual(recorded["n"], 4)

    def test_aggregates_multiple_oversampled_readings_per_executor(self) -> None:
        from people_counter.fabric_capability_probe import _run_per_executor

        rows = [
            ("1", "host-1", True, "first"),
            ("1", "host-1", True, "second"),
            ("1", "host-1", False, "third"),
        ]
        observed = _run_per_executor(
            "spark",
            self._executors("1"),
            oversample_per_executor=3,
            probe_partitions=lambda session, n, fn: rows,
            probe_partition=lambda i: None,
        )
        host, oks, evidence = observed["1"]
        self.assertEqual(host, "host-1")
        # Every oversampled (ok, evidence) pair must be preserved in order
        # -- not overwritten/collapsed to just the first or last reading.
        self.assertEqual(oks, [True, True, False])
        self.assertEqual(evidence, ["first", "second", "third"])

    def test_raises_when_a_discovered_executor_produced_zero_readings(self) -> None:
        from people_counter.fabric_capability_probe import _run_per_executor

        with self.assertRaises(RuntimeError) as ctx:
            _run_per_executor(
                "spark",
                self._executors("1", "2"),
                oversample_per_executor=1,
                probe_partitions=lambda session, n, fn: [("1", "host-1", True, "ok")],
                probe_partition=lambda i: None,
            )
        self.assertIn("'2'", str(ctx.exception))

    def test_resolves_readings_to_the_discovered_executor_id_by_host(self) -> None:
        """Live-verified regression: real rows report the observed hostname
        as the executor id (``SPARK_EXECUTOR_ID`` unset on the deployed
        Fabric Spark 4.1.1 runtime), never the discovered numeric id, and
        the aggregation must still resolve them via ``ExecutorRecord.host``
        instead of reporting every discovered executor as unobserved.
        """
        from people_counter.fabric_capability_probe import _run_per_executor

        executors = [
            SimpleNamespace(executor_id="1", host="vm-aaa"),
            SimpleNamespace(executor_id="2", host="vm-bbb"),
        ]
        rows = [
            ("vm-aaa", "vm-aaa", True, "ok-1"),
            ("vm-bbb", "vm-bbb", True, "ok-2"),
        ]
        observed = _run_per_executor(
            "spark",
            executors,
            oversample_per_executor=1,
            probe_partitions=lambda session, n, fn: rows,
            probe_partition=lambda i: None,
        )
        self.assertEqual(set(observed), {"1", "2"})
        self.assertEqual(observed["1"], ("vm-aaa", [True], ["ok-1"]))
        self.assertEqual(observed["2"], ("vm-bbb", [True], ["ok-2"]))


class CanonicalJsonTests(unittest.TestCase):
    """Direct proof of every ``json.dumps`` keyword argument
    :func:`_canonical_json` fixes -- each is required for a byte-stable,
    content-addressable evidence digest, not merely a readability choice."""

    def test_sorts_keys_regardless_of_insertion_order(self) -> None:
        from people_counter.fabric_capability_probe import _canonical_json

        first = _canonical_json({"b": 1, "a": 2})
        second = _canonical_json({"a": 2, "b": 1})
        self.assertEqual(first, second)
        self.assertEqual(first, '{"a":2,"b":1}')

    def test_uses_compact_separators_with_no_whitespace(self) -> None:
        from people_counter.fabric_capability_probe import _canonical_json

        self.assertEqual(_canonical_json({"a": 1, "b": [1, 2]}), '{"a":1,"b":[1,2]}')

    def test_escapes_non_ascii_characters(self) -> None:
        from people_counter.fabric_capability_probe import _canonical_json

        self.assertEqual(_canonical_json({"a": "\u00e9"}), '{"a":"\\u00e9"}')

    def test_rejects_non_finite_floats(self) -> None:
        from people_counter.fabric_capability_probe import _canonical_json

        with self.assertRaises(ValueError):
            _canonical_json({"a": float("nan")})


class RequireAllOkTests(unittest.TestCase):
    """Direct proof of :func:`_require_all_ok`'s two behaviors: fail closed
    on any ``False`` reading, and -- when every reading is OK -- return the
    *last* (most recent) evidence string per executor, never an arbitrary
    earlier one."""

    def test_returns_the_last_evidence_entry_per_executor(self) -> None:
        from people_counter.fabric_capability_probe import _require_all_ok

        observed = {
            "1": ("host-1", [True, True, True], ["first", "second", "third"]),
            "2": ("host-2", [True, True], ["alpha", "beta"]),
        }
        result = _require_all_ok(observed, label="test probe")
        assert result == {"1": "third", "2": "beta"}

    def test_raises_when_any_executor_has_a_failed_reading(self) -> None:
        from people_counter.fabric_capability_probe import _require_all_ok

        observed = {
            "1": ("host-1", [True, False], ["ok", "boom"]),
        }
        with self.assertRaises(RuntimeError):
            _require_all_ok(observed, label="test probe")


class ProbeStreamHashArtifactsTests(unittest.TestCase):
    def _patch_executor_id(self):
        return patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"})

    def test_reports_available_when_the_stream_hash_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "models" / "a.bin"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"fixed immutable content")
            import hashlib

            expected = hashlib.sha256(b"fixed immutable content").hexdigest()
            artifact = ConsumerArtifact(
                relative_path="models/a.bin", expected_sha256=expected
            )
            with self._patch_executor_id():
                result = probe_stream_hash_artifacts(
                    MagicMock(),
                    _single_executor(),
                    artifacts=(artifact,),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.capability, "direct_mount_stream_hash")
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertIn("streamed sha256", result.evidence)

    def test_reports_blocked_when_the_hash_does_not_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "a.bin"
            source.write_bytes(b"actual content")
            artifact = ConsumerArtifact(
                relative_path="a.bin", expected_sha256="0" * 64
            )
            with self._patch_executor_id():
                result = probe_stream_hash_artifacts(
                    MagicMock(),
                    _single_executor(),
                    artifacts=(artifact,),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("!= expected", result.evidence)
        # The exact reviewed label must be embedded in the failure evidence
        # -- never dropped/replaced by ``_require_all_ok``.
        self.assertTrue(
            result.evidence.startswith(
                "RuntimeError: stream-hash of fixed artifacts is not "
                "reliable on every executor:"
            )
        )

    def test_reports_blocked_when_an_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifact = ConsumerArtifact(relative_path="missing.bin")
            with self._patch_executor_id():
                result = probe_stream_hash_artifacts(
                    MagicMock(),
                    _single_executor(),
                    artifacts=(artifact,),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("missing.bin", result.evidence)
        self.assertIn("FileNotFoundError", result.evidence)

    def test_rejects_an_empty_artifact_tuple(self) -> None:
        result = probe_stream_hash_artifacts(
            MagicMock(),
            _single_executor(),
            artifacts=(),
            probe_partitions=_flatten_probe_partitions,
        )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertEqual(result.evidence, "RuntimeError: artifacts must not be empty")

    def test_requires_every_discovered_executor_to_be_observed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "a.bin"
            source.write_bytes(b"content")
            artifact = ConsumerArtifact(relative_path="a.bin")

            def only_executor_one(spark_session, partitions, probe):
                # Simulates executor "2" never reporting in at all.
                return probe(0)

            result = probe_stream_hash_artifacts(
                MagicMock(),
                _two_executors(),
                artifacts=(artifact,),
                mount_root=tmp_dir,
                probe_partitions=only_executor_one,
            )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("no reading observed", result.evidence)

    def test_default_mount_root_is_the_fixed_lakehouse_default_path(self) -> None:
        # Deliberately does not override ``mount_root`` -- proves the exact
        # fixed default constant, not merely "some" default.
        artifact = ConsumerArtifact(relative_path="__mutmut_canary_missing__.bin")
        with self._patch_executor_id():
            result = probe_stream_hash_artifacts(
                MagicMock(),
                _single_executor(),
                artifacts=(artifact,),
                probe_partitions=_flatten_probe_partitions,
            )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn(
            "/lakehouse/default/__mutmut_canary_missing__.bin", result.evidence
        )

    def test_default_oversample_per_executor_is_four(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "a.bin"
            source.write_bytes(b"content")
            artifact = ConsumerArtifact(relative_path="a.bin")
            recorded: dict[str, Any] = {}

            def recording_probe_partitions(spark_session, partitions, probe):
                recorded["partitions"] = partitions
                return _flatten_probe_partitions(spark_session, partitions, probe)

            with self._patch_executor_id():
                probe_stream_hash_artifacts(
                    MagicMock(),
                    _single_executor(),
                    artifacts=(artifact,),
                    mount_root=tmp_dir,
                    probe_partitions=recording_probe_partitions,
                )
        self.assertEqual(recorded["partitions"], 4)

    def test_forwards_the_real_spark_session_to_probe_partitions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "a.bin"
            source.write_bytes(b"content")
            artifact = ConsumerArtifact(relative_path="a.bin")
            recorded: dict[str, Any] = {}

            def recording_probe_partitions(spark_session, partitions, probe):
                recorded["session"] = spark_session
                return _flatten_probe_partitions(spark_session, partitions, probe)

            sentinel = MagicMock()
            with self._patch_executor_id():
                probe_stream_hash_artifacts(
                    sentinel,
                    _single_executor(),
                    artifacts=(artifact,),
                    mount_root=tmp_dir,
                    probe_partitions=recording_probe_partitions,
                )
        self.assertIs(recorded["session"], sentinel)

    def test_continues_checking_remaining_artifacts_after_a_mismatch(self) -> None:
        import hashlib

        with tempfile.TemporaryDirectory() as tmp_dir:
            bad = Path(tmp_dir) / "bad.bin"
            bad.write_bytes(b"actual")
            good = Path(tmp_dir) / "good.bin"
            good.write_bytes(b"good content")
            good_digest = hashlib.sha256(b"good content").hexdigest()
            artifacts = (
                ConsumerArtifact(relative_path="bad.bin", expected_sha256="0" * 64),
                ConsumerArtifact(
                    relative_path="good.bin", expected_sha256=good_digest
                ),
            )
            with self._patch_executor_id():
                result = probe_stream_hash_artifacts(
                    MagicMock(),
                    _single_executor(),
                    artifacts=artifacts,
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        # Every artifact after a mismatch must still be checked -- never
        # silently skipped by an early loop exit.
        self.assertIn("bad.bin", result.evidence)
        self.assertIn("good.bin", result.evidence)

    def test_falls_back_to_hostname_as_executor_id_when_unset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "a.bin"
            source.write_bytes(b"content")
            artifact = ConsumerArtifact(relative_path="a.bin")
            with patch("socket.gethostname", return_value="fake-host-xyz"):
                import os as _os

                previous = _os.environ.pop("SPARK_EXECUTOR_ID", None)
                try:
                    result = probe_stream_hash_artifacts(
                        MagicMock(),
                        _single_executor(),
                        artifacts=(artifact,),
                        mount_root=tmp_dir,
                        probe_partitions=_flatten_probe_partitions,
                    )
                finally:
                    if previous is not None:
                        _os.environ["SPARK_EXECUTOR_ID"] = previous
        # The discovered executor declares ``executor_id="1"``, but with no
        # ``SPARK_EXECUTOR_ID`` env var the real hostname must be used as
        # the fallback id -- never ``None`` -- so the id never matches "1"
        # and the probe fails closed, citing the real (patched) hostname.
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("fake-host-xyz", result.evidence)


class ProbeOpenCvVideoConsumerTests(unittest.TestCase):
    @staticmethod
    def _write_sample_video(path: Path, *, frame_count: int = 10) -> None:
        import cv2
        import numpy as np

        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        writer = cv2.VideoWriter(str(path), fourcc, 5.0, (16, 16))
        try:
            for index in range(frame_count):
                frame = np.full((16, 16, 3), index * 20, dtype=np.uint8)
                writer.write(frame)
        finally:
            writer.release()

    def test_reports_available_for_a_real_readable_video(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            video_path = Path(tmp_dir) / "videos" / "sample.mp4"
            video_path.parent.mkdir(parents=True)
            self._write_sample_video(video_path)
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_opencv_video_consumer(
                    MagicMock(),
                    _single_executor(),
                    video=ConsumerArtifact(relative_path="videos/sample.mp4"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.capability, "direct_mount_opencv_video_consumer")
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertIn("opened, 10 frames", result.evidence)
        self.assertIn("decoded frames", result.evidence)

    def test_reports_blocked_when_the_video_file_does_not_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_opencv_video_consumer(
                    MagicMock(),
                    _single_executor(),
                    video=ConsumerArtifact(relative_path="missing.mp4"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("could not open", result.evidence)

    def test_reports_blocked_when_decoding_a_frame_fails(self) -> None:
        # A capture that reports itself open with positive metadata but
        # fails to decode any frame must still fail closed, not silently
        # pass on metadata alone.
        fake_capture = MagicMock()
        fake_capture.isOpened.return_value = True
        fake_capture.get.side_effect = lambda prop: {0: 10, 1: 2}.get(prop, 10)
        fake_capture.read.return_value = (False, None)

        with patch(
            "cv2.VideoCapture", return_value=fake_capture
        ), patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
            result = probe_opencv_video_consumer(
                MagicMock(),
                _single_executor(),
                video=ConsumerArtifact(relative_path="sample.mp4"),
                mount_root="/lakehouse/default",
                probe_partitions=_flatten_probe_partitions,
            )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("failed to decode frame", result.evidence)
        # Called once per (oversampled) probe task; every invocation must
        # still release its capture, not just the first.
        self.assertEqual(
            fake_capture.release.call_count, fake_capture.isOpened.call_count
        )
        self.assertGreater(fake_capture.release.call_count, 0)

    @staticmethod
    def _fake_capture(*, frame_count: int, width: int, height: int, fps: float = 5.0):
        import cv2
        import numpy as np

        values = {
            cv2.CAP_PROP_FRAME_COUNT: frame_count,
            cv2.CAP_PROP_FRAME_WIDTH: width,
            cv2.CAP_PROP_FRAME_HEIGHT: height,
            cv2.CAP_PROP_FPS: fps,
        }
        fake_capture = MagicMock()
        fake_capture.isOpened.return_value = True
        fake_capture.get.side_effect = lambda prop, _values=values: _values.get(
            prop, -999
        )
        fake_capture.read.return_value = (True, np.zeros((5, 5, 3), dtype="uint8"))
        return fake_capture

    def test_uses_the_default_lakehouse_mount_root_when_not_given(self) -> None:
        with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
            result = probe_opencv_video_consumer(
                MagicMock(),
                _single_executor(),
                video=ConsumerArtifact(relative_path="missing.mp4"),
                probe_partitions=_flatten_probe_partitions,
            )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("/lakehouse/default/missing.mp4", result.evidence)

    def test_oversamples_four_reads_per_executor_by_default(self) -> None:
        recorded_partitions: list[int] = []

        def recording_probe_partitions(spark_session, partitions, probe):
            recorded_partitions.append(partitions)
            return _flatten_probe_partitions(spark_session, partitions, probe)

        with tempfile.TemporaryDirectory() as tmp_dir:
            video_path = Path(tmp_dir) / "videos" / "sample.mp4"
            video_path.parent.mkdir(parents=True)
            self._write_sample_video(video_path)
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                probe_opencv_video_consumer(
                    MagicMock(),
                    _single_executor(),
                    video=ConsumerArtifact(relative_path="videos/sample.mp4"),
                    mount_root=tmp_dir,
                    probe_partitions=recording_probe_partitions,
                )
        self.assertEqual(recorded_partitions, [4])

    def test_falls_back_to_the_hostname_when_no_spark_executor_id_is_set(self) -> None:
        environ_without_executor_id = {
            key: value
            for key, value in __import__("os").environ.items()
            if key != "SPARK_EXECUTOR_ID"
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            video_path = Path(tmp_dir) / "videos" / "sample.mp4"
            video_path.parent.mkdir(parents=True)
            self._write_sample_video(video_path)
            with patch.dict(
                "os.environ", environ_without_executor_id, clear=True
            ), patch("socket.gethostname", return_value="1"):
                result = probe_opencv_video_consumer(
                    MagicMock(),
                    _single_executor(),
                    video=ConsumerArtifact(relative_path="videos/sample.mp4"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)

    def test_the_reported_fps_metadata_comes_from_the_cap_prop_fps_flag(self) -> None:
        fake_capture = self._fake_capture(
            frame_count=10, width=5, height=5, fps=23.0
        )
        with patch(
            "cv2.VideoCapture", return_value=fake_capture
        ), patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
            result = probe_opencv_video_consumer(
                MagicMock(),
                _single_executor(),
                video=ConsumerArtifact(relative_path="sample.mp4"),
                mount_root="/lakehouse/default",
                probe_partitions=_flatten_probe_partitions,
            )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertIn("@23.00fps", result.evidence)

    def test_the_metadata_validity_check_covers_the_exact_zero_and_one_boundaries(
        self,
    ) -> None:
        cases = [
            (10, 5, 5, CapabilityStatus.AVAILABLE),
            (0, 5, 5, CapabilityStatus.FABRIC_PLATFORM_BLOCKED),
            (1, 5, 5, CapabilityStatus.AVAILABLE),
            (10, 0, 5, CapabilityStatus.FABRIC_PLATFORM_BLOCKED),
            (10, 1, 5, CapabilityStatus.AVAILABLE),
            (10, 5, 0, CapabilityStatus.FABRIC_PLATFORM_BLOCKED),
            (10, 5, 1, CapabilityStatus.AVAILABLE),
        ]
        for frame_count, width, height, expected_status in cases:
            with self.subTest(frame_count=frame_count, width=width, height=height):
                fake_capture = self._fake_capture(
                    frame_count=frame_count, width=width, height=height
                )
                with patch(
                    "cv2.VideoCapture", return_value=fake_capture
                ), patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                    result = probe_opencv_video_consumer(
                        MagicMock(),
                        _single_executor(),
                        video=ConsumerArtifact(relative_path="sample.mp4"),
                        mount_root="/lakehouse/default",
                        probe_partitions=_flatten_probe_partitions,
                    )
                self.assertEqual(result.status, expected_status)
                if expected_status == CapabilityStatus.FABRIC_PLATFORM_BLOCKED:
                    self.assertIn("non-positive metadata", result.evidence)

    def test_the_sampled_frame_indices_are_exactly_first_middle_and_last(
        self,
    ) -> None:
        import cv2

        fake_capture = self._fake_capture(frame_count=11, width=5, height=5)
        with patch(
            "cv2.VideoCapture", return_value=fake_capture
        ), patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
            result = probe_opencv_video_consumer(
                MagicMock(),
                _single_executor(),
                video=ConsumerArtifact(relative_path="sample.mp4"),
                mount_root="/lakehouse/default",
                probe_partitions=_flatten_probe_partitions,
            )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertIn("decoded frames [0, 5, 10]", result.evidence)
        self.assertEqual(
            fake_capture.set.call_args_list,
            [
                call(cv2.CAP_PROP_POS_FRAMES, 0),
                call(cv2.CAP_PROP_POS_FRAMES, 5),
                call(cv2.CAP_PROP_POS_FRAMES, 10),
            ]
            * 4,
        )

    def test_a_failed_read_still_fails_closed_even_when_a_frame_object_is_returned(
        self,
    ) -> None:
        import numpy as np

        fake_capture = self._fake_capture(frame_count=1, width=5, height=5)
        fake_capture.read.return_value = (False, np.zeros((5, 5, 3), dtype="uint8"))
        with patch(
            "cv2.VideoCapture", return_value=fake_capture
        ), patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
            result = probe_opencv_video_consumer(
                MagicMock(),
                _single_executor(),
                video=ConsumerArtifact(relative_path="sample.mp4"),
                mount_root="/lakehouse/default",
                probe_partitions=_flatten_probe_partitions,
            )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("failed to decode frame", result.evidence)

    def test_the_decoded_frame_shape_check_covers_the_exact_zero_and_one_boundaries(
        self,
    ) -> None:
        import numpy as np

        cases = [
            ((5, 5, 3), CapabilityStatus.AVAILABLE),
            ((0, 5, 3), CapabilityStatus.FABRIC_PLATFORM_BLOCKED),
            ((1, 5, 3), CapabilityStatus.AVAILABLE),
            ((5, 0, 3), CapabilityStatus.FABRIC_PLATFORM_BLOCKED),
            ((5, 1, 3), CapabilityStatus.AVAILABLE),
        ]
        for shape, expected_status in cases:
            with self.subTest(shape=shape):
                fake_capture = self._fake_capture(frame_count=1, width=5, height=5)
                fake_capture.read.return_value = (
                    True,
                    np.zeros(shape, dtype="uint8"),
                )
                with patch(
                    "cv2.VideoCapture", return_value=fake_capture
                ), patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                    result = probe_opencv_video_consumer(
                        MagicMock(),
                        _single_executor(),
                        video=ConsumerArtifact(relative_path="sample.mp4"),
                        mount_root="/lakehouse/default",
                        probe_partitions=_flatten_probe_partitions,
                    )
                self.assertEqual(result.status, expected_status)
                if expected_status == CapabilityStatus.FABRIC_PLATFORM_BLOCKED:
                    self.assertIn("has empty shape", result.evidence)

    def test_an_unexpected_failure_reports_the_real_exception_type_and_fails_closed(
        self,
    ) -> None:
        fake_capture = MagicMock()
        fake_capture.isOpened.return_value = True
        fake_capture.get.side_effect = ValueError("boom")
        with patch(
            "cv2.VideoCapture", return_value=fake_capture
        ), patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
            result = probe_opencv_video_consumer(
                MagicMock(),
                _single_executor(),
                video=ConsumerArtifact(relative_path="sample.mp4"),
                mount_root="/lakehouse/default",
                probe_partitions=_flatten_probe_partitions,
            )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("ValueError: boom", result.evidence)
        self.assertEqual(fake_capture.release.call_count, 4)

    def test_forwards_the_exact_spark_session_to_run_per_executor(self) -> None:
        recorded_sessions: list[Any] = []

        def recording_probe_partitions(spark_session, partitions, probe):
            recorded_sessions.append(spark_session)
            return _flatten_probe_partitions(spark_session, partitions, probe)

        sentinel_session = object()
        with tempfile.TemporaryDirectory() as tmp_dir:
            video_path = Path(tmp_dir) / "videos" / "sample.mp4"
            video_path.parent.mkdir(parents=True)
            self._write_sample_video(video_path)
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                probe_opencv_video_consumer(
                    sentinel_session,
                    _single_executor(),
                    video=ConsumerArtifact(relative_path="videos/sample.mp4"),
                    mount_root=tmp_dir,
                    probe_partitions=recording_probe_partitions,
                )
        self.assertEqual(recorded_sessions, [sentinel_session])

    def test_an_all_fail_result_uses_the_exact_opencv_video_consumer_label(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_opencv_video_consumer(
                    MagicMock(),
                    _single_executor(),
                    video=ConsumerArtifact(relative_path="missing.mp4"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertTrue(
            result.evidence.startswith(
                "RuntimeError: OpenCV video consumer proof is not reliable "
                "on every executor: "
            ),
            result.evidence,
        )

    def test_capability_name_is_exactly_direct_mount_opencv_video_consumer(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            video_path = Path(tmp_dir) / "sample.mp4"
            self._write_sample_video(video_path)
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_opencv_video_consumer(
                    MagicMock(),
                    _single_executor(),
                    video=ConsumerArtifact(relative_path="sample.mp4"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.capability, "direct_mount_opencv_video_consumer")


class ProbeModelFileConsumerTests(unittest.TestCase):
    def test_reports_available_for_a_real_safetensors_file(self) -> None:
        import torch
        from safetensors.torch import save_file

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "models" / "model.safetensors"
            model_path.parent.mkdir(parents=True)
            save_file({"weight": torch.ones(4, 4)}, str(model_path))
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_model_file_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(
                        relative_path="models/model.safetensors"
                    ),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertIn("partial read, not whole-file", result.evidence)

    def test_reports_available_for_a_real_legacy_pt_checkpoint(self) -> None:
        import torch

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "model.pt"
            torch.save({"weight": torch.ones(2, 2)}, str(model_path))
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_model_file_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="model.pt"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertIn("known full-file-read limitation of the format", result.evidence)

    def test_reports_blocked_for_a_safetensors_file_with_no_tensors(self) -> None:
        from safetensors.torch import save_file

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "empty.safetensors"
            save_file({}, str(model_path))
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_model_file_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="empty.safetensors"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("no tensors", result.evidence)

    def test_reports_blocked_when_the_model_file_does_not_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_model_file_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="missing.safetensors"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)

    def test_a_missing_model_file_reports_the_real_exception_type_name(self) -> None:
        recorded_rows: list[Any] = []

        def recording_probe_partitions(spark_session, partitions, probe):
            rows = _flatten_probe_partitions(spark_session, partitions, probe)
            recorded_rows.extend(rows)
            return rows

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                probe_model_file_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="missing.safetensors"),
                    mount_root=tmp_dir,
                    probe_partitions=recording_probe_partitions,
                )
        _, _, ok, evidence = recorded_rows[0]
        self.assertFalse(ok)
        self.assertTrue(evidence.startswith("FileNotFoundError:"))

    def test_the_default_mount_root_is_the_default_lakehouse_path(self) -> None:
        with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
            result = probe_model_file_consumer(
                MagicMock(),
                _single_executor(),
                model=ConsumerArtifact(relative_path="model.safetensors"),
                probe_partitions=_flatten_probe_partitions,
            )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("/lakehouse/default/model.safetensors", result.evidence)

    def test_the_default_oversample_per_executor_count_is_four(self) -> None:
        import torch
        from safetensors.torch import save_file

        recorded_partitions: list[int] = []

        def recording_probe_partitions(spark_session, partitions, probe):
            recorded_partitions.append(partitions)
            return _flatten_probe_partitions(spark_session, partitions, probe)

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "model.safetensors"
            save_file({"weight": torch.ones(2, 2)}, str(model_path))
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                probe_model_file_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="model.safetensors"),
                    mount_root=tmp_dir,
                    probe_partitions=recording_probe_partitions,
                )
        self.assertEqual(recorded_partitions, [4])

    def test_falls_back_to_the_hostname_when_spark_executor_id_is_unset(self) -> None:
        import torch
        from safetensors.torch import save_file

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "model.safetensors"
            save_file({"weight": torch.ones(2, 2)}, str(model_path))
            environ_without_executor_id = {
                key: value
                for key, value in __import__("os").environ.items()
                if key != "SPARK_EXECUTOR_ID"
            }
            with patch.dict(
                "os.environ", environ_without_executor_id, clear=True
            ), patch("socket.gethostname", return_value="1"):
                result = probe_model_file_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="model.safetensors"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)

    def test_the_safetensors_partial_read_evidence_text_is_exact(self) -> None:
        import torch
        from safetensors.torch import save_file

        recorded_rows: list[Any] = []

        def recording_probe_partitions(spark_session, partitions, probe):
            rows = _flatten_probe_partitions(spark_session, partitions, probe)
            recorded_rows.extend(rows)
            return rows

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "model.safetensors"
            save_file({"weight": torch.ones(4, 4)}, str(model_path))
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_model_file_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="model.safetensors"),
                    mount_root=tmp_dir,
                    probe_partitions=recording_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        _, _, ok, evidence = recorded_rows[0]
        self.assertTrue(ok)
        expected_path = Path(tmp_dir) / "model.safetensors"
        self.assertEqual(
            evidence,
            f"{expected_path}: safetensors, 1 tensors, sampled 'weight' "
            "shape=(4, 4) (partial read, not whole-file)",
        )

    def test_a_legacy_checkpoint_with_two_entries_reports_the_exact_evidence_text(
        self,
    ) -> None:
        import torch

        recorded_rows: list[Any] = []

        def recording_probe_partitions(spark_session, partitions, probe):
            rows = _flatten_probe_partitions(spark_session, partitions, probe)
            recorded_rows.extend(rows)
            return rows

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "model.pt"
            torch.save(
                {"weight": torch.ones(2, 2), "bias": torch.zeros(2)},
                str(model_path),
            )
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_model_file_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="model.pt"),
                    mount_root=tmp_dir,
                    probe_partitions=recording_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        _, _, ok, evidence = recorded_rows[0]
        self.assertTrue(ok)
        expected_path = Path(tmp_dir) / "model.pt"
        self.assertEqual(
            evidence,
            f"{expected_path}: torch.load succeeded, 2 top-level entries "
            "(legacy .pt/.pth format has no partial-read API, so this is a "
            "known full-file-read limitation of the format, not of this probe)",
        )

    def test_a_legacy_checkpoint_without_a_container_state_reports_one_top_level_entry(
        self,
    ) -> None:
        import torch

        recorded_rows: list[Any] = []

        def recording_probe_partitions(spark_session, partitions, probe):
            rows = _flatten_probe_partitions(spark_session, partitions, probe)
            recorded_rows.extend(rows)
            return rows

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "scalar.pt"
            torch.save(42, str(model_path))
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_model_file_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="scalar.pt"),
                    mount_root=tmp_dir,
                    probe_partitions=recording_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        _, _, ok, evidence = recorded_rows[0]
        self.assertTrue(ok)
        self.assertIn("torch.load succeeded, 1 top-level entries", evidence)

    def test_the_legacy_checkpoint_loader_forwards_the_exact_torch_load_kwargs(
        self,
    ) -> None:
        import torch

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "model.pt"
            torch.save({"weight": torch.ones(2, 2)}, str(model_path))
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                with patch("torch.load", wraps=torch.load) as mock_load:
                    result = probe_model_file_consumer(
                        MagicMock(),
                        _single_executor(),
                        model=ConsumerArtifact(relative_path="model.pt"),
                        mount_root=tmp_dir,
                        probe_partitions=_flatten_probe_partitions,
                    )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        expected_call = call(str(model_path), map_location="cpu", weights_only=True)
        self.assertTrue(mock_load.call_args_list)
        self.assertTrue(
            all(actual_call == expected_call for actual_call in mock_load.call_args_list)
        )

    def test_forwards_the_exact_spark_session_to_run_per_executor(self) -> None:
        import torch
        from safetensors.torch import save_file

        sentinel_session = object()
        recorded_sessions: list[Any] = []

        def recording_probe_partitions(spark_session, partitions, probe):
            recorded_sessions.append(spark_session)
            return _flatten_probe_partitions(spark_session, partitions, probe)

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "model.safetensors"
            save_file({"weight": torch.ones(2, 2)}, str(model_path))
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                probe_model_file_consumer(
                    sentinel_session,
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="model.safetensors"),
                    mount_root=tmp_dir,
                    probe_partitions=recording_probe_partitions,
                )
        self.assertEqual(recorded_sessions, [sentinel_session])

    def test_the_failure_label_identifies_the_model_file_consumer_proof_exactly(
        self,
    ) -> None:
        from safetensors.torch import save_file

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "empty.safetensors"
            save_file({}, str(model_path))
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_model_file_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="empty.safetensors"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertTrue(
            result.evidence.startswith(
                "RuntimeError: model file consumer proof is not reliable "
                "on every executor: "
            )
        )

    def test_reports_the_direct_mount_model_file_consumer_capability_name(
        self,
    ) -> None:
        import torch
        from safetensors.torch import save_file

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "model.safetensors"
            save_file({"weight": torch.ones(2, 2)}, str(model_path))
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_model_file_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="model.safetensors"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.capability, "direct_mount_model_file_consumer")


class ProbeOnnxRuntimeConsumerTests(unittest.TestCase):
    @staticmethod
    def _write_static_shape_model(path: Path) -> None:
        import onnx
        from onnx import TensorProto, helper

        node = helper.make_node("Mul", inputs=["x", "two"], outputs=["y"])
        two = helper.make_tensor("two", TensorProto.FLOAT, [1], [2.0])
        x = helper.make_tensor_value_info("x", TensorProto.FLOAT, [1, 3])
        y = helper.make_tensor_value_info("y", TensorProto.FLOAT, [1, 3])
        graph = helper.make_graph([node], "tiny", [x], [y], initializer=[two])
        model = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 13)])
        model.ir_version = 8
        onnx.checker.check_model(model)
        onnx.save(model, str(path))

    @staticmethod
    def _write_dynamic_shape_model(path: Path) -> None:
        import onnx
        from onnx import TensorProto, helper

        node = helper.make_node("Mul", inputs=["x", "two"], outputs=["y"])
        two = helper.make_tensor("two", TensorProto.FLOAT, [1], [2.0])
        x = helper.make_tensor_value_info("x", TensorProto.FLOAT, ["batch", 3])
        y = helper.make_tensor_value_info("y", TensorProto.FLOAT, ["batch", 3])
        graph = helper.make_graph([node], "tiny_dynamic", [x], [y], initializer=[two])
        model = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 13)])
        model.ir_version = 8
        onnx.checker.check_model(model)
        onnx.save(model, str(path))

    @staticmethod
    def _write_static_shape_model_with_dtype(path: Path, elem_type: int, value: Any) -> None:
        import onnx
        from onnx import helper

        node = helper.make_node("Mul", inputs=["x", "two"], outputs=["y"])
        two = helper.make_tensor("two", elem_type, [1], [value])
        x = helper.make_tensor_value_info("x", elem_type, [1, 3])
        y = helper.make_tensor_value_info("y", elem_type, [1, 3])
        graph = helper.make_graph([node], "tiny_dtype", [x], [y], initializer=[two])
        model = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 13)])
        model.ir_version = 8
        onnx.checker.check_model(model)
        onnx.save(model, str(path))

    def test_reports_available_and_runs_bounded_inference_for_static_shapes(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "models" / "tiny.onnx"
            model_path.parent.mkdir(parents=True)
            self._write_static_shape_model(model_path)
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_onnx_runtime_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="models/tiny.onnx"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertIn("bounded zero-input inference ran", result.evidence)

    def test_reports_available_without_inference_for_dynamic_shapes(self) -> None:
        recorded_rows: list[Any] = []

        def recording_probe_partitions(spark_session, partitions, probe):
            rows = _flatten_probe_partitions(spark_session, partitions, probe)
            recorded_rows.extend(rows)
            return rows

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "dynamic.onnx"
            self._write_dynamic_shape_model(model_path)
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_onnx_runtime_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="dynamic.onnx"),
                    mount_root=tmp_dir,
                    probe_partitions=recording_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        _, _, ok, evidence = recorded_rows[0]
        self.assertTrue(ok)
        self.assertEqual(
            evidence,
            f"{model_path}: session created, 1 input(s), dynamic shape(s) "
            "present so bounded inference was not attempted (cannot be "
            "safely constructed)",
        )

    def test_reports_blocked_when_the_model_file_does_not_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_onnx_runtime_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="missing.onnx"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)

    def test_the_default_mount_root_is_the_default_lakehouse_path(self) -> None:
        recorded_rows: list[Any] = []

        def recording_probe_partitions(spark_session, partitions, probe):
            rows = _flatten_probe_partitions(spark_session, partitions, probe)
            recorded_rows.extend(rows)
            return rows

        with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
            probe_onnx_runtime_consumer(
                MagicMock(),
                _single_executor(),
                model=ConsumerArtifact(relative_path="missing.onnx"),
                probe_partitions=recording_probe_partitions,
            )
        _, _, ok, evidence = recorded_rows[0]
        self.assertFalse(ok)
        self.assertIn("/lakehouse/default/missing.onnx", evidence)

    def test_the_default_oversample_per_executor_count_is_four(self) -> None:
        recorded_partitions: list[int] = []

        def recording_probe_partitions(spark_session, partitions, probe):
            recorded_partitions.append(partitions)
            return _flatten_probe_partitions(spark_session, partitions, probe)

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "tiny.onnx"
            self._write_static_shape_model(model_path)
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                probe_onnx_runtime_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="tiny.onnx"),
                    mount_root=tmp_dir,
                    probe_partitions=recording_probe_partitions,
                )
        self.assertEqual(recorded_partitions, [4])

    def test_falls_back_to_the_hostname_when_spark_executor_id_is_unset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "tiny.onnx"
            self._write_static_shape_model(model_path)
            environ_without_executor_id = {
                key: value
                for key, value in __import__("os").environ.items()
                if key != "SPARK_EXECUTOR_ID"
            }
            with patch.dict(
                "os.environ", environ_without_executor_id, clear=True
            ), patch("socket.gethostname", return_value="1"):
                result = probe_onnx_runtime_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="tiny.onnx"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)

    def test_the_inference_session_is_created_with_the_exact_cpu_provider_list(
        self,
    ) -> None:
        import onnxruntime as ort

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "tiny.onnx"
            self._write_static_shape_model(model_path)
            with patch(
                "onnxruntime.InferenceSession", wraps=ort.InferenceSession
            ) as mock_ctor:
                with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                    result = probe_onnx_runtime_consumer(
                        MagicMock(),
                        _single_executor(),
                        model=ConsumerArtifact(relative_path="tiny.onnx"),
                        mount_root=tmp_dir,
                        probe_partitions=_flatten_probe_partitions,
                    )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertTrue(mock_ctor.call_args_list)
        expected_call = call(str(model_path), providers=["CPUExecutionProvider"])
        self.assertTrue(
            all(actual == expected_call for actual in mock_ctor.call_args_list)
        )

    def test_a_zero_sized_static_dimension_is_treated_as_dynamic_not_static(
        self,
    ) -> None:
        spec = SimpleNamespace(shape=[0, 3], type="tensor(float)", name="x")
        fake_session = MagicMock()
        fake_session.get_inputs.return_value = [spec]
        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "tiny.onnx"
            model_path.write_bytes(b"fake onnx bytes")
            with patch("onnxruntime.InferenceSession", return_value=fake_session):
                with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                    result = probe_onnx_runtime_consumer(
                        MagicMock(),
                        _single_executor(),
                        model=ConsumerArtifact(relative_path="tiny.onnx"),
                        mount_root=tmp_dir,
                        probe_partitions=_flatten_probe_partitions,
                    )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertIn("not attempted", result.evidence)
        fake_session.run.assert_not_called()

    def test_the_bounded_input_dtype_matches_the_declared_onnx_element_type(
        self,
    ) -> None:
        import numpy as np
        from onnx import TensorProto

        cases = [
            (TensorProto.DOUBLE, 2.0, np.float64),
            (TensorProto.INT64, 2, np.int64),
            (TensorProto.INT32, 2, np.int32),
        ]
        for elem_type, scalar, expected_dtype in cases:
            with self.subTest(expected_dtype=expected_dtype):
                with tempfile.TemporaryDirectory() as tmp_dir:
                    model_path = Path(tmp_dir) / "tiny.onnx"
                    self._write_static_shape_model_with_dtype(
                        model_path, elem_type, scalar
                    )
                    with patch("numpy.zeros", wraps=np.zeros) as mock_zeros:
                        with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                            result = probe_onnx_runtime_consumer(
                                MagicMock(),
                                _single_executor(),
                                model=ConsumerArtifact(relative_path="tiny.onnx"),
                                mount_root=tmp_dir,
                                probe_partitions=_flatten_probe_partitions,
                            )
                    self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
                    self.assertIn(
                        "bounded zero-input inference ran", result.evidence
                    )
                    self.assertTrue(mock_zeros.call_args_list)
                    for recorded_call in mock_zeros.call_args_list:
                        self.assertEqual(
                            recorded_call.kwargs["dtype"], expected_dtype
                        )

    def test_an_unmapped_onnx_input_type_still_feeds_the_float32_default_dtype(
        self,
    ) -> None:
        import numpy as np

        spec = SimpleNamespace(shape=[1, 3], type="tensor(bool)", name="x")
        fake_session = MagicMock()
        fake_session.get_inputs.return_value = [spec]
        recorded_feed: dict[str, Any] = {}

        def fake_run(_output_names: Any, feed: Any) -> list[Any]:
            recorded_feed.update(feed)
            return [None]

        fake_session.run.side_effect = fake_run
        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "tiny.onnx"
            model_path.write_bytes(b"fake onnx bytes")
            with patch("onnxruntime.InferenceSession", return_value=fake_session):
                with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                    result = probe_onnx_runtime_consumer(
                        MagicMock(),
                        _single_executor(),
                        model=ConsumerArtifact(relative_path="tiny.onnx"),
                        mount_root=tmp_dir,
                        probe_partitions=_flatten_probe_partitions,
                    )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertEqual(recorded_feed["x"].dtype, np.dtype(np.float32))

    def test_a_failed_session_creation_reports_the_real_exception_type_name(
        self,
    ) -> None:
        recorded_rows: list[Any] = []

        def recording_probe_partitions(spark_session, partitions, probe):
            rows = _flatten_probe_partitions(spark_session, partitions, probe)
            recorded_rows.extend(rows)
            return rows

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                probe_onnx_runtime_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="missing.onnx"),
                    mount_root=tmp_dir,
                    probe_partitions=recording_probe_partitions,
                )
        _, _, ok, evidence = recorded_rows[0]
        self.assertFalse(ok)
        self.assertFalse(evidence.startswith("NoneType:"))
        self.assertNotEqual(evidence.split(":", 1)[0].strip(), "NoneType")

    def test_forwards_the_exact_spark_session_to_run_per_executor(self) -> None:
        sentinel_session = object()
        recorded_sessions: list[Any] = []

        def recording_probe_partitions(spark_session, partitions, probe):
            recorded_sessions.append(spark_session)
            return _flatten_probe_partitions(spark_session, partitions, probe)

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "tiny.onnx"
            self._write_static_shape_model(model_path)
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                probe_onnx_runtime_consumer(
                    sentinel_session,
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="tiny.onnx"),
                    mount_root=tmp_dir,
                    probe_partitions=recording_probe_partitions,
                )
        self.assertEqual(recorded_sessions, [sentinel_session])

    def test_the_failure_label_identifies_the_onnx_runtime_consumer_proof_exactly(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_onnx_runtime_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="missing.onnx"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertTrue(
            result.evidence.startswith(
                "RuntimeError: ONNX Runtime consumer proof is not reliable "
                "on every executor: "
            )
        )

    def test_reports_the_direct_mount_onnx_runtime_consumer_capability_name(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            model_path = Path(tmp_dir) / "tiny.onnx"
            self._write_static_shape_model(model_path)
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_onnx_runtime_consumer(
                    MagicMock(),
                    _single_executor(),
                    model=ConsumerArtifact(relative_path="tiny.onnx"),
                    mount_root=tmp_dir,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.capability, "direct_mount_onnx_runtime_consumer")


class ProbeConcurrentExecutorReadsTests(unittest.TestCase):
    def test_reports_available_when_every_concurrent_reader_agrees(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "a.bin"
            source.write_bytes(b"shared fixed content" * 1000)

            def single_task(spark_session, partitions, probe):
                self.assertEqual(partitions, 1)
                return probe(0)

            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_concurrent_executor_reads(
                    MagicMock(),
                    _single_executor(),
                    target=ConsumerArtifact(relative_path="a.bin"),
                    concurrent_tasks=8,
                    mount_root=tmp_dir,
                    probe_partitions=single_task,
                )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertEqual(result.capability, "direct_mount_concurrent_reads")
        self.assertIn("8 concurrent readers agreed", result.evidence)
        # The exact ``SPARK_EXECUTOR_ID`` value must be used verbatim as the
        # identity, never silently replaced with the raw hostname.
        self.assertEqual(result.value, "1")
        self.assertIn("executor '1'", result.evidence)

    def test_reports_blocked_when_concurrent_readers_disagree(self) -> None:
        call_count = {"n": 0}

        def flaky_stream_sha256(path):
            call_count["n"] += 1
            return "a" if call_count["n"] % 2 else "b"

        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "a.bin"
            source.write_bytes(b"content")

            def single_task(spark_session, partitions, probe):
                return probe(0)

            with patch(
                "people_counter.fabric_capability_probe._stream_sha256",
                side_effect=flaky_stream_sha256,
            ), patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_concurrent_executor_reads(
                    MagicMock(),
                    _single_executor(),
                    target=ConsumerArtifact(relative_path="a.bin"),
                    concurrent_tasks=4,
                    mount_root=tmp_dir,
                    probe_partitions=single_task,
                )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("disagreed on content", result.evidence)

    def test_rejects_a_non_positive_concurrent_task_count(self) -> None:
        result = probe_concurrent_executor_reads(
            MagicMock(),
            _single_executor(),
            target=ConsumerArtifact(relative_path="a.bin"),
            concurrent_tasks=0,
            probe_partitions=lambda spark_session, partitions, probe: [],
        )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertEqual(
            result.evidence, "RuntimeError: concurrent_tasks must be positive"
        )

    def test_a_single_concurrent_task_is_allowed_and_reports_its_own_digest(
        self,
    ) -> None:
        # ``concurrent_tasks=1`` must be accepted (only <= 0 is rejected),
        # and the agreed digest must come from the one and only reading
        # (index 0) -- never an out-of-range index.
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "a.bin"
            source.write_bytes(b"solo content")
            import hashlib

            expected_digest = hashlib.sha256(b"solo content").hexdigest()

            def single_task(spark_session, partitions, probe):
                return probe(0)

            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_concurrent_executor_reads(
                    MagicMock(),
                    _single_executor(),
                    target=ConsumerArtifact(relative_path="a.bin"),
                    concurrent_tasks=1,
                    mount_root=tmp_dir,
                    probe_partitions=single_task,
                )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertIn(f"sha256={expected_digest}", result.evidence)

    def test_an_exception_during_reading_fails_closed_with_its_real_type(
        self,
    ) -> None:
        def single_task(spark_session, partitions, probe):
            return probe(0)

        def raising_stream_sha256(path):
            raise ValueError("simulated real-consumer read failure")

        with patch(
            "people_counter.fabric_capability_probe._stream_sha256",
            side_effect=raising_stream_sha256,
        ), patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
            result = probe_concurrent_executor_reads(
                MagicMock(),
                _single_executor(),
                target=ConsumerArtifact(relative_path="a.bin"),
                concurrent_tasks=2,
                mount_root="/lakehouse/default",
                probe_partitions=single_task,
            )
        # A real exception while reading must never be silently reported as
        # ``ok`` -- it must fail closed, with the real exception type name
        # (never a fabricated/``None``-derived one).
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("ValueError", result.evidence)
        self.assertIn("simulated real-consumer read failure", result.evidence)

    def test_default_mount_root_is_the_fixed_lakehouse_default_path(self) -> None:
        def single_task(spark_session, partitions, probe):
            return probe(0)

        with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
            result = probe_concurrent_executor_reads(
                MagicMock(),
                _single_executor(),
                target=ConsumerArtifact(
                    relative_path="__mutmut_canary_missing__.bin"
                ),
                concurrent_tasks=2,
                probe_partitions=single_task,
            )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn(
            "/lakehouse/default/__mutmut_canary_missing__.bin", result.evidence
        )

    def test_uses_exactly_concurrent_tasks_as_the_thread_pool_size(self) -> None:
        from concurrent.futures import ThreadPoolExecutor

        recorded: dict[str, Any] = {}

        class RecordingThreadPoolExecutor(ThreadPoolExecutor):
            def __init__(self, *args: object, **kwargs: object) -> None:
                recorded["max_workers"] = kwargs.get(
                    "max_workers", args[0] if args else None
                )
                super().__init__(*args, **kwargs)

        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "a.bin"
            source.write_bytes(b"content")

            def single_task(spark_session, partitions, probe):
                return probe(0)

            with patch(
                "people_counter.fabric_capability_probe.ThreadPoolExecutor",
                RecordingThreadPoolExecutor,
            ), patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                probe_concurrent_executor_reads(
                    MagicMock(),
                    _single_executor(),
                    target=ConsumerArtifact(relative_path="a.bin"),
                    concurrent_tasks=7,
                    mount_root=tmp_dir,
                    probe_partitions=single_task,
                )
        self.assertEqual(recorded["max_workers"], 7)

    def test_forwards_the_real_spark_session_to_probe_partitions(self) -> None:
        recorded: dict[str, Any] = {}

        def recording_probe_partitions(spark_session, partitions, probe):
            recorded["session"] = spark_session
            return probe(0)

        sentinel = MagicMock()
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "a.bin"
            source.write_bytes(b"content")
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                probe_concurrent_executor_reads(
                    sentinel,
                    _single_executor(),
                    target=ConsumerArtifact(relative_path="a.bin"),
                    concurrent_tasks=2,
                    mount_root=tmp_dir,
                    probe_partitions=recording_probe_partitions,
                )
        self.assertIs(recorded["session"], sentinel)

    def test_reports_blocked_when_no_reading_is_observed(self) -> None:
        result = probe_concurrent_executor_reads(
            MagicMock(),
            _single_executor(),
            target=ConsumerArtifact(relative_path="a.bin"),
            concurrent_tasks=2,
            probe_partitions=lambda spark_session, partitions, probe: [],
        )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertEqual(
            result.evidence,
            "RuntimeError: no reading observed for the concurrent-read probe",
        )

    def test_falls_back_to_hostname_as_executor_id_when_unset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "a.bin"
            source.write_bytes(b"content")

            def single_task(spark_session, partitions, probe):
                return probe(0)

            with patch("socket.gethostname", return_value="fake-host-concurrent"):
                import os as _os

                previous = _os.environ.pop("SPARK_EXECUTOR_ID", None)
                try:
                    result = probe_concurrent_executor_reads(
                        MagicMock(),
                        _single_executor(),
                        target=ConsumerArtifact(relative_path="a.bin"),
                        concurrent_tasks=2,
                        mount_root=tmp_dir,
                        probe_partitions=single_task,
                    )
                finally:
                    if previous is not None:
                        _os.environ["SPARK_EXECUTOR_ID"] = previous
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertEqual(result.value, "fake-host-concurrent")



class ProbeDirectMountConsumerCapabilityTests(unittest.TestCase):
    def test_fails_closed_when_the_posix_prerequisite_fails(self) -> None:
        failing_posix = MagicMock(
            return_value=CapabilityProbeResult(
                capability="direct_mounted_lakehouse_path",
                status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
                evidence="simulated POSIX mount failure",
            )
        )
        result = probe_direct_mount_consumer_capability(
            MagicMock(),
            _single_executor(),
            profile=ConsumerProbeProfile(),
            posix_probe=failing_posix,
        )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("POSIX mount prerequisite failed", result.evidence)

    def test_an_empty_profile_passes_on_the_posix_prerequisite_alone(self) -> None:
        passing_posix = MagicMock(
            return_value=CapabilityProbeResult(
                capability="direct_mounted_lakehouse_path",
                status=CapabilityStatus.AVAILABLE,
                evidence="simulated POSIX mount success",
                value="/lakehouse/default",
            )
        )
        result = probe_direct_mount_consumer_capability(
            MagicMock(),
            _single_executor(),
            profile=ConsumerProbeProfile(),
            posix_probe=passing_posix,
        )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        report = result.value
        self.assertIsInstance(report, ConsumerCapabilityReport)
        self.assertEqual(report.results, (report.posix,))
        self.assertTrue(report.all_available)

    def test_reports_the_direct_mount_consumer_capability_name(self) -> None:
        passing_posix = MagicMock(
            return_value=CapabilityProbeResult(
                capability="direct_mounted_lakehouse_path",
                status=CapabilityStatus.AVAILABLE,
                evidence="simulated POSIX mount success",
                value="/lakehouse/default",
            )
        )
        result = probe_direct_mount_consumer_capability(
            MagicMock(),
            _single_executor(),
            profile=ConsumerProbeProfile(),
            posix_probe=passing_posix,
        )
        self.assertEqual(result.capability, "direct_mount_consumer_capability")

    def test_forwards_the_exact_session_executors_mount_root_and_partitioner_to_the_posix_prerequisite(
        self,
    ) -> None:
        recorded: dict[str, Any] = {}

        def recording_posix(
            session: Any,
            execs: Any,
            *,
            mount_root: str,
            probe_partitions: Any,
        ) -> CapabilityProbeResult:
            recorded["session"] = session
            recorded["executors"] = execs
            recorded["mount_root"] = mount_root
            recorded["probe_partitions"] = probe_partitions
            return CapabilityProbeResult(
                capability="direct_mounted_lakehouse_path",
                status=CapabilityStatus.AVAILABLE,
                evidence="simulated POSIX mount success",
                value=mount_root,
            )

        sentinel_session = object()
        sentinel_executors = _single_executor()
        sentinel_probe_partitions = _flatten_probe_partitions
        result = probe_direct_mount_consumer_capability(
            sentinel_session,
            sentinel_executors,
            profile=ConsumerProbeProfile(),
            mount_root="/custom/mount/root",
            posix_probe=recording_posix,
            probe_partitions=sentinel_probe_partitions,
        )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        self.assertIs(recorded["session"], sentinel_session)
        self.assertIs(recorded["executors"], sentinel_executors)
        self.assertEqual(recorded["mount_root"], "/custom/mount/root")
        self.assertIs(recorded["probe_partitions"], sentinel_probe_partitions)

    def test_the_default_mount_root_is_the_default_lakehouse_path(self) -> None:
        recorded: dict[str, Any] = {}

        def recording_posix(
            session: Any,
            execs: Any,
            *,
            mount_root: str,
            probe_partitions: Any,
        ) -> CapabilityProbeResult:
            recorded["mount_root"] = mount_root
            return CapabilityProbeResult(
                capability="direct_mounted_lakehouse_path",
                status=CapabilityStatus.AVAILABLE,
                evidence="simulated POSIX mount success",
                value=mount_root,
            )

        probe_direct_mount_consumer_capability(
            MagicMock(),
            _single_executor(),
            profile=ConsumerProbeProfile(),
            posix_probe=recording_posix,
        )
        self.assertEqual(recorded["mount_root"], "/lakehouse/default")

    def test_fails_closed_when_one_consumer_proof_in_the_profile_fails(self) -> None:
        passing_posix = MagicMock(
            return_value=CapabilityProbeResult(
                capability="direct_mounted_lakehouse_path",
                status=CapabilityStatus.AVAILABLE,
                evidence="simulated POSIX mount success",
                value="/lakehouse/default",
            )
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            profile = ConsumerProbeProfile(
                video=ConsumerArtifact(relative_path="missing.mp4")
            )
            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_direct_mount_consumer_capability(
                    MagicMock(),
                    _single_executor(),
                    profile=profile,
                    mount_root=tmp_dir,
                    posix_probe=passing_posix,
                    probe_partitions=_flatten_probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertIn("OpenCV video consumer proof failed", result.evidence)

    def test_fails_closed_with_the_exact_message_when_the_stream_hash_proof_fails(
        self,
    ) -> None:
        passing_posix = MagicMock(
            return_value=CapabilityProbeResult(
                capability="direct_mounted_lakehouse_path",
                status=CapabilityStatus.AVAILABLE,
                evidence="simulated POSIX mount success",
                value="/lakehouse/default",
            )
        )
        failing_stream_hash = CapabilityProbeResult(
            capability="stream_hash_artifacts",
            status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="simulated stream-hash failure",
        )
        profile = ConsumerProbeProfile(
            hash_targets=(ConsumerArtifact(relative_path="source.bin"),)
        )
        with patch(
            "people_counter.fabric_capability_probe.probe_stream_hash_artifacts",
            return_value=failing_stream_hash,
        ):
            result = probe_direct_mount_consumer_capability(
                MagicMock(),
                _single_executor(),
                profile=profile,
                posix_probe=passing_posix,
            )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertEqual(
            result.evidence,
            "RuntimeError: stream-hash proof failed: simulated stream-hash failure",
        )

    def test_fails_closed_with_the_exact_message_when_the_model_file_proof_fails(
        self,
    ) -> None:
        passing_posix = MagicMock(
            return_value=CapabilityProbeResult(
                capability="direct_mounted_lakehouse_path",
                status=CapabilityStatus.AVAILABLE,
                evidence="simulated POSIX mount success",
                value="/lakehouse/default",
            )
        )
        failing_model_file = CapabilityProbeResult(
            capability="model_file_consumer",
            status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="simulated model-file failure",
        )
        profile = ConsumerProbeProfile(
            safetensors_or_pytorch_model=ConsumerArtifact(
                relative_path="model.safetensors"
            )
        )
        with patch(
            "people_counter.fabric_capability_probe.probe_model_file_consumer",
            return_value=failing_model_file,
        ):
            result = probe_direct_mount_consumer_capability(
                MagicMock(),
                _single_executor(),
                profile=profile,
                posix_probe=passing_posix,
            )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertEqual(
            result.evidence,
            "RuntimeError: model file consumer proof failed: simulated model-file failure",
        )

    def test_fails_closed_with_the_exact_message_when_the_onnx_proof_fails(
        self,
    ) -> None:
        passing_posix = MagicMock(
            return_value=CapabilityProbeResult(
                capability="direct_mounted_lakehouse_path",
                status=CapabilityStatus.AVAILABLE,
                evidence="simulated POSIX mount success",
                value="/lakehouse/default",
            )
        )
        failing_onnx = CapabilityProbeResult(
            capability="onnx_runtime_consumer",
            status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="simulated ONNX failure",
        )
        profile = ConsumerProbeProfile(
            onnx_model=ConsumerArtifact(relative_path="tiny.onnx")
        )
        with patch(
            "people_counter.fabric_capability_probe.probe_onnx_runtime_consumer",
            return_value=failing_onnx,
        ):
            result = probe_direct_mount_consumer_capability(
                MagicMock(),
                _single_executor(),
                profile=profile,
                posix_probe=passing_posix,
            )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertEqual(
            result.evidence,
            "RuntimeError: ONNX Runtime consumer proof failed: simulated ONNX failure",
        )

    def test_fails_closed_with_the_exact_message_when_the_concurrent_reads_proof_fails(
        self,
    ) -> None:
        passing_posix = MagicMock(
            return_value=CapabilityProbeResult(
                capability="direct_mounted_lakehouse_path",
                status=CapabilityStatus.AVAILABLE,
                evidence="simulated POSIX mount success",
                value="/lakehouse/default",
            )
        )
        failing_concurrent_reads = CapabilityProbeResult(
            capability="concurrent_executor_reads",
            status=CapabilityStatus.FABRIC_PLATFORM_BLOCKED,
            evidence="simulated concurrent-read failure",
        )
        profile = ConsumerProbeProfile(
            concurrent_read_target=ConsumerArtifact(relative_path="source.bin"),
            planned_concurrent_tasks=3,
        )
        with patch(
            "people_counter.fabric_capability_probe.probe_concurrent_executor_reads",
            return_value=failing_concurrent_reads,
        ):
            result = probe_direct_mount_consumer_capability(
                MagicMock(),
                _single_executor(),
                profile=profile,
                posix_probe=passing_posix,
            )
        self.assertEqual(result.status, CapabilityStatus.FABRIC_PLATFORM_BLOCKED)
        self.assertEqual(
            result.evidence,
            "RuntimeError: concurrent-read proof failed: simulated concurrent-read failure",
        )

    def test_reports_available_when_every_configured_consumer_proof_passes(
        self,
    ) -> None:
        import torch
        from safetensors.torch import save_file

        passing_posix = MagicMock(
            return_value=CapabilityProbeResult(
                capability="direct_mounted_lakehouse_path",
                status=CapabilityStatus.AVAILABLE,
                evidence="simulated POSIX mount success",
                value="/lakehouse/default",
            )
        )
        sentinel_session = object()
        recorded_sessions: list[Any] = []
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "source.bin"
            source.write_bytes(b"fixed immutable content")
            video_path = Path(tmp_dir) / "sample.mp4"
            ProbeOpenCvVideoConsumerTests._write_sample_video(video_path)
            model_path = Path(tmp_dir) / "model.safetensors"
            save_file({"weight": torch.ones(2, 2)}, str(model_path))
            onnx_path = Path(tmp_dir) / "tiny.onnx"
            ProbeOnnxRuntimeConsumerTests._write_static_shape_model(onnx_path)

            profile = ConsumerProbeProfile(
                hash_targets=(ConsumerArtifact(relative_path="source.bin"),),
                video=ConsumerArtifact(relative_path="sample.mp4"),
                safetensors_or_pytorch_model=ConsumerArtifact(
                    relative_path="model.safetensors"
                ),
                onnx_model=ConsumerArtifact(relative_path="tiny.onnx"),
                concurrent_read_target=ConsumerArtifact(relative_path="source.bin"),
                planned_concurrent_tasks=3,
            )

            def probe_partitions(spark_session, partitions, probe):
                recorded_sessions.append(spark_session)
                if partitions == 1:
                    return probe(0)
                return _flatten_probe_partitions(spark_session, partitions, probe)

            with patch.dict("os.environ", {"SPARK_EXECUTOR_ID": "1"}):
                result = probe_direct_mount_consumer_capability(
                    sentinel_session,
                    _single_executor(),
                    profile=profile,
                    mount_root=tmp_dir,
                    posix_probe=passing_posix,
                    probe_partitions=probe_partitions,
                )
        self.assertEqual(result.status, CapabilityStatus.AVAILABLE)
        report = result.value
        self.assertTrue(report.all_available)
        self.assertEqual(len(report.results), 6)
        self.assertIsNotNone(report.stream_hash)
        self.assertIsNotNone(report.video)
        self.assertIsNotNone(report.model_file)
        self.assertIsNotNone(report.onnx)
        self.assertIsNotNone(report.concurrent_reads)
        # Every sub-probe (stream-hash, video, model-file, onnx, concurrent
        # reads) must receive the *exact* outer ``spark_session`` -- not
        # ``None`` nor a substitute -- proving the composition forwards it
        # rather than silently dropping it for any one consumer proof.
        self.assertEqual(len(recorded_sessions), 5)
        self.assertTrue(all(s is sentinel_session for s in recorded_sessions))
        self.assertEqual(
            result.evidence,
            "all 6 consumer proof(s) passed: "
            "direct_mounted_lakehouse_path=PASS; "
            "direct_mount_stream_hash=PASS; "
            "direct_mount_opencv_video_consumer=PASS; "
            "direct_mount_model_file_consumer=PASS; "
            "direct_mount_onnx_runtime_consumer=PASS; "
            "direct_mount_concurrent_reads=PASS",
        )


class RedactedConsumerCapabilityEvidenceTests(unittest.TestCase):
    def test_builds_a_redacted_payload_from_a_report(self) -> None:
        posix = CapabilityProbeResult(
            capability="direct_mounted_lakehouse_path",
            status=CapabilityStatus.AVAILABLE,
            evidence="posix ok",
            value="/lakehouse/default",
        )
        video = CapabilityProbeResult(
            capability="direct_mount_opencv_video_consumer",
            status=CapabilityStatus.AVAILABLE,
            evidence="video ok",
            value="sample.mp4",
        )
        report = ConsumerCapabilityReport(
            posix=posix,
            stream_hash=None,
            video=video,
            model_file=None,
            onnx=None,
            concurrent_reads=None,
        )
        payload = redacted_consumer_capability_evidence(
            report,
            backend="FABRIC_DIRECT",
            library_versions={"opencv-python": "5.0.0"},
        )
        self.assertEqual(payload["backend"], "FABRIC_DIRECT")
        self.assertEqual(payload["library_versions"], {"opencv-python": "5.0.0"})
        self.assertTrue(payload["all_available"])
        self.assertEqual(
            [entry["capability"] for entry in payload["results"]],
            ["direct_mounted_lakehouse_path", "direct_mount_opencv_video_consumer"],
        )
        self.assertEqual(
            [entry["status"] for entry in payload["results"]],
            [CapabilityStatus.AVAILABLE.value, CapabilityStatus.AVAILABLE.value],
        )
        self.assertEqual(
            [entry["evidence"] for entry in payload["results"]],
            ["posix ok", "video ok"],
        )
        # No raw artifact bytes, env vars, or anything beyond the fixed
        # evidence/value strings each probe already recorded.
        serialized = str(payload)
        self.assertNotIn("SPARK_EXECUTOR_ID", serialized)


class _FakeOneLakeFiles:
    """Minimal in-memory double for the create-only OneLakeFiles protocol."""

    def __init__(self) -> None:
        self.written: dict[str, str] = {}

    def exists(self, path: str) -> bool:
        return path in self.written

    def read_text(self, path: str) -> str:
        return self.written[path]

    def create_text(self, path: str, content: str) -> None:
        if path in self.written:
            raise FileExistsError(path)
        self.written[path] = content


class PersistConsumerCapabilityEvidenceTests(unittest.TestCase):
    @staticmethod
    def _report() -> ConsumerCapabilityReport:
        posix = CapabilityProbeResult(
            capability="direct_mounted_lakehouse_path",
            status=CapabilityStatus.AVAILABLE,
            evidence="posix ok",
            value="/lakehouse/default",
        )
        return ConsumerCapabilityReport(
            posix=posix,
            stream_hash=None,
            video=None,
            model_file=None,
            onnx=None,
            concurrent_reads=None,
        )

    def test_creates_and_verifies_a_new_evidence_file(self) -> None:
        files = _FakeOneLakeFiles()
        path = persist_consumer_capability_evidence(
            files,
            "Files/_benchmark/people-counter/candidate-a/v1/capability_probe/batch-1.json",
            self._report(),
            backend="FABRIC_DIRECT",
            library_versions={"opencv-python": "5.0.0"},
        )
        self.assertIn(path, files.written)
        payload = json.loads(files.written[path])
        self.assertEqual(payload["backend"], "FABRIC_DIRECT")
        self.assertTrue(payload["all_available"])

    def test_an_identical_retry_at_the_same_path_is_tolerated(self) -> None:
        files = _FakeOneLakeFiles()
        path = "Files/_benchmark/.../batch-1.json"
        persist_consumer_capability_evidence(
            files, path, self._report(), backend="FABRIC_DIRECT", library_versions={}
        )
        # Re-persisting the identical result at the same path must not
        # raise -- an idempotent retry, not a conflict.
        persist_consumer_capability_evidence(
            files, path, self._report(), backend="FABRIC_DIRECT", library_versions={}
        )

    def test_a_conflicting_rewrite_at_the_same_path_fails_closed(self) -> None:
        files = _FakeOneLakeFiles()
        path = "Files/_benchmark/.../batch-1.json"
        persist_consumer_capability_evidence(
            files, path, self._report(), backend="FABRIC_DIRECT", library_versions={}
        )
        with self.assertRaises(CapabilityProbeError) as ctx:
            persist_consumer_capability_evidence(
                files,
                path,
                self._report(),
                backend="FABRIC_FALLBACK",
                library_versions={},
            )
        self.assertEqual(
            str(ctx.exception),
            f"consumer capability evidence at {path!r} conflicts with "
            "a previously persisted (different) result",
        )

    def test_a_failed_readback_verification_fails_closed(self) -> None:
        files = _FakeOneLakeFiles()
        # Simulate a storage layer that silently truncates/corrupts on
        # create -- readback must catch this, not trust the write blindly.
        files.create_text = lambda path, content: files.written.__setitem__(
            path, "corrupted"
        )
        with self.assertRaises(CapabilityProbeError) as ctx:
            persist_consumer_capability_evidence(
                files,
                "path.json",
                self._report(),
                backend="FABRIC_DIRECT",
                library_versions={},
            )
        self.assertEqual(
            str(ctx.exception),
            "consumer capability evidence readback at 'path.json' did not "
            "match what was written",
        )



if __name__ == "__main__":
    unittest.main()
