import os
import sys
import unittest
from unittest.mock import MagicMock, patch

from people_counter.cpu_runtime import (
    ThreadBudget,
    ThreadOversubscriptionError,
    calculate_placement_safe_thread_budget,
    calculate_thread_budget,
    configure_cpu_runtime,
    read_effective_thread_settings,
    verify_effective_thread_settings,
)


class CpuRuntimeTests(unittest.TestCase):
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
            "people_counter.cpu_runtime",
            level="WARNING",
        ):
            balanced = calculate_thread_budget(4, 4)
        self.assertEqual(balanced.driver_cores, 4)
        self.assertEqual(balanced.active_workers, 4)
        self.assertEqual(balanced.threads_per_worker, 1)
        self.assertIsNone(balanced.interop_threads_configured)

        with self.assertLogs(
            "people_counter.cpu_runtime",
            level="WARNING",
        ) as logs:
            oversubscribed = calculate_thread_budget(2, 3)
        self.assertEqual(oversubscribed.driver_cores, 2)
        self.assertEqual(oversubscribed.active_workers, 3)
        self.assertEqual(oversubscribed.threads_per_worker, 1)
        self.assertEqual(
            logs.output,
            [
                "WARNING:people_counter.cpu_runtime:"
                "active_workers=3 exceeds driver_cores=2; assigning one "
                "thread per worker"
            ],
        )

    def test_placement_safe_budget_uses_worst_case_executor_placement(self):
        budget = calculate_placement_safe_thread_budget([16], 1, 3)
        self.assertEqual(budget.driver_cores, 16)
        self.assertEqual(budget.active_workers, 3)
        self.assertEqual(budget.threads_per_worker, 5)

        heterogeneous = calculate_placement_safe_thread_budget([16, 8], 1, 3)
        self.assertEqual(heterogeneous.driver_cores, 8)
        self.assertEqual(heterogeneous.active_workers, 3)
        self.assertEqual(heterogeneous.threads_per_worker, 2)

        fragmented = calculate_placement_safe_thread_budget([5, 8], 3, 10)
        self.assertEqual(fragmented.driver_cores, 8)
        self.assertEqual(fragmented.active_workers, 2)
        self.assertEqual(fragmented.threads_per_worker, 4)

        single_core = calculate_placement_safe_thread_budget([1], 1, 1)
        self.assertEqual(single_core.driver_cores, 1)
        self.assertEqual(single_core.active_workers, 1)
        self.assertEqual(single_core.threads_per_worker, 1)

        runnable = calculate_placement_safe_thread_budget([2, 8], 3, 2)
        self.assertEqual(runnable.driver_cores, 8)
        self.assertEqual(runnable.active_workers, 2)
        self.assertEqual(runnable.threads_per_worker, 4)

    def test_placement_safe_budget_rejects_invalid_or_unrunnable_resources(self):
        for args, message in (
            (([], 1, 1), "executor_cores contain no runnable executor"),
            (([0], 1, 1), "executor_cores must contain positive integers"),
            (([4.0], 1, 1), "executor_cores must contain positive integers"),
            (([4], 0, 1), "task_cpus must be a positive integer"),
            (([4], 1, 0), "planned_concurrency must be a positive integer"),
            (([2], 3, 1), "executor_cores contain no runnable executor"),
        ):
            with self.subTest(args=args):
                with self.assertRaises(ValueError) as raised:
                    calculate_placement_safe_thread_budget(*args)
                self.assertEqual(str(raised.exception), message)

    def test_configure_cpu_runtime_sets_environment_budget(self):
        environ = {}

        with (
            patch.dict(sys.modules, {"cv2": None, "torch": None}),
            self.assertLogs(
                "people_counter.cpu_runtime",
                level="INFO",
            ) as logs,
        ):
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
                "INFO:people_counter.cpu_runtime:"
                "Configured CPU worker: driver_cores=4 "
                "active_workers=3 threads_per_worker=1 "
                "interop_threads_configured=None"
            ],
        )

    def test_configure_cpu_runtime_can_update_process_environment(self):
        with (
            patch.dict(os.environ, {}, clear=True),
            patch.dict(sys.modules, {"cv2": None, "torch": None}),
        ):
            budget = configure_cpu_runtime(
                1,
                1,
                apply_native_limits=False,
            )

            self.assertEqual(budget.threads_per_worker, 1)
            self.assertEqual(os.environ["OMP_NUM_THREADS"], "1")

    def test_configure_cpu_runtime_overrides_existing_environment(self):
        environ = {"OMP_NUM_THREADS": "4"}

        with (
            patch.dict(sys.modules, {"cv2": None, "torch": None}),
            self.assertLogs("people_counter.cpu_runtime", level="WARNING") as logs,
        ):
            budget = configure_cpu_runtime(
                4,
                2,
                environment=environ,
                apply_native_limits=False,
            )

        self.assertEqual(budget.threads_per_worker, 2)
        self.assertEqual(environ["OMP_NUM_THREADS"], "2")
        self.assertIn(
            "WARNING:people_counter.cpu_runtime:"
            "Overriding OMP_NUM_THREADS=4 with 2 for the configured CPU budget",
            logs.output,
        )

    def test_configure_cpu_runtime_applies_native_limits_once_per_process(self):
        environ = {}
        cv2 = MagicMock()
        torch = MagicMock()

        with (
            patch(
                "people_counter.cpu_runtime._NATIVE_THREAD_BUDGET",
                None,
            ),
            patch.dict(sys.modules, {"cv2": cv2, "torch": torch}),
            self.assertLogs("people_counter.cpu_runtime", level="INFO") as logs,
        ):
            first = configure_cpu_runtime(4, 2, environment=environ)
            second = configure_cpu_runtime(4, 2, environment=environ)
            equivalent = configure_cpu_runtime(6, 3, environment=environ)
            with self.assertRaisesRegex(
                ValueError,
                "Native CPU runtime is already configured",
            ) as raised:
                configure_cpu_runtime(6, 2, environment=environ)

        self.assertEqual(first, second)
        self.assertEqual(first, equivalent)
        self.assertEqual(
            str(raised.exception),
            "Native CPU runtime is already configured with "
            "ThreadBudget(driver_cores=4, active_workers=2, "
            "threads_per_worker=2, interop_threads_configured=True); requested "
            "ThreadBudget(driver_cores=6, active_workers=2, "
            "threads_per_worker=3, interop_threads_configured=None)",
        )
        torch.set_num_threads.assert_called_once_with(2)
        torch.set_num_interop_threads.assert_called_once_with(1)
        cv2.setNumThreads.assert_called_once_with(2)
        self.assertEqual(
            logs.output,
            [
                "INFO:people_counter.cpu_runtime:"
                "Configured CPU worker: driver_cores=4 active_workers=2 "
                "threads_per_worker=2 interop_threads_configured=True",
                "INFO:people_counter.cpu_runtime:"
                "Configured CPU worker: driver_cores=4 active_workers=2 "
                "threads_per_worker=2 interop_threads_configured=True",
                "INFO:people_counter.cpu_runtime:"
                "Configured CPU worker: driver_cores=4 active_workers=2 "
                "threads_per_worker=2 interop_threads_configured=True",
            ],
        )

    def test_configure_cpu_runtime_reuses_equivalent_native_thread_limit(self):
        for existing, requested in (
            (calculate_thread_budget(4, 2), (5, 2)),
            (calculate_thread_budget(4, 3), (4, 4)),
        ):
            with self.subTest(existing=existing, requested=requested):
                environ = {"OMP_NUM_THREADS": "2"}
                with patch(
                    "people_counter.cpu_runtime._NATIVE_THREAD_BUDGET",
                    existing,
                ):
                    result = configure_cpu_runtime(
                        *requested, environment=environ
                    )
                self.assertEqual(result, existing)

                self.assertEqual(
                    set(environ.values()),
                    {str(existing.threads_per_worker)},
                )

    def test_configure_cpu_runtime_rejects_different_native_thread_limit(self):
        existing = calculate_thread_budget(4, 2)
        environ = {"OMP_NUM_THREADS": "2"}
        with (
            patch(
                "people_counter.cpu_runtime._NATIVE_THREAD_BUDGET",
                existing,
            ),
            self.assertRaisesRegex(
                ValueError,
                "Native CPU runtime is already configured",
            ),
        ):
            configure_cpu_runtime(6, 2, environment=environ)
        self.assertEqual(environ, {"OMP_NUM_THREADS": "2"})

    def test_configure_cpu_runtime_tolerates_initialized_interop_threads(self):
        environ = {}
        cv2 = MagicMock()
        torch = MagicMock()
        torch.set_num_interop_threads.side_effect = RuntimeError("already initialized")
        torch.get_num_interop_threads.return_value = 4

        with (
            patch("people_counter.cpu_runtime._NATIVE_THREAD_BUDGET", None),
            patch.dict(sys.modules, {"cv2": cv2, "torch": torch}),
            self.assertLogs("people_counter.cpu_runtime", level="WARNING") as logs,
        ):
            budget = configure_cpu_runtime(4, 1, environment=environ)

        self.assertEqual(budget.threads_per_worker, 4)
        self.assertFalse(budget.interop_threads_configured)
        self.assertEqual(
            logs.output,
            [
                "WARNING:people_counter.cpu_runtime:"
                "Could not change PyTorch inter-op threads after runtime initialization: "
                "already initialized"
            ],
        )
        torch.set_num_threads.assert_called_once_with(4)
        torch.get_num_interop_threads.assert_called_once_with()
        cv2.setNumThreads.assert_called_once_with(4)

    def test_configure_cpu_runtime_recognizes_existing_interop_limit(self):
        cv2 = MagicMock()
        torch = MagicMock()
        torch.set_num_interop_threads.side_effect = RuntimeError("already initialized")
        torch.get_num_interop_threads.return_value = 1

        with (
            patch("people_counter.cpu_runtime._NATIVE_THREAD_BUDGET", None),
            patch.dict(sys.modules, {"cv2": cv2, "torch": torch}),
            self.assertLogs("people_counter.cpu_runtime", level="WARNING"),
        ):
            budget = configure_cpu_runtime(4, 2, environment={})

        self.assertTrue(budget.interop_threads_configured)


class EffectiveThreadReadbackTests(unittest.TestCase):
    def _mock_native_modules(self, torch_threads=2, torch_interop=1, cv2_threads=2):
        cv2 = MagicMock()
        cv2.getNumThreads.return_value = cv2_threads
        torch = MagicMock()
        torch.get_num_threads.return_value = torch_threads
        torch.get_num_interop_threads.return_value = torch_interop
        return cv2, torch

    def test_reads_back_env_torch_opencv_and_onnx_settings(self):
        cv2, torch = self._mock_native_modules(torch_threads=2, cv2_threads=2)
        environ = {
            "OMP_NUM_THREADS": "2",
            "MKL_NUM_THREADS": "2",
            "PC_ONNX_INTRA_OP_THREADS": "2",
            "PC_ONNX_INTER_OP_THREADS": "1",
        }
        with patch.dict(sys.modules, {"cv2": cv2, "torch": torch}):
            settings = read_effective_thread_settings(environment=environ)

        self.assertEqual(settings.torch_num_threads, 2)
        self.assertEqual(settings.torch_num_interop_threads, 1)
        self.assertEqual(settings.opencv_num_threads, 2)
        self.assertEqual(settings.onnx_intra_op_threads, 2)
        self.assertEqual(settings.onnx_inter_op_threads, 1)
        self.assertEqual(
            settings.environment_as_dict()["OMP_NUM_THREADS"],
            "2",
        )

    def test_verify_passes_when_within_budget(self):
        cv2, torch = self._mock_native_modules(torch_threads=2, cv2_threads=2)
        budget = ThreadBudget(driver_cores=4, active_workers=2, threads_per_worker=2)
        environ = {"OMP_NUM_THREADS": "2"}
        with patch.dict(sys.modules, {"cv2": cv2, "torch": torch}):
            settings = verify_effective_thread_settings(budget, environment=environ)
        self.assertEqual(settings.torch_num_threads, 2)

    def test_verify_fails_closed_on_torch_oversubscription(self):
        cv2, torch = self._mock_native_modules(torch_threads=4, cv2_threads=1)
        budget = ThreadBudget(driver_cores=4, active_workers=2, threads_per_worker=2)
        with patch.dict(sys.modules, {"cv2": cv2, "torch": torch}):
            with self.assertRaises(ThreadOversubscriptionError) as raised:
                verify_effective_thread_settings(budget, environment={})
        self.assertIn("torch_num_threads=4 > 2", str(raised.exception))

    def test_verify_fails_closed_on_interop_oversubscription(self):
        cv2, torch = self._mock_native_modules(
            torch_threads=1, torch_interop=4, cv2_threads=1
        )
        budget = ThreadBudget(driver_cores=4, active_workers=2, threads_per_worker=2)
        with patch.dict(sys.modules, {"cv2": cv2, "torch": torch}):
            with self.assertRaises(ThreadOversubscriptionError) as raised:
                verify_effective_thread_settings(budget, environment={})
        self.assertIn("torch_num_interop_threads=4 > 1", str(raised.exception))

    def test_verify_fails_closed_on_interop_oversubscription_at_the_boundary_of_two(
        self,
    ):
        """The interop budget is hard-fixed at > 1 regardless of
        threads_per_worker; exactly 2 must already be rejected."""
        cv2, torch = self._mock_native_modules(
            torch_threads=1, torch_interop=2, cv2_threads=1
        )
        budget = ThreadBudget(driver_cores=4, active_workers=2, threads_per_worker=2)
        with patch.dict(sys.modules, {"cv2": cv2, "torch": torch}):
            with self.assertRaises(ThreadOversubscriptionError) as raised:
                verify_effective_thread_settings(budget, environment={})
        self.assertIn("torch_num_interop_threads=2 > 1", str(raised.exception))

    def test_verify_fails_closed_on_opencv_oversubscription(self):
        cv2, torch = self._mock_native_modules(torch_threads=1, cv2_threads=8)
        budget = ThreadBudget(driver_cores=4, active_workers=2, threads_per_worker=2)
        with patch.dict(sys.modules, {"cv2": cv2, "torch": torch}):
            with self.assertRaises(ThreadOversubscriptionError) as raised:
                verify_effective_thread_settings(budget, environment={})
        self.assertIn("opencv_num_threads=8 > 2", str(raised.exception))

    def test_verify_fails_closed_on_env_variable_oversubscription(self):
        cv2, torch = self._mock_native_modules(torch_threads=1, cv2_threads=1)
        budget = ThreadBudget(driver_cores=4, active_workers=2, threads_per_worker=2)
        environ = {"OMP_NUM_THREADS": "8"}
        with patch.dict(sys.modules, {"cv2": cv2, "torch": torch}):
            with self.assertRaises(ThreadOversubscriptionError) as raised:
                verify_effective_thread_settings(budget, environment=environ)
        self.assertIn("OMP_NUM_THREADS=8 > 2", str(raised.exception))

    def test_verify_fails_closed_on_onnx_oversubscription(self):
        cv2, torch = self._mock_native_modules(torch_threads=1, cv2_threads=1)
        budget = ThreadBudget(driver_cores=4, active_workers=2, threads_per_worker=2)
        environ = {"PC_ONNX_INTRA_OP_THREADS": "6"}
        with patch.dict(sys.modules, {"cv2": cv2, "torch": torch}):
            with self.assertRaises(ThreadOversubscriptionError) as raised:
                verify_effective_thread_settings(budget, environment=environ)
        self.assertIn("onnx_intra_op_threads=6 > 2", str(raised.exception))

    def test_verify_joins_multiple_offenders_with_the_exact_separator(self):
        """With two or more offenders, the join separator itself must be
        exactly '; ' (not, for example, an 'XX'-wrapped variant), which is
        only observable when at least two offenders are present."""
        cv2, torch = self._mock_native_modules(
            torch_threads=4, torch_interop=1, cv2_threads=8
        )
        budget = ThreadBudget(driver_cores=4, active_workers=2, threads_per_worker=2)
        with patch.dict(sys.modules, {"cv2": cv2, "torch": torch}):
            with self.assertRaises(ThreadOversubscriptionError) as raised:
                verify_effective_thread_settings(budget, environment={})
        self.assertEqual(
            str(raised.exception),
            "effective native thread settings exceed the reviewed placement "
            "budget (threads_per_worker=2): torch_num_threads=4 > 2; "
            "opencv_num_threads=8 > 2",
        )

    def test_verify_fails_closed_on_onnx_inter_op_oversubscription_with_exact_message(
        self,
    ):
        """The onnx_inter_op_threads label and the full composed message must
        match exactly, not merely contain a similar substring."""
        cv2, torch = self._mock_native_modules(torch_threads=1, cv2_threads=1)
        budget = ThreadBudget(driver_cores=4, active_workers=2, threads_per_worker=2)
        environ = {"PC_ONNX_INTER_OP_THREADS": "6"}
        with patch.dict(sys.modules, {"cv2": cv2, "torch": torch}):
            with self.assertRaises(ThreadOversubscriptionError) as raised:
                verify_effective_thread_settings(budget, environment=environ)
        self.assertEqual(
            str(raised.exception),
            "effective native thread settings exceed the reviewed placement "
            "budget (threads_per_worker=2): onnx_inter_op_threads=6 > 2",
        )

    def test_verify_does_not_flag_onnx_inter_op_threads_exactly_at_the_budget(
        self,
    ):
        """Exactly at the per-worker budget is within budget, not an
        offender (the comparison is strictly-greater, not >=)."""
        cv2, torch = self._mock_native_modules(torch_threads=1, cv2_threads=1)
        budget = ThreadBudget(driver_cores=4, active_workers=2, threads_per_worker=2)
        environ = {"PC_ONNX_INTER_OP_THREADS": "2"}
        with patch.dict(sys.modules, {"cv2": cv2, "torch": torch}):
            settings = verify_effective_thread_settings(budget, environment=environ)
        self.assertEqual(settings.onnx_inter_op_threads, 2)

    def test_read_effective_thread_settings_defaults_to_os_environ(self):
        """When no explicit environment mapping is supplied, the real
        process environment (os.environ) must be used, not an empty/None
        mapping."""
        cv2, torch = self._mock_native_modules(torch_threads=1, cv2_threads=1)
        with patch.dict(sys.modules, {"cv2": cv2, "torch": torch}), patch.dict(
            os.environ, {"OMP_NUM_THREADS": "7"}
        ):
            settings = read_effective_thread_settings()
        self.assertEqual(
            settings.environment_as_dict()["OMP_NUM_THREADS"], "7"
        )


if __name__ == "__main__":
    unittest.main()
