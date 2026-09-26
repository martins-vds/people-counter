import os
import sys
import unittest
from unittest.mock import MagicMock, patch

from people_counter.cpu_runtime import calculate_thread_budget, configure_cpu_runtime


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

    def test_configure_cpu_runtime_sets_environment_budget(self):
        environ = {}

        with self.assertLogs(
            "people_counter.cpu_runtime",
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
                "INFO:people_counter.cpu_runtime:"
                "Configured CPU worker: driver_cores=4 "
                "active_workers=3 threads_per_worker=1 "
                "interop_threads_configured=None"
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

    def test_configure_cpu_runtime_overrides_existing_environment(self):
        environ = {"OMP_NUM_THREADS": "4"}

        with self.assertLogs("people_counter.cpu_runtime", level="WARNING") as logs:
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
            "threads_per_worker=2, interop_threads_configured=True); requested "
            "ThreadBudget(driver_cores=6, active_workers=3, "
            "threads_per_worker=2, interop_threads_configured=None)",
        )
        torch.set_num_threads.assert_called_once_with(2)
        torch.set_num_interop_threads.assert_called_once_with(1)
        cv2.setNumThreads.assert_called_once_with(1)
        self.assertEqual(
            logs.output,
            [
                "INFO:people_counter.cpu_runtime:"
                "Configured CPU worker: driver_cores=4 active_workers=2 "
                "threads_per_worker=2 interop_threads_configured=True",
                "INFO:people_counter.cpu_runtime:"
                "Configured CPU worker: driver_cores=4 active_workers=2 "
                "threads_per_worker=2 interop_threads_configured=True",
            ],
        )

    def test_configure_cpu_runtime_validates_native_budget_before_environment(self):
        for existing, requested in (
            (calculate_thread_budget(4, 2), (5, 2)),
            (calculate_thread_budget(4, 3), (4, 4)),
            (calculate_thread_budget(4, 2), (6, 2)),
        ):
            with self.subTest(existing=existing, requested=requested):
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
                    configure_cpu_runtime(*requested, environment=environ)

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
        cv2.setNumThreads.assert_called_once_with(1)

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
