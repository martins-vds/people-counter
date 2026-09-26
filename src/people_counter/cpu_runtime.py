"""CPU thread budgeting for shared-driver and executor workers."""

from __future__ import annotations

import logging
import os
from collections.abc import MutableMapping
from dataclasses import dataclass
from typing import cast


_LOG = logging.getLogger(__name__)
_THREAD_ENVIRONMENT_VARIABLES = (
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
)


@dataclass(frozen=True)
class ThreadBudget:
    driver_cores: int
    active_workers: int
    threads_per_worker: int
    interop_threads_configured: bool | None = None


_NATIVE_THREAD_BUDGET: ThreadBudget | None = None


def calculate_thread_budget(
    driver_cores: int,
    active_workers: int,
) -> ThreadBudget:
    """Calculate a CPU thread budget, flooring oversubscribed workers at one."""
    if type(driver_cores) is not int or driver_cores < 1:
        raise ValueError("driver_cores must be a positive integer")
    if type(active_workers) is not int or active_workers < 1:
        raise ValueError("active_workers must be a positive integer")
    if active_workers > driver_cores:
        _LOG.warning(
            "active_workers=%s exceeds driver_cores=%s; assigning one thread per worker",
            active_workers,
            driver_cores,
        )
    return ThreadBudget(
        driver_cores=driver_cores,
        active_workers=active_workers,
        threads_per_worker=max(1, driver_cores // active_workers),
    )


def configure_cpu_runtime(
    driver_cores: int,
    active_workers: int,
    *,
    environment: MutableMapping[str, str] | None = None,
    apply_native_limits: bool = True,
) -> ThreadBudget:
    """Apply process-local CPU thread limits before model construction."""
    budget = calculate_thread_budget(driver_cores, active_workers)

    global _NATIVE_THREAD_BUDGET
    if apply_native_limits and _NATIVE_THREAD_BUDGET is not None and (
        _NATIVE_THREAD_BUDGET.driver_cores != budget.driver_cores
        or _NATIVE_THREAD_BUDGET.active_workers != budget.active_workers
        or _NATIVE_THREAD_BUDGET.threads_per_worker != budget.threads_per_worker
    ):
        raise ValueError(
            "Native CPU runtime is already configured with "
            f"{_NATIVE_THREAD_BUDGET}; requested {budget}"
        )

    environ = os.environ if environment is None else environment
    threads = str(budget.threads_per_worker)
    for name in _THREAD_ENVIRONMENT_VARIABLES:
        existing = environ.get(name)
        if existing not in (None, threads):
            _LOG.warning(
                "Overriding %s=%s with %s for the configured CPU budget",
                name,
                existing,
                threads,
            )
        environ[name] = threads

    if apply_native_limits and _NATIVE_THREAD_BUDGET is None:
        import cv2
        import torch

        torch.set_num_threads(budget.threads_per_worker)
        interop_threads_configured = True
        try:
            torch.set_num_interop_threads(1)
        except RuntimeError as error:
            interop_threads_configured = torch.get_num_interop_threads() == 1
            _LOG.warning(
                "Could not change PyTorch inter-op threads after runtime initialization: %s",
                error,
            )
        cv2.setNumThreads(1)
        _NATIVE_THREAD_BUDGET = ThreadBudget(
            driver_cores=budget.driver_cores,
            active_workers=budget.active_workers,
            threads_per_worker=budget.threads_per_worker,
            interop_threads_configured=interop_threads_configured,
        )
    if apply_native_limits:
        budget = cast(ThreadBudget, _NATIVE_THREAD_BUDGET)

    _LOG.info(
        "Configured CPU worker: driver_cores=%s active_workers=%s "
        "threads_per_worker=%s interop_threads_configured=%s",
        budget.driver_cores,
        budget.active_workers,
        budget.threads_per_worker,
        budget.interop_threads_configured,
    )
    return budget
