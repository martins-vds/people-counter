"""CPU thread budgeting for shared-driver and executor workers."""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable, MutableMapping
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


@dataclass(frozen=True)
class EffectiveThreadSettings:
    """Readback of the thread settings actually in effect in this process.

    Configuring environment variables and native-library setters does not
    guarantee the underlying library honored them (some only read an
    environment variable once, at first import, from a different thread).
    This readback re-queries each library directly so oversubscription can be
    detected and rejected rather than assumed.
    """

    environment_variables: tuple[tuple[str, str | None], ...]
    torch_num_threads: int
    torch_num_interop_threads: int
    opencv_num_threads: int
    onnx_intra_op_threads: int | None
    onnx_inter_op_threads: int | None

    def environment_as_dict(self) -> dict[str, str | None]:
        return dict(self.environment_variables)


class ThreadOversubscriptionError(RuntimeError):
    """Effective native thread settings exceed the reviewed placement budget."""


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


def calculate_placement_safe_thread_budget(
    executor_cores: Iterable[int],
    task_cpus: int,
    planned_concurrency: int,
) -> ThreadBudget:
    """Budget native threads for the worst-case Spark task placement."""
    if type(task_cpus) is not int or task_cpus < 1:
        raise ValueError("task_cpus must be a positive integer")
    if type(planned_concurrency) is not int or planned_concurrency < 1:
        raise ValueError("planned_concurrency must be a positive integer")

    candidates = []
    for cores in executor_cores:
        if type(cores) is not int or cores < 1:
            raise ValueError("executor_cores must contain positive integers")
        scheduler_slots = cores // task_cpus
        if scheduler_slots < 1:
            continue
        colocated_workers = min(planned_concurrency, scheduler_slots)
        candidates.append(calculate_thread_budget(cores, colocated_workers))

    if not candidates:
        raise ValueError("executor_cores contain no runnable executor")
    return min(
        candidates,
        key=lambda budget: (
            budget.threads_per_worker,
            budget.driver_cores,
            budget.active_workers,
        ),
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
    return _apply_thread_budget(
        budget, environment=environment, apply_native_limits=apply_native_limits
    )


def configure_placement_safe_cpu_runtime(
    executor_cores: Iterable[int],
    task_cpus: int,
    planned_concurrency: int,
    *,
    environment: MutableMapping[str, str] | None = None,
    apply_native_limits: bool = True,
) -> ThreadBudget:
    """Apply the worst-case colocated-task-safe CPU thread budget.

    Unlike :func:`configure_cpu_runtime` (which assumes exactly
    ``active_workers`` tasks run on this executor), this accounts for the
    reviewed wider task-CPU profiles (1/2/4): once ``executor_cores`` exceeds
    ``task_cpus``, Spark can legally run more than one task concurrently on
    the same executor, and each concurrently-running task must budget native
    threads for the worst-case colocated count or risk CPU oversubscription.
    """
    budget = calculate_placement_safe_thread_budget(
        executor_cores, task_cpus, planned_concurrency
    )
    return _apply_thread_budget(
        budget, environment=environment, apply_native_limits=apply_native_limits
    )


def _apply_thread_budget(
    budget: ThreadBudget,
    *,
    environment: MutableMapping[str, str] | None,
    apply_native_limits: bool,
) -> ThreadBudget:
    """Apply one computed :class:`ThreadBudget` to the current process."""
    global _NATIVE_THREAD_BUDGET
    if apply_native_limits and _NATIVE_THREAD_BUDGET is not None and (
        _NATIVE_THREAD_BUDGET.threads_per_worker != budget.threads_per_worker
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
        cv2.setNumThreads(budget.threads_per_worker)
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


def _onnx_thread_override(
    environ: MutableMapping[str, str], name: str
) -> int | None:
    raw = environ.get(name)
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def read_effective_thread_settings(
    *, environment: MutableMapping[str, str] | None = None
) -> EffectiveThreadSettings:
    """Read back the thread settings actually in effect right now.

    This queries PyTorch, OpenCV, and the configured environment directly
    instead of trusting that a prior ``configure_cpu_runtime`` call took
    effect, so drift or a library that ignored the setting is observable.
    """
    import cv2
    import torch

    environ = os.environ if environment is None else environment
    env_snapshot = tuple(
        (name, environ.get(name)) for name in _THREAD_ENVIRONMENT_VARIABLES
    )
    return EffectiveThreadSettings(
        environment_variables=env_snapshot,
        torch_num_threads=torch.get_num_threads(),
        torch_num_interop_threads=torch.get_num_interop_threads(),
        opencv_num_threads=cv2.getNumThreads(),
        onnx_intra_op_threads=_onnx_thread_override(
            environ, "PC_ONNX_INTRA_OP_THREADS"
        ),
        onnx_inter_op_threads=_onnx_thread_override(
            environ, "PC_ONNX_INTER_OP_THREADS"
        ),
    )


def verify_effective_thread_settings(
    budget: ThreadBudget,
    *,
    environment: MutableMapping[str, str] | None = None,
) -> EffectiveThreadSettings:
    """Fail closed if the live readback exceeds the reviewed placement budget.

    Raises :class:`ThreadOversubscriptionError` with the exact offending
    setting instead of silently allowing more native threads than the
    worst-case colocated-task budget permits.
    """
    settings = read_effective_thread_settings(environment=environment)
    offenders: list[str] = []
    if settings.torch_num_threads > budget.threads_per_worker:
        offenders.append(
            f"torch_num_threads={settings.torch_num_threads} > "
            f"{budget.threads_per_worker}"
        )
    if settings.torch_num_interop_threads > 1:
        offenders.append(
            f"torch_num_interop_threads={settings.torch_num_interop_threads} > 1"
        )
    if settings.opencv_num_threads > budget.threads_per_worker:
        offenders.append(
            f"opencv_num_threads={settings.opencv_num_threads} > "
            f"{budget.threads_per_worker}"
        )
    for name, value in settings.environment_variables:
        if value is not None and int(value) > budget.threads_per_worker:
            offenders.append(f"{name}={value} > {budget.threads_per_worker}")
    for label, value in (
        ("onnx_intra_op_threads", settings.onnx_intra_op_threads),
        ("onnx_inter_op_threads", settings.onnx_inter_op_threads),
    ):
        if value is not None and value > budget.threads_per_worker:
            offenders.append(f"{label}={value} > {budget.threads_per_worker}")
    if offenders:
        raise ThreadOversubscriptionError(
            "effective native thread settings exceed the reviewed placement "
            f"budget (threads_per_worker={budget.threads_per_worker}): "
            + "; ".join(offenders)
        )
    return settings
