"""Real Fabric/Spark executor inventory discovery and task-width planning.

Candidate A previously planned partitions from a configured, hard-coded
executor count (see ``ExecutionProfile`` in ``sjd_process.py``). This module
discovers the *actual* active executors from Spark's live status store after
a registration-stability window, computes CPU slots and a conservative
memory cap from measured resources, and fails closed (rather than guessing
or silently falling back to a fixed count) when resources are missing,
unstable, or inconsistent with the reviewed profile.

Executor discovery deliberately does **not** use
``SparkContext.statusTracker().getExecutorInfos()``: that Python wrapper
never exposed the method, and live verification against the deployed Spark
4.1.1 runtime showed the underlying JVM method no longer exists either
(``py4j.protocol.Py4JError: Method getExecutorInfos([]) does not exist``).
The live-verified equivalent is ``SparkContext.statusStore().executorList``,
the same ``AppStatusStore`` backing the Spark UI/REST ``/executors``
endpoint; see :func:`_default_executor_summaries`.

Field access on the resulting ``ExecutorSummary`` objects deliberately does
**not** use ``hasattr`` to probe which candidate name exists: live
verification showed ``hasattr`` on a Py4J JVM proxy is always true for any
name (Py4J lazily builds a callable proxy per attribute access and only
discovers a missing JVM method on invocation --
``py4j.protocol.Py4JError: Method host([]) does not exist``, since the real
``ExecutorSummary`` exposes ``hostPort()``, not ``host()``). ``_field``
therefore invokes each candidate and falls through on that error; see
:func:`_py4j_invocation_error_types`.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol


class ExecutorInventoryError(RuntimeError):
    """Observed executor resources are missing, unstable, or mismatched."""


@dataclass(frozen=True)
class ExecutorRecord:
    """One observed, live Spark executor's identity and measured resources."""

    executor_id: str
    host: str
    total_cores: int
    max_memory_bytes: int

    def __post_init__(self) -> None:
        if not self.executor_id or not isinstance(self.executor_id, str):
            raise ExecutorInventoryError("executor_id is required")
        if not self.host or not isinstance(self.host, str):
            raise ExecutorInventoryError("host is required")
        if type(self.total_cores) is not int or self.total_cores < 1:
            raise ExecutorInventoryError(
                f"executor {self.executor_id!r} reports a non-positive core count"
            )
        if type(self.max_memory_bytes) is not int or self.max_memory_bytes < 1:
            raise ExecutorInventoryError(
                f"executor {self.executor_id!r} reports non-positive max memory"
            )


class ExecutorSummaryLike(Protocol):
    def id(self) -> str: ...
    def totalCores(self) -> int: ...
    def maxMemory(self) -> int: ...


def _py4j_invocation_error_types() -> tuple[type[Exception], ...]:
    """Return the Py4J error type raised when a JVM method does not exist.

    Live verification against the deployed Spark 4.1.1 runtime showed that
    ``hasattr(java_object, name)`` is **always true** for any name: Py4J
    lazily creates a callable proxy for every attribute access and only
    discovers a missing JVM method when that proxy is actually invoked
    (``py4j.protocol.Py4JError: Method host([]) does not exist``). ``_field``
    therefore cannot use ``hasattr`` to decide whether a candidate name is
    usable; it must attempt the call and catch this error to fall through to
    the next candidate. ``py4j`` is an optional runtime dependency (present
    under the Fabric Spark runtime, not necessarily in every local
    environment), so the import is deferred and failure-tolerant.
    """
    try:
        from py4j.protocol import Py4JError
    except ImportError:
        return ()
    return (Py4JError,)


def _field(summary: Any, *names: str) -> Any:
    invocation_errors = _py4j_invocation_error_types()
    for name in names:
        try:
            value = getattr(summary, name)
        except AttributeError:
            continue
        try:
            return value() if callable(value) else value
        except invocation_errors:
            continue
    raise ExecutorInventoryError(
        f"executor summary is missing all of {names!r}; Spark API surface changed"
    )


def _executor_host(summary: Any) -> str:
    """Return the bare host, preferring a direct field over ``host:port``."""
    try:
        return str(_field(summary, "host"))
    except ExecutorInventoryError:
        host_port = str(_field(summary, "hostPort", "host_port"))
        return host_port.rsplit(":", 1)[0] if ":" in host_port else host_port


def executor_records_from_executor_summaries(
    summaries: Sequence[Any],
) -> tuple[ExecutorRecord, ...]:
    """Convert live ``ExecutorSummary`` objects into sorted immutable records.

    The driver pseudo-executor (``"driver"``) is excluded because it is not
    a placement target for ``mapPartitions`` work.
    """
    records: list[ExecutorRecord] = []
    for summary in summaries:
        executor_id = str(_field(summary, "id", "executor_id"))
        if executor_id == "driver":
            continue
        records.append(
            ExecutorRecord(
                executor_id=executor_id,
                host=_executor_host(summary),
                total_cores=int(_field(summary, "totalCores", "total_cores")),
                max_memory_bytes=int(_field(summary, "maxMemory", "max_memory")),
            )
        )
    return tuple(sorted(records, key=lambda record: record.executor_id))


def _default_executor_summaries(spark_session: Any) -> list[Any]:
    """Return live ``ExecutorSummary`` objects from Spark's status store.

    Calls the underlying JVM ``AppStatusStore.executorList(activeOnly=True)``
    directly via Py4J (bypassing the incomplete/absent Python
    ``statusTracker`` wrapper -- see the module docstring) and drains its
    Scala ``Seq`` via the standard Java ``Iterator`` protocol, since py4j
    does not auto-convert Scala collections.
    """
    status_store = spark_session.sparkContext._jsc.sc().statusStore()
    java_iterator = status_store.executorList(True).iterator()
    summaries: list[Any] = []
    while java_iterator.hasNext():
        summaries.append(java_iterator.next())
    return summaries


def discover_active_executors(
    spark_session: Any,
    *,
    minimum_executors: int,
    stability_polls: int = 3,
    poll_interval_seconds: float = 2.0,
    deadline_seconds: float = 120.0,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
    executor_summaries: Callable[[Any], Sequence[Any]] = _default_executor_summaries,
) -> tuple[ExecutorRecord, ...]:
    """Poll the live executor inventory until it is stable, then return it.

    "Stable" means ``stability_polls`` consecutive polls, each separated by
    ``poll_interval_seconds``, observed the identical executor identity/core/
    memory set and it met ``minimum_executors``. This replaces guessing a
    fixed executor count with resource-readiness evidence. Fails closed with
    :class:`ExecutorInventoryError` if the deadline elapses first.

    A poll that raises :class:`ExecutorInventoryError` (for example, a
    not-yet-fully-registered executor reporting zero ``maxMemory`` --
    live-verified: ``executor '5' reports non-positive max memory`` on the
    deployed Fabric Spark 4.1.1 runtime) is treated as "not yet stable" and
    retried, not as a fatal error: resource *readiness* means tolerating a
    transiently-incomplete snapshot during registration, while still
    failing closed if the deadline elapses without ever observing a valid,
    stable inventory.
    """
    if type(minimum_executors) is not int or minimum_executors < 1:
        raise ExecutorInventoryError("minimum_executors must be a positive integer")
    if type(stability_polls) is not int or stability_polls < 2:
        raise ExecutorInventoryError("stability_polls must be at least two")
    if poll_interval_seconds <= 0 or deadline_seconds <= 0:
        raise ExecutorInventoryError(
            "poll_interval_seconds and deadline_seconds must be positive"
        )

    start = now()
    previous: tuple[ExecutorRecord, ...] | None = None
    stable_count = 0
    last_observed: tuple[ExecutorRecord, ...] = ()
    last_error: ExecutorInventoryError | None = None
    while True:
        try:
            current = executor_records_from_executor_summaries(
                executor_summaries(spark_session)
            )
        except ExecutorInventoryError as error:
            last_error = error
            previous = None
            stable_count = 0
        else:
            last_error = None
            last_observed = current
            if (
                previous is not None
                and current == previous
                and len(current) >= minimum_executors
            ):
                stable_count += 1
                if stable_count >= stability_polls:
                    return current
            else:
                stable_count = 0
            previous = current
        if now() - start >= deadline_seconds:
            message = (
                f"executor inventory did not reach {minimum_executors} stable "
                f"executor(s) within {deadline_seconds:g}s; last observed "
                f"{len(last_observed)} executor(s): {last_observed!r}"
            )
            if last_error is not None:
                message += f"; most recent poll failed validation: {last_error}"
                raise ExecutorInventoryError(message) from last_error
            raise ExecutorInventoryError(message)
        sleep(poll_interval_seconds)


def compute_slots(executors: Sequence[ExecutorRecord], task_cpus: int) -> int:
    """Return ``sum(floor(executor_cores / task_cpus))`` over live executors."""
    if type(task_cpus) is not int or task_cpus < 1:
        raise ExecutorInventoryError("task_cpus must be a positive integer")
    if not executors:
        raise ExecutorInventoryError("executors must not be empty")
    return sum(executor.total_cores // task_cpus for executor in executors)


def compute_memory_cap(
    executors: Sequence[ExecutorRecord],
    *,
    headroom: float,
    peak_rss_bytes: int,
) -> int:
    """Return a conservative placement-safe slot cap from measured RSS.

    Uses the *minimum* usable executor memory across the live inventory so
    the bound holds even if Spark colocates every task on the smallest
    executor, matching the benchmark's deliberately conservative assumption.
    """
    if not executors:
        raise ExecutorInventoryError("executors must not be empty")
    if not (0.0 <= headroom < 1.0):
        raise ExecutorInventoryError("headroom must be within [0, 1)")
    if type(peak_rss_bytes) is not int or peak_rss_bytes < 1:
        raise ExecutorInventoryError("peak_rss_bytes must be a positive integer")
    usable_min = min(executor.max_memory_bytes for executor in executors) * (
        1.0 - headroom
    )
    per_executor_slots = int(usable_min // peak_rss_bytes)
    if per_executor_slots < 1:
        raise ExecutorInventoryError(
            "measured peak RSS does not fit the minimum usable executor memory"
        )
    return per_executor_slots * len(executors)


@dataclass(frozen=True)
class TaskWidthPlacement:
    """Deterministic, placement-safe task count for one reviewed profile."""

    task_cpus: int
    slots: int
    memory_cap: int | None
    operator_cap: int | None
    planned_task_count: int
    executors: tuple[ExecutorRecord, ...]
    per_executor_safe_tasks: tuple[int, ...] = ()


def per_executor_memory_slots(
    executor: ExecutorRecord,
    *,
    headroom: float,
    peak_rss_bytes: int,
) -> int:
    """Return one executor's own ``floor(usable_memory / peak_rss_bytes)``.

    Unlike :func:`compute_memory_cap` (which deliberately uses the *minimum*
    usable executor across the whole inventory as a coarse, single global
    ceiling statistic), this evaluates each executor's *own* measured
    memory, which is what :func:`assert_executor_placement_safe` needs to
    validate real per-executor scheduling safety under heterogeneous
    inventories.
    """
    if not (0.0 <= headroom < 1.0):
        raise ExecutorInventoryError("headroom must be within [0, 1)")
    if type(peak_rss_bytes) is not int or peak_rss_bytes < 1:
        raise ExecutorInventoryError("peak_rss_bytes must be a positive integer")
    usable = executor.max_memory_bytes * (1.0 - headroom)
    return int(usable // peak_rss_bytes)


def assert_executor_placement_safe(
    executors: Sequence[ExecutorRecord],
    *,
    task_cpus: int,
    peak_rss_bytes: int,
    headroom: float = 0.20,
) -> tuple[int, ...]:
    """Fail closed unless every executor's own cores-based ceiling fits memory.

    Spark's scheduler bounds concurrent tasks on one executor by
    ``floor(executor.total_cores / task_cpus)`` -- a hard, physical ceiling
    that a merely-conservative *global* total (such as the old
    ``min(slots, memory_cap)`` formula) cannot override: nothing stops Spark
    from scheduling every planned task onto the one executor with the most
    cores, even when a smaller/lower-memory executor's share of a uniform
    global cap would have been safe. Placement is only actually safe when,
    for *every* observed executor, that executor's own cores ceiling is
    already less than or equal to what its own measured memory can host.

    Returns one placement-safe task count per executor (same order as
    ``executors``), where each entry is
    ``min(floor(cores/task_cpus), floor(usable_memory/peak_rss_bytes))`` --
    equal to the cores ceiling whenever the check below passes.
    """
    if type(task_cpus) is not int or task_cpus < 1:
        raise ExecutorInventoryError("task_cpus must be a positive integer")
    if not executors:
        raise ExecutorInventoryError("executors must not be empty")
    per_executor: list[int] = []
    unsafe: list[tuple[str, int, int]] = []
    for executor in executors:
        cores_slots = executor.total_cores // task_cpus
        if cores_slots < 1:
            raise ExecutorInventoryError(
                f"executor {executor.executor_id!r} cannot host one task at "
                f"task_cpus={task_cpus}"
            )
        mem_slots = per_executor_memory_slots(
            executor, headroom=headroom, peak_rss_bytes=peak_rss_bytes
        )
        if mem_slots < 1:
            raise ExecutorInventoryError(
                f"executor {executor.executor_id!r} measured peak RSS does not "
                "fit its usable memory"
            )
        if cores_slots > mem_slots:
            unsafe.append((executor.executor_id, cores_slots, mem_slots))
        per_executor.append(min(cores_slots, mem_slots))
    if unsafe:
        raise ExecutorInventoryError(
            f"task_cpus={task_cpus} is not placement-safe: Spark's own "
            "cores-based scheduling ceiling exceeds the measured memory-safe "
            "concurrency on executor(s) "
            f"{[(eid, f'cores_ceiling={c}', f'memory_safe={m}') for eid, c, m in unsafe]!r}; "
            "widen task_cpus (a reviewed width of 1/2/4) or reduce executor "
            "memory pressure before admitting this concurrency"
        )
    return tuple(per_executor)


def conservative_task_cpus_for_unmeasured_rss(
    executors: Sequence[ExecutorRecord],
) -> int:
    """Return the task-CPU width that limits every executor to one task.

    Reviewed unknown-RSS policy: when warm executor inference RSS has not
    been measured (a capability-null result), concurrency must not be
    admitted above one task per executor. Since ``task_cpus`` is one
    cluster-wide Spark setting (Spark has no native per-executor task-width
    control), the only width that guarantees ``floor(cores/task_cpus) <= 1``
    for *every* observed executor -- however heterogeneous -- is the widest
    observed core count.
    """
    if not executors:
        raise ExecutorInventoryError("executors must not be empty")
    return max(executor.total_cores for executor in executors)


def plan_task_width(
    executors: Sequence[ExecutorRecord],
    *,
    task_cpus: int,
    operator_cap: int | None = None,
    peak_rss_bytes: int | None = None,
    headroom: float = 0.20,
) -> TaskWidthPlacement:
    """Compute ``P = min(N, S, M, P_operator)`` from live, measured resources.

    When ``peak_rss_bytes`` is supplied, this also fails closed via
    :func:`assert_executor_placement_safe` unless *every* executor's own
    cores-based ceiling already fits its own measured memory -- a global
    total alone cannot guarantee safe placement under heterogeneous
    inventories. Omitting ``peak_rss_bytes`` entirely (``None``) skips
    memory validation and is the caller's explicit choice; callers that must
    honor the reviewed unknown-RSS policy should instead resolve
    ``task_cpus`` via :func:`conservative_task_cpus_for_unmeasured_rss` and
    pass that resolved width here.
    """
    slots = compute_slots(executors, task_cpus)
    memory_cap: int | None = None
    per_executor_safe: tuple[int, ...] = ()
    if peak_rss_bytes is not None:
        per_executor_safe = assert_executor_placement_safe(
            executors,
            task_cpus=task_cpus,
            peak_rss_bytes=peak_rss_bytes,
            headroom=headroom,
        )
        memory_cap = compute_memory_cap(
            executors, headroom=headroom, peak_rss_bytes=peak_rss_bytes
        )
    limits = [slots]
    if memory_cap is not None:
        limits.append(memory_cap)
    if operator_cap is not None:
        if type(operator_cap) is not int or operator_cap < 1:
            raise ExecutorInventoryError("operator_cap must be a positive integer")
        limits.append(operator_cap)
    planned = min(limits)
    if planned < 1:
        raise ExecutorInventoryError("planned task width collapsed to zero slots")
    return TaskWidthPlacement(
        task_cpus=task_cpus,
        slots=slots,
        memory_cap=memory_cap,
        operator_cap=operator_cap,
        planned_task_count=planned,
        executors=tuple(executors),
        per_executor_safe_tasks=per_executor_safe,
    )


def assert_matches_reviewed_profile(
    executors: Sequence[ExecutorRecord],
    *,
    expected_executor_cores: int,
    expected_task_cpus: int,
) -> None:
    """Reject live resources that diverge from the reviewed, approved profile.

    A profile is a reviewed, benchmarked configuration; silently adapting to
    whatever Spark happens to report would invalidate prior measurements.
    Mismatches must fail closed so planning never runs against unreviewed
    resources.
    """
    if type(expected_executor_cores) is not int or expected_executor_cores < 1:
        raise ExecutorInventoryError("expected_executor_cores must be a positive integer")
    if type(expected_task_cpus) is not int or expected_task_cpus < 1:
        raise ExecutorInventoryError("expected_task_cpus must be a positive integer")
    if expected_task_cpus > expected_executor_cores:
        raise ExecutorInventoryError("expected_task_cpus cannot exceed executor cores")
    mismatched = [
        executor
        for executor in executors
        if executor.total_cores != expected_executor_cores
    ]
    if mismatched:
        raise ExecutorInventoryError(
            "observed executor cores do not match the reviewed profile "
            f"(expected {expected_executor_cores}): "
            f"{[(e.executor_id, e.total_cores) for e in mismatched]!r}"
        )


def assert_minimum_videos_per_group(
    group_sizes: Sequence[int],
    *,
    total_items: int,
    minimum_videos_per_group: int,
) -> None:
    """Fail closed if workload permits amortization but a group is too small.

    If the total claimed workload could have filled every group with at
    least ``minimum_videos_per_group`` videos, every group must actually
    receive that many; otherwise planning silently gave up on model-load
    amortization instead of reporting it.
    """
    if type(minimum_videos_per_group) is not int or minimum_videos_per_group < 1:
        raise ExecutorInventoryError(
            "minimum_videos_per_group must be a positive integer"
        )
    group_count = len(group_sizes)
    if group_count == 0:
        return
    if total_items < group_count * minimum_videos_per_group:
        return
    shortfalls = [size for size in group_sizes if size < minimum_videos_per_group]
    if shortfalls:
        raise ExecutorInventoryError(
            f"workload of {total_items} item(s) across {group_count} group(s) "
            f"permits >= {minimum_videos_per_group} videos per group, but "
            f"observed group sizes {sorted(group_sizes)!r}"
        )
