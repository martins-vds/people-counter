# Design: Capacity-Aware Parallel Inference in Fabric

Date: September 27, 2026

Status: Proposed

Target: `notebooks/fabric/15_executor_partition_inference.ipynb`

This document is a design proposal only. It records the approved capacity-aware
executor-partition inference design and the current-code facts verified against
`notebooks/fabric/15_executor_partition_inference.ipynb`,
`src/people_counter/cpu_runtime.py`, and
`src/people_counter/fabric_executor_partition.py`. It does not implement
notebook, SDK, or settings changes.

## 1. Objective

Maximize source-video minutes processed per wall-clock minute across the Spark
executors allocated to this Fabric application. The design should balance whole
video work, CPU thread budgets, and memory so that the allocated executors do
useful inference work without harmful oversubscription.

CPU percentage and task count are supporting diagnostics, not the primary
success metric. Spark allocation is also not the same as total Fabric capacity
entitlement: the notebook must reason from the Spark resources actually
requested and granted to the running application, not from the workspace or SKU
maximum alone.

## 2. Current implementation

Current notebook 15 is experimental and separate from the production
claim/lease/commit workflow in `notebooks/fabric/04_process_video.ipynb`.
Notebook 15 reads prepared rows, runs inference through Spark `mapPartitions`,
and writes compact result records. It does not currently participate in notebook
04's production protocol.

Verified current behavior:

- `notebooks/fabric/15_executor_partition_inference.ipynb` manually supplies
  `EXECUTOR_CORES = 4`, `ACTIVE_TASKS_PER_EXECUTOR = 4`, and
  `TARGET_PARTITIONS = 4`.
- The notebook calculates `threads_per_worker` from
  `max(1, EXECUTOR_CORES // ACTIVE_TASKS_PER_EXECUTOR)` through
  `calculate_thread_budget`.
- The notebook calls `spark_session.conf.set("spark.task.cpus", ...)` after the
  Spark session exists; this must not be treated as validated scheduler
  allocation for already-started resources.
- The notebook repartitions prepared work with
  `.repartition(TARGET_PARTITIONS, F.col("work_id"))`, which is hash-based and
  has no video-cost awareness.
- The notebook has no validation that the requested executor/task CPU allocation
  matches the allocation granted by Fabric/Spark.
- `src/people_counter/fabric_executor_partition.py` creates a default
  `SdkRuntimeProcessor()` inside each `process_video_partition` invocation when
  no processor is supplied. That processor has an `ExecutorRuntimeCache`, but
  because the default processor is created per partition invocation, model
  runtimes are cached only within that invocation.
- Models therefore load lazily on the first compatible video in a partition
  invocation and are reused by subsequent rows with the same runtime cache key
  in that same invocation. They are not shared executor-wide by the current
  default implementation.
- `src/people_counter/cpu_runtime.py` applies native thread limits to
  environment variables, PyTorch, and OpenCV. Its process-level guard rejects a
  later request for a changed native process budget.

## 3. Proposed notebook controls

Replace the manual core/concurrency inputs with these proposed notebook
controls:

```python
CPUS_PER_TASK = 1
PARALLEL_TASKS = "auto"
PARTITION_WAVES = 3
RESOURCE_DISCOVERY_TIMEOUT_SECONDS = 120
```

These are initial benchmark values, not validated production defaults.

| Control | Semantics |
|---|---|
| `CPUS_PER_TASK` | Positive integer CPUs requested per Spark task before Spark resources start. This controls scheduler allocation when applied through a supported Fabric startup configuration mechanism. |
| `PARALLEL_TASKS = "auto"` | Derive the application-local inference slot count from the allocated executors and `CPUS_PER_TASK`. |
| `PARALLEL_TASKS = N` | Positive integer lower application-local inference cap. It queues no more than `N` inference partitions at a time in explicit-cap mode, but it is not a cluster-wide admission control. |
| `PARTITION_WAVES` | Positive integer multiplier that queues additional planned partitions beyond immediate slots so Spark has work available as tasks complete. |
| `RESOURCE_DISCOVERY_TIMEOUT_SECONDS` | Bounded discovery/bootstrap wait for observing the running allocation. Timeout behavior must be explicit and must label any fallback resource values as assumed. |

Any discovery fallback must be explicit in the notebook output and persisted
metadata. Do not silently infer unavailable allocation data.

## 4. Resource discovery

Startup configuration and running-session discovery are separate concerns.
`spark.conf.set("spark.task.cpus", ...)` after startup must not be relied on to
change Spark scheduler CPU allocation for already-initialized resources. The
implementation must first validate the supported Fabric startup mechanism for
setting task CPUs and executor resources, then stop with restart instructions
when the effective running allocation does not match the requested startup
configuration.

The resource snapshot should be typed and persisted with at least:

- Spark application ID;
- Fabric/Spark runtime identifier where available;
- snapshot timestamp;
- discovery source/backend;
- active executor IDs and cores;
- effective task CPUs;
- dynamic allocation lower and upper bounds;
- executor and driver memory configuration;
- completeness indicator, including whether fallback values were used;
- competing work observed in the same Spark application or pool where the
  supported monitoring backend exposes it.

Discovery must exclude the driver and removed executors. It should count
allocated executor resources, not just currently idle cores. Neither driver
`os.cpu_count()` nor Spark `defaultParallelism` is authoritative for this
purpose.

Use a bounded wait/bootstrap action that can observe executor allocation without
loading models. Before implementation, validate which monitoring backend is
accessible and supported in the deployed Fabric runtime. Isolate fallbacks and
do not document or implement a fabricated working discovery API.

## 5. Slot calculation

For each active executor `i` with `C_i` cores and effective task CPU request
`T`, calculate slots as:

```text
approved_slots = sum(floor(C_i / T) for each active executor i)
```

Never calculate `floor(sum(C_i) / T)`, because Spark tasks cannot combine
fragmented CPUs across executor boundaries.

For four 8-core executors:

| `CPUS_PER_TASK` (`T`) | Slots per executor, `floor(8 / T)` | Application slots | Cores allocated when slots are full |
|---:|---:|---:|---:|
| 1 | 8 | 32 | 32 |
| 2 | 4 | 16 | 32 |
| 3 | 2 | 8 | 24 |
| 4 | 2 | 8 | 32 |

These ceilings are capacity ceilings, not guaranteed occupancy. Reject zero-slot
plans. Warn about fragments such as the unused 2 cores per 8-core executor when
`T = 3`.

In auto mode:

```text
planned_concurrency = min(approved_slots, video_count)
```

An explicit task cap cannot be enforced merely by setting fewer Spark
partitions, because queued partitions may overlap with retries or other jobs and
Spark still controls placement. The initial explicit-cap design should use
sequential batches of at most the requested inference partitions. This reduces
utilization compared with auto mode and does not limit other jobs or
applications.

Disable speculation for capped benchmark runs. Document retry and cancellation
overlap: explicit batches are not a hard process admission guarantee. Auto mode
is preferred for maximizing useful throughput.

## 6. Memory

Each worker can consume memory for model runtimes, frames, decoders, tracking
state, native libraries, and intermediate buffers. Capacity planning must
measure a peak worker memory value for the selected pipeline and calculate:

```text
memory_safe_workers = floor(usable_executor_memory / peak_worker_memory)
```

Include headroom for JVM, Python worker, native allocations, and storage/shuffle
overheads. If the memory ceiling is below the CPU slot count, revise or reject
the startup plan. Options include increasing `CPUS_PER_TASK` to reduce
colocation, requesting more executor memory, using smaller inference batches, or
choosing different models/settings.

Reducing the total number of partitions alone is not per-executor memory
protection, because Spark placement can still colocate memory-heavy tasks on the
same executor.

## 7. Partition planning

Plan partition count as:

```text
partition_count = min(video_count, planned_concurrency * PARTITION_WAVES)
```

Empty partitions should load no models. Whole videos are indivisible in this
design; do not split frames from one video across partitions.

Estimate video cost from historical duration when available, or from sampled
frames, resolution, and pipeline weights. If metadata is missing, use a labeled
fallback estimate rather than an implicit default. Use deterministic
largest-cost-first placement into the least-loaded bucket, with stable video
identities and tie-breaking. Preserve runtime affinity only when it does not
materially worsen balance.

Persist the plan, including the explicit mapping from planned buckets to
physical Spark partitions. Hash repartitioning alone is not a one-to-one mapping
from planned bucket to physical partition and is therefore insufficient as the
only plan representation.

Bound driver metadata and use distributed or windowed planning for very large
input sets. Spark ultimately controls task placement, so the notebook cannot
promise equal executor occupancy. A long-video tail can still remain even with a
balanced plan.

Before running executor inference, verify that executor tasks can access input
videos and artifact/model paths. Driver-local paths are insufficient unless they
are also valid on executor nodes.

## 8. Task native threads

The proposed partition function conceptually becomes:

```python
def partition_records(rows):
    from pyspark import TaskContext

    context = TaskContext.get()
    if context is None:
        raise RuntimeError("Partition inference requires a Spark task context")

    task_cpus = context.cpus()
    if not isinstance(task_cpus, int) or task_cpus < 1:
        raise RuntimeError(f"Invalid Spark task CPU allocation: {task_cpus}")

    configure_cpu_runtime(driver_cores=task_cpus, active_workers=1)

    dictionaries = (row.asDict(recursive=True) for row in rows)
    yield from process_video_partition(dictionaries, config_builder=build_config)
```

`TaskContext.cpus()` is the task's allocated CPU count, not an idle CPU count.
Passing `driver_cores=task_cpus` and `active_workers=1` is mathematically
correct with the current helper, though the `driver_cores` name is misleading in
task context. A later implementation may add a task-oriented wrapper while
preserving the public helper API.

Configure native threads before constructing or loading models. Retain the
existing guard that rejects conflicting native process budgets unless it is
explicitly redesigned. Validate uniform allocation within a session, restart
between benchmark settings, and test worker reuse plus changed-budget handling.

Native library thread limits are not OS-level CPU isolation. Decode, I/O, and
nested library threads must be measured rather than assumed controlled.

## 9. Model lifecycle

The current lifecycle is:

```text
partition invocation -> new default processor/cache -> first compatible video
loads a runtime -> subsequent compatible videos in the same invocation reuse it
```

More partitions can therefore increase model load count. Benchmark both tail
reduction from additional partitions and model-load amortization from fewer,
larger partitions. A persistent executor-wide runtime cache is out of scope for
this design document.

## 10. Dynamic allocation and retries

Benchmarks should use fixed approved allocation when comparing settings. Capture
resource snapshots before and after each run. If dynamic allocation changes the
executor set between batches, replan between batches and distinguish configured
maxima from observed allocation. Label any configured values used only as
scaling targets.

Do not change native thread budgets inside already-running Python workers
because executor counts changed elsewhere. Retry and recompute can reload models
and reprocess videos. Persist attempt-aware records and make publication
idempotent; cache reuse is not an exactly-once guarantee. Integration with the
production publication protocol remains separate from this notebook 15 design.

## 11. Observability

| Area | Required observations |
|---|---|
| Resources | Requested, configured, observed, and effective resources; executor IDs; executor cores; task CPUs; discovery source; timestamps. |
| Concurrency | Planned concurrency, observed concurrency, partition count, queue waves, and explicit-cap batch identity when used. |
| Balance | Estimated partition cost, actual partition runtime, skew, and long-tail impact. |
| Models | Runtime cache keys, model load counts, and model load time. |
| Timing | Staging, inference, persistence, startup/warm time, and end-to-end wall-clock time. |
| Utilization | Peak memory, CPU utilization, decode/I/O indicators, native-thread settings, errors, retries, and recomputation. |
| Throughput | Source-video minutes processed per wall-clock minute. |
| Identity | Spark application ID, planned bucket ID, physical partition ID, task attempt ID, and work/video IDs. |

## 12. Implementation sequence

Document only for this PR. Future implementation should proceed in this order:

1. Define typed resource, plan, and metric records.
2. Implement supported resource discovery and request/effective validation.
3. Separate startup configuration guidance from execution-time discovery.
4. Apply per-task native thread limits from `TaskContext.cpus()`.
5. Implement cost-aware planning and persisted planned-bucket mapping.
6. Implement auto mode and explicit sequential batches.
7. Add memory safety checks and metrics.
8. Run benchmark comparisons and select settings from measured throughput.

## 13. Tests and acceptance

Future tests should cover:

- heterogeneous executors, CPU fragments, invalid CPU values, missing resources,
  and driver exclusion;
- requested/effective allocation mismatch and restart instructions;
- native setup before model loading;
- equal and conflicting process budgets;
- empty input, few videos, skewed videos, missing metadata, and deterministic
  bucket mapping;
- explicit batches, speculation disabled for capped runs, retries, and
  cancellation/recompute overlap;
- model reuse, output equivalence, and memory-based plan rejection.

Benchmark matrix:

- `CPUS_PER_TASK` values: 1, 2, and 4;
- `PARTITION_WAVES` values: 1, 2, 3, and 4;
- representative short, long, and skewed videos;
- fixed settings with repeated runs;
- separate initialization/warm time from steady inference time;
- compare against the existing four-partition baseline.

Accept the design only when:

- requested and effective allocation budgets agree or the notebook stops with
  actionable restart instructions;
- no representative workload OOMs or shows harmful CPU oversubscription;
- outputs are equivalent to the current implementation;
- measured end-to-end throughput improves with acceptable retry, startup, and
  model-load cost.

The end principle is to maximize useful source-video throughput, not CPU
percentage alone.
