# Design: Capacity-Aware Parallel Inference in Fabric

Date: September 27, 2026

Status: Implemented prototype

Target: `notebooks/fabric/15_executor_partition_inference.ipynb`

This document records the implemented capacity-aware executor-partition
inference design and the current-code facts verified against
`notebooks/fabric/15_executor_partition_inference.ipynb`,
`src/people_counter/cpu_runtime.py`, and
`src/people_counter/fabric_executor_partition.py`.

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

- `notebooks/fabric/15_executor_partition_inference.ipynb` uses a first-code-cell
  parameterized `%%configure`, then discovers the executor cores actually
  granted to the running application.
- The notebook validates requested `CPUS_PER_TASK` against both
  `spark.task.cpus` and executor-side `TaskContext.cpus()` evidence.
- The notebook calculates native threads independently from the scheduler CPU
  token using observed executor cores and placement-safe planned concurrency.
- Prepared videos are assigned largest-cost-first to persisted planned buckets,
  with bounded runtime-affinity retention and explicit physical partition
  mappings.
- Memory-heavy inference partitions run in sequential global batches no larger
  than the measured placement-safe memory cap.
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

## 3. Notebook controls

The prototype replaces manual core/concurrency inputs with these notebook
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
| `PARALLEL_TASKS = "auto"` | Derive placement-safe application-local inference concurrency from observed scheduler slots, measured memory, and video count. |
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

The primary running-session backend is the Spark driver's `AppStatusStore`.
`executorList(true)` supplies typed active executor summaries without crossing
the Fabric Spark UI HTTP proxy. The REST endpoint remains a bounded fallback;
both sources must exclude the driver and persist their distinct discovery
source.

Fabric `SparkContext.uiWebUrl` values may include query parameters required by
the Spark UI proxy. REST discovery must append the
`/api/v1/applications/.../executors` route to the parsed URL path and then
preserve the original query string; concatenating the route after the raw URL
turns it into query data and can produce HTTP 406 responses.

## 5. Slot calculation

For each active executor `i` with `C_i` cores and effective task CPU request
`T`, calculate slots as:

```text
approved_slots = sum(floor(C_i / T) for each active executor i)
```

Never calculate `floor(sum(C_i) / T)`, because Spark tasks cannot combine
fragmented CPUs across executor boundaries.

For four 8-core executors:

| `CPUS_PER_TASK` (`T`) | Slots per executor, `floor(8 / T)` | Unused cores per executor | Application slots | Cores allocated when slots are full |
|---:|---:|---:|---:|---:|
| 1 | 8 | 0 | 32 | 32 |
| 2 | 4 | 0 | 16 | 32 |
| 3 | 2 | 2 | 8 | 24 |
| 4 | 2 | 0 | 8 | 32 |

These ceilings are capacity ceilings, not guaranteed occupancy. Reject zero-slot
plans. Warn about fragments such as the unused 2 cores per 8-core executor when
`T = 3`.

Before applying the memory cap, the scheduler slot ceiling is:

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
overheads. When Fabric retains a managed `spark.task.cpus=1`, memory safety must
not depend on increasing scheduler CPUs. Instead, cap each submitted inference
batch to at most `memory_safe_workers`. Because this is a global cap no larger
than one executor's safe limit, no placement can colocate more memory-heavy
tasks on any executor. Execute additional planned partitions in sequential
batches.

This placement-independent cap is conservative with multiple executors because
it does not multiply by executor count. Multiplying the cap would be unsafe
without enforceable executor affinity: Spark could place several partitions on
one executor. A lower explicit `PARALLEL_TASKS` value is safe but
underutilized and should emit a warning; a value above the measured cap must be
rejected.

The characterization evaluator derives the cap from persisted
`usable_executor_memory_gib`, the configured headroom fraction, and the
suggested worker peak. It includes the suggested peak, the effective
`CPUS_PER_TASK`, the placement-safe task cap, and the derived native threads per
worker at that cap in its terminal recommendation.

When no peak measurement exists, a zero configured peak selects a
characterization-only run. Force planned concurrency and physical partition
count to one regardless of the normal cap or wave settings, execute the complete
representative sample sequentially, and capture the executor Python process
`VmHWM` before and after the partition. Persist the maximum observed value and a
recommendation rounded above it by at least one 0.25 GiB quantum. The evaluation gate must always
reject characterization mode and tell the operator to rerun under a new batch
identity with the recommendation. Normal runs should retain the same
instrumentation and reject evidence when the observed high-water mark exceeds
the configured peak.

Use a zero configured usable-memory value to request automatic discovery.
Probe every runnable executor from an executor-side task. Start with a
conservative Spark configuration envelope and tighten it with finite cgroup
telemetry when that telemetry is exposed. Bound the discovered worker budget
by:

- `spark.executor.pyspark.memory`, when configured;
- otherwise explicit or Spark-derived `spark.executor.memoryOverhead`, after a
  separate JVM-native/container reserve;
- when a finite cgroup limit is available, cgroup memory currently free at the
  probe after excluding reclaimable file cache; and
- when a finite cgroup limit is available, cgroup capacity remaining after the
  configured executor heap, Spark off-heap reservation, and—when PySpark memory
  is separate—the executor overhead.

Use the minimum result across observed executors and retain the existing
headroom calculation before approving slots. Retry a bounded number of probe
stages to cover every executor identity discovered from Spark monitoring. Some
Fabric Python workers expose `SPARK_EXECUTOR_ID` as the literal string `None`;
normalize unavailable-ID sentinels before coverage evaluation and use distinct
worker host/address identities as the fallback coverage key. Do not count a
sentinel as an observed executor ID.
Persist the source, effective Spark memory settings, cgroup limit/current
usage, reclaimable cache, checked cgroup paths, Python worker RSS, and
per-executor deductions. When Fabric exposes only an unbounded parent cgroup,
use the executor-visible physical node memory as the outer envelope. Microsoft
Fabric documents a 1:1 node-to-executor ratio (except single-node sessions,
which are outside this benchmark shape) and node sizes from 32 GB upward.
Subtract the configured JVM heap and Spark off-heap reservation, then reserve
the greater of 384 MiB or 25% of the remaining node capacity for JVM native
memory, direct buffers, thread stacks, and allocator arenas. Label this
evidence `fabric-node-envelope` and persist the observed physical memory and
all deductions. A positive Spark/PySpark worker envelope, when available, can
tighten but must never expand the Fabric node envelope.

Fabric can retain a managed `spark.executor.memoryOverhead=384m` despite a
different `%%configure` request, so the benchmark must not claim that this
property created additional container memory. Fail rather than infer a budget
from capacity SKU or storage-memory telemetry when complete executor probe
coverage or a positive node envelope is unavailable. A positive operator
override remains supported but must be labeled so evaluation cannot mistake it
for runtime-derived evidence. If the physical node does not exceed configured
JVM/off-heap reservations, direct the operator to lower JVM heap or select a
larger pool node.

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

Spark scheduler CPUs and native inference threads are separate budgets.
`TaskContext.cpus()` validates the effective scheduler allocation, but it must
not directly cap CPU-heavy native inference when placement-safe batching leaves
executor cores idle.

For executor `i`, let:

```text
C_i = observed executor cores
T = effective spark.task.cpus
P = planned global inference concurrency
slots_i = floor(C_i / T)
max_colocated_workers_i = min(P, slots_i)
native_threads_i = floor(C_i / max_colocated_workers_i)

native_threads_per_worker = min(native_threads_i across runnable executors)
```

Taking the minimum makes the budget safe for heterogeneous executors and
worst-case task placement. For one observed 16-core executor, `T=1`, and
`P=3`, each worker receives `floor(16 / 3) = 5` native threads. Spark still
records one scheduler CPU per task, while the globally bounded inference stage
contains at most three tasks and therefore uses at most 15 native threads.

The partition function conceptually becomes:

```python
def partition_records(rows):
    from pyspark import TaskContext

    context = TaskContext.get()
    if context is None:
        raise RuntimeError("Partition inference requires a Spark task context")

    task_cpus = normalize_task_cpus(context.cpus())
    if task_cpus != CPUS_PER_TASK:
        raise RuntimeError("Spark task CPU allocation changed")

    configure_cpu_runtime(
        driver_cores=native_thread_budget_cores,
        active_workers=native_thread_budget_workers,
    )

    dictionaries = (row.asDict(recursive=True) for row in rows)
    yield from process_video_partition(dictionaries, config_builder=build_config)
```

`TaskContext.cpus()` is the task's allocated CPU count, not an idle CPU count.
Fabric can expose this count as an integral floating-point value such as
`1.0`; normalize positive integral numeric representations to `int`, while
rejecting fractional, zero, non-finite, Boolean, and nonnumeric values.

Configure native threads before constructing or loading models. Retain the
existing guard that rejects conflicting native process budgets unless it is
explicitly redesigned. Validate uniform allocation within a session, restart
between benchmark settings, and test worker reuse plus changed-budget handling.
Persist the derived budget in the run metric and the applied value in every
task record. Evaluation must independently recompute the expected value from
the before-run executor snapshot and reject mismatches.

Characterization mode uses one planned worker and therefore gives that worker
all cores of the smallest applicable executor. This measures peak memory under
the largest native thread pool the planner can assign. Any peak measured under
the earlier one-native-thread implementation is not valid evidence for this
design; rerun characterization with `PEAK_WORKER_MEMORY_GIB=0` after deployment.

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

The prototype was implemented in this order:

1. Define typed resource, plan, and metric records.
2. Implement supported resource discovery and request/effective validation.
3. Separate startup configuration guidance from execution-time discovery.
4. Derive native thread limits from observed executor cores and placement-safe
   concurrency, independently of `TaskContext.cpus()`.
5. Implement cost-aware planning and persisted planned-bucket mapping.
6. Implement auto mode and explicit sequential batches.
7. Add memory safety checks and metrics.
8. Run benchmark comparisons and select settings from measured throughput.

## 13. Tests and acceptance

Tests and live Fabric validation should cover:

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

## 14. Automated benchmark pipeline

Notebook 15 is tested through a dedicated Fabric Data pipeline rather
than by manually creating prepared Delta rows. The pipeline is a sibling of the
existing notebook-04 capacity benchmark, but its concurrency shape is
different: notebook 15 must run once with the complete benchmark set in one
Spark application. A per-video Fabric `ForEach` would measure multiple
applications and would not exercise executor-partition scheduling.

### 14.1 Implemented artifacts

- [`16_executor_partition_benchmark_control.ipynb`](../notebooks/fabric/16_executor_partition_benchmark_control.ipynb),
  with `PREPARE` and `EVALUATE` modes;
- [`pc-executor-partition-benchmark`](../notebooks/fabric/exports/pc-executor-partition-benchmark.json),
  a child pipeline export for one configuration;
- optional `pc-executor-partition-benchmark-suite`, a sequential parameter
  matrix wrapper; and
- a run-scoped prepared-input contract in
  `people_counter_executor_partition_input`.

The control notebook is separate from notebook 15 so source validation, direct
File API probing, and executor-access checks are not included in measured
inference throughput, while their own timing remains available for diagnosis.
The pipeline passes one `DATABASE` and `TABLE_PREFIX` value to preparation,
inference, and evaluation. The checked-in default is
`TABLE_PREFIX=people_counter`, matching the bootstrap notebook; the three
activities must never rely on independent table-name defaults.
Fabric may render an empty pipeline string parameter as Python `None`; both
notebooks normalize null sentinels to an empty database qualifier before
resolving those shared tables.

### 14.2 Prepare, run, evaluate

The child pipeline is:

```text
PrepareExecutorBenchmark
    -> RunExecutorPartitionInference
    -> EvaluateExecutorBenchmark
    -> RefreshBenchmarkModel
    -> conditional Fail propagation
```

Preparation accepts the same simple item shape operators already use for the
notebook-04 benchmark:

```json
{
  "video_uri": "abfss://.../Files/.../common-1080p.mp4",
  "sample_name": "common-1080p-medium-motion"
}
```

Duration and resolution are optional reviewed fields. Preparation verifies that
each distinct source URI references the exact attached Lakehouse, maps its
decoded `Files/...` suffix to `/lakehouse/default/Files/...`, probes that
mounted path directly without copying, and writes one complete row per
submitted item. Supplied and probed metadata must agree within reviewed
tolerances. Unreadable or incomplete media fails preparation; an approval run
must not use planning fallbacks.

Before inference, a model-free Spark action verifies executor access to direct
File API video paths and `MODELS_DIR`. This is an accessibility check, not
resource discovery and not part of measured inference wall time. PREPARE emits
structured start/success progress records around validation, source probing,
executor preflight, input writes, and the final event write so a stalled
activity identifies its blocking stage.

Notebook 15 then runs in one Notebook activity. The activity passes both
session configuration parameters for its first `%%configure` cell and normal
notebook parameters. `CPUS_PER_TASK` must be the same pipeline parameter for
both surfaces. Retries remain disabled.
The running session value remains authoritative. If Fabric retains
`spark.task.cpus=1` despite a parameterized `%%configure` request, use `1` and
enforce memory-safe concurrency through sequential global task batches rather
than claiming the scheduler allocation changed. Retries remain disabled.

The evaluator runs with an **On completion** dependency so failed inference
still produces diagnostics. It reads the exact batch and configuration identity
from notebook-15 input, plan, record, resource, and run-metric tables. A report
refresh may run after evaluation, but a conditional Fail activity must restore
the gate failure as the pipeline result.

### 14.3 Notebook-15 integration

Notebook 15 implements:

1. an `INPUT_BATCH_ID` parameter;
2. mandatory filtering by that batch ID before input counting and planning in
   pipeline mode;
3. persisted `benchmark_batch_id`, `capacity_sku`, `runtime_version`, and
   deterministic inference-configuration hash on every evidence surface;
4. exact identity propagation to pre/post resource snapshots; and
5. explicit rejection of blank, duplicate, or cross-configuration evidence.

The batch identity must not be inferred from timestamps. A failed or cancelled
activity gets a new batch ID on rerun even when Delta transaction identities
would suppress duplicate visible writes.

### 14.4 Gate and baseline comparison

Approval requires:

- prepared count equal to submitted item count;
- exactly one successful terminal video result per prepared `work_id`;
- no failures, missing results, duplicates, resumed batches, or assumed
  durations;
- complete observed resource snapshots without assumed executors;
- matching requested and effective task CPU allocations;
- passed memory and native-thread safety checks;
- observed concurrency no greater than planned concurrency;
- a reviewed minimum sustained wall time;
- successful source-video throughput above the configured requirement; and
- optional expected line counts matching for samples that provide them.

An optional `BASELINE_BENCHMARK_BATCH_ID` selects a notebook-04 benchmark. The
evaluator must first prove that both runs use the same video multiset, pipeline
settings, model identity, runtime, and capacity label. Comparison uses
end-to-end aggregate speed:

```text
speed_x = successful_source_video_seconds / aggregate_wall_seconds
improvement_percent = 100 * (notebook_15_speed_x / notebook_04_speed_x - 1)
```

Do not compare notebook 15 with a single worker's SDK processing time.

### 14.5 Matrix execution

The optional suite pipeline runs child pipelines sequentially for:

- `CPUS_PER_TASK` values 1, 2, and 4;
- `PARTITION_WAVES` values 1, 2, 3, and 4;
- auto concurrency and reviewed explicit caps;
- representative short, common, long, and skewed mixes; and
- repeated runs under fixed capacity and runtime settings.

Matrix members must not overlap on the same Fabric capacity. Concurrent matrix
runs would contaminate resource discovery, throttling, and throughput
comparisons.

Detailed operator parameters, activity dependencies, item examples, and gate
rules are documented in section 7.4.8 of
`notebooks/fabric/README.md`.
