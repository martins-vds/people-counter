# Fabric performance assessment and implementation checklist

## Purpose

This document records the 2026-09-26 review of the Microsoft Fabric
people-counting performance plan, the repository implementation, the deployed
Lakehouse benchmark schema, and the supplied Fabric Environment configuration.
It complements the broader
[Fabric inference performance improvement plan](./fabric-inference-performance-plan.md)
by identifying what is already implemented and giving the next agent an
ordered, checkable implementation and validation sequence.

Do not approve a change because it improves one short-video activity. A
performance change is accepted only when it:

1. preserves inference and line-counting correctness;
2. improves sustained aggregate throughput or materially reduces measured
   startup cost;
3. remains stable under the six-hour capacity test; and
4. fits the approved Fabric capacity and cost envelope.

## Executive assessment

Most of the high-value structural work in the original plan is already present
in the repository:

- benchmark runtime-label validation and grouping-key consistency;
- detailed benchmark timing fields and schema evolution;
- pinned offline model loading;
- typed model runtime loading and sequential runtime reuse;
- bounded multi-video production workers;
- explicit CPU thread budgeting;
- an opt-in Spark executor-partition prototype.

The most important remaining issue is deployment drift. On 2026-09-26, the
deployed Lakehouse SQL endpoint exposed only the original 18 columns in
`dbo.people_counter_processing_benchmarks`; it did not expose the newer phase
timings, thread settings, artifact mode, or Spark application ID. The table
also returned zero rows. The deployed schema must be upgraded and a smoke run
must be recorded before further tuning can produce trustworthy evidence.

The current inference architecture executes PyTorch in the notebook Python
process on the Spark driver. In that mode:

- the 4-core driver is the inference machine;
- increasing worker nodes or executor count does not add inference CPU;
- dynamic allocation of 1-5 executors is technically valid but does not
  accelerate the current inference loop;
- the meaningful near-term experiments are runtime amortization, worker/thread
  concurrency, detector batch size, and a larger driver;
- executor scaling becomes useful only after inference is moved into coarse
  Spark partitions.

The required aggregate target remains:

```text
200,000 video-hours * 1.20
-------------------------------- = 416.67x real time
30 days * 24 hours * 0.80
```

If optimized driver-local CPU execution remains far below `416.67x`, stop
micro-tuning it and proceed to executor-partition inference or GPU workers.

## Evidence reviewed

### Supplied Fabric Environment configuration

The supplied Environment screenshots showed:

| Setting | Observed value |
|---|---|
| Environment | `people-counter-dev` |
| Node family | Auto, memory optimized |
| Node size | Small |
| Pool nodes | 1-6 |
| Driver | 4 cores, 28 GB |
| Executor | 4 cores, 28 GB |
| Dynamic executor allocation | Enabled |
| Executor range | 1-5 |
| Live pool | On |
| Maximum live clusters | 2 |
| Live-pool schedule | On, daily |
| Schedule start shown | 9:00 AM |

These values are coherent for a six-node Spark cluster: one driver and up to
five executor nodes. They are not optimized for driver-local inference because
the additional executor nodes do not execute the model.

### Repository implementation status

| Original phase | Status | Evidence | Remaining work |
|---|---|---|---|
| Phase 0: repair benchmark correctness | Implemented | [`08_capacity_benchmark.ipynb`](../notebooks/fabric/08_capacity_benchmark.ipynb) rejects blank, null, `UNSET`, `none`, and `null` runtime labels and uses consistent grouping keys. | Deploy and validate. |
| Phase 1: timing instrumentation | Implemented in source | [`00_bootstrap_lakehouse.ipynb`](../notebooks/fabric/00_bootstrap_lakehouse.ipynb) defines the phase fields, and [`08_capacity_benchmark.ipynb`](../notebooks/fabric/08_capacity_benchmark.ipynb) records them. | Deploy schema evolution. Add model-staging and submission-to-session-ready timing. |
| Phase 2: offline artifacts | Implemented | [`model_artifacts.py`](../src/people_counter/model_artifacts.py) and the pipeline loaders require and validate local artifacts. | Avoid copying unused model variants and measure copy cost. |
| Phase 3: runtime reuse | Implemented | [`api.py`](../src/people_counter/api.py), [`rtdetr_osnet.py`](../src/people_counter/pipelines/rtdetr_osnet.py), [`rfdetr_botsort.py`](../src/people_counter/pipelines/rfdetr_botsort.py), and [`04_process_video.ipynb`](../notebooks/fabric/04_process_video.ipynb) support sequential reuse. | Make the benchmark pipeline submit grouped multi-video workers. Increase useful work per production runtime load. |
| Phase 4: CPU concurrency | Implemented | [`cpu_runtime.py`](../src/people_counter/cpu_runtime.py) budgets OpenMP, MKL, OpenBLAS, PyTorch, and OpenCV threads. | Validate actual notebooks per Spark application and run the thread matrix. |
| Phase 5: batching and sampling | Supported but not validated | Pipeline configuration supports batch size and sampling rate. | Run the batch matrix and labeled accuracy comparison. |
| Phase 6: source staging | Not optimized | Production staging is synchronous in [`04_process_video.ipynb`](../notebooks/fabric/04_process_video.ipynb). | Optimize only if measured staging exceeds 10% of warm processing time. |
| Phase 7: executor distribution | Prototype implemented | [`15_executor_partition_inference.ipynb`](../notebooks/fabric/15_executor_partition_inference.ipynb) and [`fabric_executor_partition.py`](../src/people_counter/fabric_executor_partition.py). | Benchmark, then integrate with the production control plane only if it wins. |

### Deployed benchmark state

The deployed Lakehouse SQL endpoint was queried on 2026-09-26.

Observed deployed columns:

```text
benchmark_id
benchmark_batch_id
benchmark_started_at
completed_at
capacity_sku
runtime_version
sdk_version
config_sha256
sample_name
video_duration_seconds
end_to_end_seconds
overhead_seconds
processing_seconds
speed_x_realtime
peak_memory_mb
concurrent_workers
succeeded
error_message
```

Missing deployed columns included:

```text
source_stage_seconds
runtime_load_seconds
video_processing_seconds
result_persist_seconds
sampled_frames
artifact_mode
driver_cores
active_workers_per_driver
threads_per_worker
interop_threads_configured
spark_application_id
```

The endpoint returned zero benchmark rows. This prevents an evidence-based
recommendation of final concurrency, driver size, or batch size.

The SQL analytics endpoint can lag a Delta schema update. After running
bootstrap, validate the schema in Spark first, then confirm that the SQL
endpoint reflects it.

## Recommended Spark and Environment settings

### Current driver-local implementation

| Setting | Recommended value or experiment | Expected effect | Caveat |
|---|---|---|---|
| Custom pool | Keep | Supports pinned native and Python dependencies and a custom live pool. | Starter pools are attractive only when their package and configuration constraints fit the workload. |
| Live pool | Keep `On` | Reduces compatible Spark session startup. | It must be hydrated and within its active schedule. |
| Environment libraries | Publish production dependencies in Full mode | Avoids per-session package installation and gives a stable dependency snapshot. | Do not leave production-only dependencies in Quick mode or install them with `%pip` at runtime. |
| Live-pool schedule | Start 60-90 minutes before latency-sensitive work; cover the full backfill window | Improves the chance that an available cluster exists before the first activity. | A scheduled start is not proof that hydration has completed. |
| Maximum live clusters | Test `1`; keep `2` only when monitoring proves two warm clusters are required | Can reduce idle warm-capacity use and contention on F8. | One cluster may force a second incompatible session to provision on demand. |
| Driver | Keep 4 cores/28 GB as baseline; A/B test 8 cores/56 GB on a Medium pool | More driver CPU and room for larger detector batches. | More memory alone does not improve CPU inference. Require measured CPU utilization and throughput improvement. |
| Executor | Keep the smallest supported profile while inference remains driver-local | Preserves capacity for the driver or other sessions. | Executor sizing matters again when inference is distributed. |
| Dynamic allocation | Benchmark minimum `1`, maximum `1` | Avoids executor growth that does not help driver-local inference. | Retain a larger range only when substantial work actually executes as Spark tasks. |
| Pool node range | Do not raise above 1-6 for the current design | Avoids a scale-out change with no driver-inference benefit. | Tighten only after observing actual shared-pool demand. |
| High concurrency | Keep for compatible session reuse, but measure contention | Reduces admission/startup overhead for compatible notebooks. | It does not add driver cores. Concurrent Python inference shares the same driver when application IDs match. |
| Native Execution Engine | Enable and profile for supported Spark SQL/Delta work | Can improve scans, filters, joins, and aggregations. | It does not accelerate arbitrary driver-side PyTorch code. |
| Adaptive Query Execution | Keep enabled for normal table workloads | Helps normal Spark SQL execution. | The executor-partition prototype may disable it to preserve deliberate partition parallelism. |

### CPU thread matrix

For each row, record the number of distinct `spark_application_id` values and
the number of simultaneous notebooks attached to each application.

| Driver | Active inference workers per driver | Threads per worker |
|---|---:|---:|
| Small, 4 cores | 1 | 4 |
| Small, 4 cores | 2 | 2 |
| Small, 4 cores | 3 | 1 |
| Small, 4 cores | 4 | 1 |
| Medium, 8 cores | 1 | 8 |
| Medium, 8 cores | 2 | 4 |
| Medium, 8 cores | 4 | 2 |

Select the best sustained aggregate throughput, not the shortest isolated
notebook activity.

### Detector matrix

Run the following only after runtime reuse and thread allocation are verified:

| Parameter | Required experiments | Promotion rule |
|---|---|---|
| `BATCH_SIZE` | `1`, `2`, `4`; test `8` only with adequate memory | Promote only when aggregate throughput improves and outputs remain equivalent within the accepted tolerance. |
| `SAMPLE_FPS` | `3`, `2`, `1` | Do not lower from 3 FPS without labeled accuracy and line-crossing acceptance gates. |
| Driver size | Small 4-core versus Medium 8-core | Promote Medium only when throughput gain justifies additional capacity consumption. |

## Remaining performance opportunities

### Make the benchmark exercise runtime reuse

The exported
[`pc-capacity-benchmark.json`](../notebooks/fabric/exports/pc-capacity-benchmark.json)
defaults to four workers but contains only three example items. It also passes
an empty `BENCHMARK_ITEMS_JSON`, which normally produces one notebook activity
and one runtime load per video.

For a production-equivalent benchmark:

- make `BENCHMARK_ITEMS` an array of worker shards;
- pass each shard through `BENCHMARK_ITEMS_JSON`;
- place several videos in each worker shard;
- keep at least the configured number of worker shards continuously occupied;
- retain one benchmark row per video;
- set `EXPECTED_BATCH_MEMBERS` to the total number of videos, not activities.

### Amortize production worker initialization

The exported
[`pc-dispatcher-00.json`](../notebooks/fabric/exports/pc-dispatcher-00.json)
defaults to `CLAIM_LIMIT=4`. Four short videos may provide very little useful
work per Spark admission, model copy, and runtime load.

Benchmark progressively larger limits such as `8`, `16`, `32`, and `64`.
Prefer evolving the claim contract toward both:

- a maximum item count; and
- a maximum expected source-duration budget.

The goal is normally 30-60 minutes of source video per worker invocation while
keeping retry scope, lease renewal, local disk use, and activity timeout
bounded.

### Measure and reduce model staging

The local RT-DETR/OSNet artifact tree inspected during this review was
approximately 243 MB because it contained both R18 and R50 detector weights.
An R18 worker should not copy R50 weights.

Add a separate `model_stage_seconds` measurement, then package or select only
the artifacts required by the requested pipeline and detector variant. Do not
remove revision checks or permit an online fallback.

### Remove avoidable per-video Spark actions

The production worker builds telemetry and line-count records as Python lists,
then creates Spark DataFrames. Avoid triggering Spark actions solely to
rediscover information already available in those lists:

- use the Python list length for emptiness and row counts;
- avoid `take(1)` followed by a write and then `count()` for the same result;
- preserve the existing idempotent Delta transaction identifiers;
- batch benchmark-row persistence where it does not weaken failure visibility.

### Optimize source staging only when measured

If source staging exceeds 10% of warm end-to-end time:

1. calculate SHA-256 while copying instead of rereading the staged file;
2. test direct OpenCV reads from the supported OneLake mount;
3. if copying remains preferable, prefetch exactly one next video while the
   current video is in inference;
4. keep inference sequential for one runtime;
5. cap temporary disk use and preserve source validation.

### Use executors only for real distributed inference

If the optimized driver design is insufficient, benchmark the existing
executor-partition prototype with whole-video, coarse partitions:

- one runtime per executor Python worker/cache key;
- several videos processed sequentially per partition;
- no splitting of one video's temporal tracking across partitions;
- explicit `spark.task.cpus`;
- compact records returned to controlled Delta persistence;
- fixed or promptly available executors during the sustained benchmark.

Do not promote the prototype to production until it preserves the existing
claim, attempt, heartbeat, retry, idempotency, and commit-pointer semantics.

## Agent implementation checklist

### Completion rules

For every checked item:

- record the commit, Fabric item version, or configuration screenshot;
- record the benchmark batch ID where performance was measured;
- attach the relevant Spark application IDs and Capacity Metrics interval;
- record correctness results, not only timing;
- do not check an item based only on source-code presence when deployment or
  runtime validation is part of the acceptance criterion.

### Gate 0: preserve the baseline

- [ ] Record the currently published Environment version, Runtime version,
  pool configuration, library mode, notebook versions, SDK version, and model
  artifact revisions.
- [ ] Export or otherwise preserve the current pipeline and Environment
  configuration before changing settings.
- [ ] Record an uncontended one-worker Small-driver baseline.
- [ ] Record submission-to-session-ready, model-stage, runtime-load,
  source-stage, inference, persistence, and total activity duration separately.
- [ ] Run the existing correctness suite on the representative labeled videos.
- [ ] Record baseline unique-person counts, line counts, and any accepted
  tolerance for timing or identity differences.

**Gate 0 acceptance:** The baseline has reproducible configuration, timing,
resource, and correctness evidence.

### Gate 1: deploy the implemented schema and notebook changes

- [ ] Publish the current SDK bundle and pinned dependencies to the Fabric
  Environment.
- [ ] Publish the Environment and record its version.
- [ ] Publish the updated Fabric notebooks, especially bootstrap, production
  worker, and capacity benchmark.
- [ ] Run
  [`00_bootstrap_lakehouse.ipynb`](../notebooks/fabric/00_bootstrap_lakehouse.ipynb)
  using the documented writer-stop safeguards.
- [ ] Verify all new benchmark columns through Spark.
- [ ] Verify all new benchmark columns through the SQL analytics endpoint
  after metadata synchronization.
- [ ] Run one short successful benchmark worker.
- [ ] Confirm that it writes non-null phase timings, thread settings, artifact
  mode, and Spark application ID where applicable.
- [ ] Run one intentional invalid-runtime-label case and confirm it fails
  before inference.
- [ ] Confirm the capacity gate observes the exact worker row.

**Gate 1 acceptance:** The deployed schema matches the repository, at least one
valid benchmark row exists, and invalid grouping labels cannot produce a
misleading zero-row gate.

### Gate 2: harden cold-start configuration

- [ ] Confirm every production dependency is pinned in the Environment.
- [ ] Confirm production dependencies are in Full mode.
- [ ] Remove production `%pip` installation and unnecessary Quick-mode
  dependencies.
- [ ] Confirm all cooperating notebooks use the same published Environment,
  Runtime, default Lakehouse, and compatible Spark configuration.
- [ ] Start the live-pool schedule 60-90 minutes before the test window.
- [ ] Verify an available hydrated live cluster in Monitoring Hub before the
  pipeline starts.
- [ ] Measure a warm live-pool start and an on-demand cold start separately.
- [ ] Test maximum live clusters `1` versus `2`.
- [ ] Select `2` only if two simultaneous/incompatible sessions improve
  sustained aggregate throughput enough to justify the capacity use.
- [ ] Prevent or skip `ProcessVideo` notebook execution when `ClaimWork`
  returns no items, if the pipeline currently starts it for empty claims.

**Gate 2 acceptance:** Warm session acquisition is measured, production
sessions perform no package installation, and the selected live-cluster count
has a documented utilization reason.

### Gate 3: make the capacity benchmark production-equivalent

- [ ] Replace the one-video-per-activity test shape with grouped worker shards.
- [ ] Pass each shard through `BENCHMARK_ITEMS_JSON`.
- [ ] Keep every configured worker slot occupied for the full test interval.
- [ ] Keep one independent benchmark row per video.
- [ ] Calculate `EXPECTED_BATCH_MEMBERS` from total videos across all shards.
- [ ] Verify one runtime load serves every compatible item in one shard.
- [ ] Verify the first item carries runtime-load time and later items report
  zero runtime-load time.
- [ ] Verify retries or worker failures cannot duplicate successful benchmark
  members.
- [ ] Record actual concurrent activities and distinct Spark application IDs.

**Gate 3 acceptance:** The benchmark measures the same runtime-reuse behavior
used by production and can prove actual rather than configured concurrency.

### Gate 4: increase useful work per production worker

- [ ] Measure current `CLAIM_LIMIT=4` worker amortization.
- [ ] Benchmark claim limits `8`, `16`, `32`, and `64`, stopping when another
  increase no longer improves aggregate throughput.
- [ ] Record source-video minutes, model loads, local disk peak, and retry scope
  per worker.
- [ ] Add an expected source-duration budget to claims if item count does not
  control runtime consistently.
- [ ] Keep the maximum worker lifetime below the pipeline activity timeout.
- [ ] Verify lease heartbeats cover queued items and long inference.
- [ ] Verify a later item failure does not discard or duplicate earlier
  committed items.
- [ ] Verify unstarted items are safely released when the lifetime limit is
  reached.

**Gate 4 acceptance:** One worker normally processes 30-60 minutes of source
video or the largest safe measured amount, with one runtime load and preserved
item-level idempotency.

### Gate 5: eliminate avoidable per-video overhead

- [ ] Add `model_stage_seconds` to worker and benchmark telemetry.
- [ ] Record notebook submission-to-first-code time from Fabric monitoring.
- [ ] Package or copy only the selected detector variant and shared ReID
  artifacts.
- [ ] Verify artifact completeness and pinned revisions before inference.
- [ ] Replace DataFrame `take(1)` and `count()` actions where Python collection
  lengths already provide the same answer.
- [ ] Preserve empty-output behavior and Delta idempotency after removing
  actions.
- [ ] Measure Delta append overhead before and after the change.
- [ ] Consider batching benchmark row writes only if each failed video remains
  visible and the expected-member gate remains exact.

**Gate 5 acceptance:** Initialization and per-video Spark overhead are lower,
all failures remain explicit, and output rows remain identical.

### Gate 6: tune driver resources and CPU concurrency

- [ ] With driver-local inference, benchmark dynamic allocation `1-1`.
- [ ] Confirm reducing from `1-5` does not lower inference throughput.
- [ ] Record executor utilization and capacity consumption for both settings.
- [ ] Run the complete Small-driver worker/thread matrix.
- [ ] Use `spark_application_id` to calculate actual workers per driver.
- [ ] Set `ACTIVE_WORKERS_PER_DRIVER` from observed sharing, not pipeline
  metadata alone.
- [ ] Create a comparable Medium-pool Environment.
- [ ] Run the Medium 8-core/56-GB matrix.
- [ ] Record CPU utilization, peak memory, model-load time, aggregate speed,
  p10 speed, failures, queue time, and CU consumption.
- [ ] Keep Medium only if measured throughput per cost is acceptable.
- [ ] Confirm `interop_threads_configured=true` for every accepted CPU row.

**Gate 6 acceptance:** The selected driver, worker count, and thread budget
produce the best measured sustained aggregate throughput without
oversubscription or unacceptable capacity cost.

### Gate 7: tune detector batching and sampling

- [ ] Run `BATCH_SIZE=1`.
- [ ] Run `BATCH_SIZE=2`.
- [ ] Run `BATCH_SIZE=4`.
- [ ] Run `BATCH_SIZE=8` only if memory headroom remains safe.
- [ ] Compare detector FPS, video speed, aggregate speed, memory, counts, and
  failures for every batch.
- [ ] Promote a batch size only when correctness remains within the accepted
  tolerance.
- [ ] Establish labeled unique-person and line-crossing acceptance thresholds.
- [ ] Run `SAMPLE_FPS=3` as the accuracy baseline.
- [ ] Run `SAMPLE_FPS=2`.
- [ ] Run `SAMPLE_FPS=1`.
- [ ] Keep 3 FPS if lower rates fail the accuracy gate or no defensible
  accuracy gate exists.

**Gate 7 acceptance:** Batch size and sampling rate are selected from measured
throughput and labeled correctness evidence.

### Gate 8: optimize source staging if material

- [ ] Calculate source-stage percentage for warm workers.
- [ ] Stop this gate if staging is at most 10% of warm end-to-end time.
- [ ] If material, implement hash-while-copy and verify identical hashes.
- [ ] Benchmark direct supported OneLake mount reads with OpenCV.
- [ ] If copying remains preferable, add one bounded prefetch slot.
- [ ] Verify prefetch performs no concurrent model inference.
- [ ] Cap local disk usage and clean files after each committed item.
- [ ] Verify retries do not recopy an already validated local source when safe
  reuse is available.

**Gate 8 acceptance:** Staging is at most 10% of warm time or the selected
staging design is the fastest validated option without weakening integrity.

### Gate 9: evaluate executor-partition scale-out

- [ ] Prepare one row per whole video for the executor prototype.
- [ ] Stage pinned offline artifacts where every executor can access them.
- [ ] Benchmark one active task per 4-core executor with
  `spark.task.cpus=4`.
- [ ] Benchmark two active tasks per executor with `spark.task.cpus=2`.
- [ ] Benchmark four active tasks per executor with `spark.task.cpus=1`.
- [ ] Confirm one model runtime is reused for several compatible videos in
  each executor Python worker.
- [ ] Confirm no video is split across temporal partitions.
- [ ] Record executor CPU, memory, task time, model loads, aggregate speed,
  serialization overhead, and failures.
- [ ] Compare executor-partition throughput and cost with the best driver
  configuration.
- [ ] If the executor design wins, produce a separate migration plan that
  preserves claim, attempt, heartbeat, retry, idempotency, and commit semantics.
- [ ] Do not connect the prototype directly to production output tables before
  that migration plan and its tests are approved.

**Gate 9 acceptance:** Executor scale-out has a measured advantage and an
approved production integration design, or it is explicitly rejected with
benchmark evidence.

### Gate 10: sustained capacity decision

- [ ] Run the selected CPU architecture continuously for at least six hours.
- [ ] Use a unique benchmark batch ID and exact Runtime label.
- [ ] Confirm exact expected-member count and zero failed members.
- [ ] Confirm all configured worker slots remained occupied.
- [ ] Confirm `best_six_hour_aggregate_speed_x >= 416.67`.
- [ ] Confirm no sustained Fabric throttling or unbounded queue growth.
- [ ] Record average and peak CU usage and estimated 30-day cost.
- [ ] Run the labeled correctness suite against the accepted baseline.
- [ ] If CPU fails the target or cost envelope, stop additional CPU
  micro-tuning and open the GPU/distributed architecture work.
- [ ] Publish the final accepted Environment, pool, Spark, worker, model, batch,
  and sampling configuration in the deployment documentation.

**Gate 10 acceptance:** A six-hour run proves the required throughput,
correctness, stability, and cost, or the evidence formally directs the project
to a different compute architecture.

## Evidence worksheet

The implementing agent should complete one row for every promoted
configuration.

| Field | Value |
|---|---|
| Benchmark batch ID | |
| Commit / SDK version | |
| Fabric Environment version | |
| Fabric Runtime | |
| Capacity SKU | |
| Pool and node size | |
| Driver cores / memory | |
| Executor cores / memory | |
| Dynamic executor range | |
| Live clusters | |
| Concurrent activities | |
| Distinct Spark applications | |
| Active workers per driver | |
| Threads per worker | |
| Claim limit / source-duration budget | |
| Detector and revision | |
| Batch size | |
| Sample FPS | |
| Total source-video hours | |
| Wall-clock hours | |
| Aggregate speed | |
| p10 per-video speed | |
| Source-stage percentage | |
| Model-stage and runtime-load time | |
| Peak driver / executor memory | |
| Average / peak CU | |
| Failed / retried members | |
| Correctness result | |
| Decision | |

## Official Microsoft Fabric references

- [Apache Spark compute](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-compute)
- [Custom live pools overview](https://learn.microsoft.com/en-us/fabric/data-engineering/custom-live-pools-overview)
- [Configure custom live pools](https://learn.microsoft.com/en-us/fabric/data-engineering/custom-live-pools-configure)
- [Manage libraries in Fabric environments](https://learn.microsoft.com/en-us/fabric/data-engineering/environment-manage-library)
- [High-concurrency sessions](https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-overview)
- [Spark basics and best practices](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-best-practices-basics)
- [Spark capacity and cluster planning](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-best-practices-capacity-planning)
- [Choosing a notebook kernel](https://learn.microsoft.com/en-us/fabric/data-engineering/fabric-notebook-selection-guide)
- [Native Execution Engine](https://learn.microsoft.com/en-us/fabric/data-engineering/native-execution-engine-overview)
- [Fabric Runtime 2.0](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0)
- [Spark session configuration](https://learn.microsoft.com/en-us/fabric/data-engineering/author-execute-notebook#spark-session-configuration-magic-command)
