# Fabric inference performance improvement plan

## Purpose

This document is an implementation plan for improving the throughput of the
Microsoft Fabric video-processing and capacity-benchmark workloads. It records
the evidence from the 2026-09-24 `pc-capacity-benchmark` run, defines the
required code and notebook changes, and gives acceptance criteria that another
agent can use to implement and validate the work.

This is not a recommendation to make every change at once. Implement the phases
in order, measure after each phase, and stop pursuing an optimization when it
does not improve aggregate throughput or when it violates correctness.

## Baseline and diagnosis

The observed run used:

- capacity SKU `F8`;
- Fabric Runtime 2.0 (Spark 4.1, Delta 4.2);
- a Small driver with 4 vCores and 28 GB memory;
- a Small executor profile with 4 vCores and 28 GB memory;
- dynamic allocation of 1-5 executors;
- `rtdetr-osnet`, detector `r18`, CPU, FP32;
- `BATCH_SIZE=1`;
- `SAMPLE_FPS=3`;
- three benchmark items and a configured concurrency value of four.

All three activities started at 3:22:30 PM, so the `ForEach` did execute them
concurrently. The parent duration approximately equaled the slowest child:

| Sample | Source duration | Approximate sampled frames | Activity duration |
|---|---:|---:|---:|
| `three_people_walking` | 7 seconds | 21 | 9m 19s |
| `subway` | 31 seconds | 93 | 12m 44s |
| `people_crossing` | 68 seconds | 204 | 15m 44s |
| `ForEachBenchmarkItems` | n/a | n/a | 15m 48s |

The Spark application reported approximately 1m 58s of queue time. One
notebook created the Spark session and the other two attached to the same
application. The three CPU inference workloads therefore shared one four-core
driver. The Spark executors did not distribute the inference loop: the
notebook calls the local Python SDK directly and uses Spark only for table
operations.

The benchmark also used a null `RUNTIME_VERSION`. The workers wrote null
runtime keys and the gate compared them with normal Spark equality, for which
`NULL == NULL` does not evaluate to true. The gate consequently observed zero
rows. Fix this correctness issue before using benchmark results to approve a
configuration.

## Performance target

The configured capacity target is 200,000 video-hours in 30 days, with 20%
headroom and 80% utilization. The required aggregate speed is:

```text
200,000 * 1.2 / (30 * 24 * 0.8) = 416.67 times real time
```

Every benchmark report must compare measured sustained aggregate speed with
`416.67x`. A design that only improves single-video latency but remains orders
of magnitude below this requirement must not be presented as sufficient for
the backfill.

## Goals

1. Make benchmark selection and measurements correct and reproducible.
2. Eliminate model downloads from normal worker execution.
3. Separate cold-start, staging, model-load, inference, and persistence costs.
4. Reuse loaded model runtimes across multiple videos in a worker.
5. Prevent CPU oversubscription when notebooks share a Spark application.
6. Tune detector batching and sampling only with correctness checks.
7. Establish whether optimized CPU execution can meet the target.
8. If CPU execution cannot meet the target, produce a measured GPU or
   distributed-executor alternative.

## Non-goals

- Do not weaken the six-hour sustained-throughput gate.
- Do not lower `EXPECTED_BATCH_MEMBERS` to make an incomplete run pass.
- Do not infer capacity from short-video latency.
- Do not count dynamically allocated Spark executors as inference capacity
  unless inference has actually been moved onto those executors.
- Do not change the detector or sampling rate without an accuracy comparison.
- Do not silently fall back to online model downloads when configured offline
  artifacts are missing.

## Phase 0: repair benchmark correctness

### Required changes

Update
[`notebooks/fabric/08_capacity_benchmark.ipynb`](../notebooks/fabric/08_capacity_benchmark.ipynb):

1. Reject a null, empty, `UNSET`, `none`, or `null` `RUNTIME_VERSION` for both
   worker and gate executions. The value is an operator-supplied grouping key,
   not an auto-detected value.
2. Keep exact equality for `runtime_version` after validation. Do not use
   null-safe equality to legitimize an unspecified runtime.
3. Include all active grouping keys in the failure message:
   `benchmark_batch_id`, `capacity_sku`, `runtime_version`, `sdk_version`,
   `config_sha256`, and `concurrent_workers`.
4. Preserve the exact-member, zero-failure, six-hour, and aggregate-throughput
   requirements.

Update
[`notebooks/fabric/README.md`](../notebooks/fabric/README.md):

1. Add an explicit pre-run check that `RUNTIME_VERSION` is non-empty.
2. State that `CONCURRENT_WORKERS` is metadata and must equal the literal
   `ForEach` batch count.
3. State that the item array must contain enough work to keep every configured
   slot occupied for at least six hours.

Update
[`tests/test_fabric_notebooks.py`](../tests/test_fabric_notebooks.py):

- test rejection of empty and null runtime labels;
- test that a valid label is retained in the benchmark row;
- test that worker and gate filtering use the same grouping keys;
- retain tests for the six-hour and expected-member gates.

### Acceptance criteria

- A run with a blank runtime fails before inference with an actionable error.
- A run with an exact runtime label observes every row written by its workers.
- Gate output prints every grouping key needed to diagnose zero matches.
- Existing notebook contract tests pass.

## Phase 1: add actionable timing instrumentation

The existing benchmark row records `end_to_end_seconds`,
`processing_seconds`, and their difference as `overhead_seconds`. That is not
enough to distinguish source staging from model initialization. Also,
`processing_seconds` starts after model loading, while the Fabric activity
duration additionally includes Spark admission and notebook startup.

### Schema changes

Extend the `processing_benchmarks` schema in
[`notebooks/fabric/00_bootstrap_lakehouse.ipynb`](../notebooks/fabric/00_bootstrap_lakehouse.ipynb)
with nullable columns:

| Column | Type | Meaning |
|---|---|---|
| `source_stage_seconds` | `DOUBLE` | OneLake existence check and copy to local storage |
| `runtime_load_seconds` | `DOUBLE` | RT-DETR processor/model and ReID runtime construction |
| `video_processing_seconds` | `DOUBLE` | SDK-reported decode, inference, tracking, and line-counting time |
| `result_persist_seconds` | `DOUBLE` | Time to persist the benchmark result, when available |
| `sampled_frames` | `BIGINT` | Frames actually passed through the detector |
| `artifact_mode` | `STRING` | `offline`, `cache`, or `remote`; normal production runs must be `offline` |
| `driver_cores` | `INT` | Explicit operator-supplied or detected driver-core count |
| `threads_per_worker` | `INT` | Configured PyTorch intra-op thread count |

Add the columns to both the create-table definition and the existing-table
schema-evolution map. New columns must remain nullable so existing Delta rows
remain readable.

### Notebook timing changes

In
[`notebooks/fabric/08_capacity_benchmark.ipynb`](../notebooks/fabric/08_capacity_benchmark.ipynb):

1. Use `time.perf_counter()` around source staging.
2. Time runtime construction separately from per-video processing.
3. Record the SDK's processed-frame count.
4. Preserve the current end-to-end timer.
5. Print one compact JSON timing summary for every worker.
6. If the benchmark row cannot be persisted, fail the notebook explicitly.

The Delta write duration cannot be included in the row being written without a
second operation. Prefer a post-write `MERGE` keyed by `benchmark_id`; if that
cost is excessive, leave `result_persist_seconds` null and report it only in
the notebook output. Do not issue a second append that creates a duplicate
benchmark member.

Fabric admission and Spark startup occur before notebook code. Record those
from Fabric monitoring or pipeline activity metadata; do not mislabel them as
SDK overhead.

### Acceptance criteria

- For a successful worker:

  ```text
  source_stage_seconds
  + runtime_load_seconds
  + video_processing_seconds
  <= end_to_end_seconds
  ```

- The unexplained remainder is printed and is less than 10% of notebook
  end-to-end time after excluding Spark admission/startup.
- `sampled_frames` matches the SDK result.
- Old benchmark rows remain queryable.
- Bootstrap remains idempotent.

## Phase 2: use pinned offline model artifacts

The repository already provides:

- [`scripts/download_models.py`](../scripts/download_models.py), which
  downloads pinned artifacts;
- [`src/people_counter/model_artifacts.py`](../src/people_counter/model_artifacts.py),
  which validates the offline layout;
- `models_dir` on the SDK configuration types in
  [`src/people_counter/config.py`](../src/people_counter/config.py).

### Required changes

1. Add a `MODELS_DIR` string parameter to:
   - [`notebooks/fabric/08_capacity_benchmark.ipynb`](../notebooks/fabric/08_capacity_benchmark.ipynb);
   - [`notebooks/fabric/04_process_video.ipynb`](../notebooks/fabric/04_process_video.ipynb).
2. Require `MODELS_DIR` for production and capacity-gate runs.
3. Convert it to `Path` and pass it to `RTDetrOsnetConfig` or
   `RFDetrBotsortConfig`.
4. Fail with the existing precise missing-file errors when artifacts are
   incomplete.
5. Publish the pinned artifact tree into a versioned Fabric Environment
   resource or controlled OneLake location during deployment.
6. If the artifact tree resides on OneLake, copy it once to worker-local
   storage before runtime construction. Do not copy it once per video in a
   persistent worker.
7. Record `artifact_mode="offline"`.
8. Do not catch an offline-artifact error and retry against Hugging Face.

### Tests

Extend:

- [`tests/test_model_artifacts.py`](../tests/test_model_artifacts.py);
- [`tests/test_download_models.py`](../tests/test_download_models.py);
- [`tests/test_pipeline_components.py`](../tests/test_pipeline_components.py);
- [`tests/test_fabric_notebooks.py`](../tests/test_fabric_notebooks.py).

Cover:

- complete offline layouts for both pipelines;
- missing and corrupt artifacts;
- passing `models_dir` from each notebook;
- absence of `from_pretrained` network fallback when `models_dir` is set.

### Acceptance criteria

- A production or capacity worker makes no HTTP request to Hugging Face.
- A missing artifact fails before video processing.
- The loaded detector and ReID revisions remain the pinned revisions.
- Offline and current online loading produce equivalent inference output on
  the test fixture.

## Phase 3: reuse one runtime for multiple videos

Repeated model construction per short video is the largest avoidable cost.
Model objects must be reused sequentially inside a worker process. Do not rely
on module globals being shared between Fabric high-concurrency notebook REPLs.

### SDK refactor

Refactor
[`src/people_counter/pipelines/rtdetr_osnet.py`](../src/people_counter/pipelines/rtdetr_osnet.py)
and
[`src/people_counter/pipelines/rfdetr_botsort.py`](../src/people_counter/pipelines/rfdetr_botsort.py)
as follows:

1. Keep the existing public `run(config) -> RunResult` API backward compatible.
2. Add a `run_with_runtime(config, runtime) -> RunResult` function for each
   pipeline.
3. Make `run(config)` equivalent to:

   ```python
   runtime = load_runtime(config)
   return run_with_runtime(config, runtime)
   ```

4. Ensure all per-video mutable tracking state and `RunResult` state are
   created inside `run_with_runtime`; only immutable/read-only model runtime
   objects are reused.
5. Validate that the supplied runtime matches the requested pipeline, device,
   model variant, and artifact revision.
6. Do not allow one runtime instance to be used concurrently by multiple
   threads unless thread safety is proven. Initial implementation must reuse
   it sequentially.

Update [`src/people_counter/api.py`](../src/people_counter/api.py) to expose a
typed runtime-loading and runtime-reuse API without weakening the existing
configuration union or introducing `Any` casts.

### Worker execution model

Replace one-notebook-per-short-video execution with bounded worker batches:

1. A notebook worker starts and loads one runtime.
2. It receives or claims a bounded list of videos.
3. It stages and processes videos sequentially with `run_with_runtime`.
4. It persists and commits each video independently before moving to the next.
5. It stops after either:
   - the configured item limit; or
   - the configured maximum worker lifetime.
6. A failure for one video must be recorded through the existing lease/error
   protocol and must not discard already committed videos.
7. Lease heartbeats must continue during long inference operations.

For the production path, adapt
[`notebooks/fabric/03_claim_work.ipynb`](../notebooks/fabric/03_claim_work.ipynb)
and
[`notebooks/fabric/04_process_video.ipynb`](../notebooks/fabric/04_process_video.ipynb)
rather than bypassing the existing claim, attempt, heartbeat, and commit
semantics. Use the existing idempotent attempt identifiers for each video.

For the capacity benchmark, each worker invocation must process a shard of
representative items while writing one `processing_benchmarks` row per video.
`EXPECTED_BATCH_MEMBERS` must continue to mean the number of rows expected,
not the number of notebook activities.

Start with:

- 30-60 minutes of source video per worker invocation;
- a maximum worker lifetime below the pipeline activity timeout;
- a bounded item count so a retry has limited scope.

### Tests

Add SDK tests proving:

- `run(config)` still loads exactly one runtime and preserves behavior;
- two sequential `run_with_runtime` calls load no additional models;
- tracking state, result objects, and line counts do not leak between videos;
- failures retain partial results only for the failing video;
- a runtime/config mismatch fails explicitly.

Add notebook contract tests proving:

- every claimed item receives an independent attempt and commit;
- the worker continues after a non-fatal item failure only when the existing
  retry policy permits it;
- completed items are not duplicated after a worker retry;
- one runtime load serves all items in the worker batch.

### Acceptance criteria

- Model load count is one per worker invocation, not one per video.
- Previously committed videos remain idempotent after a worker retry.
- Warm per-video overhead excludes model initialization.
- Output telemetry and line counts match the current implementation for the
  same configuration and fixture videos.

## Phase 4: control CPU concurrency

High-concurrency Fabric sessions share driver resources. Configure CPU thread
budgets explicitly so multiple workers do not each attempt to consume every
core.

### Implementation

Add a small typed CPU-runtime configuration helper in
`src/people_counter/`. It must:

1. accept `driver_cores` and `active_workers`;
2. calculate `threads_per_worker = max(1, driver_cores // active_workers)`;
3. configure OpenMP and MKL environment variables before importing PyTorch
   where deployment permits;
4. call:

   ```python
   torch.set_num_threads(threads_per_worker)
   torch.set_num_interop_threads(1)
   cv2.setNumThreads(1)
   ```

5. log the effective settings once per worker;
6. fail on impossible or contradictory values rather than silently defaulting.

If `torch.set_num_interop_threads` has already been initialized and cannot be
changed, configure it in the Fabric Environment/bootstrap path instead of
catching and ignoring the error.

### Benchmark matrix

On the current four-core driver, run at least:

| Active workers | Intra-op threads per worker |
|---:|---:|
| 1 | 4 |
| 2 | 2 |
| 3 | 1 |
| 4 | 1 |

Use enough items to keep every worker occupied. Compare:

- sustained aggregate speed;
- p10 per-video speed;
- CPU utilization;
- memory;
- admission and throttle time;
- output equivalence.

Select the configuration with the best sustained aggregate speed, not the
shortest isolated activity.

### Acceptance criteria

- Thread settings are visible in every benchmark row.
- The selected worker count has no lower aggregate throughput than the
  one-worker baseline.
- No configuration is approved based only on the `CONCURRENT_WORKERS`
  parameter; observed active workers must match.

## Phase 5: tune inference batching and sampling

### Detector batch size

Benchmark `BATCH_SIZE` values `1`, `2`, and `4`. Test `8` only if memory
headroom remains adequate. The detector already receives frame batches in
[`src/people_counter/pipelines/rtdetr_osnet.py`](../src/people_counter/pipelines/rtdetr_osnet.py).

For each batch size, record:

- detector frames per second;
- end-to-end video speed;
- driver memory;
- failures or throttling;
- person counts and line counts.

Batch-size changes must preserve output equivalence within existing numerical
tolerances.

### Sampling rate

Evaluate `SAMPLE_FPS` values `3`, `2`, and `1` on labeled representative
videos. Lower rates reduce detector work approximately in proportion to
sampled frames, but can damage tracking continuity and line-crossing accuracy.

Do not promote a lower sampling rate unless it passes an explicit accuracy
gate. If no labeled acceptance threshold exists, keep 3 FPS and create the
accuracy benchmark before changing production behavior.

### Optional CPU backend investigation

After runtime reuse and batching are implemented, compare the current PyTorch
CPU backend with:

- ONNX Runtime;
- OpenVINO;
- supported INT8 quantization.

Treat this as a separate change. Require output/accuracy comparison and pinned
artifact generation. Do not combine backend replacement with the runtime-reuse
refactor.

## Phase 6: remove source-staging bottlenecks

Use the phase metrics to decide whether source staging is material.

If staging is more than 10% of warm end-to-end time:

1. prefer a supported local OneLake mount path when OpenCV can read it
   reliably;
2. otherwise overlap staging of video `N+1` with inference of video `N` using
   one bounded prefetch slot;
3. retain local-file validation before inference;
4. cap temporary-disk usage and remove staged files after each committed item;
5. do not download the same source more than once after a retry if a validated
   local copy remains available.

Prefetching must not run model inference concurrently inside one worker.

## Phase 7: compute architecture decision

Increasing Spark executor count does not accelerate the current implementation
because inference runs in the notebook Python process. After Phases 0-6, run a
six-hour sustained CPU benchmark.

### Decision gate

Calculate:

```text
required_aggregate_speed_x =
    TARGET_VIDEO_HOURS * HEADROOM_FACTOR
    / (DEADLINE_DAYS * 24 * UTILIZATION)
```

If the optimized CPU design cannot plausibly reach the requirement within the
approved capacity and cost envelope, implement one of these alternatives.

### Preferred alternative: GPU workers

Use Fabric GPU compute where supported, or a managed GPU worker platform such
as Azure Machine Learning, Azure Batch, or AKS. Keep Fabric responsible for
work claims, OneLake data, Delta results, and monitoring.

Required work:

- permit `DEVICE_VARIANT=gpu`, a CUDA device, and `USE_FP16=true` in the
  benchmark after environment validation;
- stage pinned GPU-compatible artifacts;
- benchmark batch sizes appropriate to GPU memory;
- preserve the same result schema and correctness suite;
- record GPU model, memory, utilization, and cost.

### CPU alternative: Spark executor partitions

If the solution must remain CPU-only in Fabric, move inference to executors
with a `mapPartitions`-style design:

1. partition by whole videos;
2. initialize one model runtime per executor partition;
3. process multiple videos sequentially in that partition;
4. never split one video's temporal tracking across partitions;
5. return compact result records for controlled Delta persistence;
6. cap task CPU allocation and avoid nested thread oversubscription.

This is an architectural change and must be implemented separately from the
driver-based optimizations.

### Acceptance criteria

- The selected architecture sustains the required aggregate speed for at least
  six hours.
- Capacity Metrics show no sustained throttling or unbounded queue growth.
- The measured cost fits the approved budget.
- Correctness matches the CPU baseline on the labeled validation set.

## Benchmark protocol

Use this protocol after every performance phase:

1. Publish a new SDK version.
2. Use a unique `BENCHMARK_BATCH_ID`.
3. Set the exact non-empty `RUNTIME_VERSION`.
4. Use representative videos long enough that warm inference dominates.
5. Keep every configured worker slot occupied.
6. Run for at least six hours for approval tests.
7. Record cold and warm results separately.
8. Capture Fabric Capacity Metrics for the exact interval.
9. Compare against the previous accepted baseline.
10. Reject the change if correctness regresses or aggregate throughput does not
    improve materially.

The benchmark report must contain:

- SDK and model artifact versions;
- Fabric runtime and capacity SKU;
- driver and executor profiles;
- worker count and thread allocation;
- item count and total source duration;
- stage, runtime-load, processing, and persistence timings;
- p10 and average per-video speed;
- sustained aggregate speed;
- failed and retried item counts;
- peak memory and observed CPU/GPU utilization;
- accuracy comparison;
- estimated worker count and cost needed for the target.

## Recommended implementation order

1. Repair runtime validation and gate diagnostics.
2. Add phase timing and schema evolution.
3. Deploy pinned offline model artifacts.
4. Establish uncontended one-worker and two-worker baselines.
5. Refactor the SDK for runtime reuse.
6. Convert notebook execution to bounded multi-video workers.
7. Add explicit CPU thread budgets.
8. Tune detector batch size.
9. Evaluate lower sample rates with an accuracy gate.
10. Run the six-hour optimized CPU benchmark.
11. Proceed to GPU or distributed-executor implementation if the CPU design
    cannot meet the target.

## Definition of done

The performance work is complete only when:

- benchmark grouping cannot silently exclude rows due to null parameters;
- normal workers perform no model-network downloads;
- one worker loads one runtime and processes multiple videos;
- per-phase timings are persisted and queryable;
- CPU thread allocation matches actual worker concurrency;
- a six-hour test demonstrates stable aggregate throughput;
- accuracy and idempotency tests pass;
- the selected architecture has a documented capacity and cost result against
  the `416.67x` requirement;
- the Fabric deployment and pipeline parameter documentation reflects the
  implemented design.
