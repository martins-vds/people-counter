# Fabric executor production dispatcher implementation plan

Date: September 30, 2026

Status: Proposed; rubber-duck review incorporated and ready for implementation

Primary target: productionize the executor-partition approach behind an
explicitly routed dispatcher without weakening the existing queue, lease,
attempt, heartbeat, publication, retry, or recovery contracts.

## 1. Purpose

This document is an execution plan for another agent to implement, deploy, and
validate a production executor-partition worker in Microsoft Fabric.

The implementation must provide an operational path comparable to the current
`pc-dispatcher-00` path:

```text
registration
  -> shared video-work queue
  -> routed claim
  -> one bounded worker invocation
  -> independent per-video publication
  -> existing watchdog/retry/reconciliation/gold flow
```

The target canary pipeline is:

```text
pc-dispatcher-executor-00
  -> ClaimWorkExecutor
  -> ProcessVideoExecutor
```

The executor dispatcher is not a rename of
`pc-executor-partition-benchmark`. The benchmark pipeline remains an
independent measurement and approval artifact.

## 2. Mandatory execution instructions for the implementing agent

The implementing agent must complete the work end to end. Do not stop after
editing repository files or describing Fabric UI steps.

1. Use the integrated browser to open the target Fabric workspace and perform
   the actual Fabric configuration.
2. Create or update the notebooks and Data pipelines in the Fabric UI.
3. Create `pc-dispatcher-executor-00` in Fabric, configure all activities,
   connections, parameters, dependencies, policies, and schedule settings, and
   leave its schedule disabled until the rollout gate in this plan passes.
4. Update the deployed `pc-dispatcher-00` so its claim activity explicitly
   requests the notebook-04 processing engine.
5. Download/export the resulting Fabric Data pipeline definitions through the
   Fabric UI. Store the reviewed exports in
   `notebooks/fabric/exports/`.
6. Update `notebooks/fabric/README.md` with the exact labels, menus, panels,
   buttons, parameter types, expressions, and ordering observed in the current
   Fabric UI. The instructions must allow a human operator to reproduce the
   configuration manually.
7. Use screenshots or accessibility snapshots only as working evidence. Do
   not commit screenshots unless the user explicitly requests them.
8. Do not use VS Code Test Explorer or any test-running UI. Use repository
   commands for targeted validation when code changes require tests.
9. Do not claim deployment success based only on checked-in JSON. Verify the
   deployed Fabric items and at least one controlled end-to-end canary run.
10. Do not enable a recurring production schedule without explicit user
    approval after presenting the canary evidence.

If the current Fabric UI differs from this plan, follow the visible UI rather
than guessing. Record the actual UI sequence in the README and note any
necessary deviation in the final implementation report.

## 3. Current state

### 3.1 Original production path

The existing production flow is:

```text
pc-dispatcher-00
  -> 03_claim_work
  -> 04_process_video
```

`03_claim_work` grants one runtime-compatible batch from
`people_counter_video_work`. `04_process_video` then:

- validates the claimed `work_id` and `attempt_id` pairs;
- stages artifacts and loads one compatible runtime;
- emits durable worker commands;
- maintains attempt and queue heartbeats;
- writes attempt-scoped outputs;
- independently commits each successful video;
- classifies failures for retry or terminal handling; and
- leaves watchdog and reconciliation semantics intact.

The checked-in pipeline export is
`notebooks/fabric/exports/pc-dispatcher-00.json`.

### 3.2 Executor benchmark path

The executor benchmark currently runs:

```text
pc-executor-partition-benchmark
  -> PrepareExecutorBenchmark
  -> RunExecutorPartitionInference
  -> EvaluateExecutorBenchmark
  -> RefreshBenchmarkModel
```

Notebook 15 processes prepared benchmark rows with one Spark application and
persists benchmark plans, partition records, resource snapshots, and run
metrics. It does not implement the production queue contract and must not be
placed directly behind `03_claim_work`.

### 3.3 Gap

A production executor worker must combine:

- the control-plane and publication semantics of `04_process_video`; and
- the capacity-aware placement, partitioning, model reuse, resource
  validation, and native-thread budgeting of
  `15_executor_partition_inference`.

The production worker must not write successful production work only to the
executor benchmark tables.

## 4. Fixed architecture decisions

These decisions are part of the plan and should not be reopened without a
concrete implementation blocker.

### 4.1 Shared queue with explicit engine routing

Add this nullable-compatible column to `people_counter_video_work`:

```text
processing_engine STRING
```

Allowed normalized values:

```text
NOTEBOOK_04
EXECUTOR_PARTITION
```

Rules:

- New registrations default to `NOTEBOOK_04`.
- Existing rows are evolved/backfilled to `NOTEBOOK_04`.
- Keep the column nullable in both new-table DDL and compatible `ADD COLUMNS`
  evolution. Delta cannot add a required column compatibly to an existing
  table. Registration and migration code, rather than divergent DDL
  nullability, enforce canonical non-null values after migration.
- Null values are interpreted as `NOTEBOOK_04` only during the compatibility
  migration window.
- Unknown or blank non-null values fail registration or claim validation.
- Replay preserves the existing row's `processing_engine` unless the
  operator-approved replay explicitly changes it through a separately audited
  migration action.

Do not use separate queue tables. One queue preserves deduplication, replay,
watchdog, and operational reporting.

### 4.2 One claim implementation

Extend `03_claim_work.ipynb` with:

```python
PROCESSING_ENGINE = "NOTEBOOK_04"
```

The value must be normalized and validated against the two allowed engine
names. The engine predicate must be present in:

- the initial `eligible` definition used before the highest-priority runtime
  probe;
- the runtime-compatible candidate query;
- the conditional Delta claim update; and
- post-claim validation.

During the compatibility window, every one of those predicates uses
`coalesce(processing_engine, 'NOTEBOOK_04')`. Do not probe a runtime from one
engine and then apply it to the other engine's candidate set.

Both dispatchers must continue to use the same global dispatcher mutex and
active-worker count. Do not fork the claim transaction into an executor-only
copy that can race the original claim notebook.

Detection of an existing active lease owned by the current `DISPATCHER_ID`
remains engine-agnostic. Only the claim rows returned to the caller are
engine-filtered. Filtering the owned-active guard by engine could allow a
dispatcher that already owns work to allocate another batch.

### 4.3 Separate production worker notebook

Create:

```text
notebooks/fabric/17_process_video_executor.ipynb
```

Do not turn notebook 15 into a mixed benchmark/production notebook.

Notebook 17 must accept the same work identity and production-control
parameters required by notebook 04, plus reviewed executor resource controls.
It may reuse extracted shared helpers, but it must remain a separately
deployable Fabric notebook.

### 4.4 Separate canary dispatcher

Create:

```text
pc-dispatcher-executor-00
```

The original `pc-dispatcher-00` continues to process `NOTEBOOK_04` work.
The new dispatcher processes only `EXECUTOR_PARTITION` work.

### 4.5 Shared capacity admission

Routing prevents the dispatchers from claiming the wrong work, but it does not
by itself prevent capacity contention.

All dispatcher shards sharing one Fabric capacity must pass the same reviewed
`MAX_CONCURRENT_WORKERS` value to `03_claim_work`. Begin the canary with a
shared limit of `1` unless the approved capacity evidence proves that one
executor Spark application can safely coexist with another production worker.

Do not configure `pc-dispatcher-00` with a higher global limit while configuring
the executor dispatcher with `1`; the higher caller could still admit more
workers.

Before changing the original dispatcher, record its deployed pre-canary value
as `PRE_CANARY_MAX_CONCURRENT_WORKERS`. The current checked-in default is `4`,
but the deployed UI remains authoritative. Set both dispatcher pipelines to
the reviewed canary value of `1`. Rollback must restore the recorded value
after the executor schedule is disabled and all executor work has drained.

### 4.6 Driver-owned mutable control state

Spark executor tasks must not mutate:

- `people_counter_video_work`;
- dispatcher or registration leases;
- attempt status rows;
- worker command receipts; or
- committed-view ownership.

The notebook driver owns mutable control-plane transitions.
`WorkerEventClient.submit`, `process_worker_events`, and every operation that
can acquire `people_counter_control_writer` run only on the main driver
thread. Do not run control-writer operations in a background thread: the
durable lock has no automatic expiry, and thread cancellation between acquire
and release could block claim, worker, watchdog, and reconciliation activity
until manual recovery.

Executor tasks write only attempt-scoped, immutable staging records through
the reviewed idempotent materialization protocol. They never publish or commit
production output. The driver reads back the durable staging records and
performs the existing publication/commit protocol independently for every
attempt.

### 4.7 Benchmark remains separate

Keep these artifacts and semantics intact:

- `15_executor_partition_inference.ipynb`;
- `16_executor_partition_benchmark_control.ipynb`;
- `pc-executor-partition-benchmark`;
- executor benchmark Delta tables; and
- `pc_benchmark_report`.

Production notebook 17 can reuse approved executor settings, but production
attempts must be observable through the normal production tables and reports.

## 5. Required repository artifacts

Implement or update:

```text
notebooks/fabric/00_bootstrap_lakehouse.ipynb
notebooks/fabric/01_register_event.ipynb
notebooks/fabric/02_register_backfill.ipynb
notebooks/fabric/03_claim_work.ipynb
notebooks/fabric/04_process_video.ipynb             # extract shared code only if necessary
notebooks/fabric/05_watchdog_recovery.ipynb         # only if engine-aware recovery is required
notebooks/fabric/06_reconcile_publication.ipynb      # canary acceptance evidence
notebooks/fabric/10_replay_work.ipynb               # preserve/validate engine routing
notebooks/fabric/11_validate_observability.ipynb
notebooks/fabric/14_reset_test_data.ipynb            # reset the new column consistently
notebooks/fabric/15_executor_partition_inference.ipynb
notebooks/fabric/17_process_video_executor.ipynb
notebooks/fabric/README.md
notebooks/fabric/exports/pc-dispatcher-00.json
notebooks/fabric/exports/pc-dispatcher-executor-00.json
```

Add an append-only production staging Delta table, with its final reviewed
name documented in the README. The preferred name is:

```text
people_counter_executor_attempt_results
```

Its logical identity is at least:

```text
attempt_id, work_id, worker_execution_id, record_type, record_sequence
```

It stores typed executor success/failure envelopes, selected production output
rows, and input provenance before the driver publishes an attempt. It is not a
second queue and is not a committed-output table. Maintenance must retain an
attempt's staging records until publication, reconciliation, and the reviewed
retention period are complete.

Update production Python modules and tests under `src/` and `tests/` when
shared planner, processor, result-envelope, or publication helpers need to be
extracted. Prefer shared modules over copying large cells between notebooks 04,
15, and 17.

Do not modify the benchmark export merely to make the production dispatcher
work.

## 6. Phase 1: schema and routing

### 6.1 Bootstrap evolution

Update `00_bootstrap_lakehouse.ipynb` to:

1. Include nullable `processing_engine STRING` in a newly created video-work
   table.
2. Add the column compatibly to an existing table.
3. Backfill null existing values to `NOTEBOOK_04` during the documented
   stopped-writer maintenance window.
4. Validate that no non-null unsupported values exist.
5. Preserve all existing table properties, constraints implemented in code,
   and seeded locks.
6. Keep the migration idempotent.
7. Create/evolve the append-only executor attempt-results staging table and
   validate its logical identity columns.

The migration must not rewrite terminal work identities or reset queue times.

Implement the migration code before deployment, but execute it in Fabric only
in this order:

1. Record the enabled state, schedule, and parameter defaults of every
   dispatcher shard and related writer.
2. Disable all `pc-dispatcher-NN` schedules, executor dispatcher schedules,
   event-intake/backfill triggers, watchdog, replay, reconciliation, and gold
   writers that can touch the affected tables.
3. Wait for active notebook activities to finish. Do not cancel a healthy
   production worker merely to shorten the maintenance window.
4. Prove the drain gate:
   - no `people_counter_video_work` row is in `LEASED`, `STAGING`, `RUNNING`,
     or `WRITING`;
   - no unreceipted `people_counter_worker_events` row remains;
   - `people_counter_control_writer.owner_id IS NULL`; and
   - no pipeline activity still has a live Spark session writing these tables.
5. Run bootstrap migration and validate the new column, canonical backfill,
   unsupported-value count of zero, and staging-table schema.
6. Publish the engine-aware registration, claim, replay, watchdog,
   observability, and worker code.
7. Update `pc-dispatcher-00` to pass `NOTEBOOK_04`.
8. Restore the original non-executor schedules with the canary admission limit
   explicitly applied.
9. Process one controlled `NOTEBOOK_04` item successfully before routing any
   executor work.

Publishing engine-aware claim code before step 5 is prohibited because it
would query a column that does not yet exist. Running the Delta evolution
while a worker is active is prohibited because it can invalidate the worker's
metadata snapshot and commit.

### 6.2 Event and backfill registration

Update both registration notebooks so an optional manifest/config routing
value can select the engine.

Preferred external field:

```json
{
  "processing_engine": "EXECUTOR_PARTITION"
}
```

Requirements:

- Omitted value defaults to `NOTEBOOK_04`.
- Normalize case and surrounding whitespace before validation, but persist the
  canonical uppercase value.
- Reject unsupported values before acquiring or mutating queue state.
- A duplicate registration for an existing immutable work identity must not
  silently change its engine.
- Include the engine in registration diagnostics and notebook exit output.
- Update manifest documentation and examples.

If the current manifest schema requires the field under an existing config
object, use that compatible location and document the exact JSON path. Do not
invent a second ambiguous routing field.

### 6.3 Claim filtering

Update `03_claim_work.ipynb`:

1. Add and validate `PROCESSING_ENGINE`.
2. Add the compatibility-safe engine predicate to the initial `eligible`
   definition before the first candidate/runtime probe.
3. Filter runtime-compatible candidates by canonical engine.
4. Include the same compatibility-safe engine predicate in the conditional
   Delta update, not only the preliminary DataFrame query.
5. Validate that all claimed rows have exactly one runtime compatibility key
   and the requested engine.
6. Include `processing_engine` in every claimed item returned in
   `output.result.exitValue`.
7. Preserve idempotent retries for one `DISPATCHER_ID`.
8. Keep owned-active lease detection engine-agnostic, then reject a mixed or
   wrong-engine owned claim set rather than allocating additional work.
9. Preserve the existing empty-items success shape and distinguish
   `NO_CAPACITY` from `NO_ELIGIBLE_WORK` in diagnostic output without changing
   the successful empty `items` contract.

Update the original dispatcher to pass:

```text
PROCESSING_ENGINE = NOTEBOOK_04
```

The new dispatcher passes:

```text
PROCESSING_ENGINE = EXECUTOR_PARTITION
```

### 6.4 Replay, watchdog, reset, and observability

Review all queue-state readers and writers.

- Watchdog recovery must retain `processing_engine` while requeueing.
- Replay must retain and report it.
- Test reset must not leave unsupported or partially migrated values.
- Observability validation must group or expose queue depth, age, active
  leases, retries, failures, and completion by engine.
- Reconciliation must expose any executor publication mismatch with the same
  severity rules used for notebook 04.
- Operational queries in the README must include the engine where it changes
  diagnosis.

## 7. Phase 2: shared production/executor components

Before writing notebook 17, identify code in notebooks 04 and 15 that should
be shared.

Candidate shared responsibilities:

- parsing and validating claimed work items;
- reading and validating leased work rows;
- attempt event submission;
- main-thread synchronous heartbeat checkpoints;
- error classification;
- attempt-scoped output persistence;
- independent publication and commit;
- executor resource discovery;
- placement-safe concurrency planning;
- largest-cost-first partition assignment;
- worker memory enforcement;
- native thread configuration;
- executor runtime/model caching; and
- compact success/failure result envelopes.

Also extract notebook 04's production output-selection rules. In particular,
the executor path must persist the same selected line-count/telemetry rows,
including the final cumulative record required by reconciliation, rather than
blindly publishing every intermediate record produced by the benchmark
processor. Add parity tests that feed the same deterministic processor output
through both engines and compare the resulting attempt-scoped row identities,
final cumulative counts, and attempt totals.

Move reusable production code into importable modules under
`src/people_counter/` where practical. Notebook cells should orchestrate the
modules rather than duplicate hundreds of lines.

Preserve public behavior of notebook 04. Refactoring notebook 04 is acceptable
only when targeted tests prove its existing production contract remains
unchanged.

## 8. Phase 3: implement `17_process_video_executor.ipynb`

### 8.1 Required parameter cell

At minimum, support:

```text
WORK_ITEMS_JSON
WORK_ID
ATTEMPT_ID
MAX_ITEMS_PER_WORKER
MAX_WORKER_LIFETIME_SECONDS
PIPELINE_RUN_ID
ACTIVITY_RUN_ID
WORKER_EXECUTION_ID
FABRIC_JOB_INSTANCE_ID
BUNDLE_MANIFEST_SHA256
MODELS_DIR
SOURCE_STORAGE_ACCOUNT
SOURCE_CONTAINER
SOURCE_SHORTCUT_LOCAL_ROOT
DATABASE
TABLE_PREFIX
LEASE_MINUTES
HEARTBEAT_SECONDS
PROCESSING_ENGINE
CPUS_PER_TASK
PARALLEL_TASKS
PARTITION_WAVES
PEAK_WORKER_MEMORY_GIB
RESOURCE_DISCOVERY_TIMEOUT_SECONDS
MIN_APPROVED_SINGLE_VIDEO_SPEED_X
LEASE_SAFETY_FACTOR
LEASE_SAFETY_MARGIN_SECONDS
```

Add any driver/executor memory, core, and dynamic-allocation parameters needed
to keep the notebook's first `%%configure` cell aligned with the approved
executor configuration.

`PROCESSING_ENGINE` must equal `EXECUTOR_PARTITION`.

`MIN_APPROVED_SINGLE_VIDEO_SPEED_X` comes from reviewed sustained executor
evidence for the deployed model/runtime configuration; it must be positive.
`LEASE_SAFETY_FACTOR` must be at least `1.0` and initially conservative.
`LEASE_SAFETY_MARGIN_SECONDS` must be at least two configured heartbeat
intervals. These controls are operational lease guards, not throughput claims.

### 8.2 First code cell

The first code cell must be the Fabric `%%configure` cell. It exposes the
reviewed Spark session settings.

The Python parameter cell comes immediately after it. Keep
`CPUS_PER_TASK` synchronized between startup configuration and Python
validation. Fail with restart instructions when requested and effective Spark
settings differ.

Before relying on pipeline base parameters to affect `%%configure`, perform the
Fabric UI verification in section 9.2. If the current Fabric runtime does not
bind Notebook activity parameters early enough for the first configuration
cell, do not retain inert pipeline resource parameters. Deploy a notebook item
or Environment configuration per approved resource profile, select the
correct item in the dispatcher, and document that exact UI behavior.

### 8.3 Work validation

Before expensive initialization:

1. Parse and bound `WORK_ITEMS_JSON`.
2. Reject duplicate work/attempt pairs.
3. Fetch every queue row.
4. Verify the work is leased to the current dispatcher/attempt and routed to
   `EXECUTOR_PARTITION`.
5. Verify all rows are runtime compatible.
6. Treat already committed matching attempts idempotently.
7. Return `NO_WORK` without starting inference when the array is empty.
8. Require a positive source duration for each video. Do not substitute zero
   or silently invent a duration when lease projection evidence is missing.
9. Calculate for each video:

   ```text
   projected_wall_seconds =
     duration_seconds
     / MIN_APPROVED_SINGLE_VIDEO_SPEED_X
     * LEASE_SAFETY_FACTOR
   ```

10. Require:

   ```text
   max(projected_wall_seconds)
     + LEASE_SAFETY_MARGIN_SECONDS
     < LEASE_MINUTES * 60
   ```

11. Fail before staging or inference when the lease budget is insufficient.
    Persist a visible configuration/preflight failure through the production
    control path; do not leave the attempt leased for watchdog expiry and do
    not label the media corrupt.

This check is mandatory because the whole-video executor design has no safe
mid-task control-plane heartbeat. Increasing the number of waves cannot shorten
the slowest single-video task.

### 8.4 Driver heartbeat

Use a mandatory main-driver, synchronous heartbeat/checkpoint protocol. Do not
create a background heartbeat thread and do not call Spark, Delta, worker-event,
or control-writer APIs from another thread.

Maintain an explicit driver status map per attempt:

```text
LEASED -> STAGING -> RUNNING -> WRITING -> terminal
```

Requirements:

1. Pending attempts remain `LEASED`; do not send one batch-wide status.
2. Advance an attempt to `STAGING` before staging its source.
3. Heartbeat each attempt using its own current status.
4. Before a Spark wave, advance only that wave's staged attempts to `RUNNING`.
5. Submit one bounded wave as one materialization action.
6. No mutable control-plane write occurs while that Spark action is running.
7. Immediately after the wave completes, synchronously renew every remaining
   nonterminal attempt using its current status.
8. Advance each successful attempt to `WRITING` immediately before its
   production publication.
9. Stop renewals only after every attempt has a recorded terminal or retry
   transition.
10. Any heartbeat/control transition failure is surfaced and handled as a
    lease/control failure, not swallowed.

Before each wave, require its conservative projected completion time to remain
inside every participating attempt's current lease minus
`LEASE_SAFETY_MARGIN_SECONDS`. If not, do not submit the wave.

Use the live queue value, not only the configured lease duration:

```text
projected_wave_finish_at
  < min(participating lease_expires_at)
    - LEASE_SAFETY_MARGIN_SECONDS
```

Measure the wall time of a complete synchronous renewal cycle for the claimed
batch. `CLAIM_LIMIT` must be low enough that the measured cycle is less than
`HEARTBEAT_SECONDS / 2`. Record this evidence during the canary.

Do not claim that FAIR scheduling or a reserved task slot solves heartbeat
correctness. This design intentionally performs control-plane Spark work only
between completed inference actions.

### 8.5 Executor work

Use one Spark application for the complete claimed batch.

1. Discover the running allocation.
2. Validate scheduler CPU tokens.
3. Derive placement-safe concurrency from observed cores, memory, video count,
   and the approved cap;
4. assign whole videos largest-cost-first to planned partitions;
5. preserve runtime affinity;
6. For each task, resolve the reviewed source path and stage the video into an
   executor-local, attempt-specific path:
   - validate the mapped source namespace/path;
   - check local disk headroom before copying;
   - verify `expected_size_bytes`;
   - calculate and verify `expected_sha256`;
   - collect source duration, FPS, and total-frame evidence; and
   - clean up only the attempt-specific local file in success and failure
     paths.
7. Configure native CPU threads once per executor Python process;
8. lazily load and reuse compatible model runtimes within the supported cache
   lifetime;
9. process each video independently; and
10. return typed staging rows for every input attempt.

The typed result contract includes:

```text
attempt_id
work_id
worker_execution_id
status
error_type
error_message
retryable
input_sha256
source_size_bytes
source_duration_seconds
source_fps
total_source_frames
processed_frames
processing_seconds
distinct_people
line_in_count
line_out_count
record_type
record_sequence
txn_app_id
txn_version
```

Include the selected attempt-scoped detection, telemetry, and line-count data
needed to reproduce notebook 04's publication contract. Apply the shared
notebook-04 output-selection rule executor-side so the final cumulative record
and reconciliation totals are identical.

One failed video must not abort successful sibling publication. Spark task
retries must not publish duplicate production output.

### 8.6 Output and commit

Use a mandatory two-stage protocol.

#### Stage 1: one durable materialization

1. Materialize the complete wave result exactly once to
   `people_counter_executor_attempt_results`.
2. Use a stable `txnAppId` derived from the production worker/batch identity
   and a deterministic `txnVersion` for the wave.
3. Before starting inference, query staging by that transaction identity.
   Reuse a complete, validated prior materialization; fail visibly on a partial
   or conflicting materialization. Do not recompute a complete prior wave.
4. Perform one DataFrame write/action for the entire new wave. Do not filter and
   action the lazy inference DataFrame separately per attempt.
5. Verify after the write:
   - one terminal result envelope per claimed attempt in the wave;
   - no duplicate logical staging identity;
   - expected record counts and final cumulative records; and
   - the stable transaction identity persisted for audit.
6. Read records back from the Delta staging table for publication. Never
   publish from cached RDD/DataFrame lineage, because cache eviction or executor
   loss may recompute inference.

The implementing agent must prove with an execution counter/test hook that the
inference stage runs once per wave even when multiple attempts are published.

#### Stage 2: independent driver publication

For every successful staged result, the main driver invokes the same production
publication contract as notebook 04:

- attempt-scoped telemetry/detection/counting output;
- verified input and configuration hashes;
- processed-frame and timing evidence;
- durable worker commands;
- globally serialized commit;
- committed-view visibility; and
- final `SUCCEEDED` queue/attempt state.

For every failure:

- persist the exception type and bounded message;
- classify retryability with the shared production classifier;
- transition through the existing durable worker-command/control-writer path;
- avoid publishing partial output as committed; and
- allow watchdog/retry/dead-letter behavior to proceed unchanged.

Input size/hash mismatch and source validation errors use the shared
non-retryable input-error classification from notebook 04. Infrastructure copy
or transient storage failures retain the shared retryability rules.

The staging table is append-only during worker execution. Do not delete staging
rows after a successful commit inside notebook 17. Retention cleanup occurs
only after reconciliation and the documented retention period.

Notebook exit output must summarize:

```text
processing_engine
worker_execution_id
claimed_count
succeeded_count
failed_count
retryable_count
observed_executor_count
planned_concurrency
observed_concurrency
native_threads_per_worker
wall_seconds
source_video_minutes
throughput_video_minutes_per_wall_minute
```

Do not use a success-shaped exit when any control-plane transition failed.

## 9. Phase 4: create and update Fabric pipelines with the integrated browser

### 9.1 Browser requirements

The implementing agent must use the integrated browser for this phase.

1. Reuse a shared Fabric/Power BI page when available.
2. If no page is shared, open the target workspace in the integrated browser.
3. Confirm the workspace ID before editing:
   `c31ee864-230d-4005-8fd5-7c7130ebf774` for the current development
   workspace.
4. Do not navigate through VS Code Test Explorer.
5. Use accessibility snapshots and role/label-based browser actions.
6. When Power BI/Fabric controls are hidden in overflow menus, inspect the
   visible menu rather than repeatedly guessing coordinates.

Follow the strict drain/migration/publish order in section 6.1. Browser access
does not authorize a live schema migration or engine-aware claim deployment
while old writers remain enabled.

### 9.2 Publish and configure notebook 17

Publish `17_process_video_executor.ipynb` to the development workspace.

In Fabric:

1. Open the notebook item.
2. Attach and pin the same default Lakehouse used by notebooks 03 and 04.
3. Attach the validated Fabric Environment containing the project package,
   OpenCV, and offline model dependencies.
4. Confirm the `%%configure` cell is first.
5. Confirm the Python configuration cell is recognized as the parameter cell.
6. Stop any active session before validating changed startup settings.
7. Before wiring production resource overrides, run a controlled Notebook
   activity with a distinctive non-default resource parameter.
8. In an early diagnostic cell, emit the requested value and the corresponding
   effective `spark.conf` value.
9. Open the pipeline run, select the Notebook activity, and inspect its
   `Output` in the current Fabric UI.
10. Continue with parameter-driven startup configuration only if the effective
    value proves that the Notebook activity override affected the first
    `%%configure` cell.
11. If it did not, configure a dedicated notebook item or Fabric Environment
    per approved CPU profile, select that item in activity `Settings`, and
    remove misleading inert startup parameters.
12. Run a small direct notebook validation and record:
    - selected Fabric Environment;
    - effective executor count and cores;
    - effective task CPUs;
    - scheduler mode;
    - dynamic-allocation state; and
    - worker SKU/memory evidence.
13. Record the created notebook item ID for the export and README.

Document the actual observed UI labels and whether startup configuration was
parameter-driven or item/environment-driven.

### 9.3 Update `pc-dispatcher-00`

Open the deployed pipeline in the Fabric browser.

1. Select `ClaimWork`.
2. In the current UI's activity settings, add the notebook base parameter:

   | Name | Type | Value |
   |---|---|---|
   | `PROCESSING_ENGINE` | `String` | `NOTEBOOK_04` |

3. Record the deployed original `MAX_CONCURRENT_WORKERS` as
   `PRE_CANARY_MAX_CONCURRENT_WORKERS`. The checked-in default is `4`, but the
   deployed value is authoritative.
4. Set `MAX_CONCURRENT_WORKERS = 1` for the serialized canary period.
5. Confirm `LEASE_MINUTES` against notebook 04's approved worst-case evidence
   rather than changing it silently.
6. Except for the explicit admission-limit change, verify all existing
   pipeline parameters, activity dependencies, timeout, retry, connection, and
   schedule settings remain unchanged.
7. Validate the pipeline.
8. Save/publish it only after the schema migration and engine-aware notebooks
   exist.
9. Process one controlled `NOTEBOOK_04` item successfully before routing any
   executor work.

### 9.4 Create `pc-dispatcher-executor-00`

Use the Fabric UI to create a new Data pipeline. Do not rely only on editing a
JSON export offline.

Create these pipeline parameters:

| Pipeline parameter | Type | Initial default |
|---|---|---|
| `MAX_CONCURRENT_WORKERS` | `Int` | `1` for the first shared-capacity canary |
| `CLAIM_LIMIT` | `Int` | `1` for the first canary; raise only after heartbeat-cycle evidence |
| `LEASE_MINUTES` | `Int` | Greater than the approved maximum projected single-video wall time plus safety margin |
| `BUNDLE_MANIFEST_SHA256` | `String` | Exact deployed bundle manifest hash |
| `MIN_APPROVED_SINGLE_VIDEO_SPEED_X` | matching numeric type | Reviewed sustained executor evidence |
| `LEASE_SAFETY_FACTOR` | matching numeric type | Conservative value at least `1.0` |
| `LEASE_SAFETY_MARGIN_SECONDS` | `Int` | At least two heartbeat intervals |
| executor resource parameters | Matching Fabric types | Approved notebook-15 configuration |

Add `ClaimWorkExecutor`:

- activity type: Notebook;
- notebook: deployed `03_claim_work`;
- same Lakehouse connection used by the original dispatcher;
- `DISPATCHER_ID = @pipeline().RunId`;
- shared worker/claim/lease parameters from the pipeline;
- `DATABASE` and `TABLE_PREFIX` matching the environment;
- `PROCESSING_ENGINE = EXECUTOR_PARTITION`;
- same idempotent retry policy as the original `ClaimWork` activity unless
  current evidence requires a documented change.

Add an `If Condition` immediately after `ClaimWorkExecutor` with this
expression:

```text
@greater(json(activity('ClaimWorkExecutor').output.result.exitValue).claimed_count, 0)
```

Keep the false branch as an explicit successful no-work path. It must not start
a Spark session.

Add `ProcessVideoExecutor` inside the true branch:

- activity type: Notebook;
- notebook: deployed `17_process_video_executor`;
- same validated Lakehouse connection and Environment;
- `WORK_ITEMS_JSON` from:

  ```text
  @string(json(activity('ClaimWorkExecutor').output.result.exitValue).items)
  ```

- pipeline/activity/worker IDs following the notebook-04 dispatcher pattern;
- `PROCESSING_ENGINE = EXECUTOR_PARTITION`;
- reviewed lease projection and safety parameters;
- all executor startup/runtime settings passed with the exact Fabric types;
- no activity retry unless idempotency for the entire worker invocation is
  explicitly proven; and
- timeout covering the reviewed maximum worker lifetime plus cleanup margin.

Validate, save, and publish the pipeline.

Keep one worker activity per pipeline run and set its activity concurrency/batch
to `1`; Spark partitions provide per-video parallelism inside that one
application. Set `MAX_CONCURRENT_WORKERS = 1`, matching
`pc-dispatcher-00`. Keep `CLAIM_LIMIT` within both the placement-safe executor
plan and the measured heartbeat-cycle bound.

### 9.5 Schedule

Create the intended fixed schedule in the UI so its exact settings are
documented, but leave it disabled during implementation and initial canary
validation.

Record:

- cadence;
- start time;
- time zone;
- end date;
- enabled/disabled state;
- any offset from `pc-dispatcher-00`; and
- the operational alert required before a fixed schedule expires.

Enable it only after explicit user approval.

With the shared canary admission limit of `1`, run the executor canary and
notebook-04 regression serially. A zero-row claim while the other dispatcher
consumes the admitted slot is expected `NO_CAPACITY`, not a failed canary.
Capture the claim output and retry only after the active worker completes.

## 10. Phase 5: obtain and check in Fabric exports

The implementing agent must obtain exports from the deployed Fabric items
through the integrated browser.

### 10.1 Export procedure

For both pipelines:

```text
pc-dispatcher-00
pc-dispatcher-executor-00
```

1. Open the pipeline item in Fabric.
2. Use the current UI's export/download action.
3. Download the pipeline definition.
4. Confirm the exported item name, object ID, workspace IDs, notebook IDs,
   parameter types, expressions, activity policies, dependencies, and
   connection references match the deployed item.
5. Copy the reviewed definitions to:

   ```text
   notebooks/fabric/exports/pc-dispatcher-00.json
   notebooks/fabric/exports/pc-dispatcher-executor-00.json
   ```

6. Format only as needed to match existing export style. Do not hand-rewrite
   IDs or expressions after download without reimporting/revalidating the
   deployed pipeline.
7. Compare the checked-in export with the deployed item after any subsequent
   UI edit.

If Fabric does not expose a direct download action in the current tenant,
use the supported Fabric item export route available in the UI/API surfaced by
the browser session. Document the exact fallback in the README and verify that
the resulting JSON can recreate the item in the same workspace, or in another
workspace after deliberate ID remapping.

Checked-in exports are review and disaster-recovery evidence for the deployed
workspace. They contain workspace, notebook, connection, schedule, and other
object IDs and are not portable deployment templates. Importing into another
workspace requires deliberate ID remapping and post-import validation.
Preserve semantic IDs in the reviewed export. Document any redaction or
non-semantic normalization; do not substitute guessed placeholders.

## 11. Phase 6: README instructions that match the actual UI

Update `notebooks/fabric/README.md` during deployment, not afterward from
memory.

The README must include:

1. The architecture and reason for explicit engine routing.
2. The stopped-writer bootstrap migration for `processing_engine`.
3. Manifest/event/backfill examples selecting each engine.
4. Exact `03_claim_work` parameter behavior.
5. Exact manual creation steps for `pc-dispatcher-executor-00`.
6. Exact observed `If Condition` creation steps and expression.
7. The current Fabric navigation path used to create a Data pipeline.
8. The current names of the activity configuration tabs/panels.
9. How to select the Notebook activity connection.
10. How to select the Lakehouse and notebook item.
11. Every pipeline parameter with its Fabric type and default.
12. Every notebook base parameter, including whether its value is Literal or
    **Add dynamic content**.
13. Exact dynamic expressions, without adding quotes around expressions.
14. Dependency connector instructions.
15. Timeout and retry UI settings.
16. Validate/save/publish steps.
17. Schedule creation, disabled-canary state, and later enablement.
18. How to export/download both pipelines from the current UI.
19. Run-history navigation and the exact activity output property path.
20. Canary queries and expected queue/attempt/output transitions.
21. How to distinguish `NO_CAPACITY` from `NO_ELIGIBLE_WORK`.
22. Lease projection evidence, approved safety values, and abort threshold.
23. Append-only executor staging retention and reconciliation.
24. Retained control-writer owner inspection and approved recovery.
25. Export ID non-portability and cross-workspace remapping.
26. Rollback steps.

Instructions must distinguish:

- repository export import;
- manual pipeline creation;
- normal scheduled execution;
- a manual canary run; and
- the executor benchmark pipeline.

Do not describe a menu or button that was not observed. If a UI label differs
between `app.fabric.microsoft.com` and `app.powerbi.com`, document the host and
label actually used.

## 12. Phase 7: tests and local validation

Add targeted tests for changed production modules and notebook source where the
repository's existing notebook test helpers support it.

At minimum verify:

### Routing

- omitted engine defaults to `NOTEBOOK_04`;
- supported values normalize to their canonical form;
- unsupported values fail;
- fresh and evolved DDL keep the column nullable while registration writes a
  canonical non-null value;
- duplicate registration cannot change an existing engine;
- original claims cannot select executor rows;
- executor claims cannot select notebook-04 rows;
- the initial `eligible` set, runtime candidate query, and conditional claim
  merge include the compatibility-safe engine predicate;
- owned-active lease detection is engine-agnostic;
- claim retry is engine-scoped; and
- replay/watchdog preserve the engine.

### Executor worker

- empty claims return `NO_WORK` before expensive initialization;
- duplicate work/attempt pairs fail;
- mismatched lease or engine fails;
- runtime-incompatible batches fail before inference;
- resource mismatch fails with restart instructions;
- planned concurrency never exceeds scheduler, memory, explicit, or video
  caps;
- native thread budgeting remains placement-safe;
- one video failure does not discard successful siblings;
- each attempt advances through `LEASED -> STAGING -> RUNNING -> WRITING` and a
  pending-wave attempt remains `LEASED`;
- a single video that cannot finish before the lease safety margin is rejected
  before staging or inference;
- source local-staging enforces disk headroom, byte count, SHA-256, provenance,
  and attempt-scoped cleanup;
- corrupt input uses notebook 04's non-retryable input classification;
- one stable Delta transaction materializes each wave exactly once;
- a complete prior staging transaction is reused without rerunning inference,
  while a partial/conflicting transaction fails visibly;
- publishing multiple attempts executes inference only once;
- staging duplicate/cardinality checks run before publication;
- Spark retries do not cause duplicate staging or publication;
- notebook-04 line-count/output-row selection parity is preserved;
- every success commits through the production contract;
- retryable and terminal failures retain correct state;
- heartbeat failure is visible and not swallowed; and
- worker exit counts match terminal per-item outcomes.

### Existing path regression

- notebook-04 registration, claim, worker, retry, commit, and idle behavior
  remain unchanged for `NOTEBOOK_04`;
- legacy null engine rows remain claimable only by the original path during
  migration; and
- global admission still counts active workers across both engines;
- empty claims do not start a Spark session; and
- executor staging remains available through publication and reconciliation.

Use terminal commands only. Do not use VS Code Test Explorer or any
test-running UI.

Run the focused baseline:

```bash
uv run pytest \
  tests/test_fabric_control.py \
  tests/test_fabric_events.py \
  tests/test_fabric_executor_partition.py \
  tests/test_fabric_notebooks.py \
  -q
```

Run the same scope with coverage and CRAP:

```bash
uv run pytest \
  tests/test_fabric_control.py \
  tests/test_fabric_events.py \
  tests/test_fabric_executor_partition.py \
  tests/test_fabric_notebooks.py \
  -q \
  --cov=people_counter \
  --cov-report=term-missing \
  --crap \
  --crap-threshold=30 \
  --crap-top-n=20
```

After the baseline is green, run mutation testing:

```bash
uv run mutmut run \
  "people_counter.fabric_control*" \
  "people_counter.fabric_events*" \
  "people_counter.fabric_executor_partition*"
uv run mutmut results
```

If production helpers are added under another module, include that module's
quoted mutant glob in the same command. Inspect each relevant surviving,
suspicious, or timed-out mutant with
`uv run mutmut show <mutant-name>` and report verified counts. Do not claim
mutation validation passed when the run is incomplete or no tests are
associated.

Run a syntax check:

```bash
uv run python -m compileall src/people_counter
```

The repository currently has no configured Ruff, mypy, or Pyright command; do
not invent one. If implementation adds one through an approved manifest
change, run its documented repository command.

## 13. Phase 8: deployed canary validation

### 13.1 Preconditions

- Bootstrap migration completed with writers stopped.
- Both notebook items use the same reviewed package and model bundle.
- The deployed pre-canary original admission limit is recorded.
- Both dispatchers use the shared canary limit of `1`.
- Executor recurring schedule remains disabled.
- No unexpected active lease remains.
- Worker event backlog is fully receipted.
- `people_counter_control_writer.owner_id IS NULL`.
- At least one fresh input manifest is explicitly routed to
  `EXECUTOR_PARTITION`.
- A comparable input remains or is routed to `NOTEBOOK_04` for regression
  validation.
- The executor sample's projected single-video wall time passes the lease
  safety gate.

### 13.2 Manual executor canary

Use the integrated browser:

1. Open `pc-dispatcher-executor-00`.
2. Select the Fabric action for an immediate/manual run.
3. Review the displayed parameter values.
4. Start exactly one run.
5. Open run history in the UI.
6. Inspect `ClaimWorkExecutor` output and verify:
   - one engine only;
   - expected claim count;
   - unique work/attempt pairs; and
   - `processing_engine = EXECUTOR_PARTITION`.
7. Inspect `ProcessVideoExecutor` input and output.
8. Confirm the Spark application used executors with the approved task/native
   thread settings.
9. Confirm every claimed item reached a valid terminal or retry state.
10. Run the same pipeline again and verify completed attempts are not
    duplicated.

If the claim returns `NO_CAPACITY` because the original dispatcher occupies
the one admitted slot, capture that output, wait for it to finish, and retry.
Do not loosen the global limit to force the canary to start.

Abort the canary and stop further executor routing when either:

- observed single-video time exceeds the approved projection tolerance; or
- remaining lease time reaches `LEASE_SAFETY_MARGIN_SECONDS`.

Cancel the scheduled activity before lease expiry, disable further executor
starts, and use the documented control/watchdog recovery path. Do not perform
ad hoc queue-state updates. The preflight lease gate is the primary protection;
operator cancellation is only the last-resort canary abort.

### 13.3 Data validation

Verify persisted state, not only green activity icons:

- queue lease owner and expiration behavior;
- attempt rows and worker execution IDs;
- per-attempt synchronous state/heartbeat timestamps between Spark waves;
- pending-wave items remain `LEASED`;
- maximum projected single-video wall time and lease margin;
- measured complete heartbeat-cycle wall time below
  `HEARTBEAT_SECONDS / 2`;
- attempt-scoped output row counts;
- staging transaction identity, uniqueness, and cardinality;
- input hash, source byte count, duration, FPS, total frames, and processed
  frames;
- independent commit per successful video;
- committed views and downstream gold visibility;
- no duplicate committed attempt;
- no benchmark table used as the sole production output;
- failure classification and watchdog recovery;
- an empty claim does not start a Spark application; and
- operational metrics grouped by `processing_engine`.

After publication, run notebooks 06 and 11. Capture their outputs and require
zero new `ERROR` reconciliation/observability findings. Also require no
unreceipted worker events and a null control-writer owner at canary completion.

### 13.4 Original-path regression in Fabric

After the executor run reaches a terminal state and the global active count is
zero, run or observe one controlled `NOTEBOOK_04` item through
`pc-dispatcher-00`. Confirm the explicit routing parameter did not change its
normal claim, worker, or publication behavior. Keep this regression serialized
with the executor canary while the shared limit is `1`.

### 13.5 Contention validation

While one executor worker is active, verify another dispatcher sharing the
global limit returns `NO_CAPACITY` and does not admit another item. Treat that
zero-claim result as expected evidence. Do not create overlapping Spark work or
artificial production load; use development rows and a controlled window.

## 14. Acceptance criteria

Implementation is complete only when all conditions hold:

1. `processing_engine` is migrated and documented.
2. Registration defaults safely and supports explicit executor routing.
3. One shared claim implementation filters conditionally by engine.
4. `pc-dispatcher-00` explicitly passes `NOTEBOOK_04`.
5. `17_process_video_executor` preserves the production attempt,
   heartbeat, output, commit, retry, and recovery contracts.
6. Per-attempt heartbeats never advance pending-wave attempts prematurely.
7. No projected single-video task can cross the lease safety margin.
8. Executor sources are locally staged, size/hash validated, and cleaned up
   without affecting sibling files.
9. Inference is durably materialized once per wave and is not recomputed during
   per-attempt publication.
10. Executor and notebook-04 output selection produce equivalent attempt totals
    and final cumulative line-count records.
11. `pc-dispatcher-executor-00` exists in Fabric and is fully configured.
12. The executor recurring schedule is disabled pending explicit approval.
13. Both pipeline exports were downloaded from Fabric and checked in.
14. README instructions match the observed UI and can be followed manually.
15. Every command in section 12 succeeds; mutation survivors are classified
    rather than hidden.
16. One executor canary completes with persisted production evidence.
17. The complete batch heartbeat cycle remains below half the heartbeat
    interval.
18. Notebook 06 reconciliation and notebook 11 observability report zero new
    `ERROR` findings.
19. One original-path regression item completes normally.
20. Shared admission prevents unsafe overlap.
21. Worker event backlog is empty and the control-writer owner is null after
    the canary.
22. `git diff --check` passes and no secret is committed.
23. No work is silently lost, double-published, or stranded behind an
    unreported lease.

## 15. Rollout

1. Deploy with all new work defaulting to `NOTEBOOK_04`.
2. Leave `pc-dispatcher-executor-00` scheduled execution disabled.
3. Route a small, explicit development canary set to
   `EXECUTOR_PARTITION`.
4. Run the executor dispatcher manually.
5. Compare correctness, throughput, latency, memory, retry, and recovery
   evidence with notebook 04.
6. Present results to the user.
7. Enable a low-rate executor schedule only after explicit approval.
8. Expand routing by reviewed camera/manifest cohorts.
9. Keep the original dispatcher available until rollback and reconciliation
   evidence prove the executor path is operationally equivalent.
10. Consider replacing the worker behind the original dispatcher name only in
    a later migration after sustained production approval.

## 16. Rollback

Rollback must not require deleting queue rows.

1. Disable the executor schedule.
2. Stop routing new registrations to `EXECUTOR_PARTITION`.
3. Allow an active executor worker to complete when lease-safe, or follow the
   documented cancellation and watchdog-expiry procedure.
4. Prove that no executor notebook activity or Spark session can still commit
   before changing retained control state.
5. Inspect `people_counter_control_writer.owner_id`.
   - If null, continue.
   - If non-null, identify the owner and prove its Fabric execution has
     terminated and cannot commit.
   - Clear a retained owner only through the reviewed recovery transaction
     after that proof. Never clear it only because it is old.
6. Change future manifests/default routing to `NOTEBOOK_04`.
7. For queued, unleased executor rows, perform an audited routing migration to
   `NOTEBOOK_04` only after validating runtime/config compatibility.
8. Do not change the engine of an actively leased row or hand-edit attempt
   state.
9. Run watchdog and reconciliation.
10. Confirm no executor leases, nonterminal attempts, unreceipted events, or
    retained control-writer owner remain.
11. Restore `pc-dispatcher-00.MAX_CONCURRENT_WORKERS` to the recorded
    `PRE_CANARY_MAX_CONCURRENT_WORKERS`, or a separately approved replacement.
    Keep every enabled dispatcher shard on the same reviewed value.
12. Run one normal notebook-04 cycle and verify the global active-worker count.
13. Preserve the nullable `processing_engine` column, canonical values,
    executor staging evidence, notebook 17, pipeline export, and audit evidence
    for diagnosis and retention.
14. Do not delete executor output or staging rows until reconciliation is
    complete and retention permits it.
15. Export the rolled-back pipeline state and update the README.

## 17. Final implementation report

The implementing agent's final response must include:

- repository files changed;
- Fabric items created or updated;
- notebook and pipeline item IDs;
- exact export files obtained;
- local validation commands and outcomes;
- canary pipeline run ID;
- claimed/succeeded/failed/retry counts;
- observed executor resources and throughput;
- pre-canary, canary, and restored/approved shared
  `MAX_CONCURRENT_WORKERS` values;
- approved lease projection values and measured heartbeat-cycle duration;
- notebook 06 and 11 `ERROR` counts;
- original-path regression result;
- whether the executor schedule remains disabled;
- README section links; and
- any deviations from this plan with their reason.
