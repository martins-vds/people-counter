# Fabric executor production dispatcher implementation plan

Date: September 30, 2026

Status: Proposed; ready for implementation

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
names. Candidate selection and conditional claim updates must include the
engine predicate.

Both dispatchers must continue to use the same global dispatcher mutex and
active-worker count. Do not fork the claim transaction into an executor-only
copy that can race the original claim notebook.

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

### 4.6 Driver-owned mutable control state

Spark executor tasks must not mutate:

- `people_counter_video_work`;
- dispatcher or registration leases;
- attempt status rows;
- worker command receipts; or
- committed-view ownership.

The notebook driver owns mutable control-plane transitions. Executor tasks may
write only attempt-scoped, immutable output partitions when the write protocol
has been validated for concurrent tasks. Each task returns a compact result or
failure envelope to the driver. The driver then performs the existing
publication/commit protocol independently for every attempt.

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
notebooks/fabric/10_replay_work.ipynb               # preserve/validate engine routing
notebooks/fabric/11_validate_observability.ipynb
notebooks/fabric/14_reset_test_data.ipynb            # reset the new column consistently
notebooks/fabric/15_executor_partition_inference.ipynb
notebooks/fabric/17_process_video_executor.ipynb
notebooks/fabric/README.md
notebooks/fabric/exports/pc-dispatcher-00.json
notebooks/fabric/exports/pc-dispatcher-executor-00.json
```

Update production Python modules and tests under `src/` and `tests/` when
shared planner, processor, result-envelope, or publication helpers need to be
extracted. Prefer shared modules over copying large cells between notebooks 04,
15, and 17.

Do not modify the benchmark export merely to make the production dispatcher
work.

## 6. Phase 1: schema and routing

### 6.1 Bootstrap evolution

Update `00_bootstrap_lakehouse.ipynb` to:

1. Include `processing_engine STRING` in a newly created video-work table.
2. Add the column compatibly to an existing table.
3. Backfill null existing values to `NOTEBOOK_04` during the documented
   stopped-writer maintenance window.
4. Validate that no non-null unsupported values exist.
5. Preserve all existing table properties, constraints implemented in code,
   and seeded locks.
6. Keep the migration idempotent.

The migration must not rewrite terminal work identities or reset queue times.

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
2. Filter new candidates by canonical engine.
3. Include the engine predicate in the conditional Delta update, not only the
   preliminary DataFrame query.
4. Validate that all claimed rows have exactly one runtime compatibility key
   and the requested engine.
5. Include `processing_engine` in every claimed item returned in
   `output.result.exitValue`.
6. Preserve idempotent retries for one `DISPATCHER_ID`.
7. Ensure a retry never returns claims owned by that dispatcher for the other
   engine.
8. Preserve the existing empty-items success shape.

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
- Operational queries in the README must include the engine where it changes
  diagnosis.

## 7. Phase 2: shared production/executor components

Before writing notebook 17, identify code in notebooks 04 and 15 that should
be shared.

Candidate shared responsibilities:

- parsing and validating claimed work items;
- reading and validating leased work rows;
- attempt event submission;
- driver heartbeat scheduling;
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
```

Add any driver/executor memory, core, and dynamic-allocation parameters needed
to keep the notebook's first `%%configure` cell aligned with the approved
executor configuration.

`PROCESSING_ENGINE` must equal `EXECUTOR_PARTITION`.

### 8.2 First code cell

The first code cell must be the Fabric `%%configure` cell. It must expose the
reviewed Spark session settings that a pipeline Notebook activity can override.

The Python parameter cell comes immediately after it. Keep
`CPUS_PER_TASK` synchronized between startup configuration and Python
validation. Fail with restart instructions when requested and effective Spark
settings differ.

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

### 8.4 Driver heartbeat

Long Spark stages must not allow production leases to expire.

Implement a driver-owned heartbeat loop that:

- begins before staging/resource discovery can exceed the heartbeat interval;
- updates all nonterminal attempts;
- continues while Spark actions are running;
- stops only after every item is committed or transitioned to a recorded
  failure;
- surfaces heartbeat errors and fails safely rather than silently continuing;
  and
- never calls Spark APIs from an unsafe background thread.

If Fabric/PySpark threading prevents a reliable background loop, divide the
Spark work into bounded global waves and heartbeat synchronously between waves.
The maximum wave duration must remain below the lease safety margin. Document
and test the chosen mechanism.

### 8.5 Executor work

Use one Spark application for the complete claimed batch.

1. Discover the running allocation.
2. Validate scheduler CPU tokens.
3. derive placement-safe concurrency from observed cores, memory, video count,
   and the approved cap;
4. assign whole videos largest-cost-first to planned partitions;
5. preserve runtime affinity;
6. configure native CPU threads once per executor Python process;
7. lazily load and reuse compatible model runtimes within the supported cache
   lifetime;
8. process each video independently; and
9. return one typed result envelope per input attempt.

One failed video must not abort successful sibling publication. Spark task
retries must not publish duplicate production output.

### 8.6 Output and commit

For every successful executor result, the driver must invoke the same
production publication contract as notebook 04:

- attempt-scoped telemetry/detection/counting output;
- expected input and configuration hashes;
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
7. Record the created notebook item ID for the export and README.

### 9.3 Update `pc-dispatcher-00`

Open the deployed pipeline in the Fabric browser.

1. Select `ClaimWork`.
2. In the current UI's activity settings, add the notebook base parameter:

   | Name | Type | Value |
   |---|---|---|
   | `PROCESSING_ENGINE` | `String` | `NOTEBOOK_04` |

3. Verify all existing pipeline parameters, activity dependencies, timeout,
   retry, connection, and schedule settings remain unchanged.
4. Validate the pipeline.
5. Save/publish it.
6. Do not trigger it with real queued work until the schema migration and
   notebook deployment are complete.

### 9.4 Create `pc-dispatcher-executor-00`

Use the Fabric UI to create a new Data pipeline. Do not rely only on editing a
JSON export offline.

Create these pipeline parameters:

| Pipeline parameter | Type | Initial default |
|---|---|---|
| `MAX_CONCURRENT_WORKERS` | `Int` | `1` for the first shared-capacity canary |
| `CLAIM_LIMIT` | `Int` | Approved executor canary batch size |
| `LEASE_MINUTES` | `Int` | Long enough for the bounded executor wave, initially matching the validated worker setting |
| `BUNDLE_MANIFEST_SHA256` | `String` | Exact deployed bundle manifest hash |
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

Add `ProcessVideoExecutor`:

- activity type: Notebook;
- dependency: green **On success** from `ClaimWorkExecutor`;
- notebook: deployed `17_process_video_executor`;
- same validated Lakehouse connection and Environment;
- `WORK_ITEMS_JSON` from:

  ```text
  @string(json(activity('ClaimWorkExecutor').output.result.exitValue).items)
  ```

- pipeline/activity/worker IDs following the notebook-04 dispatcher pattern;
- `PROCESSING_ENGINE = EXECUTOR_PARTITION`;
- all executor startup/runtime settings passed with the exact Fabric types;
- no activity retry unless idempotency for the entire worker invocation is
  explicitly proven; and
- timeout covering the reviewed maximum worker lifetime plus cleanup margin.

Validate, save, and publish the pipeline.

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
the resulting JSON can recreate the item.

## 11. Phase 6: README instructions that match the actual UI

Update `notebooks/fabric/README.md` during deployment, not afterward from
memory.

The README must include:

1. The architecture and reason for explicit engine routing.
2. The stopped-writer bootstrap migration for `processing_engine`.
3. Manifest/event/backfill examples selecting each engine.
4. Exact `03_claim_work` parameter behavior.
5. Exact manual creation steps for `pc-dispatcher-executor-00`.
6. The current Fabric navigation path used to create a Data pipeline.
7. The current names of the activity configuration tabs/panels.
8. How to select the Notebook activity connection.
9. How to select the Lakehouse and notebook item.
10. Every pipeline parameter with its Fabric type and default.
11. Every notebook base parameter, including whether its value is Literal or
    **Add dynamic content**.
12. Exact dynamic expressions, without adding quotes around expressions.
13. Dependency connector instructions.
14. Timeout and retry UI settings.
15. Validate/save/publish steps.
16. Schedule creation, disabled-canary state, and later enablement.
17. How to export/download both pipelines from the current UI.
18. Run-history navigation and the exact activity output property path.
19. Canary queries and expected queue/attempt/output transitions.
20. Rollback steps.

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
- duplicate registration cannot change an existing engine;
- original claims cannot select executor rows;
- executor claims cannot select notebook-04 rows;
- the conditional claim merge includes the engine predicate;
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
- Spark retries do not cause duplicate publication;
- every success commits through the production contract;
- retryable and terminal failures retain correct state;
- heartbeat failure is visible and not swallowed; and
- worker exit counts match terminal per-item outcomes.

### Existing path regression

- notebook-04 registration, claim, worker, retry, commit, and idle behavior
  remain unchanged for `NOTEBOOK_04`;
- legacy null engine rows remain claimable only by the original path during
  migration; and
- global admission still counts active workers across both engines.

Run the smallest repository-standard test, lint, notebook validation, and type
check scopes covering the change. Because Python production code will likely
change, follow the repository's required coverage/CRAP workflow. If Python
tests are added or modified, also follow the required mutation-testing
workflow. Do not use Test Explorer.

## 13. Phase 8: deployed canary validation

### 13.1 Preconditions

- Bootstrap migration completed with writers stopped.
- Both notebook items use the same reviewed package and model bundle.
- Both dispatchers use the same shared capacity-admission limit.
- Executor recurring schedule remains disabled.
- At least one fresh input manifest is explicitly routed to
  `EXECUTOR_PARTITION`.
- A comparable input remains or is routed to `NOTEBOOK_04` for regression
  validation.

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

### 13.3 Data validation

Verify persisted state, not only green activity icons:

- queue lease owner and expiration behavior;
- attempt rows and worker execution IDs;
- periodic heartbeats during the Spark work;
- attempt-scoped output row counts;
- independent commit per successful video;
- committed views and downstream gold visibility;
- no duplicate committed attempt;
- no benchmark table used as the sole production output;
- failure classification and watchdog recovery; and
- operational metrics grouped by `processing_engine`.

### 13.4 Original-path regression in Fabric

Run or observe one controlled `NOTEBOOK_04` item through
`pc-dispatcher-00`. Confirm the explicit routing parameter did not change its
normal claim, worker, or publication behavior.

### 13.5 Contention validation

While one executor worker is active, verify another dispatcher sharing the
global limit does not admit work beyond the configured total. Do not create
artificial overlapping production load if it risks current work; use
development test rows and a controlled window.

## 14. Acceptance criteria

Implementation is complete only when all conditions hold:

1. `processing_engine` is migrated and documented.
2. Registration defaults safely and supports explicit executor routing.
3. One shared claim implementation filters conditionally by engine.
4. `pc-dispatcher-00` explicitly passes `NOTEBOOK_04`.
5. `17_process_video_executor` preserves the production attempt,
   heartbeat, output, commit, retry, and recovery contracts.
6. `pc-dispatcher-executor-00` exists in Fabric and is fully configured.
7. The executor recurring schedule is disabled pending explicit approval.
8. Both pipeline exports were downloaded from Fabric and checked in.
9. README instructions match the observed UI and can be followed manually.
10. Targeted local validation passes.
11. One executor canary completes with persisted production evidence.
12. One original-path regression item completes normally.
13. Shared admission prevents unsafe overlap.
14. No work is silently lost, double-published, or stranded behind an
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
2. Allow an active executor worker to complete, or follow the documented
   cancellation and watchdog-expiry procedure.
3. Stop routing new registrations to `EXECUTOR_PARTITION`.
4. Change future manifests/default routing to `NOTEBOOK_04`.
5. For queued, unleased executor rows, perform an audited routing migration to
   `NOTEBOOK_04` only after validating runtime/config compatibility.
6. Do not change the engine of an actively leased row.
7. Run watchdog and reconciliation.
8. Confirm no executor leases or nonterminal attempts remain.
9. Keep notebook 17, the pipeline export, and audit evidence for diagnosis;
   do not delete them as part of an emergency rollback.

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
- original-path regression result;
- whether the executor schedule remains disabled;
- README section links; and
- any deviations from this plan with their reason.

