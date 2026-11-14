# Canonical Spark Job Definitions

Terraform reads these files for all six production roles. Each role owns
`Main/main.py`. All roles use the metadata template under `common/`.

The Microsoft Fabric Terraform provider substitutes the target Lakehouse and
Environment IDs through its documented Go-template definition tokens. No
committed file contains an environment-specific artifact ID.
The metadata template also receives `CommandLineArguments`, including the
selected `--files-root` when it differs from the stable default. This keeps
direct job runs aligned with scheduled invocations.

Tests compare these files with `build_sjd_definition(config=...)`. This check keeps each
generated payload equal to its Terraform source.

## Manual workspace binding

For the browser-operated path, follow the
[manual deployment guide](../../docs/manual-portal-deployment.md).
The six stable runtime entry points accept an explicit prefix:

```text
--artifacts <workspace-id> <lakehouse-id> <environment-id> <role arguments>
```

All three IDs must be canonical, non-nil UUIDs. The entry points compare them
against NotebookUtils `currentWorkspaceId`, `defaultLakehouseId`,
`defaultLakehouseWorkspaceId`, `environmentId`, and `environmentWorkspaceId`
before dispatch. Missing or mismatched context fails closed. The selected
binding is also used for video/model localization and distributed-cache URIs.
Refresh rejects a different `--workspace-id`.

The older `--manual-artifacts` spelling remains an alias. Omitting the prefix
derives and validates the attached runtime context; there is no reference
deployment fallback. Local callers must supply explicit configuration.
`--files-root Files/<deployment-path>/` selects a validated deployment state
directory when it differs from the default. File operations remain confined
to that exact root; the table suffix and reviewed compute guards remain.

Cutover tooling and its SJD have been removed. Use Control `bootstrap` for
greenfield initialization and keep all schedules disabled until acceptance.
Removal does not delete existing Lakehouse tables or migrate old work.

## SDK compatibility

Upload the SDK wheel to the Fabric Environment, publish it, and start fresh
sessions. Keep the live driver and executors on the same SDK version; the
submitting SDK version may differ. No embedded
identity, detached manifest, postpublish receipt, or versioned release directory
is required. Target IDs come only from configuration and runtime context.

Registration records the submitting SDK version and release digest as audit
provenance. Eligibility depends on supported work/payload schemas, inference
settings, and model requirements, not equality with that historical version
or digest. Bulk registration still validates malformed inputs and immutable
identities before writing rows.

Claims classify each eligible READY item before runtime grouping and batch
admission. Supported work can proceed while unsupported work becomes
`BLOCKED_INCOMPATIBLE`, with reason, required contract, evaluating SDK version,
and timestamp recorded. Blocking does not consume attempts or create leases,
envelopes, or fences. Process also verifies contract support for existing claims.
Legacy work without contract versions is interpreted as v1 without rewriting
its immutable content. Missing model files, service errors, and corrupt model
bytes are operational/integrity failures, not SDK incompatibility.

Dispatcher and Process need no release-file arguments. Legacy release-file
flags and the SDK `expected_release_digest` claim argument are deprecated;
they do not impose work-version equality. SQLite and Delta use the shared
contract evaluator, with the production Fabric RT-DETR/OSNet model support
restriction retained.

After upgrading, run Control `status --work-id <id>` to inspect the structured
diagnostics, then `recheck-compatibility --work-id <id> --operator <operator>
--reason "SDK upgrade"`. Repeat `--work-id` for multiple selected items.
Only blocked work is eligible: supported items return to READY, unsupported
items remain blocked with refreshed diagnostics. Recheck preserves attempts,
fences, retry budgets, immutable inputs, and original submission provenance.
Recovery, reconciliation, replay, and ordinary dispatcher runs do not unblock
items. SDK callers can use `get_work` and `recheck_compatibility`; the local
CLI uses the same commands with `--database <path>`.

Stop old writers and restart sessions before upgrading existing state.
Bootstrap performs an idempotent SQLite/Delta migration for the new status and
diagnostic/provenance fields; do not recreate tables or delete work to upgrade.

For the optional legacy event-intake deployment, `scripts/deploy_sjd_operations.py`
accepts `--intake-pipeline-id` and `--registration-notebook-id` together.
It updates the registration notebook to derive provenance from its installed
SDK version, without reading release evidence. No pipeline release digest needs
to be configured.

For a new deployment, first use `--provision-only --config <target-config>
--output <resolved-deployment.json>`. This creates or uniquely resolves all six
SJDs, verifies target-bound definitions, disables existing schedules before
updates, and captures Fabric's assigned IDs. No release receipt or preassigned
SJD IDs are needed at this stage. Retry with the resolved output to pin those
IDs. Conflicting IDs, duplicate names, and ambiguous discovery stop the run.
The later configuration pass requires resolved reporting IDs, not release
hashes. All schedule arguments use that same target; enablement occurs
only after every disabled schedule's readback matches.

Older compatible work needs neither quarantine nor re-registration after an
SDK upgrade. Retained backfill submission plans resume with their original
IDs and provenance. Never update an existing work item's immutable digest.

The published Environment must also match `reviewed_compute_settings()`:
Runtime 2.0, fixed allocation, two 8-core/56-GB executors, speculation disabled.
The deployment verifier checks published compute before binding/enabling jobs.
Dispatcher checks the live Python/Spark/Java, executor resources, and speculation
settings before claiming. Runtime validation supports both dynamic and fixed
executor allocation. It waits up to 30 seconds for enabled dynamic allocation
and `spark.dynamicAllocation.minExecutors`, then up to 30 seconds for
`spark.executor.instances`. Explicitly disabled dynamic allocation skips the
first wait and ignores stale dynamic minimums. If neither configuration becomes
available, a warning records the fallback to a minimum of one executor (not
job exit status 1). Invalid executor counts fail explicitly.
This only selects the discovery minimum: actual executor inventory, warm-up,
and measured memory checks still determine safe processing concurrency.
The deployment template above remains the reviewed fixed-allocation default.

### Recover an investigated failed claim

`abandon-failed-claim` is an operator-only recovery command for a terminal
driver that failed before producing output. Independently verify the owning
job has terminated before invoking it. Supply the exact batch ID, owner,
envelope digest, failed job ID, operator and reason. The command refuses sealed,
staged or published output and changed ownership/fences. It invalidates the
old work fences, releases only that batch, and records the recovery evidence
in `last_error`; immutable work fields and input files remain unchanged.
Do not use it as automatic cleanup for a running or uncertain worker.
Use `quarantine` only for a deliberate operator decision to terminalize READY
work. It is not the SDK upgrade or compatibility recheck mechanism.

### Recover an investigated retained writer lock

If startup reports `Timed out acquiring ..._locks`, use the **observed**
owner token from that error to identify the earlier writer in its job logs.
The attempted token belongs to the waiting job, not the retained owner.
An old SDK failure can leave a lock retained and prevent the new SDK's
automatic schema migration before `recover`, `bootstrap`, or claim runs.

Before clearing, pause recurring writer schedules and independently confirm
the owning job and all other writers have terminated and cannot still commit.
Lock age alone is not sufficient. Then invoke Control once with the normal
target arguments and:

```text
clear-stale-lock --expected-owner-id <observed-owner-token>
```

This operator-only command bypasses control-store construction and automatic
schema migration. It reads the existing lock directly, requires the exact
owner, clears by compare-and-set, and verifies the readback. It does not
migrate tables, recover claims, or unblock incompatible work. A missing lock
table or changed owner fails explicitly; it never creates or takes over a lock.
Older wheels may still bootstrap before this command; install the updated
SDK and start a fresh session first.

After a successful clear, run Control `recover` with its normal target arguments.
That run automatically adds missing compatibility columns before recovery.
Restore the recurring Control command to `recover`, then resume the approved
schedules and check the next Dispatcher result. Do not schedule lock clearing
or delete work, batches, or lock rows as a substitute.
