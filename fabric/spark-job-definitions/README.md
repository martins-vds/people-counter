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

## Release admission

SDK release identity is deployment-independent. Its v2 embedded identity
contains release metadata only; workspace, Lakehouse, Environment, SJD IDs,
and deployment intent belong in deployment configuration and detached evidence.
Use the same approved wheel across targets. Runtime admission checks the
evidence against the selected configuration (including explicit manual artifact
bindings), rather than requiring a target-specific SDK build. The reference
production role IDs are resolved into deployment state, not SDK defaults.
Existing v1 wheels require a new approved release and matching deployment
evidence to adopt this separation; do not edit an installed wheel in place.

Stable production registration resolves the installed SDK's immutable detached
manifest and postpublish receipt before creating `READY` work. A supplied
release digest must match that verified release. Bulk registration validates
the whole partition before writing any rows.

Stable claims verify the Dispatcher binding and reject incompatible eligible
work before creating attempts, envelopes, or leases. Dispatcher also verifies
its complete manifest/receipt arguments before calling claim. Local and
benchmark stores retain their existing behavior unless an expected release
digest is supplied explicitly.

For the optional legacy event-intake deployment, `scripts/deploy_sjd_operations.py`
accepts `--intake-pipeline-id` and `--registration-notebook-id` together.
It derives the pipeline and Dispatcher configuration from the same
`--release-manifest-sha256` value and verifies readback before enabling schedules.
The registration notebook resolves the SDK release itself; the pipeline value
is a compatibility assertion, not an independent release authority.

For a new deployment, first use `--provision-only --config <target-config>
--output <resolved-deployment.json>`. This creates or uniquely resolves all six
SJDs, verifies target-bound definitions, disables existing schedules before
updates, and captures Fabric's assigned IDs. No release receipt or preassigned
SJD IDs are needed at this stage. Retry with the resolved output to pin those
IDs. Conflicting IDs, duplicate names, and ambiguous discovery stop the run.
The later configuration pass requires both real release hashes and resolved
reporting IDs. All schedule arguments use that same target; enablement occurs
only after every disabled schedule's readback matches.

Drain or explicitly quarantine/re-register incompatible work during a release
change. Never update an existing work item's immutable release digest in place.

The published Environment must also match `reviewed_compute_settings()`:
Runtime 2.0, fixed allocation, two 8-core/56-GB executors, speculation disabled.
The deployment verifier checks published compute before binding/enabling jobs.
Dispatcher checks the live Python/Spark/Java and fixed-allocation contract before
claiming, rather than discovering an incompatible pool after taking a lease.

### Recover an investigated failed claim

`abandon-failed-claim` is an operator-only recovery command for a terminal
driver that failed before producing output. Independently verify the owning
job has terminated before invoking it. Supply the exact batch ID, owner,
envelope digest, failed job ID, operator and reason. The command refuses sealed,
staged or published output and changed ownership/fences. It invalidates the
old work fences, releases only that batch, and records the recovery evidence
in `last_error`; immutable work fields and input files remain unchanged.
Do not use it as automatic cleanup for a running or uncertain worker.
Use `quarantine` separately when the released work's SDK release is obsolete.
