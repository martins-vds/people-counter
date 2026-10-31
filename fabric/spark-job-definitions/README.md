# Canonical Spark Job Definitions

Terraform reads these files for all seven production roles. Each role owns
`Main/main.py`. All roles use the metadata template under `common/`.

The Microsoft Fabric Terraform provider substitutes the target Lakehouse and
Environment IDs through its documented Go-template definition tokens. No
committed file contains an environment-specific artifact ID.

Tests compare these files with `build_sjd_definition()`. This check keeps each
generated payload equal to its Terraform source.

## Release admission

Stable production registration resolves the installed SDK's immutable detached
manifest and postpublish receipt before creating `READY` work. A supplied
release digest must match that verified release. Bulk registration validates
the whole partition before writing any rows.

Stable claims verify the Dispatcher binding and reject incompatible eligible
work before creating attempts, envelopes, or leases. Dispatcher also verifies
its complete manifest/receipt arguments before calling claim. Local and
benchmark stores retain their existing behavior unless an expected release
digest is supplied explicitly.

For the legacy event-intake deployment, `scripts/deploy_sjd_operations.py`
requires both `--intake-pipeline-id` and `--registration-notebook-id`.
It derives the pipeline and Dispatcher configuration from the same
`--release-manifest-sha256` value and verifies readback before enabling schedules.
The registration notebook resolves the SDK release itself; the pipeline value
is a compatibility assertion, not an independent release authority.

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
