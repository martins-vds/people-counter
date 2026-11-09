# Explicit backfill: prepare, preview, submit

The [manifest package notebook](../notebooks/manifest_package_operator_tutorial.ipynb)
only prepares local packages. It never uploads videos or submits work.

The separate operator command publishes the package and registers explicit work
with the deployed **Control SJD**. It does not use the local SQLite Control CLI,
run inference, enable schedules, or provision infrastructure.

## One-time setup: deployment administrator

1. Deploy and bootstrap the production SJDs with the approved published
   Environment. Keep Dispatcher, Reconciliation, Gold, and Refresh configured.
2. Select **explicit intake** for these videos. Disable or coordinate competing
   arrival-driven intake before publication. This command cannot detect an
   independent arrival rule or reconcile work IDs created by another intake
   policy; do not use it to register the same processing run twice.
3. Expose the source ADLS filesystem through a shortcut such as `Files/videos`
   in the SJD's default Lakehouse. Keep approved models under `Files/models/`.
   Verify that the Fabric runtime identity can read both. Local operator access
   does not establish runtime access.
4. Copy [backfill-target.example.json](../samples/backfill-target.example.json)
   to an operator configuration file. Replace its placeholders with the actual
   workspace, Lakehouse, Environment, Control SJD, storage target, and approved
   inference profile. Use the reviewed effective configuration hash and runtime
   key for that profile. Include every required model artifact and its SHA-256.
   Do not copy demonstration timestamps, video paths, or work IDs into it.
   For monitoring, `table_directory` defaults to `Tables/dbo` for schema-enabled
   Lakehouses. Set it to `Tables` for a non-schema Lakehouse, or `Tables/<schema>`
   if production tables use another schema.
5. Install the same SDK release as the target Environment, plus the publisher
   dependencies, and authenticate:

   ```bash
   uv sync --extra publisher
   az login
   ```

Storage operations use `DefaultAzureCredential`; Fabric job operations use the
attended Azure CLI identity. Both identities must belong to the intended tenant.
The operator needs ADLS publication access, OneLake model/release/shortcut read
access, OneLake backfill-intake write access, and permission to invoke/read the
Control SJD. Use narrowly scoped permissions. Never put secrets in configuration.
With `--wait`, the Azure CLI identity also needs read access to the production
`people_counter_sjd_work` Delta table in OneLake.

No manually copied release digest is required. Submission reads the immutable
detached release manifest and postpublish receipt from the target Lakehouse,
validates them against the local SDK release, and checks the Environment binding.
Control independently checks release compatibility before registering work.

See the [SJD deployment guide](../fabric/spark-job-definitions/README.md)
and [manual deployment guide](manual-portal-deployment.md) for deployment details.

## Every package: submission operator

Keep the notebook's complete package directory and unchanged source videos.
Relative video paths under `--video-root` must remain unchanged.

### 1. Preview (offline)

From the repository root:

```bash
uv run python -m people_counter.backfill_operator \
  --manifest-package-dir prepared/camera-17 \
  --video-root /mnt/source-videos \
  --config config/backfill-target.json \
  --state-dir state/camera-17-backfill
```

The command validates the package and rehashes the videos. It prints the target,
profile, package digest, video count, and partition count. It does not authenticate,
contact Azure/Fabric, or write submission state. Review these values before
proceeding. Cloud readiness and release checks occur only on submission.

### 2. Submit

Repeat the same command with `--submit`:

```bash
uv run --extra publisher python -m people_counter.backfill_operator \
  --manifest-package-dir prepared/camera-17 \
  --video-root /mnt/source-videos \
  --config config/backfill-target.json \
  --state-dir state/camera-17-backfill \
  --submit
```

The command:

1. Resolves published release evidence and verifies model artifact hashes.
2. Publishes unchanged videos first and final source manifests last, using the
   existing checkpointed publisher.
3. Verifies final manifests through the Lakehouse shortcut, including source
   metadata and ETag, and verifies each shortcut video's size and SHA-256.
4. Builds registration records automatically. It preserves camera/location,
   capture time, timezone, duration, and directed counting line, and uses mounted
   `/lakehouse/default/Files/...` video paths rather than ADLS URIs.
5. Creates stable work IDs bound to source identity, target, payload, configuration,
   and release. It writes immutable partitions of at most 1,000 records under
   `Files/people-counter/sjd/v1/intake/backfill/`, calculates their exact digests,
   and submits them to Control with the explicit artifact binding.
6. Waits for every Control job to complete. Any failed, cancelled, deduplicated,
   or timed-out job prevents a successful result.

Retain the entire state directory. It contains publication reports/checkpoints,
the exact registration plan and partitions, source publication evidence, job
locations/status, and the final registration summary. It must remain outside
the package directory.

### 3. Optionally wait for processing

Add **both** `--submit --wait` to the same command:

```bash
uv run --extra publisher --extra demo python -m people_counter.backfill_operator \
  --manifest-package-dir prepared/camera-17 \
  --video-root /mnt/source-videos \
  --config config/backfill-target.json \
  --state-dir state/camera-17-backfill \
  --submit --wait
```

| Flags | Behavior |
|---|---|
| None | Offline preview only |
| `--wait` | Offline preview only; the flag has no effect without `--submit` |
| `--submit` | Publish and wait for Control registration; do not wait for inference |
| `--submit --wait` | Publish, register, and monitor every submitted work ID until processing commits |

Wait mode reuses the repository's local DuckDB Delta reader from the `demo`
extra. It checks dependencies, table access, and schema **before** publication.
The DuckDB Azure/Delta extensions must be installed or downloadable. Authenticate
with Azure CLI in the intended tenant; monitoring uses its credential chain.

Progress goes to **stderr**, while the final machine-readable JSON remains on
**stdout**. Publication and registration stages print their status. During
processing, the operator sees completed/total counts and state totals every
15 seconds, for example:

```text
Processing: 12/20 committed; LEASED=3, READY=5, SUCCEEDED=12
```

Completion requires every exact work ID in the retained registration plan to
have `status == SUCCEEDED` and a nonempty `committed_attempt_id`. A completed
Spark job, unrelated successful work, or a success row without a committed
attempt does not satisfy this check. Missing work remains visibly pending.
Dead, failed, quarantined, or cancelled work stops the wait with an error and
its work ID/status/last error. Access and query errors also stop the command.

The last observed counts and committed attempt IDs are saved in
`processing-summary.json`. Successful waiting returns `status: processed` and
`processing_complete: true`. This confirms **processing**, not Gold/report refresh.
Continue with the reporting checks below.

**Press Ctrl+C to leave the wait.** The command exits with code 130 and does not
send any cancellation to Fabric. Accepted remote work continues. If interrupted
during publication or registration, some partitions may not yet be submitted.
Repeat the same command, state directory, and `--submit --wait` to resume;
retained job locations prevent duplicate submissions. There is no processing
wait timeout: if schedules are paused or capacity is unavailable, progress can
remain pending until the deployment is repaired or the operator leaves the wait.

### 4. Retry and recovery

Repeat the **same command with the same state directory** after interruption.
Verified publications are reused. Existing job locations are polled instead
of resubmitting completed or in-flight partitions. Different package bytes,
targets, profiles, or releases cannot reuse that state directory.

If a request failed before its job location was saved, the command stops with
`submission outcome unknown`. An administrator must inspect Fabric job history
and recover the matching job location in that partition's job-state JSON
(`location`, with `status` changed from `submitting` to `submitted`). Only after
confirming that no job was accepted may the administrator remove that specific
job-state file to permit a new submission. Do not delete the whole state directory.
Failed terminal jobs remain visible; investigate them before explicitly permitting
a replacement submission. Stable work IDs make unchanged registration retries
idempotent; they do not reset work that has already progressed.

Run only one command per state directory at a time. Changing a profile or SDK
release is a new processing run, not recovery of the previous run. Do not use a
fresh state directory to bypass a registration conflict.

### 5. Verify reporting (and processing if you did not use `--wait`)

`status: registered` means the Control registration jobs completed, **not** that
inference or reporting completed. Let the deployed Dispatcher run Process.
Use the exact work IDs in the registration plan to check
`people_counter_sjd_work`: successful work needs `status == SUCCEEDED` and a
non-null `committed_attempt_id`. Require the matching `(work_id, attempt_id)` in
`people_counter_sjd_gold_video`. Investigate retries, `DEAD` work, missing IDs,
duplicates, and `last_error`. Do not manually claim batches.

Require successful Reconciliation, Gold, and Refresh runs, no unresolved
reconciliation errors, and an empty semantic-refresh outbox before declaring
reporting complete. The [production runbook](../notebooks/fabric/README.md)
contains the operational checks.

For **publication only**, the existing `publish-manifests` command is unchanged.
For arrival-driven deployments, keep using the
[arrival-driven walkthrough](../notebooks/customer_end_to_end_demo.md) instead.
