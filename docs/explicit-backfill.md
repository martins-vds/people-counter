# Explicit backfill: prepare, preview, submit

For browser-based camera calibration, metadata editing, package generation,
profile setup, and controlled publication/submission, use the
[local operator portal](operator-portal.md). The CLI workflow below remains available.

The [manifest package notebook](../notebooks/manifest_package_operator_tutorial.ipynb)
prepares packages locally or directly in a Fabric Lakehouse. It never publishes
videos or submits work.

For **Fabric preparation**, import the first notebook as PySpark, attach the
default Lakehouse and a compatible published SDK Environment, and ensure
`ffprobe` is available. Set `FABRIC_MODE = True` and configure:

If Fabric raises `FileNotFoundError` for `ffprobe`, add **`ffmpeg` from Conda**
to the attached Environment under **Libraries > External repositories >
Add library from public repository**. Select an approved version, save and
publish, then stop and restart the notebook session with that Environment.
Do not install the Python packages `ffmpeg` or `ffmpeg-python` as a substitute:
they do not provide the executable. Verify the fresh session before rerunning:

```python
import shutil
import subprocess

print(shutil.which("ffprobe"))
subprocess.run(["ffprobe", "-version"], check=True)
```

See [Fabric environment library management](https://learn.microsoft.com/en-us/fabric/data-engineering/environment-manage-library).
The notebook checks executable availability before generating metadata.

| Setting | Default mounted path | Purpose |
|---|---|---|
| `VIDEO_ROOT` | `/lakehouse/default/Files/backfill/source-videos` | Source-tree root containing the selected shortcut or folder |
| `PACKAGE_DIR` | `/lakehouse/default/Files/backfill/manifest-package-tutorial` | Generated package consumed by the second notebook |
| `CATALOG_PATH` | `/lakehouse/default/Files/backfill/config/camera_catalog.csv` | Automatically generated tutorial camera metadata |
| `EXCEPTION_INVENTORY_PATH` | `/lakehouse/default/Files/backfill/config/video-inventory-exceptions.csv` | Automatically generated tutorial timestamp override |

The notebook identity needs read access to the shortcut and write access to
the package/configuration folders. These folders must be outside the source
shortcut. No repository checkout, uv, or Azure CLI login is required.
The only data input is the same three tutorial clips. Set `PARTITION_PREFIX`
in the Fabric or local branch to their relative folder beneath `VIDEO_ROOT`.
It defaults to `samples/`, but a different shortcut name or nested folder such
as `tutorial-videos/` or `footage/demo` also works. A trailing slash is optional;
empty, absolute, URL, and parent-traversal paths are rejected. The generated
camera catalog uses the normalized prefix, and the timestamp override points to
`<prefix>/three_people_walking.mp4`. Other tutorial camera metadata and the
override timestamp are unchanged. No catalog upload is required.
After changing the prefix, rerun setup and subsequent cells to regenerate the
package. Paths and asset identities are relative to `VIDEO_ROOT`; preserve that
layout for the second notebook. Use dedicated tutorial configuration/output
folders because the metadata cells rewrite their example CSVs. The sample
calibration is not intended for arbitrary production footage.
SQLite preparation state stays driver-local and temporary; the completed
package is retained in the Lakehouse.

After its step 9, continue with the
[explicit backfill operator notebook](../notebooks/explicit_backfill_operator_tutorial.ipynb)
for a guided **Fabric PySpark notebook** workflow. Attach the deployed default
Lakehouse and matching published SDK Environment. Set its `PACKAGE_DIR`,
`VIDEO_ROOT`, and `MODELS_DIR` to Python-accessible mounted Lakehouse paths,
for example `/lakehouse/default/Files/...`, preserving relative video paths.
Set the real Environment/Control SJD IDs and source ADLS account/filesystem.
Workspace and Lakehouse IDs come from the default Lakehouse runtime context.
Set `FABRIC_MODE = True` in this second notebook too. Its default package and
video paths match Fabric preparation above, so it consumes the first notebook's
output without copying. If either path was customized, use the same path in
both notebooks and preserve the video-relative layout. The preparation shortcut
is a source reader; the deployment's `shortcut_path` such as `Files/videos`
still exposes the ADLS filesystem used for published `incoming/...` objects.

The notebook generates its catalog from the actual files under `MODELS_DIR`,
prints every required artifact's SHA-256, previews the package, and triggers
publication/Control registration only after explicit confirmation. No zero-hash
sample catalog or repository checkout is used. Catalog generation is not model
approval; use the same reviewed bytes deployed under `Files/models/`.
Missing artifacts and changed existing catalog definitions fail rather than
substituting hashes or overwriting approvals. Use a new catalog for a reviewed
revision. Submission remains disabled by default.

With `--fabric`, the shared command uses `notebookutils` storage/Fabric tokens
instead of `DefaultAzureCredential`/Azure CLI. It refuses to submit to a different
default Lakehouse. Optional `--wait` uses the active Spark session and preflights
the production work Delta table before publication; it does not need DuckDB.
Publish the matching SDK (including `--fabric` support), `azure-identity`, and
`azure-storage-file-datalake` in the attached Environment before running.
Service-principal/pipeline token permissions can differ from interactive user
permissions; failures propagate without a credential fallback.

Keep `STATE_DIR` on the Lakehouse, outside the immutable package, with exclusive
operator access. Job locations, registration plans/partitions, and publication
evidence remain there across sessions. Fabric mode uses a temporary local SQLite
publication cache to avoid SQLite file locking on a OneLake mount; on retry it
revalidates remote publications and reuses retained registration jobs. Do not
delete persistent state. This is an attended tutorial: enable `RUN_SUBMISSION`
and set `SUBMISSION_CONFIRMATION` to `SUBMIT <target>` in its options cell.
Fabric mode does not request interactive stdin. Do not schedule the tutorial
with submission enabled.

For the **local preparation notebook**, run `uv sync --extra experiments` and select the
repository's `.venv` as its kernel. Preparation subprocesses use the kernel's
Python interpreter without syncing the environment. Do not replace or sync
that environment while the kernel is running. If an earlier preparation run
recreated `.venv`, restart the kernel, select the current `.venv`, and rerun
the notebook from step 1.

For local use of the backfill notebook, set `FABRIC_MODE = False` and run it
from the repository root or a subfolder. Its local branch finds the repository
root and defaults to the manifest tutorial's package/video tree, `offline-models`,
`config/backfill-catalog.json`, and persistent local state. With `True`, the
Fabric branch instead sets all paths to the mounted Lakehouse layout.
Edit paths, target, and profile in the branch you use; changing modes replaces
all path defaults together. Supply real workspace/Lakehouse IDs and deployment
bindings in local mode, and install extras before starting its
kernel: `uv sync --extra experiments --extra publisher --extra demo`.
The `demo` extra supplies the local processing monitor. The local CLI examples
below remain unchanged; omit `--fabric` outside Fabric.

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
   to an **administrator-only deployment binding**. Replace its placeholders
   with the actual workspace, Lakehouse, Environment, Control SJD, and storage
   target. It contains no inference settings or manually calculated hashes.
   For monitoring, `table_directory` defaults to `Tables/dbo` for schema-enabled
   Lakehouses. Set it to `Tables` for a non-schema Lakehouse, or `Tables/<schema>`
   if production tables use another schema.
5. Install the same SDK release as the target Environment, plus the publisher
   dependencies, and authenticate:

   ```bash
   uv sync --extra publisher
   az login
   ```

6. Generate the operator's approved catalog from the **same reviewed model
   bytes deployed under `Files/models/`**, not a different download or export:

   ```bash
   uv run python -m people_counter.backfill_operator prepare-catalog \
     --target production \
     --target-config config/production-binding.json \
     --models-dir /mnt/approved-models \
     --profile rtdetr-r18-onnx-cpu \
     --profile rtdetr-r50-onnx-cpu \
     --output config/backfill-catalog.json
   ```

   Repeat `--profile` for each approved preset. Generation requires all four
   artifacts for every selected profile, computes their SHA-256 values by
   streaming the files, expands defaults, and derives profile/runtime
   identities. Missing files fail generation. It does not download, export,
   upload, or approve models automatically. The deployment administrator reviews
   the bytes and distributes the resulting catalog; **operators do not edit it**.
   The command can add another named target to the same catalog, but refuses to
   overwrite a different existing target/profile definition. Generate a new
   catalog for an intentional revision.

Supported presets are:

| Name | Detector | Format | Device | Default batch | Default sampling |
|---|---|---|---|---|---|
| `rtdetr-r18-onnx-cpu` | R18 + OSNet | ONNX | CPU | 1 | 1 FPS |
| `rtdetr-r50-onnx-cpu` | R50 + OSNet | ONNX | CPU | 1 | 1 FPS |
| `rtdetr-r18-pytorch-cpu` | R18 + OSNet | PyTorch | CPU | 1 | 1 FPS |
| `rtdetr-r50-pytorch-cpu` | R50 + OSNet | PyTorch | CPU | 1 | 1 FPS |

These are supported configurations, not performance or quality certifications
for a particular camera or deployment. All presets use FP32 and detection
threshold 0.6. CPU thread allocation continues to follow the worker's existing
placement-safe policy; the names do not promise a fixed thread count.

Supervision is a library, not a model preset. RF-DETR/BoT-SORT is supported by
the broader SDK but not the current production Fabric model localizer.
Unsupported names, GPU settings, and incomplete artifact sets are rejected,
not silently mapped to R50 or another implementation.

### Automatically derived identities

The shared [inference profile resolver](../src/people_counter/inference_profiles.py)
is used by catalog generation, operator selection, registration validation,
and worker execution. A profile name resolves to authoritative settings rather
than encoding settings that the worker must guess from a string.

- **Model hashes:** generated from the approved artifact bytes; checked against
  the target's actual model bytes again before publication.
- **Profile hash:** hashes the canonical effective settings, explicit defaults,
  and required model hashes.
- **Runtime key:** a separate, domain-tagged digest of those effective inference
  settings and model hashes. It groups compatible work; per-video lines and
  source paths do not split runtime groups.
- **Configuration hash:** generated for each work item from the resolved profile
  hash and that video's directed counting line. Changing inference settings,
  model bytes, or line direction changes the configuration identity.

Operators never calculate or supply these values. Registration and claim
validation reject mismatched identities; workers reject resolved settings that
disagree with the selected profile. Existing legacy registered work retains its
original execution behavior.

Storage operations use `DefaultAzureCredential`; Fabric job operations use the
attended Azure CLI identity. Both identities must belong to the intended tenant.
The operator needs ADLS publication access, OneLake model/shortcut read
access, Fabric API access to list the target Lakehouse's shortcuts,
OneLake backfill-intake write access, and permission to invoke/read the
Control SJD. Use narrowly scoped permissions. Never put secrets in configuration.
With `--wait`, the Azure CLI identity also needs read access to the production
`people_counter_sjd_work` Delta table in OneLake.

No manually copied release digest or OneLake release evidence is required.
The submitting SDK version and release digest are audit provenance, not
eligibility rules. Compatibility depends on supported work/payload schemas,
inference settings, and model requirements. Target IDs come from configuration
and runtime context; no deployment identity is embedded in the wheel.

Before any ADLS publication or registration-plan write, submission reads the
target Lakehouse's shortcut definitions through the Fabric REST API. The exact
`shortcut_path` must resolve to one ADLS Gen2 shortcut whose account matches
`storage_account` and whose subpath is the configured `filesystem` container
root, not an `incoming/` subfolder. Missing, ambiguous, wrong-account/container,
non-ADLS targets, or API failures stop the run before publication. This check
runs on retries too; offline preview still makes no network requests.

On a mismatch, have an administrator align the shortcut with the reviewed
publication destination. The SDK does not repoint shortcuts or move existing
files to conceal a mismatch. Preserve retry state. Post-publication manifest
and video checks through OneLake remain required: matching shortcut metadata
does not prove that its connection credentials can read the files.

If submission fails while reading/verifying OneLake files, creating intake
directories, or uploading registration partitions, the error identifies the
operation, full `abfss://` resource URI, Azure exception type, HTTP status,
Azure error code, and request ID when available. The original Azure exception
is retained as the cause. Model checks precede publication.
Inspect the reported URI and restore the approved model at the reported
`Files/models` path, or fix the source shortcut. Do not bypass hash validation. For HTTP 401/403,
check the notebook/operator identity's OneLake access. Retain the state directory
for retries; a later source-shortcut read failure can occur after publication.

After `Verified Lakehouse source ...`, progress names each registration partition
and its intake path before writing it. A failure at this stage is an intake
directory/upload/readback failure, not a missing model or failed ADLS publication.
The SDK creates only user directories below `Files/`; it must not attempt to
create the Fabric-managed Lakehouse or `Files` root through the storage API.
Partition upload must also create the file itself before appending data.
The Azure Data Lake SDK skips file creation with `upload_data(overwrite=False)`,
which can return `PathNotFound` even when the parent directory exists.
The publisher uses `overwrite=True` with `MatchConditions.IfMissing`: the
conditional create cannot replace an existing file, and upload/flush use the
new file's ETag. An existing partition is accepted only after exact byte
comparison; different contents remain an immutable conflict.
See [OneLake managed folders](https://learn.microsoft.com/en-us/fabric/onelake/onelake-api-parity#managed-onelake-folders).
For a 404, inspect the reported Lakehouse and exact parent/partition path.
For 401/403, check the execution identity's OneLake write permissions.
Keep `registration-plan.json`, publication evidence, and any job-state files.
Repair the cause and retry with unchanged inputs and the same state directory.
Do not delete successful ADLS publications or manually create dummy partitions.
If the repair requires a newer SDK version, retain the old run's state.
Submission verifies and reuses the original registration plan, work IDs,
partitions, and submitting SDK provenance when the inputs still match.
Changed packages, targets, inference settings, or model requirements remain
immutable-plan conflicts; do not delete state to bypass validation.

See the [SJD deployment guide](../fabric/spark-job-definitions/README.md)
and [manual deployment guide](manual-portal-deployment.md) for deployment details.

### SDK upgrades

Upload the new wheel to the Fabric Environment, publish it, and start a fresh
notebook/SJD session. Keep the live driver and executors on the same SDK.
An older submitting SDK version does not by itself prevent processing.
No release directory, detached manifest, postpublish receipt, or custom
embedded identity is needed. The SDK does not read those files.

If a session still reports a read of `detached-manifest.json`, it is running
an older SDK or notebook. Update the Environment and restart the session.
Legacy release-file CLI arguments are accepted with a deprecation warning
but are not used; newly generated schedules omit them.

Model files, source shortcuts, and the normal job state/intake directories
are still required. They are processing inputs and state, not SDK packaging.
Do not delete retry state to force an upgrade. Work registered by an older
SDK, including work with an old detached-manifest digest, remains immutable
and can run if its contract is supported. Legacy work without explicit contract
versions is evaluated as v1; an unknown historical submitting version is not
invented from its digest.

Dispatcher classifies READY work individually before selecting a batch:

```text
READY -> supported contract -> LEASED -> processing
READY -> unsupported contract -> BLOCKED_INCOMPATIBLE
```

One unsupported item does not prevent compatible items from running.
Blocked work records the reason code, required contract, evaluating SDK version,
and evaluation time in `compatibility_json` and `last_error`. No execution
attempt, lease, or fence is allocated for blocking. Unsupported model formats
or pipeline requirements are compatibility failures; missing files, denied
access, and corrupt model bytes remain operational/integrity errors.

After installing an SDK that supports the required contract, explicitly invoke
the Control SJD with selected IDs and operator attribution:

```text
status --work-id <work-id>
recheck-compatibility --work-id <work-id> --operator <operator> --reason "SDK upgrade"
```

Repeat `--work-id` to select multiple items. Only blocked items are eligible for
recheck; missing, leased, or otherwise ineligible IDs are refused. Supported
items return to READY, unsupported items remain blocked with refreshed
diagnostics. Recheck preserves submission provenance, payload/configuration
identities, attempts, fences, and retry budgets. Recovery, reconciliation,
ordinary replay, repeated submission, and dispatcher runs do not unblock work.

The SDK equivalents are `store.get_work(work_id)` and
`store.recheck_compatibility(work_ids, operator=..., reason=...)`.
For SQLite use `pc-control-sjd --database <path> status --work-id <work-id>`
or the same prefix followed by `recheck-compatibility`.
Disable old writers and restart SDK sessions before migrating existing control
state; SQLite and Delta bootstrap migrate the new state fields idempotently.

Do not publish different builds under the same version. Keep artifact checksums
in release/distribution tooling, not runtime admission.

### Use assigned IDs, not reference-deployment defaults

SJD IDs do not exist until Fabric creates the items. Do not put guessed IDs
in the SDK or rebuild a wheel after uploading a job. The administrative
publication notebook resolves the six uploaded roles by their configured
names and records their IDs in `Files/deployment/deployment.json`. It rejects
missing/ambiguous matches and a state file owned by another target or changed
job IDs. Reporting publication records its returned model/report IDs there too.

Use the resulting Control ID and foundation bindings in `TARGET_BINDING`.
For a non-default deployment state directory, also set `files_root` to the
same reviewed `Files/.../` directory. Backfill uses it for intake partitions,
passes it to Control, and rejects retry state from another namespace.
`table_directory` independently selects `Tables`, `Tables/dbo`, or another
Lakehouse schema; it does not select the deployment Files root.

The API-assisted alternative is the operations script's `--provision-only`
phase, followed by its resolved JSON as `--config` for later configuration.
No preassigned SJD IDs or publication receipt are required to provision.
Only the later configuration/enablement phase needs real release evidence.

## Every package: submission operator

Custom catalog `capture_time_regex` rules use a matching timeout of 0.1 seconds
per filename. A timed-out rule rejects the video with
`CAPTURE_TIME_REGEX_TIMEOUT`; simplify the rule or use embedded capture time
or an inventory override before retrying.

Keep the notebook's complete package directory and unchanged source videos.
Relative video paths under `--video-root` must remain unchanged.

Discover approved choices without selecting videos or contacting Azure:

```bash
uv run python -m people_counter.backfill_operator --list-targets
uv run python -m people_counter.backfill_operator --target production --list-profiles
```

By default the command reads `config/backfill-catalog.json`. Use
`--catalog /path/to/approved-catalog.json` when the administrator distributes it
elsewhere. All listing and preview operations remain offline.

### 1. Preview (offline)

From the repository root:

```bash
uv run python -m people_counter.backfill_operator \
  --manifest-package-dir prepared/camera-17 \
  --video-root /mnt/source-videos \
  --target production \
  --profile rtdetr-r50-onnx-cpu \
  --state-dir state/camera-17-backfill
```

The command validates the package and rehashes the videos. It prints the target,
profile, package digest, video count, and partition count. It does not authenticate,
contact Azure/Fabric, or write submission state. Review these values before
proceeding. The preview includes the complete effective profile, so batch size,
sampling, model format, device, overrides, and identities are visible together.
Cloud readiness and release checks occur only on submission.

Advanced operators can explicitly add `--batch-size 1|2|4` and
`--sample-fps <positive-finite-number>`. These overrides are recorded in the
resolved profile and automatically change its identities; they do not edit the
catalog. Repeat the identical overrides for submission and retries. The default
workflow requires no overrides or JSON editing.

### 2. Submit

Repeat the same command with `--submit`:

```bash
uv run --extra publisher python -m people_counter.backfill_operator \
  --manifest-package-dir prepared/camera-17 \
  --video-root /mnt/source-videos \
  --target production \
  --profile rtdetr-r50-onnx-cpu \
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
  --target production \
  --profile rtdetr-r50-onnx-cpu \
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
Dead, failed, quarantined, cancelled, or `BLOCKED_INCOMPATIBLE` work stops the
wait with an error and its work ID/status/last error. Blocked work requires
explicit compatibility recheck, not a retry-budget increase.
Access and query errors also stop the command.

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

**Migration from handwritten configuration:** raw `config_sha256`/`runtime_key`
inputs are no longer accepted for new backfills. Use an administrator-generated
catalog. The advanced `--config` option accepts only a generated single-profile
document with the same resolved fields, not the former handwritten template.
Managed-profile payloads have new identities: do not switch an existing
backfill's state directory or re-register the same processing run during
migration. Complete/recover previously registered work with its original
operator version and retained state, then use the new catalog for new runs.
Upgrade the published SDK/Environment through the normal release process so
registration and execution use the same resolver.

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
