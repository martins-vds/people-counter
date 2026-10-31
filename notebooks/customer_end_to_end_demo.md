# Local customer demo

Open [customer_end_to_end_demo.ipynb](customer_end_to_end_demo.ipynb) in VS Code.
Select the repository `.venv` interpreter. This is a local notebook, not a
notebook to publish to Fabric.

```bash
uv sync --extra publisher --extra experiments --extra demo
az login
```

Install `ffprobe` if it is unavailable. Leave all cloud gates false for an
offline rehearsal.

## Presentation checkpoints

1. **Inputs:** leave `SOURCE_DIR` at `samples` for the first rehearsal. Record
   the printed `RUN_LABEL`; enter it explicitly to resume a session.
2. **Cameras:** inspect the catalog, dimensions, directed counting lines,
   timestamps, and labelled sample override. Sample lines are illustrative.
3. **Integrity:** show the generated inventory, path-based asset IDs,
   content-based versions, package checksum, and prepared JSON.
4. **Readiness:** review the legacy workspace, F64 resource and storage targets.
   Set `RUN_PREFLIGHT=True`. Approve `ALLOW_CAPACITY_RESUME` only if you accept
   F64 billing. If necessary, approve `ALLOW_ROLE_GRANT` for the container-scoped
   storage data role. The CLI identity must have authority to grant that role.
   The staging write/read/delete probe verifies effective data access.
5. **Automation:** Dispatcher and Gold need existing enabled schedules.
   `ENABLE_EXISTING_SCHEDULES` permits enabling those schedules only.
   The notebook verifies the selected storage source, stream, and arrival
   Activator destination are `Running`, with no reported runtime error. It also
   verifies the arrival rule is enabled and invokes the selected intake pipeline.
   These checks repeat before uploads, promotion, and every monitoring poll.
   Configuration readiness still does not prove event delivery or processing.
6. **Staging:** enable `UPLOAD_TO_STAGING`, run its cell, and return the gate to
   false. Pause to show that incoming publication has not happened.
7. **Arrival:** enable `PROMOTE_TO_INCOMING`, run its cell, and return the gate
   to false. Videos move first; final manifests are staged and moved last.
8. **Execution:** enable `MONITOR_FABRIC`. Wait for every exact asset's fresh
   registration and committed Gold result, plus new intake and processing SJD
   completed job activity. Spark startup and schedule intervals are visible demo latency.
9. **Close:** show result rows and retain local execution evidence.

No cell submits an ingestion or Spark job manually. Dispatcher can run Process
inside its own SJD execution, so absence of a separate Process job is not a
failure. Job histories provide timing/context, not a one-to-one mapping between
job IDs and assets; exact work identities and committed pointers establish
asset execution. The notebook does not independently prove Activator causality.

## Customer inputs

Set `SOURCE_DIR` to the input directory and provide `CAMERA_CATALOG_CSV`.
The catalog uses the schema in the
[manifest operator tutorial](manifest_package_operator_tutorial.ipynb).
Catalog prefixes and optional `CAPTURE_OVERRIDES_CSV` paths are relative to
`SOURCE_DIR`; the notebook prefixes its run-specific snapshot path automatically.
Keep real camera IDs, timezone, effective dates, dimensions, and calibrated line
direction. Do not use upload time as recording time.

Each new run copies the selected videos below `.people-counter-demo/` without
changing source bytes. This intentionally creates new path-based asset IDs.
Retries use the same label, package and incoming paths. Do not delete incoming
data or change bytes within a run to manufacture another arrival.
On restart, complete packages are reused rather than regenerated. The notebook
verifies catalog/override digests and video bytes; changed run metadata requires
a new label.

## Failure and resume

- **Capacity:** a paused capacity requires resume approval and capacity
  read/resume permission. Unknown states, service errors and timeouts fail.
- **Storage:** Azure Contributor does not imply blob data rights. Grant only
  the reviewed container role. RBAC propagation is retried for five minutes.
  Custom roles/ACL-only configurations require operator review.
- **Fabric:** Azure RBAC does not grant Fabric workspace/item/OneLake access.
  The legacy Lakehouse is schema-enabled: its physical tables are under
  `Tables/dbo`, not directly under `Tables`. The notebook uses DuckDB's native
  Delta reader so deletion vectors are applied; reading raw Parquet would
  incorrectly include deleted rows. The Azure and Delta extensions are
  downloaded from DuckDB's official extension repository on first use and
  cached locally. Both log and data reads use the Azure CLI identity only.
  The OneLake Blob endpoint handles log listing; the DFS URI handles data reads.
  The documented `curl` transport setting resolves native Azure SDK certificate
  discovery on Linux without disabling TLS verification.
  Missing permissions, shortcut mismatches, and missing tables fail explicitly.
  `TABLE_DIRECTORY` must match the target Lakehouse's physical schema directory.
  Preflight resets its success flag on every attempt so a failed rerun cannot
  leave publication enabled through a stale successful preflight.
  It also reloads the stateless local demo helpers before use so an already
  running kernel can pick up notebook fixes without losing the run label.
- **No fresh work:** inspect the arrival Activator, Eventstream and intake
  pipeline. Legacy intake could still target an older queue. This notebook
  deliberately does not substitute that queue or register work manually.
- **Release mismatch:** preflight compares the live intake release assertion
  with the Dispatcher schedule. Production registration and claiming also
  validate against the installed SDK's published manifest and receipt. Deploy
  Intake, its metadata notebook, and Dispatcher together; do not copy old hashes.
  Correcting an existing immutable work identity requires an audited recovery
  path, not editing its stored digest.
  After a release rollover has quarantined an obsolete demo registration, use
  a new `RUN_LABEL` for a new rehearsal. Do not expect the retired run's monitoring
  state to become successful, and do not edit its package or incoming objects.
- **Compute mismatch:** preflight checks published Environment settings, not
  pending staging settings. Runtime 2.0 must use fixed allocation with two
  8-core/56-GB executors and speculation disabled. Preflight also requires the
  workspace Starter Pool limits to be three nodes / two executors with compute
  customization enabled. Workspace limits can override the Environment's
  executor-count property. These checks repeat before publication and during
  monitoring; they never change workspace settings automatically.
  Dispatcher validates the basic runtime and fixed allocation before claiming.
  Deployment canaries separately verify native Spark properties and observed
  workers; published settings alone do not prove the actual allocation.
  Workspace limit changes affect other Starter Pool workloads and require
  administrator approval. Do not weaken the measured executor-RSS guard.
- **Processing diagnostic:** monitoring reads recent Dispatcher/Process
  diagnostics for the exact registered work IDs and stops immediately on a
  matching failure, even if Fabric marks a scheduled job `Completed`.
  This prevents a known processing failure from turning into hours of polling.
- **Paused Activator delivery:** open `azure_storage_events_eventstream` in
  Fabric **Live view** and activate `to_pc_manifest_arrival_activator`. Review
  **When streaming was last stopped** to resume retained events rather than
  starting only from now. Start the manifest-arrival rule in Activator if it
  is disabled. Rerun preflight using the same run label, then resume monitoring.
  The notebook uses the documented runtime-status API and stops immediately
  if delivery is inactive; it does not invent a private resume endpoint.
  An enabled rule alone is insufficient when its Eventstream destination is
  paused. Recovery of already-published events must be verified; do not delete
  or re-upload immutable incoming files to force another arrival.
- **Work but no results:** inspect Dispatcher, processing errors, Gold schedule
  and Gold failures. `DEAD`/failed/quarantined work fails immediately.
- **Interrupted promotion:** keep the same `RUN_LABEL`, rerun local setup and
  preflight, then rerun promotion. Its durable baseline is retained and incoming
  content is verified, not overwritten. Do not rerun staging after publication.
- **Regional job-history pages:** Fabric can return continuation URLs under
  `*.analysis.windows.net`. The demo client explicitly trusts this Microsoft
  regional suffix over HTTPS; unrelated origins and look-alike hostnames remain
  rejected. After updating the notebook, rerun preflight to recreate the client,
  then rerun promotion with the same label. Collecting the job baseline happens
  before publication, so a baseline failure has not moved the videos.
- **Timeout:** inspect `.people-counter-demo/<run>/execution-evidence.json`.
  A timeout is not success. Preserve the label and rerun monitoring after fixing
  the cause.

The notebook matches the production registration payload's `source_video`
default-Lakehouse path and `source_sha256`. A changed intake payload contract
must be investigated rather than accepted by loose hash-only matching.

## Costs and privacy

F64 billing starts on resume. This notebook does not automatically pause shared
capacity or disable schedules. Coordinate shutdown after all dependent work
finishes. Local snapshots, catalogs, checkpoints, and evidence are ignored by
Git. Clear executed notebook outputs before sharing or committing.

Official references:

- [Resume a Fabric capacity](https://learn.microsoft.com/en-us/rest/api/microsoftfabric/fabric-capacities/resume?view=rest-microsoftfabric-2023-11-01)
- [Access OneLake with Python](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-python)
- [Azure storage data roles](https://learn.microsoft.com/en-us/azure/storage/blobs/assign-azure-role-data-access)
- [Eventstream topology runtime status](https://learn.microsoft.com/en-us/rest/api/fabric/eventstream/topology/get-eventstream-topology)
- [Activate an Eventstream destination and choose a resume position](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/event-streams/pause-resume-data-streams)
