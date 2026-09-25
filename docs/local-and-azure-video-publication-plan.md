# Local and Azure Video Publication Plan

## Problem

The operator must prepare and publish camera metadata and video manifests for
either:

1. videos on local disk; or
2. terabytes of videos already present in Azure Storage.

The publication destination may be the source storage account or a different
account. The design should avoid downloading remote video merely to upload it
again.

## Core approach

- Preserve the existing local workflow and schema-1 behavior.
- Add a separate schema-2 path for Azure-hosted sources.
- Use the Azure SDK or Blob Inventory to discover remote objects.
- Use bounded remote `ffprobe` access to extract media metadata.
- Publish by either:
  - **reference**: manifests point to the existing source blobs; or
  - **copy**: AzCopy performs server-to-server transfer, then manifests point
    to the destination blobs.
- Use AzCopy only for transfer. Do not use it as the catalog, metadata parser,
  or source of manifest truth.
- Publish large remote runs through a bulk prefix and an atomic `_COMPLETE`
  marker so Fabric does not start one event-triggered job per manifest.

## Scope

### In scope

- Existing local input and publication.
- Azure Blob Storage and ADLS Gen2 sources.
- Live SDK enumeration.
- Blob Inventory CSV support; Parquet can follow behind an optional dependency.
- Reference-in-place publication.
- AzCopy server-to-server copy to the same or another account.
- Resumable preparation and publication.
- A schema-2 manifest that separates source provenance from readable location.
- Bulk registration of completed remote runs.
- Validation, operational reporting, and documentation.

### Deferred

- Migrating existing schema-1 local publishers to a shared event lease.
- Reworking Gold refresh, work-state journals, alias promotion, or unrelated
  Fabric concurrency.
- Automatic relocation of already-registered assets.
- Arbitrary multi-generation successor recovery.
- A global cross-publisher event-rate ledger.
- Schema-2 local publication unless a later requirement needs it.

These items should be separate follow-up designs rather than prerequisites for
remote-source support.

## Key decisions

### 1. Keep local behavior stable

The current local CLI, package shape, manifest schema, checkpoint, destination
layout, and event behavior remain unchanged by default.

Remote source arguments select schema 2. Local schema 2 is not part of the
initial implementation.

### 2. Model source and readable location separately

Each schema-2 video entry records:

- stable asset ID;
- version ID and integrity basis;
- source namespace, account, container/filesystem, path, ETag, and length;
- readable video namespace, account, container/filesystem, path, URI, ETag,
  and length;
- publication mode: `azure_reference` or `azure_copy`;
- camera ID and logical geometry;
- duration in milliseconds, decoded width/height, rotation, and probe
  provenance.

In reference mode, source and readable location are the same object.

In copy mode, source fields retain origin provenance and readable-location
fields identify the copied destination object.

### 3. Make version identity explicit

Use the strongest available stable value:

1. trusted metadata SHA-256;
2. Content-MD5;
3. ETag plus content length.

Record the algorithm and value. Treat ETag-plus-length as storage-version
identity, not a cryptographic content hash. If source properties change between
discovery, probing, and publication, fail that item and require re-preparation.

### 4. Stream remote discovery

Provide a source-adapter boundary shared by:

- local filesystem enumeration;
- Azure SDK listing; and
- Blob Inventory rows.

All adapters produce the same canonical candidate model and deterministic sort
order. Filtering and validation happen while streaming; large inventories are
externally sorted or partitioned rather than loaded fully into memory.

Start with live SDK listing as the baseline. Add Blob Inventory for accounts
where listing cost or duration is unacceptable.

### 5. Probe remote media without bulk download

Use `ffprobe` against a short-lived, read-only URL or a local credential-aware
range proxy. Credentials are generated only for the probe, redacted from logs,
and never written into packages, manifests, or checkpoints.

Apply explicit limits for probe duration, bytes read, redirects, protocols, and
concurrency. Re-read source properties after probing and reject changed
objects.

### 6. Choose publication mode deliberately

- `reference`: use when Fabric can read the source location and lifecycle,
  network, and permission policies permit long-term access.
- `copy`: use when data must move into a managed publication account or the
  source cannot be treated as durable.
- `auto`: choose reference only when the configured policy explicitly allows
  it; otherwise choose copy. Report the decision and reason before mutation.

Reference mode writes no video bytes.

### 7. Use AzCopy only for copy mode

Pin and validate an AzCopy v10 version. Authenticate with OAuth where possible;
allow short-lived SAS only as an explicit fallback.

For each bounded batch:

1. generate the expected source-to-destination mapping;
2. run a supported AzCopy server-to-server command with overwrite disabled and
   source-change detection enabled;
3. capture the job ID and sanitized summary;
4. reconcile every expected destination object by path and length;
5. verify the source ETag/length is still the prepared version;
6. only then publish its manifest.

Use directory-prefix batches when the selected set exactly covers that prefix.
Use bounded `--include-path` batches only for names that can be represented
safely. Fall back to exact-object commands for unsafe or irregular names.
Never allow a batching optimization to copy an unselected object silently.

Store AzCopy plan/log files beside the schema-2 checkpoint on durable local
storage. Resume an existing job when possible; otherwise rerun the deterministic
no-overwrite batch and reconcile destination state.

### 8. Publish manifests last and in bulk

Use a retry-stable `run_id` and immutable run descriptor. Write remote manifests
under a dedicated bulk prefix:

`incoming-bulk/<run_id>/...`

After all selected items are either published or explicitly failed, write an
atomic `_COMPLETE` marker containing counts and a digest of the manifest list.

Fabric bulk registration processes only completed runs, validates the marker
against the actual manifest set, and records a run watermark for idempotent
retry. A failed or incomplete run is not registered automatically; the operator
resumes or abandons it explicitly.

This avoids introducing a new event-lease protocol into the existing local
workflow.

### 9. Preserve no-overwrite and idempotency

- Video destinations are deterministic from asset/version identity.
- Manifests are content-addressed or otherwise deterministic.
- Existing matching video/manifest objects are accepted as already published.
- Existing conflicting objects fail visibly; they are never overwritten.
- Checkpoint state is a cache of progress, not proof. Resume revalidates Azure
  object properties.
- Manifest publication always follows successful location verification.

## Implementation work

### A. Contracts and schema

- Define the source-adapter candidate model.
- Define schema-2 package and manifest models.
- Add schema-version dispatch without changing schema-1 serialization.
- Define canonical Azure account/container/path/URI normalization.
- Add golden fixtures for identity, path, duration, rotation, and geometry.

Likely files:

- `src/people_counter/manifest.py`
- schema fixtures and manifest tests

### B. Remote discovery and probing

- Add Azure SDK enumeration.
- Add Blob Inventory CSV ingestion.
- Add optional Parquet ingestion after the CSV path is stable.
- Add remote-property consistency checks.
- Add bounded credential-safe `ffprobe` access.

Likely files:

- `src/people_counter/manifest.py`
- a focused Azure source module if separation is clearer
- related unit/integration tests

### C. Schema-2 package and checkpoint

- Add a schema-2 package writer/reader with destination binding and `run_id`.
- Add a separate schema-2 checkpoint; do not migrate or mutate the legacy
  schema-1 checkpoint.
- Persist item state, AzCopy jobs, and sanitized failure reasons.
- Reject source, package, or destination mismatches on resume.

Likely files:

- `src/people_counter/manifest_publisher.py`
- package/checkpoint tests

### D. Reference publication

- Validate source readability and policy prerequisites.
- Generate manifests whose readable location is the source object.
- Publish to the bulk prefix with no-overwrite semantics.
- Write and validate the `_COMPLETE` marker.

### E. AzCopy copy publication

- Add AzCopy command construction and version checking.
- Add safe batching and exact-object fallback.
- Add durable job/log locations and resume behavior.
- Reconcile destination objects and source immutability before manifest write.
- Redact credentials and SAS values from commands, logs, reports, and errors.

### F. Fabric registration

- Extend event/backfill manifest validation to understand schema 2.
- Add a bulk-run registration path that reads only `_COMPLETE` runs.
- Resolve processing input from the schema-2 readable-location fields.
- Preserve schema-1 registration and processing behavior.
- Keep downstream business/Gold behavior unchanged unless schema-2 fields
  expose a concrete compatibility issue.

Likely files:

- `notebooks/fabric/01_register_event.ipynb`
- `notebooks/fabric/02_register_backfill.ipynb`
- `notebooks/fabric/04_process_video.ipynb`
- bootstrap/reset notebooks only for directly required additive columns

### G. CLI, operations, and documentation

- Add mutually exclusive local and Azure source options.
- Add discovery provider, source credential, destination credential,
  publication mode, inventory input, `run_id`, and AzCopy options.
- Add dry-run reporting for selected files/bytes, probe work, transfer work,
  chosen mode, and estimated Azure operations.
- Document permissions, network requirements, lifecycle assumptions, AzCopy
  authentication, SAS hygiene, resume, incomplete-run cleanup, and costs.

## Validation

- Unit-test canonical paths, provider equivalence, filtering, deterministic
  ordering, identity selection, schema dispatch, and no-overwrite behavior.
- Test source mutation between discovery/probe/copy and require visible failure.
- Test reference and copy manifests produce the same work identity when their
  immutable media/configuration fields match.
- Test AzCopy command generation without invoking live AzCopy.
- Test irregular names, batching boundaries, partial AzCopy success, resume,
  stale checkpoints, destination conflicts, and credential redaction.
- Test `_COMPLETE` marker digest/count validation and bulk-registration
  idempotency.
- Pass emitted schema-2 fixtures through Fabric registration and processing.
- Run opt-in integration tests against a disposable Azure account for SDK
  listing, range probing, ADLS rename, OAuth AzCopy S2S copy, and ETag behavior.
- Run existing local regression tests to prove schema-1 behavior is unchanged.
- Run the repository's normal lint/type/test suite, CRAP analysis for changed
  Python production code, and mutation testing for changed Python tests.

## Delivery sequence

1. Land schema-2 contracts and fixtures behind a disabled remote-source flag.
2. Add SDK discovery and remote probing.
3. Enable reference mode for an allowlisted source and bulk registration.
4. Add Blob Inventory CSV for large-account discovery.
5. Add AzCopy copy mode for a small allowlisted source/destination pair.
6. Validate resume, no-overwrite, integrity, costs, and operational cleanup.
7. Expand allowlists and add Parquet inventory only if scale measurements
   justify it.

## Acceptance criteria

- Existing local commands and schema-1 outputs remain unchanged.
- A remote run can enumerate and prepare millions of candidates with bounded
  memory.
- Reference mode publishes manifests without copying video bytes.
- Copy mode transfers video server-to-server and publishes manifests only
  after destination reconciliation.
- Source and destination may be the same or different storage accounts.
- Credentials never appear in persisted artifacts or logs.
- Retries are idempotent and never overwrite conflicting media or manifests.
- Fabric registers only complete remote runs and processes the declared
  readable location.
- Operational output states selected count/bytes, chosen publication mode,
  skipped/already-published items, failures, and registration status.

## Notes

- AzCopy is valuable for high-throughput relocation, but it does not replace
  Azure inventory, media probing, manifest generation, or publication-state
  tracking.
- Blob Inventory is the preferred scale optimization when live enumeration
  becomes expensive; it is not required for the first remote pilot.
- The first implementation should favor explicit modes and visible failures
  over automatic recovery machinery.
- This plan is exploratory only. No repository implementation is authorized by
  the current request.
