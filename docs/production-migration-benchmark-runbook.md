# Candidate A production migration and benchmark runbook

This runbook describes a **future operator-controlled live change**. The 0.9.11
implementation and its tests do not stop triggers, mutate Fabric, publish an
Environment, deploy an SJD, start a shadow run, or start the six-hour
measurement.

## Safety boundary

The reviewed artifact binding is fixed to workspace
`c31ee864-230d-4005-8fd5-7c7130ebf774`, Lakehouse
`883cff91-eaa8-40be-870f-6e9716303cb2`, Environment
`3e580f48-9ff7-4bc6-af2e-a59158029ada`, and migration
`people_counter_ca_0001` version `1`. A mismatch is a hard failure.

Namespaces are immutable:

| mode | table prefix | Files root |
| --- | --- | --- |
| canary | `pc_ca_canary_v1_` | `Files/_canary/people-counter/candidate-a/v1/` |
| benchmark | `pc_ca_benchmark_v1_` | `Files/_benchmark/people-counter/candidate-a/v1/` |
| production shadow | `pc_ca_prod_shadow_v1_` | `Files/_shadow/people-counter/candidate-a/v1/` |
| production additive | `people_counter_ca_` | `Files/people-counter/candidate-a/v1/` |

Benchmark and shadow code cannot address `people_counter_*` legacy objects.
Direct production adapter writes remain disabled; only the production router
can produce an authorization after verifying a migration journal, a hash-bound
safety token, an explicit allowlist, and fresh quiescence. Shadow output can
never update a production committed pointer, view, publication, or semantic
model.

The migration language contains only fixed
`CREATE TABLE IF NOT EXISTS ... USING DELTA` operations for the exact
allowlist. It has no arbitrary SQL, drop, alter,
replace, truncate, delete, merge, rename, view, or rewrite operation. Rollback
is operational: **stop routing and ignore additive structures**. There is no
destructive rollback implementation.

## Additive schema

Migration 1 creates these tables only when missing:

- `people_counter_ca_migration_journal`
- `people_counter_ca_attempts`
- `people_counter_ca_batch_members`
- `people_counter_ca_batches`
- `people_counter_ca_locks`
- `people_counter_ca_publications`
- `people_counter_ca_reconciliation_findings`
- `people_counter_ca_replay_requests`
- `people_counter_ca_routing_allowlist`
- `people_counter_ca_shadow_audit`
- `people_counter_ca_work`

The plan records exact schemas and properties, Delta versions and row counts,
control-writer ownership, active leases/runs, legacy object fingerprints,
committed pointer and committed view hashes when supplied, gold evidence, and
an independently captured writer-quiescence proof. Apply re-discovers before
and between operations. Every new table must read back with the exact schema,
zero rows, and a readable Delta version. The append-only journal chains the
before, after, plan, receipts, and prior-evidence hashes. An exact completed
state reruns as a non-mutating no-op.

The Spark implementation is
`people_counter.fabric_production_migration_live`; the reviewed host producer
and orchestrator is
`people_counter.fabric_production_migration_tool`
(`pc-production-migration-controller`). Neither accepts code, SQL, table
names, Fabric IDs, or paths. Empty Spark SJD arguments are deliberately
non-runnable. The controller's empty/default command is a read-only snapshot
preview, but it still requires a safe `--run-id`. Inventory is a canonical,
SHA-256-bound and HMAC-SHA-256-signed invocation snapshot under
`Files/people-counter/migrations/people_counter_ca_0001/inventory/`. It is
rejected when future-dated, expired, older than thirty minutes, bound to another
workspace/Lakehouse/Environment, or when job, schedule, or Reflex state is
ambiguous.

Offline CLI examples remain non-mutating:

```bash
uv run pc-production-migration plan
uv run pc-production-migration status
uv run pc-production-migration verify
# `apply` without --execute prints a plan and does not write:
uv run pc-production-migration apply
```

The live SJD export is
`fabric/candidate_a/migration/migration.SparkJobDefinitionV2.json`
(SHA-256
`cb7c038c903edf6a81e63c902b93dfc024dee4ae74b9ce2badd8e2f0309cdf38`).
Its wrapper imports only the installed 0.9.11 wheel, saved arguments are empty,
the Lakehouse and Environment IDs above are fixed, additional Lakehouses and
libraries are empty, and retry is disabled. It is an export for review; this
task does not deploy it.

## Exact Reflex interpretation and writer quiescence

The exact rule `run_pc_event_intake_on_manifest_renamed` in Reflex
`c39f8c1d-e363-402d-b7f0-34f53ce31bcc`, targeting pipeline
`5548a877-38e0-4933-bf2d-0285250637d2` in the fixed workspace, currently has
`rule_settings.shouldRun=false` and
`rule_settings.shouldApplyRuleOnUpdate=true`. **It is already inactive.**
The entity-level `runSettings.isStopped=false` belongs to another entity and
has no bearing on this rule's active state.

The pure parser in
`people_counter.fabric_reflex_definition.parse_reflex_rule_definition`
requires one exact name, one exact target tuple, and authoritative booleans.
It rejects a missing, duplicate, ambiguous, wrong-target, non-boolean, or
conflicting rule. It never substitutes `runSettings.isStopped`.

1. Capture the complete Reflex definition bytes and their SHA-256.
2. If the exact rule is already inactive, preserve those bytes unchanged.
   **Do not issue a stop or restore operation.** This unchanged inactive rule
   is valid quiescence evidence.
3. Only if a future read finds authoritative `shouldRun=true`, snapshot the
   exact definition bytes, intentionally stop that one exact rule, and read it
   back through the same exact parser. After the maintenance window, restore
   the exact original bytes and read back their exact SHA-256. Never restore a
   reconstructed or normalized definition.
4. Re-inventory scheduled writers, Reflex/event writers, active SJD/Spark
   runs, the control lock owner, and live leases. Require no enabled writer, no
   active run, no owner, and no lease. Save the canonical inventory JSON and
   SHA-256 as the independent quiescence proof.
5. If any read is unavailable or ambiguous, stop. Do not infer quiescence from
   the earlier inventory.

Recovery is also fail closed. After an ambiguous create, re-discover the exact
table metadata/cardinality/version. Resume only if it exactly matches the
operation receipt. After an ambiguous journal append, read by deterministic
journal ID and require exactly one matching evidence row. A partial or
conflicting state remains stopped for investigation; never force-recreate or
delete an object.

## Reviewed host controller and Spark sequence

The host controller obtains a cryptographically random 256-bit HMAC key and a
128-bit nonce for every invocation inventory. It uploads the signed envelope
create-only as
`Files/people-counter/migrations/people_counter_ca_0001/inventory/<inventory-run-id>.json`
and verifies the exact readback hash. A collision is a hard failure; there is
no `current.json`. The raw key is held only in the immediate controller
process, passed to the SJD as the run-specific
`--inventory-hmac-key` argument, and never written to evidence, local review
files, defaults, source, or console output. **Fabric job arguments can be
visible to Fabric administrators and in platform diagnostics.** Therefore the
key is short-lived, one-use integrity material, not a confidentiality secret
or long-lived credential. Reports redact the HMAC key, lease token, and safety
token. Do not reuse any of them.

The Spark driver independently discovers tables, Delta versions, the control
row, active leases, committed pointers/views, and gold state. The host
`InvocationInventory` contains only externally observable REST state:
workspace items and definition hashes, raw Reflex parts/rule parse, fixed
writer schedules and job instances, Environment staged/published state, the
migration SJD definition (if present), and UTC capture/expiry timestamps.

Build and deployment are safe previews unless `--execute` is present. The
deploy implementation fixes version 0.9.11, builds
`dist/people_counter-0.9.11-py3-none-any.whl` with `SOURCE_DATE_EPOCH`, verifies
wheel metadata/hash, stages only that project wheel, publishes the fixed
Environment, deploys the installed-wheel-only SJD, then semantically reads
back its two parts. Saved arguments are empty; additional Lakehouses/libraries
are empty; retry is disabled.

```bash
# Read-only REST snapshot preview. No inventory upload or job invocation.
uv run pc-production-migration-controller snapshot --run-id "$RUN_ID"

# Local build/definition preview only.
uv run pc-production-migration-controller deploy

# Future explicit live deployment (not run by this implementation task).
uv run pc-production-migration-controller deploy --execute

# Upload a one-use inventory, invoke plan, poll, verify result readback, and
# save the exact local plan artifact. Without --execute this is a preview.
uv run pc-production-migration-controller plan --execute \
  --run-id "$RUN_ID" --owner "$OWNER" --lease-token "$LEASE_TOKEN"

# Explicit review boundary: writes only a mode-0600 local receipt containing
# hashes and a redacted canonical plan summary, then exits. The raw safety
# token is never stored or printed.
uv run pc-production-migration-controller review \
  --run-id "$RUN_ID"

# A separate command consumes that exact unexpired receipt, captures a fresh
# same-migration-run snapshot into a new create-only inventory, and applies.
uv run pc-production-migration-controller apply --execute \
  --run-id "$RUN_ID" --owner "$OWNER" --lease-token "$LEASE_TOKEN" \
  --plan-sha256 "$PLAN_SHA256" --safety-token "$SAFETY_TOKEN"

uv run pc-production-migration-controller verify --execute \
  --run-id "$RUN_ID" --owner "$OWNER" --lease-token "$LEASE_TOKEN"
uv run pc-production-migration-controller status --execute \
  --run-id "$RUN_ID" --owner "$OWNER" --lease-token "$LEASE_TOKEN"
```

At the review boundary, independently construct the documented deterministic
acknowledgement `1:people_counter_ca_0001:<PLAN_SHA256>` from the reviewed plan
SHA and enter it as `SAFETY_TOKEN` only in the later apply command. The
controller verifies its SHA-256 against the reviewed receipt. It never
auto-fills, stores, or logs the raw value.

1. Confirm the exact Reflex rule is still inactive without changing it.
   Capture the signed fresh inventory. It includes exact schedule definitions,
   the exact Reflex artifact ID and definition bytes/hash, active Fabric
   job/run inventory, live registrations
   and dispatcher/work leases with expiry semantics, and the exact
   The Spark job, not the host inventory, captures the
   `people_counter_control_writer` row and all table/lease/pointer/view/gold
   evidence.
2. Generate the canonical JSON plan and its printed safety token.
   Independently review its migration ID,
   snapshot hash, operation list, SQL hashes, and rollback text.
3. Apply with the exact fixed plan file/hash. The driver compares a fresh
   discovery byte-for-byte with the stored canonical plan, requires the exact
   safety token and inactive exact Reflex rule, then acquires
   `people_counter_control_writer` by CAS with a unique per-process physical
   owner (the reviewed owner/run/token tuple remains the logical plan owner).
   This prevents two concurrent invocations with the same saved arguments
   from sharing a lock identity. A lost acknowledgement is accepted only when
   exact readback proves that unique physical ownership.
4. The backend captures Spark catalog existence, exact schema/provider/table
   properties, Delta detail/history/current version/count, canonical row
   hashes for every fixed legacy table, committed pointer tuple/hash, exact
   committed-view existence/schema/row semantics, and exact gold evidence.
   Every additive table create receives exact schema/count/version readback.
   Journal append uses deterministic identity, hash chaining, and exact
   readback.
   Before any discovery or mutation, the driver reads workspace, Lakehouse,
   and Environment IDs from the active Fabric Spark runtime configuration and
   requires the fixed binding; inventory constants alone are not accepted as
   runtime identity.
5. Run `verify`; export the compatibility report for legacy table schemas,
   versions, row counts, committed pointers/views, legacy gold, and additive
   tables.
6. Regenerate a plan under a new run ID. It must contain zero operations and
   the apply must return
   `noop`; any different result blocks routing. Both the first apply and this
   no-op require the exact safety token
   `1:people_counter_ca_0001:<canonical-plan-sha256>`.
7. Populate `people_counter_ca_routing_allowlist` with explicit work IDs and
   the exact camera, location, model, source, and config hashes.
8. Construct the shadow plan and safety token from the migration plan hash,
   migration ID, explicit manifest, and declared comparison tolerances.
9. Capture a fresh (at most five minutes old) quiescence proof at authorization
   time. Authorize only against a compatible applied/no-op migration journal.
10. Run only the explicit work IDs supplied as one or more required
   `--work-id` arguments. The apply-time rows read from
   `people_counter_ca_routing_allowlist` must exactly equal the reviewed
   camera/location/model/source/config hashes. Shadow writes must remain under
   `pc_ca_prod_shadow_v1_*` and its Files root.
11. Compare shadow and legacy committed output, provenance, and metrics.
    Preserve every reconciliation finding in the shadow audit. Do not refresh
    production semantics.

For a partial or ambiguous apply, keep all writers stopped and run `status`
with the same inventory/identity. Do not auto-release an ambiguously owned
control-writer lock. For each uncertain create, inspect exact metadata,
version, and row count; for an uncertain journal append, require one exact
deterministic journal ID and hash-chain row. Resume only with a newly reviewed
fresh inventory and exact state. Never delete, force-recreate, truncate, or
destructively roll back. To stop or recover shadow, stop routing, retain both
journals and evidence, and ignore the additive/shadow objects.

## Benchmark preparation and execution

Media is stored once. The workload generator hashes the file and independently
checks OpenCV and ffprobe frame count, FPS, duration, codec, and dimensions.
The manifest records content and artifact hashes and expands deterministic
logical work identities with disclosed repetition and diversity statistics.
Size the workload above `6 * 416.67 = 2,500.02` source-hours and include margin
so backlog remains throughout the measured interval.

The immutable measurement config records warm-up, exactly 21,600 measured
seconds, drain, restart identity, interval size, bootstrap seed/resamples/block
policy, and topology, Environment, SJD, source, model, config, and release
hashes. A measurement is invalid if it stitches Spark applications/sessions,
contains more than one segment hash, starts without the required fresh
application, or is not exactly six uninterrupted hours.

Before deployment, export and review deterministic SJD V2 definitions for
`control`, `process`, and `gold`. They import the installed 0.9.11 wheel and use
only the benchmark namespace. Deploy only after the 0.9.11 Environment publish
has succeeded and its identity/readback matches.

Interpretation:

- Count one successful committed attempt per logical work ID. Retries do not
  add throughput; duplicate committed attempts are fatal.
- Logical source-hours include every unique successful logical identity.
  Physical/unique source-hours are reported separately and do not multiply
  repeated media bytes.
- Report interval and aggregate throughput, lag-one autocorrelation, chosen
  moving-block length, deterministic seed, resample count, and the one-sided
  95% lower confidence bound.
- PASS requires an exact six-hour window, LCB `>= 416.67x`, readable/sealed
  pointers, no duplicate identity/publication, no stale fence/missing output,
  no uncommitted gold visibility or critical reconciliation finding,
  retry/failure thresholds, complete observability, and all resource gates.
- Cost reports executor/core/runtime hours, startup/steady/drain, CU inputs,
  storage/transaction counters, normalized cost per 1,000 source-hours, and a
  200,000-hour projection. Missing monetary rates remain `null` and named in
  `unknown_rates`; they are never guessed.

## Gold maintenance warning

Normal gold is checkpointed and idempotent. `--force` and `--full-rebuild` are
maintenance operations and are intentionally non-idempotent from an
operational/audit perspective. They require a separate stopped-writer change
window and must never be used to make a benchmark pass.

## Candidate A production-shadow execution (0.9.11)

This layer is deployable but is not deployed by this repository change. Its
binding is fixed to workspace `c31ee864-230d-4005-8fd5-7c7130ebf774`,
Lakehouse `883cff91-eaa8-40be-870f-6e9716303cb2`, Environment
`3e580f48-9ff7-4bc6-af2e-a59158029ada`, and migration
`people_counter_ca_0001`. It cannot accept a workspace, Lakehouse,
Environment, table, path, Fabric resource, SQL statement, or source-code
override.

The three deterministic SJD names are:

- `pc-ca-production-shadow-control-v001`
- `pc-ca-production-shadow-process-v001`
- `pc-ca-production-shadow-reconcile-v001`

Their checked-in definitions live under
`fabric/candidate_a/production_shadow/`. Each contains exactly `Main/main.py`
and `SparkJobDefinitionV1.json`, imports the installed wheel only, binds the
existing Environment and default Lakehouse, has empty saved arguments, and
has no `Libs` or additional URI. Process retry is disabled.

### Fixed data boundary and schemas

All control/process output is under `pc_ca_prod_shadow_v1_*` and
`Files/_shadow/people-counter/candidate-a/v1/`. Bootstrap is create-if-absent
with exact readback for `locks`, `work`, `batches`, `batch_members`,
`attempts`, `publications`, `replay_requests`, and
`reconciliation_findings`. Their columns and nullability are the immutable
`SHADOW_SCHEMAS` contract in
`people_counter.fabric_production_shadow`. The fixed shadow publication
ledger `pc_ca_prod_shadow_v1_publications` and the canonical shadow attempt
pointer are explicitly allowed typed targets. Production/legacy
publications, views, pointers, and output paths remain forbidden.

The only production mutations permitted are one append/readback row in each
of the migration-created schemas:

```text
people_counter_ca_routing_allowlist(
  work_id, camera_sha256, location_sha256, model_sha256, source_sha256,
  config_sha256, plan_sha256, approved_at, approved_by
)
people_counter_ca_shadow_audit(
  audit_id, work_id, plan_sha256, shadow_attempt_id, legacy_attempt_id,
  comparison_sha256, critical_findings, recorded_at
)
```

### Review and authorization

Run `snapshot`, then `plan`, then `review`. A plan contains exactly one
explicit work ID and the exact camera, location, model, source, and config
hashes copied from one already-`SUCCEEDED` legacy work/attempt/publication/view
route. It expires within fifteen minutes. Review creates a separate mode-0600
receipt. The deterministic safety token binds the canonical plan SHA, work
ID, work-identity SHA, and expiry.

`authorize --execute` additionally requires the successful
`people_counter_ca_0001` journal plan hash, the exact inactive Reflex, zero
active writers, zero leases, and either an unowned control lock or the exact
stale retained owner for the same immutable partial intent. It appends and
reads back exactly one allowlist and one authorization-audit row. Replaying
the identical authorization adds no row; any difference is a conflict.
Controller signed inventories and create-only evidence are written only
under `Files/_shadow/people-counter/candidate-a/v1/controller/`. Secrets are
redacted. The controller hashes the four legacy source rows before and after
each changing action and fails if any hash changes.

This is **not** a cross-table ACID transaction. Delta cannot atomically commit
the allowlist and audit tables together. The installed-wheel control SJD uses
the exact `people_counter_control_writer` CAS and a create-only intent ledger
under `controller/live/authorization/<authorization-id>/`. It writes
`00-prepared`, appends and reads back the allowlist, writes `10-allowlist`,
appends and reads back the audit, writes `20-audit`, verifies both exact rows,
then writes `30-committed` and releases the lock. Every invocation has a
unique physical lock owner derived from its signed random invocation ID. Any
ambiguous or partial result retains that owner. A different invocation may
take over only after five minutes, by exact CAS from that retained owner, when
`00-prepared` proves the same authorization and `30-committed` is absent;
an active same-operation invocation therefore cannot share or resume the
lock. `status
--plan-sha256 <PLAN_SHA256>` reports the partial stages. Recovery is only an
exact replay of the same reviewed `authorize --execute`; the immutable intent
must prove the same work, identity, plan, row hashes, expiry, receipt, and
token. A conflict is never repaired and no row or intent is deleted.

### Process, comparison, and reconcile

The process entry passes `FabricCandidateAConfig.production_shadow()` into
the proven Candidate A register, exact-work claim, process, seal, and publish
adapters. Runtime evidence fixes package `0.9.11`, Runtime 2.0, Python 3.13,
Spark 4.1.1, Java 21, and package SHA. Diagnostics and stage records are
create-only. An idempotent rerun returns the existing committed route and
does not create another attempt, pointer, publication, or authorization row.

Comparison/reconcile checks logical identity, exact output path and SHA,
sealed/committed visibility, attempt pointer, fence, package/runtime
provenance, a single publication, and authorization/audit plan linkage. It
also compares output records, logical totals, frame counts, timestamps, and
explicit numeric tolerances. Duplicate identity/publication, stale fence,
uncommitted visibility, missing authorization, or any other critical finding
fails the gate.

### Subsequent live commands (do not run during package validation)

First build and publish the reviewed wheel and wire the fixed-scope host
backend/OneLake create-only evidence adapter. Then use only this sequence:

```bash
uv build
pc-production-shadow-controller snapshot
pc-production-shadow-controller plan --work-id <ELIGIBLE_WORK_ID>
pc-production-shadow-controller review \
  --plan-sha256 <PLAN_SHA256> --reviewer <REVIEWER>
pc-production-shadow-controller deploy --execute \
  --plan-sha256 <PLAN_SHA256> --safety-token '<SAFETY_TOKEN>'
pc-production-shadow-controller authorize --execute \
  --plan-sha256 <PLAN_SHA256> --safety-token '<SAFETY_TOKEN>'
pc-production-shadow-controller bootstrap --execute \
  --plan-sha256 <PLAN_SHA256> --safety-token '<SAFETY_TOKEN>'
pc-production-shadow-controller process --execute \
  --plan-sha256 <PLAN_SHA256> --safety-token '<SAFETY_TOKEN>'
pc-production-shadow-controller compare --execute \
  --plan-sha256 <PLAN_SHA256> --safety-token '<SAFETY_TOKEN>'
pc-production-shadow-controller reconcile --execute \
  --plan-sha256 <PLAN_SHA256> --safety-token '<SAFETY_TOKEN>'
pc-production-shadow-controller status --plan-sha256 <PLAN_SHA256>
```

Stop on any nonzero status, expiry, identity/readback mismatch, legacy hash
change, or critical comparison finding. Never substitute IDs or paths and
never run a production/legacy publication or semantic refresh as part of the
shadow flow.

The host backend is `FabricShadowControllerBackend`. It snapshots the fixed
Environment, exact Reflex definition, all fixed writer pipeline definitions,
schedules and active runs, and all three fixed SJD definitions/runs through
Fabric REST. Each Spark invocation receives only an enumerated command plus a
random invocation ID and one-use HMAC key. Its signed request, zero-exit result
or hashed failure diagnostic is create-only in the fixed controller root.
Spark is authoritative for the migration journal, control lock, leases,
eligible legacy routes, authorization tables, shadow tables, pointers, and
publications. A result artifact with a nonzero exit code is always rejected.
