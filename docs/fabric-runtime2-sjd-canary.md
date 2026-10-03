# Fabric Runtime 2.0 Spark Job Definition canary

This repository contains one isolated canary item:
`pc-ca-r20-sjd-canary-v001`. It is fixed to workspace
`c31ee864-230d-4005-8fd5-7c7130ebf774` and the
`people_counter_dev` Lakehouse
`883cff91-eaa8-40be-870f-6e9716303cb2`.

The canary does **not** enable `pc-control-sjd`, `pc-process-sjd`, or
`pc-gold-sjd` in Fabric. Their Fabric control/attempt adapters remain
unsupported and fail closed.

## Safety boundary

- The only permitted write scope is
  `Files/_canary/people-counter/runtime2-sjd/v1/run=<fresh-uuid>/`.
- Every invocation requires `PC_CANARY_ONLY_V1`, the two fixed item IDs, and
  the release digest embedded by the definition builder.
- A pre-existing run root is a hard failure. A retry needs a new UUID.
- The job uses only create-only files and Delta `errorifexists` writes. It
  does not create catalog tables, call `saveAsTable`, overwrite data, run
  `VACUUM`, or merge into production.
- Cleanup must name one complete `run=<uuid>` directory. Never recursively
  delete `_canary`, `runtime2-sjd`, `Files`, or any `Tables` path.
- `Main/main.py` is either the reviewed thin wrapper or a generated,
  SHA-256-checked exact-worktree source bootstrap. Executor work always calls
  the repository's top-level Candidate A `execute_sjd_partition` callable in
  probe mode; the canary does not contain another inference implementation.

## Prerequisites and roles

1. Install the repository development environment with `uv sync --dev`.
2. Install Azure CLI for `--auth azure-cli`, or install the `publisher` extra
   (`uv sync --extra publisher`) for Default Azure Credential, managed
   identity, or service-principal authentication.
3. The caller needs workspace access and item permissions sufficient to read,
   create, and update Spark Job Definitions, execute item jobs, read the
   Environment, and write `Files/` in `people_counter_dev`. For REST delegated
   permissions this normally means `SparkJobDefinition.ReadWrite.All` (or
   `Item.ReadWrite.All`) and `SparkJobDefinition.Execute.All` (or
   `Item.Execute.All`). Grant the identity an appropriate workspace role
   (Contributor for deployment; a narrower run/read role where supported) and
   Lakehouse write access to the canary `Files/` prefix.
4. Capacity and tenant settings must permit Fabric REST APIs, service
   principals/managed identities if selected, Spark Job Definitions, and the
   target Environment.

## Create and publish the Environment

Create a dedicated Environment item in the Fabric UI or with the official
Environment REST APIs. The `pc-fabric-canary` CLI verifies an Environment but
does not create or mutate one.

1. In workspace `c31ee864-230d-4005-8fd5-7c7130ebf774`, create an Environment.
2. Select **Runtime 2.0**.
3. Add the `people-counter` package and any required probe-mode dependencies
   using a Python 3.13/Linux wheelhouse. Do not reuse a CPython 3.12 bundle.
   The current probe path deliberately avoids importing OpenCV, Torch, or
   model packages, so it needs no third-party runtime dependency.
4. Publish the Environment and wait for the publish state to succeed.
5. Record its item UUID. `deploy` verifies the published Runtime 2.0 settings
   when the API exposes them and never silently creates or changes an
   Environment.

Check a bundle before building:

```bash
uv run pc-fabric-canary build \
  --environment-id 00000000-0000-0000-0000-000000000000 \
  --bundle build/people-counter-runtime2-wheelhouse.zip \
  --lib build/people_counter-0.6.0-py3-none-any.whl \
  --output build/pc-ca-r20-sjd-canary-v001.json
```

The `--bundle` is preflighted; include deployable wheel parts explicitly with
`--lib`. Pure Python wheels and CPython 3.13 Linux wheels are accepted.
CPython 3.12 and non-Linux binary wheels are rejected.

Fabric can reject a wheel in a Python SJD V2 `Libs/` collection even when the
wheel itself passes local preflight. When Environment library publication is
also unavailable, use only a reviewed generated `--main` that embeds the
exact worktree as a deterministic zip, verifies its SHA-256 before import,
adds it with `SparkContext.addPyFile`, and includes matching distribution
metadata. Do not use this fallback to bypass the release manifest, secret
scan, package identity, or readback checks.

## Dry-run build and inspection

No credential is needed to build:

```bash
uv run pc-fabric-canary build \
  --environment-id 00000000-0000-0000-0000-000000000000 \
  --output build/pc-ca-r20-sjd-canary-v001.json
```

The output is a deterministic `SparkJobDefinitionV2` complete replacement.
It contains `SparkJobDefinitionV1.json`, `Main/main.py`, a release manifest,
and every explicitly supplied retained `Libs/` part. Saved arguments contain
`--run-id __REQUIRED__`, so the saved definition is deliberately not
runnable. The builder canonicalizes part ordering/base64, records source
commit and project version, hashes decoded parts, and scans decoded content
for secrets.

Inspect an existing live definition:

```bash
uv run pc-fabric-canary inspect-live \
  --workspace-id c31ee864-230d-4005-8fd5-7c7130ebf774 \
  --item-id 00000000-0000-0000-0000-000000000000
```

Authentication defaults to Azure CLI. Alternatives are `--auth default`,
`--auth managed-identity`, or `--auth service-principal`; use standard Azure
identity environment variables rather than storing credentials in files.

## Deploy

This implementation task does not deploy. After review, deploy with:

```bash
uv run pc-fabric-canary deploy \
  --workspace-id c31ee864-230d-4005-8fd5-7c7130ebf774 \
  --environment-id 00000000-0000-0000-0000-000000000000
```

For a known item:

```bash
uv run pc-fabric-canary deploy \
  --workspace-id c31ee864-230d-4005-8fd5-7c7130ebf774 \
  --environment-id 00000000-0000-0000-0000-000000000000 \
  --item-id 00000000-0000-0000-0000-000000000000
```

The deployer serializes locally, reads and snapshots the current complete
definition under `build/fabric-canary-snapshots/`, retains existing `Libs/`
parts, rejects any other unmanaged part, sends a complete replacement through
the item-specific endpoint, then reads back, decodes, and verifies all hashes.
It does not rely on ETags. Only use `--allow-prune` after reviewing the
snapshot and all removed paths.

## Run, status, result, and cancel

Use a new run UUID, or omit `--run-id` to generate one:

```bash
RUN_ID="$(uv run python -c 'import uuid; print(uuid.uuid4())')"
uv run pc-fabric-canary run \
  --workspace-id c31ee864-230d-4005-8fd5-7c7130ebf774 \
  --item-id 00000000-0000-0000-0000-000000000000 \
  --run-id "$RUN_ID"
```

The run API receives the exact complete command-line argument string, including
the fixed IDs/scope/token plus run, release, project-version, and package-source
identities. The saved executable, library, Lakehouse, and Environment
references remain in the reviewed definition; the tool does not invent ABFS
paths for run-time overrides.

Poll with the server's `Retry-After` and a bounded timeout:

```bash
uv run pc-fabric-canary status \
  --workspace-id c31ee864-230d-4005-8fd5-7c7130ebf774 \
  --item-id 00000000-0000-0000-0000-000000000000 \
  --job-instance-id 00000000-0000-0000-0000-000000000000 \
  --wait --poll-timeout 1800
```

Cancel:

```bash
uv run pc-fabric-canary cancel \
  --workspace-id c31ee864-230d-4005-8fd5-7c7130ebf774 \
  --item-id 00000000-0000-0000-0000-000000000000 \
  --job-instance-id 00000000-0000-0000-0000-000000000000
```

After downloading the run's `result.json` from the exact run root, validate it:

```bash
uv run pc-fabric-canary validate-result \
  --result result.json \
  --run-id "$RUN_ID"
```

## Cleanup and rollback

Review `result.json`, the create-only `_SUCCESS`, `pointer.json`, the Delta
pointer row, and Delta attempt records before cleanup. Delete only:

```text
Files/_canary/people-counter/runtime2-sjd/v1/run=<reviewed-run-uuid>/
```

There is intentionally no broad cleanup command. Use the Lakehouse UI or an
administrator-reviewed OneLake operation that names that exact run root.

Before an update, deploy saves a complete definition snapshot. To roll back,
review and secret-scan that snapshot, then POST its `definition` object as a
complete replacement to:

```text
POST /v1/workspaces/c31ee864-230d-4005-8fd5-7c7130ebf774/
     sparkJobDefinitions/<item-id>/updateDefinition
```

Read the definition back in `SparkJobDefinitionV2` format and compare decoded
part hashes. Rollback changes only the canary item; do not deploy or modify the
three production SJD items.

## What success proves

Success proves that the selected published Environment can start Runtime 2.0,
that observed versions match Python 3.13/Spark 4.1/Java 21 (and observable
Scala 2.13/Delta 4.2), that the hash-checked release serializes the existing
Candidate A probe callable to real Spark tasks, and that the default Lakehouse
supports isolated create-only files plus Delta write/readback.

It does **not** prove production model accuracy, model bundle completeness,
production scale, control-plane fencing, semantic-model refresh, production
table writes, production SJD readiness, or permission suitability outside the
canary prefix.
