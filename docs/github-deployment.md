# GitHub deployment runbook

Git, the environment-specific Terraform state, and retained workflow evidence
are the deployment source of truth. Fabric Deployment Pipelines may be created
for comparison/history, but must not mutate resources owned by this repository.
Routine deployment never accepts a workspace or item GUID as a workflow input.

## Organizational bootstrap and tenant settings

An approved bootstrap operator needs the permissions listed in
[`infra/bootstrap/README.md`](../infra/bootstrap/README.md). Before applying
that stack:

1. register the Azure `Microsoft.Fabric` resource provider;
2. confirm the subscription and region can create the configured Fabric SKUs;
3. have a Fabric administrator prepare a least-privilege security group for
   the generated deployment service principals;
4. in the Fabric admin portal, scope the tenant settings that allow service
   principals to use Fabric public APIs and to create workspaces, connections,
   and deployment pipelines to that group; and
5. enable preview tenant settings only when reviewed configuration sets
   `enable_preview_features=true`.

Tenant settings are an external administrative gate. Do not broaden them to
the entire tenant merely to make a workflow pass. Add each generated service
principal to the approved group after bootstrap and before the first Fabric
plan.

Apply bootstrap from an authenticated operator workstation:

```bash
az login --tenant "<tenant-id>"
cp infra/bootstrap/terraform.tfvars.example \
  infra/bootstrap/terraform.tfvars
# Edit only the local terraform.tfvars.
terraform -chdir=infra/bootstrap init
terraform -chdir=infra/bootstrap plan \
  -var-file=terraform.tfvars \
  -out=bootstrap.tfplan
terraform -chdir=infra/bootstrap apply bootstrap.tfplan
terraform -chdir=infra/bootstrap output -json
```

Bootstrap state is local, access-restricted, and never committed. It creates
the state backend, one OIDC application/service principal per environment,
environment-scoped federated credentials, state-container RBAC, and Fabric
capacities. It does not create a client secret.

## OIDC trust

For each stage, the generated federated credential is:

```text
issuer:   https://token.actions.githubusercontent.com
audience: api://AzureADTokenExchange
subject:  repo:<owner>/<repository>:environment:<stage>
```

The `<owner>/<repository>` value comes from bootstrap
`github_repository`; `<stage>` is `dev`, `test`, or `prod`. A spelling or case
mismatch between that value and the GitHub Environment prevents token
exchange. All cloud jobs declare an environment, request only
`contents: read` and `id-token: write`, and use `azure/login`. Repository-level
permissions are empty by default.

## GitHub Environments and variables

Create protected GitHub Environments named exactly `dev`, `test`, and `prod`.
Restrict deployment branches to `main`. Require an independent reviewer for
Production, prevent administrator bypass, and apply the same review rule to
Test when policy requires independent promotion. Keep variables and secrets at
the environment level, not repository level, so OIDC trust and configuration
are selected together.

Configure these required non-secret variables in every environment:

| Variable | Exact source/format |
|---|---|
| `AZURE_CLIENT_ID` | `github_oidc_client_ids[stage]` |
| `AZURE_TENANT_ID` | bootstrap `tenant_id` |
| `AZURE_SUBSCRIPTION_ID` | bootstrap `subscription_id` |
| `TF_BACKEND_RESOURCE_GROUP_NAME` | bootstrap `resource_group_name` |
| `TF_BACKEND_STORAGE_ACCOUNT_NAME` | bootstrap `state_storage_account_name` |
| `TF_BACKEND_CONTAINER_NAME` | bootstrap `state_container_name` |
| `FABRIC_CAPACITY_ID` | Fabric API capacity UUID, not the Azure resource ID |
| `FABRIC_WORKSPACE_ROLE_ASSIGNMENTS` | Terraform JSON map matching `workspace_role_assignments` |

At minimum, `FABRIC_WORKSPACE_ROLE_ASSIGNMENTS` must give the stage's generated
service-principal object ID enough workspace access for postconfiguration. The
map shape is:

```json
{
  "deployment": {
    "principal_id": "<github_oidc_service_principal_object_ids[stage]>",
    "principal_type": "ServicePrincipal",
    "role": "Admin"
  }
}
```

The bootstrap `fabric_capacity_ids_by_environment` output is an Azure Resource
Manager ID, while the Fabric provider variable validates a UUID. Resolve the
matching Fabric API UUID once during environment handoff. This is a bootstrap
diagnostic, not a deployment command:

```bash
stage=test
arm_id="$(
  terraform -chdir=infra/bootstrap output -json \
    fabric_capacity_ids_by_environment |
    jq -er --arg stage "${stage}" '.[$stage]'
)"
capacity_name="${arm_id##*/}"
fabric_token="$(
  az account get-access-token \
    --resource "https://api.fabric.microsoft.com" \
    --query accessToken -o tsv
)"
FABRIC_CAPACITY_ID="$(
  curl --fail --silent --show-error \
    --header "Authorization: Bearer ${fabric_token}" \
    "https://api.fabric.microsoft.com/v1/capacities" |
    jq -er --arg name "${capacity_name}" \
      '[.value[] | select(.displayName == $name) | .id]
       | if length == 1 then .[0]
         else error("expected exactly one matching Fabric capacity")
         end'
)"
unset fabric_token
printf '%s\n' "${FABRIC_CAPACITY_ID}"
```

Verify the display name, tenant, subscription, and stage before saving the
value as an environment variable.

Optional environment variables are:

| Variable | Default and effect |
|---|---|
| `FABRIC_ONELAKE_SHORTCUTS` | `{}`; Terraform JSON map matching `onelake_shortcuts` |
| `FABRIC_ENABLE_REPORTING` | `false`; when `true`, postconfiguration refreshes both models and clean-room acceptance requires reporting |
| `FABRIC_ENABLE_RTI` | `false`; when `true`, clean-room acceptance requires Eventhouse, KQL database, queryset, and dashboard |
| `FABRIC_ENABLE_ACTIVATOR` | `false`; when `true`, Terraform creates both reviewed Reflex items and both runtime gates require them |

Reporting and RTI item creation is driven by committed bundles, not by their
acceptance flags. Do not set a flag until every bundle named in
[the source-definition section](#4-source-definitions-and-live-export) is
reviewed. Activator requires both bundles and its flag. Shortcut values must
match the exact object type in
`infra/environments/<stage>/variables.tf`; use `{}` rather than an empty string.

### Fallback secret exception policy

OIDC is mandatory for Azure, Terraform, Fabric, OneLake, and Power BI token
acquisition. An Azure client secret, Fabric client secret, storage key, SAS, or
personal access token is not an accepted fallback. Do not add a repository
secret to work around failed OIDC or tenant settings.

The only secret currently wired into deployment is the environment-level
`FABRIC_ACTIVATOR_ACTION_CHECKPOINT_JSON`. A runtime contract may reference
connection values through `valuesFromEnvironment`, but the reusable workflow
does not provide generic secret pass-through. Adding a connection secret is an
exception that requires:

1. a reviewed, connector-specific workflow mapping from one protected
   environment secret to one named process environment variable;
2. owner, purpose, least privilege, rotation, and expiry recorded in the
   change;
3. confirmation that it never enters Terraform state, command arguments,
   committed runtime JSON, logs, or artifacts; and
4. removal when workload identity or another non-secret credential is
   available.

Unsupported OAuth consent, gateway encryption, Direct Lake rewrites, and item
share removal remain operator checkpoints; a secret does not convert them into
successful automation.

## Source definitions and live export

All seven Spark Job Definitions come from
`fabric/spark-job-definitions/`; GitHub does not supply their definitions.
Terraform injects environment-specific Lakehouse and Environment IDs through
the Fabric provider's Go-template definition tokens.

Semantic model, report, RTI, Activator, Variable Library, and other supported
workspace definitions follow the deterministic live-export process in
[`fabric/README.md`](../fabric/README.md). Pull requests validate each committed
part against its schema, size, and SHA-256 evidence; the repository does not
manufacture definitions when source-workspace credentials are unavailable.

Required bundle locations are:

- reporting:
  `fabric/semantic-models/{analytics,operational}` and
  `fabric/reports/{analytics,operational}`;
- RTI: `fabric/rti/{queryset,dashboard}`; and
- Activator: `fabric/activators/{operations,manifest-arrival}`.

Terraform creates the target Eventhouse and read/write KQL database when RTI
content is enabled. Their definition bundles are optional because Fabric can
deny definition readback for managed monitoring resources.

Export on a review branch with `scripts/export_fabric_definition.py`, inspect
every tokenized text part, validate with
`scripts/validate_fabric_definitions.py`, and commit the bundle plus its
`.fabric-item.json`. Source workspace/item IDs are ephemeral exporter inputs;
they are never workflow inputs or committed target configuration. See
[`fabric/README.md`](../fabric/README.md) for the exact batch and single-item
commands.

Existing target resources are adopted before feature enablement with the
migration-only imports in
[`infra/modules/fabric-foundation/IMPORT.md`](../infra/modules/fabric-foundation/IMPORT.md).
Import into the matching backend, then require a reviewed no-delete plan.

## Plans, approvals, and drift controls

Every `_terraform.yml` environment run uploads the binary plan, `tfplan.json`,
and init, validate, plan, and (for applies) apply logs for 30 days, including
on failure after evidence initialization. Authorized PR plan artifacts are
retained for 14 days. Plans that
delete or replace protected Fabric resources fail before apply unless a
protected-environment dispatch explicitly enables `allow_protected_changes`.
OneLake shortcut changes, workspace identity changes, capacity changes, and
Production Fabric item deletions/replacements each have a separate approval
checkbox. A change in more than one category requires every applicable
checkbox. The job still waits for the target GitHub Environment reviewer, so a
dispatcher cannot self-approve by selecting a checkbox. Automatic Development
pushes never set these approvals; rerun Development manually after review.
Terraform `prevent_destroy` remains a harder control on core resources; an
approval checkbox cannot bypass it. Removing that lifecycle rule requires its
own reviewed source change before the plan-level approvals can apply.

The scheduled drift workflow plans dev, test, and prod. It opens or updates one
`Fabric deployment drift detected` issue with links to plan artifacts and closes
that issue after all three environments return to a clean plan. Each job also
exports only source-controlled Fabric item definitions, applies the same
environment token sanitization as the migration exporter in memory, and
compares aggregate SHA-256 hashes. Live definition bytes, token source values,
workspace IDs, item IDs, and operation IDs are not written to drift evidence or
uploaded. Optional manifest entries are checked as soon as their
`.fabric-item.json` bundle is committed.

## Runtime reconciliation and operator checkpoints

After Terraform apply, postconfiguration publishes the Environment wheel and
runs the Control SJD with `bootstrap` before reconciling schedules. That job
creates or validates the Delta-backed Lakehouse control tables without storing
table or file contents in Terraform state.

The stage-specific contract in `fabric/runtime/<stage>.json` controls supported
post-provisioning work. The reconciler validates the contract strictly and
does not accept arbitrary URLs or request payloads. It:

- verifies every namespaced Delta table after the Control bootstrap;
- creates declared `Files/` directories with the documented OneLake DFS API;
- writes an immutable, commit-addressed release manifest and refuses to
  overwrite conflicting content;
- updates documented Fabric connection credential types from named environment
  variables without writing values to Terraform state, command arguments, or
  evidence;
- applies supported Power BI ownership, datasource, on-premises gateway, and
  imported-model refresh-schedule operations;
- removes deprecated workspace and connection role assignments while refusing
  to remove the last workspace Admin; and
- archives resource IDs, operation IDs, definition hashes, Terraform-output
  hashes, validation results, and unresolved operator checkpoints.

Connection authentication entries contain only `valuesFromEnvironment` names.
Only use them after the connector-specific workflow wiring and secret exception
review described above; declaring a name in runtime JSON alone does not expose
a GitHub secret to the process. OAuth2 interactive consent, gateway-public-key
encryption for on-premises credentials, Direct Lake connection rewrites, and
item-level share removal have no suitable public automation contract in this
workflow. When requested, they produce
`operator_checkpoint_required` evidence and exit code 2; schedules remain
disabled. They are never represented as successful fallbacks.

The postconfiguration evidence schema is
`people-counter-fabric-postconfigure-evidence-v1`. A successful run has
`status: succeeded`; API or validation failures have `status: failed`.
Credential-bearing keys and authorization-like strings are recursively
redacted before JSON serialization. Access tokens are origin-bound so a Fabric,
OneLake, or Power BI token cannot follow a continuation or operation URL to a
different host.

## Workflow and promotion model

Pull requests run Python deployment-manifest tests and validate every current
Terraform root. Merges to `main` apply Development. Test and Production
promotion is manual and uses the protected target environment. Promotion
requires the full 40-character SHA of a commit reachable from `main`; use the
same SHA that completed the Development deployment. This prevents a rerun or a
later promotion from silently selecting a newer branch head.

Test and Production roots must exist under `infra/environments/` before using
the promotion workflow. The reusable deployment workflow serializes the complete deployment per
environment and never cancels an in-progress run. It runs Terraform, then runs
postconfiguration twice against the same immutable SHA with every schedule
disabled. Clean-room acceptance requires a subsequent Terraform plan to return
exit code 0 and proves that the repeated deployment did not create duplicate
items or schedules. Only after acceptance succeeds may the Production
`final-gate` phase enable the five recurring schedules. Development and Test
schedules remain disabled.

The workflow entry points are:

- `pr-validate.yml`: credential-free validation for every pull request;
- `pr-plan-authorized.yml`: operator-dispatched, protected cloud plan for a
  reviewed same-repository pull request;
- `deploy-dev.yml`: automatic Development apply on relevant `main` changes,
  or manual rerun for explicitly approved protected changes;
- `promote.yml`: manual immutable Test/Production promotion;
- `drift-detect.yml`: weekday plans and sanitized definition-hash checks.

`promote.yml` accepts only `environment`, `release_sha`, and these optional
boolean approvals: `allow_protected_changes`, `approve_shortcut_changes`,
`approve_workspace_identity_changes`, `approve_capacity_changes`, and
`approve_production_deletions`. Set only approvals required by the reviewed
plan. An approval input does not bypass GitHub Environment review or Terraform
`prevent_destroy`.

## Clean-room acceptance gates

The retained `people-counter-fabric-cleanroom-evidence-v1` document validates:

- all seven SJD definitions and their Environment, Lakehouse, and entry-point
  references;
- exactly one disabled schedule for each recurring role and no schedules for
  Process or Cutover before the final gate;
- namespaced Lakehouse tables, configured OneLake folders, the exact Terraform
  shortcut set, and the immutable commit-addressed release manifest;
- both semantic-model refreshes on both deployment passes, Direct Lake
  workspace/Lakehouse bindings, and each PBIR report's semantic-model reference;
- Eventhouse, KQL database, queryset, and dashboard assets when RTI is enabled;
- Activator definition readback and unexpired per-action execution evidence
  when Activator is enabled; and
- commit, definition-hash, Terraform-output, resource-ID, and zero-drift
  provenance.

Set `FABRIC_ENABLE_REPORTING=true`, `FABRIC_ENABLE_RTI=true`, and
`FABRIC_ENABLE_ACTIVATOR=true` in the protected GitHub Environment to make all
optional clean-room gates mandatory. Reporting requires committed bundles at
`fabric/semantic-models/{analytics,operational}` and
`fabric/reports/{analytics,operational}`. RTI requires committed bundles at
`fabric/rti/{queryset,dashboard}`; Terraform creates the target Eventhouse and
read/write KQL database without requiring their source definitions. Activator
requires `fabric/activators/{operations,manifest-arrival}` plus the
`FABRIC_ACTIVATOR_ACTION_CHECKPOINT_JSON` secret described below. These are
source definitions, not live item IDs.

### Reporting, RTI, and Activator gates

- With `FABRIC_ENABLE_REPORTING=true`, both postconfiguration passes must
  refresh both semantic models, and acceptance must verify Direct Lake
  bindings plus PBIR model references.
- With `FABRIC_ENABLE_RTI=true`, acceptance requires all four RTI outputs and
  definition readback. The flag does not create missing bundles.
- With `FABRIC_ENABLE_ACTIVATOR=true`, Terraform and runtime both require the
  `operations` bundle. Store a current, time-bounded action checkpoint as the
  `FABRIC_ACTIVATOR_ACTION_CHECKPOINT_JSON` environment secret. A missing,
  expired, or topology-mismatched checkpoint blocks every postconfiguration
  pass, including Production schedule enablement.

The checkpoint binds the workspace, Reflex, trigger, live definition hash,
action IDs/kinds, verification time/expiry/operator, and durable evidence for
each action. See [`fabric/activators/README.md`](../fabric/activators/README.md)
for its contract and service-principal CRUD probe.

Reporting, Activator, and operator checkpoints are hard gates. Unsupported
credentials, Direct Lake rewrites, gateway encryption, interactive consent,
item-share removal, or Activator actions produce
`operator_checkpoint_required`; the workflow uploads evidence, exits
unsuccessfully, skips downstream jobs, and leaves schedules disabled.

## Live clean-room completion procedure

Complete these steps in order. Credential-free validation is not a live
deployment, and a successful Terraform apply alone is not acceptance.

1. **Finish bootstrap.** Populate all eight required variables in each GitHub
   Environment, add the generated principal to the tenant-settings group, and
   verify OIDC with an authorized protected plan. Never add a client secret.
2. **Commit deployable definitions.** Ensure all seven SJD bundles are present.
   Export, review, and commit every reporting/RTI/Activator bundle whose gate
   will be enabled. Run:

   ```bash
   uv run python scripts/validate_fabric_definitions.py fabric \
     --require-bundles
   ```

3. **Adopt existing target items.** If the target is not empty, perform the
   migration-only imports for that stage and require a normal plan with no
   unreviewed delete or replacement.
4. **Merge and accept Development.** Merge the reviewed commit to `main`.
   Require the `Deploy Development` run for that exact SHA to complete
   Terraform, `initial`, `repeat`, and clean-room smoke jobs successfully.
   Record the full 40-character SHA from the run. Development schedules remain
   disabled.
5. **Accept Test.** Configure Test with the same optional gates intended for
   Production, then dispatch:

```bash
gh workflow run promote.yml \
  --ref main \
  -f environment=test \
  -f release_sha=<40-character-main-commit-sha>
```

   Before any Test job can start, `promote.yml` uses read-only Actions API
   access to locate the exact successful `Deploy Development` run and its
   retained `fabric-smoke-dev-*` artifact. It requires one unexpired artifact
   for that run attempt and matching successful clean-room and smoke evidence
   for the requested SHA. Missing, expired, malformed, or ambiguous evidence
   fails the promotion closed.

6. **Inspect Test evidence.** Download `tfplan-test-*`,
   `fabric-evidence-test-initial-*`, `fabric-evidence-test-repeat-*`, and
   `fabric-smoke-test-*`. Require:
   - both `fabric-evidence.json` files to use
     `people-counter-fabric-postconfigure-evidence-v1` with
     `status: succeeded`;
   - `cleanroom.json` to use
     `people-counter-fabric-cleanroom-evidence-v1` with
     `status: succeeded`;
   - `smoke.json` to use `people-counter-fabric-smoke-evidence-v1` with
     `status: succeeded`;
   - the repeated Terraform plan exit code to be `0`;
   - exactly one disabled schedule for each recurring role and none for
     Process/Cutover; and
   - every enabled optional gate to appear as successful.
7. **Promote the identical SHA to Production.** Confirm the Production
   variables and flags match the reviewed release, then dispatch:

   ```bash
   gh workflow run promote.yml \
     --ref main \
     -f environment=prod \
     -f release_sha=<same-40-character-main-commit-sha>
   ```

   Add only the boolean approval inputs required by the reviewed plan, for
   example `-f approve_shortcut_changes=true`. The independent Production
   reviewer must compare the plan/evidence with Test before approving.
   The workflow repeats the immutable Development check and additionally
   requires a successful prior Test `promote.yml` run with one retained
   `fabric-smoke-test-*` artifact whose clean-room and smoke records both name
   the exact release SHA. The gate selects artifacts by run ID and run attempt;
   it never treats branch reachability alone as deployment evidence.
8. **Inspect Production safe-state evidence.** Require the same four plan,
   `initial`, `repeat`, and smoke artifacts/checks as Test. The Production
   smoke test intentionally runs while all recurring schedules are disabled.
9. **Verify the final gate.** Require
   `fabric-evidence-prod-final-gate-*` to contain successful
   `fabric-evidence.json`. Confirm it reconciled exactly one enabled schedule
   for Control, Dispatcher, Reconciliation, Gold, and Refresh, no schedules
   for Process/Cutover, successful optional runtime gates, and the identical
   commit/definition/output hashes.
10. **Record completion.** Link the immutable SHA, workflow run, all retained
    artifact names, approver, optional-gate results, and any operational
    follow-up in the release record. Live clean-room deployment is complete
    only after this step; absent backend/OIDC/environment values or absent
    exported bundles are blockers, not waived checks.

Terraform, both postconfiguration passes, clean-room acceptance, and smoke
artifacts are retained for 30 days.
Postconfiguration evidence includes the commit, non-secret deployment IDs, definition
hashes, operation IDs, and redacted runtime results. A failed or interrupted
run is resumed by rerunning the failed workflow with the same release SHA.
Terraform replans from remote state, and the reconcilers read back resources
before mutation. Never start a second deployment to work around a running
apply or Fabric long-running operation; queued runs are serialized and do not
cancel the active deployment.

If Terraform apply fails, no postconfiguration runs. If postconfiguration
fails, no smoke test runs. Failed postconfiguration evidence is still uploaded.
If clean-room acceptance fails, schedules remain disabled. If the Production
final gate fails after enabling schedules, treat the deployment as failed,
disable affected schedules if necessary, and forward-fix or rerun the same SHA;
Fabric operations are not transactionally rolled back.

## Drift, import, and state recovery

Treat `tfplan.json`, `protected-changes.json`, and
`definition-drift.json` from the 30-day plan artifact as the recovery record.
Never apply from a scheduled drift run. First classify each difference:

1. **Unauthorized live change:** restore Git-owned infrastructure or definition
   through the normal immutable deployment workflow.
2. **Legitimate unmanaged resource:** add its exact configuration to Git,
   review a plan, and import it into the matching environment state before any
   apply.
3. **Wrong Terraform address:** use a reviewed `moved` block when possible;
   use `terraform state mv` only during an operator-controlled recovery.
4. **Stale state only:** refresh with a new plan. Do not use `state rm` to make
   a proposed deletion disappear.

Initialize the same environment root and backend used by Actions, then preserve
an access-restricted state snapshot before repair:

```bash
environment=dev
terraform -chdir="infra/environments/${environment}" init \
  -backend-config="resource_group_name=${TF_BACKEND_RESOURCE_GROUP_NAME}" \
  -backend-config="storage_account_name=${TF_BACKEND_STORAGE_ACCOUNT_NAME}" \
  -backend-config="container_name=${TF_BACKEND_CONTAINER_NAME}" \
  -backend-config="key=people-counter/${environment}.tfstate" \
  -backend-config="use_azuread_auth=true"
umask 077
terraform -chdir="infra/environments/${environment}" state pull \
  > "state-backup-${environment}.json"
```

State snapshots can contain sensitive values. Keep the backup encrypted and
outside Git and workflow artifacts, then securely remove it after recovery.
Acquire the environment state lock, stop deployments, and import one address at
a time using the import ID syntax documented for the pinned
`microsoft/fabric` provider:

```bash
terraform -chdir="infra/environments/${environment}" import \
  'module.fabric_foundation.fabric_report.this["analytics"]' \
  '<provider-documented-import-id>'
terraform -chdir="infra/environments/${environment}" plan \
  -out=recovery.tfplan
terraform -chdir="infra/environments/${environment}" show \
  -json recovery.tfplan > recovery.tfplan.json
```

Import does not prove configuration equivalence. Require a clean reviewed plan
before resuming Actions. Workspace identity and workspace capacity assignment
are workspace attributes: reconcile their configuration and refresh the
workspace resource rather than inventing a second resource address. Capacity
resources themselves belong to bootstrap state, not an environment state.
Shortcuts must be imported at their declared map key and still require the
explicit shortcut approval if the recovery plan changes them. Production item
removal always requires both the general protected-change approval and the
Production-deletion approval.

For legitimate definition changes, export on a review branch with
`scripts/export_fabric_definition.py`, inspect the tokenized files, and run
`scripts/validate_fabric_definitions.py`. For unauthorized definition changes,
redeploy the committed bundle. Never copy a live definition from drift evidence:
the drift checker intentionally retains hashes only.

Use `terraform state rm` only when the remote object must intentionally remain
unmanaged and the corresponding Terraform configuration is removed in the same
reviewed change. Record the address, object ID, state backup location, operator,
reviewer, and clean follow-up run in the drift issue. If state restoration is
required, use `terraform state push` only as a last resort with a reviewed,
serial-compatible backup and all deployment workflows stopped.

Destructive recovery is always plan-led:

1. stop or queue deployments and disable recurring schedules when live writes
   could race the repair;
2. preserve state and live evidence before changing either;
3. restore/import/readdress the smallest affected object;
4. generate a fresh plan from the normal environment root;
5. obtain every applicable protected-change approval and environment review;
6. run the immutable workflow twice plus clean-room acceptance; and
7. re-enable Production schedules only through its final gate.

There is no transactional Fabric rollback. A failed postconfiguration may have
completed earlier idempotent operations, and the workflow deliberately reads
live state before retrying. Prefer rerunning the same SHA for transient failure.
For a source or definition defect, create a new reviewed forward-fix commit
(which may revert the defective source), deploy it to Development and Test, and
promote that new immutable SHA. Never apply an old saved plan, copy target GUIDs
from another environment, rewrite remote state to simulate rollback, or use a
Fabric Deployment Pipeline to overwrite Terraform-owned items.
