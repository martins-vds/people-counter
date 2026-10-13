# GitHub deployment runbook

Git, the environment-specific Terraform state, and retained workflow evidence
are the deployment source of truth. Fabric Deployment Pipelines must not change
resources that this repository manages.
Routine deployment never accepts a workspace or item GUID as a workflow input.

## Organizational bootstrap and tenant settings

The bootstrap operator needs:

- Microsoft Entra **Application Administrator** or **Cloud Application
  Administrator**.
- Azure **Contributor** plus **User Access Administrator** or **Role Based
  Access Control Administrator** on the target subscription.
- Permission under the tenant's Fabric capacity-creation policy.
- local Azure CLI and Terraform authentication for the target tenant and
  subscription.

Before applying the bootstrap stack:

1. Register the Azure `Microsoft.Fabric` resource provider.
2. Confirm that the subscription and region support the configured Fabric SKUs.
3. have a Fabric administrator prepare a least-privilege security group for
   the generated deployment service principals.
4. in the Fabric admin portal, scope the tenant settings that allow service
   principals to use Fabric public APIs and to create workspaces, connections,
   and deployment pipelines to that group.
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

Before the bootstrap plan, configure `adls_storage_by_environment` in the
local `terraform.tfvars`. Use one entry for each environment that needs ADLS
shortcuts.

Use `mode = "create"` to create an ADLS Gen2 account:

```hcl
adls_storage_by_environment = {
  dev = {
    mode                 = "create"
    storage_account_name = "peoplecounterdev"
  }
}
```

Use `mode = "existing"` to use an existing ADLS Gen2 account:

```hcl
adls_storage_by_environment = {
  prod = {
    mode                        = "existing"
    storage_account_resource_id = "/subscriptions/<subscription-uuid>/resourceGroups/<resource-group>/providers/Microsoft.Storage/storageAccounts/<account-name>"
  }
}
```

The `create` mode creates a StorageV2 account with hierarchical namespace
enabled. The `existing` mode does not change the account. The existing account
must have hierarchical namespace enabled.

Bootstrap state is local, access-restricted, and never committed. It creates
the state backend, one OIDC application/service principal per environment,
environment-scoped federated credentials, state-container RBAC, and Fabric
capacities. It also creates configured ADLS accounts and ADLS role
assignments. It does not create a client secret.

Record the bootstrap outputs immediately:

```bash
terraform -chdir=infra/bootstrap output -json \
  backend_config_by_environment
terraform -chdir=infra/bootstrap output -json github_oidc_client_ids
terraform -chdir=infra/bootstrap output -json \
  github_oidc_service_principal_object_ids
terraform -chdir=infra/bootstrap output -json \
  fabric_capacity_ids_by_environment
terraform -chdir=infra/bootstrap output -json \
  adls_storage_account_ids_by_environment
terraform -chdir=infra/bootstrap output -raw tenant_id
terraform -chdir=infra/bootstrap output -raw subscription_id
```

Configure each downstream environment's `azurerm` backend from
`backend_config_by_environment` with `use_azuread_auth = true`. The bootstrap
stack cannot use the backend it creates, so retain its local state securely.

## OIDC trust

For each stage, the generated federated credential is:

```text
issuer:   https://token.actions.githubusercontent.com
audience: api://AzureADTokenExchange
subject:  repo:<owner>/<repository>:environment:<stage>
```

The `<owner>/<repository>` value comes from bootstrap
`github_repository`. `<stage>` is `dev`, `test`, or `prod`. A spelling or case
mismatch between that value and the GitHub Environment prevents token
exchange. All cloud jobs declare an environment, request only
`contents: read` and `id-token: write`, and use `azure/login`. Repository-level
permissions are empty by default.

## GitHub Environments and variables

Create protected GitHub Environments named exactly `dev`, `test`, and `prod`.
Restrict deployment branches to `main`. Require an independent reviewer for
Production, prevent administrator bypass, and apply the same review rule to
Test when policy requires independent promotion. Keep variables and secrets at
the environment level, not repository level. This selects OIDC trust and
configuration together.

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
| `FABRIC_WORKSPACE_ROLE_ASSIGNMENTS` | JSON object keyed by stable assignment labels. Each value contains `principal_id`, `principal_type`, and `role`. |

### Workspace role assignments

`FABRIC_WORKSPACE_ROLE_ASSIGNMENTS` is a GitHub Environment variable whose
value is JSON text. Its top level is an **object/map, not an array**. Each
top-level property name is a caller-chosen stable label. Terraform uses the
label to track that assignment. It is not a Fabric role or principal name.

Each assignment value must contain these required fields:

- `principal_id`: the Microsoft Entra **object ID** of a user, group, service
  principal, or service-principal profile. For the deployment identity, use
  `github_oidc_service_principal_object_ids[stage]`, not
  `github_oidc_client_ids[stage]`.
- `principal_type`: one of `User`, `Group`, `ServicePrincipal`, or
  `ServicePrincipalProfile`.
- `role`: one of `Admin`, `Contributor`, `Member`, or `Viewer`.

At minimum, grant the stage's generated deployment service principal `Admin`
so Terraform and postconfiguration can manage the workspace. For one
assignment, paste this JSON object directly as the GitHub variable value:

```json
{
  "deployment": {
    "principal_id": "<github_oidc_service_principal_object_ids[stage]>",
    "principal_type": "ServicePrincipal",
    "role": "Admin"
  }
}
```

Do not wrap the entire JSON value in another pair of quotes. Compact,
single-line JSON and formatted, multiline JSON represent the same value.

To grant multiple principals access, add sibling properties to the same
top-level object. Do not convert it to an array:

```json
{
  "deployment": {
    "principal_id": "<github_oidc_service_principal_object_ids[stage]>",
    "principal_type": "ServicePrincipal",
    "role": "Admin"
  },
  "platform_operators": {
    "principal_id": "<operators-group-object-id>",
    "principal_type": "Group",
    "role": "Admin"
  },
  "report_readers": {
    "principal_id": "<readers-group-object-id>",
    "principal_type": "Group",
    "role": "Viewer"
  }
}
```

Use a separate JSON value for each GitHub Environment so its `deployment`
entry references that stage's generated service-principal object ID. Keep
stable labels unchanged after deployment unless intentionally replacing the
corresponding Terraform-managed role assignment.

The bootstrap `fabric_capacity_ids_by_environment` output is an Azure Resource
Manager ID, while the Fabric provider variable validates a UUID. Resolve the
matching Fabric capacity UUID once during environment handoff. Do not save an
Azure resource ID such as
`/subscriptions/.../providers/Microsoft.Fabric/capacities/fabpcdev...` as
`FABRIC_CAPACITY_ID`.

### Recommended: copy the Capacity ID from the Fabric Admin portal

For each deployed stage:

1. Open Microsoft Fabric and select **Settings** > **Admin portal**.
2. Select **Capacity settings**.
3. Select the capacity created by the bootstrap stack. Match its capacity name
   to the final segment of that stage's
   `fabric_capacity_ids_by_environment` Azure resource ID.
4. In the capacity's **Settings** panel, copy the UUID shown under
   **Capacity ID**.
5. Verify the capacity name, SKU, region, and target stage.
6. Save the copied UUID as `FABRIC_CAPACITY_ID` in the matching GitHub
   Environment.

The required value is only the UUID displayed as **Capacity ID**, for example
`00000000-0000-0000-0000-000000000000`.

### Alternative: resolve the Capacity ID through the Fabric API

An operator whose Fabric API identity can list capacities may resolve the same
UUID with the following bootstrap diagnostic. This is not a deployment
command:

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

If the API returns no matching capacity or an authorization error, use the
Fabric Admin portal method above. Verify the display name, tenant,
subscription, and stage before saving the value as an environment variable.

## Resource ownership and deployment order

The deployment has four ownership boundaries:

| Owner | Resources and actions |
|---|---|
| Bootstrap Terraform | Azure resource group, state storage, Entra applications, service principals, OIDC trust, state RBAC, Fabric capacities, and configured ADLS accounts |
| Environment Terraform | Target workspace, workspace roles, Lakehouse, Environment, seven SJDs, Variable Library, configured shortcuts, and optional Fabric items |
| Runtime automation | Lakehouse tables, `Files/` directories, release manifest, model refreshes, supported service settings, and schedules |
| Operator or external system | Tenant settings, GitHub configuration, approvals, shortcut source items, Activator action evidence, and unsupported integrations |

Do not create the target workspace, Lakehouse, Environment, SJD, semantic
model, report, RTI item, Activator, or shortcut manually. Environment Terraform
creates these resources.

### Destination IDs and shortcut source IDs

Terraform creates the destination workspace and Lakehouse. It passes their IDs
directly to the Fabric provider. An operator does not enter these destination
IDs.

A OneLake shortcut points from the new Lakehouse to an existing OneLake item.
The shortcut configuration therefore requires two source IDs:

- `target_workspace_id` identifies the existing source workspace.
- `target_item_id` identifies the existing source item, usually a Lakehouse.

The source item can belong to another Terraform environment or another team.
The source item must exist before shortcut creation. If you need no shortcut,
set `FABRIC_ONELAKE_SHORTCUTS` to `{}`.

An ADLS Gen2 shortcut points to a container in the configured storage account.
Bootstrap can create the storage account or reference an existing account.
Environment Terraform creates the workspace identity, connection, RBAC, and
shortcut. It can also create the configured container.

### Definition bundles and target IDs

Definition bundles are files in Git. They are not target Fabric resources.
Terraform creates the target resources from these committed files.

During apply, Terraform injects the new target IDs into the bundles. This
includes workspace, Lakehouse, Environment, semantic model, Eventhouse, and KQL
database IDs.

The repository already contains the required deployment bundles. A normal
environment deployment does not require a source authoring workspace. Use the
definition export process only when you change a committed bundle.

### Required steps after bootstrap

Complete these steps after the bootstrap apply:

1. Add each generated service principal to the approved tenant-settings group.
2. Create GitHub Environments named `dev`, `test`, and `prod`.
3. Configure branch restrictions and required reviewers.
4. Add the eight required variables to each GitHub Environment.
5. Set each `FABRIC_CAPACITY_ID` to the Fabric capacity UUID.
6. Set `FABRIC_WORKSPACE_ROLE_ASSIGNMENTS` for each environment.
7. Leave `FABRIC_ONELAKE_SHORTCUTS` unset unless you need a OneLake shortcut.
8. Set the two ADLS variables when the environment needs ADLS shortcuts.
9. Review each optional definition bundle.
10. Set the Reporting, RTI, and Activator flags.
11. Add the Activator checkpoint secret when you enable Activator.
12. Run an authorized protected Terraform plan to verify OIDC and access.
13. Merge the reviewed commit to deploy Development.

Terraform creates the target Fabric resources after step 13. Operators approve
Test and Production promotion through the protected GitHub Environments.

Optional environment variables are:

| Variable | Accepted value and behavior |
|---|---|
| `FABRIC_ONELAKE_SHORTCUTS` | Set this variable only for OneLake-to-OneLake shortcuts. Leave it unset to create no OneLake shortcuts. |
| `FABRIC_ADLS_STORAGE_ACCOUNT_RESOURCE_ID` | Value from `adls_storage_account_ids_by_environment[stage]`. Set it when `FABRIC_ADLS_SHORTCUTS` is not `{}`. |
| `FABRIC_ADLS_SHORTCUTS` | JSON object described under [ADLS Gen2 shortcut configuration](#adls-gen2-shortcut-configuration). Omit it or use `{}` to create no ADLS shortcuts. |
| `FABRIC_ENABLE_REPORTING` | String `true` or `false`. The default is `false`. `true` requires both reporting bundles and verifies both semantic models and reports. |
| `FABRIC_ENABLE_RTI` | String `true` or `false`. The default is `false`. `true` requires and verifies the Eventhouse, KQL database, queryset, and dashboard. |
| `FABRIC_ENABLE_ACTIVATOR` | String `true` or `false`. The default is `false`. `true` creates both Reflex items and requires definition and action evidence. |

Committed bundles control Reporting and RTI item creation. Do not set a flag
until every bundle named in
[Definition bundles and target IDs](#definition-bundles-and-target-ids) is
reviewed. Activator requires both bundles and its flag.

### OneLake shortcut configuration

Do not set `FABRIC_ONELAKE_SHORTCUTS` for the ADLS Gen2 shortcut described in
this runbook. `FABRIC_ADLS_SHORTCUTS` configures that shortcut.

Set `FABRIC_ONELAKE_SHORTCUTS` only when the managed Lakehouse must read data
from an existing OneLake item. The pipeline passes `{}` to Terraform when the
GitHub Environment variable is absent. Therefore, both of these configurations
create no OneLake shortcuts:

- `FABRIC_ONELAKE_SHORTCUTS` does not exist.
- `FABRIC_ONELAKE_SHORTCUTS` contains `{}`.

Leaving the variable unset is the recommended configuration when you use only
ADLS Gen2 shortcuts.

The repository manages the destination Lakehouse and the shortcut resource.
It does not automatically select or create the source OneLake item. A OneLake
shortcut always requires the UUID of an existing source workspace and source
item.

The pipeline does not copy source UUIDs from another Terraform state. If
another environment in this repository owns the source item, copy its
`workspace_id` and `lakehouse_id` outputs into this variable. Otherwise,
request the source UUIDs from the team that owns the source item.

`FABRIC_ONELAKE_SHORTCUTS` contains JSON text. Its top level is an object, not
an array. Each top-level key is a stable Terraform label for one shortcut.

Every shortcut value requires:

- `name`: non-empty leaf name of the shortcut. The managed Lakehouse exposes
  it at `<path>/<name>`.
- `path`: parent path inside the destination Lakehouse managed by this
  deployment. It must start with `Files/` or `Tables/` and must not start with
  `/`.
- `target_workspace_id`: UUID of the workspace containing the source OneLake
  item.
- `target_item_id`: UUID of the source OneLake item, typically a Lakehouse.
- `target_path`: source path inside that item. It must start with `Files/` or
  `Tables/` and must not start with `/`.

For example, this creates
`Files/input/incoming_videos` and
`Tables/shared/shared_dimensions` inside the stage's managed Lakehouse:

```json
{
  "incoming_videos": {
    "name": "incoming_videos",
    "path": "Files/input",
    "target_workspace_id": "<source-workspace-uuid>",
    "target_item_id": "<source-lakehouse-uuid>",
    "target_path": "Files/videos"
  },
  "shared_dimensions": {
    "name": "shared_dimensions",
    "path": "Tables/shared",
    "target_workspace_id": "<source-workspace-uuid>",
    "target_item_id": "<source-lakehouse-uuid>",
    "target_path": "Tables/dimensions"
  }
}
```

Paste the JSON object directly as the GitHub variable value. Do not wrap the
entire value in another pair of quotes. Compact single-line and formatted
multiline JSON are equivalent. Use separate values per GitHub Environment
when Dev, Test, and Production point at different source workspaces or items.

The deployment identity must have access to the configured source workspace
and source item.

If Terraform already manages OneLake shortcuts and you remove this variable,
the next plan proposes deletion of those shortcuts. Review that plan before
apply.

Terraform uses conflict policy `Abort`. Deployment fails when another object
uses `<path>/<name>`. Remove the conflict or choose another destination.
Keep each stable map label unchanged after deployment.

### ADLS Gen2 shortcut configuration

Set `FABRIC_ADLS_STORAGE_ACCOUNT_RESOURCE_ID` to the bootstrap output for the
same stage. Use the complete Azure resource ID. Do not use the account name or
DFS URL.

`FABRIC_ADLS_SHORTCUTS` is a GitHub Environment variable containing JSON text.
Its top level is an object, not an array. Each top-level key is a stable
Terraform label for one shortcut.

Every shortcut value requires:

- `name`: leaf name of the shortcut in the managed Lakehouse.
- `path`: parent path in the managed Lakehouse. Start it with `Files/` or
  `Tables/`.
- `container`: ADLS Gen2 container name.
- `target_path`: source path inside the container. Use `""` for the container
  root.
- `create_container`: boolean value. Use `true` to create the container.

Use `create_container = false` when the container already exists. Terraform
does not create or change that container. The deployment fails if Fabric
cannot access it.

This example creates two shortcuts. Terraform creates the `incoming` container.
The `archive` container must already exist.

```json
{
  "incoming_videos": {
    "name": "incoming_videos",
    "path": "Files/input",
    "container": "incoming",
    "target_path": "videos",
    "create_container": true
  },
  "archive": {
    "name": "archive",
    "path": "Files/history",
    "container": "archive",
    "target_path": "",
    "create_container": false
  }
}
```

Paste the object directly as the GitHub variable value. Do not add quotes
around the complete object. Use `{}` when the environment needs no ADLS
shortcut.

Environment Terraform enables the workspace identity. It grants that identity
`Storage Blob Data Contributor` on the storage account. It then creates one
WorkspaceIdentity Fabric connection and all configured shortcuts.

Bootstrap grants the deployment principal `Storage Blob Data Contributor`.
This role permits container creation. Bootstrap also grants
`Reader` so Terraform can verify the account. Bootstrap grants
`User Access Administrator` for the workspace identity role assignment.

The storage account must use ADLS Gen2 hierarchical namespace. Fabric must be
able to reach its DFS endpoint. This implementation does not support S3,
Dataverse, storage keys, SAS tokens, or client secrets.

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
   environment secret to one named process environment variable.
2. owner, purpose, least privilege, rotation, and expiry recorded in the
   change.
3. confirmation that it never enters Terraform state, command arguments,
   committed runtime JSON, logs, or artifacts.
4. removal when workload identity or another non-secret credential is
   available.

Unsupported OAuth consent, gateway encryption, Direct Lake rewrites, and item
share removal remain operator checkpoints. A secret does not convert them into
successful automation.

## Source definitions and live export

All seven Spark Job Definitions come from
`fabric/spark-job-definitions/`. GitHub does not supply their definitions.
Terraform injects environment-specific Lakehouse and Environment IDs through
the Fabric provider's Go-template definition tokens.

Semantic model, report, RTI, Activator, Variable Library, and other supported
workspace definitions follow the deterministic definition export process in
[`fabric/README.md`](../fabric/README.md). Normal deployment reads the committed
bundles. It does not access the authoring workspace.

Use the definition export process only when you change a bundle. Pull requests
validate each committed part against its schema, size, and SHA-256 evidence.

Required bundle locations are:

- reporting:
  `fabric/semantic-models/{analytics,operational}` and
  `fabric/reports/{analytics,operational}`.
- RTI: `fabric/rti/{queryset,dashboard}`.
- Activator: `fabric/activators/{operations,manifest-arrival}`.

When `FABRIC_ENABLE_RTI=true`, Terraform creates the target Eventhouse and
read/write KQL database. Their definition bundles are optional because Fabric can
deny definition readback for managed monitoring resources.

Export on a review branch with `scripts/export_fabric_definition.py`, inspect
every tokenized text part. Validate with
`scripts/validate_fabric_definitions.py`. Commit the bundle and its
`.fabric-item.json`. Source workspace/item IDs are temporary exporter inputs.
They are never workflow inputs or committed target configuration. See
[`fabric/README.md`](../fabric/README.md) for the exact batch and single-item
commands.

## Plans, approvals, and drift controls

Every `_terraform.yml` environment run uploads the binary plan, `tfplan.json`,
and init, validate, plan, and (for applies) apply logs for 30 days, including
on failure after evidence initialization. Authorized PR plan artifacts are
retained for 14 days. Plans that
delete or replace protected Fabric resources fail before apply unless a
protected-environment dispatch explicitly enables `allow_protected_changes`.
Managed shortcut changes, workspace identity changes, capacity changes, and
Production Fabric item deletions/replacements each have a separate approval
checkbox. A change in more than one category requires every applicable
checkbox. The job still waits for the target GitHub Environment reviewer. A dispatcher
cannot self-approve by selecting a checkbox. Automatic Development pushes
never set these approvals. Rerun Development manually after review.
Terraform `prevent_destroy` remains a harder control on core resources. An
approval checkbox cannot bypass it. Removing that lifecycle rule requires its
own reviewed source change before the plan-level approvals can apply.

The scheduled drift workflow plans dev, test, and prod. It opens or updates one
`Fabric deployment drift detected` issue with links to plan artifacts and closes
that issue after all three environments return to a clean plan. Each job also
exports only source-controlled Fabric item definitions, applies the same
environment token sanitization as the definition exporter in memory, and
compares aggregate SHA-256 hashes. Live definition bytes, token source values,
workspace IDs, item IDs, and operation IDs are not written to drift evidence or
uploaded. The drift workflow checks an optional entry after Git contains its
`.fabric-item.json` bundle.

## Runtime reconciliation and operator checkpoints

After Terraform apply, postconfiguration publishes the Environment wheel and
runs the Control SJD with `bootstrap` before reconciling schedules. That job
creates or validates the Delta-backed Lakehouse control tables without storing
table or file contents in Terraform state.

The stage-specific contract in `fabric/runtime/<stage>.json` controls supported
post-provisioning work. The reconciler validates the contract strictly and
does not accept arbitrary URLs or request payloads. It:

- verifies every namespaced Delta table after the Control bootstrap.
- creates declared `Files/` directories with the documented OneLake DFS API.
- writes an immutable, commit-addressed release manifest and refuses to
  overwrite conflicting content.
- updates documented Fabric connection credential types from named environment
  variables without writing values to Terraform state, command arguments, or
  evidence.
- applies supported Power BI ownership, datasource, on-premises gateway, and
  imported-model refresh-schedule operations.
- removes deprecated workspace and connection role assignments while refusing
  to remove the last workspace Admin.
- archives resource IDs, operation IDs, definition hashes, Terraform-output
  hashes, validation results, and unresolved operator checkpoints.

Connection authentication entries contain only `valuesFromEnvironment` names.
Only use them after the connector-specific workflow wiring and secret exception
review described above. A name in runtime JSON does not expose
a GitHub secret to the process. OAuth2 interactive consent, gateway-public-key
encryption for on-premises credentials, Direct Lake connection rewrites, and
item-level share removal have no suitable public automation contract in this
workflow. When requested, they produce
`operator_checkpoint_required` evidence and exit code 2. Schedules remain
disabled. They are never represented as successful fallbacks.

The postconfiguration evidence schema is
`people-counter-fabric-postconfigure-evidence-v1`. A successful run has
`status: succeeded`. API or validation failures have `status: failed`.
Credential-bearing keys and authorization-like strings are recursively
redacted before JSON serialization. Access tokens are origin-bound so a Fabric,
OneLake, or Power BI token cannot follow a continuation or operation URL to a
different host.

## Workflow and promotion model

Pull requests run Python deployment-manifest tests and validate every current
Terraform root. Merges to `main` apply Development. Test and Production
promotion is manual and uses the protected target environment. Promotion
requires the full 40-character SHA of a commit reachable from `main`. Use the
same SHA that completed the Development deployment. This prevents a rerun or a
later promotion from silently selecting a newer branch head.

Test and Production roots must exist under `infra/environments/` before using
the promotion workflow. The reusable deployment workflow serializes the complete deployment per
environment and never cancels an in-progress run. It runs Terraform, then runs
postconfiguration twice against the same immutable SHA with every schedule
disabled. Deployment verification requires the next Terraform plan to return
exit code 0. It also proves that the repeated deployment did not create
duplicate items or schedules. Only after verification succeeds may the Production
`final-gate` phase enable the five recurring schedules. Development and Test
schedules remain disabled.

The workflow entry points are:

- `pr-validate.yml`: credential-free validation for every pull request.
- `pr-plan-authorized.yml`: operator-dispatched, protected cloud plan for a
  reviewed same-repository pull request.
- `deploy-dev.yml`: automatic Development apply on relevant `main` changes,
  or manual rerun for explicitly approved protected changes.
- `promote.yml`: manual immutable Test/Production promotion.
- `drift-detect.yml`: weekday plans and sanitized definition-hash checks.

`promote.yml` accepts only `environment`, `release_sha`, and these optional
boolean approvals: `allow_protected_changes`, `approve_shortcut_changes`,
`approve_workspace_identity_changes`, `approve_capacity_changes`, and
`approve_production_deletions`. Set only approvals required by the reviewed
plan. An approval input does not bypass GitHub Environment review or Terraform
`prevent_destroy`.

## Deployment verification gates

The retained `people-counter-fabric-deployment-verification-v1` document
validates:

- all seven SJD definitions and their Environment, Lakehouse, and entry-point
  references.
- exactly one disabled schedule for each recurring role and no schedules for
  Process or Cutover before the final gate.
- namespaced Lakehouse tables, configured OneLake folders, the exact Terraform
  shortcut set, and the immutable commit-addressed release manifest.
- both semantic-model refreshes on both deployment passes, Direct Lake
  workspace/Lakehouse bindings, and each PBIR report's semantic-model reference.
- Eventhouse, KQL database, queryset, and dashboard assets when
  `FABRIC_ENABLE_RTI=true`.
- Activator definition readback and unexpired per-action execution evidence
  when `FABRIC_ENABLE_ACTIVATOR=true`.
- commit, definition-hash, Terraform-output, resource-ID, and zero-drift
  provenance.

Set `FABRIC_ENABLE_REPORTING=true`, `FABRIC_ENABLE_RTI=true`, and
`FABRIC_ENABLE_ACTIVATOR=true` in the protected GitHub Environment. These
settings make their verification gates mandatory. Reporting requires bundles at
`fabric/semantic-models/{analytics,operational}` and
`fabric/reports/{analytics,operational}`. RTI requires committed bundles at
`fabric/rti/{queryset,dashboard}`. Terraform creates the target Eventhouse and
read/write KQL database without requiring their source definitions. Activator
requires `fabric/activators/{operations,manifest-arrival}` plus the
`FABRIC_ACTIVATOR_ACTION_CHECKPOINT_JSON` secret described below. These are
source definitions, not live item IDs.

### Reporting, RTI, and Activator gates

- With `FABRIC_ENABLE_REPORTING=true`, both postconfiguration passes must
  refresh both semantic models. Verification must check Direct Lake
  bindings plus PBIR model references.
- With `FABRIC_ENABLE_RTI=true`, verification requires all four RTI outputs and
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
`operator_checkpoint_required`. The workflow uploads evidence, exits
unsuccessfully, skips downstream jobs, and leaves schedules disabled.

## Deployment procedure

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

3. **Merge and verify Development.** Merge the reviewed commit to `main`.
   Require the `Deploy Development` run for that exact SHA to complete
   Terraform, `initial`, `repeat`, deployment verification, and smoke jobs.
   Record the full 40-character SHA from the run. Development schedules remain
   disabled.
4. **Verify Test.** Configure Test with the same optional gates intended for
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
   for that run attempt and matching deployment verification and smoke evidence
   for the requested SHA. Missing, expired, malformed, or ambiguous evidence
   fails the promotion closed.

5. **Inspect Test evidence.** Download `tfplan-test-*`,
   `fabric-evidence-test-initial-*`, `fabric-evidence-test-repeat-*`, and
   `fabric-smoke-test-*`. Require:
   - both `fabric-evidence.json` files to use
     `people-counter-fabric-postconfigure-evidence-v1` with
     `status: succeeded`.
   - `deployment-verification.json` to use
     `people-counter-fabric-deployment-verification-v1` with
     `status: succeeded`.
   - `smoke.json` to use `people-counter-fabric-smoke-evidence-v1` with
     `status: succeeded`.
   - the repeated Terraform plan exit code to be `0`.
   - exactly one disabled schedule for each recurring role and none for
     Process/Cutover.
   - every enabled optional gate to appear as successful.
6. **Promote the identical SHA to Production.** Confirm the Production
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
   requires a successful Test `promote.yml` run with one retained
   `fabric-smoke-test-*` artifact. Both verification records must name
   the exact release SHA. The gate selects artifacts by run ID and run attempt.
   It never treats branch reachability alone as deployment evidence.
7. **Inspect Production safe-state evidence.** Require the same four plan,
   `initial`, `repeat`, and smoke artifacts/checks as Test. The workflow
   disables all recurring schedules during the Production smoke test.
8. **Verify the final gate.** Require
   `fabric-evidence-prod-final-gate-*` to contain successful
   `fabric-evidence.json`. Confirm it reconciled exactly one enabled schedule
   for Control, Dispatcher, Reconciliation, Gold, and Refresh, no schedules
   for Process/Cutover, successful optional runtime gates, and the identical
   commit/definition/output hashes.
9. **Record completion.** Link the immutable SHA, workflow run, all retained
    artifact names, approver, optional-gate results, and any operational
    follow-up in the release record. The deployment is complete after this
    step. Missing configuration or source bundles block deployment.

GitHub retains Terraform, runtime, deployment verification, and smoke artifacts
for 30 days.
Postconfiguration evidence includes the commit, non-secret deployment IDs, definition
hashes, operation IDs, and redacted runtime results. A failed or interrupted
run is resumed by rerunning the failed workflow with the same release SHA.
Terraform replans from remote state, and the reconcilers read back resources
before mutation. Never start a second deployment during a running apply or Fabric operation.
GitHub serializes queued runs. They do not
cancel the active deployment.

If Terraform apply fails, no postconfiguration runs. If postconfiguration
fails, no smoke test runs. Failed postconfiguration evidence is still uploaded.
If deployment verification fails, schedules remain disabled. If the Production
final gate fails after enabling schedules, treat the deployment as failed.
Disable affected schedules if necessary. Then correct the source or rerun the same SHA.
Fabric operations are not transactionally rolled back.

## Drift and state recovery

Treat `tfplan.json`, `protected-changes.json`, and
`definition-drift.json` from the 30-day plan artifact as the recovery record.
Never apply from a scheduled drift run. First classify each difference:

1. **Unauthorized live change:** restore Git-owned infrastructure or definition
   through the normal immutable deployment workflow.
2. **Missing state entry:** add its exact configuration to Git. Review a plan.
   Then import it into the matching environment state.
3. **Wrong Terraform address:** use a reviewed `moved` block when possible.
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

Use `terraform state rm` only when the remote object must remain unmanaged.
Remove the corresponding Terraform configuration in the same reviewed change.
Record the address, object ID, state backup location, operator,
reviewer, and clean follow-up run in the drift issue. If state restoration is
required, use `terraform state push` only as a last resort with a reviewed,
serial-compatible backup and all deployment workflows stopped.

Destructive recovery is always plan-led:

1. Stop or queue deployments.
2. Disable recurring schedules when live writes can conflict with recovery.
3. Preserve state and live evidence before you change either one.
4. Restore, import, or readdress the smallest affected object.
5. Generate a new plan from the normal environment root.
6. Obtain each required approval and environment review.
7. Run the immutable workflow twice and complete deployment verification.
8. Enable Production schedules only through the final gate.

There is no transactional Fabric rollback. A failed postconfiguration may have
completed earlier idempotent operations, and the workflow deliberately reads
live state before retrying. Prefer rerunning the same SHA for transient failure.
For a source or definition defect, create a corrective commit. Deploy it to
Development and Test. Then promote that new immutable SHA. Never apply a stale plan, copy target GUIDs
from another environment, rewrite remote state to simulate rollback, or use a
Fabric Deployment Pipeline to overwrite Terraform-owned items.
