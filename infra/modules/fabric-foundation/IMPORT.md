# Importing existing Fabric resources

> **Migration/recovery only.** Imports adopt existing objects into the matching
> protected state. They are not the normal deployment path and must never be
> used to copy IDs between environments. New deployments use the GitHub
> workflow and receive IDs from Terraform outputs automatically.

Import existing resources before the first plan that enables their matching
configuration. These forms are from the Microsoft Fabric provider 1.14
resource documentation. Run imports from the target environment root so the
remote state key and provider identity are correct.

Never import into the wrong environment and never use `terraform state mv`
between Dev, Test, and Production without a separately reviewed recovery plan.
Back up state, initialize the intended backend, and confirm the signed-in
tenant before starting.

## Recommended adoption sequence

1. Configure names and stable `for_each` keys to match the existing tenant.
2. Run `terraform init` in `infra/environments/<stage>`.
3. Import the workspace, then its role assignments.
4. Import the Lakehouse, Environment, SJDs, Variable Library, analytics, RTI,
   shortcuts, folders, and optional Activator.
5. Import tenant-scoped domains/connections before their assignments.
6. Run `terraform plan -refresh-only`; investigate every mismatch.
7. Run a normal plan. Do not apply if it proposes replacement or deletion of
   an adopted resource.

Use shell variables for real IDs. The examples below show Production addresses;
replace keys with the exact configured map keys.

## Workspace and core items

```bash
terraform -chdir=infra/environments/prod import \
  'module.fabric_foundation.fabric_workspace.this' \
  "$WORKSPACE_ID"

terraform -chdir=infra/environments/prod import \
  'module.fabric_foundation.fabric_workspace_role_assignment.this["operators"]' \
  "$WORKSPACE_ID/$PRINCIPAL_ID"

terraform -chdir=infra/environments/prod import \
  'module.fabric_foundation.fabric_lakehouse.this' \
  "$WORKSPACE_ID/$LAKEHOUSE_ID"

terraform -chdir=infra/environments/prod import \
  'module.fabric_foundation.fabric_environment.this' \
  "$WORKSPACE_ID/$ENVIRONMENT_ID"

terraform -chdir=infra/environments/prod import \
  'module.fabric_foundation.fabric_spark_job_definition.this["control"]' \
  "$WORKSPACE_ID/$CONTROL_SJD_ID"

terraform -chdir=infra/environments/prod import \
  'module.fabric_foundation.fabric_variable_library.deployment[0]' \
  "$WORKSPACE_ID/$VARIABLE_LIBRARY_ID"
```

Repeat the SJD import for `dispatcher`, `process`, `refresh`,
`reconciliation`, `gold`, and `cutover`.

## Analytics and RTI items

All provider-managed workspace items below use
`<WorkspaceID>/<ItemID>`:

```text
fabric_semantic_model.this["analytics"]
fabric_semantic_model.this["operational"]
fabric_report.this["analytics"]
fabric_report.this["operational"]
fabric_eventhouse.operations[0]
fabric_kql_database.operations["operations"]
fabric_kql_queryset.operations["operations"]
fabric_kql_dashboard.operations["operations"]
fabric_activator.this["operations"]
```

For example:

```bash
terraform -chdir=infra/environments/prod import \
  'module.fabric_foundation.fabric_semantic_model.this["analytics"]' \
  "$WORKSPACE_ID/$SEMANTIC_MODEL_ID"
```

Only import conditional items whose source definition exists and whose feature
flag is enabled. Importing an address absent from configuration will fail.

## Shortcuts and folders

The shortcut import ID includes the item-relative shortcut path and name:

```bash
terraform -chdir=infra/environments/prod import \
  'module.fabric_foundation.fabric_shortcut.onelake["curated"]' \
  "$WORKSPACE_ID/$LAKEHOUSE_ID/Files/curated/source"
```

The exact provider form is
`<WorkspaceID>/<ItemID>/<ShortcutPath>/<ShortcutName>`. If `path` contains
multiple segments, include every segment before the final shortcut name.

Folders use `<WorkspaceID>/<FolderID>`:

```bash
terraform -chdir=infra/environments/prod import \
  'module.fabric_foundation.fabric_folder.this["governance"]' \
  "$WORKSPACE_ID/$FOLDER_ID"
```

## Connections and governance

Connections and domains are tenant-scoped and use their bare IDs:

```bash
terraform -chdir=infra/environments/prod import \
  'module.fabric_foundation.fabric_connection.this["public_http"]' \
  "$CONNECTION_ID"

terraform -chdir=infra/environments/prod import \
  'module.fabric_foundation.fabric_connection_role_assignment.this["public_http/operators"]' \
  "$CONNECTION_ID/$PRINCIPAL_ID"

terraform -chdir=infra/environments/prod import \
  'module.fabric_foundation.fabric_domain.this[0]' \
  "$DOMAIN_ID"
```

OneLake data-access roles use
`<WorkspaceID>/<ItemID>/<RoleName>`:

```bash
terraform -chdir=infra/environments/prod import \
  'module.fabric_foundation.fabric_onelake_data_access_security.this["readers"]' \
  "$WORKSPACE_ID/$LAKEHOUSE_ID/$ROLE_NAME"
```

Provider 1.14 documentation exposes no import operation for
`fabric_domain_role_assignments`, `fabric_domain_workspace_assignments`, or
`fabric_tag`. Do not claim those existing objects have been adopted. Leave the
matching inputs disabled, or explicitly reconcile/remove the existing tenant
configuration before Terraform creates it.

## Deployment Pipeline

Deployment Pipelines use the bare pipeline ID:

```bash
terraform -chdir=infra/environments/dev import \
  'module.fabric_foundation.fabric_deployment_pipeline.this[0]' \
  "$DEPLOYMENT_PIPELINE_ID"

terraform -chdir=infra/environments/dev import \
  'module.fabric_foundation.fabric_deployment_pipeline_role_assignment.this["operators"]' \
  "$DEPLOYMENT_PIPELINE_ID/$PRINCIPAL_ID"
```

The pipeline is provisioned for comparison/history only. It must not deploy or
mutate resources already managed by this Terraform state.

## Post-import checks

Confirm that:

- `terraform state show` reports the expected workspace and item IDs.
- item formats and names match source; changing a ForceNew field may replace an
  imported resource.
- Variable Library definitions contain no secrets and match the committed
  `definitions/variable-library` files.
- connection credential types are only `Anonymous` or `WorkspaceIdentity`.
- preview mode is enabled before adopting folders, domain workspace
  assignments, or OneLake data-access roles.
- the final normal plan contains no destroy or replacement action.
