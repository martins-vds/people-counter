# Fabric foundation module

This internal module supports `infra/environments/{dev,test,prod}`. Use the
protected GitHub workflows to deploy these roots. Do not run this module
directly. Do not put workspace or item IDs in its source.

Provisions one environment's:

- Fabric workspace, assigned capacity, and optional system identity
- workspace roles
- schema-enabled Lakehouse
- one ADLS Gen2 shortcut
- Fabric Environment
- all seven canonical Spark Job Definitions
- a source-controlled, non-secret deployment Variable Library
- optional Anonymous or WorkspaceIdentity Fabric connections and role assignments
- optional governance domain, domain roles, managed tags, and workspace assignment
- optional preview workspace folders and OneLake data-access roles
- optional three-stage Deployment Pipeline

The workspace, Lakehouse, Environment, and Deployment Pipeline use
`prevent_destroy`. Remove those gates only during an approved
replacement or teardown.

SJD definitions come from `fabric/spark-job-definitions`. The provider injects
the target Lakehouse and Environment IDs through Go-template definition tokens,
so the workspace, Lakehouse, Environment, and seven SJDs apply in one plan.
Callers provide only per-role names and descriptions.

This module does not configure schedules or publish Environment libraries.
The postconfiguration reconciler performs those API operations after apply.

Git stores the Variable Library definition under `definitions/variable-library`.
The definition contains the deployment stage plus item
references for the workspace, Lakehouse, Environment, and all seven Spark Job
Definitions. It intentionally contains no credentials. Dev, Test, and
Production instantiate the same definition with environment-specific IDs.

`non_secret_connections` is deliberately limited to the provider's
`Anonymous` and `WorkspaceIdentity` credential types. Callers must supply the
connector's documented `connection_type`, `creation_method`, and non-secret
parameters. The module does not guess connector-specific schema. The module
rejects credential types that require keys, passwords, client secrets, or SAS
values. Nested configuration supports connection role assignments.

Domain creation requires the deployment principal to be a Fabric
administrator. Workspace folder creation, domain-to-workspace assignment, and
OneLake data-access security are provider-preview resources. To use any of
them, set `enable_preview_features = true`. Environment roots pass that value
to both the provider and module. Preview resources default to disabled.

The module applies managed tags to each item that supports provider tags. You
can also supply tag IDs through `tags`.

The module finds TMDL and PBIR bundles under
`fabric/semantic-models/{analytics,operational}` and
`fabric/reports/{analytics,operational}`. It uses `.fabric-item.json` metadata.
Reports receive the target semantic-model ID and name through definition
tokens.

Reviewed queryset and dashboard bundles under `fabric/rti` create a portable
operational Eventhouse and read/write KQL database, then deploy both
definitions against those target resources. Exported Eventhouse or KQL
database definitions are optional because Fabric may deny definition readback
for managed monitoring resources. Activator remains disabled by default and
requires reviewed `fabric/activators/{operations,manifest-arrival}` bundles
plus `enable_activator = true`.

The scalar `adls_shortcut_*` inputs manage one ADLS Gen2 shortcut. Bootstrap
owns the storage account and container. This module owns the workspace
identity, role assignment, WorkspaceIdentity connection, and shortcut.

The storage account must use hierarchical namespace.

Other external shortcut target types are not supported. Use
`non_secret_connections` only for reviewed non-secret connections.

For bootstrap, environment variables, OIDC, definition export, deployment
verification, approvals, and recovery, use the
[deployment runbook](../../../docs/github-deployment.md).

## Optional configuration example

```hcl
enable_preview_features = true

workspace_folders = {
  governance = { display_name = "Governance" }
}

domain = {
  display_name     = "People Counter"
  assign_workspace = true
}

governance_tags = {
  managed = {
    display_name = "Git managed"
    scope        = "Domain"
  }
}
```

Keep `non_secret_connections` empty until you know the exact provider schema.
The schema must include the connection type, creation method, and parameter
names.
