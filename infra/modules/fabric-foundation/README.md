# Fabric foundation module

This is an internal module used by `infra/environments/{dev,test,prod}`. Normal
operators deploy those roots only through the protected GitHub workflows; do
not run this module directly or paste workspace/item IDs into its source.

Provisions one environment's:

- Fabric workspace, assigned capacity, and optional system identity
- workspace roles
- schema-enabled Lakehouse
- Fabric Environment
- all seven canonical Spark Job Definitions
- a source-controlled, non-secret deployment Variable Library
- optional Anonymous or WorkspaceIdentity Fabric connections and role assignments
- optional governance domain, domain roles, managed tags, and workspace assignment
- optional preview workspace folders and OneLake data-access roles
- optional three-stage Deployment Pipeline

The workspace, Lakehouse, Environment, and Deployment Pipeline use
`prevent_destroy`. Remove those gates only during an explicitly approved
replacement or teardown.

SJD definitions come from `fabric/spark-job-definitions`. The provider injects
the target Lakehouse and Environment IDs through Go-template definition tokens,
so the workspace, Lakehouse, Environment, and seven SJDs apply in one plan.
Callers provide only per-role names and descriptions.

This module does not configure schedules or publish Environment libraries; the
postconfiguration reconciler performs those API operations after apply.

The Variable Library definition is committed under
`definitions/variable-library`. It contains the deployment stage plus item
references for the workspace, Lakehouse, Environment, and all seven Spark Job
Definitions. It intentionally contains no credentials. Dev, Test, and
Production instantiate the same definition with environment-specific IDs.

`non_secret_connections` is deliberately limited to the provider's
`Anonymous` and `WorkspaceIdentity` credential types. Callers must supply the
connector's documented `connection_type`, `creation_method`, and non-secret
parameters; the module does not guess connector-specific schema. Credential
types requiring keys, passwords, client secrets, or SAS values are rejected.
Connection role assignments are supported as nested configuration.

Domain creation requires the deployment principal to be a Fabric
administrator. Workspace folder creation, domain-to-workspace assignment, and
OneLake data-access security are provider-preview resources. To use any of
them, set `enable_preview_features = true`; environment roots pass that value
to both the provider and module. Preview resources default to disabled.

Managed tags are applied to every item that supports provider tags. Existing
tag IDs may still be supplied through `tags`.

Reviewed TMDL and PBIR bundles under `fabric/semantic-models/{analytics,
operational}` and `fabric/reports/{analytics,operational}` are discovered by
their `.fabric-item.json` metadata and deployed automatically. Reports receive
the target semantic-model ID and name through provider definition tokens.

Reviewed queryset and dashboard bundles under `fabric/rti` create a portable
operational Eventhouse and read/write KQL database, then deploy both
definitions against those target resources. Exported Eventhouse or KQL
database definitions are optional because Fabric may deny definition readback
for managed monitoring resources. Activator remains disabled by default and
requires reviewed `fabric/activators/{operations,manifest-arrival}` bundles
plus `enable_activator = true`.

OneLake-to-OneLake shortcuts are managed by `fabric_shortcut.onelake` from the
`onelake_shortcuts` input. External shortcuts that need a credential-bearing
Fabric connection are intentionally not generalized: every shortcut target
has a different provider schema. Add a typed target only after its connector
definition is known. Non-secret connections themselves can be managed through
`non_secret_connections`.

Existing Production resources must be imported before the first apply. Most
workspace items use the import form `<workspace-id>/<item-id>`; consult the
provider's resource import documentation before adopting each resource. See
[IMPORT.md](IMPORT.md) for exact provider 1.14 import forms, addresses, and
adoption checks.

For bootstrap, environment variables, OIDC, live definition export, clean-room
acceptance, approvals, and recovery, use the
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

Keep `non_secret_connections` empty until the intended connector's exact
provider-supported type, creation method, and parameter names are known. Those
values are connector-specific and are intentionally not guessed here.
