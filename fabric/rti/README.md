# Real-Time Intelligence definitions

Use the environment-backed batch manifest in
[`fabric/README.md`](../README.md) to export reviewed live definitions into:

- `eventhouse/`
- `kql-database/`
- `queryset/`
- `dashboard/`

The Terraform module creates `pc_operations_eventhouse`, its read/write KQL
database, `pc_operations_queryset`, and `pc_operations_dashboard` as each
definition bundle becomes available.

Definitions may use these Terraform-injected tokens:

- `TargetWorkspaceId`
- `TargetEventhouseId`
- `TargetKqlDatabaseId`
- `TargetKqlQueryServiceUri`

The dashboard can be committed independently when it targets an external
workspace-monitoring source; database-specific tokens are supplied only when
the repository also contains the KQL database definition.

The example manifest omits `format` for these items so the exporter records the
format returned by the live API. Remove any token that genuinely does not occur
in that item only after reviewing the first live definition; missing required
tokens intentionally fail the export.
