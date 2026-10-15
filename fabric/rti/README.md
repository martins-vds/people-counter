# Real-Time Intelligence definitions

Use the environment-backed batch manifest in
[`fabric/README.md`](../README.md) to export reviewed live definitions into:

- `eventhouse/`
- `kql-database/`
- `queryset/`
- `dashboard/`

The Terraform module retains `pc_operations_eventhouse` and its read/write KQL
database, and creates `pc_operations_queryset` and `pc_operations_dashboard`.
The queryset, dashboard, and operations Activator use the managed Workspace
Monitoring database named:

`<workspace>_Monitoring_item_<workspace>_Database`

Enable Workspace Monitoring before deploying RTI definitions. Do not rename its
managed database. Terraform discovers the database and injects its item ID,
parent Eventhouse ID, and current query endpoint.

Definitions may use these Terraform-injected tokens:

- `TargetWorkspaceId`
- `TargetEventhouseId`
- `TargetKqlDatabaseId`
- `TargetKqlQueryServiceUri`

The dashboard and queryset target Workspace Monitoring. Their definitions must
use the database ID and query-service tokens instead of a captured authoring
endpoint.

The example manifest omits `format` for these items so the exporter records the
format returned by the live API. Remove any token that genuinely does not occur
in that item only after reviewing the first live definition; missing required
tokens intentionally fail the export.
