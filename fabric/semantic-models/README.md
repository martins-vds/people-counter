# Semantic model definitions

Export the analytics and operations models into `analytics/` and
`operational/`. Use the manifest in [`fabric/README.md`](../README.md).
The manifest requests TMDL format.

Use manifest `tokens` or single-item `--token-env` for every
environment-specific value in the source definition.
Terraform supplies these supported tokens:

- `TargetWorkspaceId`
- `TargetLakehouseId`
- `TargetLakehouseName`

Review `.fabric-item.json` and every listed part. The Fabric foundation module
deploys each committed model.

Confirm these items in Development:

- refresh results
- measures
- relationships
- roles
- Direct Lake bindings
