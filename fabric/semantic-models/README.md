# Semantic model definitions

Export the reviewed analytics and operational models into `analytics/` and
`operational/` with the environment-backed batch manifest in
[`fabric/README.md`](../README.md). The manifest explicitly requests `TMDL`;
the export fails rather than accepting a different response format.

Use manifest `tokens` or single-item `--token-env` for every
environment-specific value in the source definition.
Terraform supplies these supported tokens:

- `TargetWorkspaceId`
- `TargetLakehouseId`
- `TargetLakehouseName`

After `.fabric-item.json` and all listed parts are reviewed and committed, the
Fabric foundation module deploys the model automatically.

Do not export a model as TMDL unless its live item supports that format. Keep
the existing clone migration path until refreshes, measures, relationships,
roles, and Direct Lake bindings have been compared in nonproduction.
