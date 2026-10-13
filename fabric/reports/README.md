# Power BI report definitions

Export reviewed report definitions into `analytics/` and `operational/` with
the batch manifest in [`fabric/README.md`](../README.md). It requests `PBIR`
and rejects a legacy or otherwise different response format.

Replace the source model ID and name during export with the environment-backed
`TargetSemanticModelId` and `TargetSemanticModelName` tokens. Terraform injects
the corresponding model created in the same plan.

After `.fabric-item.json` and all listed parts are reviewed and committed, the
Fabric foundation module deploys the report automatically.

Keep the clone-based migration path until visuals, filters, bookmarks, mobile
layout, model binding, and permissions have been compared in nonproduction.
