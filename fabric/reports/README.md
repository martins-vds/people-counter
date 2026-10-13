# Power BI report definitions

Export reviewed report definitions into `analytics/` and `operational/` with
the manifest in [`fabric/README.md`](../README.md). The metadata records the
format that Fabric returns.

Replace the source model ID and name during export with the environment-backed
`TargetSemanticModelId` and `TargetSemanticModelName` tokens. Terraform injects
the corresponding model created in the same plan.

Review `.fabric-item.json` and every listed part. The Fabric foundation module
deploys each committed report.

Confirm these items in Development:

- visuals
- filters
- bookmarks
- mobile layout
- semantic model binding
- permissions
