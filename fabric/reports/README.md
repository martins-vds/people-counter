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

## Legacy benchmark report

The live `pc_benchmark_report` in `people-counter-dev-legacy` uses
`pc_sjd_analytics_model`, not the retired `pc_benchmark_model`. It contains
three pages based on the current analytics report:

- **Processing Performance:** processed videos, video hours, processing speed,
  per-video details, and camera, location, pipeline, and detector filters.
- **Throughput:** hourly completed video hours and processing outcomes, with
  date filtering. KPI titles distinguish capture dates from completion dates.
- **Forecast:** the existing workload selector and capacity-planning calculation,
  using the analytics model's calculated `Forecast Workload` and
  `Forecast Capacity` tables. It retains the fixed 1.5899x F64 pilot baseline,
  linear SKU scaling, a 30-day target, 20% headroom, and 80% utilization.
  This is a directional estimate, not a live benchmark or capacity guarantee.

The old run comparisons, stage timings, executor comparisons, and approach
comparisons were removed because their source tables were retired. Do not
restore those tables or bind the report back to the retired benchmark model.
Performance and throughput use the current `people_counter_sjd_gold_*` tables
through the analytics model. The forecast does not query retired benchmark
tables.

This legacy report is maintained live; it is not an additional Terraform report
bundle. Development and Test continue to deploy the analytics and operational
reports defined above.
