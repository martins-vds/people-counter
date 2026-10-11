# Stable Fabric SJD pipeline

The production implementation uses seven Spark Job Definitions (SJDs) and the
fixed `people_counter_sjd_*` control contract. Legacy notebook processing,
Data Pipeline exports, canary definitions, production-shadow code, and
migration controllers have been retired from this repository.

## Active files

- `18_sjd_getting_started_demo.ipynb`: operator demo that registers the three
  sample videos through the stable Control SJD.
- `18_sjd_getting_started_demo_tutorial.md`: presenter instructions and
  expected results for the demo.

The notebook is a demonstration client, not a production processing boundary.
Production registration is triggered by manifest arrival and invokes the
stable Control SJD `register` command.

## Production roles

| Role | Stable item ID | Responsibility |
|---|---|---|
| Control | `6df10e00-517c-409a-8dd6-40ae9bc62003` | Registration, replay, recovery, and control-plane reconciliation |
| Dispatcher | `d2d65594-cc36-4b2c-ad3d-6c520113589c` | Claims bounded homogeneous batches and invokes Process |
| Process | `c6397f52-176d-40eb-ab8e-c37844fd90d6` | Performs inference and publishes immutable attempt output |
| Reconciliation | `c94fc6fb-7397-4d11-9bd8-c4d3e778b501` | Detects and records control/data inconsistencies |
| Gold | `621391c9-ba0c-4965-8f1e-52fbb46b59eb` | Builds analytics and operational Gold projections |
| Refresh | `0257b389-54d8-4281-92ec-125240fe1d32` | Refreshes both semantic models and acknowledges the durable outbox |
| Cutover | `bef2a981-69b8-4202-9a9c-c0a08d902615` | Provides reviewed migration and retirement operations |

All mutable production state is below:

- tables: `people_counter_sjd_*`
- files: `Files/people-counter/sjd/v1/`

The runtime rejects table names and file paths outside these boundaries.

## Deployment

Deploy or reconcile the stable definitions and schedules with:

```bash
uv run python -m scripts.deploy_sjd_operations \
  --workspace-id c31ee864-230d-4005-8fd5-7c7130ebf774 \
  --release-manifest-sha256 <MANIFEST_SHA256> \
  --release-receipt-sha256 <RECEIPT_SHA256> \
  --semantic-model-id 5dbaf3d1-9f86-4bcd-a590-f32c7fbea08c \
  --additional-semantic-model-id 098c8182-4159-4e26-8c5f-8e1fbb400bd0 \
  --enable-schedules
```

Omit `--enable-schedules` to reconcile the same definitions with all five
recurring schedules disabled.

## Reporting

The supported report surfaces are:

- `pc_sjd_analytics_report`: SJD Gold analytics and capacity forecast.
- `pc_sjd_operations_report`: work, attempts, hourly operations, and
  reconciliation from stable SJD tables.
- `pc_operations_dashboard`: workspace-monitoring KQL for job/runtime
  observability. This dashboard remains supported because it complements the
  Delta-backed reports.

The old `pc_operations_report` and `pc_operations_model` are rollback-only.
Remove their user/app links and permissions immediately, label them
deprecated, and delete them after the approved rollback window. They must not
be used for normal operations because they bind to retired legacy tables.

## Capacity pause behavior

If Fabric capacity
`27fe2dc5-cdfd-4a28-8bba-75413296d02d` is intentionally paused, disable the
Control, Dispatcher, Reconciliation, Gold, and Refresh schedules for the
duration of the pause. Leaving schedules enabled produces repeated
`CapacityNotActive` failures and false operational noise.

After capacity is resumed:

1. enable the five schedules;
2. require one successful run of every role;
3. confirm no open reconciliation errors;
4. confirm the semantic-refresh outbox is empty;
5. query both SJD reports.

## Reliability contract

- Registration is idempotent and creates only stable `READY` work.
- Claims are fenced and lease bounded.
- Attempt output is immutable and becomes visible only through committed
  pointers.
- Gold reads only committed output and validates exact table contracts.
- Semantic refresh acknowledges its outbox only after both models complete.
- Reconciliation findings remain visible until explicitly resolved.

## Repository retirement boundary

Historical notebook processors, pipeline export JSON, experimental definitions,
canary and production-shadow tooling, and one-time migration controllers were
removed after the stable SJD cutover. Immutable release evidence and archived
OneLake retirement bundles remain the rollback/audit source of truth; they do
not require executable legacy code in the active repository.
