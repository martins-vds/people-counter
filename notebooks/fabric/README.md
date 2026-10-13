# Stable Fabric SJD pipeline

New environments and promotions are deployed through the
[GitHub/Terraform workflow](../../docs/github-deployment.md). The commands
below remain useful for diagnosis and migration, but they are not the
authoritative production deployment path.

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

Item IDs are environment-specific Terraform outputs. Operators must resolve
them from the retained deployment evidence or
`terraform output -json spark_job_definition_ids`; never copy an ID from
another workspace into a command or source file.

| Role/output key | Responsibility |
|---|---|
| `control` | Registration, replay, recovery, and control-plane reconciliation |
| `dispatcher` | Claims bounded homogeneous batches and invokes Process |
| `process` | Performs inference and publishes immutable attempt output |
| `reconciliation` | Detects and records control/data inconsistencies |
| `gold` | Builds analytics and operational Gold projections |
| `refresh` | Refreshes both semantic models and acknowledges the durable outbox |
| `cutover` | Provides reviewed migration and retirement operations |

All mutable production state is below:

- tables: `people_counter_sjd_*`
- files: `Files/people-counter/sjd/v1/`

The runtime rejects table names and file paths outside these boundaries.

## Deployment

The supported deployment boundary is:

1. merge reviewed definitions and infrastructure to `main`;
2. let `deploy-dev.yml` deploy that immutable commit to Development;
3. dispatch `promote.yml` with the same full commit SHA for Test;
4. after Test clean-room acceptance, dispatch the same workflow and SHA for
   Production.

```bash
gh workflow run promote.yml --ref main \
  -f environment=test \
  -f release_sha=<40-character-main-commit-sha>
```

See the
[GitHub deployment runbook](../../docs/github-deployment.md#live-clean-room-completion-procedure)
for required variables, approvals, artifacts, optional reporting/RTI/Activator
gates, and the Production command. The workflow obtains workspace, Lakehouse,
Environment, report, and SJD IDs from Terraform state. Direct invocation of
`scripts.deploy_sjd_operations` is retained only for investigation of a legacy
deployment; it is not a supported create, promotion, or schedule-enablement
path.

### Diagnostic-only state inspection

After authenticating as an approved operator and initializing the matching
backend exactly as described in the runbook, inspect IDs without copying them
into source:

```bash
terraform -chdir=infra/environments/prod output -json \
  spark_job_definition_ids | jq
```

This command is read-only. Import and state-repair commands are migration
operations and are documented separately in
[`infra/modules/fabric-foundation/IMPORT.md`](../../infra/modules/fabric-foundation/IMPORT.md).

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

If the environment's Fabric capacity is intentionally paused, disable the
Control, Dispatcher, Reconciliation, Gold, and Refresh schedules for the
duration of the pause. This is an incident action, not a deployment. Record the
operator, time, and reason in the incident, and do not change Terraform state or
committed definitions. Leaving schedules enabled produces repeated
`CapacityNotActive` failures and false operational noise.

After capacity is resumed:

1. rerun `promote.yml` for Production with the currently deployed immutable
   commit SHA so the normal final gate re-enables exactly five schedules;
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
