# Fabric SJD operations

The solution uses seven Spark Job Definitions (SJDs). GitHub Actions and
Terraform deploy each environment.

Use the [GitHub deployment runbook](../../docs/github-deployment.md) for setup,
deployment, promotion, and recovery.

## Demonstration files

- `18_sjd_getting_started_demo.ipynb` registers three sample videos through
  the Control SJD.
- `18_sjd_getting_started_demo_tutorial.md` contains the demonstration steps
  and expected results.

The notebook is a demonstration client. Manifest arrival starts production
registration and calls the Control SJD `register` command.

## Production roles

Terraform creates environment-specific item IDs. Read these IDs from workflow
evidence or Terraform output. Do not copy an ID between environments.

| Role/output key | Responsibility |
|---|---|
| `control` | Registers work and coordinates replay and recovery |
| `dispatcher` | Claims bounded homogeneous batches and invokes Process |
| `process` | Performs inference and publishes immutable attempt output |
| `reconciliation` | Detects and records control/data inconsistencies |
| `gold` | Builds analytics and operational Gold projections |
| `refresh` | Refreshes both semantic models and acknowledges the durable outbox |
| `cutover` | Applies approved schema changes and verifies release boundaries |

All mutable production state is below:

- tables: `people_counter_sjd_*`
- files: `Files/people-counter/sjd/v1/`

The runtime rejects other table names and file paths.

## Deployment

Use this deployment sequence:

1. Merge reviewed definitions and infrastructure to `main`.
2. Let `deploy-dev.yml` deploy the commit to Development.
3. Run `promote.yml` with the same full commit SHA for Test.
4. Verify the Test evidence.
5. Run `promote.yml` with the same commit SHA for Production.

```bash
gh workflow run promote.yml --ref main \
  -f environment=test \
  -f release_sha=<40-character-main-commit-sha>
```

See the
[deployment procedure](../../docs/github-deployment.md#deployment-procedure)
for variables, approvals, artifacts, feature gates, and Production commands.
The workflow reads all Fabric item IDs from Terraform state.

### Inspect Terraform output

Authenticate as an approved operator. Initialize the backend from the
runbook. Then inspect the SJD IDs:

```bash
terraform -chdir=infra/environments/prod output -json \
  spark_job_definition_ids | jq
```

This command does not change Fabric or Terraform state.

## Reporting

The solution contains these reporting surfaces:

- `pc_sjd_analytics_report`: SJD Gold analytics and capacity forecast.
- `pc_sjd_operations_report`: work, attempts, hourly operations, and
  reconciliation from stable SJD tables.
- `pc_operations_dashboard`: KQL views of job and runtime activity.

## Capacity pause behavior

Disable all five recurring schedules before you pause a Fabric capacity.
Record the operator, time, and reason in the incident record. Do not change
Terraform state or committed definitions.

After you resume capacity:

1. Run `promote.yml` for Production with the deployed commit SHA.
2. Check that the final gate enables five schedules.
3. Require one successful run of every role.
4. Confirm that no reconciliation errors are open.
5. Confirm that the semantic-refresh outbox is empty.
6. Query both SJD reports.

## Reliability contract

- Registration is idempotent and creates only stable `READY` work.
- Fences and leases protect all claims.
- Attempt output is immutable and becomes visible only through committed
  pointers.
- Gold reads only committed output and checks exact table contracts.
- Semantic refresh acknowledges its outbox only after both models complete.
- Reconciliation findings remain visible until explicitly resolved.
