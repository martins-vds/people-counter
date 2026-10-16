# Local deployment runbook

An operator can run the same deployment sequence from a trusted Linux
workstation with [deploy-local.sh](../scripts/deploy-local.sh).
The script uses the operator's Azure CLI user session instead of GitHub OIDC.
It does not use, fetch, or modify GitHub Environment variables or secrets.

For automated deployment through GitHub Actions, use the
[GitHub deployment runbook](github-deployment.md).

## Prerequisites and configuration

Complete the shared
[organizational bootstrap and tenant settings](github-deployment.md#organizational-bootstrap-and-tenant-settings)
before deploying. Review the
[configuration variables](github-deployment.md#github-environments-and-variables)
and [deployment verification gates](github-deployment.md#deployment-verification-gates)
for the selected environment.

Install Bash, Git, Azure CLI, Terraform `>= 1.9.0, < 2.0.0`, `uv`, `jq`, and
`curl`. The local bootstrap script creates the environment file automatically. When
using an existing external bootstrap instead, copy the non-secret template:

```bash
cp scripts/deploy-dev-local.env.example .env.deploy-dev.local
```

Fill `.env.deploy-dev.local` with the values from the GitHub `dev`
Environment. Keep the file local; Git ignores it. It is a trusted Bash
environment file, so it must contain assignments only. The optional
`FABRIC_ACTIVATOR_ACTION_CHECKPOINT_JSON` value may remain empty while
`FABRIC_REQUIRE_ACTIVATOR_ACTION_CHECKPOINT=false`.

Authenticate interactively and select the configured subscription:

```bash
az login --tenant "<tenant-guid>"
az account set --subscription "<subscription-guid>"
```

## Deployment procedure

Before running the script, confirm that no GitHub or operator deployment is
active. Run Development from the repository root on a clean `main` branch that
exactly matches `origin/main`:

```bash
scripts/deploy-local.sh
```

For Test or Production, bootstrap and deploy the selected environment:

```bash
scripts/bootstrap-local.sh --environment test
scripts/deploy-local.sh --environment test
```

`scripts/deploy-dev-local.sh` remains a compatibility wrapper for Development.
When RTI is enabled, local deployment also creates or configures Workspace
Monitoring after the workspace exists. The tenant-level **Allow monitoring
data collection** setting remains an external administrative prerequisite.

The deployment script fetches `origin/main`, records the full immutable commit
SHA, validates the Azure tenant, subscription, and Fabric token audiences, and
then:

1. initializes and validates the selected environment's Terraform root;
2. creates and classifies a saved Terraform plan;
3. applies only that saved plan;
4. builds and validates the committed Fabric definitions;
5. reconciles the Fabric runtime twice with schedules disabled;
6. requires a zero-drift Terraform plan;
7. runs clean-room acceptance and direct Fabric REST probes.

## Approvals and evidence

Approval-gated changes stop before apply unless the matching reviewed flag is
present:

```bash
scripts/deploy-local.sh --environment test \
  --allow-protected-changes \
  --approve-shortcut-changes \
  --approve-workspace-identity-changes \
  --approve-capacity-changes
```

Pass only the flags required by the printed plan. The script stores
access-restricted, ignored evidence under
`evidence/local-deploy-<environment>/`. It
retains Terraform logs and the human-readable plan, protected-change
classification, definition validation, both reconciliation records, and smoke
evidence. Raw Terraform output, saved plans, access tokens, and temporary
Activator checkpoint files are removed when the process exits.

Local execution does not add distributed concurrency beyond the Terraform
state lock. It is the operator's responsibility to prevent overlap with
GitHub Actions or another workstation. Fabric operations are not
transactionally rolled back. A failed postconfiguration or acceptance gate
leaves the selected environment's schedules disabled.
