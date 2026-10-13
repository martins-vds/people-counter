# Terraform bootstrap

This local-state stack creates:

- Azure Blob Storage for downstream Terraform state using Microsoft Entra
  authentication rather than access keys.
- One GitHub OIDC application and service principal for each GitHub Environment.
- Least-privilege state-container access for those identities.
- One Microsoft Fabric capacity per environment.

## Prerequisites

The bootstrap operator needs:

- Microsoft Entra Application Administrator or Cloud Application Administrator.
- Azure Contributor plus User Access Administrator or Role Based Access Control
  Administrator on the target subscription.
- The `Microsoft.Fabric` resource provider registered in the subscription.
- Permission under the tenant's Fabric capacity-creation policy.

Before the first environment deployment, a Fabric administrator must scope the
tenant settings that allow service principals to call Fabric public APIs and
to create workspaces, connections, and deployment pipelines to a security
group containing the three generated deployment service principals. Enable
preview tenant settings only when the corresponding reviewed Terraform inputs
are enabled. Tenant settings and group membership are organizational bootstrap
gates; this stack intentionally does not grant tenant-admin policy to itself.

## Apply

```bash
cd infra/bootstrap
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

Keep the resulting local state secure and do not commit it. This stack cannot use
the backend that it creates.

Record the outputs immediately:

```bash
terraform output -json backend_config_by_environment
terraform output -json github_oidc_client_ids
terraform output -json github_oidc_service_principal_object_ids
terraform output -json fabric_capacity_ids_by_environment
terraform output -raw tenant_id
terraform output -raw subscription_id
```

`fabric_capacity_ids_by_environment` contains Azure Resource Manager resource
IDs. The environment stack's `capacity_id` requires the Fabric API capacity
UUID, so do not pass the bootstrap output directly as `FABRIC_CAPACITY_ID`.
Resolve and verify the matching Fabric capacity once during the GitHub
Environment handoff as documented in the
[deployment runbook](../../docs/github-deployment.md#github-environments-and-variables).

Configure downstream environment stacks from
`backend_config_by_environment`, using an `azurerm` backend with
`use_azuread_auth = true`.

Create matching `dev`, `test`, and `prod` GitHub Environments. Set their
`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, and `AZURE_SUBSCRIPTION_ID` variables from
the bootstrap outputs. The generated federated subjects are exactly
`repo:<owner>/<repository>:environment:<environment>` and therefore work only
in jobs that declare the matching GitHub Environment. Workflows request
`id-token: write`; no client secret is created or required.

The full output-to-variable mapping, environment protection rules, capacity
UUID handoff, fallback-secret policy, and live completion procedure are in the
[GitHub deployment runbook](../../docs/github-deployment.md).
