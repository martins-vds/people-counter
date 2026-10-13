# Activator definitions

Export the reviewed Activator `ReflexEntities.json` into `operations/` with
the environment-backed batch manifest in
[`fabric/README.md`](../README.md). No Activator definition is generated
without a successful live `getDefinition` response.

Activator is preview-isolated and remains disabled unless
`enable_activator=true`. Enabling it without a committed definition fails the
Terraform configuration instead of reporting a false deployment success.

Supported definition tokens are:

- `TargetWorkspaceId`
- `TargetEventhouseId`
- `TargetKqlDatabaseId`
- `TargetKqlQuerysetId`
- `TargetKqlDashboardId`

Definition deployment does not prove that every configured action can execute.
The post-deployment Activator contract test remains a required production gate.
Review action recipients, connection references, and destinations for secrets
or source-environment values before commit.

## Validate the public definition contract

After exporting `operations/`, run:

```bash
uv run python scripts/validate_activator_definition.py \
  fabric/activators/operations \
  --output activator-definition-evidence.json
```

Validation requires the documented `json` format, `ReflexEntities.json`, an
optional valid `.platform`, supported entity/source types, valid rule template
JSON, and the documented `TeamsMessage`, `EmailMessage`, or
`FabricItemInvocation` action kinds. It rejects unknown entity/action kinds and
the documented lifecycle-management exclusions (Power BI or Azure Blob event
sources and User Data Function actions) instead of silently deploying them.

## Probe service-principal CRUD

Authenticate `az` as the deployment service principal (or use its workload
identity login), then run:

```bash
uv run python scripts/probe_activator_sp_contract.py \
  --workspace-id "$FABRIC_TARGET_WORKSPACE_ID" \
  > activator-sp-crud-evidence.json
```

The probe uses only the documented Reflex v1 create, get, update, list, and
delete APIs. It creates a uniquely named disposable Reflex and hard-deletes it
even when an intermediate check fails. Run this probe in every target tenant
because successful definition validation does not prove that tenant settings,
workspace roles, and service-principal access permit CRUD.

## Production action-execution gate

Enablement is explicit:

```bash
uv run python scripts/reconcile_fabric_runtime.py \
  --manifest-path src/people_counter/data/deploy/prod.json \
  --terraform-output-path terraform-output.json \
  --stage prod \
  --whl-path dist/people_counter-0.9.57-py3-none-any.whl \
  --enable-activator \
  --activator-bundle-path fabric/activators/operations \
  --activator-checkpoint-path activator-action-checkpoint.json
```

The gate reads the live Reflex and its public definition, verifies the manifest
`reflex_id` and `trigger_name`, requires that rule's `shouldRun` setting, checks
the source-controlled/live action topology, and verifies referenced Fabric
item targets. These checks prove configuration, not execution.

Fabric publishes no generic API that can force an arbitrary Activator rule and
prove its downstream side effect. Without a current operator checkpoint, the
runtime verifier returns `operator_checkpoint_required`; the reconciler fails
and never reports enablement as successful. The failure evidence prints the
exact checkpoint contract. It binds:

- workspace, Reflex, trigger, and the live `ReflexEntities.json` SHA-256;
- every live action ID and kind;
- `verifiedAt`, `expiresAt`, and `verifiedBy`;
- a successful result and durable evidence reference for every action.

Use a short expiry and link immutable evidence from a controlled test event,
the Activator run/monitoring view, and the observable downstream result. A
definition or action change invalidates the checkpoint automatically.
