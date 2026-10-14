# Source-controlled Microsoft Fabric definitions

This directory stores reviewable `getDefinition` responses. Each exported item
has a `.fabric-item.json` file. This file records:

- response format
- item type
- sorted parts
- byte lengths
- SHA-256 hashes
- token evidence
- aggregate content hash

The validator rejects modified, missing, extra, unsorted, unsafe, or symlinked
files.

## Export a live definition

Before export, authenticate Azure CLI to the tenant. Confirm that you can read
each source item. Install the Python dependencies with `uv`.

Do not put source workspace or item GUIDs in Terraform or GitHub inputs. Keep
source IDs only in the operator environment. The exporter replaces these IDs
with provider tokens.

```bash
az login --tenant "$AZURE_TENANT_ID"
export FABRIC_SOURCE_WORKSPACE_ID="<source-workspace-uuid>"
export FABRIC_SOURCE_LAKEHOUSE_ID="<source-lakehouse-uuid>"
export FABRIC_SOURCE_LAKEHOUSE_NAME="<source-lakehouse-name>"
export FABRIC_SOURCE_ANALYTICS_MODEL_ID="<source-model-uuid>"
export FABRIC_SOURCE_ANALYTICS_MODEL_NAME="<source-model-name>"
export FABRIC_SOURCE_OPERATIONAL_MODEL_ID="<source-model-uuid>"
export FABRIC_SOURCE_OPERATIONAL_MODEL_NAME="<source-model-name>"
export FABRIC_SOURCE_ANALYTICS_REPORT_ID="<source-report-uuid>"
export FABRIC_SOURCE_OPERATIONAL_REPORT_ID="<source-report-uuid>"
export FABRIC_SOURCE_EVENTHOUSE_ID="<source-eventhouse-uuid>"
export FABRIC_SOURCE_KQL_DATABASE_ID="<source-kql-database-uuid>"
export FABRIC_SOURCE_KQL_QUERYSET_ID="<source-queryset-uuid>"
export FABRIC_SOURCE_KQL_DASHBOARD_ID="<source-dashboard-uuid>"
export FABRIC_SOURCE_KQL_QUERY_SERVICE_URI="<source-query-service-uri>"
export FABRIC_SOURCE_SJD_DISPATCHER_ID="<source-dispatcher-sjd-uuid>"
export FABRIC_SOURCE_ACTIVATOR_ID="<source-activator-uuid>"
export FABRIC_SOURCE_VARIABLE_LIBRARY_ID="<source-variable-library-uuid>"

uv run python scripts/export_fabric_definition.py \
  --manifest fabric/definition-export-manifest.example.json \
  --evidence fabric-definition-export-evidence.json
uv run python scripts/validate_fabric_definitions.py fabric \
  --require-bundles \
  --output fabric-definition-validation-evidence.json
```

The evidence files contain live item and operation IDs. Do not commit these
files. The manifest stores only environment variable names.

Edit the item and token lists before export. Each token must occur at least
once. Source values must not overlap. The exporter rejects token matches in
non-UTF-8 parts. Use `parts` to limit a token to exact definition paths.

For a single item:

```bash
export SOURCE_ITEM_VALUE="<exact-environment-specific-value>"
uv run python scripts/export_fabric_definition.py \
  --workspace-id "$FABRIC_SOURCE_WORKSPACE_ID" \
  --item-id "$FABRIC_SOURCE_ANALYTICS_MODEL_ID" \
  --item-type SemanticModel \
  --format TMDL \
  --destination fabric/semantic-models/analytics \
  --token-env TargetWorkspaceId=FABRIC_SOURCE_WORKSPACE_ID \
  --token-env TargetLakehouseId=FABRIC_SOURCE_LAKEHOUSE_ID \
  --evidence fabric-definition-export-evidence.json
```

Use the report format that the Fabric item supports. Omit `--format` when the
item exposes only its default format.

`--token-env` reads the source value from an environment variable. It does not
put the value in command arguments. Export fails when Fabric returns a
different requested format.

The batch interface accepts any item with a public `getDefinition` operation.
Set `itemIdEnv`, `itemType`, and `destination`. Add `format` and `tokens` when
required.

The exporter reports Fabric API errors. It does not create placeholder files.

Scheduled drift verification uses `definition-drift-manifest.json`. Terraform resolves
target item IDs and token values during the workflow.

The checker tokenizes each live definition in memory. It compares the content
hash with `.fabric-item.json`. Evidence contains only paths, item types,
statuses, and SHA-256 hashes.

The checker does not store definition parts or live IDs. An optional entry
becomes active when its source bundle exists.

After you commit reviewed bundles, use `deploy-dev.yml` and `promote.yml`.
Do not use the exporter to deploy a target environment.

Committed bundles control reporting and RTI deployment. Activator deployment
also requires `FABRIC_ENABLE_ACTIVATOR=true`. See the
[GitHub deployment runbook](../docs/github-deployment.md).

## Review and commit

1. Inspect every exported text part.
2. Confirm that no source endpoint, ID, credential, email, or action target
   remains.
3. Confirm that `.platform` logical IDs remain unchanged.
4. Run the validator and compare `treeSha256`, per-bundle `contentSha256`, and
   `bundleSha256` in its evidence.
5. Commit only the definition directories and `.fabric-item.json` metadata.
6. Deploy to Development and confirm item behavior.

Re-export refuses to overwrite a modified or unlisted file. Review, revert, or
commit local changes before replacing a bundle.

## Runtime postconfiguration

`runtime/dev.json`, `runtime/test.json`, and `runtime/prod.json` define runtime
configuration after Terraform apply. They contain no secret values.

These files define Lakehouse checks and optional service configuration. Keep
secrets in protected environment variables. Reference only their names through
`valuesFromEnvironment`.

The runtime CLI uses documented Fabric, OneLake DFS, and Power BI operations.
Some operations require an operator checkpoint. Examples include OAuth2
consent and on-premises gateway credential encryption.

An unresolved checkpoint returns status 2. Recurring SJD schedules remain
disabled.
