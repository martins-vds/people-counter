# Source-controlled Microsoft Fabric definitions

This tree stores exact `getDefinition` responses as reviewable files. No live
workspace definition is synthesized in this repository. Each exported item has
a `.fabric-item.json` file that records the response format, item type, sorted
parts, byte lengths, SHA-256 hashes, token evidence, and an aggregate content
hash. The validator rejects modified, missing, extra, unsorted, unsafe, or
symlinked files.

## Export a live definition (migration only)

Prerequisites are Azure CLI authentication to the tenant, permission to read
each source item definition, and Python dependencies installed by `uv`.
Export is the only supported way to seed definition bundles from a live
workspace. Do not copy source workspace/item GUIDs into Terraform, GitHub
workflow inputs, or definition files. The source IDs below exist only in the
operator's environment for the duration of the export and are replaced with
provider tokens before commit.

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
export FABRIC_SOURCE_ACTIVATOR_ID="<source-activator-uuid>"
export FABRIC_SOURCE_VARIABLE_LIBRARY_ID="<source-variable-library-uuid>"

uv run python scripts/export_fabric_definition.py \
  --manifest fabric/migration-manifest.example.json \
  --evidence fabric-definition-export-evidence.json
uv run python scripts/validate_fabric_definitions.py fabric \
  --require-bundles \
  --output fabric-definition-validation-evidence.json
```

The two evidence files contain live item IDs and operation IDs and are review
artifacts, not source definitions; do not commit them. The manifest stores only
environment-variable names. Edit its item list and token list to match the
actual source workspace before export. A token is required to occur at least
once, source values may not overlap, substitution is simultaneous, and a match
inside a non-UTF-8 part is rejected. This prevents silent binary corruption,
partial sanitization, and replacement cascades. `parts` may be added to a token
entry to restrict replacement to exact definition part paths.

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

Use `--format PBIR` for new report definitions. Omit `--format` when that item
type exposes only its default definition format. `--token-env` reads the source
literal from the named environment variable, so it does not expose that value
in command arguments. Export fails if Fabric returns a different requested
format.

The batch interface is item-type agnostic: append any workspace item that
supports the public `getDefinition` operation with `itemIdEnv`, `itemType`,
`destination`, optional `format`, and optional `tokens`. Unsupported or
permission-gated item types fail with the original Fabric API error rather than
creating placeholder content.

Scheduled drift checks use `definition-drift-manifest.json`. Terraform outputs
resolve target item IDs and token values at runtime; no live IDs are committed.
The checker decodes and tokenizes each live definition in memory, compares its
aggregate content hash with the validated `.fabric-item.json`, and writes only
paths, item types, statuses, and SHA-256 hashes. It never writes or uploads live
definition parts, operation IDs, item IDs, workspace IDs, or token source
values. Entries marked `optional` become active automatically when their source
bundle is committed.

After committing reviewed bundles, deployment is performed only by
`deploy-dev.yml` and `promote.yml`; do not run the exporter against a target as
a substitute for promotion. Reporting creation is bundle-driven, RTI creation
is bundle-driven, and Activator additionally requires
`FABRIC_ENABLE_ACTIVATOR=true`. The protected environment flags and acceptance
gates are documented in the
[GitHub deployment runbook](../docs/github-deployment.md).

## Review and commit

1. Inspect every exported text part and confirm no source-only endpoint, ID,
   credential, email address, or action target remains.
2. Confirm `.platform` logical IDs remain unchanged; never replace logical IDs
   merely because they resemble environment IDs.
3. Run the validator and compare `treeSha256`, per-bundle `contentSha256`, and
   `bundleSha256` in its evidence.
4. Commit only the definition directories and `.fabric-item.json` metadata.
5. Keep clone-based migration scripts until TMDL/PBIR and runtime behavior have
   been compared in a nonproduction workspace.

Re-export refuses to overwrite a modified or unlisted file. Review, revert, or
commit local changes before replacing a bundle.

## Runtime postconfiguration

`runtime/dev.json`, `runtime/test.json`, and `runtime/prod.json` are the
non-secret post-provisioning contracts consumed after Terraform apply. They
declare Lakehouse folder/table/release-manifest checks plus optional connection,
Power BI, and access reconciliation. Keep secret values in protected
environment variables and reference only their names through
`valuesFromEnvironment`.

The runtime CLI uses only documented Fabric, OneLake DFS, and Power BI
operations. Unsupported OAuth2 consent, on-premises gateway credential
encryption, Direct Lake rewrites, and item-share removal become explicit
operator checkpoints. Any unresolved checkpoint exits with status 2 and leaves
recurring SJD schedules disabled.
