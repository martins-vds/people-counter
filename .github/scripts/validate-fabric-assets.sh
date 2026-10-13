#!/usr/bin/env bash
set -euo pipefail

root="fabric"
evidence_dir="evidence/fabric"
mkdir -p "${evidence_dir}"

uv run python scripts/validate_fabric_definitions.py "${root}" \
  --output "${evidence_dir}/definition-hashes.json"

jq -e '
  .schema == "people-counter-fabric-definition-export-v1"
  and (.workspaceIdEnv | type == "string" and length > 0)
  and (.items | type == "array")
  and (all(.items[];
    (.itemIdEnv | type == "string" and length > 0)
    and (.itemType | type == "string" and length > 0)
    and (.destination | type == "string" and length > 0)
    and (.destination | startswith("/") | not)
    and (.destination | contains("..") | not)
  ))
' "${root}/definition-export-manifest.example.json" \
  > "${evidence_dir}/definition-export-schema.txt"

jq -e '
  .schema == "people-counter-fabric-definition-drift-v1"
  and (.workspaceIdOutput | type == "array" and length > 0)
  and (.items | type == "array" and length > 0)
  and (all(.items[];
    (.itemIdOutput | type == "array" and length > 0)
    and (.itemType | type == "string" and length > 0)
    and (.destination | type == "string" and length > 0)
    and (.destination | startswith("/") | not)
    and (.destination | contains("..") | not)
    and (.optional | type == "boolean")
    and (.tokens | type == "array")
  ))
' "${root}/definition-drift-manifest.json" \
  > "${evidence_dir}/drift-schema.txt"

json_count=0
while IFS= read -r -d '' json_path; do
  jq empty "${json_path}"
  ((json_count += 1))
done < <(
  find "${root}" -type f \
    \( -name '*.json' -o -name '*.pbip' -o -name '*.pbir' \
       -o -name '*.json.tmpl' \) -print0
)

tmdl_count=0
while IFS= read -r -d '' tmdl_path; do
  test -s "${tmdl_path}"
  iconv -f UTF-8 -t UTF-8 "${tmdl_path}" >/dev/null
  ((tmdl_count += 1))
done < <(find "${root}" -type f -name '*.tmdl' -print0)

bundle_count=0
while IFS= read -r -d '' metadata_path; do
  ((bundle_count += 1))
  bundle_dir="$(dirname "${metadata_path}")"
  item_type="$(jq -r .itemType "${metadata_path}")"
  format_name="$(jq -r .format "${metadata_path}")"
  case "${item_type}:${format_name}" in
    SemanticModel:TMDL)
      find "${bundle_dir}" -type f -name '*.tmdl' -print -quit \
        | grep -q .
      ;;
    Report:PBIR|Report:PBIR-Legacy)
      find "${bundle_dir}" -type f \
        \( -name '*.json' -o -name '*.pbir' \) -print -quit \
        | grep -q .
      ;;
  esac
done < <(find "${root}" -type f -name '.fabric-item.json' -print0)

jq -n \
  --argjson bundles "${bundle_count}" \
  --argjson jsonDocuments "${json_count}" \
  --argjson tmdlDocuments "${tmdl_count}" \
  --slurpfile hashes "${evidence_dir}/definition-hashes.json" \
  '{
    schema: "people-counter-fabric-asset-validation-v1",
    bundles: $bundles,
    jsonDocuments: $jsonDocuments,
    tmdlDocuments: $tmdlDocuments,
    definitionEvidence: $hashes[0]
  }' > "${evidence_dir}/asset-validation.json"
