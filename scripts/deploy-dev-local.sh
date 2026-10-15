#!/usr/bin/env bash
set -Eeuo pipefail

readonly SCRIPT_NAME="${0##*/}"
readonly TF_ROOT="infra/environments/dev"
readonly TF_ENVIRONMENT="dev"
readonly DEFAULT_CONFIG=".env.deploy-dev.local"

config_path="${DEFAULT_CONFIG}"
allow_protected_changes=false
approve_shortcut_changes=false
approve_workspace_identity_changes=false
approve_capacity_changes=false
temp_dir=""

usage() {
  cat <<EOF
Usage: ${SCRIPT_NAME} [options]

Deploy the checked-out main commit to the Development Fabric workspace using
the active Azure CLI user session.

Options:
  --config PATH                         Bash environment file (default: ${DEFAULT_CONFIG})
  --allow-protected-changes             Allow reviewed delete/replace actions
  --approve-shortcut-changes            Approve reviewed shortcut changes
  --approve-workspace-identity-changes  Approve reviewed workspace identity changes
  --approve-capacity-changes            Approve reviewed capacity changes
  -h, --help                            Show this help

The script applies only a saved Terraform plan, reconciles the Fabric runtime
twice with schedules disabled, requires zero Terraform drift, and runs the
clean-room acceptance and direct REST probes. Evidence is retained under
evidence/local-deploy-dev/.
EOF
}

die() {
  printf 'Error: %s\n' "$*" >&2
  exit 1
}

log() {
  printf '\n==> %s\n' "$*"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

require_variable() {
  local name=$1
  [[ -n "${!name:-}" ]] || die "Required configuration variable is empty: ${name}"
}

validate_boolean() {
  local name=$1
  case "${!name:-}" in
    true|false) ;;
    *) die "${name} must be true or false" ;;
  esac
}

cleanup() {
  if [[ -n "${temp_dir}" && -d "${temp_dir}" ]]; then
    rm -f "${temp_dir}/activator-action-checkpoint.json"
    rm -rf "${temp_dir}"
  fi
}

trap cleanup EXIT

while (($# > 0)); do
  case "$1" in
    --config)
      (($# >= 2)) || die "--config requires a path"
      config_path=$2
      shift 2
      ;;
    --allow-protected-changes)
      allow_protected_changes=true
      shift
      ;;
    --approve-shortcut-changes)
      approve_shortcut_changes=true
      shift
      ;;
    --approve-workspace-identity-changes)
      approve_workspace_identity_changes=true
      shift
      ;;
    --approve-capacity-changes)
      approve_capacity_changes=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown option: $1"
      ;;
  esac
done

require_command git
repo_root=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)
[[ "${PWD}" == "${repo_root}" ]] || die "Run ${SCRIPT_NAME} from ${repo_root}"

[[ -f "${config_path}" ]] || die \
  "Configuration file not found: ${config_path}. Copy scripts/deploy-dev-local.env.example first."

# The operator owns this local file; sourcing preserves JSON and empty values exactly.
# shellcheck disable=SC1090
source "${config_path}"

required_variables=(
  AZURE_TENANT_ID
  AZURE_SUBSCRIPTION_ID
  TF_BACKEND_RESOURCE_GROUP_NAME
  TF_BACKEND_STORAGE_ACCOUNT_NAME
  TF_BACKEND_CONTAINER_NAME
  FABRIC_CAPACITY_ID
  FABRIC_WORKSPACE_ROLE_ASSIGNMENTS
  FABRIC_ADLS_STORAGE_ACCOUNT_RESOURCE_ID
  FABRIC_ADLS_SHORTCUT_CONTAINER
  FABRIC_ADLS_SHORTCUT_NAME
  FABRIC_ADLS_SHORTCUT_PATH
  FABRIC_ENABLE_REPORTING
  FABRIC_ENABLE_RTI
  FABRIC_ENABLE_ACTIVATOR
  FABRIC_REQUIRE_ACTIVATOR_ACTION_CHECKPOINT
)
for name in "${required_variables[@]}"; do
  require_variable "${name}"
done
: "${FABRIC_ADLS_SHORTCUT_TARGET_PATH:=}"
: "${FABRIC_ACTIVATOR_ACTION_CHECKPOINT_JSON:=}"

for name in \
  FABRIC_ENABLE_REPORTING \
  FABRIC_ENABLE_RTI \
  FABRIC_ENABLE_ACTIVATOR \
  FABRIC_REQUIRE_ACTIVATOR_ACTION_CHECKPOINT; do
  validate_boolean "${name}"
done

guid_pattern='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
[[ "${AZURE_TENANT_ID}" =~ ${guid_pattern} ]] || die "AZURE_TENANT_ID must be a GUID"
[[ "${AZURE_SUBSCRIPTION_ID}" =~ ${guid_pattern} ]] || die "AZURE_SUBSCRIPTION_ID must be a GUID"
[[ "${FABRIC_CAPACITY_ID}" =~ ${guid_pattern} ]] || die "FABRIC_CAPACITY_ID must be a GUID"

require_command jq
jq -e '
  type == "object"
  and all(
    to_entries[];
    (.key | type == "string" and length > 0)
    and (.value | type == "object")
    and (.value.principal_id | type == "string" and length > 0)
    and (.value.principal_type | IN("User", "Group", "ServicePrincipal", "ServicePrincipalProfile"))
    and (.value.role | IN("Admin", "Contributor", "Member", "Viewer"))
  )
' <<<"${FABRIC_WORKSPACE_ROLE_ASSIGNMENTS}" >/dev/null \
  || die "FABRIC_WORKSPACE_ROLE_ASSIGNMENTS is not a valid assignment object"
if [[ -n "${FABRIC_ACTIVATOR_ACTION_CHECKPOINT_JSON}" ]]; then
  jq -e 'type == "object"' <<<"${FABRIC_ACTIVATOR_ACTION_CHECKPOINT_JSON}" >/dev/null \
    || die "FABRIC_ACTIVATOR_ACTION_CHECKPOINT_JSON must be a JSON object"
fi

for command in az terraform uv curl; do
  require_command "${command}"
done

terraform_version=$(terraform version -json | jq -r '.terraform_version')
terraform_major=${terraform_version%%.*}
terraform_minor=${terraform_version#*.}
terraform_minor=${terraform_minor%%.*}
if [[ ! "${terraform_major}" =~ ^[0-9]+$ || ! "${terraform_minor}" =~ ^[0-9]+$ ]] \
  || ((terraform_major != 1 || terraform_minor < 9)); then
  die "Terraform >= 1.9.0 and < 2.0.0 is required; found ${terraform_version}"
fi

[[ "$(git branch --show-current)" == "main" ]] || die \
  "Development deployments must run from the main branch"
[[ -z "$(git status --porcelain --untracked-files=all)" ]] || die \
  "The worktree must be clean before deployment"

log "Confirming immutable main commit"
git fetch --quiet --no-tags origin main
deploy_ref=$(git rev-parse HEAD)
[[ "${deploy_ref}" =~ ^[0-9a-f]{40}$ ]] || die "HEAD is not a full commit SHA"
[[ "${deploy_ref}" == "$(git rev-parse refs/remotes/origin/main)" ]] || die \
  "Local main must exactly match origin/main"

unset \
  ACTIONS_ID_TOKEN_REQUEST_TOKEN \
  ACTIONS_ID_TOKEN_REQUEST_URL \
  ARM_CLIENT_CERTIFICATE_PASSWORD \
  ARM_CLIENT_CERTIFICATE_PATH \
  ARM_CLIENT_ID \
  ARM_CLIENT_SECRET \
  ARM_OIDC_TOKEN \
  ARM_OIDC_TOKEN_FILE_PATH \
  ARM_USE_MSI \
  ARM_USE_OIDC \
  FABRIC_CLIENT_ID \
  FABRIC_CLIENT_SECRET \
  FABRIC_USE_OIDC
export ARM_SUBSCRIPTION_ID="${AZURE_SUBSCRIPTION_ID}"
export ARM_TENANT_ID="${AZURE_TENANT_ID}"
export ARM_USE_AZUREAD_AUTH=true
export TF_IN_AUTOMATION=true
export TF_INPUT=false
export TF_VAR_adls_shortcut_container="${FABRIC_ADLS_SHORTCUT_CONTAINER}"
export TF_VAR_adls_shortcut_name="${FABRIC_ADLS_SHORTCUT_NAME}"
export TF_VAR_adls_shortcut_path="${FABRIC_ADLS_SHORTCUT_PATH}"
export TF_VAR_adls_shortcut_target_path="${FABRIC_ADLS_SHORTCUT_TARGET_PATH}"
export TF_VAR_adls_storage_account_resource_id="${FABRIC_ADLS_STORAGE_ACCOUNT_RESOURCE_ID}"
export TF_VAR_capacity_id="${FABRIC_CAPACITY_ID}"
export TF_VAR_enable_activator="${FABRIC_ENABLE_ACTIVATOR}"
export TF_VAR_workspace_role_assignments="${FABRIC_WORKSPACE_ROLE_ASSIGNMENTS}"
export UV_PYTHON=3.13

log "Validating Azure CLI context"
account_json=$(az account show --output json 2>/dev/null) \
  || die "Azure CLI is not authenticated; run az login"
[[ "$(jq -r '.id' <<<"${account_json}")" == "${AZURE_SUBSCRIPTION_ID}" ]] || die \
  "Azure CLI subscription does not match AZURE_SUBSCRIPTION_ID"
[[ "$(jq -r '.tenantId' <<<"${account_json}")" == "${AZURE_TENANT_ID}" ]] || die \
  "Azure CLI tenant does not match AZURE_TENANT_ID"
az account get-access-token \
  --scope "https://api.fabric.microsoft.com/.default" \
  --query accessToken --output tsv >/dev/null
az account get-access-token \
  --scope "https://storage.azure.com/.default" \
  --query accessToken --output tsv >/dev/null
if [[ "${FABRIC_ENABLE_REPORTING}" == "true" ]]; then
  az account get-access-token \
    --scope "https://analysis.windows.net/powerbi/api/.default" \
    --query accessToken --output tsv >/dev/null
fi

run_id="$(date -u +'%Y%m%dT%H%M%SZ')"
evidence_dir="${repo_root}/evidence/local-deploy-dev/${run_id}-${deploy_ref:0:12}"
terraform_evidence="${evidence_dir}/terraform"
initial_evidence="${evidence_dir}/postconfigure-initial"
repeat_evidence="${evidence_dir}/postconfigure-repeat"
smoke_evidence="${evidence_dir}/smoke"
common_evidence="${evidence_dir}/common"
umask 077
mkdir -p \
  "${terraform_evidence}" \
  "${initial_evidence}" \
  "${repeat_evidence}" \
  "${smoke_evidence}" \
  "${common_evidence}"
temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/people-counter-deploy-dev.XXXXXX")

jq -n \
  --arg commitSha "${deploy_ref}" \
  --arg environment "${TF_ENVIRONMENT}" \
  --arg runId "${run_id}" \
  '{
    schema: "people-counter-local-deployment-v1",
    environment: $environment,
    commitSha: $commitSha,
    runId: $runId
  }' > "${evidence_dir}/context.json"

log "Initializing and validating Terraform"
set -o pipefail
terraform -chdir="${TF_ROOT}" init -input=false \
  -backend-config="resource_group_name=${TF_BACKEND_RESOURCE_GROUP_NAME}" \
  -backend-config="storage_account_name=${TF_BACKEND_STORAGE_ACCOUNT_NAME}" \
  -backend-config="container_name=${TF_BACKEND_CONTAINER_NAME}" \
  -backend-config="key=people-counter/${TF_ENVIRONMENT}.tfstate" \
  -backend-config="use_azuread_auth=true" \
  2>&1 | tee "${terraform_evidence}/init.log"
terraform -chdir="${TF_ROOT}" validate -no-color \
  2>&1 | tee "${terraform_evidence}/validate.log"

log "Creating saved Terraform plan"
set +e
terraform -chdir="${TF_ROOT}" plan -input=false -detailed-exitcode -no-color \
  -out="${temp_dir}/tfplan" \
  2>&1 | tee "${terraform_evidence}/plan.log"
plan_status=${PIPESTATUS[0]}
set -e
case "${plan_status}" in
  0|2) ;;
  *) die "Terraform plan failed with exit code ${plan_status}" ;;
esac
terraform -chdir="${TF_ROOT}" show -json "${temp_dir}/tfplan" \
  > "${temp_dir}/tfplan.json"
terraform -chdir="${TF_ROOT}" show -no-color "${temp_dir}/tfplan" \
  > "${terraform_evidence}/plan.txt"

jq --arg environment "${TF_ENVIRONMENT}" '
  def changed:
    any(.change.actions[]?; . != "no-op" and . != "read");
  def deletes:
    any(.change.actions[]?; . == "delete");
  def protected_type:
    (.type | startswith("fabric_"))
    or .type == "azurerm_fabric_capacity";
  [
    .resource_changes[]?
    | select(changed)
    | {
        address,
        actions: .change.actions,
        type,
        protectedDeletion: (deletes and protected_type),
        shortcutChange: (
          .type == "fabric_shortcut"
          or .type == "azurerm_storage_data_lake_gen2_filesystem"
          or (
            .type == "fabric_connection"
            and (.address | contains("adls_workspace_identity"))
          )
          or (
            .type == "azurerm_role_assignment"
            and (.address | contains("adls_workspace_identity"))
          )
        ),
        workspaceIdentityChange: (
          .type == "fabric_workspace"
          and ((.change.before.identity // null) != (.change.after.identity // null))
        ),
        capacityChange: (
          .type == "azurerm_fabric_capacity"
          or (
            .type == "fabric_workspace"
            and (
              (.change.before.capacity_id // null)
              != (.change.after.capacity_id // null)
            )
          )
        ),
        productionDeletion: (
          $environment == "prod"
          and (.type | startswith("fabric_"))
          and deletes
        )
      }
  ] as $changes
  | {
      protectedChanges: [$changes[] | select(.protectedDeletion)],
      shortcutChanges: [$changes[] | select(.shortcutChange)],
      workspaceIdentityChanges: [$changes[] | select(.workspaceIdentityChange)],
      capacityChanges: [$changes[] | select(.capacityChange)],
      productionDeletions: [$changes[] | select(.productionDeletion)]
    }
' "${temp_dir}/tfplan.json" > "${terraform_evidence}/protected-changes.json"
jq -r '
  to_entries[]
  | .key as $category
  | .value[]
  | "\($category): \(.address) [\(.actions | join("/"))]"
' "${terraform_evidence}/protected-changes.json" \
  > "${terraform_evidence}/protected-changes.txt"

log "Terraform plan summary"
jq -r '
  [.resource_changes[]?.change.actions | join("/")]
  | group_by(.)
  | map({action: .[0], count: length})
  | .[]
  | "\(.action): \(.count)"
' "${temp_dir}/tfplan.json"
if [[ -s "${terraform_evidence}/protected-changes.txt" ]]; then
  printf '\nApproval-gated changes:\n'
  cat "${terraform_evidence}/protected-changes.txt"
fi

violations=()
if (( $(jq '.protectedChanges | length' \
  "${terraform_evidence}/protected-changes.json") > 0 )) \
  && [[ "${allow_protected_changes}" != "true" ]]; then
  violations+=("--allow-protected-changes")
fi
if (( $(jq '.shortcutChanges | length' \
  "${terraform_evidence}/protected-changes.json") > 0 )) \
  && [[ "${approve_shortcut_changes}" != "true" ]]; then
  violations+=("--approve-shortcut-changes")
fi
if (( $(jq '.workspaceIdentityChanges | length' \
  "${terraform_evidence}/protected-changes.json") > 0 )) \
  && [[ "${approve_workspace_identity_changes}" != "true" ]]; then
  violations+=("--approve-workspace-identity-changes")
fi
if (( $(jq '.capacityChanges | length' \
  "${terraform_evidence}/protected-changes.json") > 0 )) \
  && [[ "${approve_capacity_changes}" != "true" ]]; then
  violations+=("--approve-capacity-changes")
fi
if ((${#violations[@]} > 0)); then
  printf -v missing '%s, ' "${violations[@]}"
  die "Saved plan requires explicit approvals: ${missing%, }. Evidence: ${evidence_dir}"
fi

log "Applying saved Terraform plan"
terraform -chdir="${TF_ROOT}" apply -input=false -auto-approve \
  "${temp_dir}/tfplan" \
  2>&1 | tee "${terraform_evidence}/apply.log"

log "Exporting non-secret deployment outputs"
terraform -chdir="${TF_ROOT}" output -json > "${temp_dir}/terraform-output.json"
jq '{
  workspace_id: .workspace_id.value,
  lakehouse_id: .lakehouse_id.value,
  managed_shortcuts: .managed_shortcuts.value,
  environment_id: .environment_id.value,
  spark_job_definition_ids: .spark_job_definition_ids.value,
  semantic_model_ids: .semantic_model_ids.value,
  report_ids: .report_ids.value,
  eventhouse_id: .eventhouse_id.value,
  kql_database_id: .kql_database_id.value,
  kql_queryset_id: .kql_queryset_id.value,
  kql_dashboard_id: .kql_dashboard_id.value,
  activator_ids: .activator_ids.value
}' "${temp_dir}/terraform-output.json" \
  > "${common_evidence}/deployment-outputs.json"

log "Building deployment wheel and validating definitions"
mkdir -p "${temp_dir}/dist"
uv build --wheel --out-dir "${temp_dir}/dist" \
  2>&1 | tee "${common_evidence}/wheel-build.log"
uv run --frozen python scripts/validate_fabric_definitions.py fabric \
  --output "${common_evidence}/fabric-definitions.json"
wheel=$(find "${temp_dir}/dist" -maxdepth 1 -name 'people_counter-*.whl' -print -quit)
[[ -n "${wheel}" ]] || die "The deployment wheel was not created"

checkpoint_args=()
if [[ -n "${FABRIC_ACTIVATOR_ACTION_CHECKPOINT_JSON}" ]]; then
  printf '%s' "${FABRIC_ACTIVATOR_ACTION_CHECKPOINT_JSON}" \
    > "${temp_dir}/activator-action-checkpoint.json"
  checkpoint_args=(
    --activator-checkpoint-path
    "${temp_dir}/activator-action-checkpoint.json"
  )
fi

run_reconcile() {
  local phase=$1
  local phase_evidence=$2
  local exit_code
  local status
  local args=(
    --manifest-path "src/people_counter/data/deploy/${TF_ENVIRONMENT}.json"
    --runtime-contract-path "fabric/runtime/${TF_ENVIRONMENT}.json"
    --stage "${TF_ENVIRONMENT}"
    --terraform-output-path "${temp_dir}/terraform-output.json"
    --definition-evidence-path "${common_evidence}/fabric-definitions.json"
    --commit-sha "${deploy_ref}"
    --whl-path "${wheel}"
    --bootstrap-lakehouse
  )

  cp "${common_evidence}/deployment-outputs.json" \
    "${phase_evidence}/deployment-outputs.json"
  jq -n \
    --arg commitSha "${deploy_ref}" \
    --arg environment "${TF_ENVIRONMENT}" \
    --arg phase "${phase}" \
    --arg runId "${run_id}" \
    '{
      schema: "people-counter-fabric-workflow-evidence-v1",
      commitSha: $commitSha,
      environment: $environment,
      phase: $phase,
      runId: $runId
    }' > "${phase_evidence}/workflow-context.json"

  [[ "${FABRIC_ENABLE_REPORTING}" == "true" ]] \
    && args+=(--refresh-reporting)
  if [[ "${FABRIC_ENABLE_ACTIVATOR}" == "true" ]]; then
    args+=(--enable-activator --activator-bundle-path fabric/activators/operations)
    [[ "${FABRIC_REQUIRE_ACTIVATOR_ACTION_CHECKPOINT}" == "false" ]] \
      && args+=(--allow-unverified-activator-actions)
    args+=("${checkpoint_args[@]}")
  fi

  log "Reconciling Fabric runtime (${phase}, schedules disabled)"
  set +e
  PEOPLE_COUNTER_DEPLOY_STAGE="${TF_ENVIRONMENT}" \
    uv run --frozen pc-reconcile-fabric-runtime "${args[@]}" \
    | tee "${phase_evidence}/fabric-evidence.json"
  exit_code=${PIPESTATUS[0]}
  set -e
  status=$(jq -r '.status // "invalid-evidence"' \
    "${phase_evidence}/fabric-evidence.json" 2>/dev/null \
    || printf 'invalid-evidence')
  case "${status}:${exit_code}" in
    succeeded:0) ;;
    operator_checkpoint_required:2)
      die "Reconciliation requires an operator checkpoint; schedules remain disabled"
      ;;
    *)
      die "Fabric reconciliation failed (${phase}); schedules remain disabled"
      ;;
  esac
}

run_reconcile initial "${initial_evidence}"
run_reconcile repeat "${repeat_evidence}"
rm -f "${temp_dir}/activator-action-checkpoint.json"

log "Requiring zero Terraform drift"
set +e
terraform -chdir="${TF_ROOT}" plan \
  -detailed-exitcode -input=false -lock-timeout=5m -no-color \
  -out="${temp_dir}/repeat.tfplan" \
  > "${smoke_evidence}/terraform-repeat-plan.txt" 2>&1
drift_status=$?
set -e

acceptance_args=(
  --manifest-path "src/people_counter/data/deploy/${TF_ENVIRONMENT}.json"
  --runtime-contract-path "fabric/runtime/${TF_ENVIRONMENT}.json"
  --terraform-output-path "${temp_dir}/terraform-output.json"
  --definition-evidence-path "${common_evidence}/fabric-definitions.json"
  --postconfigure-evidence-path "${initial_evidence}/fabric-evidence.json"
  --postconfigure-evidence-path "${repeat_evidence}/fabric-evidence.json"
  --stage "${TF_ENVIRONMENT}"
  --commit-sha "${deploy_ref}"
  --terraform-drift-exit-code "${drift_status}"
)
[[ "${FABRIC_ENABLE_REPORTING}" == "true" ]] \
  && acceptance_args+=(--require-reporting)
[[ "${FABRIC_ENABLE_RTI}" == "true" ]] \
  && acceptance_args+=(--require-rti)
[[ "${FABRIC_ENABLE_ACTIVATOR}" == "true" ]] \
  && acceptance_args+=(--require-activator)
[[ "${FABRIC_REQUIRE_ACTIVATOR_ACTION_CHECKPOINT}" == "false" ]] \
  && acceptance_args+=(--allow-unverified-activator-actions)

log "Running clean-room deployment acceptance"
uv run --frozen pc-validate-fabric-deployment "${acceptance_args[@]}" \
  | tee "${smoke_evidence}/deployment-verification.json"

checks='[]'
failures=0
record_probe() {
  local name=$1
  local status=$2
  local detail=$3
  checks=$(jq -c \
    --arg name "${name}" \
    --arg status "${status}" \
    --arg detail "${detail}" \
    '. + [{name: $name, status: $status, detail: $detail}]' \
    <<<"${checks}")
  [[ "${status}" != "failed" ]] || failures=$((failures + 1))
}

output_value() {
  jq -er --arg name "$1" '.[$name].value // empty' \
    "${temp_dir}/terraform-output.json"
}

token=$(az account get-access-token \
  --scope "https://api.fabric.microsoft.com/.default" \
  --query accessToken --output tsv)
[[ -n "${token}" ]] || die "Azure CLI returned no Fabric access token"
curl_config="${temp_dir}/curl.conf"
printf 'header = "Authorization: Bearer %s"\nheader = "Accept: application/json"\n' \
  "${token}" > "${curl_config}"
unset token

probe() {
  local name=$1
  local path=$2
  local output=${3:-"${temp_dir}/response.json"}
  local status
  status=$(curl --silent --show-error \
    --connect-timeout 15 --max-time 90 \
    --retry 3 --retry-all-errors --retry-delay 5 \
    --config "${curl_config}" \
    --output "${output}" --write-out '%{http_code}' \
    "https://api.fabric.microsoft.com/v1${path}") || status="000"
  if [[ "${status}" =~ ^2[0-9][0-9]$ ]]; then
    record_probe "${name}" succeeded "HTTP ${status}"
    return 0
  fi
  record_probe "${name}" failed "HTTP ${status}"
  return 1
}

log "Running direct Fabric REST probes"
workspace_id=$(output_value workspace_id)
lakehouse_id=$(output_value lakehouse_id)
environment_id=$(output_value environment_id)
sjd_ids=$(output_value spark_job_definition_ids)
semantic_model_ids=$(output_value semantic_model_ids)
report_ids=$(output_value report_ids)
activator_ids=$(output_value activator_ids)

probe workspace "/workspaces/${workspace_id}" || true
probe lakehouse "/workspaces/${workspace_id}/lakehouses/${lakehouse_id}" || true
probe environment \
  "/workspaces/${workspace_id}/environments/${environment_id}" || true

expected_roles='["control","cutover","dispatcher","gold","process","reconciliation","refresh"]'
actual_roles=$(jq -c 'keys | sort' <<<"${sjd_ids}")
if [[ "${actual_roles}" == "${expected_roles}" ]]; then
  record_probe sjd-role-set succeeded "All seven canonical roles exist"
else
  record_probe sjd-role-set failed \
    "Expected seven canonical roles; received ${actual_roles}"
fi
while IFS=$'\t' read -r role item_id; do
  probe "sjd-${role}" \
    "/workspaces/${workspace_id}/sparkJobDefinitions/${item_id}" || true
done < <(jq -r 'to_entries[] | [.key, .value] | @tsv' <<<"${sjd_ids}")

for role in control dispatcher refresh reconciliation gold; do
  item_id=$(jq -er --arg role "${role}" '.[$role]' <<<"${sjd_ids}")
  schedule_file="${temp_dir}/schedule-${role}.json"
  if probe "schedule-${role}-endpoint" \
    "/workspaces/${workspace_id}/items/${item_id}/jobs/sparkjob/schedules" \
    "${schedule_file}"; then
    if jq -e '(.value // []) | length == 1 and .[0].enabled == false' \
      "${schedule_file}" >/dev/null; then
      record_probe "schedule-${role}" succeeded \
        "Exactly one schedule is disabled"
    else
      record_probe "schedule-${role}" failed \
        "Expected exactly one disabled schedule"
    fi
  fi
done

if [[ "${FABRIC_ENABLE_REPORTING}" == "true" ]]; then
  for bundle in analytics operational; do
    model_id=$(jq -er --arg key "${bundle}" '.[$key]' \
      <<<"${semantic_model_ids}")
    report_id=$(jq -er --arg key "${bundle}" '.[$key]' \
      <<<"${report_ids}")
    probe "semantic-model-${bundle}" \
      "/workspaces/${workspace_id}/semanticModels/${model_id}" || true
    probe "report-${bundle}" \
      "/workspaces/${workspace_id}/reports/${report_id}" || true
  done
else
  record_probe reporting-gate skipped "FABRIC_ENABLE_REPORTING is false"
fi

if [[ "${FABRIC_ENABLE_ACTIVATOR}" == "true" ]]; then
  activator_id=$(jq -er '.operations' <<<"${activator_ids}")
  probe activator \
    "/workspaces/${workspace_id}/reflexes/${activator_id}" || true
else
  record_probe activator-gate skipped "FABRIC_ENABLE_ACTIVATOR is false"
fi

smoke_status=succeeded
((failures == 0)) || smoke_status=failed
jq -n \
  --arg status "${smoke_status}" \
  --arg generatedAt "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
  --arg environment "${TF_ENVIRONMENT}" \
  --arg commitSha "${deploy_ref}" \
  --arg runId "${run_id}" \
  --argjson checks "${checks}" \
  '{
    schema: "people-counter-fabric-smoke-evidence-v1",
    status: $status,
    environment: $environment,
    commitSha: $commitSha,
    runId: $runId,
    generatedAt: $generatedAt,
    checks: $checks
  }' > "${smoke_evidence}/smoke.json"
[[ "${smoke_status}" == "succeeded" ]] || die \
  "Direct Fabric probes failed; schedules remain disabled"

log "Development deployment succeeded"
printf 'Commit:  %s\nEvidence: %s\n' "${deploy_ref}" "${evidence_dir}"
