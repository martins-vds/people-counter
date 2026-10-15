#!/usr/bin/env bash
set -Eeuo pipefail

readonly SCRIPT_NAME="${0##*/}"
readonly TF_ROOT="infra/bootstrap"
readonly DEFAULT_VAR_FILE="infra/bootstrap/terraform.tfvars"

var_file="${DEFAULT_VAR_FILE}"
target_environment="dev"
env_file=""
force=false
temp_dir=""
env_temp=""

usage() {
  cat <<EOF
Usage: ${SCRIPT_NAME} [options]

Bootstrap the Azure and Microsoft Entra resources using the active Azure CLI
user session, then create a local deployment environment file.

Options:
  --environment NAME  Target environment: dev, test, or prod (default: dev)
  --var-file PATH  Terraform variable file (default: ${DEFAULT_VAR_FILE})
  --env-file PATH  Generated environment file (default: .env.deploy-<environment>.local)
  --force          Replace an existing environment file
  -h, --help       Show this help

The script applies only a saved Terraform plan. It resolves the selected
Fabric capacity UUID through the Fabric API and writes the environment file
with mode 600.
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

cleanup() {
  if [[ -n "${env_temp}" ]]; then
    rm -f "${env_temp}"
  fi
  if [[ -n "${temp_dir}" && -d "${temp_dir}" ]]; then
    rm -rf "${temp_dir}"
  fi
}

trap cleanup EXIT

while (($# > 0)); do
  case "$1" in
    --environment)
      (($# >= 2)) || die "--environment requires a name"
      target_environment=$2
      shift 2
      ;;
    --var-file)
      (($# >= 2)) || die "--var-file requires a path"
      var_file=$2
      shift 2
      ;;
    --env-file)
      (($# >= 2)) || die "--env-file requires a path"
      env_file=$2
      shift 2
      ;;
    --force)
      force=true
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

case "${target_environment}" in
  dev|test|prod) ;;
  *) die "--environment must be dev, test, or prod" ;;
esac
[[ -n "${env_file}" ]] || env_file=".env.deploy-${target_environment}.local"

for command in git az terraform jq curl; do
  require_command "${command}"
done

repo_root=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)
[[ "${PWD}" == "${repo_root}" ]] || die "Run ${SCRIPT_NAME} from ${repo_root}"
[[ "${var_file}" == /* ]] || var_file="${repo_root}/${var_file}"
[[ "${env_file}" == /* ]] || env_file="${repo_root}/${env_file}"
[[ -f "${var_file}" ]] || die \
  "Terraform variable file not found: ${var_file}. Copy infra/bootstrap/terraform.tfvars.example first."
if [[ -e "${env_file}" && "${force}" != "true" ]]; then
  die "Environment file already exists: ${env_file}. Use --force to replace it."
fi
[[ -d "$(dirname "${env_file}")" ]] || die \
  "Environment file directory does not exist: $(dirname "${env_file}")"

terraform_version=$(terraform version -json | jq -r '.terraform_version')
terraform_major=${terraform_version%%.*}
terraform_minor=${terraform_version#*.}
terraform_minor=${terraform_minor%%.*}
if [[ ! "${terraform_major}" =~ ^[0-9]+$ || ! "${terraform_minor}" =~ ^[0-9]+$ ]] \
  || ((terraform_major != 1 || terraform_minor < 9)); then
  die "Terraform >= 1.9.0 and < 2.0.0 is required; found ${terraform_version}"
fi

[[ "$(git branch --show-current)" == "main" ]] || die \
  "Bootstrap must run from the main branch"
[[ -z "$(git status --porcelain --untracked-files=all)" ]] || die \
  "The worktree must be clean before bootstrap"

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
  ARM_USE_OIDC
export TF_IN_AUTOMATION=true
export TF_INPUT=false

run_id="$(date -u +'%Y%m%dT%H%M%SZ')"
evidence_dir="${repo_root}/evidence/local-bootstrap/${run_id}-${deploy_ref:0:12}"
umask 077
mkdir -p "${evidence_dir}"
temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/people-counter-bootstrap.XXXXXX")

jq -n \
  --arg commitSha "${deploy_ref}" \
  --arg environment "${target_environment}" \
  --arg runId "${run_id}" \
  '{
    schema: "people-counter-local-bootstrap-v1",
    environment: $environment,
    commitSha: $commitSha,
    runId: $runId
  }' > "${evidence_dir}/context.json"

log "Initializing Bootstrap Terraform"
set -o pipefail
terraform -chdir="${TF_ROOT}" init -input=false \
  2>&1 | tee "${evidence_dir}/terraform-init.log"
terraform -chdir="${TF_ROOT}" validate -no-color \
  2>&1 | tee "${evidence_dir}/terraform-validate.log"

subscription_id=$(
  printf 'var.subscription_id\n' \
    | terraform -chdir="${TF_ROOT}" console -var-file="${var_file}" \
    | jq -er .
)
tenant_id=$(
  printf 'var.tenant_id\n' \
    | terraform -chdir="${TF_ROOT}" console -var-file="${var_file}" \
    | jq -er .
)
guid_pattern='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
[[ "${subscription_id}" =~ ${guid_pattern} ]] || die \
  "subscription_id in ${var_file} must be a GUID"
[[ "${tenant_id}" =~ ${guid_pattern} ]] || die \
  "tenant_id in ${var_file} must be a GUID"

log "Validating Azure CLI context"
account_json=$(az account show --output json 2>/dev/null) \
  || die "Azure CLI is not authenticated; run az login"
[[ "$(jq -r '.id' <<<"${account_json}")" == "${subscription_id}" ]] || die \
  "Azure CLI subscription does not match bootstrap subscription_id"
[[ "$(jq -r '.tenantId' <<<"${account_json}")" == "${tenant_id}" ]] || die \
  "Azure CLI tenant does not match bootstrap tenant_id"

get_fabric_token() {
  az account get-access-token \
    --scope "https://api.fabric.microsoft.com/.default" \
    --query accessToken --output tsv
}

write_curl_config() {
  local token=$1
  printf 'header = "Authorization: Bearer %s"\nheader = "Accept: application/json"\n' \
    "${token}" > "${temp_dir}/curl.conf"
}

fabric_token=$(get_fabric_token)
[[ -n "${fabric_token}" ]] || die "Azure CLI returned no Fabric access token"
write_curl_config "${fabric_token}"
unset fabric_token
curl --fail --silent --show-error \
  --connect-timeout 15 --max-time 90 \
  --retry 3 --retry-all-errors --retry-delay 5 \
  --config "${temp_dir}/curl.conf" \
  --output /dev/null \
  "https://api.fabric.microsoft.com/v1/capacities" \
  || die "The Azure CLI identity cannot list Fabric capacities"

log "Creating saved Bootstrap Terraform plan"
set +e
terraform -chdir="${TF_ROOT}" plan \
  -input=false -detailed-exitcode -no-color \
  -var-file="${var_file}" \
  -out="${temp_dir}/bootstrap.tfplan" \
  2>&1 | tee "${evidence_dir}/terraform-plan.log"
plan_status=${PIPESTATUS[0]}
set -e
case "${plan_status}" in
  0|2) ;;
  *) die "Bootstrap Terraform plan failed with exit code ${plan_status}" ;;
esac
terraform -chdir="${TF_ROOT}" show -json "${temp_dir}/bootstrap.tfplan" \
  > "${temp_dir}/bootstrap.tfplan.json"
terraform -chdir="${TF_ROOT}" show -no-color "${temp_dir}/bootstrap.tfplan" \
  > "${evidence_dir}/terraform-plan.txt"

log "Bootstrap Terraform plan summary"
jq -r '
  [.resource_changes[]?.change.actions | join("/")]
  | group_by(.)
  | map({action: .[0], count: length})
  | .[]
  | "\(.action): \(.count)"
' "${temp_dir}/bootstrap.tfplan.json"

log "Applying saved Bootstrap Terraform plan"
terraform -chdir="${TF_ROOT}" apply -input=false -auto-approve \
  "${temp_dir}/bootstrap.tfplan" \
  2>&1 | tee "${evidence_dir}/terraform-apply.log"

terraform -chdir="${TF_ROOT}" output -json > "${temp_dir}/terraform-output.json"
jq '{
  resource_group_name: .resource_group_name.value,
  state_storage_account_name: .state_storage_account_name.value,
  state_container_name: .state_container_name.value,
  fabric_capacity_ids_by_environment: .fabric_capacity_ids_by_environment.value,
  adls_shortcut_source_by_environment: .adls_shortcut_source_by_environment.value,
  tenant_id: .tenant_id.value,
  subscription_id: .subscription_id.value
}' "${temp_dir}/terraform-output.json" > "${evidence_dir}/bootstrap-outputs.json"

resource_group_name=$(jq -er '.resource_group_name.value' \
  "${temp_dir}/terraform-output.json")
state_storage_account_name=$(jq -er '.state_storage_account_name.value' \
  "${temp_dir}/terraform-output.json")
state_container_name=$(jq -er '.state_container_name.value' \
  "${temp_dir}/terraform-output.json")
capacity_arm_id=$(jq -er \
  --arg environment "${target_environment}" \
  '.fabric_capacity_ids_by_environment.value[$environment]' \
  "${temp_dir}/terraform-output.json")
adls_storage_account_resource_id=$(jq -er \
  --arg environment "${target_environment}" \
  '.adls_shortcut_source_by_environment.value[$environment].storage_account_resource_id' \
  "${temp_dir}/terraform-output.json")
adls_container=$(jq -er \
  --arg environment "${target_environment}" \
  '.adls_shortcut_source_by_environment.value[$environment].container_name' \
  "${temp_dir}/terraform-output.json")
capacity_name=${capacity_arm_id##*/}

fabric_token=$(get_fabric_token)
[[ -n "${fabric_token}" ]] || die "Azure CLI returned no Fabric access token"
write_curl_config "${fabric_token}"
unset fabric_token
curl --fail --silent --show-error \
  --connect-timeout 15 --max-time 90 \
  --retry 3 --retry-all-errors --retry-delay 5 \
  --config "${temp_dir}/curl.conf" \
  --output "${temp_dir}/fabric-capacities.json" \
  "https://api.fabric.microsoft.com/v1/capacities" \
  || die "Fabric capacity discovery failed after Bootstrap apply"
fabric_capacity_id=$(jq -er --arg name "${capacity_name}" '
  [.value[] | select(.displayName == $name) | .id]
  | if length == 1 then .[0]
    else error("expected exactly one matching Fabric capacity")
    end
' "${temp_dir}/fabric-capacities.json") || die \
  "Expected exactly one Fabric capacity named ${capacity_name}"

env_temp="${env_file}.tmp.$$"
write_assignment() {
  local name=$1
  local value=$2
  printf '%s=%s\n' "${name}" "$(jq -rn --arg value "${value}" '$value | @sh')"
}

{
  printf '# Generated by scripts/bootstrap-local.sh from Bootstrap Terraform outputs.\n'
  printf '# This is a trusted Bash environment file. Do not add commands.\n\n'
  write_assignment AZURE_TENANT_ID "${tenant_id}"
  write_assignment AZURE_SUBSCRIPTION_ID "${subscription_id}"
  printf '\n'
  write_assignment TF_BACKEND_RESOURCE_GROUP_NAME "${resource_group_name}"
  write_assignment TF_BACKEND_STORAGE_ACCOUNT_NAME "${state_storage_account_name}"
  write_assignment TF_BACKEND_CONTAINER_NAME "${state_container_name}"
  printf '\n'
  write_assignment FABRIC_CAPACITY_ID "${fabric_capacity_id}"
  write_assignment FABRIC_WORKSPACE_ROLE_ASSIGNMENTS '{}'
  printf '\n'
  write_assignment FABRIC_ADLS_STORAGE_ACCOUNT_RESOURCE_ID \
    "${adls_storage_account_resource_id}"
  write_assignment FABRIC_ADLS_SHORTCUT_CONTAINER "${adls_container}"
  write_assignment FABRIC_ADLS_SHORTCUT_NAME "${adls_container}"
  write_assignment FABRIC_ADLS_SHORTCUT_PATH "Files/${adls_container}"
  write_assignment FABRIC_ADLS_SHORTCUT_TARGET_PATH ''
  printf '\n'
  write_assignment FABRIC_ENABLE_REPORTING true
  write_assignment FABRIC_ENABLE_RTI true
  write_assignment FABRIC_ENABLE_ACTIVATOR true
  write_assignment FABRIC_REQUIRE_ACTIVATOR_ACTION_CHECKPOINT false
  printf '\n'
  printf '# Optional while action-execution verification is deferred.\n'
  write_assignment FABRIC_ACTIVATOR_ACTION_CHECKPOINT_JSON ''
} > "${env_temp}"
chmod 600 "${env_temp}"
mv -f "${env_temp}" "${env_file}"
env_temp=""

log "Local bootstrap succeeded"
printf 'Commit:      %s\nEnvironment: %s\nEvidence:    %s\n' \
  "${deploy_ref}" "${env_file}" "${evidence_dir}"
