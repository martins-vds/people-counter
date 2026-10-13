#!/usr/bin/env bash
set -euo pipefail

evidence_dir="evidence/python-quality"
mkdir -p "${evidence_dir}"

base_sha="${BASE_SHA:-HEAD^}"
head_sha="${HEAD_SHA:-HEAD}"
git cat-file -e "${base_sha}^{commit}"
git cat-file -e "${head_sha}^{commit}"

mapfile -t changed_python < <(
  git diff --diff-filter=ACMR --name-only "${base_sha}" "${head_sha}" -- \
    '*.py' | sort -u
)
printf '%s\n' "${changed_python[@]}" > "${evidence_dir}/changed-python.txt"

if ((${#changed_python[@]} == 0)); then
  printf '%s\n' '{"changedPython":0,"linted":0,"typeChecked":0}' \
    > "${evidence_dir}/summary.json"
  exit 0
fi

uvx --from ruff==0.14.1 ruff check \
  --output-format=concise "${changed_python[@]}" \
  2>&1 | tee "${evidence_dir}/ruff.txt"

readonly -a type_debt=(
  "src/people_counter/fabric_reconcilers.py"
)
type_targets=()
for path in "${changed_python[@]}"; do
  case "${path}" in
    src/people_counter/*.py|scripts/*.py) ;;
    *) continue ;;
  esac
  skip=false
  for debt_path in "${type_debt[@]}"; do
    if [[ "${path}" == "${debt_path}" ]]; then
      printf '%s\n' "${path}" >> "${evidence_dir}/typecheck-baseline-debt.txt"
      skip=true
      break
    fi
  done
  if [[ "${skip}" == false ]]; then
    type_targets+=("${path}")
  fi
done

if ((${#type_targets[@]} > 0)); then
  printf '%s\n' "${type_targets[@]}" > "${evidence_dir}/typechecked-python.txt"
  PYRIGHT_PYTHON_FORCE_VERSION=1.1.406 \
    uvx --from pyright==1.1.406 pyright \
      --level error "${type_targets[@]}" \
      2>&1 | tee "${evidence_dir}/pyright.txt"
else
  echo "No changed Python files outside the documented type-debt baseline." \
    | tee "${evidence_dir}/pyright.txt"
fi

jq -n \
  --argjson changed "${#changed_python[@]}" \
  --argjson typed "${#type_targets[@]}" \
  '{changedPython: $changed, linted: $changed, typeChecked: $typed}' \
  > "${evidence_dir}/summary.json"
