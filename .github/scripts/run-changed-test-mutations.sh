#!/usr/bin/env bash
set -euo pipefail

evidence_dir="evidence/mutation"
mkdir -p "${evidence_dir}"

base_sha="${BASE_SHA:-HEAD^}"
head_sha="${HEAD_SHA:-HEAD}"
git cat-file -e "${base_sha}^{commit}"
git cat-file -e "${head_sha}^{commit}"

mapfile -t changed_tests < <(
  git diff --diff-filter=ACMRD --name-only "${base_sha}" "${head_sha}" -- \
    'tests/test_*.py' | sort -u
)
printf '%s\n' "${changed_tests[@]}" > "${evidence_dir}/changed-tests.txt"

if ((${#changed_tests[@]} == 0)); then
  printf '%s\n' \
    '{"changedTests":0,"targets":0,"status":"not-applicable"}' \
    > "${evidence_dir}/policy.json"
  echo "No changed Python tests; mutation policy is not applicable."
  exit 0
fi

targets=()
active_tests=()
for test_path in "${changed_tests[@]}"; do
  [[ -f "${test_path}" ]] && active_tests+=("${test_path}")
  stem="$(basename "${test_path}" .py)"
  stem="${stem#test_}"
  if [[ -f "src/people_counter/${stem}.py" ]]; then
    targets+=("people_counter.${stem}*")
    continue
  fi
  if [[ -f "scripts/${stem}.py" ]] \
    && grep -Fq "\"scripts/${stem}.py\"" pyproject.toml; then
    targets+=("scripts.${stem}*")
    continue
  fi

  if [[ -f "${test_path}" ]]; then
    test_source=(cat "${test_path}")
  else
    test_source=(git show "${base_sha}:${test_path}")
  fi
  while IFS= read -r module; do
    module="${module%,}"
    module="${module%% as *}"
    module="${module%% import *}"
    if [[ "${module}" == people_counter.* ]]; then
      module_path="src/${module//./\/}.py"
      [[ -f "${module_path}" ]] && targets+=("${module}*")
    elif [[ "${module}" == scripts.* ]]; then
      module_path="${module//./\/}.py"
      if [[ -f "${module_path}" ]] \
        && grep -Fq "\"${module_path}\"" pyproject.toml; then
        targets+=("${module}*")
      fi
    fi
  done < <(
    "${test_source[@]}" | sed -nE \
      -e 's/^from ((people_counter|scripts)\.[A-Za-z0-9_.]+) import .*/\1/p' \
      -e 's/^import ((people_counter|scripts)\.[A-Za-z0-9_.]+).*/\1/p'
  )
done

if ((${#targets[@]} > 0)); then
  mapfile -t targets < <(printf '%s\n' "${targets[@]}" | sort -u)
fi
printf '%s\n' "${targets[@]}" > "${evidence_dir}/targets.txt"
if ((${#targets[@]} == 0)); then
  echo "::error::Changed tests did not map to a configured mutation target."
  printf '%s\n' \
    '{"changedTests":1,"targets":0,"status":"unmapped"}' \
    > "${evidence_dir}/policy.json"
  exit 1
fi

set -o pipefail
if ((${#active_tests[@]} > 0)); then
  uv run pytest "${active_tests[@]}" \
    --cov=people_counter --cov-branch --cov-report= --crap \
    2>&1 | tee "${evidence_dir}/baseline-pytest-crap.txt"
else
  uv run pytest tests -m "not compose" \
    --cov=people_counter --cov-branch --cov-report= --crap \
    2>&1 | tee "${evidence_dir}/baseline-pytest-crap.txt"
fi

uv run mutmut run --max-children 4 "${targets[@]}" \
  2>&1 | tee "${evidence_dir}/mutmut-run.txt"
uv run mutmut results --all true > "${evidence_dir}/mutmut-results.txt"
uv run mutmut export-cicd-stats \
  2>&1 | tee "${evidence_dir}/mutmut-export.txt"
if [[ -f mutants/mutmut-cicd-stats.json ]]; then
  cp mutants/mutmut-cicd-stats.json "${evidence_dir}/mutmut-cicd-stats.json"
fi

selected_results="${evidence_dir}/selected-results.txt"
: > "${selected_results}"
for target in "${targets[@]}"; do
  prefix="${target%\*}"
  awk -v prefix="${prefix}" '
    {
      line = $0
      sub(/^[[:space:]]+/, "", line)
      if (index(line, prefix) == 1) {
        print line
      }
    }
  ' "${evidence_dir}/mutmut-results.txt" >> "${selected_results}"
done
sort -u -o "${selected_results}" "${selected_results}"

if [[ ! -s "${selected_results}" ]]; then
  echo "::error::mutmut produced no results for the selected targets."
  exit 1
fi
if grep -Eq ': (not checked|no tests|suspicious|timeout|segfault)$' \
  "${selected_results}"; then
  echo "::error::Mutation run was incomplete or has invalid results."
  grep -E ': (not checked|no tests|suspicious|timeout|segfault)$' \
    "${selected_results}"
  exit 1
fi

killed=$(grep -Ec ': killed$' "${selected_results}" || true)
survived=$(grep -Ec ': survived$' "${selected_results}" || true)
skipped=$(grep -Ec ': skipped$' "${selected_results}" || true)
jq -n \
  --argjson changedTests "${#changed_tests[@]}" \
  --argjson targets "${#targets[@]}" \
  --argjson killed "${killed}" \
  --argjson survived "${survived}" \
  --argjson skipped "${skipped}" \
  '{
    changedTests: $changedTests,
    targets: $targets,
    killed: $killed,
    survived: $survived,
    skipped: $skipped,
    status: "complete",
    survivorPolicy: "advisory"
  }' > "${evidence_dir}/policy.json"

if ((survived > 0)); then
  echo "::warning::${survived} selected mutants survived; inspect mutation evidence."
fi
