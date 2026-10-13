#!/usr/bin/env bash
set -euo pipefail

status=0
while IFS= read -r workflow; do
  if grep -Eq '^[[:space:]]*permissions:[[:space:]]*write-all' "${workflow}"; then
    echo "${workflow}: write-all permissions are forbidden"
    status=1
  fi
  if grep -Eq '^[[:space:]]*pull_request_target:' "${workflow}"; then
    echo "${workflow}: pull_request_target is forbidden"
    status=1
  fi
  if grep -Eq 'persist-credentials:[[:space:]]*true' "${workflow}"; then
    echo "${workflow}: checkout credentials must not persist"
    status=1
  fi
  while IFS= read -r use; do
    reference="${use#*uses:}"
    reference="${reference%%#*}"
    reference="$(xargs <<<"${reference}")"
    [[ "${reference}" == ./* ]] && continue
    if [[ ! "${reference}" =~ ^[^[:space:]@]+@[0-9a-f]{40}$ ]]; then
      echo "${workflow}: action is not pinned to a full commit SHA: ${reference}"
      status=1
    fi
  done < <(grep -E '^[[:space:]]*(-[[:space:]]+)?uses:' "${workflow}" || true)
done < <(find .github/workflows -type f \( -name '*.yml' -o -name '*.yaml' \) | sort)

mapfile -t shell_scripts < <(find .github/scripts -type f -name '*.sh' | sort)
if ((${#shell_scripts[@]} > 0)); then
  shellcheck -x "${shell_scripts[@]}"
fi

if ((status != 0)); then
  exit "${status}"
fi
echo "Workflow policy passed: immutable action pins and shell scripts are valid."
