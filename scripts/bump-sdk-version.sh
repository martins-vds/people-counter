#!/usr/bin/env bash
set -euo pipefail

if (($# != 1)); then
  echo "Usage: bash scripts/bump-sdk-version.sh MAJOR.MINOR.PATCH" >&2
  exit 1
fi
version="$1"
if [[ ! "${version}" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "Version must be MAJOR.MINOR.PATCH without a v prefix or prerelease suffix." >&2
  exit 1
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root}"
if git show-ref --verify --quiet "refs/tags/v${version}"; then
  echo "Tag v${version} already exists. Choose a new release version." >&2
  exit 1
fi

backup="$(mktemp -d)"
trap 'rm -rf "${backup}"' EXIT
cp pyproject.toml uv.lock "${backup}/"
cleanup() {
  local status=$?
  if ((status != 0)); then
    cp "${backup}/pyproject.toml" pyproject.toml
    cp "${backup}/uv.lock" uv.lock
    echo "Version preparation failed; restored the original release files." >&2
  fi
  rm -rf "${backup}"
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

uv version --frozen "${version}"
uv lock
bash .github/scripts/validate-release.sh pr "releases/v${version}"
echo "SDK ${version} prepared. Commit the release files on releases/v${version}."
