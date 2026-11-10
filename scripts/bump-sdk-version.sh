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

identity="src/people_counter/data/sjd_release_identity.json"
backup="$(mktemp -d)"
trap 'rm -rf "${backup}"' EXIT
cp pyproject.toml uv.lock "${identity}" "${backup}/"
cleanup() {
  local status=$?
  if ((status != 0)); then
    cp "${backup}/pyproject.toml" pyproject.toml
    cp "${backup}/uv.lock" uv.lock
    cp "${backup}/sjd_release_identity.json" "${identity}"
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
uv run --no-project python - "${version}" <<'PY'
import json
import runpy
import sys
from pathlib import Path

root = Path.cwd()
module = runpy.run_path(str(root / "src/people_counter/fabric_release_provenance.py"))
path = root / "src/people_counter/data/sjd_release_identity.json"
identity = json.loads(path.read_text(encoding="utf-8"))
identity["package_version"] = sys.argv[1]
prefix, _ = identity["release_evidence_root"].rstrip("/").rsplit("/", 1)
identity["release_evidence_root"] = f"{prefix}/{sys.argv[1]}"
identity["build_input_sha256"] = module["release_build_input_sha256"](root)
path.write_text(
    json.dumps(identity, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY
bash .github/scripts/validate-release.sh pr "releases/v${version}"
echo "SDK ${version} prepared. Commit the release files on releases/v${version}."
