#!/usr/bin/env bash
set -euo pipefail

mode="${1:?Validation mode is required}"
branch="${2:?Release branch is required}"
source_commit="${3:-}"

if [[ "${mode}" != pr && "${mode}" != release ]]; then
  echo "::error::Unknown release validation mode: ${mode}" >&2
  exit 1
fi
if [[ ! "${branch}" =~ ^releases/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "::error::Release branch must be releases/vMAJOR.MINOR.PATCH" >&2
  exit 1
fi

tag="${branch#releases/}"
version="$(uv version --short)"
if [[ "${tag}" != "v${version}" ]]; then
  echo "::error::Branch ${branch} does not match project version ${version}. Update pyproject.toml and uv.lock before merging." >&2
  exit 1
fi
if [[ "${mode}" == release ]]; then
  source_commit="$(git rev-parse --verify "${source_commit:?Source commit is required}^{commit}")"
fi
if git show-ref --verify --quiet "refs/tags/${tag}"; then
  if [[ "${mode}" == pr ]]; then
    echo "::error::Tag ${tag} already exists. Choose a new release version." >&2
    exit 1
  fi
  tagged_commit="$(git rev-parse --verify "refs/tags/${tag}^{commit}")"
  if [[ "${tagged_commit}" != "${source_commit}" ]]; then
    echo "::error::Tag ${tag} already points to a different commit; it will not be overwritten." >&2
    exit 1
  fi
fi
if ! uv lock --check; then
  echo "::error::uv.lock is out of date. Run uv lock and commit it with the version change." >&2
  exit 1
fi
if ! uv run --no-project python src/people_counter/fabric_release_provenance.py "${version}"; then
  echo "::error::Release identity is out of date. Update its version, evidence root, and build-input digest before merging." >&2
  exit 1
fi
{
  echo "tag=${tag}"
  echo "version=${version}"
} >> "${GITHUB_OUTPUT:-/dev/stdout}"
