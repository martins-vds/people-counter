#!/usr/bin/env bash
set -euo pipefail

scripts="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
temp="$(mktemp -d)"
trap 'rm -rf "${temp}"' EXIT
cd "${temp}"
git init --quiet
git config user.name "Release test"
git config user.email "release-test@example.invalid"
printf '[project]\nname = "release-test"\nversion = "1.2.3"\n' > pyproject.toml
git add pyproject.toml
git commit --quiet -m "Release fixture"
commit="$(git rev-parse HEAD)"
export GITHUB_OUTPUT="${temp}/output"

expect_failure() {
  local message="$1"
  shift
  if "$@" > "${temp}/error" 2>&1; then
    echo "Expected failure: ${message}" >&2
    exit 1
  fi
  grep -F "${message}" "${temp}/error"
}

bash "${scripts}/validate-release.sh" pr releases/v1.2.3
grep -Fx "tag=v1.2.3" "${GITHUB_OUTPUT}"
grep -Fx "version=1.2.3" "${GITHUB_OUTPUT}"
expect_failure "does not match" \
  bash "${scripts}/validate-release.sh" pr releases/v1.2.4
for branch in releases/v1.2 releases/v01.2.3 releases/v1.2.3-rc1 feature/v1.2.3; do
  expect_failure "releases/vMAJOR.MINOR.PATCH" \
    bash "${scripts}/validate-release.sh" pr "${branch}"
done
expect_failure "Unknown release validation mode" \
  bash "${scripts}/validate-release.sh" typo releases/v1.2.3
bash "${scripts}/validate-release.sh" release releases/v1.2.3 "${commit}"
git tag -a v1.2.3 -m "Existing annotated release tag"
expect_failure "already exists" \
  bash "${scripts}/validate-release.sh" pr releases/v1.2.3
bash "${scripts}/validate-release.sh" release releases/v1.2.3 "${commit}"
git commit --quiet --allow-empty -m "Different source"
expect_failure "different commit" \
  bash "${scripts}/validate-release.sh" release releases/v1.2.3 "$(git rev-parse HEAD)"

mkdir bin assets
cat > bin/gh <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${GH_CALLS}"
case "$*" in
  "api repos/example/repo/commits/v1.2.3 --jq .sha")
    printf '%s\n' "${REMOTE_COMMIT}"
    ;;
  "api --paginate repos/example/repo/releases "*)
    if [[ "${FAIL_API:-false}" == true ]]; then
      echo "Release API failed" >&2
      exit 1
    fi
    if [[ "${RELEASE_STATE}" != absent ]]; then
      printf '%s\n' "${RELEASE_STATE}"
    fi
    ;;
  "release view v1.2.3 --json assets "*)
    printf '%s\n' "${EXISTING_ASSETS}"
    ;;
  "release create "*|"release upload "*|"release edit "*)
    if [[ "${FAIL_UPLOAD:-false}" == true && "$2" == upload ]]; then
      echo "Upload failed" >&2
      exit 1
    fi
    ;;
  *)
    echo "Unexpected gh command: $*" >&2
    exit 1
    ;;
esac
MOCK
chmod +x bin/gh
export PATH="${temp}/bin:${PATH}"
export GH_CALLS="${temp}/gh-calls"
export GH_REPO=example/repo RELEASE_TAG=v1.2.3 SDK_VERSION=1.2.3
export SOURCE_COMMIT="${commit}" REMOTE_COMMIT="${commit}"
export EXISTING_ASSETS=$'people-counter-1.2.3-cpu-linux-amd64-cp313.zip\npeople-counter-1.2.3-cpu-linux-arm64-cp313.zip\npeople-counter-1.2.3-models.zip\nSHA256SUMS'
cd assets
for asset in people-counter-1.2.3-cpu-linux-{amd64,arm64}-cp313.zip people-counter-1.2.3-models.zip; do
  printf 'fixture\n' > "${asset}"
done
export RELEASE_STATE=absent
bash "${scripts}/publish-sdk-release.sh"
sha256sum --check SHA256SUMS
grep -F "release create v1.2.3" "${GH_CALLS}"
grep -F "release upload v1.2.3" "${GH_CALLS}"
grep -F "release edit v1.2.3 --draft=false" "${GH_CALLS}"

export RELEASE_STATE=true
: > "${GH_CALLS}"
bash "${scripts}/publish-sdk-release.sh"
if grep -F "release create" "${GH_CALLS}"; then
  echo "A retry must reuse the existing draft" >&2
  exit 1
fi
grep -F "release edit v1.2.3 --draft=false" "${GH_CALLS}"

export RELEASE_STATE=false
: > "${GH_CALLS}"
bash "${scripts}/publish-sdk-release.sh"
if grep -E "release (create|upload|edit)" "${GH_CALLS}"; then
  echo "A published release must not be changed" >&2
  exit 1
fi
export EXISTING_ASSETS=SHA256SUMS
expect_failure "missing asset" bash "${scripts}/publish-sdk-release.sh"
export REMOTE_COMMIT=wrong
expect_failure "tag moved" bash "${scripts}/publish-sdk-release.sh"
export REMOTE_COMMIT="${commit}" RELEASE_STATE=true FAIL_UPLOAD=true
expect_failure "Upload failed" bash "${scripts}/publish-sdk-release.sh"
if tail -n 1 "${GH_CALLS}" | grep -F "release edit"; then
  echo "An upload failure must not publish the draft" >&2
  exit 1
fi
unset FAIL_UPLOAD
export FAIL_API=true
: > "${GH_CALLS}"
expect_failure "Release API failed" bash "${scripts}/publish-sdk-release.sh"
if grep -E "release (create|upload|edit)" "${GH_CALLS}"; then
  echo "An API failure must not create or publish a release" >&2
  exit 1
fi
unset FAIL_API
rm people-counter-1.2.3-models.zip
expect_failure "Missing release asset" bash "${scripts}/publish-sdk-release.sh"
echo "Release validation and publication tests passed."
