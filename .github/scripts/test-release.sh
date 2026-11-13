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
printf 'Release fixture\n' > README.md
uv lock --offline
git add pyproject.toml uv.lock README.md
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

# Exercise the workflow's trust check before any selected release code runs.
awk '
  /      - name: Resolve immutable release source/ { source = 1; next }
  source && /        run: \|/ { script = 1; next }
  script && /^      - / { exit }
  script { sub(/^          /, ""); print }
' "${scripts}/../workflows/release.yml" > "${temp}/resolve-release-source.sh"
git update-ref refs/remotes/origin/main "${commit}"
unmerged="$(printf 'Unmerged release\n' | git commit-tree "HEAD^{tree}" -p HEAD)"
git tag v9.0.0 "${unmerged}"
expect_failure "is not part of main" \
  env EVENT_NAME=workflow_dispatch RELEASE_TAG=v9.0.0 \
  bash -e -o pipefail "${temp}/resolve-release-source.sh"
test ! -s "${GITHUB_OUTPUT}"
git tag v1.2.3 "${commit}"
env EVENT_NAME=workflow_dispatch RELEASE_TAG=v1.2.3 \
  bash -e -o pipefail "${temp}/resolve-release-source.sh"
grep -Fx "commit=${commit}" "${GITHUB_OUTPUT}"
grep -Fx "tag=v1.2.3" "${GITHUB_OUTPUT}"
: > "${GITHUB_OUTPUT}"
git update-ref refs/remotes/origin/main "${unmerged}"
env EVENT_NAME=workflow_dispatch RELEASE_TAG=v1.2.3 \
  bash -e -o pipefail "${temp}/resolve-release-source.sh"
grep -Fx "commit=${commit}" "${GITHUB_OUTPUT}"
: > "${GITHUB_OUTPUT}"
env EVENT_NAME=pull_request MERGE_COMMIT="${unmerged}" RELEASE_BRANCH=releases/v9.0.0 \
  bash -e -o pipefail "${temp}/resolve-release-source.sh"
grep -Fx "commit=${unmerged}" "${GITHUB_OUTPUT}"
git update-ref -d refs/remotes/origin/main
: > "${GITHUB_OUTPUT}"
expect_failure "is not part of main" \
  env EVENT_NAME=workflow_dispatch RELEASE_TAG=v1.2.3 \
  bash -e -o pipefail "${temp}/resolve-release-source.sh"
test ! -s "${GITHUB_OUTPUT}"
git tag -d v1.2.3 v9.0.0

mkdir -p scripts .github/scripts original-release
cp "${scripts}/../../scripts/bump-sdk-version.sh" scripts/
cp "${scripts}/validate-release.sh" .github/scripts/
cp pyproject.toml uv.lock original-release/
release_files=(pyproject.toml uv.lock)
sha256sum "${release_files[@]}" > "${temp}/original-release.sha256"
expect_failure "Usage:" bash scripts/bump-sdk-version.sh
for version in '' v1.2.4 1.2 01.2.4 1.2.4-rc1 '1.2.4 '; do
  expect_failure "MAJOR.MINOR.PATCH" bash scripts/bump-sdk-version.sh "${version}"
  expect_failure "MAJOR.MINOR.PATCH" bash scripts/bump-sdk-version.sh --patch "${version}"
done
expect_failure "Usage:" bash scripts/bump-sdk-version.sh 1.2.4 extra
for arguments in '--major --minor' '--minor --patch' '--major --patch' '--patch --patch' '--unknown'; do
  read -r -a args <<< "${arguments}"
  expect_failure "Usage:" bash scripts/bump-sdk-version.sh "${args[@]}"
done
sha256sum --check "${temp}/original-release.sha256"
git tag v1.2.4
expect_failure "already exists" bash scripts/bump-sdk-version.sh 1.2.4
expect_failure "already exists" bash scripts/bump-sdk-version.sh --patch
sha256sum --check "${temp}/original-release.sha256"
git tag -d v1.2.4

for bump in major minor patch; do
  case "${bump}" in
    major) expected=2.0.0 ;;
    minor) expected=1.3.0 ;;
    patch) expected=1.2.4 ;;
  esac
  cp original-release/pyproject.toml original-release/uv.lock .
  (cd /; bash "${temp}/scripts/bump-sdk-version.sh" "--${bump}")
  [[ "$(uv version --short)" == "${expected}" ]]
  bash "${scripts}/validate-release.sh" pr "releases/v${expected}"
  for args in "--${bump} 3.4.5" "3.4.5 --${bump}"; do
    read -r -a arguments <<< "${args}"
    bash scripts/bump-sdk-version.sh "${arguments[@]}"
    [[ "$(uv version --short)" == 3.4.5 ]]
    bash "${scripts}/validate-release.sh" pr releases/v3.4.5
  done
done

# Exercise the command from outside the repository, without SDK dependencies.
(cd /; bash "${temp}/scripts/bump-sdk-version.sh" 1.2.4)
[[ "$(uv version --short)" == 1.2.4 ]]
bash "${scripts}/validate-release.sh" pr releases/v1.2.4
sha256sum "${release_files[@]}" > "${temp}/bumped-release.sha256"
bash scripts/bump-sdk-version.sh 1.2.4
sha256sum --check "${temp}/bumped-release.sha256"
printf 'Changed release fixture\n' > README.md
bash scripts/bump-sdk-version.sh 1.2.4
bash "${scripts}/validate-release.sh" pr releases/v1.2.4

mkdir failing-uv
real_uv="$(command -v uv)"
cat > failing-uv/uv <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == lock ]]; then
  echo "Lock resolution failed" >&2
  exit 23
fi
exec "${REAL_UV}" "$@"
MOCK
chmod +x failing-uv/uv
sha256sum "${release_files[@]}" > "${temp}/before-failure.sha256"
expect_failure "Lock resolution failed" \
  env REAL_UV="${real_uv}" PATH="${temp}/failing-uv:${PATH}" \
  bash scripts/bump-sdk-version.sh 1.2.5
expect_failure "Lock resolution failed" \
  env REAL_UV="${real_uv}" PATH="${temp}/failing-uv:${PATH}" \
  bash scripts/bump-sdk-version.sh --patch
sha256sum --check "${temp}/before-failure.sha256"

cat > .github/scripts/validate-release.sh <<'MOCK'
#!/usr/bin/env bash
echo "Final release validation failed" >&2
exit 42
MOCK
expect_failure "Final release validation failed" bash scripts/bump-sdk-version.sh 1.2.5
sha256sum --check "${temp}/before-failure.sha256"
cp "${scripts}/validate-release.sh" .github/scripts/

cp original-release/pyproject.toml pyproject.toml
cp original-release/uv.lock uv.lock
printf 'Release fixture\n' > README.md
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
uv lock --offline
printf '[project]\nname = "release-test"\nversion = "1.2.4"\n' > pyproject.toml
expect_failure "uv.lock is out of date" \
  bash "${scripts}/validate-release.sh" pr releases/v1.2.4
printf '[project]\nname = "release-test"\nversion = "1.2.3"\n' > pyproject.toml
mv uv.lock saved.lock
expect_failure "uv.lock is out of date" \
  bash "${scripts}/validate-release.sh" pr releases/v1.2.3
mv saved.lock uv.lock
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
