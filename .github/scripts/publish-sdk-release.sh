#!/usr/bin/env bash
set -euo pipefail

: "${GH_REPO:?Repository is required}"
: "${RELEASE_TAG:?Release tag is required}"
: "${SOURCE_COMMIT:?Source commit is required}"
: "${SDK_VERSION:?SDK version is required}"
if [[ ! "${RELEASE_TAG}" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ || "${RELEASE_TAG}" != "v${SDK_VERSION}" ]]; then
  echo "::error::Release tag must match the stable SDK version" >&2
  exit 1
fi
remote_commit="$(gh api "repos/${GH_REPO}/commits/${RELEASE_TAG}" --jq .sha)"
if [[ "${remote_commit}" != "${SOURCE_COMMIT}" ]]; then
  echo "::error::Release tag moved after the build started" >&2
  exit 1
fi
assets=(
  "people-counter-${SDK_VERSION}-cpu-linux-amd64-cp313.zip"
  "people-counter-${SDK_VERSION}-cpu-linux-arm64-cp313.zip"
  "people-counter-${SDK_VERSION}-models.zip"
)
for asset in "${assets[@]}"; do
  if [[ ! -s "${asset}" ]]; then
    echo "::error::Missing release asset: ${asset}" >&2
    exit 1
  fi
done
sha256sum "${assets[@]}" > SHA256SUMS
sha256sum --check SHA256SUMS

# Listing distinguishes a missing release from an API or authentication failure.
state="$(gh api --paginate "repos/${GH_REPO}/releases" \
  --jq ".[] | select(.tag_name == \"${RELEASE_TAG}\") | .draft")"
case "${state}" in
  false)
    existing="$(gh release view "${RELEASE_TAG}" --json assets --jq '.assets[].name')"
    for asset in "${assets[@]}" SHA256SUMS; do
      if ! grep -Fxq "${asset}" <<< "${existing}"; then
        echo "::error::Published release ${RELEASE_TAG} has a missing asset: ${asset}. It will not be overwritten." >&2
        exit 1
      fi
    done
    echo "Release ${RELEASE_TAG} is already published; leaving it unchanged."
    exit 0
    ;;
  "")
    gh release create "${RELEASE_TAG}" --draft --verify-tag \
      --generate-notes --title "People Counter SDK ${RELEASE_TAG}"
    ;;
  true) ;;
  *)
    echo "::error::Unexpected release state: ${state}" >&2
    exit 1
    ;;
esac
gh release upload "${RELEASE_TAG}" "${assets[@]}" SHA256SUMS --clobber
gh release edit "${RELEASE_TAG}" --draft=false
