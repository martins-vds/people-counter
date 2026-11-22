#!/usr/bin/env bash
set -Eeuo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
env_args=()
if [[ -f .env ]]; then
  env_args=(--env-file .env)
fi
# Expand the port only after uv loads .env.
# shellcheck disable=SC2016
exec uv run "${env_args[@]}" --frozen --extra portal --extra publisher --extra demo \
  -- bash -c 'exec pc-admin --port "${PC_PORTAL_PORT:-8088}" "$@"' pc-admin "$@"
