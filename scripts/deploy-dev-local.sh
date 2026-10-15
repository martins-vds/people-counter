#!/usr/bin/env bash
set -Eeuo pipefail

exec "$(dirname "${BASH_SOURCE[0]}")/deploy-local.sh" --environment dev "$@"
