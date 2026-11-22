#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
real_uv="$(command -v uv)"
temp_dir="$(mktemp -d)"
trap 'rm -rf -- "$temp_dir"' EXIT
mkdir -p "$temp_dir/bin"
mkdir -p "$temp_dir/repo/scripts"
cp "$repo_root/scripts/start-portal-local.sh" "$temp_dir/repo/scripts/"
launcher="$temp_dir/repo/scripts/start-portal-local.sh"
cat > "$temp_dir/bin/uv" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
[[ "$PWD" == "$EXPECTED_ROOT" ]]
[[ "${PC_PORTAL_ADLS_FILESYSTEM:-}" == "test-videos" ]]
printf '%s\n' "$@" > "$CAPTURE"
if [[ "${UV_EXIT_CODE:-0}" != 0 ]]; then
  exit "$UV_EXIT_CODE"
fi
while [[ "$1" != -- ]]; do
  if [[ "$1" == --env-file && -n "${FAKE_DOTENV_PORT:-}" ]]; then
    export PC_PORTAL_PORT="$FAKE_DOTENV_PORT"
  fi
  shift
done
shift
exec "$@"
EOF
cat > "$temp_dir/bin/pc-admin" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
printf '%s\n' "$@" > "$PORTAL_CAPTURE"
EOF
chmod +x "$temp_dir/bin/uv" "$temp_dir/bin/pc-admin"
export PATH="$temp_dir/bin:$PATH"
export EXPECTED_ROOT="$temp_dir/repo" CAPTURE="$temp_dir/arguments"
export PORTAL_CAPTURE="$temp_dir/portal-arguments"
export PC_PORTAL_ADLS_FILESYSTEM=test-videos
unset PC_PORTAL_PORT
cd "$temp_dir"
# shellcheck disable=SC2016
portal_command='exec pc-admin --port "${PC_PORTAL_PORT:-8088}" "$@"'

"$launcher"
printf '%s\n' run --frozen --extra portal --extra publisher --extra demo \
  -- bash -c "$portal_command" \
  pc-admin > "$temp_dir/expected"
diff -u "$temp_dir/expected" "$CAPTURE"
printf '%s\n' --port 8088 > "$temp_dir/expected"
diff -u "$temp_dir/expected" "$PORTAL_CAPTURE"

PC_PORTAL_PORT=8090 "$launcher" \
  --port 8091 --state-dir "$temp_dir/state with spaces"
printf '%s\n' run --frozen --extra portal --extra publisher --extra demo \
  -- bash -c "$portal_command" \
  pc-admin --port 8091 --state-dir "$temp_dir/state with spaces" \
  > "$temp_dir/expected"
diff -u "$temp_dir/expected" "$CAPTURE"
printf '%s\n' --port 8090 --port 8091 --state-dir "$temp_dir/state with spaces" \
  > "$temp_dir/expected"
diff -u "$temp_dir/expected" "$PORTAL_CAPTURE"

printf 'PC_PORTAL_ADLS_FILESYSTEM=dotenv-videos\nPC_PORTAL_PORT=8092\n' \
  > "$temp_dir/repo/.env"
FAKE_DOTENV_PORT=8092 "$launcher"
printf '%s\n' run --env-file .env --frozen --extra portal --extra publisher \
  --extra demo -- bash -c "$portal_command" \
  pc-admin > "$temp_dir/expected"
diff -u "$temp_dir/expected" "$CAPTURE"
printf '%s\n' --port 8092 > "$temp_dir/expected"
diff -u "$temp_dir/expected" "$PORTAL_CAPTURE"

if UV_EXIT_CODE=42 "$launcher"; then
  printf 'Expected the launcher to propagate uv failure\n' >&2
  exit 1
else
  [[ "$?" == 42 ]]
fi

# shellcheck disable=SC2016
env -u PC_PORTAL_ADLS_FILESYSTEM "$real_uv" run --no-project \
  --env-file "$temp_dir/repo/.env" -- bash -c \
  '[[ "$PC_PORTAL_ADLS_FILESYSTEM" == dotenv-videos && "$PC_PORTAL_PORT" == 8092 ]]'
# shellcheck disable=SC2016
PC_PORTAL_PORT=8093 "$real_uv" run --no-project \
  --env-file "$temp_dir/repo/.env" -- bash -c \
  '[[ "$PC_PORTAL_ADLS_FILESYSTEM" == test-videos && "$PC_PORTAL_PORT" == 8093 ]]'
printf 'Portal local launcher tests passed\n'
