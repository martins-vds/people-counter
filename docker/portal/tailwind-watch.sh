#!/bin/sh
set -eu

fingerprint() {
  {
    find /source/portal_styles /source/portal_templates -type f -exec sha256sum {} +
    sha256sum /source/portal_static/portal.js
  } | sort | sha256sum
}

rm -f /tmp/tailwind-ready
previous="$(fingerprint)"
tailwindcss -i /source/portal_styles/input.css -o /output/portal.css --minify
touch /tmp/tailwind-ready

# Docker bind mounts can drop native watch events; only this small UI source set is polled.
while sleep 1; do
  current="$(fingerprint)"
  if [ "$current" != "$previous" ]; then
    tailwindcss -i /source/portal_styles/input.css -o /output/portal.css --minify
    previous="$current"
  fi
done
