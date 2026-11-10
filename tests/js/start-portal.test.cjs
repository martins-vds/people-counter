/** @jest-environment node */
"use strict";

const { spawnSync } = require("node:child_process");
const { mkdtempSync, readFileSync, rmSync, writeFileSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join, resolve } = require("node:path");

const root = resolve(__dirname, "../..");
let temporary;

beforeEach(() => {
  temporary = mkdtempSync(join(tmpdir(), "portal-start-"));
  writeFileSync(join(temporary, "docker"), `#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$PWD" "$@" "\${PC_PORTAL_PORT}" > "\${PORTAL_LAUNCH_LOG}"
exit "\${DOCKER_EXIT_CODE}"
`, { mode: 0o755 });
});

afterEach(() => {
  rmSync(temporary, { recursive: true, force: true });
});

test.each([0, 42])("starts only the portal from any directory and preserves Compose exit status %s", (status) => {
  const log = join(temporary, "docker-call");
  const result = spawnSync(join(root, "scripts/start-portal.sh"), {
    cwd: temporary,
    env: {
      ...process.env,
      PATH: `${temporary}:${process.env.PATH}`,
      PC_PORTAL_PORT: "18088",
      PORTAL_LAUNCH_LOG: log,
      DOCKER_EXIT_CODE: String(status),
    },
    encoding: "utf8",
  });
  expect(result.error).toBeUndefined();
  expect(result.status).toBe(status);
  expect(readFileSync(log, "utf8").trim().split("\n")).toEqual([
    root, "compose", "--profile", "portal", "up", "--build", "--wait", "portal", "18088",
  ]);
});
