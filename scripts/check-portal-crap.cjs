"use strict";

const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");

function checkReport(report) {
  const files = Object.entries(report);
  assert.ok(files.length > 0, "CRAP report contains no source files");
  let maximum = 0;
  for (const [file, functions] of files) {
    const entries = Object.values(functions);
    assert.ok(entries.length > 0, `${file}: CRAP report contains no functions`);
    for (const entry of entries) {
      const score = entry.statements.crap;
      assert.ok(Number.isFinite(score) && score < 10,
        `${file}:${entry.start.line}: ${entry.functionDescriptor}: CRAP ${score} must be < 10`);
      maximum = Math.max(maximum, score);
    }
  }
  return maximum;
}

if (require.main === module) {
  const report = JSON.parse(readFileSync("evidence/portal-js/crap-report.json", "utf8"));
  console.log(`Portal JavaScript maximum CRAP: ${checkReport(report).toFixed(3)} (< 10 required)`);
}

module.exports = { checkReport };
