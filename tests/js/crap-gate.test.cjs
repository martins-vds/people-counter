/** @jest-environment node */
"use strict";

const { checkReport } = require("../../scripts/check-portal-crap.cjs");

function report(score) {
  return {
    "portal.js": {
      update: {
        functionDescriptor: "update",
        start: { line: 12, column: 0 },
        statements: { crap: score },
      },
    },
  };
}

test("accepts scores strictly below ten and returns the maximum", () => {
  expect(checkReport(report(9.999))).toBe(9.999);
});

test.each([10, 10.001, Infinity, NaN, undefined])("rejects invalid or excessive score %s", (score) => {
  expect(() => checkReport(report(score))).toThrow(/portal.js:12: update: CRAP .* must be < 10/);
});

test("rejects an empty report or a file without measured functions", () => {
  expect(() => checkReport({})).toThrow("no source files");
  expect(() => checkReport({ "portal.js": {} })).toThrow("no functions");
});
