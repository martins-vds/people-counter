"use strict";

const { createRequire } = require("node:module");
const merge = createRequire(require.resolve("jest-config"))("deepmerge");

describe("patched deepmerge dependency", () => {
  test.each(["target", "source"])("rejects prototype poisoning through the %s", (side) => {
    const malicious = JSON.parse('{"__proto__":{"injected":true},"safe":1}');
    const target = side === "target" ? malicious : Object.create(null);
    const source = side === "source" ? malicious : {};

    const result = merge(target, source);

    expect(Object.getPrototypeOf(result)).toBe(Object.prototype);
    expect(result.injected).toBeUndefined();
    expect(Object.hasOwn(result, "__proto__")).toBe(false);
    expect(result.safe).toBe(1);
  });

  test("preserves nested cloning, array concatenation, and custom array merging", () => {
    const first = { nested: { left: 1 }, values: [{ value: 1 }] };
    const second = { nested: { right: 2 }, values: [{ value: 2 }] };

    const result = merge.all([first, second]);

    expect(result).toEqual({
      nested: { left: 1, right: 2 },
      values: [{ value: 1 }, { value: 2 }],
    });
    expect(result.values[0]).not.toBe(first.values[0]);
    expect(merge(first, second, { arrayMerge: (_target, source) => source }).values)
      .toBe(second.values);
  });
});
