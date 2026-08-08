/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * testing.helpers.test.ts: Tests for the cross-cutting testing-helpers barrel. The barrel is the canonical import path for tests outside `src/testing/`
 * (enforced by the `prismcast/testing-helpers-barrel-only` ESLint rule). These tests pin the barrel's runtime export surface so a forgotten re-export, a
 * renamed submodule symbol, or an unintended addition surfaces immediately at unit-tier rather than as a confusing import error in some downstream test file.
 *
 * Three checks form a symmetric drift catch:
 *
 *   1. Every documented symbol IS re-exported (catches missing re-exports - a submodule export the barrel forgot to forward).
 *   2. No undocumented runtime exports exist (catches accidental additions - a submodule export that flowed through without being documented).
 *   3. Cardinality matches (catches same-size swap - one removed, one added would pass the first two tests by accident).
 *
 * When the barrel grows or shrinks, update the EXPECTED_*_EXPORTS lists below to match.
 */
import * as barrel from "./testing.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

/* The complete list of function runtime symbols the barrel is documented to re-export. Type-only re-exports (CapturedCdpCommand, CdpSessionListenerOp,
 * CapturedLogLine, TestLogger, FakeExecFile, FakeExecFileResult) are erased at runtime and therefore not part of this surface. Value-typed runtime exports
 * (constants like TMPDIR_PREFIX) are listed separately in EXPECTED_VALUE_EXPORTS so the function-typeof check stays clean.
 */
const EXPECTED_FUNCTION_EXPORTS = [

  "FakeCdpSession",
  "FakeConnection",
  "assertNoUnhandledRejections",
  "assertSameShape",
  "bufferOrStringToString",
  "capturingLog",
  "closePuppeteerStreamWss",
  "closePuppeteerStreamWssOnIdle",
  "declareKeysOf",
  "execFileAlwaysSucceeds",
  "execFileFromMap",
  "expectAt",
  "firstOf",
  "makeExecFileError",
  "makeFakeCdpPage",
  "noop",
  "nthOf",
  "silentLog",
  "withTempDir"
] as const;

/* Value-typed runtime exports (constants, not functions). Kept separate from EXPECTED_FUNCTION_EXPORTS so each list's typeof check is uniform.
 */
const EXPECTED_VALUE_EXPORTS: Readonly<Record<string, "boolean" | "number" | "string">> = {

  TMPDIR_PREFIX: "string"
};

describe("testing.helpers barrel", () => {

  test("re-exports every documented function symbol as a function", () => {

    // Narrowing the namespace import to a record so we can index by string. Each entry's contract is "typeof === 'function'."
    const exports = barrel as unknown as Record<string, unknown>;

    for(const name of EXPECTED_FUNCTION_EXPORTS) {

      assert.equal(typeof exports[name], "function", "Expected " + name + " to be re-exported as a function from the testing.helpers barrel.");
    }
  });

  test("re-exports every documented value symbol with the expected primitive type", () => {

    // Value exports check the primitive type rather than identity so TMPDIR_PREFIX's actual content can vary per project without breaking this test.
    const exports = barrel as unknown as Record<string, unknown>;

    for(const [ name, expectedType ] of Object.entries(EXPECTED_VALUE_EXPORTS)) {

      assert.equal(typeof exports[name], expectedType,
        "Expected " + name + " to be re-exported as a " + expectedType + " from the testing.helpers barrel.");
    }
  });

  test("does not leak unexpected runtime exports beyond the documented list", () => {

    /* Drift catch in the other direction: an addition to a submodule that propagates to the barrel without being added to the EXPECTED lists would survive the
     * previous tests silently. Walking the actual runtime keys and asserting each one is documented closes that gap. The barrel has no `default` export, so no
     * filtering is needed.
     */
    const allowed = new Set<string>([ ...EXPECTED_FUNCTION_EXPORTS, ...Object.keys(EXPECTED_VALUE_EXPORTS) ]);
    const actual = Object.keys(barrel);

    for(const name of actual) {

      assert.ok(allowed.has(name),
        "Unexpected runtime export from the testing.helpers barrel: " + name +
          ". Add it to EXPECTED_FUNCTION_EXPORTS or EXPECTED_VALUE_EXPORTS, or remove it from the barrel.");
    }
  });

  test("documented and actual runtime export sets match exactly in cardinality", () => {

    // Symmetric pin: the previous tests catch missing or extra exports individually; this one asserts the cardinality matches so a same-size swap (one removed,
    // one added) cannot pass both above tests by accident.
    const actual = new Set(Object.keys(barrel));
    const expected = EXPECTED_FUNCTION_EXPORTS.length + Object.keys(EXPECTED_VALUE_EXPORTS).length;

    assert.equal(actual.size, expected,
      "Runtime export count mismatch: barrel has " + String(actual.size) + " keys, EXPECTED lists total " + String(expected) + ".");
  });
});
