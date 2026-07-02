/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * testing.helpers.test.ts: Tests for the cross-cutting testing-helpers barrel. The barrel is the canonical import path for tests outside `src/testing/` (enforced
 * by the `prismcast/testing-helpers-barrel-only` ESLint rule); these tests pin the barrel's runtime export surface so a forgotten re-export, a renamed submodule
 * symbol, or an unintended addition surfaces immediately at unit-tier rather than as a confusing import error in some downstream test file.
 */
import * as barrel from "./testing.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

/* The complete list of runtime symbols the barrel is documented to re-export. Type-only re-exports (CapturedCdpCommand, CdpSessionListenerOp, CapturedLogLine,
 * TestLogger, FakeExecFile, FakeExecFileResult) are erased at runtime and therefore not part of this surface. Update this list when the barrel grows or shrinks;
 * a mismatch with the actual exports fails the drift-check test below.
 */
const EXPECTED_RUNTIME_EXPORTS = [

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

describe("testing.helpers barrel", () => {

  test("re-exports every documented runtime symbol as a function", () => {

    // Narrowing the namespace import to a record so we can index by string. Each documented symbol is a function (factory or assertion utility); none are values
    // or constants, which keeps the assertion uniform.
    const exports = barrel as unknown as Record<string, unknown>;

    for(const name of EXPECTED_RUNTIME_EXPORTS) {

      assert.equal(typeof exports[name], "function", "Expected " + name + " to be re-exported as a function from the testing.helpers barrel.");
    }
  });

  test("does not leak unexpected runtime exports beyond the documented list", () => {

    /* Drift catch in the other direction: an addition to a submodule that propagates to the barrel without being added to EXPECTED_RUNTIME_EXPORTS would survive
     * the previous test silently. Walking the actual runtime keys and asserting each one is documented closes that gap. The barrel has no `default` export, so no
     * filtering is needed.
     */
    const allowed = new Set<string>(EXPECTED_RUNTIME_EXPORTS);
    const actual = Object.keys(barrel);

    for(const name of actual) {

      assert.ok(allowed.has(name),
        "Unexpected runtime export from the testing.helpers barrel: " + name + ". Add it to EXPECTED_RUNTIME_EXPORTS or remove it from the barrel.");
    }
  });

  test("documented and actual runtime export sets match exactly", () => {

    // Symmetric pin: the previous two tests catch missing or extra exports individually; this one asserts the cardinality matches so a same-size swap (one removed,
    // one added) cannot pass both above tests by accident.
    const actual = new Set(Object.keys(barrel));

    assert.equal(actual.size, EXPECTED_RUNTIME_EXPORTS.length,
      "Runtime export count mismatch: barrel has " + String(actual.size) + " keys, EXPECTED_RUNTIME_EXPORTS lists " + String(EXPECTED_RUNTIME_EXPORTS.length) + ".");
  });
});
