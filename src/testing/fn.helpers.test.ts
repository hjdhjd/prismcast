/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fn.helpers.test.ts: Unit tests for function-shaped test fixtures.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { noop } from "./fn.helpers.ts";

describe("noop", () => {

  test("is invocable as a side-effect-free no-op without throwing", () => {

    // Behavioral contract: noop can be called with no arguments and produces no observable effect, no thrown error, no return value worth inspecting (the
    // signature is void). Tests pass noop into callback sites that erase to () => void and the runtime expectation is "invoke, do nothing, return cleanly."
    assert.doesNotThrow(() => { noop(); });
  });

  test("takes no arguments declared in the signature", () => {

    // Locking the zero-arg signature is what allows noop to satisfy any callback type erased to () => void at the call site. If someone later widens the
    // signature, every existing consumer that aliased noop to a typed callback would still compile but the change would surface here as a documentation drift.
    assert.equal(noop.length, 0, "noop declares zero parameters");
  });
});
