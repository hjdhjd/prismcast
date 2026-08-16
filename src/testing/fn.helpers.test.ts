/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fn.helpers.test.ts: Unit tests for function-shaped test fixtures.
 */
import { describe, test } from "node:test";
import { flushMicrotasks, noop } from "./fn.helpers.ts";
import assert from "node:assert/strict";

describe("noop", () => {

  test("is invocable as a side-effect-free no-op without throwing", () => {

    // Behavioral contract: noop can be called with no arguments and produces no observable effect, no thrown error, no return value worth inspecting (the
    // signature is void). Tests pass noop into callback sites that erase to () => void and the runtime expectation is "invoke, do nothing, return cleanly."
    assert.doesNotThrow(() => { noop(); });
  });

  test("takes no arguments declared in the signature", () => {

    // Locking the zero-arg signature is what allows noop to satisfy any callback type erased to () => void at the call site. If someone later widens the
    // signature, every existing consumer that aliased noop to a typed callback would still compile, but the new parameter makes noop.length non-zero and this
    // assertion fails outright, a rule this test enforces rather than letting the signature widening pass through silently.
    assert.equal(noop.length, 0, "noop declares zero parameters");
  });
});

describe("flushMicrotasks", () => {

  test("yields enough turns for a chained settlement to travel the whole chain", async () => {

    // The reason the helper exists: a settlement that crosses several promise links needs one microtask turn per link. Four links here stands in for the
    // wrapper-catch-finally chains real production code builds, and a single await would only advance the first.
    let depth = 0;

    void Promise.resolve().then(() => { depth = 1; }).then(() => { depth = 2; }).then(() => { depth = 3; }).then(() => { depth = 4; });

    await flushMicrotasks();

    assert.equal(depth, 4, "the whole chain settled");
  });

  test("yields exactly the requested number of turns", async () => {

    // Boundary: with one turn requested, only the first link of the chain can have run. This is what makes the default's generosity a deliberate choice rather
    // than an accident of implementation.
    let depth = 0;

    void Promise.resolve().then(() => { depth = 1; }).then(() => { depth = 2; }).then(() => { depth = 3; });

    await flushMicrotasks(1);

    assert.ok(depth < 3, "a single turn does not settle a three-link chain");

    await flushMicrotasks();

    assert.equal(depth, 3, "the default settles it");
  });

  test("is safe with a zero count", async () => {

    await assert.doesNotReject(flushMicrotasks(0));
  });
});
