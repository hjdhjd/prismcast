/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * never.test.ts: Unit tests for the exhaustiveness guard. The guard's real work happens at compile time - a switch that forgets a union member fails the build -
 * and the type checker is what proves that half, not a test. What a test can reach is the runtime half: a value that entered from outside the type system arrives
 * at the default arm, and the guard reports it instead of letting the switch fall through as though nothing had happened.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { assertNever } from "./never.ts";

describe("assertNever", () => {

  test("throws and carries the value that reached it", () => {

    /* The cast is the point of the row rather than a convenience: it stands in for the values the compile-time proof cannot cover - parsed JSON, a wire payload,
     * a hand-edited configuration file - which are exactly the ones that arrive as a member nobody wrote an arm for.
     */
    assert.throws(() => assertNever({ kind: "unexpected" } as never), /Unhandled value/, "the guard reports rather than returning");
    assert.throws(() => assertNever({ kind: "unexpected" } as never), /"kind":"unexpected"/, "and the message carries the value, so the caller can be found");
  });

  test("renders a primitive as legibly as an object", () => {

    // The message is only ever read in a stack trace, so both shapes have to survive the trip: a string keeps its quotes and a number stays bare.
    assert.throws(() => assertNever("stray" as never), /"stray"/, "a string arrives quoted");
    assert.throws(() => assertNever(7 as never), /: 7\./, "and a number arrives bare, with the sentence's own period after it");
  });
});
