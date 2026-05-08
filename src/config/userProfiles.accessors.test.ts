/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userProfiles.accessors.test.ts: Unit tests for the user-profile module's snapshot accessors. Pins the shallow-copy contract for getUserProfiles / getUserDomains
 * (callers can mutate the returned record without affecting module-internal state) and the boolean shape of hasProfilesParseError.
 */
import { describe, test } from "node:test";
import { getUserDomains, getUserProfiles, hasProfilesParseError } from "./userProfiles.ts";
import assert from "node:assert/strict";

describe("getUserProfiles", () => {

  test("returns a fresh object that does not leak module-internal state", () => {

    const a = getUserProfiles();
    const b = getUserProfiles();

    assert.notEqual(a, b, "two calls return distinct references");
  });

  test("mutating the returned record (adding a key) does not affect a subsequent call's result", () => {

    /* Pins the shallow-copy contract: callers can mutate the returned record freely without affecting module state. Adding a key on the first snapshot must
     * not surface in the second snapshot. A regression to a returned-by-reference implementation would fail here.
     */
    const a = getUserProfiles();

    (a as Record<string, unknown>)["__test-injected-key"] = { extends: "fullscreenApi" };

    const b = getUserProfiles();

    assert.equal("__test-injected-key" in b, false, "second snapshot does not see the injected key");
  });
});

describe("getUserDomains", () => {

  test("returns a fresh object that does not leak module-internal state", () => {

    const a = getUserDomains();
    const b = getUserDomains();

    assert.notEqual(a, b);
  });

  test("mutating the returned record (adding a domain) does not affect a subsequent call's result", () => {

    const a = getUserDomains();

    (a as Record<string, unknown>)["injected.example"] = { profile: "fullscreenApi" };

    const b = getUserDomains();

    assert.equal("injected.example" in b, false, "second snapshot does not see the injected domain");
  });
});

describe("hasProfilesParseError", () => {

  test("returns a boolean reflecting the current parse-error state", () => {

    const result = hasProfilesParseError();

    assert.equal(typeof result, "boolean", "always boolean even before initialization");
  });
});
