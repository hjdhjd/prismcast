/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * selection.test.ts: Unit tests for the runtime type guard exported from selection.ts. The module is mostly type-only, but isChannelSelectionProfile is a
 * runtime function: a predicate that proves channelSelector is a non-empty string and narrows ResolvedSiteProfile to ChannelSelectionProfile. The tests pin
 * the boundary cases (null, empty string, non-empty string) and exercise the type-narrowing behavior of the guard.
 */
import type { ChannelSelectionProfile, ResolvedSiteProfile } from "./index.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { isChannelSelectionProfile } from "./selection.ts";
import { makeProfile } from "../config/profiles.helpers.ts";

/* makeProfile builds a ResolvedSiteProfile literal with sensible defaults so each test can override only the field it cares about. The resolver in
 * config/profiles.ts produces values of this shape from SiteProfile inheritance chains; we mirror that contract here without hitting the resolver itself.
 */

describe("isChannelSelectionProfile", () => {

  test("returns true for a profile with a non-empty channelSelector", () => {

    // The happy path: a profile with channelSelector set to a real string is a valid ChannelSelectionProfile and the guard should pass.
    const profile = makeProfile({ channelSelector: "espn" });

    assert.equal(isChannelSelectionProfile(profile), true, "non-empty selector should pass");
  });

  test("returns false when channelSelector is null", () => {

    // The default state for non-multi-channel sites: channelSelector is null. The guard must reject this so the coordinator does not dispatch to a strategy
    // that would dereference null.
    const profile = makeProfile({ channelSelector: null });

    assert.equal(isChannelSelectionProfile(profile), false, "null selector should fail");
  });

  test("returns false for an empty-string channelSelector (boundary)", () => {

    // The empty string is a JSON serialization edge case and a common downstream of "user cleared this field." The guard treats it the same as null - both
    // mean "no selector configured" - so an empty string must fail the guard.
    const profile = makeProfile({ channelSelector: "" });

    assert.equal(isChannelSelectionProfile(profile), false, "empty-string selector should fail");
  });

  test("returns true for a single-character channelSelector (smallest non-empty)", () => {

    // Boundary: a one-character selector is the smallest non-empty string. The length > 0 check should accept it.
    const profile = makeProfile({ channelSelector: "x" });

    assert.equal(isChannelSelectionProfile(profile), true, "single-character selector should pass");
  });

  test("narrows the profile type when the guard returns true", () => {

    // The type-level contract: when the guard returns true, the profile is narrowed to ChannelSelectionProfile and channelSelector is `string` (not
    // `Nullable<string>`). Inside the `if` branch, we can use the field as a string without a non-null assertion.
    const profile: ResolvedSiteProfile = makeProfile({ channelSelector: "fox" });

    if(isChannelSelectionProfile(profile)) {

      // After narrowing, profile is ChannelSelectionProfile. We assign to a ChannelSelectionProfile binding to prove the narrowing without a cast.
      const narrowed: ChannelSelectionProfile = profile;

      assert.equal(narrowed.channelSelector.length, 3, "narrowed selector is a definite string with a length");
      assert.equal(narrowed.channelSelector, "fox");
    } else {

      assert.fail("the guard should have admitted a non-empty selector");
    }
  });

  test("does not throw when given a profile with all-default fields", () => {

    // Defensive: the guard must complete cleanly on a fully-default profile (channelSelector: null). It must not depend on any other field being non-null.
    const profile = makeProfile();

    assert.doesNotThrow(() => isChannelSelectionProfile(profile));
    assert.equal(isChannelSelectionProfile(profile), false, "default profile fails the guard");
  });
});
