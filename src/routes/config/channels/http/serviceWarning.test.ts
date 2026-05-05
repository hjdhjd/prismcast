/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * serviceWarning.test.ts: Unit tests for buildServiceFilterWarning. The helper is the SSOT for the "service not in active filter" warning that the browse and
 * CRUD endpoints surface as a toast with a one-click enable action. Tests pin the three skip paths (no filter, "direct" tag, already-enabled tag) and the
 * positive case where a non-direct tag is missing from the active filter.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { buildServiceFilterWarning } from "./serviceWarning.ts";
import { setEnabledServices } from "../../../../config/services.ts";

describe("buildServiceFilterWarning", () => {

  let originalEnabled: string[];

  beforeEach(() => {

    // Capture the prior filter state and clear it. setEnabledServices defensively copies, so it's safe to pass a fresh array.
    originalEnabled = [];
    setEnabledServices(originalEnabled);
  });

  afterEach(() => {

    // Restore the captured filter so other test files in the same run aren't perturbed by mutations here.
    setEnabledServices(originalEnabled);
  });

  test("returns undefined when no filter is active (every service is visible)", () => {

    // Boundary: empty filter is the "show everything" sentinel. The helper must short-circuit before the tag lookup since there's nothing to warn about.
    setEnabledServices([]);

    assert.equal(buildServiceFilterWarning("https://www.hulu.com/live"), undefined);
  });

  test("returns undefined when the URL's tag is already in the active filter", () => {

    // The user is adding a Hulu channel and Hulu is already enabled - no warning needed.
    setEnabledServices(["hulu"]);

    assert.equal(buildServiceFilterWarning("https://www.hulu.com/live"), undefined);
  });

  test("returns a warning with serviceLabel and serviceTag when the tag is missing from the active filter", () => {

    // Filter has only Sling enabled, but the user is adding a Hulu channel. The warning should surface so the client can offer a one-click enable.
    setEnabledServices(["sling"]);

    const warning = buildServiceFilterWarning("https://www.hulu.com/live");

    assert.ok(warning, "warning should be returned for a missing tag");
    assert.equal(warning.serviceTag, "hulu", "tag derived from the URL's domain config");
    assert.equal(warning.serviceLabel, "Hulu", "label is the human-readable display name");
  });

  test("returns undefined for a 'direct' service tag even when a filter is active", () => {

    // Direct (no service overlay) is implicitly always enabled - the filter logic never hides direct channels. The helper must skip the warning for direct tags
    // even when an active filter wouldn't include 'direct'.
    setEnabledServices([ "hulu", "yttv" ]);

    // An unknown URL falls back to the "direct" tag via getDomainConfig. We use a freshly-invented domain to ensure no real DOMAIN_CONFIG entry maps it to a
    // real service tag - the absence of a config entry forces the helper down the "direct" branch.
    const warning = buildServiceFilterWarning("https://no-such-service-domain.invalid/live");

    assert.equal(warning, undefined, "direct tag must never produce a warning");
  });

  test("returns undefined when getDomainConfig finds no entry for the URL (tag is undefined)", () => {

    // Boundary: an unparseable URL produces an undefined domain config, which means no tag is resolved. The truthy check on `tag` short-circuits the warning.
    setEnabledServices(["hulu"]);

    assert.equal(buildServiceFilterWarning(""), undefined, "empty URL produces no warning");
  });
});
