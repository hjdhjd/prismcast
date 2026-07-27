/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.accessors.test.ts: Direct unit tests for the read-accessor cluster - small predicates and pure value lookups whose contracts are stable enough to
 * be locked in at the unit level. The accessors split into two coverage tiers:
 *
 *   1. Pure-relative-to-compile-time-data: tagsMatch, getEffectiveHdhrEnabled, isPredefinedChannel, getEastCanonicalKey, isVisibleChannel. These read only static
 *      module imports (PREDEFINED_CHANNELS) or their argument, so they are safe to unit-test directly without any module initialization.
 *
 *   2. Runtime-state-dependent: isUserChannel (loadedUserChannels), isPredefinedChannelDisabled (CONFIG.channels.disabledPredefined), isInVocabulary and
 *      getChannelEffectiveTags (loadedTagRegistry via getActiveTagVocabulary), isChannelAvailable (getAllChannels merge result). These depend on state populated
 *      by initializeUserChannels and the broader CONFIG bootstrap. Bringing that up cleanly in a unit test would re-implement the bootstrap; the existing pattern
 *      in this directory keeps unit tests on pure helpers and routes integration coverage through HTTP-endpoint tests where the full system is already wired
 *      up. The tier-2 accessors are exercised there transitively.
 *
 * The tests below cover tier 1 exhaustively. A third group - the thin state and delegation wrappers hasChannelsParseError, getChannelsParseErrorMessage, and
 * getUserChannelsFilePath - is contract-tested directly here: their shape (boolean, string-or-undefined, and throw-or-resolve) holds regardless of bring-up state,
 * so we pin the contract without standing up the bootstrap. The remaining tier-2 accessors are documented at each function's describe block as a deliberate
 * transitive-coverage decision.
 */
import type { ChannelListingEntry, ResolvedChannel } from "../types/index.ts";
import { describe, test } from "node:test";
import { getChannelsParseErrorMessage, getEastCanonicalKey, getEffectiveHdhrEnabled, getUserChannelsFilePath, hasChannelsParseError, isPredefinedChannel,
  isVisibleChannel, tagsMatch } from "./userChannels.ts";
import assert from "node:assert/strict";
import { makeChannel } from "./userChannels.helpers.ts";

describe("tagsMatch", () => {

  test("returns true for identical case", () => {

    assert.equal(tagsMatch("Sports", "Sports"), true);
  });

  test("returns true for differing case (the entire reason this helper exists)", () => {

    // The single-source-of-truth for case-insensitive tag identity. Every tag comparison in the system routes through this so we can change the policy in one
    // place if the user-facing rules ever shift (e.g., to Unicode locale-aware folding).
    assert.equal(tagsMatch("Sports", "SPORTS"), true);
    assert.equal(tagsMatch("Sports", "sports"), true);
    assert.equal(tagsMatch("sPoRtS", "SpOrTs"), true);
  });

  test("returns false for different tag values", () => {

    assert.equal(tagsMatch("Sports", "News"), false);
  });

  test("returns false for different lengths even when one is a prefix of the other", () => {

    // Documents that this is identity, not prefix-match. A future refactor that confused the two would be caught here.
    assert.equal(tagsMatch("Sport", "Sports"), false);
  });

  test("treats empty strings as equal to themselves and unequal to anything else", () => {

    assert.equal(tagsMatch("", ""), true);
    assert.equal(tagsMatch("", "Sports"), false);
  });

  test("treats whitespace as significant (tag values are not auto-trimmed)", () => {

    // The trimming policy lives at the input boundary (parseTagInput); tagsMatch is a pure comparator and never normalizes whitespace itself.
    assert.equal(tagsMatch("Sports", "Sports "), false);
    assert.equal(tagsMatch(" Sports", "Sports "), false);
  });
});

describe("getEffectiveHdhrEnabled", () => {

  /* Sparse-storage convention: absent or true => included in the HDHR lineup; only an explicit false excludes. The function is the single source of truth for
   * that convention. Tests pin the boundary between "false" (the only excluding value) and every other valid value or shape.
   */

  test("returns true when hdhrEnabled is absent (the typical predefined-channel shape)", () => {

    const channel = { name: "ABC", url: "https://abc.com" } as ResolvedChannel;

    assert.equal(getEffectiveHdhrEnabled(channel), true);
  });

  test("returns true when hdhrEnabled is explicitly true", () => {

    const channel = { hdhrEnabled: true, name: "ABC", url: "https://abc.com" } as ResolvedChannel;

    assert.equal(getEffectiveHdhrEnabled(channel), true);
  });

  test("returns false ONLY when hdhrEnabled is explicitly false", () => {

    const channel = { hdhrEnabled: false, name: "ABC", url: "https://abc.com" } as ResolvedChannel;

    assert.equal(getEffectiveHdhrEnabled(channel), false);
  });

  test("returns true when hdhrEnabled is undefined (sparse-storage equivalence with absent)", () => {

    // undefined and absent are equivalent under the sparse-storage convention; both flow through the !== false comparison the same way.
    const channel = { hdhrEnabled: undefined, name: "ABC", url: "https://abc.com" } as ResolvedChannel;

    assert.equal(getEffectiveHdhrEnabled(channel), true);
  });
});

describe("isPredefinedChannel", () => {

  /* Reads the static PREDEFINED_CHANNELS map (compile-time module import). Safe to call without any runtime initialization. Tests use real predefined keys to
   * verify both canonical entries and programmatically-generated variant entries are recognized.
   */

  test("returns true for a real canonical predefined key", () => {

    // abc is a real canonical entry in PREDEFINED_CHANNELS.
    assert.equal(isPredefinedChannel("abc"), true);
  });

  test("returns true for a real Pacific canonical predefined key", () => {

    // bravop is a Pacific feed canonical, programmatically generated by generatePacificDefinitions but present in the flattened map.
    assert.equal(isPredefinedChannel("bravop"), true);
  });

  test("returns true for a real variant predefined key", () => {

    // abc-hulu is a service-variant entry built by buildVariantEntry; it lives in PREDEFINED_CHANNELS alongside its canonical.
    assert.equal(isPredefinedChannel("abc-hulu"), true);
  });

  test("returns false for an unknown key", () => {

    assert.equal(isPredefinedChannel("nonexistent-channel-key"), false);
  });

  test("returns false for an empty string", () => {

    // Boundary: empty string is not a key in PREDEFINED_CHANNELS. The function does not special-case this; the `in` operator returns false naturally.
    assert.equal(isPredefinedChannel(""), false);
  });

  test("is case-sensitive ('ABC' is not a predefined key, only 'abc' is)", () => {

    // Channel keys are lowercase by validation rule; the static map mirrors that. A case-insensitive match would be a bug.
    assert.equal(isPredefinedChannel("ABC"), false);
  });
});

describe("getEastCanonicalKey", () => {

  /* The Pacific-feed naming convention: a key that ends with "p" and whose base (key minus the trailing "p") is itself a predefined canonical resolves to the
   * base key. Used for logo/brand-metadata fallback when the Pacific feed inherits visual identity from its East counterpart.
   */

  test("returns the east canonical when the key is a real Pacific predefined", () => {

    // bravop -> bravo is a real pairing in the predefined catalog.
    assert.equal(getEastCanonicalKey("bravop"), "bravo");
  });

  test("returns the east canonical for tbsp -> tbs", () => {

    assert.equal(getEastCanonicalKey("tbsp"), "tbs");
  });

  test("returns the east canonical for tntp -> tnt", () => {

    assert.equal(getEastCanonicalKey("tntp"), "tnt");
  });

  test("returns undefined for a key that doesn't end with 'p'", () => {

    // The first guard is a structural check; if the key doesn't end in "p" we return undefined before consulting the predefined map.
    assert.equal(getEastCanonicalKey("abc"), undefined);
  });

  test("returns undefined when the key ends with 'p' but the base isn't predefined", () => {

    // "foop" ends in "p" so the suffix guard passes; "foo" is not in PREDEFINED_CHANNELS so the lookup returns undefined.
    assert.equal(getEastCanonicalKey("foop"), undefined);
  });

  test("returns undefined for the literal 'p' (single character, base is empty string)", () => {

    // Edge case: stripping the trailing "p" from "p" yields the empty string, which is not predefined - returns undefined.
    assert.equal(getEastCanonicalKey("p"), undefined);
  });

  test("returns undefined for an empty string (no trailing 'p')", () => {

    assert.equal(getEastCanonicalKey(""), undefined);
  });

  test("does NOT recursively resolve nested 'p' suffixes (only one strip)", () => {

    // Documents that the function strips exactly one trailing "p" and asks once. A key like "abcpp" would strip to "abcp", and only resolve if "abcp" is itself
    // predefined. There is no recursive descent.
    assert.equal(getEastCanonicalKey("abcpp"), undefined, "abcp is not in the predefined catalog so abcpp does not resolve");
  });
});

describe("isVisibleChannel", () => {

  /* Pure predicate over a ChannelListingEntry: visible iff enabled AND availableByService. Single source of truth for what "visible" means across bulk
   * operations, the playlist, and the merged channel map.
   */

  function makeEntry(overrides: Partial<ChannelListingEntry> = {}): ChannelListingEntry {

    return { availableByService: true, channel: makeChannel(), enabled: true, key: "test", source: "user", ...overrides };
  }

  test("returns true when both enabled and availableByService are true", () => {

    assert.equal(isVisibleChannel(makeEntry()), true);
  });

  test("returns false when enabled is false (operator-disabled predefined)", () => {

    assert.equal(isVisibleChannel(makeEntry({ enabled: false })), false);
  });

  test("returns false when availableByService is false (filtered out by service filter)", () => {

    assert.equal(isVisibleChannel(makeEntry({ availableByService: false })), false);
  });

  test("returns false when both are false", () => {

    assert.equal(isVisibleChannel(makeEntry({ availableByService: false, enabled: false })), false);
  });
});

describe("hasChannelsParseError / getChannelsParseErrorMessage / getUserChannelsFilePath", () => {

  /* Accessor wrappers exposing module-level state. Without runtime initialization, the parse-error flag is the default false / undefined and the file path
   * resolves via the paths module to whatever default is in effect. Tests pin contract shape rather than specific values.
   */

  test("hasChannelsParseError returns a boolean", () => {

    /* The flag's contract is boolean; the value depends on whether initializeUserChannels has run with a parseable file. Asserting type-only keeps the test
     * unit-shaped without depending on bring-up state.
     */
    assert.equal(typeof hasChannelsParseError(), "boolean");
  });

  test("getChannelsParseErrorMessage returns string-or-undefined matching the parse-error flag", () => {

    /* Contract: when hasChannelsParseError() is false, getChannelsParseErrorMessage() returns undefined. When true, it returns a string. Pinning the relationship
     * between the two getters captures the rule linking them without depending on a specific value.
     */
    const message = getChannelsParseErrorMessage();
    const hasError = hasChannelsParseError();

    if(hasError) {

      assert.equal(typeof message, "string");
    } else {

      assert.equal(message, undefined);
    }
  });

  test("getUserChannelsFilePath delegates to the paths module (throws when data dir not initialized; resolves when initialized)", () => {

    /* The wrapper delegates to getChannelsFilePath in the paths module. Without a prior initializeDataDir call the resolver throws "Data directory not
     * initialized". We pin the throw-or-resolve contract: either the call resolves to a non-empty string (when run under an integration harness that initialized
     * the dir) or it throws. Both are valid documented states for this thin wrapper. Document the structural delegation here without taking on the bring-up cost
     * of paths.ts initialization just to assert a wrapped getter.
     */
    try {

      const path = getUserChannelsFilePath();

      assert.equal(typeof path, "string");
      assert.ok(path.length > 0, "path must be non-empty when data dir is initialized");
    } catch(error) {

      /* The expected error from getDataDir() before initializeDataDir() runs. Pinning the message anchors the contract.
       */
      assert.match((error as Error).message, /Data directory not initialized/, "delegates to paths.getDataDir which throws this exact message pre-init");
    }
  });
});

/* Runtime-state-dependent accessors deliberately not unit-tested here. Coverage for these flows comes from HTTP-endpoint integration tests (channels CRUD,
 * tag management, playlist generation) and from unit suites that call the accessor directly once module state is already populated by the boot sequence:
 *
 *   - isUserChannel: exercised by test/e2e/channels/crud.test.ts through the sibling-variant match branch (editing a canonical override so it matches a
 *     sibling predefined variant); the duplicate-key-on-create branch inside validateChannelKey has no dedicated test yet.
 *   - isPredefinedChannelDisabled: covered by routes/config/channels/endpoints/predefined.test.ts via toggle/bulk-toggle endpoints.
 *   - isInVocabulary, getActiveTagVocabulary: covered by routes/config/channels/endpoints/tags.test.ts via tag-vocabulary endpoints. getChannelEffectiveTags
 *     is covered separately by config/channelForm.test.ts (through channelMatches) and routes/config/channels/table.test.ts (direct calls).
 *   - isChannelAvailable: covered directly by test/e2e/channels/userchannels-public-api.test.ts (presence and absence in the merged channel map, including
 *     after disabling a predefined channel).
 *
 * Adding a unit-level state-bring-up here would re-implement the boot pipeline (CONFIG load, persistence framework, service-group construction). The cost
 * exceeds the benefit since each function is a one-or-two-line predicate whose contract is enforced by the integration tests. If a tier-2 accessor grows in
 * complexity (more branches, derived state, multi-step logic), revisit this decision and extract a pure variant that can be tested in isolation.
 */
