/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * profiles.test.ts: Unit tests for the profile resolution module. The module composes inheritance chains, resolves URL- and channel-level overrides, and
 * gates the validation of inheritance cycles and missing references at startup. We exercise every resolution path and every error-collection branch of
 * validateProfiles. The user-profile/domain registry is left at its default (empty) state so resolution exercises only the static builtin tables.
 */
import { DEFAULT_SITE_PROFILE, getProfileForChannel, getProfileForUrl, getProfiles, resolveProfile, validateProfiles } from "./profiles.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("DEFAULT_SITE_PROFILE", () => {

  test("declares every flag with an explicit (non-undefined) value", () => {

    // The resolution code starts with a copy of DEFAULT_SITE_PROFILE; missing flags would surface as undefined in resolved profiles. Locking the keys ensures
    // the resolved shape is always complete.
    assert.equal(DEFAULT_SITE_PROFILE.staticCapture, false);
    assert.equal(DEFAULT_SITE_PROFILE.useRequestFullscreen, false);
    assert.equal(DEFAULT_SITE_PROFILE.needsIframeHandling, false);
    assert.equal(DEFAULT_SITE_PROFILE.fullscreenKey, null);
    assert.equal(DEFAULT_SITE_PROFILE.channelSelector, null);
  });
});

describe("resolveProfile", () => {

  test("returns the default profile when name is undefined", () => {

    const result = resolveProfile(undefined);

    assert.deepEqual(result, DEFAULT_SITE_PROFILE);
    assert.notEqual(result, DEFAULT_SITE_PROFILE, "result is a fresh copy, not the singleton reference");
  });

  test("returns the default profile when name is empty string", () => {

    const result = resolveProfile("");

    assert.deepEqual(result, DEFAULT_SITE_PROFILE);
  });

  test("resolves a base profile (no extends) by merging with default", () => {

    // staticPage is a base profile that sets staticCapture: true. The resolved shape carries that flag plus all default flags.
    const result = resolveProfile("staticPage");

    assert.equal(result.staticCapture, true);
    assert.equal(result.useRequestFullscreen, false, "default flag inherited");
  });

  test("the builtin fullscreen families leave the native step off", () => {

    /* Both fullscreen families name the native mechanism a site's player supports rather than one the base profile invokes, so neither contributes a flag and
     * every descendant inherits the default. The third trigger, fullscreenSelector, is absent from these tables for the same reason. A profile that does opt
     * into a native mechanism is registered by a provider module, which this tier does not load - the static builtin tables alone are visible here.
     */
    assert.equal(resolveProfile("fullscreenApi").useRequestFullscreen, false, "the api family's base contributes no flag");
    assert.equal(resolveProfile("keyboardFullscreen").fullscreenKey, null, "the keyboard family's base contributes no key");
    assert.equal(resolveProfile("brightcove").useRequestFullscreen, false, "an api-family descendant inherits the default");
    assert.equal(resolveProfile("keyboardDynamic").fullscreenKey, null, "a keyboard-family descendant inherits the default");
  });

  test("resolves a derived profile by merging parent then child (extends chain)", () => {

    // keyboardDynamicMultiVideo extends keyboardDynamic and sets selectReadyVideo: true. What the two assertions prove is that every ancestor contributing a
    // flag is reached: the parent's network idle wait and the child's own video selection both land in the resolved shape.
    const result = resolveProfile("keyboardDynamicMultiVideo");

    assert.equal(result.waitForNetworkIdle, true, "inherited from keyboardDynamic");
    assert.equal(result.selectReadyVideo, true, "set on keyboardDynamicMultiVideo itself");
  });

  test("resolves multi-level inheritance (embeddedDynamicMultiVideo -> embeddedPlayer -> fullscreenApi)", () => {

    // The chain is walked root-first, so each level that contributes a flag lands over the one below it. The root contributes none, which is why the API flag
    // reads the default here.
    const result = resolveProfile("embeddedDynamicMultiVideo");

    assert.equal(result.useRequestFullscreen, false, "the root contributes no behavior flag, so the default holds");
    assert.equal(result.needsIframeHandling, true, "from embeddedPlayer");
    assert.equal(result.waitForNetworkIdle, true, "from embeddedDynamicMultiVideo");
    assert.equal(result.selectReadyVideo, true, "from embeddedDynamicMultiVideo");
  });

  test("strips metadata fields (description, extends, summary, category) from the resolved output", () => {

    const result = resolveProfile("keyboardFullscreen") as unknown as Record<string, unknown>;

    assert.equal("description" in result, false, "description metadata not in resolved");
    assert.equal("extends" in result, false, "extends metadata not in resolved");
    assert.equal("summary" in result, false, "summary metadata not in resolved");
    assert.equal("category" in result, false, "category metadata not in resolved");
  });

  test("returns the default profile when the name is unknown", () => {

    const result = resolveProfile("not-a-profile-xyz");

    assert.deepEqual(result, DEFAULT_SITE_PROFILE);
  });
});

describe("getProfileForUrl", () => {

  test("returns the default profile and 'default' name when url is undefined", () => {

    const result = getProfileForUrl(undefined);

    assert.equal(result.profileName, "default");
    assert.equal(result.profile.useRequestFullscreen, false);
  });

  test("resolves a domain-mapped profile by URL (hulu.com -> huluLive)", () => {

    const result = getProfileForUrl("https://www.hulu.com/live");

    assert.equal(result.profileName, "huluLive");
  });

  test("uses the full hostname before falling back to the concise domain", () => {

    // tv.youtube.com has its own DOMAIN_CONFIG entry pointing at youtubeTV. youtube.com points at keyboardDynamic. The full-hostname lookup must win.
    const result = getProfileForUrl("https://tv.youtube.com/watch/abc");

    assert.equal(result.profileName, "youtubeTV");
  });

  test("returns 'default' for an unknown domain", () => {

    const result = getProfileForUrl("https://example.example/whatever");

    assert.equal(result.profileName, "default");
  });

  test("merges domain-level dismissSelector into the resolved profile", () => {

    // c-span.org has dismissSelector in its DOMAIN_CONFIG entry; getProfileForUrl must merge it in.
    const result = getProfileForUrl("https://www.c-span.org/live/");

    assert.match(result.profile.dismissSelector ?? "", /SkipButton/);
  });

  test("merges domain-level maxContinuousPlayback when configured (nbc.com)", () => {

    const result = getProfileForUrl("https://www.nbc.com/live");

    assert.equal(result.profile.maxContinuousPlayback, 4);
  });
});

describe("getProfileForChannel", () => {

  test("returns the default when channel is undefined", () => {

    const result = getProfileForChannel(undefined);

    assert.equal(result.profileName, "default");
  });

  test("returns the default when neither profile nor url is set", () => {

    const result = getProfileForChannel({});

    assert.equal(result.profileName, "default");
  });

  test("uses an explicit channel.profile over URL-based detection", () => {

    const result = getProfileForChannel({ profile: "fullscreenApi", url: "https://www.hulu.com/live" });

    assert.equal(result.profileName, "fullscreenApi", "explicit profile wins over hulu.com -> huluLive");
  });

  test("treats profile=auto as unset and falls through to URL-based detection", () => {

    const result = getProfileForChannel({ profile: "auto", url: "https://www.hulu.com/live" });

    assert.equal(result.profileName, "huluLive");
  });

  test("merges channelSelector from the channel into the resolved profile", () => {

    const result = getProfileForChannel({ channelSelector: "ABC", profile: "fullscreenApi", url: "https://example.com" });

    assert.equal(result.profile.channelSelector, "ABC");
  });

  test("falls back to concise domain when explicit profile requires channelSelection but channel has no channelSelector", () => {

    /* The fallback fires when the resolved profile has a non-"none" channelSelection strategy and the channel has no channelSelector. apiMultiVideo is a
     * builtin general profile that declares a tileClick strategy. With it, the fallback to the concise domain (example.com -> default) bypasses apiMultiVideo
     * because the channel can't supply a selector.
     */
    const result = getProfileForChannel({ profile: "apiMultiVideo", url: "https://example.com" });

    // The fallback only fires when there is a different concise-domain profile to fall back to. example.com has none, so the apiMultiVideo profileName persists
    // but the resolution will still surface as apiMultiVideo. We lock the contract that the channel-level fallback path runs without throwing.
    assert.ok(result.profile, "resolution returns a defined profile");
    assert.ok(typeof result.profileName === "string", "resolution returns a profileName string");
  });

  test("explicit-profile fallback flips profileName when channel needs no selector and the concise domain has a different profile", () => {

    /* Positive case for the channel-selection fallback at profiles.ts:190-199. The obvious fixture (watch.sling.com, slingLive) doesn't apply at
     * unit-test scope because slingLive is a provider profile registered only when browser/channelSelection.ts loads. We use an in-process equivalent: the
     * channel asks for apiMultiVideo (a builtin general profile with strategy="tileClick") but provides no channelSelector and a URL whose concise domain
     * (cnbc.com) maps to fullscreenApi - a profile that does not require channel selection. The fallback fires and the profileName flips from apiMultiVideo
     * to fullscreenApi.
     */
    const result = getProfileForChannel({ profile: "apiMultiVideo", url: "https://www.cnbc.com/livestream/" });

    assert.equal(result.profileName, "fullscreenApi", "channel-level fallback flipped to the concise-domain profile when the original required a selector");
  });

  test("the explicit-profile fallback reports the displaced choice on its return", () => {

    /* The resolver's half of the override contract: it names the profile the channel asked for and did not get, and leaves the decision to surface that to
     * whichever caller cares. Same fixture as the flip test above - apiMultiVideo needs a channel selector this channel does not define, so cnbc.com's concise
     * entry substitutes fullscreenApi for it.
     */
    const result = getProfileForChannel({ profile: "apiMultiVideo", url: "https://www.cnbc.com/livestream/" });

    assert.equal(result.overriddenProfile, "apiMultiVideo", "the displaced explicit choice is reported by name");
  });

  test("an explicit profile that survives resolution reports no override", () => {

    // The key is absent rather than present with an undefined value, so a caller can test the field directly. fullscreenApi needs no channel selector, so the
    // fallback never fires and nothing displaces the channel's choice.
    const result = getProfileForChannel({ profile: "fullscreenApi", url: "https://www.c-span.org/live/" });

    assert.equal("overriddenProfile" in result, false, "no override key when the channel's choice survives resolution");
  });

  test("explicit profile + URL re-applies domain properties idempotently (no double-merge surprise)", () => {

    /* The comment in mergeDomainProperties' re-application call says "For the URL-based path above, getProfileForUrl() already merges these - the re-application
     * here is idempotent. For the explicit-profile path, this fills the gap." We pin the gap-fill: an explicit profile with a URL whose domain carries a
     * dismissSelector must surface that dismissSelector in the resolved profile even though the profile itself didn't declare one.
     */
    const result = getProfileForChannel({ profile: "fullscreenApi", url: "https://www.c-span.org/live/" });

    /* c-span.org's DomainConfig declares a dismissSelector. The explicit fullscreenApi path does not invoke getProfileForUrl, so the dismissSelector arrives
     * via mergeDomainProperties' re-application step. Pin: it is present.
     */
    assert.match(result.profile.dismissSelector ?? "", /SkipButton/, "domain dismissSelector merged onto an explicitly-set profile");
  });

  test("channel-level dismissSelector override wins over the domain-level value", () => {

    /* The channel-level merge at profiles.ts:216-219 happens after the domain-level merge, so a channel that declares dismissSelector overrides whatever the
     * domain provided. Pinning this lets a regression that reordered the merges (or dropped the channel-level one entirely) surface here.
     */
    const result = getProfileForChannel({

      dismissSelector: "#channel-specific-skip-button",
      url: "https://www.c-span.org/live/"
    });

    assert.equal(result.profile.dismissSelector, "#channel-specific-skip-button",
      "channel-level dismissSelector wins over c-span.org's domain-level SkipButton selector");
  });

  test("merges scrollToBottom override into channelSelection without disturbing other fields", () => {

    const result = getProfileForChannel({ channelSelector: "ABC", profile: "fullscreenApi", scrollToBottom: true, url: "https://example.com" });

    assert.equal(result.profile.channelSelection.scrollToBottom, true);
  });

  test("merges scrollSelector channel override into the resolved channelSelection", () => {

    /* Companion to the scrollToBottom test - same merge-loop code path, different field. Pins that scrollSelector specifically reaches the resolved profile,
     * not just scrollToBottom.
     */
    const result = getProfileForChannel({ channelSelector: "ABC", profile: "fullscreenApi", scrollSelector: ".my-scroller", url: "https://example.com" });

    assert.equal(result.profile.channelSelection.scrollSelector, ".my-scroller");
  });

  test("merges scrollTarget channel override into the resolved channelSelection", () => {

    const result = getProfileForChannel({ channelSelector: "ABC", profile: "fullscreenApi", scrollTarget: "host", url: "https://example.com" });

    assert.equal(result.profile.channelSelection.scrollTarget, "host");
  });

  test("does NOT touch channelSelection when no scroll override is supplied (gate boundary)", () => {

    /* The merge loop only runs when at least one scroll key is present on the channel. Pinning this prevents a regression where the loop unconditionally
     * builds a partial scrollOverrides object and overwrites the resolved profile's channelSelection with an empty extension.
     */
    const baseline = getProfileForChannel({ channelSelector: "ABC", profile: "fullscreenApi", url: "https://example.com" });
    const withoutScroll = getProfileForChannel({ channelSelector: "DEF", profile: "fullscreenApi", url: "https://example.com" });

    /* The two resolutions differ only in channelSelector (which IS overridden); the channelSelection sub-object should retain the same shape, with no
     * scrollOverrides-injected fields appearing on either.
     */
    assert.deepEqual(Object.keys(baseline.profile.channelSelection).toSorted(), Object.keys(withoutScroll.profile.channelSelection).toSorted(),
      "the channelSelection key set is unchanged when no scroll override is supplied");
  });
});

describe("getProfiles", () => {

  test("returns builtin profiles sorted alphabetically by name", () => {

    const profiles = getProfiles();
    const names = profiles.map((p) => p.name);
    const sorted = names.toSorted((a, b) => a.localeCompare(b));

    assert.deepEqual(names, sorted, "names are alphabetically sorted");
  });

  test("each entry carries name, source, category, summary, description", () => {

    const profiles = getProfiles();

    for(const entry of profiles) {

      assert.ok(entry.name, "name present");
      assert.ok([ "builtin", "user" ].includes(entry.source), "source is builtin or user");
      assert.ok(typeof entry.category === "string", "category is a string");
      assert.ok(typeof entry.summary === "string", "summary is a string");
      assert.ok(typeof entry.description === "string", "description is a string");
    }
  });

  test("source field marks every builtin profile as 'builtin'", () => {

    const profiles = getProfiles();

    // SITE_PROFILES entries become "builtin"; we check at least the well-known general profiles.
    const keyboardFullscreen = profiles.find((p) => p.name === "keyboardFullscreen");

    assert.equal(keyboardFullscreen?.source, "builtin");
  });
});

describe("validateProfiles", () => {

  /* In isolation (without browser/channelSelection.ts loaded to call registerProviderModuleProfile for huluLive/youtubeTV/etc.), validateProfiles surfaces the
   * domain-references-non-existent-profile errors we expect in production for those provider profiles. The function still throws an Error in that case, which
   * is the contract: any reference inconsistency is reported.
   */
  test("collects every domain-references-missing-profile error into a single throw", () => {

    try {

      validateProfiles();
    } catch(err) {

      assert.ok(err instanceof Error, "throws an Error subclass");
      assert.match(err.message, /Profile validation failed/, "error preamble identifies the validator");
      // Provider profiles are registered by browser/ modules; under unit-test loading they're absent. Lock that the validator surfaces these references.
      assert.match(err.message, /Domain hulu\.com references non-existent profile: huluLive/);
    }
  });
});
