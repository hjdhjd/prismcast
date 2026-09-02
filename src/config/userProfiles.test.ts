/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userProfiles.test.ts: Unit tests for the validation predicates of the user profiles and domain mappings module - normalizeLegacyProfileFlags,
 * validateProfileKey, validateProfile, validateDomain, validateImportedProfiles. Snapshot accessors live in userProfiles.accessors.test.ts; persistence
 * orchestrators (initializeUserProfiles, mutateProfiles) are exercised at the integration tier in test/e2e/persistence/profiles.test.ts.
 */
import { describe, test } from "node:test";
import { normalizeLegacyProfileFlags, validateDomain, validateImportedProfiles, validateProfile, validateProfileKey } from "./userProfiles.ts";
import type { SiteProfile } from "../types/index.ts";
import assert from "node:assert/strict";
import { registerProviderModuleProfile } from "./sites.ts";

describe("normalizeLegacyProfileFlags", () => {

  test("renames noVideo to staticCapture in place", () => {

    const profiles: Record<string, SiteProfile> = {

      // Cast to permissive shape so we can write the legacy flag.
      myProfile: { extends: "fullscreenApi", noVideo: true } as unknown as SiteProfile
    };
    const changed = normalizeLegacyProfileFlags(profiles);

    assert.equal(changed, true, "function reports a rename happened");
    assert.equal((profiles["myProfile"] as Record<string, unknown>)["staticCapture"], true);
    assert.equal("noVideo" in profiles["myProfile"]!, false, "legacy field removed");
  });

  test("does not overwrite the current field when both legacy and current are present", () => {

    const profiles: Record<string, SiteProfile> = {

      myProfile: { extends: "fullscreenApi", noVideo: true, staticCapture: false } as unknown as SiteProfile
    };

    normalizeLegacyProfileFlags(profiles);

    // Current value (staticCapture: false) wins; the legacy noVideo: true does not overwrite it.
    assert.equal((profiles["myProfile"] as Record<string, unknown>)["staticCapture"], false);
    assert.equal("noVideo" in profiles["myProfile"]!, false, "legacy field still removed");
  });

  test("returns false and makes no changes when no legacy flags are present", () => {

    const profiles: Record<string, SiteProfile> = {

      myProfile: { extends: "fullscreenApi", staticCapture: true }
    };

    const changed = normalizeLegacyProfileFlags(profiles);

    assert.equal(changed, false);
  });

  test("renames noVideo to staticCapture even when the legacy value is false (rename is value-independent)", () => {

    /* The contract documented at the top of normalizeLegacyProfileFlags: it ports values verbatim regardless of truthiness. A user who explicitly disabled the
     * legacy flag (noVideo: false) must still see the rename, because their disabled state is a real configuration choice that should survive the rename.
     */
    const profiles: Record<string, SiteProfile> = {

      myProfile: { extends: "fullscreenApi", noVideo: false } as unknown as SiteProfile
    };
    const changed = normalizeLegacyProfileFlags(profiles);

    assert.equal(changed, true, "function reports the rename happened even with a falsy legacy value");
    assert.equal((profiles["myProfile"] as Record<string, unknown>)["staticCapture"], false, "false value carried over verbatim");
    assert.equal("noVideo" in profiles["myProfile"]!, false, "legacy field removed");
  });
});

describe("validateProfileKey", () => {

  test("rejects empty key", () => {

    assert.match(validateProfileKey("", false) ?? "", /required/);
    assert.match(validateProfileKey("   ", false) ?? "", /required/);
  });

  test("rejects invalid format (starts with digit, contains underscore, ends with hyphen)", () => {

    assert.match(validateProfileKey("1abc", false) ?? "", /must start with a letter/);
    assert.match(validateProfileKey("foo_bar", false) ?? "", /must start with a letter/);
    assert.match(validateProfileKey("foo-", false) ?? "", /must start with a letter/);
  });

  test("accepts a valid camelCase key", () => {

    assert.equal(validateProfileKey("myCustomProfile", false), undefined);
  });

  test("accepts a valid kebab-case key", () => {

    assert.equal(validateProfileKey("my-custom-profile", false), undefined);
  });

  test("rejects keys longer than 50 characters", () => {

    const longKey = "a".repeat(51);

    assert.match(validateProfileKey(longKey, false) ?? "", /50 characters or less/);
  });

  test("accepts a key of exactly 50 characters (boundary)", () => {

    const exactKey = "a".repeat(50);

    assert.equal(validateProfileKey(exactKey, false), undefined);
  });

  test("rejects keys that collide with builtin profiles", () => {

    /* These names live in the general SITE_PROFILES table. Every call here passes isNew=false - the framing an import takes - and is still rejected, which is
     * also the assertion against gating the builtin check on the isNew flag: an implementation that only reserved builtin names while creating would let an import
     * introduce a shadowed key here.
     */
    assert.match(validateProfileKey("keyboardFullscreen", false) ?? "", /conflicts with a builtin/);
    assert.match(validateProfileKey("fullscreenApi", false) ?? "", /conflicts with a builtin/);
  });

  test("rejects a key that collides with a provider profile", () => {

    // disneyNow lives in the PROVIDER_PROFILES table rather than the general one. A profile saved under this name would be permanently shadowed at resolution,
    // because resolution consults every builtin source before the user store.
    assert.match(validateProfileKey("disneyNow", true) ?? "", /conflicts with a builtin/);
    assert.match(validateProfileKey("disneyPlus", false) ?? "", /conflicts with a builtin/);
  });

  test("rejects a key that collides with a profile a provider module registered", () => {

    /* Provider modules register their profiles at import time, so those names exist in neither static table. The registry has no unregister call and this
     * process outlives the test, so the name below is deliberately synthetic and used by no other suite - registering a real provider name here would collide
     * with the static-table guard in registerProviderModuleProfile.
     */
    registerProviderModuleProfile("syntheticProviderProfileForKeyValidation", { description: "Registered by this test only.", extends: "fullscreenApi" });

    assert.match(validateProfileKey("syntheticProviderProfileForKeyValidation", true) ?? "", /conflicts with a builtin/);
  });

  test("isNew=true returns undefined for a non-colliding key (no duplicate among loaded user profiles in unit-test state)", () => {

    /* The isNew=true branch checks loadedUserProfiles for a duplicate key. In unit tests no user profiles are loaded, so any non-builtin non-colliding key
     * returns undefined. This asserts the contract that the check fires (it does not fall through to the builtin check) and that isNew=true produces a clean
     * result for the empty-state. The duplicate-among-user-profiles branch requires module state that only the integration tier provides.
     */
    assert.equal(validateProfileKey("brand-new-user-key", true), undefined);
  });
});

describe("validateProfile", () => {

  test("requires extends field", () => {

    const errors = validateProfile("test", {});

    assert.match(errors[0] ?? "", /extends is required/);
  });

  test("rejects extends pointing to a non-existent builtin profile", () => {

    const errors = validateProfile("test", { extends: "not-a-real-profile" });

    assert.match(errors[0] ?? "", /non-existent builtin profile/);
  });

  test("accepts valid profile extending a general builtin", () => {

    const errors = validateProfile("test", { extends: "fullscreenApi" });

    assert.equal(errors.length, 0);
  });

  test("rejects extends pointing at a provider profile (DOM-coupled)", () => {

    // disneyNow is a static provider profile; users cannot extend it.
    const errors = validateProfile("test", { extends: "disneyNow" });

    assert.ok(errors.some((e) => e.includes("service-specific profile")), "expected a service-specific error");
  });

  test("rejects unrecognized strategy", () => {

    const profile: SiteProfile = {

      channelSelection: { strategy: "wonky-strategy" as never },
      extends: "fullscreenApi"
    };
    const errors = validateProfile("test", profile);

    assert.ok(errors.some((e) => e.includes("unrecognized channel selection strategy")));
  });

  test("rejects use of a provider-specific strategy by user profiles", () => {

    // huluLive uses guideGrid; user profiles cannot use guideGrid (only generic strategies).
    const profile: SiteProfile = {

      channelSelection: { strategy: "guideGrid" as never },
      extends: "fullscreenApi"
    };
    const errors = validateProfile("test", profile);

    assert.ok(errors.some((e) => e.includes("builtin service strategy")));
  });

  test("requires matchSelector for tileClick strategy", () => {

    const profile: SiteProfile = {

      channelSelection: { strategy: "tileClick" },
      extends: "fullscreenApi"
    };
    const errors = validateProfile("test", profile);

    assert.ok(errors.some((e) => e.includes("matchSelector is required")));
  });

  test("requires matchSelector for thumbnailRow strategy", () => {

    const profile: SiteProfile = {

      channelSelection: { strategy: "thumbnailRow" },
      extends: "fullscreenApi"
    };
    const errors = validateProfile("test", profile);

    assert.ok(errors.some((e) => e.includes("matchSelector is required")));
  });

  test("does not require matchSelector for 'none' strategy", () => {

    const profile: SiteProfile = {

      channelSelection: { strategy: "none" },
      extends: "fullscreenApi"
    };
    const errors = validateProfile("test", profile);

    assert.equal(errors.length, 0);
  });

  test("rejects unrecognized top-level flags", () => {

    const profile = {

      bogusFlag: true,
      extends: "fullscreenApi"
    } as unknown as SiteProfile;
    const errors = validateProfile("test", profile);

    assert.ok(errors.some((e) => e.includes("unrecognized flag 'bogusFlag'")));
  });

  test("accepts every valid SiteProfile flag without error", () => {

    const profile: SiteProfile = {

      clickToPlay: true,
      extends: "fullscreenApi",
      lockVolumeProperties: true,
      needsIframeHandling: true,
      selectReadyVideo: true,
      staticCapture: false,
      useRequestFullscreen: true,
      waitForNetworkIdle: true
    };
    const errors = validateProfile("test", profile);

    assert.equal(errors.length, 0);
  });

  test("accepts a strategy from STRATEGIES_REQUIRING_MATCH_SELECTOR when matchSelector is supplied (combination boundary)", () => {

    /* Asserts the four-way intersection: strategy is recognized AND generic AND requires-match-selector AND the selector is supplied. The existing tests cover
     * three of the four branches in isolation; this combination test verifies the happy intersection so a regression that demands match-selector even when
     * present surfaces here.
     */
    const profile: SiteProfile = {

      channelSelection: { matchSelector: ".my-tile", strategy: "tileClick" },
      extends: "fullscreenApi"
    };
    const errors = validateProfile("test", profile);

    assert.equal(errors.length, 0, "tileClick + matchSelector is the happy intersection");
  });
});

describe("validateDomain", () => {

  // The profile-reference check consults a caller-supplied predicate rather than a name table, so each case supplies the oracle its assertion needs. Cases that
  // exercise a check unrelated to the profile field answer that no name is known.
  const noKnownProfiles = (): boolean => false;

  test("rejects empty domain", () => {

    const errors = validateDomain("", {}, noKnownProfiles);

    assert.match(errors[0] ?? "", /required/);
  });

  test("rejects domain with invalid hostname format (missing TLD)", () => {

    const errors = validateDomain("nodot", {}, noKnownProfiles);

    assert.ok(errors.some((e) => e.includes("invalid hostname format")));
  });

  test("accepts a valid domain", () => {

    const errors = validateDomain("custom-site.example", {}, noKnownProfiles);

    assert.equal(errors.length, 0);
  });

  test("rejects collision with builtin DOMAIN_CONFIG entry", () => {

    const errors = validateDomain("hulu.com", {}, noKnownProfiles);

    assert.ok(errors.some((e) => e.includes("already mapped to builtin service")));
  });

  test("rejects a profile reference the caller's predicate does not recognize", () => {

    const errors = validateDomain("custom-site.example", { profile: "missing-profile" }, (name) => name === "other-profile");

    assert.ok(errors.some((e) => e.includes("references non-existent profile")));
  });

  test("accepts a profile reference the caller's predicate recognizes", () => {

    const errors = validateDomain("custom-site.example", { profile: "myProfile" }, (name) => name === "myProfile");

    assert.equal(errors.length, 0);
  });

  test("rejects empty service string", () => {

    const errors = validateDomain("custom-site.example", { service: "" }, noKnownProfiles);

    assert.ok(errors.some((e) => e.includes("service must be a non-empty string")));
  });

  test("rejects empty serviceTag", () => {

    const errors = validateDomain("custom-site.example", { serviceTag: "" }, noKnownProfiles);

    assert.ok(errors.some((e) => e.includes("serviceTag must be a non-empty string")));
  });

  test("rejects loginUrl that's not a valid URL", () => {

    const errors = validateDomain("custom-site.example", { loginUrl: "not a url" }, noKnownProfiles);

    assert.ok(errors.some((e) => e.includes("not a valid URL")));
  });

  test("rejects loginUrl with non-http(s) protocol", () => {

    const errors = validateDomain("custom-site.example", { loginUrl: "ftp://example.com" }, noKnownProfiles);

    assert.ok(errors.some((e) => e.includes("must use http or https")));
  });

  test("accepts a valid http loginUrl", () => {

    const errors = validateDomain("custom-site.example", { loginUrl: "https://login.example.com" }, noKnownProfiles);

    assert.equal(errors.length, 0);
  });

  test("rejects negative or zero maxContinuousPlayback", () => {

    const errors = validateDomain("custom-site.example", { maxContinuousPlayback: 0 }, noKnownProfiles);

    assert.ok(errors.some((e) => e.includes("maxContinuousPlayback must be a positive number")));
  });

  test("rejects non-integer videoTimeout", () => {

    const errors = validateDomain("custom-site.example", { videoTimeout: 1.5 }, noKnownProfiles);

    assert.ok(errors.some((e) => e.includes("videoTimeout must be a positive integer")));
  });

  test("rejects an empty dismissSelector string when the field is supplied", () => {

    /* The validator accepts a missing dismissSelector but rejects an explicit empty string. Asserts the asymmetry so a future refactor that flipped to "any
     * string is fine" loses no signal here.
     */
    const errors = validateDomain("custom-site.example", { dismissSelector: "" }, noKnownProfiles);

    assert.ok(errors.some((e) => e.includes("dismissSelector must be a non-empty string")));
  });

  test("rejects a non-string dismissSelector (defensive against hand-edited JSON)", () => {

    const errors = validateDomain("custom-site.example", { dismissSelector: 42 as unknown as string }, noKnownProfiles);

    assert.ok(errors.some((e) => e.includes("dismissSelector must be a non-empty string")));
  });

  test("rejects a negative videoTimeout (boundary on the positivity check)", () => {

    /* Companion to the non-integer rejection: the validator also rejects zero and negative integer values. We test -1 to lock the lower bound; positive
     * integers and the existing non-integer rejection cover the rest of the space.
     */
    const errors = validateDomain("custom-site.example", { videoTimeout: -1 }, noKnownProfiles);

    assert.ok(errors.some((e) => e.includes("videoTimeout must be a positive integer")));
  });

  test("rejects Infinity for maxContinuousPlayback (Number.isFinite gate)", () => {

    /* The validator's Number.isFinite gate rejects Infinity and NaN explicitly, beyond the typeof === number check. Asserts the gate so a refactor that loosened
     * to typeof-only would surface here.
     */
    const errors = validateDomain("custom-site.example", { maxContinuousPlayback: Number.POSITIVE_INFINITY }, noKnownProfiles);

    assert.ok(errors.some((e) => e.includes("maxContinuousPlayback must be a positive number")));
  });

  test("rejects Infinity for videoTimeout (Number.isFinite gate)", () => {

    const errors = validateDomain("custom-site.example", { videoTimeout: Number.POSITIVE_INFINITY }, noKnownProfiles);

    assert.ok(errors.some((e) => e.includes("videoTimeout must be a positive integer")));
  });
});

describe("validateImportedProfiles", () => {

  test("rejects non-object input", () => {

    const result = validateImportedProfiles(null);

    assert.equal(result.valid, false);
    assert.match(result.errors[0] ?? "", /Invalid format/);
  });

  test("rejects array input (must be an object)", () => {

    const result = validateImportedProfiles([]);

    assert.equal(result.valid, false);
  });

  test("validates an empty object successfully (nothing to validate)", () => {

    const result = validateImportedProfiles({});

    assert.equal(result.valid, true);
    assert.deepEqual(result.profiles, {});
    assert.deepEqual(result.domains, {});
  });

  test("validates a profiles-only batch and returns the validated entries", () => {

    const result = validateImportedProfiles({


      profiles: {

        myProfile: { extends: "fullscreenApi", staticCapture: true }
      }
    });

    assert.equal(result.valid, true);
    assert.ok(result.profiles["myProfile"], "myProfile included");
  });

  test("collects per-profile errors but still includes valid entries in the result", () => {

    const result = validateImportedProfiles({

      profiles: {

        bad: { extends: "not-a-real-profile" },
        good: { extends: "fullscreenApi" }
      }
    });

    assert.equal(result.valid, false, "any error makes the batch invalid");
    assert.ok(result.profiles["good"], "valid entries are still included so partial imports are possible");
    assert.equal(result.profiles["bad"], undefined, "invalid entry is excluded");
    assert.match(result.errors.join(" "), /non-existent builtin profile/);
  });

  test("rejects domain with hostname that collides with builtin", () => {

    const result = validateImportedProfiles({


      domains: {

        "hulu.com": {}
      }
    });

    assert.equal(result.valid, false);
  });

  test("a domain referencing a profile from the same import batch resolves to that profile", () => {

    /* The cross-reference rule: a referenced name counts as known when it is a builtin, a profile validated earlier in this same batch, or a profile already in
     * the store. A domain mapping that references a profile validated within the SAME batch must resolve cleanly without "non-existent profile" errors. Asserts
     * that in-batch resolution actually fires (an implementation that only recognized builtins would fail here).
     */
    const result = validateImportedProfiles({


      domains: {

        "custom-cross-ref.example": { profile: "newCustomProfile" }
      },
      profiles: {

        newCustomProfile: { extends: "fullscreenApi" }
      }
    });

    assert.equal(result.valid, true, "domain referencing a same-batch profile is valid");
    assert.ok(result.profiles["newCustomProfile"], "profile included in result");
    assert.ok(result.domains["custom-cross-ref.example"], "domain included in result");
  });

  test("a domain referencing a provider profile is accepted", () => {

    /* Mapping a domain onto a provider profile is coherent - the builtin DOMAIN_CONFIG entries do exactly that - so an imported mapping naming one has to
     * validate. disneyNow is absent from the general profile table, so only a check that asks every builtin source can accept this document.
     */
    const result = validateImportedProfiles({

      domains: {

        "provider-ref.example": { profile: "disneyNow" }
      }
    });

    assert.equal(result.valid, true, "a domain naming a provider profile is valid; errors: " + result.errors.join(" "));
    assert.ok(result.domains["provider-ref.example"], "the domain mapping is collected");
  });
});

describe("validateImportedProfiles field-type guards", () => {

  test("reports 'Invalid profiles field' when profiles is present but is an array", () => {

    /* The profiles top-level key is guarded by a plain-object check ((typeof !== "object") || Array.isArray). An array passes the truthiness gate that enters the
     * profiles branch but fails the plain-object shape check, so the batch is rejected with the field-shape error rather than being iterated as entries.
     */
    const result = validateImportedProfiles({ profiles: [] });

    assert.equal(result.valid, false, "an array profiles field invalidates the batch");
    assert.ok(result.errors.some((e) => e.includes("Invalid profiles field")), "expected the profiles field-shape error");
    assert.deepEqual(result.profiles, {}, "no profiles are collected from a malformed profiles field");
  });

  test("reports 'Invalid profiles field' when profiles is present but is a string", () => {

    // A string profiles field fails the typeof === "object" arm of the shape guard, producing the same field-shape error as the array case.
    const result = validateImportedProfiles({ profiles: "not-an-object" });

    assert.equal(result.valid, false, "a string profiles field invalidates the batch");
    assert.ok(result.errors.some((e) => e.includes("Invalid profiles field")), "expected the profiles field-shape error");
    assert.deepEqual(result.profiles, {}, "no profiles are collected from a malformed profiles field");
  });

  test("reports 'Invalid domains field' when domains is present but is an array", () => {

    // The domains top-level key carries the same plain-object guard as profiles; an array enters the domains branch but fails the shape check and is rejected.
    const result = validateImportedProfiles({ domains: [] });

    assert.equal(result.valid, false, "an array domains field invalidates the batch");
    assert.ok(result.errors.some((e) => e.includes("Invalid domains field")), "expected the domains field-shape error");
    assert.deepEqual(result.domains, {}, "no domains are collected from a malformed domains field");
  });

  test("reports 'Invalid domains field' when domains is present but is a string", () => {

    // A string domains field fails the typeof === "object" arm of the guard, producing the field-shape error rather than iterating characters as entries.
    const result = validateImportedProfiles({ domains: "not-an-object" });

    assert.equal(result.valid, false, "a string domains field invalidates the batch");
    assert.ok(result.errors.some((e) => e.includes("Invalid domains field")), "expected the domains field-shape error");
    assert.deepEqual(result.domains, {}, "no domains are collected from a malformed domains field");
  });

  test("skips a single profile that fails validateProfile while collecting every valid sibling", () => {

    /* One profile failing validateProfile must not discard the whole import: the validator continues past the bad entry and still collects the valid siblings.
     * The existing single-sibling test asserts that one good entry survives one bad entry; this strengthens the guarantee to two good siblings so a regression that
     * kept only the first valid entry (or aborted the loop on the first error) surfaces here.
     */
    const result = validateImportedProfiles({

      profiles: {

        bad: { extends: "not-a-real-profile" },
        goodOne: { extends: "fullscreenApi" },
        goodTwo: { extends: "keyboardFullscreen" }
      }
    });

    assert.equal(result.valid, false, "the bad profile makes the batch invalid");
    assert.ok(result.profiles["goodOne"], "first valid sibling collected");
    assert.ok(result.profiles["goodTwo"], "second valid sibling collected past the bad entry");
    assert.equal(result.profiles["bad"], undefined, "the failing profile is skipped, not collected");
    assert.match(result.errors.join(" "), /non-existent builtin profile/, "the skipped profile's validateProfile error is reported");
  });
});
