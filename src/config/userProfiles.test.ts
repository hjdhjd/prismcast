/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userProfiles.test.ts: Unit tests for the user profiles and domain mappings module. Coverage focuses on the pure validation helpers (validateProfileKey,
 * validateProfile, validateDomain, validateImportedProfiles) and the legacy-flag normalization utility. Persistence (initializeUserProfiles, mutateProfiles)
 * is exercised indirectly via the persistence test layer.
 */
import { describe, test } from "node:test";
import { getUserDomains, getUserProfiles, hasProfilesParseError, normalizeLegacyProfileFlags, validateDomain, validateImportedProfiles, validateProfile,
  validateProfileKey } from "./userProfiles.ts";
import type { SiteProfile } from "../types/index.ts";
import assert from "node:assert/strict";

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

  test("rejects keys that collide with built-in profiles", () => {

    assert.match(validateProfileKey("keyboardFullscreen", false) ?? "", /conflicts with a built-in/);
    assert.match(validateProfileKey("fullscreenApi", false) ?? "", /conflicts with a built-in/);
  });
});

describe("validateProfile", () => {

  test("requires extends field", () => {

    const errors = validateProfile("test", {});

    assert.match(errors[0] ?? "", /extends is required/);
  });

  test("rejects extends pointing to a non-existent built-in profile", () => {

    const errors = validateProfile("test", { extends: "not-a-real-profile" });

    assert.match(errors[0] ?? "", /non-existent built-in profile/);
  });

  test("accepts valid profile extending a general built-in", () => {

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

    assert.ok(errors.some((e) => e.includes("built-in service strategy")));
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
});

describe("validateDomain", () => {

  test("rejects empty domain", () => {

    const errors = validateDomain("", {}, new Set());

    assert.match(errors[0] ?? "", /required/);
  });

  test("rejects domain with invalid hostname format (missing TLD)", () => {

    const errors = validateDomain("nodot", {}, new Set());

    assert.ok(errors.some((e) => e.includes("invalid hostname format")));
  });

  test("accepts a valid domain", () => {

    const errors = validateDomain("custom-site.example", {}, new Set());

    assert.equal(errors.length, 0);
  });

  test("rejects collision with built-in DOMAIN_CONFIG entry", () => {

    const errors = validateDomain("hulu.com", {}, new Set());

    assert.ok(errors.some((e) => e.includes("already mapped to built-in service")));
  });

  test("rejects profile reference that's not in the available set", () => {

    const errors = validateDomain("custom-site.example", { profile: "missing-profile" }, new Set(["other-profile"]));

    assert.ok(errors.some((e) => e.includes("references non-existent profile")));
  });

  test("accepts profile reference that is in the available set", () => {

    const errors = validateDomain("custom-site.example", { profile: "myProfile" }, new Set(["myProfile"]));

    assert.equal(errors.length, 0);
  });

  test("rejects empty service string", () => {

    const errors = validateDomain("custom-site.example", { service: "" }, new Set());

    assert.ok(errors.some((e) => e.includes("service must be a non-empty string")));
  });

  test("rejects empty serviceTag", () => {

    const errors = validateDomain("custom-site.example", { serviceTag: "" }, new Set());

    assert.ok(errors.some((e) => e.includes("serviceTag must be a non-empty string")));
  });

  test("rejects loginUrl that's not a valid URL", () => {

    const errors = validateDomain("custom-site.example", { loginUrl: "not a url" }, new Set());

    assert.ok(errors.some((e) => e.includes("not a valid URL")));
  });

  test("rejects loginUrl with non-http(s) protocol", () => {

    const errors = validateDomain("custom-site.example", { loginUrl: "ftp://example.com" }, new Set());

    assert.ok(errors.some((e) => e.includes("must use http or https")));
  });

  test("accepts a valid http loginUrl", () => {

    const errors = validateDomain("custom-site.example", { loginUrl: "https://login.example.com" }, new Set());

    assert.equal(errors.length, 0);
  });

  test("rejects negative or zero maxContinuousPlayback", () => {

    const errors = validateDomain("custom-site.example", { maxContinuousPlayback: 0 }, new Set());

    assert.ok(errors.some((e) => e.includes("maxContinuousPlayback must be a positive number")));
  });

  test("rejects non-integer videoTimeout", () => {

    const errors = validateDomain("custom-site.example", { videoTimeout: 1.5 }, new Set());

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
    assert.match(result.errors.join(" "), /non-existent built-in profile/);
  });

  test("rejects domain with hostname that collides with built-in", () => {

    const result = validateImportedProfiles({


      domains: {

        "hulu.com": {}
      }
    });

    assert.equal(result.valid, false);
  });
});

describe("getUserProfiles", () => {

  test("returns a fresh object that does not leak module-internal state", () => {

    const a = getUserProfiles();
    const b = getUserProfiles();

    assert.notEqual(a, b, "two calls return distinct references");
  });
});

describe("getUserDomains", () => {

  test("returns a fresh object that does not leak module-internal state", () => {

    const a = getUserDomains();
    const b = getUserDomains();

    assert.notEqual(a, b);
  });
});

describe("hasProfilesParseError", () => {

  test("returns a boolean reflecting the current parse-error state", () => {

    const result = hasProfilesParseError();

    assert.equal(typeof result, "boolean", "always boolean even before initialization");
  });
});
