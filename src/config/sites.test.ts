/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * sites.test.ts: Unit tests for the site profile and domain mapping module. The module is the SSOT for builtin profile data, the lookup precedence between
 * full-hostname and concise-domain matching, and the provider-module profile registry. We exercise every lookup path and the profile registration guards.
 */
import { DOMAIN_CONFIG, PROVIDER_PROFILES, SITE_PROFILES, getBuiltinProfile, getDomainConfig, getRegisteredProviderModuleProfiles, isProviderProfile,
  registerProviderModuleProfile } from "./sites.ts";
import { describe, test } from "node:test";
import type { SiteProfile } from "../types/index.ts";
import assert from "node:assert/strict";

describe("SITE_PROFILES", () => {

  test("declares the documented base profiles (keyboardFullscreen, fullscreenApi, staticPage)", () => {

    assert.ok(SITE_PROFILES["keyboardFullscreen"], "keyboardFullscreen present");
    assert.ok(SITE_PROFILES["fullscreenApi"], "fullscreenApi present");
    assert.ok(SITE_PROFILES["staticPage"], "staticPage present");
  });

  test("derived profiles set the correct extends target", () => {

    // Locking the inheritance chain prevents accidental rewiring during refactors.
    assert.equal(SITE_PROFILES["keyboardDynamic"]?.extends, "keyboardFullscreen");
    assert.equal(SITE_PROFILES["embeddedPlayer"]?.extends, "fullscreenApi");
    assert.equal(SITE_PROFILES["embeddedDynamicMultiVideo"]?.extends, "embeddedPlayer");
  });

  test("every profile carries description and summary metadata for UI surfacing", () => {

    for(const [ name, profile ] of Object.entries(SITE_PROFILES)) {

      assert.ok(profile.description, name + " has description");
      assert.ok(profile.summary, name + " has summary");
    }
  });
});

describe("PROVIDER_PROFILES", () => {

  test("contains the static provider entries (disneyNow, disneyPlus)", () => {

    assert.ok(PROVIDER_PROFILES["disneyNow"], "disneyNow present");
    assert.ok(PROVIDER_PROFILES["disneyPlus"], "disneyPlus present");
  });
});

describe("DOMAIN_CONFIG", () => {

  test("maps full hostnames to subdomain-specific entries (tv.youtube.com -> youtubeTV)", () => {

    assert.equal(DOMAIN_CONFIG["tv.youtube.com"]?.profile, "youtubeTV");
  });

  test("maps concise domains to base profiles (youtube.com -> keyboardDynamic)", () => {

    assert.equal(DOMAIN_CONFIG["youtube.com"]?.profile, "keyboardDynamic");
  });

  test("entries with serviceTag participate in the service filter", () => {

    assert.equal(DOMAIN_CONFIG["hulu.com"]?.serviceTag, "hulu");
    assert.equal(DOMAIN_CONFIG["disneyplus.com"]?.serviceTag, "disneyplus");
  });
});

describe("getDomainConfig", () => {

  test("resolves a known full hostname before the concise domain", () => {

    // tv.youtube.com -> youtubeTV must win over youtube.com -> keyboardDynamic for the same URL.
    const result = getDomainConfig("https://tv.youtube.com/watch/abc");

    assert.equal(result?.profile, "youtubeTV", "subdomain-specific entry wins");
  });

  test("falls back to the concise domain when the full hostname has no entry", () => {

    const result = getDomainConfig("https://www.hulu.com/live");

    assert.equal(result?.profile, "huluLive");
  });

  test("returns the matching entry verbatim (DomainConfig fields preserved)", () => {

    const result = getDomainConfig("https://watch.spectrum.net/live");

    assert.ok(result, "watch.spectrum.net resolves to a domain config");
    assert.equal(result.profile, "spectrum");
    assert.equal(result.service, "Spectrum TV");
    assert.equal(result.serviceTag, "spectrum");
  });

  test("returns undefined for an unknown domain", () => {

    assert.equal(getDomainConfig("https://example.example/live"), undefined);
  });

  test("returns undefined for an unparseable URL", () => {

    // Boundary: new URL throws; the catch falls through to extractDomain (returns the input verbatim) which then misses the lookup.
    assert.equal(getDomainConfig("not a url at all"), undefined);
  });

  test("returns undefined for an empty string", () => {

    assert.equal(getDomainConfig(""), undefined);
  });
});

describe("getBuiltinProfile", () => {

  test("returns a SITE_PROFILES entry by name", () => {

    const result = getBuiltinProfile("keyboardFullscreen");

    assert.equal(result, SITE_PROFILES["keyboardFullscreen"]);
  });

  test("returns a PROVIDER_PROFILES entry by name (falls through SITE_PROFILES first)", () => {

    const result = getBuiltinProfile("disneyNow");

    assert.equal(result, PROVIDER_PROFILES["disneyNow"]);
  });

  test("returns undefined for an unknown profile name", () => {

    assert.equal(getBuiltinProfile("not-a-profile"), undefined);
  });

  test("returns undefined for an empty string", () => {

    assert.equal(getBuiltinProfile(""), undefined);
  });
});

describe("isProviderProfile", () => {

  test("returns true for static PROVIDER_PROFILES entries", () => {

    assert.equal(isProviderProfile("disneyNow"), true);
    assert.equal(isProviderProfile("disneyPlus"), true);
  });

  test("returns false for general SITE_PROFILES entries", () => {

    assert.equal(isProviderProfile("keyboardFullscreen"), false);
    assert.equal(isProviderProfile("fullscreenApi"), false);
  });

  test("returns false for unknown profile names", () => {

    assert.equal(isProviderProfile("not-a-profile"), false);
  });
});

describe("registerProviderModuleProfile", () => {

  test("registers a new profile so getBuiltinProfile and isProviderProfile both find it", () => {

    const name = "test-provider-" + String(Date.now()) + "-" + String(Math.random());
    const profile: SiteProfile = { description: "test", extends: "fullscreenApi", summary: "test" };

    registerProviderModuleProfile(name, profile);

    assert.equal(getBuiltinProfile(name), profile, "registered profile resolves via getBuiltinProfile");
    assert.equal(isProviderProfile(name), true, "registered profile is recognized as a provider profile");

    // The dynamic registry has no public deregister helper; the suite's profile names embed Date.now/Math.random to avoid collisions across runs.
  });

  test("throws when the name collides with a static SITE_PROFILES entry", () => {

    assert.throws(() => {


      registerProviderModuleProfile("keyboardFullscreen",
        { description: "x", summary: "x" });
    }, /collides with an existing static profile/);
  });

  test("throws when the name collides with a static PROVIDER_PROFILES entry", () => {

    assert.throws(() => { registerProviderModuleProfile("disneyNow", { description: "x", summary: "x" }); },
      /collides with an existing static profile/);
  });
});

describe("getRegisteredProviderModuleProfiles", () => {

  test("yields entries in [name, profile] tuple form", () => {

    // Register a known marker so the iteration has at least one identifiable entry from this test's perspective.
    const name = "iter-marker-" + String(Date.now()) + "-" + String(Math.random());
    const profile: SiteProfile = { description: "iter", extends: "fullscreenApi", summary: "iter" };

    registerProviderModuleProfile(name, profile);

    const entries = [...getRegisteredProviderModuleProfiles()];
    const found = entries.find(([key]) => key === name);

    assert.ok(found, "iteration yields the registered marker entry");
    assert.equal(found[1], profile, "tuple value is the registered profile");
  });
});
