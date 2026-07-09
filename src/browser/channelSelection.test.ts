/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channelSelection.test.ts: Unit tests for the channel-selection coordinator's pure surface in channelSelection.ts. The module bundles two layers:
 *
 *   1. The provider-module registry plus its lookup helpers (getProviderBySlug, getProviderByStrategy, getProviderSlugs, getProviderModuleInfo,
 *      getProviderDomainMap, getProviderGuideUrls, getCachedProviderChannels, clearChannelSelectionCaches). These are testable as pure data accessors against the
 *      registered provider modules - the registry is built at module evaluation time and is stable across tests.
 *
 *   2. The selectChannel coordinator and its companions resolveDirectUrl/invalidateDirectUrl. The bulk of selectChannel drives Puppeteer through provider-specific
 *      tuning strategies and is deferred to e2e. The unit tests here cover the no-op short-circuit branches: strategy "none" and missing channelSelector.
 *
 * Strategy execute paths that drive Chrome (every provider's tune flow, scroll-to-bottom and matchSelector polling, category resolution against page DOM) are
 * outside the unit-test scope and listed in the deferred-coverage block at the bottom of this file.
 */
import { clearChannelSelectionCaches, getCachedProviderChannels, getProviderBySlug, getProviderByStrategy, getProviderDomainMap, getProviderGuideUrls,
  getProviderModuleInfo, getProviderSlugs, getProvidersForDomain, invalidateDirectUrl, resolveDirectUrl, selectChannel } from "./channelSelection.ts";
import { describe, test } from "node:test";
import type { Page } from "puppeteer-core";
import assert from "node:assert/strict";
import { extractDomain } from "../utils/index.ts";
import { isChannelSelectionProfile } from "../types/index.ts";
import { makeProfile } from "../config/profiles.helpers.ts";

/* makeProfile builds a ResolvedSiteProfile literal with all required fields populated. selectChannel only inspects channelSelection and channelSelector; the rest
 * are present to satisfy the interface. Tests override only the fields they care about.
 */

/* makeFakePage returns a minimal Page stub - selectChannel's no-op branches don't touch the page, but the function signature requires a Page reference. The cast
 * through unknown bypasses the Puppeteer Page interface.
 */
function makeFakePage(): Page {

  return {} as unknown as Page;
}

describe("getProviderBySlug", () => {

  test("returns the provider module for each registered slug", () => {

    // Iterating the slug list rather than hard-coding pairs keeps this test stable when providers are added or relabeled. The slug-to-slug round-trip is the
    // contract: if a slug is registered, the lookup must return a module whose slug matches.
    for(const slug of getProviderSlugs()) {

      const provider = getProviderBySlug(slug);

      assert.ok(provider, "provider exists for slug " + slug);
      assert.equal(provider.slug, slug, "returned module's slug matches the lookup");
    }
  });

  test("returns undefined for an unknown slug", () => {

    assert.equal(getProviderBySlug("never-registered"), undefined, "unknown slug -> undefined");
  });

  test("returns undefined for an empty slug", () => {

    // Boundary: an empty string is not a valid slug. The lookup falls through Array.find without matching.
    assert.equal(getProviderBySlug(""), undefined, "empty slug -> undefined");
  });
});

describe("getProviderByStrategy", () => {

  test("returns the provider module whose strategyName matches", () => {

    // Iterate the slug list, look up each module, then verify the round-trip from strategyName back to the same module.
    for(const slug of getProviderSlugs()) {

      const bySlug = getProviderBySlug(slug);

      assert.ok(bySlug, "slug lookup succeeded");

      const byStrategy = getProviderByStrategy(bySlug.strategyName);

      assert.equal(byStrategy?.slug, slug, "strategyName lookup returns the same provider");
    }
  });

  test("returns undefined for a generic strategy name (thumbnailRow is not a provider)", () => {

    // Negative test: thumbnailRow and tileClick are bare strategies registered alongside provider modules but not associated with a ProviderModule. The lookup
    // must report undefined so callers know to fall through.
    assert.equal(getProviderByStrategy("thumbnailRow"), undefined, "thumbnailRow is not a provider");
    assert.equal(getProviderByStrategy("tileClick"), undefined, "tileClick is not a provider");
  });

  test("returns undefined for an unknown strategy name", () => {

    assert.equal(getProviderByStrategy("never-registered-strategy"), undefined, "unknown strategy -> undefined");
  });
});

describe("getProviderSlugs", () => {

  test("returns a non-empty array of unique slugs", () => {

    const slugs = getProviderSlugs();

    assert.ok(slugs.length > 0, "at least one provider registered");
    assert.equal(slugs.length, new Set(slugs).size, "every slug is unique");
  });

  test("includes the documented core providers (sanity check on the registry)", () => {

    // We assert a stable subset rather than an exact list so adding a provider does not break the test. The core providers below have been part of the registry
    // since the multi-provider system landed.
    const slugs = new Set(getProviderSlugs());

    for(const expected of [ "cox", "directv", "hbomax", "hulu", "sling", "spectrum", "xfinity", "yttv" ]) {

      assert.ok(slugs.has(expected), "registered slug includes " + expected);
    }
  });
});

describe("getProviderModuleInfo", () => {

  test("returns one entry per provider with slug, label, and domain populated", () => {

    const info = getProviderModuleInfo();

    assert.equal(info.length, getProviderSlugs().length, "one entry per provider");

    for(const entry of info) {

      assert.equal(typeof entry.slug, "string", "slug is a string");
      assert.equal(typeof entry.label, "string", "label is a string");
      assert.equal(typeof entry.domain, "string", "domain is a string");
      assert.ok(entry.slug.length > 0, "slug is non-empty");
      assert.ok(entry.label.length > 0, "label is non-empty");
      assert.ok(entry.domain.length > 0, "domain is non-empty");
    }
  });

  test("derives the domain from the provider's guide URL hostname", () => {

    // The contract is that domain matches the hostname of the corresponding provider's guideUrl. We verify by looking up the provider and comparing.
    for(const entry of getProviderModuleInfo()) {

      const provider = getProviderBySlug(entry.slug);

      assert.ok(provider, "provider lookup succeeded for " + entry.slug);

      const expectedHost = new URL(provider.guideUrl).hostname;

      assert.equal(entry.domain, expectedHost, "domain matches guideUrl hostname for " + entry.slug);
    }
  });
});

describe("getProviderDomainMap", () => {

  test("maps each provider's hostname to its slug", () => {

    const map = getProviderDomainMap();

    for(const slug of getProviderSlugs()) {

      const provider = getProviderBySlug(slug);

      assert.ok(provider, "provider lookup succeeded");

      const host = new URL(provider.guideUrl).hostname;

      assert.equal(map[host], slug, "hostname " + host + " maps to slug " + slug);
    }
  });

  test("the result has exactly one entry per provider (no duplicate hostnames)", () => {

    const map = getProviderDomainMap();

    assert.equal(Object.keys(map).length, getProviderSlugs().length, "one mapping per provider");
  });
});

describe("getProviderGuideUrls", () => {

  test("maps each slug to its registered guide URL", () => {

    const urls = getProviderGuideUrls();

    for(const slug of getProviderSlugs()) {

      const provider = getProviderBySlug(slug);

      assert.ok(provider, "provider lookup succeeded");
      assert.equal(urls[slug], provider.guideUrl, "guide URL matches for slug " + slug);
    }
  });

  test("returns one entry per provider", () => {

    assert.equal(Object.keys(getProviderGuideUrls()).length, getProviderSlugs().length, "one URL per provider");
  });
});

describe("getProvidersForDomain", () => {

  test("returns exactly the single provider whose guide lives on the given domain", () => {

    // sling.com is the registrable domain of the sling provider's guide URL (watch.sling.com/...). extractDomain collapses the "watch." subdomain, so the lookup
    // must return precisely one provider and it must be sling.
    const matches = getProvidersForDomain("sling.com");

    assert.equal(matches.length, 1, "sling.com maps to exactly one provider");
    assert.equal(matches[0]?.slug, "sling", "the single match is the sling provider");
  });

  test("returns an empty array when no registered provider's guide lives on the domain", () => {

    // A domain no provider registers must yield an empty array, never undefined. This is the contract callers rely on to iterate the result unconditionally.
    const matches = getProvidersForDomain("no-such-provider.example");

    assert.ok(Array.isArray(matches), "result is an array");
    assert.equal(matches.length, 0, "unregistered domain -> empty array");
  });

  test("every returned provider extracts to the requested domain, for each provider's own domain", () => {

    // Property check across the whole registry: for each provider, look up by its own guide-URL domain. The result must include that provider, and every provider
    // in the result must itself extract to the requested domain (never a stray registration on a different domain). This locks the "exactly the modules whose
    // extractDomain(guideUrl) equals the argument" contract without hard-coding the provider list.
    for(const slug of getProviderSlugs()) {

      const provider = getProviderBySlug(slug);

      assert.ok(provider, "provider lookup succeeded for " + slug);

      const domain = extractDomain(provider.guideUrl);
      const matches = getProvidersForDomain(domain);

      assert.ok(matches.some((p) => p.slug === slug), "domain " + domain + " includes provider " + slug);

      for(const match of matches) {

        assert.equal(extractDomain(match.guideUrl), domain, "every match for " + domain + " extracts to that domain");
      }
    }
  });

  test("returns an empty array for the empty-string domain (no provider guide is domain-less)", () => {

    // Boundary: the empty string is not the registrable domain of any provider guide URL, so the filter matches nothing.
    assert.deepEqual(getProvidersForDomain(""), [], "empty domain -> empty array");
  });
});

describe("getCachedProviderChannels", () => {

  test("returns an array (possibly empty) of cached channel groups", () => {

    // The cache is populated by tune flows and precaching - in a unit-test environment we expect either zero entries (no provider has cached) or a small number
    // from previous test runs in the same process. The contract is that the function does not throw and the result is iterable.
    const cached = getCachedProviderChannels();

    assert.ok(Array.isArray(cached), "result is an array");
  });

  test("each entry has hostname and entries fields when present", () => {

    // Boundary: when an entry exists, it must have the documented shape. Even with no entries this test passes; with entries the shape is locked.
    for(const group of getCachedProviderChannels()) {

      assert.equal(typeof group.hostname, "string", "hostname is a string");
      assert.ok(Array.isArray(group.entries), "entries is an array");

      for(const entry of group.entries) {

        assert.equal(typeof entry.label, "string", "entry label is a string");
        assert.equal(typeof entry.value, "string", "entry value is a string");
      }
    }
  });
});

describe("clearChannelSelectionCaches", () => {

  test("does not throw when called against the registry with no populated caches", () => {

    // Negative test: clearChannelSelectionCaches iterates every strategy's optional clearCache hook. Even when no caches are populated, the call must be a clean
    // no-op. We exercise the iteration to lock the contract that the optional-chain handles strategies without clearCache.
    assert.doesNotThrow(() => {

      clearChannelSelectionCaches();
    }, "clearing empty caches must not throw");
  });

  test("is safely idempotent across multiple back-to-back invocations", () => {

    // Boundary: callers (browser/index.ts handleBrowserDisconnect) may invoke this multiple times during a crash recovery sequence. Each call must succeed
    // independently.
    for(let i = 0; i < 3; i++) {

      assert.doesNotThrow(() => {

        clearChannelSelectionCaches();
      }, "iteration " + String(i + 1));
    }
  });
});

describe("isChannelSelectionProfile (re-checked here since selectChannel relies on it)", () => {

  test("returns false when channelSelector is null", () => {

    assert.equal(isChannelSelectionProfile(makeProfile({ channelSelector: null })), false, "null selector -> not a selection profile");
  });

  test("returns false when channelSelector is the empty string", () => {

    // Boundary: empty string is rejected the same way null is, mirroring the original truthiness check.
    assert.equal(isChannelSelectionProfile(makeProfile({ channelSelector: "" })), false, "empty string selector -> not a selection profile");
  });

  test("returns true when channelSelector is a non-empty string", () => {

    assert.equal(isChannelSelectionProfile(makeProfile({ channelSelector: "ESPN" })), true, "non-empty selector -> is a selection profile");
  });
});

describe("selectChannel", () => {

  test("returns success immediately when the strategy is 'none' (single-channel sites)", async () => {

    // Happy path 1: the no-op short-circuit. Strategy "none" means there is nothing to select - the caller already navigated to the right page.
    const result = await selectChannel(makeFakePage(), makeProfile({ channelSelection: { strategy: "none" }, channelSelector: "anything" }));

    assert.deepEqual(result, { success: true }, "strategy 'none' returns immediate success");
  });

  test("returns success immediately when channelSelector is null (no target to select)", async () => {

    // Happy path 2: even when a strategy is configured, missing a channelSelector means the caller is not routing to a specific channel. The coordinator
    // short-circuits to success without dispatching to the strategy. Mirrors the !isChannelSelectionProfile path.
    const result = await selectChannel(makeFakePage(), makeProfile({ channelSelection: { strategy: "guideGrid" }, channelSelector: null }));

    assert.deepEqual(result, { success: true }, "missing channelSelector returns immediate success");
  });

  test("returns success immediately when channelSelector is the empty string", async () => {

    // Boundary: empty string is treated as missing by isChannelSelectionProfile and produces the same short-circuit.
    const result = await selectChannel(makeFakePage(), makeProfile({ channelSelection: { strategy: "guideGrid" }, channelSelector: "" }));

    assert.deepEqual(result, { success: true }, "empty channelSelector returns immediate success");
  });
});

describe("resolveDirectUrl", () => {

  test("returns null when the profile has no channelSelector (nothing to resolve)", async () => {

    // Negative test: without a channelSelector there is no identity to look up. The coordinator must return null without calling into any strategy.
    const result = await resolveDirectUrl(makeProfile({ channelSelection: { strategy: "guideGrid" }, channelSelector: null }), makeFakePage());

    assert.equal(result, null, "no selector -> null");
  });

  test("returns null when the profile has empty-string channelSelector", async () => {

    // The check is `if(!channelSelector)` which rejects both null and empty. We lock the contract for the empty-string case.
    const result = await resolveDirectUrl(makeProfile({ channelSelection: { strategy: "guideGrid" }, channelSelector: "" }), makeFakePage());

    assert.equal(result, null, "empty selector -> null");
  });

  test("returns null when the strategy is 'none' (no resolver registered)", async () => {

    // The "none" strategy is not in the strategies registry under that key (only providers contribute to the registry). The optional-chain access returns null.
    const result = await resolveDirectUrl(makeProfile({ channelSelection: { strategy: "none" }, channelSelector: "anything" }), makeFakePage());

    assert.equal(result, null, "no strategy entry -> null");
  });
});

describe("invalidateDirectUrl", () => {

  test("returns silently when the profile has no channelSelector (no-op)", () => {

    // Mirrors resolveDirectUrl's null-channelSelector guard. Locks the contract that callers can pass any profile without precondition checks.
    assert.doesNotThrow(() => {

      invalidateDirectUrl(makeProfile({ channelSelection: { strategy: "guideGrid" }, channelSelector: null }));
    }, "no selector -> no-op");
  });

  test("returns silently when the profile has empty-string channelSelector", () => {

    assert.doesNotThrow(() => {

      invalidateDirectUrl(makeProfile({ channelSelection: { strategy: "guideGrid" }, channelSelector: "" }));
    }, "empty selector -> no-op");
  });

  test("returns silently for the 'none' strategy", () => {

    // The strategy is not in the registry under key "none" so the optional-chain access produces undefined - the call is a clean no-op.
    assert.doesNotThrow(() => {

      invalidateDirectUrl(makeProfile({ channelSelection: { strategy: "none" }, channelSelector: "anything" }));
    }, "none strategy -> no-op");
  });
});

/* Deferred to e2e (require Puppeteer/Chrome integration):
 *
 * - selectChannel dispatch into strategy.execute for any of the registered providers (every provider tune flow drives page.evaluate, page.waitForSelector,
 *   page.click, page.bringToFront, page.mouse.click, page.keyboard, etc.).
 *
 * - selectChannel scroll-to-bottom and scrollSelector/scrollTarget paths (both call page.evaluate, page.waitForSelector, page.keyboard.press).
 *
 * - selectChannel matchSelector poll (page.waitForFunction + DOM measurement).
 *
 * - selectChannel category resolution path (categoryResolution.resolve drives provider DOM/API logic against a real page).
 *
 * - resolveDirectUrl/invalidateDirectUrl positive paths (require a real strategy.resolveDirectUrl which performs response interception or async API calls against
 *   the live provider).
 *
 * - clearChannelSelectionCaches positive observation (asserting that caches were actually populated and then cleared - populating requires a real tune).
 *
 * The shared utilities re-exported through this module (logAvailableChannels, normalizeChannelName, resolveMatchSelector, scrollAndClick) live in
 * tuning/shared.ts and are part of the e2e-only tuning surface.
 */
