/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channelSelection.test.ts: Unit tests for the channel-selection coordinator's pure surface in channelSelection.ts. The module bundles distinct concerns:
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
import type { ChannelStrategyEntry, ResolvedSiteProfile } from "../types/index.ts";
import { EvaluateAbortError, extractDomain } from "../utils/index.ts";
import { clearChannelSelectionCaches, getCachedProviderChannels, getProviderBySlug, getProviderByStrategy, getProviderDomainMap, getProviderGuideUrls,
  getProviderModuleInfo, getProviderSlugs, getProvidersForDomain, invalidateDirectUrl, resolveDirectUrl, selectChannel } from "./channelSelection.ts";
import { describe, test } from "node:test";
import { getPersistedWatchUrl, loadProviderLineups, persistProviderLineup } from "../config/providerLineups.ts";
import type { Page } from "puppeteer-core";
import assert from "node:assert/strict";
import { initializeDataDir } from "../config/paths.ts";
import { isChannelSelectionProfile } from "../types/index.ts";
import { makeProfile } from "../config/profiles.helpers.ts";
import { withTempDir } from "../testing.helpers.ts";

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

  test("tolerates multiple back-to-back invocations", () => {

    // Boundary: browser/index.ts calls this from relinquishBrowserReadiness, which handleBrowserDisconnect and other crash-recovery paths invoke, so it may be
    // called multiple times back to back. Each call must succeed independently.
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

    // Mirrors resolveDirectUrl's null-channelSelector guard. Locks the contract that callers can pass any profile without precondition checks. The cause is a
    // generic failure - one the policy would treat as evidence - so the guard is shown to short-circuit ahead of the retention decision.
    assert.doesNotThrow(() => {

      invalidateDirectUrl(makeProfile({ channelSelection: { strategy: "guideGrid" }, channelSelector: null }), new Error("tune failed"));
    }, "no selector -> no-op");
  });

  test("returns silently when the profile has empty-string channelSelector", () => {

    assert.doesNotThrow(() => {

      invalidateDirectUrl(makeProfile({ channelSelection: { strategy: "guideGrid" }, channelSelector: "" }), new Error("tune failed"));
    }, "empty selector -> no-op");
  });

  test("returns silently for the 'none' strategy", () => {

    // The strategy is not in the registry under key "none" so the optional-chain access produces undefined - the call is a clean no-op.
    assert.doesNotThrow(() => {

      invalidateDirectUrl(makeProfile({ channelSelection: { strategy: "none" }, channelSelector: "anything" }), new Error("tune failed"));
    }, "none strategy -> no-op");
  });
});

/* The retention policy decides whether a tune failure is evidence against the cached watch URL. The cases below drive it through a profile naming a registered
 * strategy and observe the dispatch on that strategy's own invalidator, which is the decision's only visible effect. HBO is incidental: it is simply a
 * registered provider that publishes an invalidateDirectUrl hook.
 */
describe("invalidateDirectUrl retention policy", () => {

  /* Resolves the registered HBO strategy entry - the exact object the coordinator dispatches through for an hboGrid profile, so instrumenting it observes the
   * real decision rather than a stand-in. The assertion establishes the hook is present; the cast then expresses that checked fact for t.mock.method, which
   * requires a method the type system knows is there. node:test restores the method when the case ends, which is what makes instrumenting the shared provider
   * object safe.
   */
  function hboStrategy(): Required<Pick<ChannelStrategyEntry, "invalidateDirectUrl">> {

    const strategy = getProviderBySlug("hbomax")?.strategy;

    assert.ok(strategy?.invalidateDirectUrl, "the HBO strategy publishes an invalidateDirectUrl hook");

    return strategy as Required<Pick<ChannelStrategyEntry, "invalidateDirectUrl">>;
  }

  // A profile whose strategy name routes to the HBO entry, carrying the channelSelector the provider would evict by.
  function hboProfile(): ResolvedSiteProfile {

    return makeProfile({ channelSelection: { strategy: "hboGrid" }, channelSelector: "HBO" });
  }

  test("dispatches to the provider's invalidator for a generic tune failure", (t) => {

    // A failure that reached a verdict on the page it navigated to - the video never appeared - is evidence the cached URL is wrong, so the entry goes.
    const strategy = hboStrategy();
    const invalidate = t.mock.method(strategy, "invalidateDirectUrl", () => { /* Captured via the mock. */ });

    invalidateDirectUrl(hboProfile(), new Error("Waiting for selector `video` failed: timeout 30000ms exceeded"));

    assert.equal(invalidate.mock.calls.length, 1, "a genuine tune failure evicts");
    assert.equal(invalidate.mock.calls[0]?.arguments[0], "HBO", "the provider is told which channel's entry to drop");
  });

  test("retains the cached URL when the frame was detached", (t) => {

    // The incident shape: the page died under the tune, which says nothing about the URL. Evicting here forces every later tune through guide navigation.
    const strategy = hboStrategy();
    const invalidate = t.mock.method(strategy, "invalidateDirectUrl", () => { /* Captured via the mock. */ });

    invalidateDirectUrl(hboProfile(), new Error("Attempted to use detached Frame '5D2393C3BF7A9BFEAB6C38D638EA01D8'"));

    assert.equal(invalidate.mock.calls.length, 0, "a dead frame is not evidence against the URL");
  });

  test("retains the cached URL when the execution context was destroyed", (t) => {

    const strategy = hboStrategy();
    const invalidate = t.mock.method(strategy, "invalidateDirectUrl", () => { /* Captured via the mock. */ });

    invalidateDirectUrl(hboProfile(), new Error("Execution context was destroyed, most likely because of a navigation"));

    assert.equal(invalidate.mock.calls.length, 0, "a torn-down context is not evidence against the URL");
  });

  test("retains the cached URL when the stream was cancelled mid-tune", (t) => {

    // An EvaluateAbortError means this stream was terminated while the tune ran. The tune reached no verdict at all, so the entry survives for the next stream.
    const strategy = hboStrategy();
    const invalidate = t.mock.method(strategy, "invalidateDirectUrl", () => { /* Captured via the mock. */ });

    invalidateDirectUrl(hboProfile(), new EvaluateAbortError());

    assert.equal(invalidate.mock.calls.length, 0, "the stream's own cancellation is not evidence against the URL");
  });
});

describe("exportDurableLineup registration", () => {

  test("only the providers with a session-independent watch address publish the hook", () => {

    /* The structural guarantee behind the whole persisted fallback: a provider that tunes by interacting with its own guide has no address worth carrying across
     * a restart, so it publishes no hook and its persisted slice can only ever hold channel identities. That is what makes the coordinator's fallback inert for
     * those providers rather than conditionally skipped - there is nothing in the store for it to find.
     */
    const exporters = getProviderSlugs().filter((slug) => getProviderBySlug(slug)?.exportDurableLineup !== undefined).sort();

    assert.deepEqual(exporters, [ "hbomax", "spectrum", "yttv" ], "the page-independent providers are the only ones that export a durable lineup");
  });

  test("every published hook reports nothing while its cache is cold", () => {

    // A hook that answered with an empty array on a cold cache would look to the recorder like a statement that the provider carries no channels. Null is the
    // only honest answer before a walk has happened, and the store's own empty-array guard is what would otherwise have to catch the mistake.
    clearChannelSelectionCaches();

    for(const slug of [ "hbomax", "spectrum", "yttv" ]) {

      assert.equal(getProviderBySlug(slug)?.exportDurableLineup?.(), null, slug + " exports nothing before its cache is filled");
    }
  });
});

/* The persisted lineup is the coordinator's second source for a direct watch URL and its second eviction target. The cases below seed the real store against a
 * temp data directory and drive the coordinator's own resolution and invalidation, so what is observed is the coordinator's decision rather than a stand-in for
 * it. Every provider cache is cleared first, so the strategy resolvers have nothing to offer and the fallback is genuinely the thing under test.
 */
describe("resolveDirectUrl persisted fallback", () => {

  test("serves the persisted watch URL when the live cache has nothing", async () => {

    /* The cold-boot path: this browser session has walked no guide, so the provider's own resolver comes back empty and the lineup an earlier session persisted
     * is what lets the tune skip the guide entirely.
     */
    await withTempDir(async (dir) => {

      initializeDataDir(dir);
      clearChannelSelectionCaches();

      await loadProviderLineups();

      await persistProviderLineup("hbomax", [{ channelSelector: "HBO", name: "HBO", watchUrl: "https://play.hbomax.test/channel/watch/cold-boot" }]);

      const resolved = await resolveDirectUrl(makeProfile({ channelSelection: { strategy: "hboGrid" }, channelSelector: "HBO" }), makeFakePage());

      assert.equal(resolved, "https://play.hbomax.test/channel/watch/cold-boot", "the persisted hint stands in for a cache this session never filled");
    });
  });

  test("never crosses providers: a hint persisted under another slug is invisible", async () => {

    /* The guard that keeps the fallback honest. A lineup seeded under a real but different provider proves the slug lookup is doing work - without it, a
     * selector-only match would hand an HBO tune a YouTube TV address, and the assertion would pass vacuously against an empty store.
     */
    await withTempDir(async (dir) => {

      initializeDataDir(dir);
      clearChannelSelectionCaches();

      await loadProviderLineups();

      await persistProviderLineup("yttv", [{ channelSelector: "HBO", name: "HBO", watchUrl: "https://tv.youtube.test/watch/hbo" }]);

      const resolved = await resolveDirectUrl(makeProfile({ channelSelection: { strategy: "hboGrid" }, channelSelector: "HBO" }), makeFakePage());

      assert.equal(resolved, null, "the HBO strategy sees nothing of the YouTube TV slice");
    });
  });

  test("returns null for a strategy no provider registers", async () => {

    // The generic strategies (thumbnailRow, tileClick) belong to no provider, so there is no slug to look a lineup up under and the fallback has nothing to do.
    await withTempDir(async (dir) => {

      initializeDataDir(dir);
      clearChannelSelectionCaches();

      await loadProviderLineups();

      await persistProviderLineup("hbomax", [{ channelSelector: "HBO", name: "HBO", watchUrl: "https://play.hbomax.test/channel/watch/unreachable" }]);

      const resolved = await resolveDirectUrl(makeProfile({ channelSelection: { strategy: "tileClick" }, channelSelector: "HBO" }), makeFakePage());

      assert.equal(resolved, null, "a non-provider strategy has no persisted lineup to reach");
    });
  });
});

describe("invalidateDirectUrl persisted eviction", () => {

  test("evicts the persisted hint and reports the URL-evidence verdict", async () => {

    // One policy statement, two hints. A failure the policy blames on the URL has to reach the durable copy too, or the next boot resurrects exactly the address
    // this tune just proved wrong.
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      await loadProviderLineups();
      await persistProviderLineup("hbomax", [{ channelSelector: "HBO", name: "HBO", watchUrl: "https://play.hbomax.test/channel/watch/discredited" }]);

      const evicted = invalidateDirectUrl(makeProfile({ channelSelection: { strategy: "hboGrid" }, channelSelector: "HBO" }),
        new Error("Waiting for selector `video` failed: timeout 30000ms exceeded"));

      assert.equal(evicted, true, "the coordinator reports that the failure was evidence against the URL");
      assert.equal(getPersistedWatchUrl("hbomax", "HBO"), null, "the durable hint is gone");

      // The eviction's own write is fire-and-forget, and the store runs one mutation at a time, so awaiting a write issued after it drains the queue before this
      // scope removes the directory underneath it.
      await persistProviderLineup("queue-drain-marker", [{ channelSelector: "Marker", name: "Marker" }]);
    });
  });

  test("retains the persisted hint and reports no verdict when the page died", async () => {

    // The retention arm, on the durable copy. A dead page reached no verdict about the URL, and dropping the hint here would force every later boot through the
    // guide navigation an incident is most likely to have broken as well.
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      await loadProviderLineups();
      await persistProviderLineup("hbomax", [{ channelSelector: "HBO", name: "HBO", watchUrl: "https://play.hbomax.test/channel/watch/retained" }]);

      const evicted = invalidateDirectUrl(makeProfile({ channelSelection: { strategy: "hboGrid" }, channelSelector: "HBO" }),
        new Error("Attempted to use detached Frame '5D2393C3BF7A9BFEAB6C38D638EA01D8'"));

      assert.equal(evicted, false, "the coordinator reports that the retention policy kept the entry");
      assert.equal(getPersistedWatchUrl("hbomax", "HBO"), "https://play.hbomax.test/channel/watch/retained", "the durable hint survives");
    });
  });

  test("reports no verdict when there is no channel selector to act on", () => {

    // The guard ahead of the policy. Nothing is evicted and nothing is claimed, so a caller branching on the answer treats it like a retention.
    assert.equal(invalidateDirectUrl(makeProfile({ channelSelection: { strategy: "hboGrid" }, channelSelector: null }), new Error("tune failed")), false,
      "no selector -> no verdict");
  });
});

describe("getCachedProviderChannels persisted fallback", () => {

  test("serves a provider's persisted lineup when this session has walked no guide", async () => {

    /* The channel form's suggestion list, on a boot whose precache came back empty. Without this the list is simply missing every provider channel until a tune
     * or a manual discovery fills a cache, which is exactly the state the incident this feature answers leaves behind.
     */
    await withTempDir(async (dir) => {

      initializeDataDir(dir);
      clearChannelSelectionCaches();

      await loadProviderLineups();

      await persistProviderLineup("hbomax", [{ channelSelector: "HBO", name: "HBO", watchUrl: "https://play.hbomax.test/channel/watch/hbo" }]);

      const group = getCachedProviderChannels().find((entry) => entry.hostname === "play.hbomax.com");

      assert.ok(group, "the provider appears in the suggestion groups on the strength of its persisted lineup alone");
      assert.deepEqual(group.entries, [{ label: "HBO", stationId: undefined, value: "HBO" }], "the persisted rows are projected as label and value pairs");
    });
  });

  test("omits a provider with neither a live cache nor a persisted lineup", async () => {

    // The negative half: the fallback adds a source, it does not invent entries. A provider nothing has ever discovered stays out of the list.
    await withTempDir(async (dir) => {

      initializeDataDir(dir);
      clearChannelSelectionCaches();

      await loadProviderLineups();

      assert.equal(getCachedProviderChannels().length, 0, "no provider has anything to contribute");
    });
  });
});

/* Deferred to e2e (require Puppeteer/Chrome integration):
 *
 * - selectChannel dispatch into strategy.execute for any of the registered providers (every provider tune flow drives page.evaluate, page.waitForSelector,
 *   page.click, page.mouse.click, page.keyboard, etc.).
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
