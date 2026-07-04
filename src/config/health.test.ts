/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * health.test.ts: Unit tests for the channel health and domain auth tracking module. The module owns two pieces of in-memory state (channelHealth and
 * domainAuth maps) backed by debounced writes to health.json. Tests focus on the read/write contract of the public API - including the tri-state domain auth
 * transitions (verified / needsLogin / unknown) and the status-aware TTL exemption; persistence is exercised indirectly via the file store framework's own
 * tests in persistence.test.ts, and the v1-to-v2 schema migration is exercised at the integration tier (test/e2e/persistence/health-state-persistence.test.ts).
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { clearDomainAuthRequirement, flushHealthStateNow, getChannelHealth, getDomainAuthState, getHealthSnapshot, loadHealthState, markChannelFailure,
  markChannelSuccess, markDomainAuth, markDomainAuthRequired, subscribeToHealth } from "./health.ts";
import { firstOf, withTempDir } from "../testing.helpers.ts";
import { getHealthFilePath, initializeDataDir } from "./paths.ts";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

describe("markChannelSuccess", () => {

  beforeEach(() => {

    // We mock Date for deterministic timestamps and setTimeout to suppress the 2-second debounced flushHealthState() write timer. Without setTimeout mocking,
    // the timer would survive the test, attempt a write to (potentially missing) data-dir, and keep the test process alive.
    mock.timers.enable({ apis: [ "Date", "setTimeout" ], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("records success status and timestamp for the channel + domain pair", () => {

    markChannelSuccess("nbc", "nbc.com");

    const result = getChannelHealth("nbc", "nbc.com");

    assert.ok(result, "channel health entry exists");
    assert.equal(result.status, "success");
    assert.equal(result.timestamp, 1_700_000_000_000);
  });

  test("marks the domain as authenticated by default", () => {

    markChannelSuccess("nbc", "nbc.com");

    assert.deepEqual(getDomainAuthState("nbc.com"), { status: "verified", timestamp: 1_700_000_000_000 });
  });

  test("does NOT mark the domain when markAuth=false", () => {

    // Boundary: Sling Freestream channels succeed without proving subscription auth.
    markChannelSuccess("freestream-channel-" + String(Math.random()), "sling-freestream-domain-" + String(Math.random()), false);

    // The domain auth map should not have an entry for our unique synthetic domain.
    assert.equal(getDomainAuthState("never-marked-domain-" + String(Math.random())), null);
  });

  test("emits a healthChanged event with the success payload", () => {

    let captured: unknown;
    const unsubscribe = subscribeToHealth((event) => {

      captured = event;
    });

    try {

      markChannelSuccess("abc", "abc.com");

      assert.deepEqual(captured, { channelKey: "abc", domain: "abc.com", status: "success", timestamp: 1_700_000_000_000 });
    } finally {

      unsubscribe();
    }
  });

  test("emits exactly one channel-scoped event per call, on both the markAuth and non-markAuth paths", () => {

    /* Event-contract pin for the verified write chokepoint: the markAuth branch routes through setDomainVerified, which emits the channel-scoped event on the
     * caller's behalf, while the non-markAuth branch emits directly. If the chokepoint refactor ever double-emitted (markChannelSuccess emitting its own event in
     * addition to the chokepoint's) the markAuth count below would read 2; if the chokepoint dropped the channelKey the payload assertion would catch it. Traced
     * path: markChannelSuccess -> setDomainVerified -> healthEmitter.emit (markAuth), markChannelSuccess -> healthEmitter.emit (non-markAuth).
     */
    const captured: { channelKey: string; domain: string; status: string; timestamp: number }[] = [];
    const unsubscribe = subscribeToHealth((event) => {

      captured.push(event);
    });

    try {

      markChannelSuccess("event-count-markauth", "event-count.com");

      assert.equal(captured.length, 1, "markAuth path emits exactly one event");
      assert.deepEqual(captured[0], { channelKey: "event-count-markauth", domain: "event-count.com", status: "success", timestamp: 1_700_000_000_000 });

      markChannelSuccess("event-count-plain", "event-count.com", false);

      assert.equal(captured.length, 2, "non-markAuth path emits exactly one additional event");
      assert.deepEqual(captured[1], { channelKey: "event-count-plain", domain: "event-count.com", status: "success", timestamp: 1_700_000_000_000 });
    } finally {

      unsubscribe();
    }
  });

  test("overwrites a needs-sign-in entry to verified (success evidence clears the flag)", () => {

    /* Tri-state transition pin: needsLogin -> verified through markChannelSuccess's markAuth path. The scenario enters setDomainVerified with an existing
     * needsLogin entry in the map; the domainAuth.set there is the mutation under test - if it stopped overwriting (e.g., a guard that skipped existing entries),
     * the read below would still see needsLogin.
     */
    markDomainAuthRequired("overwrite-flag.com");

    assert.equal(getDomainAuthState("overwrite-flag.com")?.status, "needsLogin", "precondition: flag set");

    markChannelSuccess("overwrite-flag-channel", "overwrite-flag.com");

    assert.deepEqual(getDomainAuthState("overwrite-flag.com"), { status: "verified", timestamp: 1_700_000_000_000 }, "success evidence overwrites to verified");
  });
});

describe("markChannelFailure", () => {

  beforeEach(() => {

    // We mock Date for deterministic timestamps and setTimeout to suppress the 2-second debounced flushHealthState() write timer. Without setTimeout mocking,
    // the timer would survive the test, attempt a write to (potentially missing) data-dir, and keep the test process alive.
    mock.timers.enable({ apis: [ "Date", "setTimeout" ], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("records failed status without affecting domain auth", () => {

    markChannelFailure("cbs", "cbs.com");

    const result = getChannelHealth("cbs", "cbs.com");

    assert.equal(result?.status, "failed");
    assert.equal(getDomainAuthState("cbs.com"), null, "failure does not mark the domain as authenticated");
  });

  test("leaves an existing needs-sign-in entry untouched (failures never mutate domain auth)", () => {

    /* Behavior-contract pin: markChannelFailure must not touch domain auth in either direction. The scenario seeds a needsLogin entry so a regression that made
     * failures delete or overwrite domain entries (the mutation would live in markChannelFailure's body) is observable - the entry must read back unchanged.
     */
    markDomainAuthRequired("failure-preserves.com");
    markChannelFailure("failure-preserves-channel", "failure-preserves.com");

    assert.deepEqual(getDomainAuthState("failure-preserves.com"), { status: "needsLogin", timestamp: 1_700_000_000_000 }, "needsLogin entry survives a channel failure");
  });

  test("emits a healthChanged event with the failed payload", () => {

    let captured: { status: string } | undefined;
    const unsubscribe = subscribeToHealth((event) => {

      captured = event;
    });

    try {

      markChannelFailure("fox", "fox.com");
      assert.equal(captured?.status, "failed");
    } finally {

      unsubscribe();
    }
  });
});

describe("markDomainAuth", () => {

  beforeEach(() => {

    // We mock Date for deterministic timestamps and setTimeout to suppress the 2-second debounced flushHealthState() write timer. Without setTimeout mocking,
    // the timer would survive the test, attempt a write to (potentially missing) data-dir, and keep the test process alive.
    mock.timers.enable({ apis: [ "Date", "setTimeout" ], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("marks the domain as authenticated without recording any channel state", () => {

    markDomainAuth("just-domain-" + String(Date.now()));

    // Most-recently-marked domain auth is a verified entry stamped exactly Date.now().
    const domain = "just-domain-" + String(Date.now());

    markDomainAuth(domain);
    assert.deepEqual(getDomainAuthState(domain), { status: "verified", timestamp: 1_700_000_000_000 });
  });

  test("emits a healthChanged event with empty channelKey", () => {

    let captured: { channelKey?: string } | undefined;
    const unsubscribe = subscribeToHealth((event) => {

      captured = event;
    });

    try {

      markDomainAuth("nbc.com");
      assert.equal(captured?.channelKey, "");
    } finally {

      unsubscribe();
    }
  });
});

describe("getChannelHealth", () => {

  beforeEach(() => {

    // We mock Date for deterministic timestamps and setTimeout to suppress the 2-second debounced flushHealthState() write timer. Without setTimeout mocking,
    // the timer would survive the test, attempt a write to (potentially missing) data-dir, and keep the test process alive.
    mock.timers.enable({ apis: [ "Date", "setTimeout" ], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("returns null when no entry exists for the key", () => {

    assert.equal(getChannelHealth("never-marked-" + String(Date.now()), "any.com"), null);
  });

  test("returns null when the stored entry is for a different domain", () => {

    markChannelSuccess("rotate-domain-test", "old.com");

    // Domain switched - stored entry is stale relative to the new domain.
    assert.equal(getChannelHealth("rotate-domain-test", "new.com"), null);
  });

  test("returns null when the entry is older than the 7-day TTL", () => {

    markChannelSuccess("ttl-test", "ttl.com");

    // Advance time by 8 days; entry should now be considered expired.
    mock.timers.tick(8 * 24 * 60 * 60 * 1000);

    assert.equal(getChannelHealth("ttl-test", "ttl.com"), null);
  });
});

describe("getDomainAuthState", () => {

  beforeEach(() => {

    // We mock Date for deterministic timestamps and setTimeout to suppress the 2-second debounced flushHealthState() write timer. Without setTimeout mocking,
    // the timer would survive the test, attempt a write to (potentially missing) data-dir, and keep the test process alive.
    mock.timers.enable({ apis: [ "Date", "setTimeout" ], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("returns null when the domain has no recorded auth", () => {

    assert.equal(getDomainAuthState("never-auth-" + String(Date.now())), null);
  });

  test("returns null when a verified entry is older than TTL", () => {

    markDomainAuth("ttl-domain-test");

    mock.timers.tick(8 * 24 * 60 * 60 * 1000);

    assert.equal(getDomainAuthState("ttl-domain-test"), null);
  });

  test("returns a needs-sign-in entry past the TTL window (needsLogin is exempt from expiry)", () => {

    /* TTL-exemption pin for the single-key read path. The scenario ages a needsLogin entry past HEALTH_TTL and reads it: the read enters getDomainAuthState's
     * isDomainAuthExpired guard, whose status === "verified" conjunct is the mutation under test - if the predicate dropped the status check (expiring every
     * entry by age alone), the read would return null and delete the entry.
     */
    markDomainAuthRequired("ttl-exempt-domain-test");

    mock.timers.tick(8 * 24 * 60 * 60 * 1000);

    assert.deepEqual(getDomainAuthState("ttl-exempt-domain-test"), { status: "needsLogin", timestamp: 1_700_000_000_000 },
      "needsLogin entry survives past the TTL with its original timestamp");
  });
});

describe("markDomainAuthRequired", () => {

  beforeEach(() => {

    // We mock Date for deterministic timestamps and setTimeout to suppress the 2-second debounced flushHealthState() write timer. Without setTimeout mocking,
    // the timer would survive the test, attempt a write to (potentially missing) data-dir, and keep the test process alive.
    mock.timers.enable({ apis: [ "Date", "setTimeout" ], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("records a needs-sign-in entry without touching channel health", () => {

    const domain = "needs-login-" + String(Math.random());

    markDomainAuthRequired(domain);

    assert.deepEqual(getDomainAuthState(domain), { status: "needsLogin", timestamp: 1_700_000_000_000 });
  });

  test("overwrites an existing verified entry (a fresh auth wall trumps stale verification)", () => {

    /* Tri-state transition pin: verified -> needsLogin. The scenario enters markDomainAuthRequired with a verified entry already in the map; the unconditional
     * domainAuth.set there is the mutation under test - a guard that preserved existing verified entries would leave the read below green.
     */
    markDomainAuth("wall-after-verified.com");
    markDomainAuthRequired("wall-after-verified.com");

    assert.equal(getDomainAuthState("wall-after-verified.com")?.status, "needsLogin", "the wall observation overwrites the verified entry");
  });

  test("emits a healthChanged event with empty channelKey and the needsLogin status", () => {

    const captured: { channelKey: string; domain: string; status: string; timestamp: number }[] = [];
    const unsubscribe = subscribeToHealth((event) => {

      captured.push(event);
    });

    try {

      markDomainAuthRequired("needs-login-event.com");

      assert.equal(captured.length, 1, "exactly one event per call");
      assert.deepEqual(captured[0], { channelKey: "", domain: "needs-login-event.com", status: "needsLogin", timestamp: 1_700_000_000_000 });
    } finally {

      unsubscribe();
    }
  });
});

describe("clearDomainAuthRequirement", () => {

  beforeEach(() => {

    // We mock Date for deterministic timestamps and setTimeout to suppress the 2-second debounced flushHealthState() write timer. Without setTimeout mocking,
    // the timer would survive the test, attempt a write to (potentially missing) data-dir, and keep the test process alive.
    mock.timers.enable({ apis: [ "Date", "setTimeout" ], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("removes a needs-sign-in entry, returning the domain to unknown, and emits a change event", () => {

    /* Tri-state transition pin: needsLogin -> unknown (the unproven-access clearing). The scenario enters clearDomainAuthRequirement with a needsLogin entry, so
     * the status guard passes and removeDomainAuth's domainAuth.delete is the mutation under test - without the delete, the read below would still see the flag.
     * The event assertion pins the removal notification the health bridge relies on to re-render affected rows.
     */
    const captured: { channelKey: string; domain: string; status: string }[] = [];
    const unsubscribe = subscribeToHealth((event) => {

      captured.push(event);
    });

    try {

      markDomainAuthRequired("clear-flag.com");

      captured.length = 0;

      clearDomainAuthRequirement("clear-flag.com");

      assert.equal(getDomainAuthState("clear-flag.com"), null, "the entry is removed");
      assert.equal(captured.length, 1, "exactly one event per removal");

      const removalEvent = firstOf(captured, "removal event");

      assert.equal(removalEvent.channelKey, "", "removal events carry an empty channelKey");
      assert.equal(removalEvent.domain, "clear-flag.com", "removal events carry the domain");
    } finally {

      unsubscribe();
    }
  });

  test("is a no-op on a verified entry (unproven access never deletes verification)", () => {

    /* The status guard is the mutation under test: the scenario enters clearDomainAuthRequirement with a verified entry, so the needsLogin comparison fails and
     * removeDomainAuth must never run. Without the guard, a Sling-style unvalidated discovery would erase legitimate verified state.
     */
    const captured: unknown[] = [];
    const unsubscribe = subscribeToHealth((event) => {

      captured.push(event);
    });

    try {

      markDomainAuth("clear-verified.com");

      captured.length = 0;

      clearDomainAuthRequirement("clear-verified.com");

      assert.deepEqual(getDomainAuthState("clear-verified.com"), { status: "verified", timestamp: 1_700_000_000_000 }, "verified entry untouched");
      assert.equal(captured.length, 0, "no event fires when nothing changes");
    } finally {

      unsubscribe();
    }
  });

  test("is a no-op on an unknown domain (no entry, no event)", () => {

    const captured: unknown[] = [];
    const unsubscribe = subscribeToHealth((event) => {

      captured.push(event);
    });

    try {

      clearDomainAuthRequirement("never-flagged-" + String(Math.random()));

      assert.equal(captured.length, 0, "no event fires for an absent entry");
    } finally {

      unsubscribe();
    }
  });
});

describe("getHealthSnapshot", () => {

  beforeEach(() => {

    // We mock Date for deterministic timestamps and setTimeout to suppress the 2-second debounced flushHealthState() write timer. Without setTimeout mocking,
    // the timer would survive the test, attempt a write to (potentially missing) data-dir, and keep the test process alive.
    mock.timers.enable({ apis: [ "Date", "setTimeout" ], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("returns the current channels and domains maps", () => {

    markChannelSuccess("snap-channel", "snap.com");
    markDomainAuth("only-domain.com");

    const snapshot = getHealthSnapshot();
    const snapEntry = snapshot.channels["snap-channel"];

    // Lock the shape of the snapshot. The maps are objects keyed by name/domain; domain values are status-bearing entries.
    assert.ok(snapEntry, "snap-channel present in snapshot");
    assert.equal(snapEntry.status, "success");
    assert.deepEqual(snapshot.domains["only-domain.com"], { status: "verified", timestamp: 1_700_000_000_000 });
  });

  test("includes a needs-sign-in entry past the TTL window (needsLogin is exempt from the snapshot prune)", () => {

    /* TTL-exemption pin for the bulk prune path. getHealthSnapshot calls pruneExpiredEntries before serializing; the domains loop's isDomainAuthExpired call is
     * the mutation under test - an age-only predicate there would delete the aged needsLogin entry and the snapshot below would omit it.
     */
    const domain = "snap-needs-login-" + String(Math.random());

    markDomainAuthRequired(domain);

    mock.timers.tick(8 * 24 * 60 * 60 * 1000);

    assert.deepEqual(getHealthSnapshot().domains[domain], { status: "needsLogin", timestamp: 1_700_000_000_000 }, "aged needsLogin entry survives the snapshot prune");
  });

  test("excludes stale entries (older than TTL) from the snapshot", () => {

    markChannelSuccess("stale-channel", "stale.com");

    mock.timers.tick(8 * 24 * 60 * 60 * 1000);

    const snapshot = getHealthSnapshot();

    assert.equal(snapshot.channels["stale-channel"], undefined);
  });
});

describe("subscribeToHealth", () => {

  beforeEach(() => {

    // We mock Date for deterministic timestamps and setTimeout to suppress the 2-second debounced flushHealthState() write timer. Without setTimeout mocking,
    // the timer would survive the test, attempt a write to (potentially missing) data-dir, and keep the test process alive.
    mock.timers.enable({ apis: [ "Date", "setTimeout" ], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("returns an unsubscribe function that detaches the listener", async () => {

    // We use withTempDir to isolate any flush attempts that fire from prior tests.
    await withTempDir((dir) => {

      initializeDataDir(dir);

      let calls = 0;
      const unsubscribe = subscribeToHealth(() => {

        calls++;
      });

      markChannelSuccess("sub-test-1", "x.com");
      assert.equal(calls, 1, "subscribed callback fired");

      unsubscribe();

      markChannelSuccess("sub-test-2", "x.com");
      assert.equal(calls, 1, "after unsubscribe, callback no longer fires");

      return Promise.resolve();
    });
  });
});

describe("expired-entry pruning (memory hygiene)", () => {

  beforeEach(() => {

    // We mock Date for deterministic timestamps and setTimeout to suppress the 2-second debounced flushHealthState() write timer. Without setTimeout mocking,
    // the timer would survive the test, attempt a write to (potentially missing) data-dir, and keep the test process alive.
    mock.timers.enable({ apis: [ "Date", "setTimeout" ], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("getHealthSnapshot excludes expired channel and domain entries from its result", () => {

    // The snapshot is a read-side chokepoint that both filters its own result and prunes the live maps. The directly observable, isolated behavior is the result
    // exclusion: an entry aged past the 7-day TTL must not appear in the returned snapshot. The pruning of the live maps it performs as a side effect is proven on disk
    // by the dedicated flush tests below, which read the persisted file rather than the snapshot result.
    const channelKey = "prune-snap-channel-" + String(Math.random());
    const domainKey = "prune-snap-domain-" + String(Math.random());

    markChannelSuccess(channelKey, domainKey);

    // Age both entries one day past the 7-day TTL so the next snapshot reads them as expired and prunes them.
    mock.timers.tick((7 * 24 * 60 * 60 * 1000) + (24 * 60 * 60 * 1000));

    const expiredSnapshot = getHealthSnapshot();

    assert.equal(expiredSnapshot.channels[channelKey], undefined, "expired channel excluded from snapshot");
    assert.equal(expiredSnapshot.domains[domainKey], undefined, "expired domain excluded from snapshot");
  });

  test("an expired single-key read returns null, and the expired entry is never persisted", async () => {

    /* Pins the observable [27] contract for the single-key read path: an entry aged past the TTL reads back as null and does not survive to disk. We mark, expire,
     * read, then mark a separate fresh key and flush. Whether the touched-expired key is dropped by the read-path delete (getChannelHealth) or by the write-path
     * prune at flush is not independently observable here - with a monotonic clock, anything expired at read time is still expired at flush time, so an on-disk read
     * cannot isolate the two - but both enforce the same no-persist-expired guarantee. The write-path prune is pinned on its own by the flush test below.
     */
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      // Reload clears the module-level maps from any residue left by prior tests so this assertion observes only the keys we mark below.
      await loadHealthState();

      const channelKey = "prune-single-channel-" + String(Math.random());
      const freshChannel = "prune-single-fresh-" + String(Math.random());

      markChannelSuccess(channelKey, "prune-single.com", false);

      mock.timers.tick(8 * 24 * 60 * 60 * 1000);

      // An expired single-key read returns null (and drops the touched key from the live map).
      assert.equal(getChannelHealth(channelKey, "prune-single.com"), null, "expired single-key read returns null");

      // Mark a fresh key inside the current (post-tick) window so the flush has a reason to write and the fresh key survives.
      markChannelSuccess(freshChannel, "fresh.com", false);

      await flushHealthStateNow();

      const written = JSON.parse(await readFile(getHealthFilePath(), "utf8")) as { channels: Record<string, unknown> };

      assert.equal(written.channels[channelKey], undefined, "the expired key is not persisted");
      assert.ok(written.channels[freshChannel], "the fresh key marked after the expired read survives the flush");
    });
  });

  test("an expired single-key domain read returns null, and the expired entry is never persisted", async () => {

    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      await loadHealthState();

      const domainKey = "prune-single-domain-" + String(Math.random());
      const freshDomain = "prune-single-fresh-domain-" + String(Math.random());

      markDomainAuth(domainKey);

      mock.timers.tick(8 * 24 * 60 * 60 * 1000);

      // An expired single-key domain read returns null (and drops the touched key from the live map).
      assert.equal(getDomainAuthState(domainKey), null, "expired single-key domain read returns null");

      markDomainAuth(freshDomain);

      await flushHealthStateNow();

      const written = JSON.parse(await readFile(getHealthFilePath(), "utf8")) as { domains: Record<string, { status: string; timestamp: number }> };

      assert.equal(written.domains[domainKey], undefined, "the expired domain is not persisted");
      assert.ok(written.domains[freshDomain], "the fresh domain marked after the expired read survives the flush");
    });
  });

  test("a needs-sign-in entry aged past the TTL still persists through the flush prune (needsLogin is exempt)", async () => {

    /* TTL-exemption pin for the write-chokepoint prune path. writeHealthState calls pruneExpiredEntries before serializing; the domains loop's isDomainAuthExpired
     * call is the mutation under test - an age-only predicate there would shed the aged needsLogin entry and the on-disk readback below would miss it. A verified
     * entry aged identically is asserted absent in the same write, proving the exemption discriminates on status rather than skipping the prune entirely.
     */
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      await loadHealthState();

      const flaggedDomain = "flush-exempt-needs-login-" + String(Math.random());
      const verifiedDomain = "flush-exempt-verified-" + String(Math.random());

      markDomainAuthRequired(flaggedDomain);
      markDomainAuth(verifiedDomain);

      mock.timers.tick(8 * 24 * 60 * 60 * 1000);

      await flushHealthStateNow();

      const written = JSON.parse(await readFile(getHealthFilePath(), "utf8")) as { domains: Record<string, { status: string; timestamp: number }> };

      assert.deepEqual(written.domains[flaggedDomain], { status: "needsLogin", timestamp: 1_700_000_000_000 }, "aged needsLogin entry persists");
      assert.equal(written.domains[verifiedDomain], undefined, "the identically-aged verified entry is pruned");
    });
  });

  test("flushHealthStateNow does not write expired entries to health.json", async () => {

    // The write chokepoint prunes before serializing, so the on-disk record must never carry stale entries. We load a fresh in-memory state from an isolated data dir,
    // mark one entry that will expire and one that will stay fresh, age past the TTL only for the first, then flush and read the file back. Only the fresh entry may be
    // present on disk.
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      // Reload clears the module-level maps from any residue left by prior tests so this assertion sees only the keys we mark below.
      await loadHealthState();

      const expiredChannel = "flush-expired-channel-" + String(Math.random());
      const freshChannel = "flush-fresh-channel-" + String(Math.random());
      const expiredDomain = "flush-expired-domain-" + String(Math.random());
      const freshDomain = "flush-fresh-domain-" + String(Math.random());

      // Mark the soon-to-expire entries first, then advance the clock past the TTL, then mark the fresh entries so only the latter remain inside the window at flush.
      markChannelSuccess(expiredChannel, expiredDomain);

      mock.timers.tick(8 * 24 * 60 * 60 * 1000);

      markChannelSuccess(freshChannel, freshDomain);

      await flushHealthStateNow();

      const written = JSON.parse(await readFile(getHealthFilePath(), "utf8")) as { channels: Record<string, unknown>; domains: Record<string, unknown> };

      assert.equal(written.channels[expiredChannel], undefined, "expired channel must not be persisted");
      assert.equal(written.domains[expiredDomain], undefined, "expired domain must not be persisted");
      assert.ok(written.channels[freshChannel], "fresh channel persisted");
      assert.ok(written.domains[freshDomain], "fresh domain persisted");
    });
  });
});
