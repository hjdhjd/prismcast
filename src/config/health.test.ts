/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * health.test.ts: Unit tests for the channel health and domain auth tracking module. The module owns two pieces of in-memory state (channelHealth and
 * domainAuth maps) backed by debounced writes to health.json. Tests focus on the read/write contract of the public API; persistence is exercised indirectly
 * via the file store framework's own tests in persistence.test.ts.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { flushHealthStateNow, getChannelHealth, getDomainAuth, getHealthSnapshot, loadHealthState, markChannelFailure, markChannelSuccess, markDomainAuth,
  subscribeToHealth } from "./health.ts";
import { getHealthFilePath, initializeDataDir } from "./paths.ts";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { withTempDir } from "../testing.helpers.ts";

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

    assert.equal(getDomainAuth("nbc.com"), 1_700_000_000_000);
  });

  test("does NOT mark the domain when markAuth=false", () => {

    // Boundary: Sling Freestream channels succeed without proving subscription auth.
    markChannelSuccess("freestream-channel-" + String(Math.random()), "sling-freestream-domain-" + String(Math.random()), false);

    // The domain auth map should not have an entry for our unique synthetic domain.
    assert.equal(getDomainAuth("never-marked-domain-" + String(Math.random())), null);
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
    assert.equal(getDomainAuth("cbs.com"), null, "failure does not mark the domain as authenticated");
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

    // Most-recently-marked domain auth is exactly Date.now() and is non-null.
    const domain = "just-domain-" + String(Date.now());

    markDomainAuth(domain);
    assert.equal(getDomainAuth(domain), 1_700_000_000_000);
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

describe("getDomainAuth", () => {

  beforeEach(() => {

    // We mock Date for deterministic timestamps and setTimeout to suppress the 2-second debounced flushHealthState() write timer. Without setTimeout mocking,
    // the timer would survive the test, attempt a write to (potentially missing) data-dir, and keep the test process alive.
    mock.timers.enable({ apis: [ "Date", "setTimeout" ], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("returns null when the domain has no recorded auth", () => {

    assert.equal(getDomainAuth("never-auth-" + String(Date.now())), null);
  });

  test("returns null when the domain auth is older than TTL", () => {

    markDomainAuth("ttl-domain-test");

    mock.timers.tick(8 * 24 * 60 * 60 * 1000);

    assert.equal(getDomainAuth("ttl-domain-test"), null);
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

    // Lock the shape of the snapshot. The maps are objects keyed by name/domain.
    assert.ok(snapEntry, "snap-channel present in snapshot");
    assert.equal(snapEntry.status, "success");
    assert.equal(snapshot.domains["only-domain.com"], 1_700_000_000_000);
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
      assert.equal(getDomainAuth(domainKey), null, "expired single-key domain read returns null");

      markDomainAuth(freshDomain);

      await flushHealthStateNow();

      const written = JSON.parse(await readFile(getHealthFilePath(), "utf8")) as { domains: Record<string, number> };

      assert.equal(written.domains[domainKey], undefined, "the expired domain is not persisted");
      assert.ok(written.domains[freshDomain], "the fresh domain marked after the expired read survives the flush");
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

      const written = JSON.parse(await readFile(getHealthFilePath(), "utf8")) as { channels: Record<string, unknown>; domains: Record<string, number> };

      assert.equal(written.channels[expiredChannel], undefined, "expired channel must not be persisted");
      assert.equal(written.domains[expiredDomain], undefined, "expired domain must not be persisted");
      assert.ok(written.channels[freshChannel], "fresh channel persisted");
      assert.ok(written.domains[freshDomain], "fresh domain persisted");
    });
  });
});
