/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * health.test.ts: Unit tests for the channel health and domain auth tracking module. The module owns two pieces of in-memory state (channelHealth and
 * domainAuth maps) backed by debounced writes to health.json. Tests focus on the read/write contract of the public API; persistence is exercised indirectly
 * via the file store framework's own tests in persistence.test.ts.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { getChannelHealth, getDomainAuth, getHealthSnapshot, markChannelFailure, markChannelSuccess, markDomainAuth, subscribeToHealth } from "./health.ts";
import assert from "node:assert/strict";
import { initializeDataDir } from "./paths.ts";
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
