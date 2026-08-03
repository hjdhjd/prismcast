/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * health.bootGuard.test.ts: Unit tests for the load gate on health-state persistence in health.ts. The gate is what stops a shutdown signal that arrives between
 * the signal handlers being installed and the initial load completing from serializing the still-empty in-memory maps over a populated health.json.
 *
 * These pins live apart from health.test.ts because they need module state that has NEVER seen loadHealthState, and health.test.ts loads in its earlier tests.
 * Node runs each test file in its own process, so a separate file is what buys a never-loaded module...within a single process the gate arms on the first load
 * and stays armed for the rest of the run, which would mask the very behavior the opening tests exist to pin.
 *
 * That standing gate state also fixes the order below: every test needing an unarmed gate comes first, ahead of the ones that load and arm it. A loading test
 * inserted among them makes the tests after it fail loudly on their byte comparison rather than pass while measuring nothing.
 */
import { describe, test } from "node:test";
import { flushHealthStateNow, getChannelHealth, loadHealthState, markChannelSuccess } from "./health.ts";
import { getHealthFilePath, initializeDataDir } from "./paths.ts";
import { readFile, writeFile } from "node:fs/promises";
import assert from "node:assert/strict";
import { withTempDir } from "../testing.helpers.ts";

describe("health state persistence load gate", () => {

  test("a flush that precedes the initial load leaves a populated health.json untouched", async () => {

    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      /* Seed a populated file in the current shape with fresh timestamps so the TTL filter has nothing to say about it, then flush WITHOUT ever loading. The
       * fixture has to carry real entries: an implementation that skipped only empty writes would pass an empty-fixture variant just as well, so the populated
       * content is what tells the designs apart. Byte-comparing the file across the flush is the strongest reading available - the gate skips the write
       * outright, so any write at all shows up here, even one that re-serialized equivalent content in the store's own key order.
       */
      const now = Date.now();
      const seeded = JSON.stringify({

        channels: { "boot-guard-channel": { domain: "bootguard.test", status: "success", timestamp: now } },
        domains: { "bootguard.test": { status: "verified", timestamp: now } },
        schemaVersion: 2
      });

      await writeFile(getHealthFilePath(), seeded, "utf8");

      const before = await readFile(getHealthFilePath(), "utf8");

      await flushHealthStateNow();

      const after = await readFile(getHealthFilePath(), "utf8");

      assert.equal(after, before, "the shutdown flush left health.json byte-identical because the initial load had not run");
    });
  });

  test("a flush that lands mid-load leaves the file untouched and does not disturb the load", async () => {

    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      /* The window here is narrower than the one above: the signal arrives after loadHealthState has started but before its read comes back. We reproduce it by
       * starting the load and deliberately not awaiting it, then flushing. flushHealthStateNow consults the gate synchronously, before it yields, while the load
       * arms the gate only once its read has resolved - so the flush finds an unarmed gate and skips, with no timing assumption to go wrong. Byte-comparing the
       * file is again the strongest read: an implementation that armed the gate before reading would let this flush serialize the empty startup maps, and any
       * write at all differs from the hand-written fixture. Awaiting the load and reading a seeded entry back covers the other direction, so byte-stability
       * bought by breaking the load itself would fail here too.
       */
      const now = Date.now();
      const seeded = JSON.stringify({

        channels: { "boot-guard-midload-channel": { domain: "midload.test", status: "success", timestamp: now } },
        domains: { "midload.test": { status: "verified", timestamp: now } },
        schemaVersion: 2
      });

      await writeFile(getHealthFilePath(), seeded, "utf8");

      const loadPromise = loadHealthState();

      await flushHealthStateNow();
      await loadPromise;

      const after = await readFile(getHealthFilePath(), "utf8");

      assert.equal(after, seeded, "the shutdown flush that landed mid-load left health.json byte-identical");
      assert.deepEqual(getChannelHealth("boot-guard-midload-channel", "midload.test"), { status: "success", timestamp: now },
        "the load still hydrated the maps from the seeded file");
    });
  });

  test("a flush after the initial load persists a mark to disk", async () => {

    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      // An empty directory loads as defaults, and completing that load is what arms persistence. The mark below must then reach disk exactly as it does on a
      // normal boot, which is the other half of the pin above: the gate has to open, not merely close.
      await loadHealthState();

      const channelKey = "boot-guard-armed-" + String(Math.random());

      markChannelSuccess(channelKey, "armed.test");

      await flushHealthStateNow();

      const written = JSON.parse(await readFile(getHealthFilePath(), "utf8")) as { channels: Record<string, unknown> };

      assert.ok(written.channels[channelKey], "the mark reached disk once the completed load had armed persistence");
    });
  });

  test("a load that filters everything out still flushes, shedding the stale entries from disk", async () => {

    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      /* Seed entries well past the TTL - a channel entry and a VERIFIED domain entry, since needs-sign-in entries are exempt from expiry by design and would
       * survive the load. The load drops both, so the maps are genuinely empty while the gate is armed, and the flush that follows must persist that emptiness:
       * a machine that was off for over a week has to shed its stale record rather than keep it alive. An implementation gating on the maps' own emptiness
       * instead of on the load would preserve the stale content here, which is the escape this test exists to catch.
       */
      const expired = Date.now() - (30 * 24 * 60 * 60 * 1000);
      const seeded = JSON.stringify({

        channels: { "boot-guard-stale-channel": { domain: "stale.test", status: "success", timestamp: expired } },
        domains: { "stale.test": { status: "verified", timestamp: expired } },
        schemaVersion: 2
      });

      await writeFile(getHealthFilePath(), seeded, "utf8");

      await loadHealthState();
      await flushHealthStateNow();

      const written = JSON.parse(await readFile(getHealthFilePath(), "utf8")) as { channels: Record<string, unknown>; domains: Record<string, unknown> };

      assert.deepEqual(written.channels, {}, "the expired channel entry was shed from disk");
      assert.deepEqual(written.domains, {}, "the expired domain entry was shed from disk");
    });
  });
});
