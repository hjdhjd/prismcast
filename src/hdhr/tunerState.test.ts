/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tunerState.test.ts: Unit tests for the HDHR tuner-slot SSOT. The module's contract is:
 *
 *   1. Returns max(CONFIG.streaming.maxConcurrentStreams, activeStreams) entries, indexed from zero - exactly maxConcurrentStreams in the normal case, and the
 *      active count (with no idle slots) when oversubscribed.
 *   2. Active slots come first in ascending stream-id order; idle slots fill the remainder.
 *   3. Channel-info merge prefers the channel map (channelName + channelNumber), falls back to stream.channelName (channelName only), or both null when neither.
 *   4. Client address is normalized via normalizeClientAddress when present, null otherwise.
 *
 * The tests run against a real Map-backed channel lookup (no mocking of buildChannelMap) because the registry is in-process state and behaves correctly under
 * register/unregister scaffolding. Each test isolates its stream registrations in try/finally so a failure in one test cannot strand a stream in the registry
 * for the next test.
 */
import { afterEach, describe, test } from "node:test";
import { registerStream, unregisterStream } from "../streaming/registry.ts";
import { CONFIG } from "../config/index.ts";
import assert from "node:assert/strict";
import { buildChannelMap } from "./channelMap.ts";
import { firstOf } from "../testing.helpers.ts";
import { getTunerStates } from "./tunerState.ts";
import { makeRegistryEntry } from "../streaming/registry.helpers.ts";

// Stream IDs in the test range are chosen well above any real stream id to avoid collision; the registry is keyed by id so deterministic test ids are safe.
const TEST_ID_BASE = 2_000_000;

describe("getTunerStates", () => {

  // Tracks ids registered during a test so afterEach can unwind even if the test body threw before its own cleanup ran.
  const registeredIds: number[] = [];

  afterEach(() => {

    while(registeredIds.length > 0) {

      const id = registeredIds.pop();

      if(id !== undefined) {

        try {

          unregisterStream(id);
        } catch {

          // Already unregistered by the test body - swallow.
        }
      }
    }
  });

  test("returns exactly maxConcurrentStreams entries when no streams are active", () => {

    const states = getTunerStates();

    assert.equal(states.length, CONFIG.streaming.maxConcurrentStreams, "slot count matches configured limit");

    for(const [ slot, state ] of states.entries()) {

      assert.equal(state.slot, slot, "slot index reflected on the record");
      assert.equal(state.resource, "tuner" + String(slot), "resource id derived from slot");
      assert.equal(state.active, false, "all slots idle");
      assert.equal(state.channelName, null);
      assert.equal(state.channelNumber, null);
      assert.equal(state.clientAddress, null);
    }
  });

  test("an active stream with a known channel populates name and number from the channel map", () => {

    // The channel map is the SSOT for both the display name and the numeric channel number; pull the expected values from it rather than from the channel
    // record directly because channel numbers are auto-assigned by buildChannelMap when not explicit on the channel definition.
    const map = buildChannelMap();
    const expectedEntry = firstOf(map, "channel map entry");

    const id = TEST_ID_BASE + 1;
    const entry = makeRegistryEntry({ id, info: { lastPlaylistRequest: 0, storeKey: expectedEntry.key } });

    registerStream(entry);
    registeredIds.push(id);

    const states = getTunerStates();
    const active = states[0];

    assert.ok(active, "slot zero exists");
    assert.equal(active.active, true);
    assert.equal(active.resource, "tuner0");
    assert.equal(active.channelName, expectedEntry.name);
    assert.equal(active.channelNumber, expectedEntry.number);
  });

  test("falls back to stream.channelName when the storeKey is missing from the channel map", () => {

    const id = TEST_ID_BASE + 2;
    const entry = makeRegistryEntry({

      channelName: "Removed Channel",
      id,
      info: { lastPlaylistRequest: 0, storeKey: "definitely-not-a-real-channel-key" }
    });

    registerStream(entry);
    registeredIds.push(id);

    const active = getTunerStates()[0];

    assert.ok(active);
    assert.equal(active.channelName, "Removed Channel", "fallback name surfaces");
    assert.equal(active.channelNumber, null, "no number when channel is missing from the map");
  });

  test("normalizes clientAddress: ::ffff:-prefixed IPv4 strips the prefix, plain IPv4 passes through, null is preserved", () => {

    const cases = [
      { address: "::ffff:192.168.1.42", expected: "192.168.1.42", id: TEST_ID_BASE + 3 },
      { address: "10.0.0.1", expected: "10.0.0.1", id: TEST_ID_BASE + 4 },
      { address: null, expected: null, id: TEST_ID_BASE + 5 }
    ] as const;

    for(const c of cases) {

      const entry = makeRegistryEntry({

        clientAddress: c.address,
        id: c.id,
        info: { lastPlaylistRequest: 0, storeKey: "definitely-not-a-real-channel-key" }
      });

      registerStream(entry);
      registeredIds.push(c.id);
    }

    const active = getTunerStates().filter((s) => s.active);

    assert.equal(active.length, 3, "all three test streams active");

    // The registry sorts by ascending id; our cases array is in id order, so positions align.
    for(const [ i, c ] of cases.entries()) {

      assert.equal(active[i]?.clientAddress, c.expected, "case " + String(i) + ": " + String(c.address));
    }
  });

  test("active streams take the first slots in ascending stream-id order; idle slots fill the rest", () => {

    const ids = [ TEST_ID_BASE + 12, TEST_ID_BASE + 10, TEST_ID_BASE + 11 ];

    for(const id of ids) {

      const entry = makeRegistryEntry({ id, info: { lastPlaylistRequest: 0, storeKey: "definitely-not-a-real-channel-key" } });

      registerStream(entry);
      registeredIds.push(id);
    }

    const states = getTunerStates();
    const activeSlots = states.filter((s) => s.active);

    assert.equal(activeSlots.length, ids.length, "all three active streams represented");
    assert.equal(activeSlots[0]?.slot, 0, "first active stream takes slot 0");
    assert.equal(activeSlots[1]?.slot, 1, "second active stream takes slot 1");
    assert.equal(activeSlots[2]?.slot, 2, "third active stream takes slot 2");

    for(const idle of states.filter((s) => !s.active)) {

      assert.equal(idle.active, false);
      assert.equal(idle.channelName, null);
      assert.equal(idle.channelNumber, null);
      assert.equal(idle.clientAddress, null);
    }
  });

  test("reports every active stream and no idle slots when active streams exceed maxConcurrentStreams", () => {

    // Oversubscription is reachable at runtime by lowering maxConcurrentStreams while streams are active - live streams are not terminated by a limit change.
    // The projection must never drop an active tuner, so the array length is the active count (max(limit, active)) with zero idle entries.
    const priorLimit = CONFIG.streaming.maxConcurrentStreams;
    const ids = [ TEST_ID_BASE + 20, TEST_ID_BASE + 21, TEST_ID_BASE + 22 ];

    CONFIG.streaming.maxConcurrentStreams = 1;

    try {

      for(const id of ids) {

        const entry = makeRegistryEntry({ id, info: { lastPlaylistRequest: 0, storeKey: "definitely-not-a-real-channel-key" } });

        registerStream(entry);
        registeredIds.push(id);
      }

      const states = getTunerStates();

      assert.equal(states.length, ids.length, "array length equals the active count, not the lowered limit");
      assert.equal(states.filter((s) => s.active).length, ids.length, "every active stream is reported");
      assert.equal(states.filter((s) => !s.active).length, 0, "no idle slots when oversubscribed");
    } finally {

      CONFIG.streaming.maxConcurrentStreams = priorLimit;
    }
  });
});
