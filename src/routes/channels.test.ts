/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channels.test.ts: Unit tests for the channel listing route in channels.ts. setupChannelsEndpoint registers GET /channels which returns a JSON object with the
 * full channel listing - including both enabled and disabled entries - augmented with source metadata. The handler delegates to getChannelListing() in the
 * config layer; these tests verify the response shape, the optional-field contract, and the count agreement with the listing length. Because the listing
 * depends on PREDEFINED_CHANNELS at import time, we run against the actual production listing rather than fabricating a substitute fixture.
 */
import type { AddressInfo, Server } from "node:net";
import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import express from "express";
import { getChannelListing } from "../config/userChannels.ts";
import { setupChannelsEndpoint } from "./channels.ts";

interface ChannelEntryShape {

  channelNumber?: number;
  channelSelector?: string;
  enabled: boolean;
  key: string;
  name: string;
  profile?: string;
  source: string;
  stationId?: string;
  url: string;
}

interface ChannelsResponse {

  channels: ChannelEntryShape[];
  count: number;
}

function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  setupChannelsEndpoint(app);

  return new Promise((resolve, reject) => {

    const server = app.listen(0, "127.0.0.1", () => {

      const address = server.address() as AddressInfo;

      resolve({ port: address.port, server });
    });

    server.on("error", reject);
  });
}

function closeServer(server: Server): Promise<void> {

  return new Promise((resolve) => {

    server.close(() => {

      resolve();
    });
  });
}

let sharedServer: Server;
let sharedPort = 0;

function urlFor(path: string): string {

  return "http://127.0.0.1:" + String(sharedPort) + path;
}

before(async () => {

  const created = await makeServer();

  sharedServer = created.server;
  sharedPort = created.port;
});

after(async () => {

  await closeServer(sharedServer);
});

describe("setupChannelsEndpoint - GET /channels", () => {

  test("returns 200 with JSON body containing channels array and count", async () => {

    const res = await fetch(urlFor("/channels"));

    assert.equal(res.status, 200);
    assert.match(res.headers.get("content-type") ?? "", /application\/json/);

    const body = await res.json() as ChannelsResponse;

    assert.ok(Array.isArray(body.channels), "channels should be an array");
    assert.equal(typeof body.count, "number", "count should be a number");
  });

  test("count agrees with the channels array length (locks the documented contract)", async () => {

    const res = await fetch(urlFor("/channels"));
    const body = await res.json() as ChannelsResponse;

    assert.equal(body.count, body.channels.length, "count must equal channels.length");
  });

  test("includes one entry per non-variant channel from getChannelListing()", async () => {

    // The handler iterates the listing exactly; it does not skip or duplicate. We compare the keys to the listing's keys to lock the contract.
    const expected = getChannelListing().map((entry) => entry.key).sort();
    const res = await fetch(urlFor("/channels"));
    const body = await res.json() as ChannelsResponse;
    const got = body.channels.map((c) => c.key).sort();

    assert.deepEqual(got, expected, "every listing key should appear in the response");
  });

  test("each channel entry includes the required fields (enabled, key, name, source, url)", async () => {

    const res = await fetch(urlFor("/channels"));
    const body = await res.json() as ChannelsResponse;

    assert.ok(body.channels.length > 0, "expected at least one channel for assertions to be meaningful");

    for(const entry of body.channels) {

      assert.equal(typeof entry.enabled, "boolean", "enabled should be boolean");
      assert.equal(typeof entry.key, "string", "key should be string");
      assert.equal(typeof entry.name, "string", "name should be string");
      assert.equal(typeof entry.url, "string", "url should be string");
      assert.match(entry.source, /^(override|predefined|user)$/, "source should be one of override/predefined/user");
    }
  });

  test("source values are constrained to the documented set", async () => {

    // Boundary: the response interface declares source as "override" | "predefined" | "user". Locking the constraint catches a regression that would emit
    // arbitrary strings.
    const res = await fetch(urlFor("/channels"));
    const body = await res.json() as ChannelsResponse;
    const seenSources = new Set(body.channels.map((c) => c.source));

    for(const source of seenSources) {

      assert.match(source, /^(override|predefined|user)$/);
    }
  });

  test("name falls back to key when channel.name is missing (locks the defensive fallback)", async () => {

    // The handler computes name = entry.channel.name ?? entry.key. Every name in the response should be a non-empty string (either the channel's name or its
    // key), so we verify name.length > 0 for every entry.
    const res = await fetch(urlFor("/channels"));
    const body = await res.json() as ChannelsResponse;

    for(const entry of body.channels) {

      assert.ok(entry.name.length > 0, "name should be non-empty for key " + entry.key);
    }
  });

  test("optional fields appear only when defined on the underlying channel (locks the projection contract)", async () => {

    // The handler appends channelNumber, channelSelector, profile, stationId only when the source channel.* is not undefined. We pair each response entry with
    // its source listing entry and verify that an undefined source field is absent in the response.
    const listing = getChannelListing();
    const byKey = new Map(listing.map((e) => [ e.key, e ] as const));
    const res = await fetch(urlFor("/channels"));
    const body = await res.json() as ChannelsResponse;

    for(const entry of body.channels) {

      const source = byKey.get(entry.key);

      assert.ok(source, "source listing should have entry for key " + entry.key);

      const sourceChannel = source.channel as unknown as Record<string, unknown>;

      for(const field of [ "channelNumber", "channelSelector", "profile", "stationId" ] as const) {

        if(sourceChannel[field] === undefined) {

          assert.equal((entry as unknown as Record<string, unknown>)[field], undefined,
            "field " + field + " should be absent on response when undefined on source (key=" + entry.key + ")");
        }
      }
    }
  });

  test("response is alphabetically ordered by key (matches getChannelListing's sort)", async () => {

    // Boundary: the listing sorts by key.localeCompare(other.key). The handler iterates in listing order, so the response preserves it. A regression that
    // re-sorted by name or shuffled the order would surface here.
    const res = await fetch(urlFor("/channels"));
    const body = await res.json() as ChannelsResponse;
    const keys = body.channels.map((c) => c.key);
    const sorted = [...keys].sort((a, b) => a.localeCompare(b));

    assert.deepEqual(keys, sorted, "channels should be alphabetically ordered by key");
  });

  test("two consecutive requests return identical responses (idempotent reads)", async () => {

    const a = await (await fetch(urlFor("/channels"))).json() as ChannelsResponse;
    const b = await (await fetch(urlFor("/channels"))).json() as ChannelsResponse;

    assert.deepEqual(a, b, "the listing should be deterministic across reads when nothing has changed");
  });
});
