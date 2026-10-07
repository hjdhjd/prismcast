/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * services.test.ts: Unit tests for the service channel discovery route in services.ts. setupServicesEndpoint registers GET /services/:slug/channels which
 * dispatches to a provider's discoverChannels function inside a temporary browser page. The full discovery walk requires a real Chrome browser and a live
 * service guide, and no automated suite covers it; the e2e suite covers only the route's error envelope. Here we cover the synchronous validation branches that
 * run before any browser interaction: unknown slug returns 404 with a descriptive error, and the documented response shape for the unknown-slug branch is
 * locked. We also cover the lineup annotation a provider's warm cache answers, which runs before any browser work too, against the real channel store in a temp
 * data directory: the alternatives each channel reports and the lineups a user override's :predefined entry and selection annotate a discovered channel in.
 */
import type { AddressInfo, Server } from "node:net";
import type { DiscoveredChannel, ProviderModule } from "../types/index.ts";
import { PREDEFINED_SUFFIX, getServiceGroup, setEnabledServices } from "../config/services.ts";
import { after, afterEach, before, beforeEach, describe, test } from "node:test";
import { defaultPrecachingDeps, recordDiscoveryOutcome, withProviderGuidePage } from "../browser/precaching.ts";
import { initializeUserChannels, mutateChannels } from "../config/userChannels.ts";
import { mkdtemp, rm } from "node:fs/promises";
import type { ServiceDiscoveryDeps } from "./services.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";
import { initializeDataDir } from "../config/paths.ts";
import os from "node:os";
import path from "node:path";
import { setupServicesEndpoint } from "./services.ts";

function makeServer(deps?: ServiceDiscoveryDeps): Promise<{ port: number; server: Server }> {

  const app = express();

  setupServicesEndpoint(app, deps);

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
  await closePuppeteerStreamWss();
});

describe("setupServicesEndpoint - GET /services/:slug/channels (unknown slug)", () => {

  test("returns 404 for an unknown service slug (locks the validation branch)", async () => {

    // Negative test: the handler calls getProviderBySlug() and returns 404 with a descriptive error before any browser interaction. This lets the test exercise
    // the route registration and validation pass-through without a real Chrome browser running.
    const res = await fetch(urlFor("/services/totally-not-a-real-service-x9z2/channels"));

    assert.equal(res.status, 404);
  });

  test("response body for unknown slug includes the slug name in the error message", async () => {

    // The error message is "Unknown service: <slug>." - we lock the format so a regression that loses the specific slug surfaces as a real diff.
    const res = await fetch(urlFor("/services/madeUpSlug/channels"));
    const body = await res.json() as { error: string };

    assert.match(body.error, /Unknown service/);
    assert.match(body.error, /madeUpSlug/);
  });

  test("emits Content-Type application/json for the 404 response", async () => {

    const res = await fetch(urlFor("/services/bogus/channels"));

    assert.match(res.headers.get("content-type") ?? "", /application\/json/);
    await res.json();
  });

  test("a path that does not match the documented :slug/channels shape falls through to Express's default 404", async () => {

    // Boundary: only /services/:slug/channels is registered. A nearby path like /services should not be picked up by this route.
    const res = await fetch(urlFor("/services"));

    assert.equal(res.status, 404);
    await res.text();
  });

  test("an empty slug segment falls through to Express's default 404 (path-to-regexp does not match)", async () => {

    // Boundary: an empty path segment for :slug doesn't satisfy path-to-regexp, so the route doesn't match. Express returns its default 404.
    const res = await fetch(urlFor("/services//channels"));

    assert.equal(res.status, 404);
    await res.text();
  });
});

describe("setupServicesEndpoint - GET /services/:slug/channels (refresh/lineup query parameters)", () => {

  test("refresh=true with unknown slug still returns 404 (validation runs before refresh handling)", async () => {

    // Boundary: the slug check is the first thing the handler does. The refresh=true branch runs only when a known provider is found, so unknown+refresh still
    // produces a 404 with the standard error.
    const res = await fetch(urlFor("/services/totally-not-a-real-service-x9z2/channels?refresh=true"));

    assert.equal(res.status, 404);
    await res.json();
  });

  test("lineup=true with unknown slug still returns 404", async () => {

    const res = await fetch(urlFor("/services/totally-not-a-real-service-x9z2/channels?lineup=true"));

    assert.equal(res.status, 404);
    await res.json();
  });
});

describe("setupServicesEndpoint - GET /services/:slug/channels (lineup annotation from a warm cache)", () => {

  /* A provider whose cache is warm answers a lineup request through annotateWithLineupState before any browser work, and the annotation reads the real channel
   * listing and service groups, which each row builds in a data directory of its own. The stub provider answers the hulu, sling and yttv slugs with one cached
   * channel whose selector the amcthrillers entries carry, and the discovery collaborators are the real ones, which the warm-cache branch never calls. The
   * catalog's amcthrillers defaults to its sling service beside its yttv variant, and a row that overrides it on hulu.com builds a user override group whose
   * entries are the custom canonical (tag hulu), its :predefined entry (tag sling) and amcthrillers-yttv (tag yttv).
   */

  interface LineupEntry {

    lineup?: { canonicalKey: string; currentTag: string; hasAlternatives: boolean };
  }

  const lineupChannels = [{ channelSelector: "AMC Thrillers", name: "AMC Thrillers" }] as unknown as DiscoveredChannel[];

  const lineupProvider = {

    getCachedChannels: (): DiscoveredChannel[] => lineupChannels,
    label: "Stub Lineup",
    slug: "stub-lineup",
    strategy: {}
  } as unknown as ProviderModule;

  let dir: string;
  let lineupPort = 0;
  let lineupServer: Server;

  before(async () => {

    const created = await makeServer({

      getProviderBySlug: (slug: string): ProviderModule | undefined => ([ "hulu", "sling", "yttv" ].includes(slug) ? lineupProvider : undefined),
      precachingDeps: defaultPrecachingDeps,
      recordDiscoveryOutcome,
      withProviderGuidePage
    });

    lineupPort = created.port;
    lineupServer = created.server;
  });

  after(async () => {

    await closeServer(lineupServer);
  });

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-lineup-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();
  });

  // The service filter is cleared and the data directory pointed back at os.tmpdir(), a directory that exists, before the row's directory is removed.
  afterEach(async () => {

    setEnabledServices([]);
    initializeDataDir(os.tmpdir());
    await rm(dir, { force: true, recursive: true });
  });

  /**
   * Fetches a service's lineup from the stub server and asserts the request succeeds.
   * @param slug - The service slug.
   * @returns The annotated channels.
   */
  async function fetchLineup(slug: string): Promise<LineupEntry[]> {

    const res = await fetch("http://127.0.0.1:" + String(lineupPort) + "/services/" + slug + "/channels?lineup=true");
    const body = await res.json() as LineupEntry[];

    assert.equal(res.status, 200);

    return body;
  }

  /**
   * Overrides amcthrillers on hulu.com, storing a service selection with the override when one is named, and asserts the user override group the write builds.
   * @param selection - The service selection to store for amcthrillers, or undefined to store none.
   */
  async function overrideOnHulu(selection?: string): Promise<void> {

    await mutateChannels((data) => {

      data.channels["amcthrillers"] = { url: "https://www.hulu.com/live" };

      if(selection !== undefined) {

        data.serviceSelections["amcthrillers"] = selection;
      }
    });

    assert.deepEqual(getServiceGroup("amcthrillers")?.variants.map((variant) => [ variant.key, variant.tag ]),
      [ [ "amcthrillers", "hulu" ], [ "amcthrillers" + PREDEFINED_SUFFIX, "sling" ], [ "amcthrillers-yttv", "yttv" ] ], "precondition: the user override group");
  }

  test("hasAlternatives reads false while the browsed service is the one the channel falls back to with no selection", async () => {

    const sling = await fetchLineup("sling");
    const yttv = await fetchLineup("yttv");

    assert.equal(sling[0]?.lineup?.currentTag, "sling", "the catalog's amcthrillers defaults to its sling service");
    assert.equal(sling[0].lineup.hasAlternatives, false, "removing the default service disables the channel");
    assert.equal(yttv[0]?.lineup?.hasAlternatives, true, "removing the yttv service leaves the sling default");
  });

  test("a stored :predefined selection keeps an alternative while the filter excludes the custom URL's service", async () => {

    await overrideOnHulu("amcthrillers" + PREDEFINED_SUFFIX);
    setEnabledServices(["sling"]);

    const sling = await fetchLineup("sling");

    assert.equal(sling[0]?.lineup?.currentTag, "sling", "the selection reads as the sling service");
    assert.equal(sling[0].lineup.hasAlternatives, true, "removing sling falls back to the custom canonical, so the channel stays");
  });

  test("a stored :predefined selection reads as current in the predefined service's lineup and as a switch in the custom URL's", async () => {

    await overrideOnHulu("amcthrillers" + PREDEFINED_SUFFIX);

    const sling = await fetchLineup("sling");
    const hulu = await fetchLineup("hulu");

    assert.equal(sling[0]?.lineup?.currentTag, "sling", "the sling lineup annotates the channel through the listing entry's own selector");
    assert.equal(hulu[0]?.lineup?.currentTag, "sling", "the hulu lineup annotates it through the custom canonical, as a switch");
  });

  test("the selector match never annotates a discovered channel through the :predefined entry", async () => {

    await overrideOnHulu();

    const sling = await fetchLineup("sling");
    const yttv = await fetchLineup("yttv");

    assert.equal(sling.length, 1, "the cached lineup is answered");
    assert.equal(sling[0]?.lineup, undefined, "the sling tag on the :predefined entry annotates nothing without a selection");
    assert.equal(yttv[0]?.lineup?.canonicalKey, "amcthrillers", "the loop goes on past the :predefined entry to the yttv variant");
  });
});
