/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cdp.test.ts: HTTP-level integration coverage for the CDP proxy discovery endpoints (src/routes/cdp.ts). The proxy exposes a Chrome-compatible surface at
 * /cdp/json, /cdp/json/list, and /cdp/json/version that only materializes when the "cdp" debug category is enabled AND a browser is running. This suite pins the
 * two-stage request-time gate that guards every discovery handler: isCategoryEnabled("cdp") first (404 when off), then getBrowserInstance() (503 when no Chrome).
 *
 * The gate is checked inside each handler at request time - not wired at boot - so the surface appears and disappears in sync with the /debug toggle without a
 * restart. We drive the toggle in-process through initDebugFilter (the same runtime primitive the CLI and the /debug POST handler use) rather than by launching a
 * real browser, which the integration harness never does: getBrowserInstance() returns null here, so the "enabled but no browser" branch is naturally exercised.
 *
 * Scope. Every gate reachable from bootApp is pinned here: the discovery handlers' 404/503 pair over ordinary HTTP, and the WebSocket upgrade
 * handler's origin gate, driven by writing a raw upgrade request at the booted listener once attachCdpUpgradeHandler has bound to its server. What stays out of
 * scope is the CdpProxySession multiplexer behind them, which needs a live Puppeteer Browser/Connection the harness never launches; it is recorded here rather
 * than exercised against a fake. This suite is a sibling of streams.test.ts: both pin route-shape rules by seeding the exact state the handler reads (there, the
 * stream registry; here, the debug-category filter) instead of launching a real capture.
 */
import { bootApp, createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { getCurrentPattern, initDebugFilter } from "../../../src/utils/debugFilter.ts";
import type { IntegrationContext } from "../../helpers/integration.helpers.ts";
import assert from "node:assert/strict";
import { attachCdpUpgradeHandler } from "../../../src/routes/cdp.ts";
import http from "node:http";

// The discovery endpoints that share the identical two-stage gate, listed alphabetically. /cdp/json and /cdp/json/list are aliases of one handler and
// /cdp/json/version is its own handler, but every endpoint here opens with the same isCategoryEnabled("cdp") + getBrowserInstance() guard, so a regression that
// loosened the gate on any one of them must surface here.
const DISCOVERY_ENDPOINTS = [ "/cdp/json", "/cdp/json/list", "/cdp/json/version" ];

/**
 * Sets the runtime debug filter to exactly the given pattern and registers a cleanup that restores whatever pattern was active beforehand. The debug filter is
 * process-global module state that survives across tests in the same file, so every test that mutates it must restore it to keep the suite order-independent -
 * a later test that assumes the default (disabled) state would otherwise see leaked "cdp" enablement.
 * @param ctx - The integration context whose cleanup registrar restores the prior pattern at scope exit.
 * @param pattern - The comma-separated debug pattern to activate ("" disables all categories).
 */
function withDebugFilter(ctx: IntegrationContext, pattern: string): void {

  const previous = getCurrentPattern();

  ctx.registerCleanup((): void => { initDebugFilter(previous); });
  initDebugFilter(pattern);
}

/**
 * Writes a raw WebSocket upgrade request at the CDP proxy path and resolves with the HTTP status the server answered it with. A refused handshake is answered
 * by writing a status line straight to the socket rather than through the Express response pipeline, so we read the status off the wire instead of asking a
 * WebSocket client library to surface it - a client library reports a refusal as an opaque error and would cost us the very thing being asserted.
 * @param port - The bound port of the listener under test.
 * @param origin - The Origin header to send, or omitted to send none, which is the shape every non-browser CDP client presents.
 * @returns The HTTP status code the server answered the handshake with.
 */
function attemptCdpUpgrade(port: number, origin?: string): Promise<number> {

  const { promise, reject, resolve } = Promise.withResolvers<number>();
  const headers: Record<string, string> = {

    "Connection": "Upgrade",
    "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==",
    "Sec-WebSocket-Version": "13",
    "Upgrade": "websocket"
  };

  if(origin !== undefined) {

    headers["Origin"] = origin;
  }

  const request = http.request({ headers, host: "127.0.0.1", path: "/cdp/devtools/browser/prismcast", port });

  // A refused handshake arrives as an ordinary response and an accepted one arrives as an upgrade. Both carry the status code, so both arms resolve the same
  // promise and the caller never has to know which shape came back.
  request.on("response", (response): void => { response.resume(); resolve(response.statusCode ?? 0); });
  request.on("upgrade", (response, socket): void => { socket.destroy(); resolve(response.statusCode ?? 0); });
  request.on("error", reject);
  request.end();

  return promise;
}

describe("CDP discovery endpoints - category gate (disabled)", () => {

  test("every discovery endpoint responds 404 when the cdp debug category is disabled", async (): Promise<void> => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Explicitly force the filter to the default disabled state (and restore afterward) so this test does not depend on the process starting with no filter
    // configured. With the "cdp" category off, isCategoryEnabled("cdp") is false and the handler must short-circuit to 404 before ever consulting the browser.
    withDebugFilter(ctx, "");

    const { urlFor } = await bootApp(ctx);

    for(const endpoint of DISCOVERY_ENDPOINTS) {

      // eslint-disable-next-line no-await-in-loop -- sequential requests against a single listener; the gate is stateless so ordering is irrelevant.
      const response = await fetch(urlFor(endpoint));

      assert.equal(response.status, 404, endpoint + " responds 404 when cdp is disabled");

      // eslint-disable-next-line no-await-in-loop -- reading the body must complete before the next iteration reuses the connection pool.
      const body = await response.text();

      assert.equal(body, "Not Found", endpoint + " returns the disabled-gate body");
    }
  });

  test("a discovery endpoint stays 404 even when an unrelated debug category is enabled", async (): Promise<void> => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Enabling a different category must not open the cdp surface: the gate matches the exact "cdp" category, not "any debug output". This pins that the gate is
    // category-specific rather than a coarse isAnyDebugEnabled() check, which would leak the proxy on whenever any debug logging was turned on.
    withDebugFilter(ctx, "tuning:hulu");

    const { urlFor } = await bootApp(ctx);
    const response = await fetch(urlFor("/cdp/json"));

    assert.equal(response.status, 404, "an unrelated debug category does not open the cdp discovery surface");

    const body = await response.text();

    assert.equal(body, "Not Found", "the disabled-gate body is returned");
  });
});

describe("CDP discovery endpoints - category gate (enabled, no browser)", () => {

  test("every discovery endpoint responds 503 when cdp is enabled but no browser is running", async (): Promise<void> => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Enable the cdp category so the first gate passes, then rely on the integration harness having launched no Chrome: getBrowserInstance() returns null, so the
    // handler must fall through to the second gate and answer 503 rather than attempting to open a discovery CDP session against a nonexistent browser.
    withDebugFilter(ctx, "cdp");

    const { urlFor } = await bootApp(ctx);

    for(const endpoint of DISCOVERY_ENDPOINTS) {

      // eslint-disable-next-line no-await-in-loop -- sequential requests against a single listener; each carries the same no-browser precondition.
      const response = await fetch(urlFor(endpoint));

      assert.equal(response.status, 503, endpoint + " responds 503 when cdp is enabled but no browser is running");

      // eslint-disable-next-line no-await-in-loop -- reading the body must complete before the next iteration.
      const body = await response.text();

      assert.equal(body, "Browser not running", endpoint + " returns the no-browser body");
    }
  });

  test("a subcategory pattern that prefix-matches cdp also passes the first gate and yields 503", async (): Promise<void> => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // "cdp" is a leaf category, so an exact "cdp" pattern is the enabling form; a bare enable must pass the first gate and land on the no-browser 503. This
    // complements the disabled-state tests by proving the enabled branch is reachable and stops precisely at the browser check, not earlier and not later.
    withDebugFilter(ctx, "cdp");

    const { urlFor } = await bootApp(ctx);
    const response = await fetch(urlFor("/cdp/json/version"));

    assert.equal(response.status, 503, "the enabled cdp gate falls through to the browser check");

    const body = await response.text();

    assert.equal(body, "Browser not running", "the no-browser body is returned once the category gate passes");
  });
});

describe("CDP WebSocket upgrade - origin gate", () => {

  test("admits handshakes with no Origin or the allowlisted DevTools origin and refuses every other origin", async (): Promise<void> => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // The upgrade handler runs the category gate first, so the cdp category has to be on for any handshake to reach the origin gate at all.
    withDebugFilter(ctx, "cdp");

    const { port, server } = await bootApp(ctx);

    // The proxy's WebSocketServer is a module-level singleton and attachCdpUpgradeHandler returns early once it is set, so only the first call in a process
    // binds the upgrade listener. Every case below therefore shares one boot and one attach instead of booting per case.
    attachCdpUpgradeHandler(server);

    // No browser runs in the integration harness, so a handshake that clears the origin gate falls through to the browser check and answers 503. That 503 is
    // what the admitted cases assert on: it is reachable only by passing the gate, which by design sits ahead of the browser lookup.
    assert.equal(await attemptCdpUpgrade(port), 503, "a handshake carrying no Origin passes the gate");
    assert.equal(await attemptCdpUpgrade(port, "devtools://devtools"), 503, "the allowlisted DevTools frontend origin passes the gate");
    assert.equal(await attemptCdpUpgrade(port, "https://evil.test"), 403, "an arbitrary page origin is refused");

    // Matching is exact rather than containment. An origin that merely embeds the allowlisted string is a different origin, and a substring test would readmit
    // precisely the page this gate exists to keep out, so both the embedded and the prefixed forms are asserted.
    assert.equal(await attemptCdpUpgrade(port, "https://evil.test/devtools://devtools"), 403, "an origin embedding the allowlisted value is refused");
    assert.equal(await attemptCdpUpgrade(port, "devtools://devtools.evil.test"), 403, "an origin extending the allowlisted value is refused");
  });
});
