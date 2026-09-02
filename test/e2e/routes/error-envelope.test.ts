/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * error-envelope.test.ts: Integration coverage for the HTTP error envelope contract across every endpoint registered under src/routes that uses the envelope
 * helpers. The contract documented in src/routes/config/http/envelope.ts is: every error response carries the shape `{ error: string, ...extensions, success:
 * false }` for top-level errors or `{ errors: Record<string, string>, success: false }` for field-level form errors; every success response that uses the
 * envelope helpers carries `{ success: true, ... }`. The client-side toast and error-handling logic depends on this shape - any endpoint that ships a
 * non-conforming envelope silently breaks the UI.
 *
 * Why this suite exists: per-endpoint suites cover behavior, but none asserts the cross-endpoint envelope-shape rule. A new endpoint that forgets to
 * call sendValidationError / sendErrorResponse / sendFormErrors and instead writes a raw `res.status(400).json({ error: "..." })` would slip past every per-endpoint
 * suite - the per-endpoint test would still pass because the endpoint's behavior is correct, but the envelope shape would be off and the client wouldn't know.
 *
 * Sweep scope:
 *
 *   ENDPOINT_SPECS below enumerates every endpoint registered by setupRoutes() whose validation surface is well-defined - both mutating endpoints (POST / PUT /
 *   PATCH / DELETE) AND read-side endpoints (GET) that use validation envelopes via query-parameter validation. The drift-check at the top of the suite walks
 *   app.router.stack at runtime, lists every GET / POST / PUT / PATCH / DELETE registered anywhere on the app, and asserts the same set is present in either
 *   ENDPOINT_SPECS or the EXCLUDED_ENDPOINTS list. A new endpoint added to production without a corresponding spec entry fails this check loud, before any
 *   sweep assertion runs - the SSOT for "what endpoints exist" is the runtime route stack, not the test fixture.
 *
 *   The sweep covers every endpoint that uses the envelope helpers, regardless of HTTP method: mutating endpoints (POST / PUT / PATCH / DELETE) across the full
 *   app, plus GET endpoints with validation envelopes (playlist.ts query-parameter rejections, config/profiles/export missing-key rejection, services/:slug/channels
 *   unknown-service rejection). The rich-payload form of sendValidationError / sendErrorResponse (`{ error, validTags, ... }`, `{ error, validFields, ... }`,
 *   `{ error, validDirections, ... }` - the diagnostic-context shapes the GET-validation endpoints emit) is part of the canonical contract, so one drift check and
 *   one sweep cover every envelope-helper endpoint regardless of method... validation envelopes on GET are not second-class. The sweep asserts the envelope marker
 *   (success: false) and the simple-vs-form variant (error vs errors), not the diagnostic extension fields themselves.
 *
 * Why drift-check via stack-walk and not via a production-side endpoint registry: three options were considered - (A) export an endpoint registry from production,
 * (B) walk Express's app.router.stack, (C) hardcode + drift-check the test list. Option C is used, with the drift-check implemented via stack-walk - hardcoded
 * test list as the SSOT for "which endpoints we exercise," runtime stack-walk as the SSOT for "what endpoints exist now." If they ever disagree, the drift
 * assertion fails and the maintainer adds the missing entry. Stack-walk reads the same
 * `app.router.stack` shape Express's own internals depend on. Express 5 exposes `app.router` as a public accessor (it was the `app._router` private field in
 * older Express), and the shape is stable, so the coupling is justified on its own terms. This suite is the sole stack-walk consumer in the codebase: the unit
 * suites at src/routes/config/services.test.ts and similar introspect route registrations through the makeExpressStub recorder, a different surface, not through
 * the real router stack. Option A is the eventual right answer (a production
 * registry would be referenced from both the route setup and this test), but extracting it is a production refactor; the test
 * runs without it and remains correct.
 *
 * What this suite does NOT assert: GET endpoints with no validation rejection path (read-only data responses, streaming endpoints, server-rendered HTML pages,
 * static assets - all of these are listed in EXCLUDED_ENDPOINTS with a one-line rationale per entry), and exception-only error envelopes whose 500 path
 * requires environmental fault injection (e.g., GET /logs whose 500 catch fires only on filesystem read failure - the envelope shape is asserted by the
 * sendErrorResponse rich-payload unit tests at src/routes/config/http/envelope.test.ts; integration coverage would require a production refactor to inject
 * the failure deterministically).
 */
import { bootApp, createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import type { Express } from "express";
import assert from "node:assert/strict";

/**
 * One registered endpoint specification. The body is what triggers a 4xx response from the handler; expectedStatus and expectedField document the response shape
 * the test asserts. registeredPath uses the literal path string Express records (e.g., "/config/channels/:key" for parameterized routes); requestPath is the
 * concrete URL the test actually hits, with placeholder values substituted (test-key for :key, nonexistent-tag for :tag, etc.).
 */
interface EndpointSpec {

  // Body to send. JSON-serialized for application/json content-type. Ignored for GET requests (which fetch refuses to send a body for).
  body?: unknown;

  // Which envelope variant we expect: "error" for `{ error: string, success: false }`, "errors" for `{ errors: Record<string, string>, success: false }`.
  expectedField: "error" | "errors";

  // HTTP status code to assert.
  expectedStatus: number;

  // HTTP method. GET is included for read-side endpoints with query-parameter validation; mutating methods cover the bulk of the surface.
  method: "DELETE" | "GET" | "PATCH" | "POST" | "PUT";

  // The literal request path the test hits (with substituted parameters and query string for GET-validation entries).
  requestPath: string;

  // The literal path Express recorded in app.router.stack (e.g., /config/channels/:key). Used for drift-check matching.
  registeredPath: string;
}

/* The single source of truth for "which endpoints this suite exercises." Drift-check at the bottom of the file validates that every registered route (GET / POST /
 * PUT / PATCH / DELETE) captured at runtime appears here or in EXCLUDED_ENDPOINTS - so a new endpoint cannot be added without a maintainer's deliberate decision
 * about whether to sweep its envelope or document its exclusion.
 *
 * Per-entry rationale: each body is the simplest input that triggers a 4xx response from the handler. Most are empty `{}` (the handler validates required
 * fields first); a few use `null` or specific shapes to trip type-check guards in the validator. The expectedField/expectedStatus values are what the production
 * code actually returns today - a regression that flipped the shape would fail the matching assertion.
 */
const ENDPOINT_SPECS: readonly EndpointSpec[] = [

  // Channel CRUD - crud.ts.
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/channels", requestPath: "/config/channels" },
  { body: {}, expectedField: "errors", expectedStatus: 400, method: "PUT", registeredPath: "/config/channels/:key", requestPath: "/config/channels/test-key" },
  { body: {}, expectedField: "error", expectedStatus: 400, method: "DELETE", registeredPath: "/config/channels/:key", requestPath: "/config/channels/test-key" },
  { body: {}, expectedField: "error", expectedStatus: 400, method: "PATCH", registeredPath: "/config/channels/:key", requestPath: "/config/channels/test-key" },
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/channels/:key/revert",
    requestPath: "/config/channels/test-key/revert" },

  /* Channel bulk - bulk.ts. start=0 is a valid "clear mode" sentinel (bulk.ts:29), so we use sortField="invalid-sort-field" to trip the VALID_SORT_FIELDS check at
   * line 40. */
  { body: { sortField: "invalid-sort-field", start: 1 }, expectedField: "error", expectedStatus: 400, method: "POST",
    registeredPath: "/config/channels/auto-number", requestPath: "/config/channels/auto-number" },
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/channels/bulk-tags", requestPath: "/config/channels/bulk-tags" },

  // Channel browse - browse.ts.
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/channels/modify", requestPath: "/config/channels/modify" },

  // Channel import/export - importExport.ts.
  { body: { not: "valid-channels-shape" }, expectedField: "error", expectedStatus: 400, method: "POST",
    registeredPath: "/config/channels/import", requestPath: "/config/channels/import" },
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/channels/import-m3u",
    requestPath: "/config/channels/import-m3u" },

  // Channel predefined - predefined.ts.
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/channels/toggle-predefined",
    requestPath: "/config/channels/toggle-predefined" },
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/channels/bulk-toggle-predefined",
    requestPath: "/config/channels/bulk-toggle-predefined" },

  // Channel prefs - prefs.ts. display-prefs has a "visibleColumns must be an array" guard when visibleColumns is non-array; empty body succeeds (all fields optional).
  { body: { visibleColumns: "not-an-array" }, expectedField: "error", expectedStatus: 400, method: "POST",
    registeredPath: "/config/channels/display-prefs", requestPath: "/config/channels/display-prefs" },

  // Channel service - service.ts.
  { body: {}, expectedField: "error", expectedStatus: 400, method: "PUT", registeredPath: "/config/channels/:key/service",
    requestPath: "/config/channels/test-key/service" },
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/service-bulk-assign", requestPath: "/config/service-bulk-assign" },
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/service-bulk-restore",
    requestPath: "/config/service-bulk-restore" },
  { body: { enabledServices: "not-an-array" }, expectedField: "error", expectedStatus: 400, method: "POST",
    registeredPath: "/config/service-filter", requestPath: "/config/service-filter" },

  // Tags - tags.ts.
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/tags", requestPath: "/config/tags" },
  { body: {}, expectedField: "error", expectedStatus: 404, method: "DELETE", registeredPath: "/config/tags/:tag", requestPath: "/config/tags/nonexistent-tag" },
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/tags/rename", requestPath: "/config/tags/rename" },
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/tags/restore", requestPath: "/config/tags/restore" },

  // Profiles - services.ts.
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/profiles", requestPath: "/config/profiles" },
  { body: {}, expectedField: "error", expectedStatus: 404, method: "DELETE", registeredPath: "/config/profiles/:key",
    requestPath: "/config/profiles/nonexistent-profile" },
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/profiles/import", requestPath: "/config/profiles/import" },
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/profiles/test", requestPath: "/config/profiles/test" },
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/profiles/test/check",
    requestPath: "/config/profiles/test/check" },

  // Settings - settings.ts. Settings POST validates per-field via CONFIG_METADATA; an out-of-range port produces a `{ errors }` response.
  { body: { server: { port: 0 } }, expectedField: "errors", expectedStatus: 400, method: "POST", registeredPath: "/config", requestPath: "/config" },

  // Settings import - an array body trips the "expected an object" rejection at the entry of the import handler. Empty `{}` would survive the type check and
  // proceed to schema validation, whose surface is dependent on CONFIG_METADATA evolution; an array stays rejected by the entry guard regardless.
  { body: [], expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/import", requestPath: "/config/import" },

  // Restart-now - the validation error fires when the process is not running under a service manager; the integration harness runs the app under node directly,
  // so isRunningAsService() returns false and the 400 path is reliably exercised.
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/config/restart-now", requestPath: "/config/restart-now" },

  /* Auth - auth.ts. Body without channel or url trips the "Either channel or url must be provided." validation at the entry of the handler, before any browser
   * interaction is attempted. Safe to exercise: the handler short-circuits before startLoginMode is called.
   */
  { body: {}, expectedField: "error", expectedStatus: 400, method: "POST", registeredPath: "/auth/login", requestPath: "/auth/login" },

  /* Streams - streams.ts. Two paths from the same registered route: a non-numeric ID trips the parseInt guard (400); a numeric ID that doesn't match any active
   * stream trips the registry lookup (404). Both are deterministic in the harness because no streams are active at boot.
   */
  { body: {}, expectedField: "error", expectedStatus: 400, method: "DELETE", registeredPath: "/streams/:id", requestPath: "/streams/not-a-number" },
  { body: {}, expectedField: "error", expectedStatus: 404, method: "DELETE", registeredPath: "/streams/:id", requestPath: "/streams/999999" },

  /* GET-side validation envelopes - read endpoints whose query-parameter validation paths use the envelope helpers. The rich-payload form of sendValidationError
   * (`{ error, validTags?, validFields?, validDirections? }`, etc.) is part of the canonical envelope contract, so GET-side rejection paths participate
   * in the drift sweep on equal footing with mutating endpoints.
   */

  /* Playlist - playlist.ts. Four paths from the same registered route, each tripping a different validation rejection: an unknown service tag (hits
   * parseServiceFilter), an unknown user tag (hits parseTagFilter), an invalid sort field (hits VALID_SORT_FIELDS), an invalid sort
   * direction (hits VALID_SORT_DIRECTIONS). Each carries a different diagnostic extension field on the rich-payload envelope (validTags, validFields,
   * validDirections), but the sweep does not assert those fields - the matching assertion below verifies only the envelope marker (success: false) and the
   * simple-vs-form variant (an `error` string with `errors` absent). What these entries assert is that every rejection path still ships a conforming top-level
   * error envelope.
   */
  { expectedField: "error", expectedStatus: 400, method: "GET", registeredPath: "/playlist", requestPath: "/playlist?service=nonexistent-service-tag-foo" },
  { expectedField: "error", expectedStatus: 400, method: "GET", registeredPath: "/playlist", requestPath: "/playlist?tag=nonexistent-user-tag-foo" },
  { expectedField: "error", expectedStatus: 400, method: "GET", registeredPath: "/playlist", requestPath: "/playlist?sort=not-a-sort-field" },
  { expectedField: "error", expectedStatus: 400, method: "GET", registeredPath: "/playlist", requestPath: "/playlist?direction=sideways" },

  /* Profiles export - config/services.ts. The "Profile key is required" rejection fires when ?profile= is absent from the query string. This is the simplest
   * deterministic 400 path on this endpoint. The "No valid profile keys provided" path (?profile=, with only commas/whitespace) is structurally identical and
   * adds nothing to drift detection - one entry per registered path is sufficient to assert the envelope contract.
   */
  { expectedField: "error", expectedStatus: 400, method: "GET", registeredPath: "/config/profiles/export", requestPath: "/config/profiles/export" },

  /* Services - services.ts. Unknown service slug trips sendNotFoundError. Deterministic in the harness because no provider exists
   * at the synthetic slug.
   */
  { expectedField: "error", expectedStatus: 404, method: "GET", registeredPath: "/services/:slug/channels",
    requestPath: "/services/nonexistent-provider-slug/channels" }
];

/* Registered endpoints whose envelope shape is excluded from the sweep, with reasoning. The drift-check expects every registered endpoint (GET / POST / PUT /
 * PATCH / DELETE) to be in either ENDPOINT_SPECS or this list, so excluding an endpoint here is a deliberate maintenance decision documented inline.
 */
interface ExclusionSpec {

  method: string;
  reason: string;
  registeredPath: string;
}

const EXCLUDED_ENDPOINTS: readonly ExclusionSpec[] = [

  // No validation rejection path: empty body succeeds as "No changes needed."
  { method: "POST", reason: "Empty body succeeds (No changes needed). No 4xx envelope to assert.", registeredPath: "/config/channels/hdhr-bulk" },

  // No validation: just sets a boolean flag, accepts any body.
  { method: "POST", reason: "No body validation. Always succeeds with { success: true }.", registeredPath: "/config/channels/setup-completed" },

  // No body validation - signals end of test mode.
  { method: "POST", reason: "No body validation. Always succeeds.", registeredPath: "/config/profiles/test/done" },

  // No body validation - calls endLoginMode and reports success regardless of input.
  { method: "POST", reason: "No body validation. Always succeeds.", registeredPath: "/auth/done" },

  /* Redirect-only response. POST /debug accepts a form submission and responds with HTTP 303 to /debug; there is no JSON envelope to assert. The endpoint is
   * intentionally outside the JSON envelope contract because it is a server-rendered form handler, not an API.
   */
  { method: "POST", reason: "Redirects 303 to /debug (server-rendered form handler). No JSON envelope to assert.", registeredPath: "/debug" },

  /* Manual version check has no validation rejection path - it always invokes checkForUpdates and reports the result. The success envelope is exercised by
   * the dedicated success-shape test below; there is no 4xx path to sweep.
   */
  { method: "POST", reason: "No body validation. Always succeeds.", registeredPath: "/version/check" },

  /* The 4xx path requires `info.upgradeable === false`, which depends on environment-detected install method (file path under node_modules / Cellar / Docker
   * marker). The integration harness does not inject a DetectionContext into the route handler, so the 4xx vs success branch cannot be deterministically
   * controlled from a test. Including this in the sweep would either spuriously pass (when the env detects a non-upgradeable install) or actually invoke
   * `npm install -g prismcast@latest` (when the env detects an upgradeable install). Excluded until the route accepts an injectable detector.
   */
  { method: "POST", reason: "4xx path depends on environmental install-method detection. Requires production refactor to inject detector for deterministic test.",
    registeredPath: "/upgrade" },

  /* GET-side exclusions - read endpoints with no validation rejection path or with exception-only error envelopes that require environmental fault injection.
   * Each entry carries a one-line rationale; the bulk of the GET surface is data responses, streaming, server-rendered HTML, and static assets, so the
   * envelope contract simply does not apply. Exception-only paths (e.g., /logs's filesystem-failure 500) are asserted by the sendErrorResponse rich-payload unit
   * tests at src/routes/config/http/envelope.test.ts; integration coverage would require a production refactor to inject the failure deterministically.
   */
  { method: "GET", reason: "Static asset response. No JSON envelope to assert.", registeredPath: "/favicon.svg" },
  { method: "GET", reason: "Static asset response. No JSON envelope to assert.", registeredPath: "/favicon.png" },
  { method: "GET", reason: "Static asset response. No JSON envelope to assert.", registeredPath: "/logo.png" },
  { method: "GET", reason: "Static asset response. No JSON envelope to assert.", registeredPath: "/logo.svg" },
  { method: "GET", reason: "Read-only data response (channel listing). No validation rejection path.", registeredPath: "/channels" },
  { method: "GET", reason: "Server-rendered HTML page (debug log configuration). No JSON envelope.", registeredPath: "/debug" },
  { method: "GET", reason: "Health check. No validation rejection path.", registeredPath: "/health" },
  { method: "GET", reason: "HLS playlist response (manifest content). Streaming endpoint, not an API.", registeredPath: "/hls/:name/stream.m3u8" },
  { method: "GET", reason: "HLS playlist response (video manifest). Streaming endpoint, not an API.", registeredPath: "/hls/:name/video.m3u8" },
  { method: "GET", reason: "HLS playlist response (audio manifest). Streaming endpoint, not an API.", registeredPath: "/hls/:name/audio.m3u8" },
  { method: "GET", reason: "HLS segment response. Streaming endpoint, not an API.", registeredPath: "/hls/:name/:segment" },
  { method: "GET",
    reason: "Read-only data response (success path); 500 path requires fs fault injection. Envelope shape asserted by sendErrorResponse rich-payload unit tests.",
    registeredPath: "/logs" },
  { method: "GET", reason: "Server-Sent Events stream. Long-lived connection, no JSON envelope.", registeredPath: "/logs/stream" },
  { method: "GET", reason: "MPEG-TS streaming response. Streaming endpoint, not an API.", registeredPath: "/stream/:name" },
  { method: "GET", reason: "Read-only data response (active stream listing). No validation rejection path.", registeredPath: "/streams" },
  { method: "GET", reason: "Server-Sent Events stream (status). No JSON envelope.", registeredPath: "/streams/status" },
  { method: "GET", reason: "Ad-hoc URL streaming. Streaming endpoint, not an API.", registeredPath: "/play" },
  { method: "GET", reason: "Preroll fMP4 init segment. Streaming endpoint, not an API.", registeredPath: "/preroll/:codec/init.mp4" },
  { method: "GET", reason: "Preroll fMP4 segment. Streaming endpoint, not an API.", registeredPath: "/preroll/:codec/:segment" },
  { method: "GET", reason: "Read-only data response (login mode status). No validation rejection path.", registeredPath: "/auth/status" },
  { method: "GET",
    reason: "500 catch only (no validation rejection). Envelope shape asserted by sendErrorResponse caught-exception unit tests; success path returns data.",
    registeredPath: "/upgrade/info" },
  { method: "GET", reason: "Read-only data response (profile listing). No validation rejection path.", registeredPath: "/config/profiles" },
  { method: "GET",
    reason: "500 catch only (no validation rejection). Envelope shape asserted by sendErrorResponse caught-exception unit tests; success path returns export JSON.",
    registeredPath: "/config/export" },
  { method: "GET", reason: "Read-only export response (channels JSON download). No validation rejection path.", registeredPath: "/config/channels/export" },
  { method: "GET", reason: "Read-only data response (tag vocabulary). No validation rejection path.", registeredPath: "/config/tags" },
  { method: "GET", reason: "Read-only data response (changelog markdown). No validation rejection path.", registeredPath: "/version/changelog" },
  { method: "GET", reason: "Server-rendered HTML page (landing page). No JSON envelope.", registeredPath: "/" },

  /* CDP proxy discovery endpoints. These speak the Chrome DevTools Protocol's JSON wire shape (https://chromedevtools.github.io/devtools-protocol/), not the
   * PrismCast HTTP error envelope - their consumers (chrome://inspect, puppeteer.connect, chrome-remote-interface) parse fixed CDP-defined fields like
   * `webSocketDebuggerUrl` and `Browser`. Each returns 404 (no body) when the cdp debug category is disabled and the upstream CDP-shaped JSON otherwise; in
   * neither case does the PrismCast validation envelope apply.
   */
  { method: "GET", reason: "Chrome DevTools Protocol discovery (CDP-shaped JSON or 404). Not a PrismCast-envelope endpoint.", registeredPath: "/cdp/json" },
  { method: "GET", reason: "Chrome DevTools Protocol discovery (alias of /cdp/json). Not a PrismCast-envelope endpoint.", registeredPath: "/cdp/json/list" },
  { method: "GET", reason: "Chrome DevTools Protocol version (CDP-shaped JSON or 404). Not a PrismCast-envelope endpoint.", registeredPath: "/cdp/json/version" }
];

/**
 * Walks the booted app's router stack and returns every route registered under setupRoutes(). The shape `app.router.stack[i].route.methods` is the standard
 * Express surface; `app.router` is the public accessor in Express 5 (it was the `app._router` private field in older Express). This suite is the only
 * stack-walk consumer in the codebase - the unit suites (services.test.ts, settings.test.ts) introspect route registrations through the makeExpressStub
 * recorder, a different mechanism, not through the real router stack.
 *
 * No path-prefix filter and no method filter: the sweep covers the entire endpoint surface (GET + mutating) so envelope-shape drift in any future top-level
 * route file (alongside auth.ts, streams.ts, upgrade.ts, etc.) is caught the same way as drift inside /config/*. GET endpoints with validation envelopes
 * (playlist.ts, config/profiles/export, services/:slug/channels) are first-class participants in the drift sweep alongside mutating endpoints.
 * @param app - The booted Express application.
 * @returns Array of `${METHOD} ${path}` strings for every route on the app.
 */
function enumerateRoutes(app: Express): string[] {

  const stack = (app as unknown as {

    router?: { stack?: { route?: { methods: Record<string, boolean>; path: string } }[] };
  }).router?.stack ?? [];

  const result: string[] = [];

  for(const layer of stack) {

    const route = layer.route;

    if(!route) {

      continue;
    }

    for(const method of Object.keys(route.methods)) {

      if((method === "get") || (method === "post") || (method === "put") || (method === "patch") || (method === "delete")) {

        result.push(method.toUpperCase() + " " + route.path);
      }
    }
  }

  return result;
}

describe("HTTP error envelope - drift check across every registered endpoint", () => {

  test("every registered endpoint is either in ENDPOINT_SPECS or in EXCLUDED_ENDPOINTS", async () => {

    /* SSOT assertion: the runtime route stack is authoritative for "what endpoints exist." This test asserts the test fixture (ENDPOINT_SPECS + EXCLUDED_ENDPOINTS)
     * is a complete enumeration of registered routes across the entire app (mutating endpoints AND read-side endpoints). A new endpoint added to production
     * without a matching test entry fails this assertion loud, before any sweep test runs - the maintainer must decide explicitly whether to add it to
     * ENDPOINT_SPECS (sweep its envelope) or to EXCLUDED_ENDPOINTS (with a reason). Either choice is fine; the drift check catches accidental omissions.
     *
     * Both sides of the comparison (test fixture + production registry) are sources of truth; they must agree.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { app } = await bootApp(ctx);

    const registeredRoutes = new Set(enumerateRoutes(app));
    const specRoutes = new Set(ENDPOINT_SPECS.map((spec) => spec.method + " " + spec.registeredPath));
    const excludedRoutes = new Set(EXCLUDED_ENDPOINTS.map((spec) => spec.method.toUpperCase() + " " + spec.registeredPath));

    // Every registered route must be covered by exactly one of the two lists.
    const uncovered = [...registeredRoutes].filter((route) => !specRoutes.has(route) && !excludedRoutes.has(route)).toSorted();

    assert.deepEqual(uncovered, [],
      "every registered route must be either in ENDPOINT_SPECS or in EXCLUDED_ENDPOINTS - " +
      "uncovered routes mean a new endpoint shipped without an envelope-shape decision");

    // Every spec entry must correspond to a registered route - a stale entry surfaces as a registration mismatch.
    const stale = [...specRoutes].filter((route) => !registeredRoutes.has(route)).toSorted();

    assert.deepEqual(stale, [],
      "every entry in ENDPOINT_SPECS must correspond to a registered route - stale entries indicate the test fixture references a removed endpoint");

    const staleExclusions = [...excludedRoutes].filter((route) => !registeredRoutes.has(route)).toSorted();

    assert.deepEqual(staleExclusions, [],
      "every entry in EXCLUDED_ENDPOINTS must correspond to a registered route - stale exclusions indicate a removed endpoint that is still being excluded");
  });
});

describe("HTTP error envelope - parameterized sweep across every endpoint with a validation envelope", () => {

  // One sub-test per spec. Each sub-test hits the endpoint with the malformed body or query and asserts the response shape conforms to the documented envelope.
  for(const spec of ENDPOINT_SPECS) {

    test(spec.method + " " + spec.requestPath + " returns the documented error envelope", async () => {

      /* The sweep assertion: every endpoint that ships a 4xx response must use the documented envelope shape. The contract per envelope.ts:
       *   - Top-level errors: `{ error: string, ...extensions, success: false }` (extensions are the rich-payload form for endpoints with diagnostic context)
       *   - Form-level errors: `{ errors: Record<string, string>, success: false }`
       *
       * Every entry in ENDPOINT_SPECS declares its expected expectedField (which envelope variant) and expectedStatus (which 4xx code). A regression that
       * shipped a different shape - missing success: false, raw text body, wrong field name - fails the matching assertion.
       */
      await using ctx = await createIntegrationContext();

      await initializePersistence(ctx);

      const { urlFor } = await bootApp(ctx);

      // GET requests cannot carry a body; for them the validation rejection is driven by the request path's query string. For mutating methods, the malformed
      // body is the trigger and we attach the JSON content-type. Splitting the fetch options keeps each shape minimal and avoids fetch's "GET with body" reject.
      const fetchOptions: RequestInit = (spec.method === "GET") ?
        { method: spec.method } :
        { body: JSON.stringify(spec.body), headers: { "Content-Type": "application/json" }, method: spec.method };
      const response = await fetch(urlFor(spec.requestPath), fetchOptions);

      assert.equal(response.status, spec.expectedStatus,
        spec.method + " " + spec.requestPath + " status: expected " + String(spec.expectedStatus) + ", got " + String(response.status) +
        "; body: " + (await response.clone().text()).slice(0, 200));

      const body = await response.json() as Record<string, unknown>;

      assert.equal(body["success"], false,
        spec.method + " " + spec.requestPath + " envelope: success must be false on error responses");

      if(spec.expectedField === "error") {

        assert.equal(typeof body["error"], "string",
          spec.method + " " + spec.requestPath + " envelope: error field must be a string");
        assert.equal(body["errors"], undefined,
          spec.method + " " + spec.requestPath + " envelope: errors field must be absent for top-level error responses");
      } else {

        assert.equal(typeof body["errors"], "object",
          spec.method + " " + spec.requestPath + " envelope: errors field must be an object map");
        assert.notEqual(body["errors"], null,
          spec.method + " " + spec.requestPath + " envelope: errors field must not be null");
        assert.equal(body["error"], undefined,
          spec.method + " " + spec.requestPath + " envelope: error field must be absent for form-error responses");
      }
    });
  }
});

describe("HTTP success envelope - representative success response", () => {

  test("a successful POST follows the sendSuccess shape: { success: true, message?, ... }", async () => {

    /* The success-path counterpart to the error sweep above. Every successful response must carry success: true and conform to the SuccessPayload shape
     * documented in envelope.ts. We pick POST /config/tags as the representative endpoint - it has minimal precondition (just a valid tag name) and goes
     * through sendSuccess with the tags bundle attached. A regression that dropped the success: true flag, or that flipped the shape to a raw `{ tag: ... }`
     * body, fails this assertion.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/tags"), {

      body: JSON.stringify({ tag: "envelope-success-test" }),
      headers: { "Content-Type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "successful tag create must return 200; body: " + (await response.clone().text()).slice(0, 200));

    const body = await response.json() as Record<string, unknown>;

    assert.equal(body["success"], true, "success envelope: success must be true on successful responses");
    assert.equal(body["error"], undefined, "success envelope: error field must be absent on success");
    assert.equal(body["errors"], undefined, "success envelope: errors field must be absent on success");
  });
});
