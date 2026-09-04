/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * crud.test.ts: Integration tests for the channel CRUD endpoints. The priority target is handlePredefinedEdit, which routes per-field edits to the correct
 * stored entry: identity fields land on the canonical entry, binding fields land on the active variant entry. Tests stand up a temporary data directory and
 * exercise the real persistence layer end-to-end - no mocked persistence boundary - so verified behavior matches what production users see.
 */
import type { Express, RequestHandler } from "express";
import { afterEach, beforeEach, describe, test } from "node:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import assert from "node:assert/strict";
import { initializeDataDir } from "../../../../config/paths.ts";
import { initializeUserChannels } from "../../../../config/userChannels.ts";
import { makeReqRes } from "../../../express.helpers.ts";
import os from "node:os";
import path from "node:path";
import { registerCrudRoutes } from "./crud.ts";

/* CapturedRoute mirrors the registration shape of an Express app entry. Keys identify the verb+path combination so tests can look up the registered handler
 * directly instead of routing through a real HTTP server.
 */
interface CapturedRoute {

  handler: RequestHandler;
  method: "delete" | "patch" | "post" | "put";
  path: string;
}

/* Minimal Express stub. The crud endpoints register via app.post / app.put / app.delete / app.patch with a string path and a single RequestHandler. We capture
 * each registration in order so tests can dispatch by method+path. Other Express surface area is unused by these endpoints, so we don't model it.
 */
function makeMockApp(): { app: Express; routes: CapturedRoute[] } {

  const routes: CapturedRoute[] = [];

  function capture(method: CapturedRoute["method"]) {

    return (routePath: string, handler: RequestHandler): unknown => {

      routes.push({ handler, method, path: routePath });

      return undefined;
    };
  }

  const app = {

    delete: capture("delete"),
    patch: capture("patch"),
    post: capture("post"),
    put: capture("put")
  } as unknown as Express;

  return { app, routes };
}

/* Resolves a registered handler by method+path. Throws if no match is found so a test that mistypes the path fails loudly instead of skipping silently.
 */
function findRoute(routes: CapturedRoute[], method: CapturedRoute["method"], routePath: string): RequestHandler {

  const route = routes.find((r) => (r.method === method) && (r.path === routePath));

  assert.ok(route, "no route registered for " + method + " " + routePath);

  return route.handler;
}

/* makeFormBody constructs a complete form-body record from per-test overrides. The handler's parseFormBody reads every field, so callers must supply every
 * field even if just to declare it absent (empty string). Centralizing the defaults keeps each test focused on the values that matter for the scenario rather
 * than restating the full shape.
 */
function makeFormBody(overrides: Partial<Record<string, string>> = {}): Record<string, string> {

  return {

    channelNumber: "",
    channelSelector: "",
    guideTitle: "",
    hdhrEnabled: "true",
    logoUrl: "",
    name: "",
    profile: "",
    stationId: "",
    tags: "",
    url: "",
    ...overrides
  };
}

/* setupTempDataDir builds a fresh temp dir, points the data-dir resolver at it, and runs initializeUserChannels so the in-memory state matches the empty
 * channels.json that the file store will lazily create on first write. Tests use this once per test and rm() in afterEach.
 */
async function setupTempDataDir(): Promise<string> {

  const dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-crud-test-"));

  initializeDataDir(dir);
  await initializeUserChannels();

  return dir;
}

/* Reads channels.json from the temp data dir. Returns the stored entries map (channel-key -> stored fields) by stripping the framework's metadata fields. The
 * file format spreads each channel entry at the top level alongside framework keys (schemaVersion, migrationsApplied, serviceSelections, tagRegistry). We
 * extract just the channel entries so test assertions can index by channel key without dancing around metadata.
 */
async function readChannelsFile(dir: string): Promise<Record<string, Record<string, unknown>>> {

  try {

    const raw = await readFile(path.join(dir, "channels.json"), "utf8");
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    const result: Record<string, Record<string, unknown>> = {};
    const metaFields = new Set([ "migrationsApplied", "schemaVersion", "serviceSelections", "tagRegistry" ]);

    for(const [ key, value ] of Object.entries(parsed)) {

      if(!metaFields.has(key) && (typeof value === "object") && (value !== null)) {

        result[key] = value as Record<string, unknown>;
      }
    }

    return result;
  } catch {

    return {};
  }
}

/* Reads the serviceSelections map from channels.json. The stored-entry reader above strips framework metadata, so selection assertions need their own accessor
 * rather than re-parsing the file inline at each call site.
 */
async function readServiceSelections(dir: string): Promise<Record<string, string>> {

  try {

    const raw = await readFile(path.join(dir, "channels.json"), "utf8");
    const parsed = JSON.parse(raw) as Record<string, unknown>;

    return (parsed["serviceSelections"] ?? {}) as Record<string, string>;
  } catch {

    return {};
  }
}

describe("registerCrudRoutes", () => {

  test("registers all five RESTful endpoints with the expected verb+path combinations", () => {

    const { app, routes } = makeMockApp();

    registerCrudRoutes(app);

    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/channels")), "POST /config/channels");
    assert.ok(routes.find((r) => (r.method === "put") && (r.path === "/config/channels/:key")), "PUT /config/channels/:key");
    assert.ok(routes.find((r) => (r.method === "delete") && (r.path === "/config/channels/:key")), "DELETE /config/channels/:key");
    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/channels/:key/revert")), "POST /config/channels/:key/revert");
    assert.ok(routes.find((r) => (r.method === "patch") && (r.path === "/config/channels/:key")), "PATCH /config/channels/:key");
  });
});

describe("handlePredefinedEdit (PUT /config/channels/:key)", () => {

  let dir: string;
  let put: RequestHandler;

  beforeEach(async () => {

    dir = await setupTempDataDir();

    const { app, routes } = makeMockApp();

    registerCrudRoutes(app);

    put = findRoute(routes, "put", "/config/channels/:key");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  /* The form values used by the edit form mirror the resolved display channel. When the user opens the edit dialog for a predefined channel like 'abc' (no
   * variant active), the form is pre-populated with ABC's identity (name "ABC", tags ["Local"]) and the canonical site URL. Tests construct realistic form
   * payloads from those known values so the comparison logic the handler runs is deterministic.
   */

  test("no-op save: submitting unchanged form values does not write a stored override and reports 'No changes to save.'", async () => {

    // Submission matches the resolved display channel exactly. The handler short-circuits via channelMatches and returns the no-op message without writing.
    const formBody = makeFormBody({ name: "ABC", tags: "Local", url: "https://abc.com/watch-live" });
    const { json, req, res } = makeReqRes({ body: formBody, params: { key: "abc" } });

    await put(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.equal(body["message"], "No changes to save.", "no-op message");

    const stored = await readChannelsFile(dir);

    assert.equal(stored["abc"], undefined, "no canonical override should be written for an unchanged save");
  });

  test("identity edit (no variant active): the canonical entry receives an identity-only delta", async () => {

    // The user renames ABC. Since no variant is active for this canonical, the entire delta routes to the canonical entry. The stored entry should carry the
    // renamed identity field only; URL and other binding fields stay absent (they match canonical).
    const formBody = makeFormBody({ name: "ABC Custom Rename", tags: "Local", url: "https://abc.com/watch-live" });
    const { json, req, res } = makeReqRes({ body: formBody, params: { key: "abc" } });

    await put(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.match(body["message"] as string, /updated successfully/);

    const stored = await readChannelsFile(dir);
    const abcEntry = stored["abc"];

    assert.ok(abcEntry, "canonical entry must be written when an identity field changed");
    assert.equal(abcEntry["name"], "ABC Custom Rename", "name override stored on canonical");
    assert.equal(abcEntry["url"], undefined, "URL must not be stored as it matches the canonical default");
  });

  /* The variant-routing tests below lock the rule that when a variant is active and a user submits mixed identity+binding edits, identity values land on
   * the canonical entry and binding values land on the variant entry. A regression that misroutes either direction would silently lose customization or
   * apply the wrong value at resolution time.
   */

  test("identity-vs-binding routing (variant active): identity goes to canonical, binding goes to the variant entry", async () => {

    // Switch the active service for ABC to Hulu via the channels file (we set the selection before the test exercises the PUT). We don't go through the
    // /config/channels/:key/service endpoint because the test surface is the PUT handler, not the service selector. Writing the selection file directly mirrors
    // what setServiceSelection would persist.
    const { mutateChannels } = await import("../../../../config/userChannels.ts");

    await mutateChannels((data) => {

      data.serviceSelections["abc"] = "abc-hulu";
    });

    // Submit a save that customizes both an identity field (name -> "ABC Custom") and a binding field (channelSelector -> "ABC-CUSTOM"). The handler must split
    // these between abc (canonical) and abc-hulu (variant).
    const formBody = makeFormBody({ channelSelector: "ABC-CUSTOM", name: "ABC Custom", tags: "Local", url: "https://www.hulu.com/live" });
    const { json, req, res } = makeReqRes({ body: formBody, params: { key: "abc" } });

    await put(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true, "save should succeed");

    const stored = await readChannelsFile(dir);
    const abcEntry = stored["abc"];
    const variantEntry = stored["abc-hulu"];

    // Canonical entry: identity-only. Should have the renamed name field; should NOT have the channelSelector binding field.
    assert.ok(abcEntry, "canonical entry must exist for the identity edit");
    assert.equal(abcEntry["name"], "ABC Custom", "identity field routed to the canonical entry");
    assert.equal(abcEntry["channelSelector"], undefined, "binding field must NOT be written on the canonical entry");

    // Variant entry: binding-only. Should have the channelSelector override; should NOT have the name identity field. canonicalKey may be present (preserved
    // by replaceVariantBinding from the prior entry) but is not an identity field, so it doesn't violate the partition.
    assert.ok(variantEntry, "variant entry must exist for the binding edit");
    assert.equal(variantEntry["channelSelector"], "ABC-CUSTOM", "binding field routed to the variant entry");
    assert.equal(variantEntry["name"], undefined, "identity field must NOT be written on the variant entry");
  });

  test("identity-only edit with a variant active routes nothing to the variant entry (canonical-only write)", async () => {

    // When only identity fields differ, the canonical-relative delta has identity changes but the binding-relative delta against the variant is empty (the
    // user's submitted URL still matches the variant's predefined URL). The handler must NOT create an empty variant override row - it should leave the
    // variant entry alone (no key in the stored map).
    const { mutateChannels } = await import("../../../../config/userChannels.ts");

    await mutateChannels((data) => {

      data.serviceSelections["abc"] = "abc-hulu";
    });

    const formBody = makeFormBody({ channelSelector: "ABC", name: "ABC Identity Only", tags: "Local", url: "https://www.hulu.com/live" });
    const { json, req, res } = makeReqRes({ body: formBody, params: { key: "abc" } });

    await put(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);

    const stored = await readChannelsFile(dir);
    const abcEntry = stored["abc"];

    assert.ok(abcEntry, "canonical entry must carry the identity edit");
    assert.equal(abcEntry["name"], "ABC Identity Only");

    // The variant entry must not exist - there's no binding override and no preserved canonicalKey to keep around (the variant didn't have one stored).
    assert.equal(stored["abc-hulu"], undefined, "variant entry must NOT be written when no binding-field override applies");
  });

  test("revert path: submission matching predefined exactly clears any stored override and announces revert", async () => {

    // Pre-seed a canonical override so the test exercises the revert branch (hasCanonicalEntry = true).
    const { mutateChannels } = await import("../../../../config/userChannels.ts");

    await mutateChannels((data) => {

      data.channels["abc"] = { name: "ABC Override" };
    });

    // Submit values matching the predefined exactly. handlePredefinedEdit's canonicalHasChanges is false, so it routes to the revert branch.
    const formBody = makeFormBody({ name: "ABC", tags: "Local", url: "https://abc.com/watch-live" });
    const { json, req, res } = makeReqRes({ body: formBody, params: { key: "abc" } });

    await put(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.match(body["message"] as string, /reverted to defaults/, "revert message expected");

    const stored = await readChannelsFile(dir);

    assert.equal(stored["abc"], undefined, "stored override must be cleared on revert");
  });

  test("forceCapture round-trip: a checked box stores the flag on the canonical entry and a later unchecked save removes it", async () => {

    // A rename goes along with the flag so the second save still carries a change of its own. That keeps the clear on the delta path rather than emptying the
    // delta and routing through the revert branch, which would prove nothing about how the field itself is stored.
    const setBody = makeFormBody({ forceCapture: "true", name: "ABC Renamed", tags: "Local", url: "https://abc.com/watch-live" });
    const set = makeReqRes({ body: setBody, params: { key: "abc" } });

    await put(set.req, set.res, () => undefined);

    const afterSet = (await readChannelsFile(dir))["abc"];

    assert.ok(afterSet, "the canonical entry must be written");
    assert.equal(afterSet["forceCapture"], true, "a checked box stores the flag on the canonical entry");

    // Second save with the box clear. The stored delta is replaced wholesale, so the field goes away rather than persisting as an explicit null.
    const clearBody = makeFormBody({ name: "ABC Renamed", tags: "Local", url: "https://abc.com/watch-live" });
    const clear = makeReqRes({ body: clearBody, params: { key: "abc" } });

    await put(clear.req, clear.res, () => undefined);

    const afterClear = (await readChannelsFile(dir))["abc"];

    assert.ok(afterClear, "the rename override still stands");
    assert.equal(afterClear["name"], "ABC Renamed", "the unrelated override survives the clear");
    assert.equal("forceCapture" in afterClear, false, "unchecking removes the field rather than storing null");
  });

  test("missing key in URL produces a 400 validation error", async () => {

    const { json, req, res, status } = makeReqRes({ body: {}, params: {} });

    await put(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], false);
    assert.match(body["error"] as string, /Channel key is required/);
  });

  test("invalid form values produce a 400 with the field-keyed errors map", async () => {

    // Channel name is required by validateChannelName. An empty form name with a valid URL surfaces only the name error.
    const formBody = makeFormBody({ url: "https://abc.com/watch-live" });
    const { json, req, res, status } = makeReqRes({ body: formBody, params: { key: "abc" } });

    await put(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], false);
    assert.ok(body["errors"], "field-keyed errors map should be present");
  });

  /* Sibling-variant inference (no variant pre-selected). The earlier identity-vs-binding routing tests above set serviceSelections[key] = variantKey before
   * invoking PUT, exercising the explicit-variant branch. The tests below cover the URL-inferred branch: no serviceSelections entry exists, but the submitted
   * URL's domain matches a sibling variant. The handler routes per-field exactly like the variant-active branch and records serviceSelections[key] = inferred
   * variant. This is the producer-side enforcement of the sibling-variant non-overlap rule documented in types/channels.ts (CanonicalChannel block).
   */

  test("PUT with submitted URL matching a sibling variant's domain (no pre-existing selection) routes per-field and records the redirect", async () => {

    /* User opens the Edit form for ABC (no service selection set). Form pre-populates with the canonical (site URL = abc.com/watch-live). User adds a
     * stationId AND changes the URL to hulu.com/live (intending "default ABC to Hulu"). Expected: stationId lands on the canonical entry, URL is recognized as
     * matching the abc-hulu sibling and is NOT stored on the canonical (binding matches predefined exactly so no variant override is created either),
     * serviceSelections.abc = "abc-hulu".
     */
    const formBody = makeFormBody({ name: "ABC", stationId: "20456", tags: "Local", url: "https://www.hulu.com/live" });
    const { json, req, res } = makeReqRes({ body: formBody, params: { key: "abc" } });

    await put(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);

    const stored = await readChannelsFile(dir);
    const abcEntry = stored["abc"];
    const variantEntry = stored["abc-hulu"];

    assert.ok(abcEntry, "canonical entry must carry the identity edit (stationId)");
    assert.equal(abcEntry["stationId"], "20456", "stationId routed to the canonical entry");
    assert.equal(abcEntry["url"], undefined, "URL must NOT land on the canonical (it matches a sibling variant's domain)");
    assert.equal(abcEntry["channelSelector"], undefined, "channelSelector must NOT land on the canonical");

    assert.equal(variantEntry, undefined, "no variant override created when binding matches predefined exactly");

    const raw = await readFile(path.join(dir, "channels.json"), "utf8");
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    const selections = (parsed["serviceSelections"] ?? {}) as Record<string, string>;

    assert.equal(selections["abc"], "abc-hulu", "serviceSelections records the URL-inferred redirect");
  });

  test("PUT with submitted URL matching a sibling AND a divergent binding field persists the divergence as a variant binding-only override", async () => {

    /* Same as above but the user also customizes channelSelector. The matching sibling abc-hulu has predefined channelSelector "ABC"; the user submits
     * "MyCustomABC". The handler routes the divergent binding to the variant entry, leaving the canonical with identity-only fields.
     */
    const formBody = makeFormBody({ channelSelector: "MyCustomABC", name: "ABC", stationId: "20456", tags: "Local", url: "https://www.hulu.com/live" });
    const { json, req, res } = makeReqRes({ body: formBody, params: { key: "abc" } });

    await put(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);

    const stored = await readChannelsFile(dir);
    const abcEntry = stored["abc"];
    const variantEntry = stored["abc-hulu"];

    assert.deepEqual(abcEntry, { stationId: "20456" }, "canonical retains identity-only delta");
    assert.ok(variantEntry, "variant entry must exist when binding diverges from predefined");
    assert.equal(variantEntry["channelSelector"], "MyCustomABC", "divergent channelSelector persisted as variant binding override");
    assert.equal(variantEntry["url"], undefined, "URL not stored on variant - it matches the predefined Hulu URL");
  });

  test("PUT with a truly custom URL (no sibling match) leaves the full delta on the canonical and clears serviceSelections", async () => {

    /* The legitimate Custom-URL case: user enters a URL whose domain matches no sibling variant. The handler treats this as a real custom override - full
     * delta lands on the canonical, serviceSelections is cleared so the dropdown reflects the "Custom (domain)" state via buildServiceGroups Scenario B. This
     * preserves backward-compatible behavior for users who have genuinely customized URLs.
     */
    const formBody = makeFormBody({ name: "ABC", stationId: "20456", tags: "Local", url: "https://example.com/abc-mirror" });
    const { json, req, res } = makeReqRes({ body: formBody, params: { key: "abc" } });

    await put(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);

    const stored = await readChannelsFile(dir);
    const abcEntry = stored["abc"];

    assert.ok(abcEntry, "canonical entry must exist for the identity edit");
    assert.equal(abcEntry["stationId"], "20456");
    assert.equal(abcEntry["url"], "https://example.com/abc-mirror", "custom URL stays on the canonical when no sibling matches");

    const raw = await readFile(path.join(dir, "channels.json"), "utf8");
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    const selections = (parsed["serviceSelections"] ?? {}) as Record<string, string>;

    assert.equal(selections["abc"], undefined, "serviceSelections cleared for the custom-URL case");
  });

  /* Full-value sibling matching (the implicit revert-to-a-sibling path). The submission is compared against every sibling's PURE predefined definition, so a save
   * that reproduces a sibling's values exactly is a revert to that sibling whatever was stored beforehand: the selection moves to the match, and the canonical
   * override, the matched sibling's own override, and the previously-active sibling's override all clear so the resolved channel equals what the user saved. The
   * tests below cover each stored-state shape that reaches the branch - an explicit selection, a filter-computed selection, and no selection at all - plus the
   * same-domain case that URL-domain inference cannot tell apart.
   */

  test("full-value sibling match with a different variant selected switches the selection and stores no overrides", async () => {

    // Cox is the active service and nothing is stored. Submitting Hulu's exact predefined values reverts to the Hulu sibling: the selection moves and neither the
    // old nor the new variant is left with an entry.
    const { mutateChannels } = await import("../../../../config/userChannels.ts");

    await mutateChannels((data) => {

      data.serviceSelections["abc"] = "abc-cox";
    });

    const formBody = makeFormBody({ channelSelector: "ABC", name: "ABC", tags: "Local", url: "https://www.hulu.com/live" });
    const { json, req, res } = makeReqRes({ body: formBody, params: { key: "abc" } });

    await put(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.match(body["message"] as string, /reverted to defaults/, "the revert branch announces the revert");

    const stored = await readChannelsFile(dir);

    assert.equal(stored["abc"], undefined, "no canonical override survives the revert");
    assert.equal(stored["abc-cox"], undefined, "the previously-active variant keeps no override");
    assert.equal(stored["abc-hulu"], undefined, "the matched variant keeps no override");

    const selections = await readServiceSelections(dir);

    assert.equal(selections["abc"], "abc-hulu", "the selection lands on the matched sibling");
  });

  test("full-value sibling match clears the override on the variant it switches away from", async () => {

    // Cox is the active service and carries a binding override of its own. Reverting to the Hulu sibling moves the channel off Cox, so that override is cleared
    // too - leaving it behind parks a customization on a service this channel has stopped resolving through, ready to resurface if the user ever switches back.
    const { mutateChannels } = await import("../../../../config/userChannels.ts");

    await mutateChannels((data) => {

      data.channels["abc-cox"] = { canonicalKey: "abc", channelSelector: "COX-CUSTOM" };
      data.serviceSelections["abc"] = "abc-cox";
    });

    const formBody = makeFormBody({ channelSelector: "ABC", name: "ABC", tags: "Local", url: "https://www.hulu.com/live" });
    const { json, req, res } = makeReqRes({ body: formBody, params: { key: "abc" } });

    await put(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.match(body["message"] as string, /reverted to defaults/, "the revert branch announces the revert");

    const stored = await readChannelsFile(dir);

    assert.equal(stored["abc-cox"], undefined, "the override on the variant being switched away from is cleared");
    assert.equal(stored["abc"], undefined, "no canonical override survives the revert");
    assert.equal(stored["abc-hulu"], undefined, "the matched variant keeps no override");

    const selections = await readServiceSelections(dir);

    assert.equal(selections["abc"], "abc-hulu", "the selection lands on the matched sibling");
  });

  test("full-value sibling match with a filter-computed active variant records the match as an explicit selection", async () => {

    /* No selection is stored: the active variant arises computationally because the service filter excludes the canonical's own service, leaving Cox first among
     * the enabled siblings. A submission matching Hulu's predefined values exactly still reverts to Hulu, and the resolution is written back as an explicit
     * stored selection.
     */
    const { getEnabledServices, setEnabledServices } = await import("../../../../config/services.ts");
    const previousServices = getEnabledServices();

    setEnabledServices([ "cox", "hulu" ]);

    try {

      const formBody = makeFormBody({ channelSelector: "ABC", name: "ABC", tags: "Local", url: "https://www.hulu.com/live" });
      const { json, req, res } = makeReqRes({ body: formBody, params: { key: "abc" } });

      await put(req, res, () => undefined);

      const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

      assert.equal(body["success"], true);
      assert.match(body["message"] as string, /reverted to defaults/, "the revert branch announces the revert");

      const stored = await readChannelsFile(dir);

      assert.equal(stored["abc"], undefined, "no canonical override is written");
      assert.equal(stored["abc-cox"], undefined, "the filter-resolved variant keeps no override");
      assert.equal(stored["abc-hulu"], undefined, "the matched variant keeps no override");

      const selections = await readServiceSelections(dir);

      assert.equal(selections["abc"], "abc-hulu", "the matched sibling becomes the explicit selection");
    } finally {

      // The enabled-services filter is module state shared by every test in this process, so restore it even when an assertion above throws.
      setEnabledServices(previousServices);
    }
  });

  test("full-value match on the active variant clears that variant's own stored override", async () => {

    // The user has customized the canonical's identity and the active variant's binding, then saves the variant's exact predefined values. Both entries clear - a
    // surviving variant override would resurface the channelSelector the user just cleared.
    const { mutateChannels } = await import("../../../../config/userChannels.ts");

    await mutateChannels((data) => {

      data.channels["abc"] = { name: "ABC Custom" };
      data.channels["abc-hulu"] = { canonicalKey: "abc", channelSelector: "MyCustomABC" };
      data.serviceSelections["abc"] = "abc-hulu";
    });

    const formBody = makeFormBody({ channelSelector: "ABC", name: "ABC", tags: "Local", url: "https://www.hulu.com/live" });
    const { json, req, res } = makeReqRes({ body: formBody, params: { key: "abc" } });

    await put(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.match(body["message"] as string, /reverted to defaults/, "the revert branch announces the revert");

    const stored = await readChannelsFile(dir);

    assert.equal(stored["abc"], undefined, "the canonical override is cleared");
    assert.equal(stored["abc-hulu"], undefined, "the matched variant's own override is cleared");

    const selections = await readServiceSelections(dir);

    assert.equal(selections["abc"], "abc-hulu", "the selection still points at the matched variant");
  });

  test("full-value match on the active variant clears its override even with no canonical override stored", async () => {

    // Only the variant carries an override. The response is what tells the two paths apart here: this is a revert, not an ordinary update that happens to leave
    // the same stored state behind.
    const { mutateChannels } = await import("../../../../config/userChannels.ts");

    await mutateChannels((data) => {

      data.channels["abc-hulu"] = { canonicalKey: "abc", channelSelector: "MyCustomABC" };
      data.serviceSelections["abc"] = "abc-hulu";
    });

    const formBody = makeFormBody({ channelSelector: "ABC", name: "ABC", tags: "Local", url: "https://www.hulu.com/live" });
    const { json, req, res } = makeReqRes({ body: formBody, params: { key: "abc" } });

    await put(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.match(body["message"] as string, /reverted to defaults/, "a full-value match reports a revert, not an update");

    const stored = await readChannelsFile(dir);

    assert.equal(stored["abc-hulu"], undefined, "the matched variant's own override is cleared");
  });

  test("full-value sibling match with only a canonical override switches the selection and clears the canonical", async () => {

    // The original documented case: a customized canonical, no selection, and a submission matching a sibling exactly.
    const { mutateChannels } = await import("../../../../config/userChannels.ts");

    await mutateChannels((data) => {

      data.channels["abc"] = { name: "ABC Override" };
    });

    const formBody = makeFormBody({ channelSelector: "ABC", name: "ABC", tags: "Local", url: "https://www.hulu.com/live" });
    const { json, req, res } = makeReqRes({ body: formBody, params: { key: "abc" } });

    await put(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.match(body["message"] as string, /reverted to defaults/, "the revert branch announces the revert");

    const stored = await readChannelsFile(dir);

    assert.equal(stored["abc"], undefined, "the canonical override is cleared");

    const selections = await readServiceSelections(dir);

    assert.equal(selections["abc"], "abc-hulu", "the selection lands on the matched sibling");
  });

  test("full-value sibling match picks the exact variant when a same-domain sibling exists", async () => {

    /* A user-stored sibling sits on the same domain as the Hulu variant and sorts ahead of it alphabetically, so URL-domain inference would pick that one.
     * Full-value matching compares every submitted field against each sibling's predefined data, so it lands on abc-hulu - the sibling whose values the user
     * actually submitted - and leaves the same-domain entry untouched.
     */
    const { mutateChannels } = await import("../../../../config/userChannels.ts");
    const alphaEntry = { canonicalKey: "abc", url: "https://www.hulu.com/live/alpha" };

    await mutateChannels((data) => {

      data.channels["abc-alpha"] = { ...alphaEntry };
    });

    const formBody = makeFormBody({ channelSelector: "ABC", name: "ABC", tags: "Local", url: "https://www.hulu.com/live" });
    const { json, req, res } = makeReqRes({ body: formBody, params: { key: "abc" } });

    await put(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.match(body["message"] as string, /reverted to defaults/, "the revert branch announces the revert");

    const stored = await readChannelsFile(dir);

    assert.equal(stored["abc"], undefined, "no canonical override is written");
    assert.equal(stored["abc-hulu"], undefined, "the matched variant keeps no override");
    assert.deepEqual(stored["abc-alpha"], alphaEntry, "the same-domain sibling is left untouched");

    const selections = await readServiceSelections(dir);

    assert.equal(selections["abc"], "abc-hulu", "the selection lands on the full-value match, not the alphabetically-first domain match");
  });
});

describe("DELETE /config/channels/:key and POST /config/channels/:key/revert (validation paths)", () => {

  let dir: string;
  let del: RequestHandler;
  let revert: RequestHandler;

  beforeEach(async () => {

    dir = await setupTempDataDir();

    const { app, routes } = makeMockApp();

    registerCrudRoutes(app);

    del = findRoute(routes, "delete", "/config/channels/:key");
    revert = findRoute(routes, "post", "/config/channels/:key/revert");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("DELETE rejects deletion of a key that is not a user-defined channel (predefined cannot be deleted)", async () => {

    const { json, req, res, status } = makeReqRes({ params: { key: "abc" } });

    await del(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /not a user-defined channel/);
  });

  test("DELETE rejects when the key is missing entirely", async () => {

    const { req, res, status } = makeReqRes({ params: {} });

    await del(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);
  });

  test("POST /revert rejects when key is not a predefined channel", async () => {

    const { json, req, res, status } = makeReqRes({ params: { key: "definitely-not-a-real-channel" } });

    await revert(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /not a predefined channel/);
  });

  test("POST /revert rejects when no override exists for an otherwise-valid predefined key", async () => {

    const { json, req, res, status } = makeReqRes({ params: { key: "abc" } });

    await revert(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /no override exists/);
  });
});

describe("POST /config/channels - profile reference validation", () => {

  let dir: string;
  let create: RequestHandler;

  beforeEach(async () => {

    dir = await setupTempDataDir();

    const { app, routes } = makeMockApp();

    registerCrudRoutes(app);

    create = findRoute(routes, "post", "/config/channels");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("accepts a channel whose profile names a provider profile", async () => {

    /* The editor save answers the profile-existence question through the single builtin lookup. disneyNow lives in the provider table and never appears in the
     * web UI's profile picker, so an oracle built from that catalog would reject this save as an unknown profile.
     */
    const { json, req, res } = makeReqRes({

      body: { key: "disney-user", name: "Disney Channel", profile: "disneyNow", url: "https://example.com/live.m3u8" }
    });

    await create(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true, "the save succeeds; errors: " + JSON.stringify(body["errors"] ?? body["error"]));
  });

  test("rejects a channel whose profile names nothing at all", async () => {

    // The lookup is wider than the UI catalog, not absent - a name no source owns still draws a field error naming the rejected value.
    const { json, req, res, status } = makeReqRes({

      body: { key: "bogus-user", name: "Bogus", profile: "noSuchProfileAnywhere", url: "https://example.com/live.m3u8" }
    });

    await create(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;
    const errors = body["errors"] as Record<string, string>;

    assert.equal(errors["profile"], "Unknown profile: noSuchProfileAnywhere.");
  });
});

describe("forceCapture on the user-channel path (POST /config/channels, PUT /config/channels/:key)", () => {

  let dir: string;
  let create: RequestHandler;
  let put: RequestHandler;

  beforeEach(async () => {

    dir = await setupTempDataDir();

    const { app, routes } = makeMockApp();

    registerCrudRoutes(app);

    create = findRoute(routes, "post", "/config/channels");
    put = findRoute(routes, "put", "/config/channels/:key");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("create with the box checked stores the flag, and a later save with the box clear removes it", async () => {

    /* A channel with no predefined base never reaches the delta machinery: buildUserChannelFromForm hand-enumerates each field onto a fresh record, and both
     * the create and the standalone-channel replace go through it. No predefined-path test exercises that builder, so this is the assertion that covers it.
     */
    const createBody = makeFormBody({ forceCapture: "true", key: "forced-user", name: "Forced", url: "https://example.com/live.m3u8" });
    const created = makeReqRes({ body: createBody });

    await create(created.req, created.res, () => undefined);

    const createdBody = created.json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(createdBody["success"], true, "the create succeeds; errors: " + JSON.stringify(createdBody["errors"] ?? createdBody["error"]));

    const afterCreate = (await readChannelsFile(dir))["forced-user"];

    assert.ok(afterCreate, "the created record must be written");
    assert.equal(afterCreate["forceCapture"], true, "a checked box stores the flag on the user record");

    // The standalone-channel PUT replaces the record wholesale, so leaving the box clear drops the field.
    const clearBody = makeFormBody({ key: "forced-user", name: "Forced", url: "https://example.com/live.m3u8" });
    const cleared = makeReqRes({ body: clearBody, params: { key: "forced-user" } });

    await put(cleared.req, cleared.res, () => undefined);

    const afterClear = (await readChannelsFile(dir))["forced-user"];

    assert.ok(afterClear, "the record still exists after the second save");
    assert.equal("forceCapture" in afterClear, false, "unchecking removes the field from the stored record");
  });
});

describe("PATCH /config/channels/:key", () => {

  let dir: string;
  let patch: RequestHandler;

  beforeEach(async () => {

    dir = await setupTempDataDir();

    const { app, routes } = makeMockApp();

    registerCrudRoutes(app);

    patch = findRoute(routes, "patch", "/config/channels/:key");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects when no inline-edit field is provided and when more than one is provided", async () => {

    const { json: jsonNone, req: reqNone, res: resNone, status: statusNone } = makeReqRes({ body: {}, params: { key: "abc" } });

    await patch(reqNone, resNone, () => undefined);

    assert.equal(statusNone.mock.calls[0]?.arguments[0], 400);
    assert.match((jsonNone.mock.calls[0]?.arguments[0] as Record<string, unknown>)["error"] as string, /No field provided/);

    const { json: jsonMulti, req: reqMulti, res: resMulti, status: statusMulti } = makeReqRes({ body: { channelNumber: 5, stationId: "12345" }, params: { key: "abc" } });

    await patch(reqMulti, resMulti, () => undefined);

    assert.equal(statusMulti.mock.calls[0]?.arguments[0], 400);
    assert.match((jsonMulti.mock.calls[0]?.arguments[0] as Record<string, unknown>)["error"] as string, /Only one field/);
  });

  test("rejects an out-of-range channelNumber via the shared validator", async () => {

    const { req, res, status } = makeReqRes({ body: { channelNumber: 999999 }, params: { key: "abc" } });

    await patch(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);
  });

  test("accepts a valid channelNumber and persists it as a delta on the canonical entry", async () => {

    const { json, req, res } = makeReqRes({ body: { channelNumber: 7 }, params: { key: "abc" } });

    await patch(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.match(body["message"] as string, /Channel number/);

    const abcEntry = (await readChannelsFile(dir))["abc"];

    assert.ok(abcEntry, "canonical entry must exist after patch");
    assert.equal(abcEntry["channelNumber"], 7, "delta must persist the new channel number");
  });
});
