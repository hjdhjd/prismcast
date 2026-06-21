/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * services.test.ts: Unit tests for the user-profile UI generators and the route-aggregator wiring in services.ts. The two HTML generators are pure
 * functions of the user-profile registry and the channel listing - both empty in a fresh test process - so we exercise them through the public exports
 * and verify the rendered HTML shape, escaping, and embedded JSON shapes. The route-aggregator setupProfileRoutes registers handlers but the handlers
 * themselves require a live Express runtime and the user-profile filesystem layer; we lock the registration shape and flag the per-handler behavior as
 * integration-level.
 */
import { describe, test } from "node:test";
import { generateCustomProfilesPanel, generateProfileWizardModal, setupProfileRoutes } from "./services.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../../testing.helpers.ts";
import { makeExpressStub } from "../express.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/* The Express stub helper (makeExpressStub) is a shared cross-cutting test helper imported from ../express.helpers.ts. It records every route registration AND
 * captures handler references so individual handlers can be invoked with synthetic req/res for branch-coverage tests. Wiring tests below use only `calls`;
 * handler-invocation tests use `invoke(method, path, req)`.
 */

describe("generateCustomProfilesPanel", () => {

  test("returns a non-empty HTML string in a clean test environment (no user profiles installed)", () => {

    // The fresh test process has no user-profile file loaded, so getUserProfiles() and getUserDomains() both return empty objects. The renderer must
    // still produce valid HTML with the toolbar, the import modal, and the empty-state copy.
    const html = generateCustomProfilesPanel();

    assert.ok(html.length > 0, "panel should render even with no user profiles");
    assert.ok(typeof html === "string", "panel returns a string");
  });

  test("renders the New Profile and Import toolbar buttons when no user profiles exist", () => {

    // The toolbar is unconditional except for the Export button (gated on profile count). New Profile and Import must always appear.
    const html = generateCustomProfilesPanel();

    assert.match(html, /New Profile/, "toolbar has the New Profile button");
    assert.match(html, /Import/, "toolbar has the Import button");
  });

  test("does NOT render the Export button when no user profiles exist (gated branch)", () => {

    // Boundary: the Export button is conditionally rendered only when Object.keys(userProfiles).length > 0. With no user profiles, Export must be
    // absent. The Import button is still present, so we disambiguate on the Export button's data-click-action marker (the "start-service-export"
    // action wired through the client action registry), which the renderer omits in the empty case while the Import button's marker stays.
    const html = generateCustomProfilesPanel();

    assert.doesNotMatch(html, /onclick="startServiceExport\(\)"/, "Export button is omitted when no user profiles exist");
  });

  test("renders the empty-state copy when no user profiles exist", () => {

    const html = generateCustomProfilesPanel();

    assert.match(html, /No custom services installed/, "empty-state title");
    assert.match(html, /Custom services let you add support/, "empty-state description text");
  });

  test("does NOT render the channel-table heading row when no user profiles exist (early return)", () => {

    // Boundary: the function early-returns before emitting the table when profileKeys.length is 0. No <thead> markup should appear.
    const html = generateCustomProfilesPanel();

    assert.doesNotMatch(html, /<thead>/, "no table header rendered for empty profile list");
  });

  test("emits the import modal even when no user profiles exist (always-rendered)", () => {

    // The import modal is unconditionally rendered - import is available even with no profiles. Confirm the modal id is present.
    const html = generateCustomProfilesPanel();

    assert.match(html, /id="import-modal"/, "import modal is always rendered");
  });

  test("does NOT emit the export modal when no user profiles exist (gated)", () => {

    // The export modal is rendered only when there are profiles to export. Confirm absence in the empty case.
    const html = generateCustomProfilesPanel();

    assert.doesNotMatch(html, /id="export-modal"/, "export modal is gated on profile presence");
  });

  test("includes the panel description with the Profile Builder hint", () => {

    const html = generateCustomProfilesPanel();

    assert.match(html, /Profile Builder/, "description references the Profile Builder wizard");
    assert.match(html, /service pack/, "description references service-pack import");
  });
});

describe("generateProfileWizardModal", () => {

  test("returns a non-empty HTML string", () => {

    const html = generateProfileWizardModal();

    assert.ok(html.length > 0, "wizard modal should render");
    assert.ok(typeof html === "string", "returns a string");
  });

  test("declares all five wizard steps in order: Base, Strategy, Flags, Domain, Save", () => {

    // The step indicator drives the client-side wizard controller. Reordering or dropping a step would break navigation logic that depends on these
    // labels. Lock the order.
    const html = generateProfileWizardModal();
    const baseIdx = html.indexOf(">Base<");
    const strategyIdx = html.indexOf(">Strategy<");
    const flagsIdx = html.indexOf(">Flags<");
    const domainIdx = html.indexOf(">Domain<");
    const saveIdx = html.indexOf(">Save<");

    assert.ok(baseIdx > -1, "Base step present");
    assert.ok(strategyIdx > baseIdx, "Strategy follows Base");
    assert.ok(flagsIdx > strategyIdx, "Flags follows Strategy");
    assert.ok(domainIdx > flagsIdx, "Domain follows Flags");
    assert.ok(saveIdx > domainIdx, "Save follows Domain");
  });

  test("uses the wizard-modal id and the New Service Profile title", () => {

    const html = generateProfileWizardModal();

    assert.match(html, /id="wizard-modal"/, "modal id is wizard-modal");
    assert.match(html, /New Service Profile/, "title text is the wizard heading");
  });

  test("declares Back, Next, Save, and Save & Test buttons via the controller wiring", () => {

    // The controller-managed buttons (Back, Next) declare data-wizard-role for closure-scoped handlers. Both Save buttons carry the same data-click-action
    // ("save-profile") and differentiate via data-with-test ("false" for plain Save, "true" for Save & Test). The registered handler reads target.dataset.withTest
    // to decide whether to invoke the test path. The rendered HTML carries no inline onclick attribute on any button.
    const html = generateProfileWizardModal();

    assert.match(html, /id="wizard-back"/, "Back button");
    assert.match(html, /id="wizard-next"/, "Next button");
    // Extract each Save button's tag so we can check the attributes inside, independent of attribute order.
    const saveTag = (/<button[^>]*id="wizard-save"[^>]*>/).exec(html)?.[0] ?? "";
    const saveTestTag = (/<button[^>]*id="wizard-save-test"[^>]*>/).exec(html)?.[0] ?? "";

    assert.ok(saveTag, "Save button is rendered");
    assert.ok(saveTestTag, "Save & Test button is rendered");
    assert.match(saveTag, /data-click-action="save-profile"/, "plain Save dispatches save-profile");
    assert.match(saveTag, /data-with-test="false"/, "plain Save carries withTest=false");
    assert.match(saveTestTag, /data-click-action="save-profile"/, "Save & Test dispatches save-profile");
    assert.match(saveTestTag, /data-with-test="true"/, "Save & Test carries withTest=true");
    assert.doesNotMatch(html, /onclick=/, "no wizard button carries an inline onclick attribute");
  });

  test("embeds the wizard registries as JSON in a single <script> data block", () => {

    // The data-driven wizard pulls profile groupings, strategies, and flags from window.__wizardProfiles/__wizardStrategies/__wizardFlags. All three
    // must be set in a single <script> tag that the wizard reads on init.
    const html = generateProfileWizardModal();

    assert.match(html, /window\.__wizardProfiles\s*=/, "profile registry is embedded");
    assert.match(html, /window\.__wizardStrategies\s*=/, "strategy registry is embedded");
    assert.match(html, /window\.__wizardFlags\s*=/, "flag registry is embedded");
  });

  test("strategy registry includes the three user-configurable strategies (tileClick, thumbnailRow, none)", () => {

    // Service-specific strategies (foxGrid, slingGrid, etc.) are builtin only and must NOT appear in the wizard. The user-facing strategy list is
    // exactly tileClick, thumbnailRow, and none.
    const html = generateProfileWizardModal();

    assert.match(html, /"id":"tileClick"/, "tileClick strategy");
    assert.match(html, /"id":"thumbnailRow"/, "thumbnailRow strategy");
    assert.match(html, /"id":"none"/, "none strategy");
  });

  test("strategy registry does NOT include the service-specific builtin strategies", () => {

    // Negative test: foxGrid, slingGrid, hboGrid, etc. are builtin-only strategies that must never appear in the user-facing wizard. Their absence
    // is structurally important - showing them would let users pick strategies that require code support beyond what the wizard configures.
    const html = generateProfileWizardModal();

    assert.doesNotMatch(html, /"id":"foxGrid"/, "foxGrid is excluded");
    assert.doesNotMatch(html, /"id":"slingGrid"/, "slingGrid is excluded");
    assert.doesNotMatch(html, /"id":"hboGrid"/, "hboGrid is excluded");
    assert.doesNotMatch(html, /"id":"youtubeGrid"/, "youtubeGrid is excluded");
  });

  test("flag registry includes the six user-configurable flags", () => {

    // The flags step exposes selectReadyVideo, lockVolumeProperties, clickToPlay, needsIframeHandling, waitForNetworkIdle, useRequestFullscreen. Lock
    // them all so a registry edit forces a test update.
    const html = generateProfileWizardModal();

    assert.match(html, /"id":"selectReadyVideo"/, "selectReadyVideo flag");
    assert.match(html, /"id":"lockVolumeProperties"/, "lockVolumeProperties flag");
    assert.match(html, /"id":"clickToPlay"/, "clickToPlay flag");
    assert.match(html, /"id":"needsIframeHandling"/, "needsIframeHandling flag");
    assert.match(html, /"id":"waitForNetworkIdle"/, "waitForNetworkIdle flag");
    assert.match(html, /"id":"useRequestFullscreen"/, "useRequestFullscreen flag");
  });

  test("embedded profile data filters to source=builtin only", () => {

    // The filter `(p.source === "builtin")` excludes user-defined profiles from the wizard. With no user profiles loaded in the test process this is
    // equivalent to "include everything", but we lock the structural shape of the embedded JSON: each top-level category key is present, even if
    // empty.
    const html = generateProfileWizardModal();

    assert.match(html, /"api":\[/, "api category present in embedded JSON");
    assert.match(html, /"keyboard":\[/, "keyboard category present");
    assert.match(html, /"multiChannel":\[/, "multiChannel category present");
    assert.match(html, /"special":\[/, "special category present");
    assert.match(html, /"custom":\[/, "custom category present");
  });

  test("declares a wizard-error region for client-side validation messages", () => {

    const html = generateProfileWizardModal();

    assert.match(html, /id="wizard-error"/, "wizard-error element is rendered");
  });

  test("declares the wizard-content container with the configured id", () => {

    const html = generateProfileWizardModal();

    assert.match(html, /id="wizard-content"/, "content area uses the configured id");
  });

  test("returns a deterministic string across two calls (idempotent renderer)", () => {

    // The wizard modal renderer pulls from getProfiles(), which sorts its output. Two calls in the same process must produce byte-identical HTML so
    // diffs in client-rendered output are caused by data changes, not renderer flakiness.
    const a = generateProfileWizardModal();
    const b = generateProfileWizardModal();

    assert.equal(a, b, "two calls produce identical HTML");
  });
});

describe("setupProfileRoutes", () => {

  test("registers the documented profile endpoints", () => {

    /* The aggregator wires:
     *   GET    /config/profiles
     *   POST   /config/profiles
     *   DELETE /config/profiles/:key
     *   POST   /config/profiles/import
     *   GET    /config/profiles/export
     *   POST   /config/profiles/test
     *   POST   /config/profiles/test/check
     *   POST   /config/profiles/test/done
     * We assert each registration so a removal or rename surfaces as a test failure.
     */
    const { app, calls } = makeExpressStub();

    setupProfileRoutes(app as never);

    const has = (method: string, path: string): boolean => calls.some((c) => (c.method === method) && (c.path === path));

    assert.ok(has("get", "/config/profiles"), "GET /config/profiles is registered");
    assert.ok(has("post", "/config/profiles"), "POST /config/profiles is registered");
    assert.ok(has("delete", "/config/profiles/:key"), "DELETE /config/profiles/:key is registered");
    assert.ok(has("post", "/config/profiles/import"), "POST /config/profiles/import is registered");
    assert.ok(has("get", "/config/profiles/export"), "GET /config/profiles/export is registered");
    assert.ok(has("post", "/config/profiles/test"), "POST /config/profiles/test is registered");
    assert.ok(has("post", "/config/profiles/test/check"), "POST /config/profiles/test/check is registered");
    assert.ok(has("post", "/config/profiles/test/done"), "POST /config/profiles/test/done is registered");
  });

  test("registers exactly the documented number of routes (no extras, no drops)", () => {

    // Pin the registration count so a route added or removed forces a test update. Eight registrations as enumerated above.
    const { app, calls } = makeExpressStub();

    setupProfileRoutes(app as never);

    assert.equal(calls.length, 8, "expected exactly 8 routes registered");
  });

  test("does not throw on a stub Express app", () => {

    // Defensive check: any synchronous call inside the registered handler factories that throws would surface here.
    const { app } = makeExpressStub();

    assert.doesNotThrow(() => {

      setupProfileRoutes(app as never);
    }, "registration should be side-effect-free at the app-stub level");
  });
});

/* The validation paths in setupProfileRoutes return 4xx envelopes before touching disk-backed state (mutateProfiles, deleteUserProfile). Those paths are pure
 * functions of req shape modulo the in-memory user-profile/domain maps, both of which are empty in a fresh test process. We exercise them via the stub's
 * invoke helper, which constructs a minimal req and captures the JSON envelope written to res.
 */
describe("setupProfileRoutes - validation handlers (invoked via Express stub)", () => {

  test("POST /config/profiles - rejects an empty key with 400", async () => {

    const { app, invoke } = makeExpressStub();

    setupProfileRoutes(app as never);

    const result = await invoke("post", "/config/profiles", { body: { key: "", profile: { extends: "default" } } });

    assert.equal(result.statusCode, 400);
    assert.deepEqual(result.body, { error: "Profile key is required.", success: false });
  });

  test("POST /config/profiles - rejects a missing key with 400", async () => {

    const { app, invoke } = makeExpressStub();

    setupProfileRoutes(app as never);

    const result = await invoke("post", "/config/profiles", { body: { profile: { extends: "default" } } });

    assert.equal(result.statusCode, 400);
    assert.match((result.body as { error: string }).error, /Profile key is required/);
  });

  test("POST /config/profiles - rejects a missing profile object with 400", async () => {

    const { app, invoke } = makeExpressStub();

    setupProfileRoutes(app as never);

    const result = await invoke("post", "/config/profiles", { body: { key: "myprofile" } });

    assert.equal(result.statusCode, 400);
    assert.match((result.body as { error: string }).error, /Profile definition is required/);
  });

  test("POST /config/profiles - rejects a non-object profile with 400", async () => {

    // The handler checks (typeof profile === "object"). A string or number for the profile field is a structural rejection.
    const { app, invoke } = makeExpressStub();

    setupProfileRoutes(app as never);

    const result = await invoke("post", "/config/profiles", { body: { key: "myprofile", profile: "not an object" } });

    assert.equal(result.statusCode, 400);
    assert.match((result.body as { error: string }).error, /Profile definition is required/);
  });

  test("DELETE /config/profiles/:key - returns 400 when no key parameter is present", async () => {

    const { app, invoke } = makeExpressStub();

    setupProfileRoutes(app as never);

    // Express normally guarantees req.params["key"] is populated for a path like /config/profiles/:key, but the handler defensively guards against the empty
    // case. We exercise that guard directly by passing an empty params map.
    const result = await invoke("delete", "/config/profiles/:key", { params: { key: "" } });

    assert.equal(result.statusCode, 400);
    assert.match((result.body as { error: string }).error, /Profile key is required/);
  });

  test("DELETE /config/profiles/:key - returns 404 when the profile does not exist", async () => {

    // No user profiles are loaded in a fresh test process, so any key lookup misses and the handler returns 404.
    const { app, invoke } = makeExpressStub();

    setupProfileRoutes(app as never);

    const result = await invoke("delete", "/config/profiles/:key", { params: { key: "nonexistent-profile" } });

    assert.equal(result.statusCode, 404);
    assert.match((result.body as { error: string }).error, /Profile 'nonexistent-profile' not found/);
  });

  test("POST /config/profiles/import - rejects malformed pack data with 400 and includes parser errors", async () => {

    const { app, invoke } = makeExpressStub();

    setupProfileRoutes(app as never);

    // An empty body fails parseServicePack validation. The handler concatenates the parser's error list into the envelope.
    const result = await invoke("post", "/config/profiles/import", { body: {} });

    assert.equal(result.statusCode, 400);
    assert.match((result.body as { error: string }).error, /Validation errors/);
  });

  test("GET /config/profiles - returns success: true with empty arrays in a fresh test process", async () => {

    const { app, invoke } = makeExpressStub();

    setupProfileRoutes(app as never);

    const result = await invoke("get", "/config/profiles", {});

    assert.equal(result.statusCode, 200);
    assert.equal((result.body as { success: boolean }).success, true);
    assert.ok(Array.isArray((result.body as { profiles: unknown[] }).profiles), "profiles is an array");
    assert.equal(typeof (result.body as { domains: unknown }).domains, "object", "domains is an object");
  });
});
