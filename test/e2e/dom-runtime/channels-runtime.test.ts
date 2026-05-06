/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channels-runtime.test.ts: DOM-runtime coverage for the Channels subtab client-side script (src/routes/root/scripts/channels.ts). The unit suite next to the
 * generator pins the SHAPE of the emitted string ("the script defines window.openTagManager"); this suite pins the RUNTIME BEHAVIOR of that emitted string when
 * a synthetic browser parses and executes it ("when window.createTag runs against a real DOM, it POSTs the right body and updates the modal").
 *
 * The bug class this tier catches is the one most likely to bite the operator-facing UI: a typo in a fetch URL, a stale closure capturing the wrong service slug,
 * a wrong action label in a browse entry, a missed branch when toggling a select-all checkbox. These regressions ship past the unit suite (the string shape is
 * still right) and past the rendering suite (the static HTML is still right) but blow up the moment a user clicks the affected control. channels.ts is the
 * highest-leverage script in the page - it owns the profile wizard, the Browse Channels modal, the Tag Manager, and the Setup Wizard - so this suite is where the
 * Fox/FoxOne-class bugs (variant resolution, service-filter interaction, identity-vs-binding edits) would surface first.
 *
 * The harness loads the full landing page through the production bootApp listener, then selectively executes both shared.ts (because channels.ts depends on its
 * window.* utilities like createWizardController, channelTable, processServiceDisplays, initSubtab, showToast) and channels.ts itself. status.ts is NOT executed
 * because happy-dom does not implement EventSource; config.ts is also skipped to keep the namespace under test focused on channels.ts.
 *
 * Pattern guidance for adding tests:
 *
 *   - Pin invariants, not historical incidents. "saveProfile sends the canonical body shape" is the contract; "the d2ee7be variant-dropdown bug doesn't recur" is
 *     a symptom to derive coverage from but not the test name.
 *   - Use evaluate(...) for one-shot expressions and DOM seeding; for complex setup, set ctx.document.body.innerHTML or insertAdjacentHTML in a single block.
 *   - For fetch-shape verification (POST bodies, URL paths, methods), override window.fetch with a spy before triggering the operation. Asserting on persisted
 *     state via the bootApp listener is also acceptable but couples the test to the server response shape - the spy is preferred when only the call shape matters.
 *   - When a runtime invariant reveals a real bug, pin current (buggy) behavior with a FIX-PENDING comment showing exactly which assertion to flip post-fix.
 *     Do NOT fix the production script in this suite - fixes are a separate authorized arc.
 *
 * Auto-open guard: the channels.ts IIFE auto-opens the Setup Wizard when the setup-modal carries data-setup-completed='false', which is the default for a fresh
 * data directory. setupChannelsRuntime() flips the attribute to 'true' BEFORE running the scripts so the wizard does not show during normal tests. The dedicated
 * "auto-opens setup wizard" test keeps the attribute at its default to pin the auto-open path explicitly.
 */
import type { DisposableDomTestContext, DomTestContextOptions } from "../../helpers/dom.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { createDomTestContext } from "../../helpers/dom.helpers.ts";

/**
 * Shared bootstrap for the suite. Boots a DOM context, flips the setup-modal's data-setup-completed attribute to 'true' so the channels.ts IIFE does not auto-open
 * the Setup Wizard during normal tests, then runs both shared.ts (channels.ts depends on its window.* utilities) and channels.ts. Tests that need the auto-open
 * behavior should call createDomTestContext directly and skip this helper. Returns the context with both scripts loaded and the channels namespace populated.
 */
async function setupChannelsRuntime(options?: DomTestContextOptions & { readonly leaveSetupIncomplete?: boolean }): Promise<DisposableDomTestContext> {

  const ctx = await createDomTestContext(options);

  if(!options?.leaveSetupIncomplete) {

    // The IIFE-tail block reads getAttribute('data-setup-completed') === 'false'. Flipping it to 'true' here suppresses the auto-open without touching production.
    ctx.evaluate("const m = document.getElementById('setup-modal'); if(m) m.setAttribute('data-setup-completed', 'true');");
  }

  /* Three scripts are loaded together:
   *   1. shared.ts (marker: "window.channelTable = {") - the namespace and wizard controller channels.ts depends on.
   *   2. The profile-wizard data block (marker: "window.__wizardStrategies") - one-line <script> emitted by generateProfileWizardModal that planted the data
   *      registries (window.__wizardProfiles / window.__wizardStrategies / window.__wizardFlags). channels.ts's editUserProfile / saveProfile / startProfileTest
   *      handlers all read these registries; without them the handlers throw on .find().
   *   3. channels.ts (marker: "window.openTagManager") - the script under test.
   *
   * The runScripts harness executes selected scripts in their source order regardless of predicate iteration, so shared.ts → wizard-data → channels.ts is the
   * established document order.
   */
  const ran = ctx.runScripts((s) => s.content.includes("window.channelTable = {") ||
    s.content.includes("window.__wizardStrategies") || s.content.includes("window.openTagManager"));

  if(ran.length !== 3) {

    throw new Error("setupChannelsRuntime: expected exactly three scripts (shared.ts + wizard-data + channels.ts); got " + String(ran.length));
  }

  return ctx;
}

/**
 * Installs a fetch spy on the Window. Captured calls are JSON-introspectable via ctx.evaluateJson("window.harnessFetchCalls"). The default response is the
 * supplied responseBody serialized as JSON. Use the spy whenever a test cares only about the call shape (URL, method, body) and not about the server response.
 */
function installFetchSpy(ctx: DisposableDomTestContext, responseBody: Record<string, unknown> = { success: true }): void {

  ctx.evaluate(
    "window.harnessFetchCalls = []; " +
    "window.fetch = (url, opts) => { " +
    "  window.harnessFetchCalls.push({ url, method: (opts && opts.method) || 'GET', body: opts && opts.body, " +
    "    contentType: opts && opts.headers && opts.headers['Content-Type'] }); " +
    "  return Promise.resolve({ ok: true, json: () => Promise.resolve(" + JSON.stringify(responseBody) + ") }); " +
    "};"
  );
}

/**
 * Captured fetch call shape used by tests reading window.harnessFetchCalls back through evaluateJson.
 */
interface CapturedFetchCall {

  body?: string;
  contentType?: string;
  method: string;
  url: string;
}

/**
 * Reads style.display from a synthetic-DOM element by id. happy-dom's Element type does not surface .style on the Node side (only HTMLElement does), so direct
 * property access via ctx.document.getElementById(id)?.style.display fails TypeScript on Element. Evaluating the expression inside the sandbox sidesteps the
 * cross-realm type lift - happy-dom's getElementById returns its own HTMLElement, on which .style.display is a string. Returns the literal value (e.g., "none",
 * "flex") or empty string when the element is missing or carries no inline display value.
 */
function getDisplay(ctx: DisposableDomTestContext, id: string): string {

  return ctx.evaluate("(document.getElementById('" + id + "') && document.getElementById('" + id + "').style.display) || ''") as string;
}

/**
 * Seeds the profile wizard via editUserProfile with a synthetic /config/profiles response. The wizard's internal controller (profileWizard) is closure-scoped in
 * the channels.ts IIFE and cannot be referenced from outside; this helper is the supported way to plant a known state shape so subsequent saveProfile /
 * startProfileTest tests have something to operate on. The wizard ends up at step 1 in edit mode with the supplied profile loaded.
 *
 * @param ctx - The DOM test context.
 * @param spec - Profile shape to inject. Mirrors the GET /config/profiles entry shape: { key, profile, domains }.
 */
async function seedProfileWizardEdit(ctx: DisposableDomTestContext, spec: {
  domains?: { domain: string; service?: string; serviceTag?: string }[];
  key: string;
  profile: Record<string, unknown>;
}): Promise<void> {

  const payload = JSON.stringify({

    profiles: [{

      domains: spec.domains ?? [],
      key: spec.key,
      profile: spec.profile
    }],
    success: true
  });

  ctx.evaluate("window.fetch = () => Promise.resolve({ ok: true, json: () => Promise.resolve(" + payload + ") });");
  ctx.evaluate("window.editUserProfile('" + spec.key + "')");
  await ctx.flushAsync();
}

/**
 * Clicks the controller-bound role=next button inside the supplied modal id. createWizardController binds the button's onclick to ctrl.next() at construction
 * time, so this is the public surface for advancing a wizard step from a test (the controller itself is closure-scoped).
 */
function clickWizardNext(ctx: DisposableDomTestContext, modalId: string): void {

  ctx.evaluate("document.querySelector('#" + modalId + " [data-wizard-role=\"next\"]').click();");
}

/**
 * Opens the Browse Channels wizard and ensures browseWizard.state.slug is set, so subsequent submitBrowseChannels calls have the service context they need.
 * Replaces window.fetch with a stub that returns an empty channel list for /services/* discovery (so the step-2 spinner resolves cleanly without a real
 * upstream), then opens the modal. The first available service is selected: either by openBrowseModal's single-service short-circuit, or by clicking the
 * first .wizard-provider-card in the rendered step-1 grid (which invokes the closure-scoped selectBrowseService).
 *
 * Side effect: assigns window.__harnessClickedSlug to the slug that was clicked (or empty string if openBrowseModal short-circuited). Tests that need to
 * predict the entry's serviceSlug field can read this value via ctx.evaluate("window.__harnessClickedSlug").
 */
async function openBrowseAndSelectFirstService(ctx: DisposableDomTestContext): Promise<void> {

  ctx.evaluate(
    "window.fetch = (url) => Promise.resolve({ ok: true, json: () => Promise.resolve(" +
    "  url.indexOf('/services/') === 0 ? [] : { success: true }" +
    ") });"
  );

  ctx.evaluate("window.openBrowseModal()");

  /* Capture the slug we are about to click in the same evaluate call so the read happens before the click replaces the DOM. If openBrowseModal short-circuited
   * to step 2, no .wizard-provider-card exists and the captured slug stays empty - tests that require a slug must check window.__harnessClickedSlug.
   */
  ctx.evaluate(
    "window.__harnessClickedSlug = '';" +
    "const card = document.querySelector('.wizard-provider-card');" +
    "if(card) { window.__harnessClickedSlug = card.getAttribute('data-slug') || ''; card.click(); }"
  );
  await ctx.flushAsync();
}

describe("channels.ts: subtab initialization", () => {

  test("registers window.switchChannelsSubtab as a function during IIFE init", async () => {

    /* The first thing the channels.ts IIFE does is wire createSubtabSwitcher with the channels-specific config and assign it to window.switchChannelsSubtab so
     * routing code (e.g., status.ts hash handling, config.ts cross-tab actions) can switch to a channels subtab from outside this script.
     */
    await using ctx = await setupChannelsRuntime();

    assert.equal(ctx.evaluate("typeof window.switchChannelsSubtab"), "function", "switchChannelsSubtab should be defined as a function after IIFE runs");
  });

  test("persists subtab selection under the channels-specific localStorage key when switchChannelsSubtab fires", async () => {

    /* The factory's storageKey is the SSOT for the channels subtab's localStorage namespace. We seed a button + panel that the production switcher does not
     * know about (so we don't disturb the page's existing subtab state), call the switcher, and assert localStorage carries the expected key/value pair.
     */
    await using ctx = await setupChannelsRuntime();

    /* The production page already renders channels-subtab-btn buttons and channels-subtab-* panels for each subtab. We pick a subtab known to exist in the
     * server-rendered page (manage) and call switchChannelsSubtab to confirm the persistence wires through.
     */
    ctx.evaluate("window.switchChannelsSubtab('manage', false)");

    assert.equal(ctx.evaluate("localStorage.getItem('prismcast-channels-subtab')"), "manage",
      "switchChannelsSubtab must persist under the channels-specific storage key");
  });
});

describe("channels.ts: window.deleteUserProfile", () => {

  test("aborts the delete request when confirm() returns false", async () => {

    /* The handler gates on confirm() before sending DELETE. happy-dom does not provide window.confirm by default, so we install a stub that returns false and
     * confirm no fetch was issued. This pins the click-to-confirm-then-fetch contract: a cancelled confirmation must NOT touch the server.
     */
    await using ctx = await setupChannelsRuntime();

    installFetchSpy(ctx);
    ctx.evaluate("window.confirm = () => false;");
    ctx.evaluate("window.deleteUserProfile('myProfile')");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 0, "no fetch should fire when confirm returns false");
  });

  test("DELETEs /config/profiles/<key> with URL-encoded key when confirm() returns true", async () => {

    /* On confirm, the handler issues DELETE to /config/profiles/<key> with the key URL-encoded. We use a key with a slash to confirm encoding semantics - %2F
     * must appear in the URL, not a literal forward slash that would re-route the request.
     */
    await using ctx = await setupChannelsRuntime();

    installFetchSpy(ctx, { message: "Deleted.", success: true });
    ctx.evaluate("window.confirm = () => true;");
    ctx.evaluate("window.deleteUserProfile('my/profile')");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 1);

    const call = calls[0]!;

    assert.equal(call.url, "/config/profiles/my%2Fprofile", "key must be URL-encoded so route matching is deterministic");
    assert.equal(call.method, "DELETE");
  });
});

describe("channels.ts: window.editUserProfile", () => {

  test("loads /config/profiles, opens the wizard in edit mode with the profile name pre-populated and locked readonly", async () => {

    /* editUserProfile fetches the profile list, finds the matching entry by key, and pre-populates the wizard for editing. The internal profileWizard controller
     * is closure-scoped (not on window), so we assert through user-visible DOM state: the wizard modal becomes visible, step 1 renders, the profile name input
     * carries the loaded key, and the readonly attribute is set (which is the visible signal of edit mode - the step-1 renderer adds 'readonly' only when
     * editMode is true).
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate(
      "window.fetch = () => Promise.resolve({ ok: true, json: () => Promise.resolve({" +
      "  success: true," +
      "  profiles: [{" +
      "    key: 'myProfile'," +
      "    profile: {" +
      "      extends: 'default'," +
      "      channelSelection: { strategy: 'guideGrid', matchSelector: '.cell' }," +
      "      hideSelector: '.overlay'," +
      "      description: 'My docs'" +
      "    }," +
      "    domains: [{ domain: 'example.test', service: 'My Service', serviceTag: 'myservice' }]" +
      "  }]" +
      "}) });"
    );

    ctx.evaluate("window.editUserProfile('myProfile')");
    await ctx.flushAsync();

    assert.equal(getDisplay(ctx, "wizard-modal"), "flex", "wizard modal should be open after editUserProfile resolves");

    // Step 1 renders the profile name input. In edit mode the step-1 renderer adds 'readonly' so the user cannot change the identity field after creation.
    assert.equal(ctx.evaluate("(document.getElementById('wizard-profile-name') || {}).value"), "myProfile",
      "profile name input should be pre-populated with the loaded key");
    assert.equal(ctx.evaluate("document.getElementById('wizard-profile-name').hasAttribute('readonly')"), true,
      "edit mode renders the profile name input as readonly");
  });

  test("shows an error toast and does not open the wizard when the GET response carries success:false", async () => {

    /* The handler bails when the load fails. We confirm by overriding fetch to return success:false and asserting the wizard stays closed. The toast itself is
     * verified via DOM presence under #toast-container.
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate("window.fetch = () => Promise.resolve({ ok: true, json: () => Promise.resolve({ success: false, error: 'Read failed.' }) });");
    ctx.evaluate("window.editUserProfile('any')");
    await ctx.flushAsync();

    assert.equal(getDisplay(ctx, "wizard-modal"), "none",
      "wizard modal must NOT open when the profile load fails");

    const toast = ctx.document.querySelector("#toast-container .toast");

    assert.ok(toast, "an error toast should appear when the load fails");
  });
});

describe("channels.ts: window.openWizard (create mode)", () => {

  test("opens the wizard at step 1 with empty profile name input and no readonly attribute", async () => {

    /* openWizard is the create-mode entry point. The behavioral signals visible to the test: the wizard modal is shown, the profile name input renders blank,
     * and the readonly attribute is absent (i.e., editMode is false so the user can type a new name). The internal profileWizard.state is closure-scoped so we
     * cannot inspect it directly - the rendered form is the proxy that proves the reset happened.
     */
    await using ctx = await setupChannelsRuntime();

    // First open the wizard in edit mode so we can confirm openWizard wipes the prior state. Use editUserProfile with a stub fetch.
    ctx.evaluate(
      "window.fetch = () => Promise.resolve({ ok: true, json: () => Promise.resolve({" +
      "  success: true," +
      "  profiles: [{ key: 'stale', profile: { extends: 'default' }, domains: [] }]" +
      "}) });"
    );
    ctx.evaluate("window.editUserProfile('stale')");
    await ctx.flushAsync();

    assert.equal(ctx.evaluate("document.getElementById('wizard-profile-name').value"), "stale", "edit-mode pre-condition: input shows 'stale'");
    assert.equal(ctx.evaluate("document.getElementById('wizard-profile-name').hasAttribute('readonly')"), true, "edit-mode pre-condition: readonly is set");

    ctx.evaluate("window.openWizard()");

    assert.equal(getDisplay(ctx, "wizard-modal"), "flex", "wizard should be visible after openWizard");
    assert.equal(ctx.evaluate("document.getElementById('wizard-profile-name').value"), "", "profile name input must be cleared");
    assert.equal(ctx.evaluate("document.getElementById('wizard-profile-name').hasAttribute('readonly')"), false,
      "create mode must remove the readonly attribute so the user can type");
  });
});

describe("channels.ts: window.saveProfile", () => {

  test("POSTs /config/profiles with the canonical body shape (key + profile + domains)", async () => {

    /* The save body is the contract between the wizard form and the POST endpoint. The shape is { key, profile, domains } where:
     *   - key is the profile name
     *   - profile carries extends, channelSelection, override flags, hideSelector, description
     *   - domains is keyed by domain string, with profile/service/serviceTag fields
     * A regression in any of these would either break creation or silently drop user-entered values.
     */
    await using ctx = await setupChannelsRuntime();

    /* Seed the wizard via editUserProfile (the only externally-driveable path that sets state). The fake server response carries the canonical mix of fields:
     * extends, hideSelector, description, and one domain row. After this resolves, the wizard's internal state is populated.
     */
    await seedProfileWizardEdit(ctx, {

      domains: [{ domain: "example.test", service: "Example", serviceTag: "example" }],
      key: "myProfile",
      profile: { description: "docs", extends: "default", hideSelector: ".overlay", lockVolumeProperties: true }
    });

    installFetchSpy(ctx, { message: "Saved.", success: true });

    ctx.evaluate("window.saveProfile(false)");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 1, "saveProfile should issue exactly one POST");

    const call = calls[0]!;

    assert.equal(call.url, "/config/profiles");
    assert.equal(call.method, "POST");
    assert.equal(call.contentType, "application/json");

    interface SaveBody {

      domains: Record<string, { profile: string; service?: string; serviceTag?: string }>;
      key: string;
      profile: { channelSelection?: unknown; description?: string; extends?: string; hideSelector?: string; lockVolumeProperties?: boolean };
    }

    const body = JSON.parse(call.body ?? "{}") as SaveBody;

    assert.equal(body.key, "myProfile");
    assert.deepEqual(body.profile, {

      description: "docs",
      extends: "default",
      hideSelector: ".overlay",
      lockVolumeProperties: true
    }, "profile body must merge extends, hideSelector, description, and override flags");
    assert.deepEqual(body.domains, {

      "example.test": {

        profile: "myProfile",
        service: "Example",
        serviceTag: "example"
      }
    }, "domains must be keyed by domain string, with profile pointing to the saved profile name");
  });

  test("attaches channelSelection only when strategy is not 'none'", async () => {

    /* The 'none' strategy means inherit-from-base; the body must omit channelSelection so the server doesn't store an empty selector block. Loading a profile
     * with no channelSelection block via editUserProfile sets state.strategy to 'none' (the editStrat fallback), which is the path we want to pin.
     */
    await using ctx = await setupChannelsRuntime();

    await seedProfileWizardEdit(ctx, {

      domains: [{ domain: "example.test", service: "Example", serviceTag: "example" }],
      key: "p1",
      profile: { extends: "default" }
    });

    installFetchSpy(ctx, { success: true });
    ctx.evaluate("window.saveProfile(false)");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 1);

    const body = JSON.parse(calls[0]!.body ?? "{}") as { profile: { channelSelection?: unknown } };

    assert.equal(Object.hasOwn(body.profile, "channelSelection"), false,
      "strategy === 'none' must NOT attach channelSelection to the profile body");
  });

  test("surfaces server-side error envelopes via the wizard's error display without closing the wizard", async () => {

    /* The save handler keeps the wizard open on failure so the user can correct and retry. We assert: the error appears in #wizard-error AND the wizard is still
     * displayed (display:flex). The reverse - closing on error - would lose the user's form data.
     */
    await using ctx = await setupChannelsRuntime();

    await seedProfileWizardEdit(ctx, {

      domains: [{ domain: "example.test", service: "Example", serviceTag: "example" }],
      key: "p1",
      profile: { extends: "default" }
    });

    ctx.evaluate("window.fetch = () => Promise.resolve({ ok: true, json: () => Promise.resolve({ success: false, error: 'Conflict.' }) });");
    ctx.evaluate("window.saveProfile(false)");
    await ctx.flushAsync();

    assert.equal(getDisplay(ctx, "wizard-modal"), "flex", "wizard must remain open on save failure");
    assert.equal(ctx.document.getElementById("wizard-error")?.textContent, "Conflict.");
  });
});

describe("channels.ts: profile wizard validation gates (driven via clickWizardNext)", () => {

  /* Validation is what gates the wizard's next() advance. We drive each test by triggering the role=next button click (the controller binds it to ctrl.next at
   * construction) and observing the rendered #wizard-error message. Tests cannot inspect the closure-scoped controller's currentStep directly; they instead
   * assert via the error message text which is uniquely keyed per validation failure mode.
   */

  test("step 1 rejects empty profile name in create mode (error message contains 'Profile name is required')", async () => {

    await using ctx = await setupChannelsRuntime();

    /* Open the wizard fresh in create mode (openWizard resets state to a blank shape with profileName = ''). The Next click triggers validateProfileStep(1)
     * which fails immediately because s.profileName.trim() is falsy.
     */
    ctx.evaluate("window.openWizard()");

    clickWizardNext(ctx, "wizard-modal");
    await ctx.flushAsync();

    assert.match(ctx.document.getElementById("wizard-error")?.textContent ?? "", /Profile name is required/,
      "the validation error string must surface the 'profile name required' message");
  });

  test("step 1 rejects profile names that violate the character pattern (underscore is rejected)", async () => {

    /* The regex: /^[a-zA-Z]([a-zA-Z0-9-]*[a-zA-Z0-9])?$/ rejects underscores. We type 'my_profile' into the wizard-profile-name input via a synthetic input
     * event so the bound 'input' handler in attachProfileHandlers sets state.profileName, then click Next.
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate("window.openWizard()");
    ctx.evaluate(
      "const inp = document.getElementById('wizard-profile-name');" +
      "inp.value = 'my_profile';" +
      "inp.dispatchEvent(new Event('input', { bubbles: true }));"
    );

    clickWizardNext(ctx, "wizard-modal");
    await ctx.flushAsync();

    assert.match(ctx.document.getElementById("wizard-error")?.textContent ?? "", /letters, numbers, and hyphens/,
      "the validation error string must explain the allowed character set");
  });

  test("step 4 rejects when no domain row contains a non-blank domain (after navigating from step 1 to step 4 in edit mode)", async () => {

    /* Step 4 is the domain-mapping step. Validation requires at least one non-blank domain row. To reach step 4 we seed an edit-mode wizard with a baseProfile
     * (so step 1 passes) and the default empty domains array (so step 4 will fail). We click Next 3 times to traverse 1→2→3→4 (each prior step validates clean
     * because there is no required strategy field with strategy='none' and step 3 has no validation), then click Next once more on step 4 to fire the gate.
     */
    await using ctx = await setupChannelsRuntime();

    await seedProfileWizardEdit(ctx, {

      // Empty domains list - editUserProfile fills with a single { domain: '', service: '', serviceTag: '' } fallback row.
      domains: [],
      key: "p1",
      profile: { extends: "default" }
    });

    /* Click next 3 times to land on step 4. Each click must flushAsync to drain the controller's microtask queue (validateAndAdvance is async).
     */
    clickWizardNext(ctx, "wizard-modal");
    await ctx.flushAsync();
    clickWizardNext(ctx, "wizard-modal");
    await ctx.flushAsync();
    clickWizardNext(ctx, "wizard-modal");
    await ctx.flushAsync();

    /* The 4th click on Next is the one that should fail. The error message is the witness that we (a) made it to step 4 and (b) the domain validation fired.
     */
    clickWizardNext(ctx, "wizard-modal");
    await ctx.flushAsync();

    assert.match(ctx.document.getElementById("wizard-error")?.textContent ?? "", /At least one domain is required/,
      "step 4 must reject submission when every domain row is blank");
  });
});

describe("channels.ts: profile test flow handlers", () => {

  test("startProfileTest prefixes 'https://' when the URL has no scheme and POSTs /config/profiles/test", async () => {

    /* The contract: a bare domain like "example.test" becomes "https://example.test"; a fully-qualified URL is shipped as-is. Pinning both ensures the wrong
     * scheme isn't injected when the user pastes an http:// URL.
     */
    await using ctx = await setupChannelsRuntime();

    /* startProfileTest reads profileWizard.state to populate testSelectors; a seeded edit-mode profile gives the handler the closure-state it needs. The empty
     * profile is enough because we only care about the URL transformation, not the selectors.
     */
    await seedProfileWizardEdit(ctx, { key: "p1", profile: { extends: "default" } });

    installFetchSpy(ctx, { success: true });

    ctx.evaluate("window.startProfileTest('example.test')");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 1);

    const call = calls[0]!;

    assert.equal(call.url, "/config/profiles/test");
    assert.equal(call.method, "POST");

    const body = JSON.parse(call.body ?? "{}") as { url: string };

    assert.equal(body.url, "https://example.test", "scheme-less URL must be coerced to https://");
  });

  test("startProfileTest preserves a URL that already carries a scheme", async () => {

    await using ctx = await setupChannelsRuntime();

    await seedProfileWizardEdit(ctx, { key: "p1", profile: { extends: "default" } });

    installFetchSpy(ctx, { success: true });

    ctx.evaluate("window.startProfileTest('http://internal.example/path')");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 1);

    const body = JSON.parse(calls[0]!.body ?? "{}") as { url: string };

    assert.equal(body.url, "http://internal.example/path", "URL with scheme must NOT be re-prefixed");
  });

  test("checkSelectors POSTs the testSelectors map to /config/profiles/test/check", async () => {

    /* checkSelectors uses the closure-scoped testSelectors variable that startProfileTest populates. We seed the profileWizard with a 'tileClick' strategy
     * (a strategy the wizard registry exposes - editUserProfile filters strategy fields against window.__wizardStrategies, so unknown strategies leave
     * strategyFields empty and the selectors never reach testSelectors). After seeding, startProfileTest copies the relevant selectors into the closure-scoped
     * testSelectors; checkSelectors then ships them. We assert the second fetch call carries the full selector map.
     */
    await using ctx = await setupChannelsRuntime();

    await seedProfileWizardEdit(ctx, {

      key: "p1",
      profile: {

        channelSelection: { matchSelector: ".cell", playSelector: ".play", strategy: "tileClick" },
        extends: "default",
        hideSelector: ".overlay"
      }
    });

    installFetchSpy(ctx, { success: true });
    ctx.evaluate("window.startProfileTest('example.test')");
    await ctx.flushAsync();

    ctx.evaluate("window.checkSelectors()");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 2, "startProfileTest then checkSelectors should produce two fetch calls");

    const checkCall = calls[1]!;

    assert.equal(checkCall.url, "/config/profiles/test/check");

    const body = JSON.parse(checkCall.body ?? "{}") as { selectors: Record<string, string> };

    assert.deepEqual(body.selectors, { hideSelector: ".overlay", matchSelector: ".cell", playSelector: ".play" });
  });

  test("endProfileTest POSTs /config/profiles/test/done and hides the test modal", async () => {

    await using ctx = await setupChannelsRuntime();

    ctx.evaluate("document.getElementById('test-modal').style.display = 'flex';");

    installFetchSpy(ctx, { success: true });
    ctx.evaluate("window.endProfileTest()");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 1);

    const call = calls[0]!;

    assert.equal(call.url, "/config/profiles/test/done");
    assert.equal(call.method, "POST");
    assert.equal(getDisplay(ctx, "test-modal"), "none", "test modal must be hidden after endProfileTest");
  });
});

describe("channels.ts: import/export modal handlers", () => {

  test("closeImportModal hides the import-modal element and clears the pending import data", async () => {

    /* The import flow stores the parsed file content in a closure-scope pendingImportData variable that executeImport reads. closeImportModal must clear this
     * (so cancelling the modal doesn't leave a stale payload that a subsequent confirm could re-submit).
     *
     * We can only observe the clearing indirectly: after closeImportModal, calling executeImport must early-return without firing a fetch. That's the
     * observable invariant.
     */
    await using ctx = await setupChannelsRuntime();

    // Show the modal first so we can assert it's hidden after close.
    ctx.evaluate("document.getElementById('import-modal').style.display = 'flex';");

    ctx.evaluate("window.closeImportModal()");

    assert.equal(getDisplay(ctx, "import-modal"), "none");

    // Cross-check by attempting executeImport - with pendingImportData null, it must early-return without firing fetch.
    installFetchSpy(ctx);
    ctx.evaluate("window.executeImport()");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 0, "executeImport must early-return when pendingImportData is null (closeImportModal cleared it)");
  });

  test("closeExportModal hides the export-modal element", async () => {

    /* The production page only renders the export modal when at least one user profile exists (see generateCustomProfilesPanel - the modal is gated on user
     * profile count). For this isolated test we synthesize the element so we can pin the function's behavior independently of profile state. The handler reads
     * the element by id and toggles display, so a synthetic fixture is structurally identical to the production-rendered one.
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate("document.body.insertAdjacentHTML('beforeend', '<div id=\"export-modal\" style=\"display:flex;\"></div>');");

    ctx.evaluate("window.closeExportModal()");

    assert.equal(getDisplay(ctx, "export-modal"), "none");
  });

  test("toggleExportAll cascades the master checkbox state to every .export-profile-cb", async () => {

    /* The master "select all" checkbox toggles every per-profile checkbox in the export list. We seed three checkboxes in mixed states, fire toggleExportAll
     * with a master that is checked=true, and assert all three end checked. Then flip the master and assert all three uncheck.
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<input type=\"checkbox\" class=\"export-profile-cb\" id=\"epc-1\" checked>' + " +
      "'<input type=\"checkbox\" class=\"export-profile-cb\" id=\"epc-2\">' + " +
      "'<input type=\"checkbox\" class=\"export-profile-cb\" id=\"epc-3\" checked>' + " +
      "'<input type=\"checkbox\" id=\"epc-master\">');"
    );

    ctx.evaluate("const m = document.getElementById('epc-master'); m.checked = true; window.toggleExportAll(m);");

    assert.equal((ctx.document.getElementById("epc-1") as unknown as { checked: boolean }).checked, true);
    assert.equal((ctx.document.getElementById("epc-2") as unknown as { checked: boolean }).checked, true);
    assert.equal((ctx.document.getElementById("epc-3") as unknown as { checked: boolean }).checked, true);

    ctx.evaluate("const m2 = document.getElementById('epc-master'); m2.checked = false; window.toggleExportAll(m2);");

    assert.equal((ctx.document.getElementById("epc-1") as unknown as { checked: boolean }).checked, false);
    assert.equal((ctx.document.getElementById("epc-2") as unknown as { checked: boolean }).checked, false);
    assert.equal((ctx.document.getElementById("epc-3") as unknown as { checked: boolean }).checked, false);
  });

  test("executeExport without selected profiles shows an error toast and does not navigate", async () => {

    /* The handler bails when zero profiles are checked. We seed an empty-selection state, capture the assignment to window.location.href via a setter spy, and
     * confirm both: (a) no navigation, (b) an error toast appeared.
     */
    await using ctx = await setupChannelsRuntime();

    /* Set up an empty .export-profile-cb:checked selection (no checked elements at all) and a placeholder include-channels checkbox so the executor's DOM lookups
     * don't throw on the missing element.
     */
    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', '<input type=\"checkbox\" class=\"export-profile-cb\">');" +
      "if(!document.getElementById('export-include-channels')) {" +
      "  document.body.insertAdjacentHTML('beforeend', '<input type=\"checkbox\" id=\"export-include-channels\">');" +
      "}"
    );

    /* Capture location.href assignments. happy-dom's location is a settable property; setting it does not navigate but does change the value. We snapshot before
     * and after to confirm nothing was assigned.
     */
    const before = ctx.evaluate("window.location.href") as string;

    ctx.evaluate("window.executeExport()");
    await ctx.flushAsync();

    assert.equal(ctx.evaluate("window.location.href"), before, "executeExport must not navigate when no profiles are checked");

    const toast = ctx.document.querySelector("#toast-container .toast");

    assert.ok(toast, "an error toast should appear");
    assert.match(toast.textContent, /Select at least one profile/);
  });
});

describe("channels.ts: openBrowseModal", () => {

  test("opens the browse-modal and renders content (either a service grid for multi-service or the discovery spinner for single-service)", async () => {

    /* openBrowseModal reads embedded service data from the #browse-services-data script tag, applies the provider-chip filter, and opens the wizard. We
     * confirm the modal is visible and that browse-content is non-empty. We do not assert specific service tags or grid count because the embedded data depends
     * on the production provider registry which evolves over time.
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate("window.openBrowseModal()");

    assert.equal(getDisplay(ctx, "browse-modal"), "flex");
    assert.notEqual(ctx.evaluate("(document.getElementById('browse-content').innerHTML || '').length"), 0,
      "browse-content must be populated by the open call (either picker grid or step-2 spinner)");
  });
});

describe("channels.ts: submitBrowseChannels", () => {

  test("collects 'add' entries from new+checked checkboxes and POSTs to /config/channels/modify with the right serviceSlug", async () => {

    /* The browse modal collects categorized actions (add/enable/switch/remove) into a single POST. This test pins the 'add' action: a new channel (data-original=
     * 'new') that's checked must surface as { action: 'add', name, channelSelector, ... } in the request body, with serviceSlug matching the wizard's selected
     * service. We use the openBrowseAndSelectFirstService helper to set the closure-scoped browseWizard.state.slug, then synthesize a checkbox fixture and submit.
     */
    await using ctx = await setupChannelsRuntime();

    await openBrowseAndSelectFirstService(ctx);

    /* Replace browse-content with our controlled checkbox fixture. The submitBrowseChannels handler reads .browse-channel-cb selectors document-wide, so the
     * scope of insertion does not matter; we mount inside browse-content for visual hygiene. The button id browse-add-btn is the action target the handler
     * disables on submit.
     */
    ctx.evaluate(
      "document.getElementById('browse-content').innerHTML = " +
      "'<div class=\"browse-channel-item\">' + " +
      "'<input type=\"checkbox\" class=\"browse-channel-cb\" data-name=\"NewChan\" data-selector=\"NEW\" data-original=\"new\" checked>' + " +
      "'</div>';"
    );

    installFetchSpy(ctx, { message: "Added.", success: true });

    ctx.evaluate("window.submitBrowseChannels()");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 1, "submitBrowseChannels should POST once when there is a checked new channel");

    const call = calls[0]!;

    assert.equal(call.url, "/config/channels/modify");
    assert.equal(call.method, "POST");

    const body = JSON.parse(call.body ?? "{}") as { channels: { action: string; canonicalKey?: string; channelSelector: string; name: string; serviceSlug: string }[] };

    assert.equal(body.channels.length, 1);

    const entry = body.channels[0]!;
    const expectedSlug = ctx.evaluate("window.__harnessClickedSlug") as string;

    assert.equal(entry.action, "add");
    assert.equal(entry.name, "NewChan");
    assert.equal(entry.channelSelector, "NEW");
    assert.ok(entry.serviceSlug, "serviceSlug must be present");

    if(expectedSlug) {

      // Multi-service path: the slug we clicked must show up verbatim. Single-service short-circuit leaves expectedSlug empty and the assertion is skipped.
      assert.equal(entry.serviceSlug, expectedSlug);
    }
  });

  test("classifies switch action only when checked AND not indeterminate (a checked-but-indeterminate switch must NOT submit)", async () => {

    /* The browse modal's three-state semantics: a 'switch' channel becomes a 'switch' action only when the user explicitly checks it (not just leaves it
     * indeterminate). This pins the gating: a checkbox with data-original='switch' that is left as indeterminate must NOT produce a switch action.
     */
    await using ctx = await setupChannelsRuntime();

    await openBrowseAndSelectFirstService(ctx);

    ctx.evaluate(
      "document.getElementById('browse-content').innerHTML = " +
      "'<div class=\"browse-channel-item\">' + " +
      "'<input type=\"checkbox\" class=\"browse-channel-cb\" id=\"sw-cb\" data-name=\"S\" data-selector=\"S\" data-original=\"switch\" data-canonical=\"S\">' + " +
      "'</div>';" +
      "const cb = document.getElementById('sw-cb'); cb.checked = true; cb.indeterminate = true;"
    );

    installFetchSpy(ctx, { success: true });
    ctx.evaluate("window.submitBrowseChannels()");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 0, "checked + indeterminate switch checkbox must NOT trigger a fetch");
  });

  test("classifies remove action when a current-state checkbox is unchecked", async () => {

    /* A currently-active channel becomes a 'remove' action when the user unchecks it. The handler reads data-original='current' and the cb.checked=false
     * combination.
     */
    await using ctx = await setupChannelsRuntime();

    await openBrowseAndSelectFirstService(ctx);

    ctx.evaluate(
      "document.getElementById('browse-content').innerHTML = " +
      "'<div class=\"browse-channel-item\">' + " +
      "'<input type=\"checkbox\" class=\"browse-channel-cb\" data-name=\"R\" data-selector=\"R\" data-original=\"current\" data-canonical=\"R\">' + " +
      "'</div>';"
    );

    installFetchSpy(ctx, { success: true });
    ctx.evaluate("window.submitBrowseChannels()");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 1);

    const body = JSON.parse(calls[0]!.body ?? "{}") as { channels: { action: string; canonicalKey: string }[] };

    assert.equal(body.channels.length, 1);

    const entry = body.channels[0]!;

    assert.equal(entry.action, "remove");
    assert.equal(entry.canonicalKey, "R");
  });

  test("skips checkboxes whose row is hidden by the search filter", async () => {

    /* The search filter sets row.style.display='none' on non-matching rows. submitBrowseChannels must skip hidden rows so a typed-then-cleared search doesn't
     * accidentally include channels the user can't see at submit time.
     */
    await using ctx = await setupChannelsRuntime();

    await openBrowseAndSelectFirstService(ctx);

    ctx.evaluate(
      "document.getElementById('browse-content').innerHTML = " +
      "'<div class=\"browse-channel-item\" style=\"display:none\">' + " +
      "'<input type=\"checkbox\" class=\"browse-channel-cb\" data-name=\"H\" data-selector=\"H\" data-original=\"new\" checked>' + " +
      "'</div>';"
    );

    installFetchSpy(ctx, { success: true });
    ctx.evaluate("window.submitBrowseChannels()");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 0, "hidden rows must be excluded from the modify entries");
  });
});

describe("channels.ts: tag manager modal lifecycle", () => {

  test("openTagManager shows the tag-manager-modal element", async () => {

    await using ctx = await setupChannelsRuntime();

    /* The server-rendered tag-manager-modal has style.display='none' initially. openTagManager flips it to 'flex'. Pin both endpoints so a regression in either
     * direction surfaces here.
     */
    assert.equal(getDisplay(ctx, "tag-manager-modal"), "none", "modal starts hidden");

    ctx.evaluate("window.openTagManager()");

    assert.equal(getDisplay(ctx, "tag-manager-modal"), "flex");
  });

  test("closeTagManager hides the tag-manager-modal element", async () => {

    await using ctx = await setupChannelsRuntime();

    ctx.evaluate("document.getElementById('tag-manager-modal').style.display = 'flex';");

    ctx.evaluate("window.closeTagManager()");

    assert.equal(getDisplay(ctx, "tag-manager-modal"), "none");
  });
});

describe("channels.ts: window.createTag", () => {

  test("early-returns without firing fetch when the input is empty", async () => {

    /* Defensive guard: empty/whitespace tag values must not hit the server. We seed an empty input and confirm the spy captured no calls.
     */
    await using ctx = await setupChannelsRuntime();

    /* The tag-manager-input lives inside the tag-manager-modal which is hidden. createTag reads document.getElementById('tag-manager-input'). The element is
     * present (it's inside the rendered modal) and starts with empty value.
     */
    installFetchSpy(ctx);
    ctx.evaluate("window.createTag()");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 0, "createTag must not fetch with an empty input value");
  });

  test("POSTs /config/tags with { tag: <name> } when the input is non-empty", async () => {

    await using ctx = await setupChannelsRuntime();

    ctx.evaluate("document.getElementById('tag-manager-input').value = 'sports';");

    installFetchSpy(ctx, { active: [], filterContent: "", modalBody: "", registry: {}, success: true });
    ctx.evaluate("window.createTag()");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 1);

    const call = calls[0]!;

    assert.equal(call.url, "/config/tags");
    assert.equal(call.method, "POST");
    assert.equal(call.contentType, "application/json");

    const body = JSON.parse(call.body ?? "{}") as { tag: string };

    assert.equal(body.tag, "sports");
  });

  test("surfaces server validation errors via the inline error div without a toast", async () => {

    /* The error display path: data.success === false → the error message is written to #tag-manager-error and shown. No toast is emitted in this path (per the
     * implementation; toasts are reserved for non-tag-manager surfaces).
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate("document.getElementById('tag-manager-input').value = 'badtag';");

    ctx.evaluate("window.fetch = () => Promise.resolve({ ok: true, json: () => Promise.resolve({ success: false, error: 'Already exists.' }) });");

    ctx.evaluate("window.createTag()");
    await ctx.flushAsync();

    const errorDiv = ctx.document.getElementById("tag-manager-error");

    assert.equal(errorDiv?.textContent, "Already exists.");
    assert.notEqual(getDisplay(ctx, "tag-manager-error"), "none", "error div must be visible after a server validation error");
  });
});

describe("channels.ts: window.deleteTag and window.restoreTag", () => {

  test("deleteTag DELETEs /config/tags/<encoded-tag>", async () => {

    await using ctx = await setupChannelsRuntime();

    installFetchSpy(ctx, { active: [], filterContent: "", modalBody: "", registry: {}, success: true });

    ctx.evaluate("window.deleteTag('news/sports')");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 1);

    const call = calls[0]!;

    assert.equal(call.url, "/config/tags/news%2Fsports");
    assert.equal(call.method, "DELETE");
  });

  test("restoreTag POSTs /config/tags/restore with the tag in the body", async () => {

    await using ctx = await setupChannelsRuntime();

    installFetchSpy(ctx, { active: [], filterContent: "", modalBody: "", registry: {}, success: true });

    ctx.evaluate("window.restoreTag('news')");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 1);

    const call = calls[0]!;

    assert.equal(call.url, "/config/tags/restore");
    assert.equal(call.method, "POST");

    const body = JSON.parse(call.body ?? "{}") as { tag: string };

    assert.equal(body.tag, "news");
  });
});

describe("channels.ts: window.startTagRename", () => {

  test("replaces the supplied span with an input pre-populated with the old tag value", async () => {

    /* startTagRename swaps the original tag pill for an inline editable input. The input must carry the existing tag as its initial value so an unintentional
     * Enter immediately after open does not blank the tag.
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', '<span id=\"trn-host\"><span id=\"trn-pill\">news</span></span>');"
    );

    ctx.evaluate("window.startTagRename(document.getElementById('trn-pill'), 'news')");

    const host = ctx.document.getElementById("trn-host");
    const input = host?.querySelector("input.tag-rename-input") as unknown as { value: string } | null;

    assert.ok(input, "an input.tag-rename-input must replace the pill");
    assert.equal(input.value, "news", "input must be pre-populated with the old tag value");
    assert.equal(ctx.document.getElementById("trn-pill"), null, "original pill should be removed from DOM");
  });

  test("Escape cancels the rename and restores the original span without firing fetch", async () => {

    /* Escape must restore the original pill and leave the server untouched. We dispatch a synthetic keydown with key='Escape' on the input and confirm both
     * conditions.
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', '<span id=\"trn-host2\"><span id=\"trn-pill2\">old</span></span>');"
    );

    installFetchSpy(ctx);
    ctx.evaluate("window.startTagRename(document.getElementById('trn-pill2'), 'old')");

    /* Construct a synthetic KeyboardEvent. happy-dom's KeyboardEvent constructor accepts the standard init dict; the inline onkeydown handler reads e.key, so
     * the event's key property must be 'Escape'.
     */
    ctx.evaluate(
      "const inp = document.querySelector('#trn-host2 input.tag-rename-input');" +
      "const ev = new KeyboardEvent('keydown', { key: 'Escape' });" +
      "inp.dispatchEvent(ev);"
    );

    /* The onkeydown reads e.key without checking event-type lifecycle: production uses inp.onkeydown = (e) => { ... }. dispatchEvent('keydown') on the input
     * does not fire the .onkeydown property by default in happy-dom (it fires addEventListener-registered handlers). Use the property assignment directly.
     */
    ctx.evaluate(
      "const inp2 = document.querySelector('#trn-host2 input.tag-rename-input');" +
      "if(inp2 && inp2.onkeydown) { inp2.onkeydown({ key: 'Escape', preventDefault: () => {} }); }"
    );

    await ctx.flushAsync();

    const host = ctx.document.getElementById("trn-host2");

    assert.ok(host?.querySelector("#trn-pill2"), "Escape must restore the original pill");

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 0, "Escape must NOT trigger a rename fetch");
  });

  test("Enter submits a rename POST to /config/tags/rename with both newTag and oldTag", async () => {

    await using ctx = await setupChannelsRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', '<span id=\"trn-host3\"><span id=\"trn-pill3\">old</span></span>');"
    );

    installFetchSpy(ctx, { active: [], filterContent: "", modalBody: "", registry: {}, success: true });
    ctx.evaluate("window.startTagRename(document.getElementById('trn-pill3'), 'old')");

    /* Set the new value, then invoke onkeydown with Enter. The submit() function inside startTagRename does the POST.
     */
    ctx.evaluate(
      "const inp3 = document.querySelector('#trn-host3 input.tag-rename-input');" +
      "inp3.value = 'fresh';" +
      "if(inp3 && inp3.onkeydown) { inp3.onkeydown({ key: 'Enter', preventDefault: () => {} }); }"
    );

    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 1);

    const call = calls[0]!;

    assert.equal(call.url, "/config/tags/rename");
    assert.equal(call.method, "POST");

    const body = JSON.parse(call.body ?? "{}") as { newTag: string; oldTag: string };

    assert.deepEqual(body, { newTag: "fresh", oldTag: "old" });
  });

  test("submitting the same tag value as the original is a no-op (no fetch, restore the original span)", async () => {

    /* The submit() helper inside startTagRename has an early-return when newTag === oldTag (or empty trim). This avoids a pointless server roundtrip when the
     * user opens the editor and immediately commits without changing anything.
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', '<span id=\"trn-host4\"><span id=\"trn-pill4\">same</span></span>');"
    );

    installFetchSpy(ctx);
    ctx.evaluate("window.startTagRename(document.getElementById('trn-pill4'), 'same')");

    ctx.evaluate(
      "const inp4 = document.querySelector('#trn-host4 input.tag-rename-input');" +
      "if(inp4 && inp4.onkeydown) { inp4.onkeydown({ key: 'Enter', preventDefault: () => {} }); }"
    );

    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 0, "no-change Enter must not fire a fetch");
    assert.ok(ctx.document.getElementById("trn-pill4"), "original pill must be restored");
  });
});

describe("channels.ts: window.applyTagColumnFilter", () => {

  test("when all checkboxes are checked, every display row clears its tag-filtered class", async () => {

    /* The "all checked" state means "no client-side filter applied". Every existing row, regardless of its tags, must have tag-filtered removed. We seed two
     * rows in a body-level table (avoiding the happy-dom tbody.insertAdjacentHTML parser-context drop), check both filter checkboxes, run the filter, and assert
     * neither row is filtered.
     */
    await using ctx = await setupChannelsRuntime();

    /* Replace any production tag-filter-checkboxes the page may already render so we control the input set. Use a unique scope id so other tests don't bleed.
     * The handler queries by class without a scope, so we strip the existing checkboxes first.
     */
    ctx.evaluate("document.querySelectorAll('.tag-filter-checkbox').forEach((el) => el.remove());");
    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"a\" checked>' + " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"b\" checked>' + " +
      "'<table><tbody>' + " +
      "'<tr id=\"display-row-tcr1\" data-channel-tags=\"a\" class=\"tag-filtered\"><td></td></tr>' + " +
      "'<tr id=\"display-row-tcr2\" data-channel-tags=\"b\" class=\"tag-filtered\"><td></td></tr>' + " +
      "'</tbody></table>');"
    );

    ctx.evaluate("window.applyTagColumnFilter()");

    assert.equal(ctx.document.getElementById("display-row-tcr1")?.classList.contains("tag-filtered"), false);
    assert.equal(ctx.document.getElementById("display-row-tcr2")?.classList.contains("tag-filtered"), false);
  });

  test("when only some are checked, rows lacking any of the checked tags get tag-filtered; rows with at least one checked tag do not", async () => {

    /* The filter intersection: a row is visible iff at least one of its data-channel-tags appears in the checked set. We seed three rows with different tag sets
     * and check 'a' only.
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate("document.querySelectorAll('.tag-filter-checkbox').forEach((el) => el.remove());");
    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"a\" checked>' + " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"b\">' + " +
      "'<table><tbody>' + " +
      "'<tr id=\"display-row-mix1\" data-channel-tags=\"a\"><td></td></tr>' + " +
      "'<tr id=\"display-row-mix2\" data-channel-tags=\"b\"><td></td></tr>' + " +
      "'<tr id=\"display-row-mix3\" data-channel-tags=\"a,b\"><td></td></tr>' + " +
      "'</tbody></table>');"
    );

    ctx.evaluate("window.applyTagColumnFilter()");

    assert.equal(ctx.document.getElementById("display-row-mix1")?.classList.contains("tag-filtered"), false, "row with 'a' is visible");
    assert.equal(ctx.document.getElementById("display-row-mix2")?.classList.contains("tag-filtered"), true, "row with only 'b' (unchecked) is filtered out");
    assert.equal(ctx.document.getElementById("display-row-mix3")?.classList.contains("tag-filtered"), false, "row with 'a,b' is visible because 'a' is checked");
  });

  test("toggleTagColumnFilter inverts the all-checked state to all-unchecked and vice versa", async () => {

    /* toggleTagColumnFilter is the "Show All / Show None" toggle. From all-checked it goes to all-unchecked; from any-unchecked it goes to all-checked.
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate("document.querySelectorAll('.tag-filter-checkbox').forEach((el) => el.remove());");
    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"a\" checked id=\"tg-cb-a\">' + " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"b\" checked id=\"tg-cb-b\">');"
    );

    // From all-checked, toggle should uncheck both.
    ctx.evaluate("window.toggleTagColumnFilter()");

    assert.equal((ctx.document.getElementById("tg-cb-a") as unknown as { checked: boolean }).checked, false);
    assert.equal((ctx.document.getElementById("tg-cb-b") as unknown as { checked: boolean }).checked, false);

    // From any-unchecked, toggle should check all.
    ctx.evaluate("window.toggleTagColumnFilter()");

    assert.equal((ctx.document.getElementById("tg-cb-a") as unknown as { checked: boolean }).checked, true);
    assert.equal((ctx.document.getElementById("tg-cb-b") as unknown as { checked: boolean }).checked, true);
  });
});

describe("channels.ts: playlist hint dropdown", () => {

  test("showPlaylistHint constructs the playlist URL using the include form when fewer tags are checked than unchecked", async () => {

    /* The handler picks the shorter URL form: include (?tag=a,b) vs exclude (?tag=-c,-d). When the checked set is smaller, include wins. We seed a state with
     * 2 checked and 5 unchecked tags, open the popover, and verify the URL inside the dropdown content uses the include form.
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate("document.querySelectorAll('.tag-filter-checkbox').forEach((el) => el.remove());");
    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"x\" checked>' + " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"y\" checked>' + " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"a\">' + " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"b\">' + " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"c\">' + " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"d\">' + " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"e\">' + " +
      "'<button id=\"plh-btn\">Hint</button>' + " +
      "'<div class=\"dropdown-menu\" id=\"plh-menu\"></div>');"
    );

    /* showPlaylistHint delegates to toggleDropdown(btn, { onOpen }) which adds .show to the menu and runs onOpen with the menu element. We need to wire btn.menu
     * by passing { menu: ... } - but production calls toggleDropdown(btn, { onOpen }) without a menu option, so toggleDropdown finds the menu via DOM convention.
     * For this test, the most reliable surface is to call showPlaylistHint and then read the popover content from any rendered .playlist-hint-content node.
     */
    ctx.evaluate("window.showPlaylistHint(document.getElementById('plh-btn'))");

    const content = ctx.document.querySelector(".playlist-hint-content");

    assert.ok(content, "playlist-hint-content must be rendered into the dropdown menu");

    const codeEl = content.querySelector("code");

    assert.ok(codeEl, "URL <code> element must be present");
    assert.match(codeEl.textContent, /\/playlist\?tag=x,y/, "URL must use the include form (shorter side)");
  });

  test("showPlaylistHint constructs the exclude form when fewer tags are unchecked than checked", async () => {

    /* The reverse of the above: 4 checked, 1 unchecked → exclude form (?tag=-c).
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate("document.querySelectorAll('.tag-filter-checkbox').forEach((el) => el.remove());");
    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"a\" checked>' + " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"b\" checked>' + " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"c\">' + " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"d\" checked>' + " +
      "'<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"e\" checked>' + " +
      "'<button id=\"plh-btn2\">Hint</button>' + " +
      "'<div class=\"dropdown-menu\" id=\"plh-menu2\"></div>');"
    );

    ctx.evaluate("window.showPlaylistHint(document.getElementById('plh-btn2'))");

    const codeEl = ctx.document.querySelector(".playlist-hint-content code");

    assert.match(codeEl?.textContent ?? "", /\/playlist\?tag=-c/, "URL must use the exclude form when fewer tags are unchecked");
  });

  test("copyPlaylistHintUrl reads the URL from the popover and delegates to copyToClipboard", async () => {

    /* copyPlaylistHintUrl is a thin wrapper. We seed the popover content directly, override navigator.clipboard.writeText to capture the call, and confirm the
     * URL string is passed through.
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<div class=\"playlist-hint-content\">' + " +
      "'<code>http://example.test/playlist?tag=a,b</code>' + " +
      "'</div>');" +
      "window.harnessClipboardWrite = null;" +
      "Object.defineProperty(navigator, 'clipboard', {" +
      "  configurable: true, value: { writeText: (text) => { window.harnessClipboardWrite = text; return Promise.resolve(); } }" +
      "});"
    );

    ctx.evaluate("window.copyPlaylistHintUrl()");
    await ctx.flushAsync();

    assert.equal(ctx.evaluate("window.harnessClipboardWrite"), "http://example.test/playlist?tag=a,b");
  });
});

describe("channels.ts: setup wizard handlers", () => {

  test("openSetupWizard opens the setup-modal and renders the step-1 service grid", async () => {

    /* openSetupWizard initializes the wizard state and renders step 1 (service selection grid). We assert via DOM: the modal becomes visible, and the rendered
     * content includes the wizard-provider-grid container that step 1 produces. Internal state (setupWizard.state.selectedServices, authIndex) is closure-scoped
     * and not directly inspectable; the rendered grid is the proxy that proves step 1 fired.
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate("window.openSetupWizard()");

    assert.equal(getDisplay(ctx, "setup-modal"), "flex", "setup-modal must be visible after openSetupWizard");
    assert.notEqual(ctx.evaluate("(document.querySelector('#setup-content .wizard-provider-grid') !== null)"), false,
      "step 1's wizard-provider-grid must render after openSetupWizard");
  });

  test("skipSetup POSTs /config/channels/setup-completed and closes the wizard", async () => {

    await using ctx = await setupChannelsRuntime();

    /* Open the wizard via the public entry point so the closure-scoped controller is in the open state when we call skipSetup. Without the open call, the
     * close from skipSetup would still execute (it's idempotent) but we want to assert the open→closed transition.
     */
    ctx.evaluate("window.openSetupWizard()");

    installFetchSpy(ctx, { success: true });
    ctx.evaluate("window.skipSetup()");
    await ctx.flushAsync();

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as CapturedFetchCall[];

    assert.equal(calls.length, 1);

    const call = calls[0]!;

    assert.equal(call.url, "/config/channels/setup-completed");
    assert.equal(call.method, "POST");
    assert.equal(getDisplay(ctx, "setup-modal"), "none", "setup-modal must close after skip");
  });

  test("finishSetup POSTs /config/channels/setup-completed, closes the wizard, and applies the returned patch via channelTable", async () => {

    /* The finish path: POST setup-completed, receive a counts-only patch, apply it via channelTable.applyPatch, then close the wizard. The total-count element
     * already exists on the page (it's part of the channel-table summary). We override fetch to return a patch with a known total and witness the application
     * via the rendered count text.
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate("window.openSetupWizard()");
    ctx.evaluate(
      "window.fetch = () => Promise.resolve({ ok: true, json: () => Promise.resolve({" +
      "  success: true," +
      "  patch: { counts: { disabled: 0, enabled: 7, predefined: 5, total: 7, user: 2 }, rows: [], scopeCounts: {} }" +
      "}) });"
    );

    ctx.evaluate("window.finishSetup()");
    await ctx.flushAsync();

    assert.equal(getDisplay(ctx, "setup-modal"), "none");
    assert.equal(ctx.document.getElementById("total-count")?.textContent, "7", "patch.counts.total must land in #total-count");
    assert.equal(ctx.document.getElementById("user-count")?.textContent, ", 2 user", "patch.counts.user must land in #user-count with the 'user' suffix");
  });
});

describe("channels.ts: window.applyTagResponse", () => {

  test("updates the tag-manager-modal-content innerHTML and the tag-filter-menu innerHTML, then applies the patch", async () => {

    /* applyTagResponse is the cross-script helper used by both channels.ts (createTag, deleteTag, restoreTag, startTagRename) and config.ts (bulkToggleTag). It
     * has three side effects: (1) modalBody → #tag-manager-modal-content innerHTML, (2) filterContent → #tag-filter-menu innerHTML, (3) patch →
     * channelTable.applyPatch. We pin all three.
     */
    await using ctx = await setupChannelsRuntime();

    ctx.evaluate(
      "window.applyTagResponse({" +
      "  modalBody: '<div id=\"tmb-witness\">replaced modal</div>'," +
      "  filterContent: '<div id=\"tfm-witness\">replaced filter</div>'," +
      "  patch: { counts: { disabled: 0, enabled: 99, predefined: 50, total: 99, user: 1 } }" +
      "}, 'Tag updated.');"
    );

    assert.ok(ctx.document.getElementById("tmb-witness"), "modalBody must replace #tag-manager-modal-content innerHTML");
    assert.ok(ctx.document.getElementById("tfm-witness"), "filterContent must replace #tag-filter-menu innerHTML");
    assert.equal(ctx.document.getElementById("enabled-count")?.textContent, "99", "patch counts must land via channelTable.applyPatch");

    const toast = ctx.document.querySelector("#toast-container .toast");

    assert.ok(toast, "toast message must surface");
    assert.match(toast.textContent, /Tag updated\./);
  });
});

describe("channels.ts: auto-open of the Setup Wizard on first visit", () => {

  test("opens the setup-modal on IIFE init when data-setup-completed='false' (default state)", async () => {

    /* The setup-modal carries data-setup-completed='false' when CONFIG.channels.setupCompleted is false (the fresh-install default). The channels.ts IIFE-tail
     * code reads this attribute and calls openSetupWizard() automatically. We confirm by NOT flipping the attribute (via the leaveSetupIncomplete option) and
     * asserting the modal is visible after the script runs.
     */
    await using ctx = await setupChannelsRuntime({ leaveSetupIncomplete: true });

    assert.equal(getDisplay(ctx, "setup-modal"), "flex",
      "setup-modal must auto-open on first visit when setupCompleted is false");
  });

  test("does NOT open the setup-modal on IIFE init when data-setup-completed='true'", async () => {

    /* The negative case: setupChannelsRuntime() flips the attribute to 'true' before running the scripts, so the auto-open branch is skipped and the modal
     * stays hidden.
     */
    await using ctx = await setupChannelsRuntime();

    assert.equal(getDisplay(ctx, "setup-modal"), "none",
      "setup-modal must NOT auto-open when setupCompleted is true");
  });
});
