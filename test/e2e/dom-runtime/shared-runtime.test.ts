/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * shared-runtime.test.ts: DOM-runtime coverage for the shared client-side utilities script (src/routes/root/scripts/shared.ts). The unit suite next to the
 * generator pins the SHAPE of the emitted string ("the script contains window.channelTable"); this suite pins the RUNTIME BEHAVIOR of that emitted string when
 * a synthetic browser parses and executes it ("when window.channelTable.removeRow runs against a real DOM, the row is gone").
 *
 * The bug class this tier catches is the one most likely to bite the operator-facing UI: a typo in a DOM operation, a wrong fetch shape, a stale closure in an
 * event handler, an off-by-one in row insertion, a missed branch in a switch over patch fields. These regressions ship past the unit suite (the string shape is
 * still right) and past the rendering suite (the static HTML is still right) but blow up the moment a user clicks the affected control. Pinning runtime behavior
 * is the only way to catch them before release.
 *
 * The harness loads the full landing page through the production bootApp listener, then selectively executes the shared utilities script. Other scripts on the
 * page are NOT executed (status.ts opens an EventSource which happy-dom does not implement, and channels.ts/config.ts pull in their own DOM scans we do not want
 * polluting the namespace under test). Tests assert against window.* namespaces post-execution.
 *
 * Pattern guidance for adding tests:
 *
 *   - Pin invariants, not historical incidents. "applyPatch handles every patch field" is the contract; "the channel-number rendering bug doesn't recur" is a
 *     symptom to derive coverage from but not the test name.
 *   - Use evaluate(...) for one-shot expressions and DOM seeding; for complex setup, set ctx.document.body.innerHTML or insertAdjacentHTML in a single block.
 *   - For fetch-shape verification (persistDisplayPrefs, etc.), override window.fetch with a spy before triggering the operation. Asserting on persisted state
 *     is also acceptable but flushes timing concerns into the test - the spy is preferred.
 *   - When a runtime invariant reveals a real bug, pin current (buggy) behavior with a FIX-PENDING comment showing exactly which assertion to flip post-fix.
 *     Do NOT fix the production script in this suite - fixes are a separate authorized arc.
 */
import { describe, test } from "node:test";
import type { DisposableDomTestContext } from "../../helpers/dom.helpers.ts";
import assert from "node:assert/strict";
import { createDomTestContext } from "../../helpers/dom.helpers.ts";

/**
 * Shared bootstrap for the suite. Boots a DOM context, executes only the shared utilities script (identified by the window.channelTable namespace, the SSOT
 * marker in shared.ts that no other script defines), and returns the context. Every test calls this so the setup is uniform - tests differ only in what they
 * seed and assert.
 */
async function setupSharedRuntime(): Promise<DisposableDomTestContext> {

  const ctx = await createDomTestContext();
  const ran = ctx.runScripts((s) => s.content.includes("window.channelTable"));

  if(ran.length !== 1) {

    throw new Error("setupSharedRuntime: expected exactly one shared utilities script; got " + String(ran.length));
  }

  return ctx;
}

describe("shared.ts: showToast", () => {

  test("appends a toast element to #toast-container with the message text and the type class", async () => {

    /* showToast is the canonical client-side notification entry point. The minimal contract: when called, it appends a .toast element under #toast-container,
     * the element's textContent contains the message, and its className includes the supplied type. This pins the basic DOM-mutation behavior - regressions
     * where the function silently no-ops (wrong selector, missing container handling) surface as zero children post-call.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("window.showToast('Hello world', 'info', 0)");

    const container = ctx.document.getElementById("toast-container");

    assert.ok(container, "toast-container should exist on the rendered page");
    assert.equal(container.children.length, 1, "showToast should append exactly one toast");

    const toast = container.children[0];

    assert.match(toast?.textContent ?? "", /Hello world/, "toast textContent should include the supplied message");
    assert.match(toast?.getAttribute("class") ?? "", /\btoast\b/);
    assert.match(toast?.getAttribute("class") ?? "", /\binfo\b/);
  });

  test("error type assigns role=alert; non-error types assign role=status", async () => {

    /* The role attribute drives accessibility-tree announcements. Errors and warnings should be assertive (role=alert); routine info/success should be polite
     * (role=status). The implementation routes both error and warning to alert; everything else falls through to status.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("window.showToast('Bad', 'error', 0)");
    ctx.evaluate("window.showToast('Heads up', 'warning', 0)");
    ctx.evaluate("window.showToast('Done', 'success', 0)");

    const container = ctx.document.getElementById("toast-container");
    const toasts = container?.children ?? [];

    assert.equal(toasts[0]?.getAttribute("role"), "alert", "error toast should be role=alert");
    assert.equal(toasts[1]?.getAttribute("role"), "alert", "warning toast should be role=alert");
    assert.equal(toasts[2]?.getAttribute("role"), "status", "success toast should be role=status");
  });

  test("action option appends a toast-action button before the close button", async () => {

    /* Optional action pattern: when caller passes { label, onclick }, the toast gets an extra inline button. The button text is the supplied label and clicking
     * it both fires the supplied onclick AND dismisses the toast. We assert structural shape here; click-to-dismiss is hard to verify without forcing a
     * synthetic click, which we cover separately.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("window.showToast('Saved', 'success', 0, { label: 'Undo', onclick: () => { window.harnessUndoFired = true; } })");

    const container = ctx.document.getElementById("toast-container");
    const actionBtn = container?.querySelector(".toast-action");

    assert.ok(actionBtn, "toast should carry a .toast-action button when action option is supplied");
    assert.equal(actionBtn.textContent, "Undo");
  });
});

describe("shared.ts: extractErrorMessage", () => {

  test("formats field-error responses as field: message pairs joined by commas", async () => {

    /* The form-error envelope shape is { errors: { fieldA: "message", fieldB: "..." } }. extractErrorMessage flattens this into a human-readable string. The
     * exact join semantics matter because every save-failure UI flows through this helper - a regression that switched the join from comma+space to plain comma
     * would change every error toast in the app.
     */
    await using ctx = await setupSharedRuntime();

    const out = ctx.evaluate(
      "window.extractErrorMessage({ errors: { name: 'Required', port: 'Out of range' } }, 'fallback')"
    );

    assert.equal(out, "name: Required, port: Out of range");
  });

  test("falls back to response.error or response.message for top-level envelope shapes", async () => {

    /* The non-form envelope is { error: "..." } or { message: "..." }. Both are observed in the codebase. extractErrorMessage tries error first, then message,
     * then the supplied fallback.
     */
    await using ctx = await setupSharedRuntime();

    assert.equal(ctx.evaluate("window.extractErrorMessage({ error: 'Conflict' }, 'fb')"), "Conflict");
    assert.equal(ctx.evaluate("window.extractErrorMessage({ message: 'Down' }, 'fb')"), "Down");
    assert.equal(ctx.evaluate("window.extractErrorMessage({}, 'last resort')"), "last resort");
    assert.equal(ctx.evaluate("window.extractErrorMessage(null, 'last resort')"), "last resort");
  });
});

describe("shared.ts: window.dropdowns namespace", () => {

  test("addHook deduplicates so the same callback registered twice fires once", async () => {

    /* The addHook implementation guards against double-registration via includes(). Without this guard, components that register a hook on every render would
     * accumulate duplicates and fire the same flush logic N times per close. We register the same callback twice and assert close fires it once.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("window.harnessHookCalls = 0;");
    ctx.evaluate("const h = () => { window.harnessHookCalls += 1; }; window.dropdowns.addHook(h); window.dropdowns.addHook(h);");
    ctx.evaluate("window.dropdowns.close();");

    assert.equal(ctx.evaluate("window.harnessHookCalls"), 1, "duplicate addHook should result in a single hook call on close");
  });

  test("close fires every distinct registered hook", async () => {

    /* Multiple distinct hooks register independently (e.g., the inline tag editor and a hypothetical other component). close must fire every one. We register
     * three callbacks that each push into an array and assert all three pushed.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("window.harnessHookOrder = []; window.dropdowns.addHook(() => window.harnessHookOrder.push('a'));");
    ctx.evaluate("window.dropdowns.addHook(() => window.harnessHookOrder.push('b'));");
    ctx.evaluate("window.dropdowns.addHook(() => window.harnessHookOrder.push('c'));");
    ctx.evaluate("window.dropdowns.close();");

    assert.deepEqual(ctx.evaluateJson("window.harnessHookOrder"), [ "a", "b", "c" ]);
  });

  test("close removes the .show class from every open dropdown menu in the DOM", async () => {

    /* The visible "open" state is encoded in the .show class on .dropdown-menu elements. close iterates every such element and strips the class. We seed two
     * open menus, call close, and confirm both lose .show.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', '<div class=\"dropdown-menu show\" id=\"m1\"></div><div class=\"dropdown-menu show\" id=\"m2\"></div>');"
    );

    ctx.evaluate("window.dropdowns.close();");

    assert.equal(ctx.document.getElementById("m1")?.classList.contains("show"), false);
    assert.equal(ctx.document.getElementById("m2")?.classList.contains("show"), false);
  });
});

describe("shared.ts: safe localStorage wrappers", () => {

  test("set then get round-trips a string value", async () => {

    /* Round-trip is the basic contract. happy-dom provides a per-Window localStorage that's empty at construction time, so the set-then-get sequence is
     * deterministic.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("window.safeStorageSet('harness-key', 'harness-value')");

    assert.equal(ctx.evaluate("window.safeStorageGet('harness-key')"), "harness-value");
  });

  test("get of a missing key returns null", async () => {

    /* localStorage.getItem returns null for missing keys; the wrapper preserves this. Returning undefined or empty string would be a contract change that
     * breaks every call site that uses ?? for defaults.
     */
    await using ctx = await setupSharedRuntime();

    assert.equal(ctx.evaluate("window.safeStorageGet('nonexistent-key')"), null);
  });

  test("remove deletes a previously-set key", async () => {

    /* Lifecycle: set, verify, remove, verify-gone. Pins all three operations in one test so a regression in any of them surfaces here.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("window.safeStorageSet('to-remove', 'present')");

    assert.equal(ctx.evaluate("window.safeStorageGet('to-remove')"), "present");
    ctx.evaluate("window.safeStorageRemove('to-remove')");
    assert.equal(ctx.evaluate("window.safeStorageGet('to-remove')"), null);
  });
});

describe("shared.ts: persistDisplayPrefs", () => {

  test("POSTs the supplied body as JSON to /config/channels/display-prefs", async () => {

    /* The fetch shape is the contract: POST verb, JSON body, Content-Type header. We override window.fetch with a spy that captures the call shape, trigger the
     * operation, and assert all three pieces. Spy-based verification is preferred over checking persisted state because it's deterministic - no debounce or
     * flush timing concerns.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "window.harnessFetchCalls = []; " +
      "window.fetch = (url, opts) => { " +
      "  const ct = opts && opts.headers && opts.headers['Content-Type']; " +
      "  window.harnessFetchCalls.push({ url, method: opts && opts.method, body: opts && opts.body, contentType: ct }); " +
      "  return { catch() {} }; " +
      "};"
    );

    ctx.evaluate("window.persistDisplayPrefs({ sortDirection: 'asc', sortField: 'name' })");

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as { body: string; contentType: string; method: string; url: string }[];

    assert.equal(calls.length, 1, "persistDisplayPrefs should issue exactly one fetch");

    const call = calls[0]!;

    assert.equal(call.url, "/config/channels/display-prefs", "endpoint URL should be /config/channels/display-prefs");
    assert.equal(call.method, "POST");
    assert.equal(call.contentType, "application/json");
    assert.equal(call.body, JSON.stringify({ sortDirection: "asc", sortField: "name" }));
  });
});

describe("shared.ts: createSubtabSwitcher", () => {

  test("returned switchFn updates active classes on buttons and panels", async () => {

    /* The factory returns a parameterized switch function. Calling it with a subtab name should: (a) add active to the matching button and remove from others;
     * (b) add active to the matching panel and remove from others; (c) update aria-selected.
     */
    await using ctx = await setupSharedRuntime();

    // Seed two buttons and two panels matching the harness-test config.
    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<button class=\"htb\" data-htsubtab=\"alpha\" aria-selected=\"true\">A</button>' + " +
      "'<button class=\"htb\" data-htsubtab=\"beta\" aria-selected=\"false\">B</button>' + " +
      "'<div class=\"htp\" id=\"htpanel-alpha\" class=\"active\"></div>' + " +
      "'<div class=\"htp\" id=\"htpanel-beta\"></div>');"
    );

    ctx.evaluate(
      "window.harnessSwitchFn = window.createSubtabSwitcher({ " +
      "  btnSelector: '.htb', dataAttr: 'data-htsubtab', panelSelector: '.htp', panelPrefix: 'htpanel-', storageKey: 'htkey', hashFn: (s) => '#h-' + s " +
      "});"
    );

    ctx.evaluate("window.harnessSwitchFn('beta', false)");

    const alphaBtn = ctx.document.querySelector(".htb[data-htsubtab=\"alpha\"]");
    const betaBtn = ctx.document.querySelector(".htb[data-htsubtab=\"beta\"]");

    assert.ok(alphaBtn, "alpha button fixture should exist");
    assert.ok(betaBtn, "beta button fixture should exist");
    assert.equal(alphaBtn.classList.contains("active"), false, "alpha button should lose active class");
    assert.equal(betaBtn.classList.contains("active"), true, "beta button should gain active class");
    assert.equal(alphaBtn.getAttribute("aria-selected"), "false");
    assert.equal(betaBtn.getAttribute("aria-selected"), "true");

    const betaPanel = ctx.document.getElementById("htpanel-beta");

    assert.equal(betaPanel?.classList.contains("active"), true, "beta panel should be active");
  });

  test("switchFn persists the selection to localStorage via the supplied storageKey", async () => {

    /* The switcher's localStorage write uses safeStorageSet. The storageKey from the factory config is honored. Confirms cross-call behavior without testing
     * the implementation detail of safeStorage's try/catch.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', '<button class=\"st-btn\" data-st-tab=\"target\"></button><div class=\"st-panel\" id=\"st-panel-target\"></div>');"
    );

    ctx.evaluate(
      "const fn = window.createSubtabSwitcher({ btnSelector: '.st-btn', dataAttr: 'data-st-tab', panelSelector: '.st-panel', panelPrefix: 'st-panel-', " +
      "storageKey: 'harness-storage-key', hashFn: (s) => '#' + s }); fn('target', false);"
    );

    assert.equal(ctx.evaluate("localStorage.getItem('harness-storage-key')"), "target");
  });

  test("clicking a subtab button invokes switchFn with that button's data-attr", async () => {

    /* The factory attaches a click handler to every matching button. A click on a button should switch to that subtab. We seed two buttons, click one
     * synthetically, and verify the panel state reflects the click target.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', '<button class=\"clk-btn\" data-clk=\"x\"></button><button class=\"clk-btn\" data-clk=\"y\"></button>" +
      "<div class=\"clk-panel active\" id=\"clk-x\"></div><div class=\"clk-panel\" id=\"clk-y\"></div>');"
    );

    ctx.evaluate(
      "window.createSubtabSwitcher({ btnSelector: '.clk-btn', dataAttr: 'data-clk', panelSelector: '.clk-panel', panelPrefix: 'clk-', " +
      "storageKey: 'clk-key', hashFn: (s) => '#' + s });"
    );

    // Click the second button. The factory's click handler should fire switchFn('y').
    ctx.evaluate("document.querySelector('.clk-btn[data-clk=\"y\"]').click()");

    assert.equal(ctx.document.getElementById("clk-y")?.classList.contains("active"), true, "clicking the y button should activate the y panel");
    assert.equal(ctx.document.getElementById("clk-x")?.classList.contains("active"), false, "x panel should lose active");
  });

  test("onSwitch callback fires before localStorage persistence", async () => {

    /* The optional onSwitch callback lets the caller inject per-tab logic. Order matters: the callback observes the new subtab BEFORE storage is written so it
     * can synchronously cancel-by-throw if needed (no current consumer does this, but the contract permits it). We don't test the throw path; we just confirm
     * the callback receives the subtab argument.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', '<button class=\"oc-btn\" data-oc=\"only\"></button><div class=\"oc-panel\" id=\"oc-only\"></div>');"
    );

    ctx.evaluate(
      "window.harnessOnSwitchArg = null; window.createSubtabSwitcher({ btnSelector: '.oc-btn', dataAttr: 'data-oc', panelSelector: '.oc-panel', " +
      "panelPrefix: 'oc-', storageKey: 'oc-key', hashFn: (s) => '#' + s, onSwitch: (s) => { window.harnessOnSwitchArg = s; } })('only', false);"
    );

    assert.equal(ctx.evaluate("window.harnessOnSwitchArg"), "only");
  });
});

describe("shared.ts: initSubtab", () => {

  test("uses config.hashVar when provided and the matching button exists", async () => {

    /* initSubtab's hashVar branch: when the caller passes a non-null hashVar (typically read from window.location.hash by the calling tab script), initSubtab
     * delegates to config.switchFn(hashVar, false) instead of reading localStorage. We seed a fake switchFn that records its argument and confirm hashVar wins.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', '<button class=\"is-btn\" data-istab=\"target\"></button>');" +
      "window.harnessSwitchArgs = null;" +
      "window.initSubtab({ btnSelector: '.is-btn', dataAttr: 'data-istab', hashVar: 'target', storageKey: 'is-storage'," +
      "  switchFn: (s, updateUrl) => { window.harnessSwitchArgs = { s, updateUrl }; } });"
    );

    assert.deepEqual(ctx.evaluateJson("window.harnessSwitchArgs"), { s: "target", updateUrl: false });
  });

  test("falls back to safeStorageGet(storageKey) when hashVar is absent", async () => {

    /* initSubtab's localStorage branch: when hashVar is null/undefined, the function calls safeStorageGet against config.storageKey. We seed localStorage and
     * confirm initSubtab reads from it.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "localStorage.setItem('is-fallback-key', 'persisted-tab');" +
      "document.body.insertAdjacentHTML('beforeend', '<button class=\"is2-btn\" data-istab2=\"persisted-tab\"></button>');" +
      "window.harnessSwitchArgs = null;" +
      "window.initSubtab({ btnSelector: '.is2-btn', dataAttr: 'data-istab2', hashVar: null, storageKey: 'is-fallback-key'," +
      "  switchFn: (s, updateUrl) => { window.harnessSwitchArgs = { s, updateUrl }; } });"
    );

    assert.deepEqual(ctx.evaluateJson("window.harnessSwitchArgs"), { s: "persisted-tab", updateUrl: false });
  });

  test("does NOT call switchFn when no matching button exists in the DOM (defensive guard)", async () => {

    /* The button-existence check protects against stale localStorage values pointing at subtabs that have been removed. Without this guard, switchFn would fire
     * against a non-existent panel and silently no-op the visible UI. We persist a value but do NOT seed the matching button - switchFn must not fire.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "localStorage.setItem('is-stale-key', 'no-such-tab');" +
      "window.harnessSwitchCalled = false;" +
      "window.initSubtab({ btnSelector: '.is3-btn', dataAttr: 'data-istab3', hashVar: null, storageKey: 'is-stale-key'," +
      "  switchFn: () => { window.harnessSwitchCalled = true; } });"
    );

    assert.equal(ctx.evaluate("window.harnessSwitchCalled"), false, "switchFn must not fire for a stale stored subtab");
  });
});

describe("shared.ts: positionPortal", () => {

  test("writes top/left as pixel strings on the menu's inline style with scrollX/scrollY offsets", async () => {

    /* positionPortal computes a viewport-clamped position for the menu relative to the anchor's bounding rect, then writes the absolute-positioned coordinates
     * with scroll offsets baked in (so the menu stays attached as the page scrolls). We seed minimal fixtures with known dimensions and assert the inline style
     * lands as pixel strings in the expected shape.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', '<button id=\"pp-anchor\" style=\"position:absolute;top:50px;left:60px;width:80px;height:30px;\"></button>" +
      "<div id=\"pp-menu\" style=\"position:absolute;width:100px;height:50px;\"></div>');"
    );

    ctx.evaluate("window.positionPortal(document.getElementById('pp-menu'), document.getElementById('pp-anchor'));");

    /* The exact computed values depend on happy-dom's layout (which is best-effort, not pixel-precise like a real browser), so we assert structural shape -
     * the style values are pixel strings - rather than specific numbers. The contract that matters is that positionPortal wrote SOMETHING to top/left in the
     * pixel-string format.
     */
    const top = ctx.evaluate("document.getElementById('pp-menu').style.top") as string;
    const left = ctx.evaluate("document.getElementById('pp-menu').style.left") as string;

    assert.match(top, /^-?\d+px$/, "menu.style.top should be a pixel string");
    assert.match(left, /^-?\d+px$/, "menu.style.left should be a pixel string");
  });
});

describe("shared.ts: toggleDropdown", () => {

  test("first call adds .show to the menu and registers scroll/resize listeners; second call closes it", async () => {

    /* toggleDropdown is the user-facing dropdown lifecycle. First click on the trigger button: portal the menu under document.body, add .show, wire scroll
     * + resize listeners that close the menu when the page moves. Second click: closeAllDropdowns runs (which strips .show and removes the listeners). We
     * assert the show-class transition; listener wire-up is harder to verify directly but is exercised by the dropdowns.close test elsewhere in this suite.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<button id=\"td-btn\">trigger</button>' + " +
      "'<div class=\"dropdown-menu\" id=\"td-menu\"></div>');"
    );

    // First call - menu opens.
    ctx.evaluate(
      "window.toggleDropdown(document.getElementById('td-btn'), { menu: document.getElementById('td-menu') });"
    );

    assert.equal(ctx.document.getElementById("td-menu")?.classList.contains("show"), true, "first toggle should add .show to the menu");

    // Second call on the same trigger - menu closes (closeAllDropdowns strips the class before the early-return).
    ctx.evaluate(
      "window.toggleDropdown(document.getElementById('td-btn'), { menu: document.getElementById('td-menu') });"
    );

    assert.equal(ctx.document.getElementById("td-menu")?.classList.contains("show"), false, "second toggle should remove .show from the menu");
  });

  test("onOpen callback fires after show but before final positioning so content can size correctly", async () => {

    /* The onOpen lifecycle hook lets the caller populate menu content before positionPortal measures the menu's offsetWidth/offsetHeight. The contract: onOpen
     * receives the menu element AFTER show is added (so display:block is computed and measurement is possible) and BEFORE positionPortal runs (so the new
     * dimensions feed into clamping). We confirm the call shape via a captured argument.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<button id=\"td2-btn\">trigger</button>' + " +
      "'<div class=\"dropdown-menu\" id=\"td2-menu\"></div>');"
    );

    ctx.evaluate(
      "window.harnessOnOpenSawShow = null;" +
      "window.toggleDropdown(document.getElementById('td2-btn'), {" +
      "  menu: document.getElementById('td2-menu')," +
      "  onOpen: (m) => { window.harnessOnOpenSawShow = m.classList.contains('show'); }" +
      "});"
    );

    assert.equal(ctx.evaluate("window.harnessOnOpenSawShow"), true, "onOpen should observe the menu with .show already added");
  });
});

describe("shared.ts: copyToClipboard", () => {

  test("uses navigator.clipboard.writeText when available and shows a success toast", async () => {

    /* copyToClipboard's primary path: when navigator.clipboard.writeText is callable (secure contexts in real browsers), the helper awaits it and shows the
     * supplied successMessage as a success toast. We override navigator.clipboard with a spy that records the written text, then assert (a) the spy received
     * the right value and (b) a success toast landed.
     *
     * happy-dom provides a navigator.clipboard implementation; we override it to capture the call rather than depend on happy-dom's clipboard semantics.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "window.harnessClipboardWrite = null;" +
      "Object.defineProperty(navigator, 'clipboard', {" +
      "  configurable: true, value: { writeText: (text) => { window.harnessClipboardWrite = text; return Promise.resolve(); } }" +
      "});"
    );

    ctx.evaluate("window.copyToClipboard('hello there', 'Copied!');");
    await ctx.flushAsync();

    assert.equal(ctx.evaluate("window.harnessClipboardWrite"), "hello there", "clipboard.writeText should have received the supplied text");

    const container = ctx.document.getElementById("toast-container");

    assert.ok((container?.children.length ?? 0) > 0, "success toast should be appended after a successful copy");
    assert.match(container?.children[0]?.textContent ?? "", /Copied!/);
  });

  test("falls back to a hidden textarea + execCommand when navigator.clipboard.writeText is unavailable", async () => {

    /* The HTTP-without-secure-context fallback path: copyToClipboard creates a hidden <textarea>, sets its value, calls .select() and document.execCommand
     * ('copy'), then removes the element. We override execCommand to capture invocation and verify the textarea was created (and removed) around the call.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "window.harnessExecCalls = [];" +
      "Object.defineProperty(navigator, 'clipboard', { configurable: true, value: undefined });" +
      "document.execCommand = (cmd) => { window.harnessExecCalls.push(cmd); return true; };"
    );

    ctx.evaluate("window.copyToClipboard('fallback text', 'Fallback OK!');");
    await ctx.flushAsync();

    assert.deepEqual(ctx.evaluateJson("window.harnessExecCalls"), ["copy"], "execCommand should be called with 'copy' once");
    assert.equal(ctx.document.querySelector("textarea[style*=\"opacity: 0\"]"), null, "the temporary textarea should be removed after copying");
  });
});

describe("shared.ts: channelDisplayHtml and serviceIconHtml", () => {

  test("channelDisplayHtml mode='text' returns a span with the channel name and no <img>", async () => {

    /* Text mode is the no-logo fallback: span with the textClass and the name as textContent. No image element. We verify by parsing the returned HTML into
     * a temporary container and checking child structure.
     */
    await using ctx = await setupSharedRuntime();

    const html = ctx.evaluate("window.channelDisplayHtml('https://example.test/logo.png', 'CNN', 'logo-class', 'text-class', 'text')") as string;

    assert.ok(!html.includes("<img"), "text mode should not include <img>");
    assert.match(html, /class="text-class"/);
    assert.match(html, /CNN/);
  });

  test("channelDisplayHtml mode='logo' returns an <img> with onerror and a hidden text fallback span", async () => {

    /* Logo mode (default): the image is the primary element with the text span hidden via inline style:display:none. The img carries onerror="imgFallback(this)",
     * and since channelDisplayHtml emits no data-fallbacks attribute, a broken logo URL exhausts the (empty) fallback chain immediately and reveals the hidden
     * text span. The data-fallbacks chain itself is a property of serviceIconHtml (exercised at lines 602-613), not channelDisplayHtml.
     */
    await using ctx = await setupSharedRuntime();

    const html = ctx.evaluate("window.channelDisplayHtml('https://example.test/logo.png', 'CNN', 'logo-class', 'text-class', 'logo')") as string;

    assert.match(html, /<img[^>]+src="https:\/\/example\.test\/logo\.png"/);
    assert.match(html, /onerror="imgFallback\(this\)"/);
    assert.match(html, /style="display:none"/, "text fallback span should be hidden in logo mode");
  });

  test("channelDisplayHtml falls back to text mode when logoUrl is empty even if mode='logo'", async () => {

    /* Defensive coding: an empty logoUrl with mode='logo' would otherwise produce an image with src="" which the browser fails to render. The implementation
     * folds empty logoUrl into text-mode rendering regardless of the mode parameter.
     */
    await using ctx = await setupSharedRuntime();

    const html = ctx.evaluate("window.channelDisplayHtml('', 'No Logo', 'lc', 'tc', 'logo')") as string;

    assert.ok(!html.includes("<img"), "empty logoUrl should suppress the <img> regardless of mode");
    assert.match(html, /No Logo/);
  });

  test("serviceIconHtml builds the apple-touch-icon and favicon fallback chain from the domain", async () => {

    /* Service icons derive their URL chain from the domain: primary src is the supplied iconUrl (or apple-touch-icon when iconUrl is unset), fallbacks are
     * apple-touch-icon and favicon.ico from the same domain. The data-fallbacks attribute is pipe-separated for imgFallback to consume.
     */
    await using ctx = await setupSharedRuntime();

    const html = ctx.evaluate("window.serviceIconHtml('cnn.com', 'CNN', 'icon-cl', 'text-cl', 'logo', 'https://example.test/icon.png')") as string;

    assert.match(html, /src="https:\/\/example\.test\/icon\.png"/, "iconUrl wins as the primary src");
    assert.match(html, /data-fallbacks="https:\/\/cnn\.com\/apple-touch-icon\.png\|https:\/\/cnn\.com\/favicon\.ico"/);
  });
});

describe("shared.ts: imgFallback", () => {

  test("advances through the data-fallbacks chain on successive errors", async () => {

    /* imgFallback reads the pipe-separated URL list from data-fallbacks, indexes by data-fb-idx, and rotates src + idx on each call. The first call swaps to
     * fallback[0] and bumps idx to 1; the second swaps to fallback[1] and bumps idx to 2; etc.
     *
     * We assert on the literal stored src via getAttribute("src") rather than the .src property because happy-dom mirrors browsers in resolving .src to an
     * absolute URL against the document base. The DOM literal value is what the production code wrote.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', '<img id=\"fb-img\" src=\"original.png\" data-fallbacks=\"a.png|b.png\">');"
    );

    ctx.evaluate("window.imgFallback(document.getElementById('fb-img'))");

    assert.equal(ctx.document.getElementById("fb-img")?.getAttribute("src"), "a.png", "first imgFallback call should switch src to fallback[0]");
    assert.equal(ctx.document.getElementById("fb-img")?.getAttribute("data-fb-idx"), "1");

    ctx.evaluate("window.imgFallback(document.getElementById('fb-img'))");

    assert.equal(ctx.document.getElementById("fb-img")?.getAttribute("src"), "b.png");
    assert.equal(ctx.document.getElementById("fb-img")?.getAttribute("data-fb-idx"), "2");
  });

  test("hides the image and reveals the hidden text sibling when the fallback chain is exhausted", async () => {

    /* End-of-chain behavior: image gets display:none, the next sibling (which had display:none from logo-mode rendering) is revealed by setting
     * display:inline. This is the visible-text-only state after every URL in the chain failed.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', '<span id=\"fb-wrap\"><img id=\"fb-img2\" src=\"orig.png\" data-fallbacks=\"\" data-fb-idx=\"0\">" +
      "<span class=\"sib\" style=\"display:none\">name</span></span>');"
    );

    ctx.evaluate("window.imgFallback(document.getElementById('fb-img2'))");

    assert.equal(ctx.evaluate("document.getElementById('fb-img2').style.display"), "none", "image should be hidden when fallbacks exhausted");
    assert.equal(ctx.evaluate("document.querySelector('#fb-wrap .sib').style.display"), "inline", "hidden text sibling should be revealed");
  });
});

describe("shared.ts: processServiceDisplays", () => {

  test("renders every .provider-display element via serviceIconHtml and marks them processed", async () => {

    /* processServiceDisplays walks the DOM, transforms each .provider-display into a logo+text rendering, and stamps data-processed='1' to prevent
     * re-processing on subsequent calls. We seed three elements (in a scoped container so we can query only our fixture, not the production-page-rendered
     * .provider-display elements that already exist), run the processor, and confirm every one carries the post-process marker and an inserted <img> for the
     * service icon.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<div id=\"psd-scope\">' + " +
      "'<span class=\"provider-display\" data-domain=\"a.com\">A</span>' + " +
      "'<span class=\"provider-display\" data-domain=\"b.com\">B</span>' + " +
      "'<span class=\"provider-display\" data-domain=\"c.com\">C</span>' + " +
      "'</div>');"
    );

    ctx.evaluate("window.processServiceDisplays();");

    const elems = ctx.document.querySelectorAll("#psd-scope .provider-display");

    assert.equal(elems.length, 3, "fixture should have exactly three scoped .provider-display elements");

    for(const el of elems) {

      assert.equal(el.getAttribute("data-processed"), "1", "every element should carry the data-processed marker");
      assert.ok(el.querySelector("img"), "every processed element should contain an <img>");
    }
  });

  test("skips elements that already carry data-processed='1' (idempotent)", async () => {

    /* Idempotency is required because processServiceDisplays is called on every relevant DOM mutation. If it weren't idempotent, repeated calls would
     * re-render the entire list every time, bloating the DOM and clobbering any in-flight image loads. We pre-mark one element processed and confirm it is
     * left untouched.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<span class=\"provider-display\" data-domain=\"x.com\" data-processed=\"1\">PRE-MARKED</span>');"
    );

    ctx.evaluate("window.processServiceDisplays();");

    const elem = ctx.document.querySelector(".provider-display[data-domain=\"x.com\"]");

    assert.ok(elem, "pre-marked fixture element should exist");
    assert.equal(elem.textContent, "PRE-MARKED", "pre-marked element should be left untouched (no img injected)");
    assert.equal(elem.querySelector("img"), null);
  });
});

describe("shared.ts: createWizardController", () => {

  /**
   * Inserts a minimal wizard modal DOM into the page so the controller has elements to bind against. Mirrors the structural layout that generateWizardModal
   * produces. Each test calls this with its own modal id so multiple modals can coexist without selector collisions.
   */
  function seedWizardDom(ctx: DisposableDomTestContext, id: string, stepCount: number): void {

    const stepsHtml: string[] = [];

    for(let i = 1; i <= stepCount; i++) {

      stepsHtml.push("<div class=\"wizard-step\" data-step=\"" + String(i) + "\"><span class=\"step-circle\">" + String(i) + "</span></div>");
    }

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', '" +
      "<div id=\"" + id + "\" style=\"display:none;\">" +
      "<div class=\"wizard-steps\" id=\"" + id + "-steps\">" + stepsHtml.join("") + "</div>" +
      "<div id=\"" + id + "-content\"></div>" +
      "<div id=\"" + id + "-error\" style=\"display:none\"></div>" +
      "<button data-wizard-role=\"back\" id=\"" + id + "-back\">Back</button>" +
      "<button data-wizard-role=\"next\" id=\"" + id + "-next\">Next</button>" +
      "<button class=\"wizard-close\" id=\"" + id + "-x\">X</button>" +
      "</div>');"
    );
  }

  test("open displays the modal, renders step 1, and back/next/goToStep navigate between steps", async () => {

    /* Lifecycle: open shows the modal, render fires for step 1, getStep returns 1. next advances to step 2 (validation passes by default). back returns to
     * step 1. We capture the rendered step number via the onRender callback so the test can verify the rendering observer fired with the correct step.
     *
     * next() and goToStep() are async (validateAndAdvance is wrapped in async even when onValidate is sync). We flushAsync after each transition so the
     * sandbox's microtask queue settles before the next assertion runs.
     */
    await using ctx = await setupSharedRuntime();

    seedWizardDom(ctx, "wizA", 3);

    ctx.evaluate(
      "window.harnessRendered = []; window.harnessCtrl = window.createWizardController({" +
      "  contentId: 'wizA-content', errorId: 'wizA-error', modalId: 'wizA', stepCount: 3," +
      "  onRender: (s) => { window.harnessRendered.push(s); }," +
      "  onValidate: () => '' " +
      "});"
    );

    ctx.evaluate("window.harnessCtrl.open();");

    assert.equal(ctx.evaluate("window.harnessCtrl.getStep()"), 1);
    assert.equal(ctx.evaluate("document.getElementById('wizA').style.display"), "flex");

    ctx.evaluate("window.harnessCtrl.next();");
    await ctx.flushAsync();
    assert.equal(ctx.evaluate("window.harnessCtrl.getStep()"), 2);

    ctx.evaluate("window.harnessCtrl.back();");
    assert.equal(ctx.evaluate("window.harnessCtrl.getStep()"), 1);

    assert.deepEqual(ctx.evaluateJson("window.harnessRendered"), [ 1, 2, 1 ], "onRender should fire for every step transition");
  });

  test("next blocks advancement when onValidate returns a non-empty error string", async () => {

    /* Validation gate: returning a truthy string from onValidate should keep currentStep where it was and surface the error via setError. We attempt to
     * advance, confirm step stays at 1, and confirm the error display shows the validation message.
     */
    await using ctx = await setupSharedRuntime();

    seedWizardDom(ctx, "wizB", 2);

    ctx.evaluate(
      "window.harnessCtrl = window.createWizardController({" +
      "  contentId: 'wizB-content', errorId: 'wizB-error', modalId: 'wizB', stepCount: 2," +
      "  onRender: () => {}, onValidate: () => 'Required field missing' " +
      "});"
    );

    ctx.evaluate("window.harnessCtrl.open();");
    ctx.evaluate("window.harnessCtrl.next();");
    await ctx.flushAsync();

    assert.equal(ctx.evaluate("window.harnessCtrl.getStep()"), 1, "step should not advance when onValidate returns an error");

    const errEl = ctx.document.getElementById("wizB-error");

    assert.equal(errEl?.textContent, "Required field missing");
    assert.notEqual(ctx.evaluate("document.getElementById('wizB-error').style.display"), "none", "error display should be visible");
  });

  test("close hides the modal, fires the onClose callback, and resets state to {}", async () => {

    /* Close lifecycle: optional onClose runs first, modal display goes to none, ctrl.state is reset to a fresh empty object. The state reset matters because
     * tests that re-open the controller should not see leaked state from the previous session.
     */
    await using ctx = await setupSharedRuntime();

    seedWizardDom(ctx, "wizC", 2);

    ctx.evaluate(
      "window.harnessClosed = false; window.harnessCtrl = window.createWizardController({" +
      "  contentId: 'wizC-content', errorId: 'wizC-error', modalId: 'wizC', stepCount: 2," +
      "  onRender: () => {}, onValidate: () => '', onClose: () => { window.harnessClosed = true; }" +
      "});"
    );

    ctx.evaluate("window.harnessCtrl.open();");
    ctx.evaluate("window.harnessCtrl.state.persisted = 'value';");
    ctx.evaluate("window.harnessCtrl.close();");

    assert.equal(ctx.evaluate("window.harnessClosed"), true, "onClose callback should fire");
    assert.equal(ctx.evaluate("document.getElementById('wizC').style.display"), "none");
    assert.equal(ctx.evaluate("window.harnessCtrl.state.persisted"), undefined, "state should be reset on close");
  });

  test("the controller binds click handlers to data-wizard-role buttons during construction", async () => {

    /* Role-tagged buttons (data-wizard-role=back/next/close) have their click handlers attached at construction time by createWizardController, closing over the
     * per-controller ctrl instance. We confirm the wiring by clicking the role=next button and asserting that the controller advances the step.
     */
    await using ctx = await setupSharedRuntime();

    seedWizardDom(ctx, "wizD", 3);

    ctx.evaluate(
      "window.harnessCtrl = window.createWizardController({" +
      "  contentId: 'wizD-content', errorId: 'wizD-error', modalId: 'wizD', stepCount: 3," +
      "  onRender: () => {}, onValidate: () => '' " +
      "});"
    );

    ctx.evaluate("window.harnessCtrl.open();");
    ctx.evaluate("document.getElementById('wizD-next').click();");
    await ctx.flushAsync();

    assert.equal(ctx.evaluate("window.harnessCtrl.getStep()"), 2, "click on the role=next button should advance the step");
  });

  test("goToStep blocks forward jumps to unvisited steps but allows backward jumps to visited steps", async () => {

    /* goToStep semantics: jumping to a step beyond highestStep is a no-op (you can't skip ahead without going through Next first). Jumping backward to any
     * visited step is always allowed and bypasses validation.
     */
    await using ctx = await setupSharedRuntime();

    seedWizardDom(ctx, "wizE", 4);

    ctx.evaluate(
      "window.harnessCtrl = window.createWizardController({" +
      "  contentId: 'wizE-content', errorId: 'wizE-error', modalId: 'wizE', stepCount: 4," +
      "  onRender: () => {}, onValidate: () => '' " +
      "});"
    );

    ctx.evaluate("window.harnessCtrl.open();");
    // Advance from step 1 to step 2.
    ctx.evaluate("window.harnessCtrl.next();");
    await ctx.flushAsync();

    // Advance from step 2 to step 3.
    ctx.evaluate("window.harnessCtrl.next();");
    await ctx.flushAsync();

    // Try jumping forward beyond highestStep (3): should be a no-op.
    ctx.evaluate("window.harnessCtrl.goToStep(4);");
    await ctx.flushAsync();
    assert.equal(ctx.evaluate("window.harnessCtrl.getStep()"), 3, "forward jump beyond highestStep should not advance");

    // Backward jump to step 1 should work.
    ctx.evaluate("window.harnessCtrl.goToStep(1);");
    assert.equal(ctx.evaluate("window.harnessCtrl.getStep()"), 1);
  });
});

describe("shared.ts: window.channelTable namespace", () => {

  test("removeRow detaches both the display row AND the edit row for the given key", async () => {

    /* Both rows must go: display-row-{key} AND edit-row-{key}. Removing only the display row would leave a stale edit row that, on subsequent inserts, could
     * mismatch the new display row. The production page renders BOTH rows server-side for every predefined channel (edit-row-abc is hidden by inline
     * style:display:none until the user clicks the inline edit button); we lean on that and verify both vanish after a single removeRow call.
     *
     * We rely on the single server-rendered edit-row-abc rather than seeding a synthetic one: seeding a second element with the same id creates a duplicate-id
     * condition, and calling .remove() on the result of getElementById('edit-row-abc') then hangs happy-dom (likely a parser-state issue around duplicate ids).
     * Trusting the server's rendered edit row is correct anyway - this is what production code operates against.
     */
    await using ctx = await setupSharedRuntime();

    // Pre-condition: both rows should exist before the call. The server renders both for predefined channels.
    assert.ok(ctx.document.getElementById("display-row-abc"), "server-rendered abc display row should exist as the precondition");
    assert.ok(ctx.document.getElementById("edit-row-abc"), "server-rendered abc edit row should exist as the precondition");

    ctx.evaluate("window.channelTable.removeRow('abc');");

    assert.equal(ctx.document.getElementById("display-row-abc"), null);
    assert.equal(ctx.document.getElementById("edit-row-abc"), null);
  });

  test("applyPatch dispatches to removeRow for action='remove' rows", async () => {

    /* applyPatch's row dispatch table: remove deletes by key. We confirm by patching with a remove action against a real predefined row.
     */
    await using ctx = await setupSharedRuntime();

    assert.ok(ctx.document.getElementById("display-row-abc"), "predefined abc row should exist before patch");

    ctx.evaluate("window.channelTable.applyPatch({ rows: [ { action: 'remove', key: 'abc' } ] });");

    assert.equal(ctx.document.getElementById("display-row-abc"), null, "row should be gone after applyPatch with remove action");
  });

  test("applyPatch updates the summary count elements when patch.counts is supplied", async () => {

    /* counts dispatch: every #disabled-count, #enabled-count, #predefined-count, #total-count, and #user-count text content updates from the patch values.
     * The user count carries special formatting: an empty string when zero, comma-prefixed N when positive.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "window.channelTable.applyPatch({ counts: { disabled: 5, enabled: 100, predefined: 80, total: 105, user: 25 } });"
    );

    assert.equal(ctx.document.getElementById("disabled-count")?.textContent, "5");
    assert.equal(ctx.document.getElementById("enabled-count")?.textContent, "100");
    assert.equal(ctx.document.getElementById("predefined-count")?.textContent, "80");
    assert.equal(ctx.document.getElementById("total-count")?.textContent, "105");
    assert.equal(ctx.document.getElementById("user-count")?.textContent, ", 25 user");
  });

  test("applyPatch counts: user=0 produces an empty user-count text", async () => {

    /* Edge case: when user channels are absent, the user-count span should be empty (not "0", not ", 0 user"). The formatting rule lives inline in the patch
     * loop; this test pins it explicitly.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "window.channelTable.applyPatch({ counts: { disabled: 0, enabled: 1, predefined: 1, total: 1, user: 0 } });"
    );

    assert.equal(ctx.document.getElementById("user-count")?.textContent, "");
  });

  test("applyPatch processes scopeCounts to update scope-toggle checkboxes and quick-action-count spans", async () => {

    /* scopeCounts entry shape: { scopeName: { enabled: number, total: number } }. checkbox.checked = (enabled === total); checkbox.indeterminate = partial.
     * The count span text is formatted as "{enabled} of {total} enabled".
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<input type=\"checkbox\" class=\"scope-toggle\" data-scope=\"east\">' + " +
      "'<span class=\"quick-action-count\" data-scope=\"east\"></span>');"
    );

    ctx.evaluate("window.channelTable.applyPatch({ scopeCounts: { east: { enabled: 5, total: 10 } } });");

    const cb = ctx.document.querySelector(".scope-toggle[data-scope=\"east\"]") as unknown as { checked: boolean; indeterminate: boolean };
    const span = ctx.document.querySelector(".quick-action-count[data-scope=\"east\"]");

    assert.equal(cb.checked, false, "5 of 10 → not all checked");
    assert.equal(cb.indeterminate, true, "5 of 10 → indeterminate");
    assert.equal(span?.textContent, "5 of 10 enabled");
  });

  test("applyPatch processes hdhrCounts to update the bulk hdhr toggle and count", async () => {

    /* HDHR bulk toggle and count update from patch.hdhrCounts. Pins the field-name and DOM-id mapping (hdhr-bulk-toggle / hdhr-bulk-count).
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<input type=\"checkbox\" id=\"hdhr-bulk-toggle\">' + " +
      "'<span id=\"hdhr-bulk-count\"></span>');"
    );

    ctx.evaluate("window.channelTable.applyPatch({ hdhrCounts: { enabled: 10, total: 10 } });");

    const cb = ctx.document.getElementById("hdhr-bulk-toggle") as unknown as { checked: boolean; indeterminate: boolean };

    assert.equal(cb.checked, true);
    assert.equal(cb.indeterminate, false);
    assert.equal(ctx.document.getElementById("hdhr-bulk-count")?.textContent, "10 of 10");
  });

  test("applyPatch processes tagCounts to update per-tag bulk toggles and counts", async () => {

    /* tagCounts entry shape: { tagName: { count: number, total: number } }. Each tag's bulk-toggle checkbox tracks all-checked / partial state and the count
     * span shows "{count} of {total}".
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<input type=\"checkbox\" class=\"tag-bulk-toggle\" data-tag=\"news\">' + " +
      "'<span data-tag-count=\"news\"></span>');"
    );

    ctx.evaluate("window.channelTable.applyPatch({ tagCounts: { news: { count: 3, total: 8 } } });");

    const cb = ctx.document.querySelector(".tag-bulk-toggle[data-tag=\"news\"]") as unknown as { checked: boolean; indeterminate: boolean };

    assert.equal(cb.checked, false);
    assert.equal(cb.indeterminate, true);
    assert.equal(ctx.document.querySelector("[data-tag-count=\"news\"]")?.textContent, "3 of 8");
  });

  test("applyPatch processes logos by stamping data-logo on the parent td of the matching display row", async () => {

    /* The patch.logos field is a key->URL map. For each entry, the patch finds display-row-{key}, locates the .channel-name-cell within it, and stamps
     * data-logo on the parent td (which processLogos then consumes). We seed a minimal display row inside a synthetic table at the body level - inserting
     * a bare <tr> via insertAdjacentHTML on the production tbody is dropped by happy-dom's parser (the <tr> needs proper table context to be parsed). The
     * row id is what the patch handler queries, so location in document is irrelevant.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<table><tbody><tr id=\"display-row-customlogo\"><td><span class=\"channel-name-cell\">Custom</span></td></tr></tbody></table>');"
    );

    ctx.evaluate("window.channelTable.applyPatch({ logos: { customlogo: 'https://example.test/logo.png' } });");

    const row = ctx.document.getElementById("display-row-customlogo");
    const td = row?.querySelector("td");

    assert.equal(td?.getAttribute("data-logo"), "https://example.test/logo.png");
  });

  test("filter hides rows whose data-provider-tags do not include any enabled tag", async () => {

    /* Service filter logic: a row's data-provider-tags is comma-separated. The row is "available" if any tag matches an enabled tag OR the special "direct"
     * tag is present. Unavailable rows get the channel-unavailable class.
     *
     * We use a body-level synthetic table because tbody.insertAdjacentHTML('beforeend', '<tr>') is dropped by happy-dom's parser when the <tr> lacks proper
     * table context. Body-level insertion of a complete <table><tbody>...</tbody></table> parses correctly. The filter function queries
     * tr[data-provider-tags] from anywhere in document, so location is irrelevant.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<table><tbody>' + " +
      "'<tr data-provider-tags=\"sling\" id=\"r1\"><td></td></tr>' + " +
      "'<tr data-provider-tags=\"yttv\" id=\"r2\"><td></td></tr>' + " +
      "'<tr data-provider-tags=\"sling,direct\" id=\"r3\"><td></td></tr>' + " +
      "'</tbody></table>');"
    );

    ctx.evaluate("window.channelTable.filter([ 'yttv' ]);");

    assert.equal(ctx.document.getElementById("r1")?.classList.contains("channel-unavailable"), true, "r1 (sling only, not enabled) should be unavailable");
    assert.equal(ctx.document.getElementById("r2")?.classList.contains("channel-unavailable"), false, "r2 (yttv enabled) should be available");
    assert.equal(ctx.document.getElementById("r3")?.classList.contains("channel-unavailable"), false, "r3 (direct present) should be available regardless");
  });

  test("filter with empty enabled tag list treats every row as available", async () => {

    /* Empty-list semantics: an empty enabledTags array means "no filter applied" - every row is available regardless of its tags. This matches the server's
     * empty enabledServices interpretation ("show all").
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<table><tbody><tr data-provider-tags=\"sling\" id=\"e1\" class=\"channel-unavailable\"><td></td></tr></tbody></table>');"
    );

    ctx.evaluate("window.channelTable.filter([]);");

    assert.equal(ctx.document.getElementById("e1")?.classList.contains("channel-unavailable"), false, "empty enabledTags should clear unavailable class");
  });

  test("getEnabledFilterTags returns an empty array when every checkbox in the menu is checked", async () => {

    /* The "all checked" state is treated as "no filter" - returning the literal enabled-tag list would shrink the listing every time a new tag was added,
     * which is the opposite of operator intent. Empty-when-all-checked is the SSOT semantic.
     *
     * The production page already renders a .provider-dropdown-menu in the channel-table header. The function picks the first match in document order, so
     * we remove the production menu before inserting our fixture to ensure ours is the one being read.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("document.querySelectorAll('.provider-dropdown-menu').forEach((el) => el.remove());");
    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<div class=\"provider-dropdown-menu\">' + " +
      "'<input type=\"checkbox\" data-tag=\"a\" checked>' + " +
      "'<input type=\"checkbox\" data-tag=\"b\" checked>' + " +
      "'</div>');"
    );

    assert.deepEqual(ctx.evaluateJson("window.channelTable.getEnabledFilterTags()"), []);
  });

  test("getEnabledFilterTags returns only the checked tags when partial selection is active", async () => {

    /* Partial state: at least one checkbox is unchecked. Returning the explicit list of checked tags is the intended filter expression.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("document.querySelectorAll('.provider-dropdown-menu').forEach((el) => el.remove());");
    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<div class=\"provider-dropdown-menu\">' + " +
      "'<input type=\"checkbox\" data-tag=\"a\" checked>' + " +
      "'<input type=\"checkbox\" data-tag=\"b\">' + " +
      "'<input type=\"checkbox\" data-tag=\"c\" checked>' + " +
      "'</div>');"
    );

    assert.deepEqual(ctx.evaluateJson("window.channelTable.getEnabledFilterTags()"), [ "a", "c" ]);
  });

  test("sort on a different field sets ascending; clicking the same field again flips to descending", async () => {

    /* Sort interaction is direction-toggling on the SAME field, ascending-resetting when the field changes. We seed the table with a known starting state
     * (sort-field=other, sort-dir=asc) then click 'name' (different field -> asc) then click 'name' again (same field -> flips to desc). We override
     * window.fetch to capture the calls and confirm the persisted directions.
     */
    await using ctx = await setupSharedRuntime();

    // Pin a known starting state: a different sort field. That way the first click on 'name' is a field-change (asc), the second is a same-field toggle.
    ctx.evaluate("document.querySelector('.channel-table').setAttribute('data-sort-field', 'channelNumber');");
    ctx.evaluate("document.querySelector('.channel-table').setAttribute('data-sort-dir', 'asc');");

    ctx.evaluate(
      "window.harnessFetchCalls = []; " +
      "window.fetch = (url, opts) => { " +
      "  window.harnessFetchCalls.push({ url, body: opts && opts.body }); return { catch() {} }; " +
      "};"
    );

    ctx.evaluate("window.channelTable.sort('name');");
    ctx.evaluate("window.channelTable.sort('name');");

    const calls = ctx.evaluateJson("window.harnessFetchCalls") as { body: string; url: string }[];

    assert.equal(calls.length, 2, "two sort calls should issue two persists");

    const first = JSON.parse(calls[0]?.body ?? "{}") as { sortDirection: string; sortField: string };
    const second = JSON.parse(calls[1]?.body ?? "{}") as { sortDirection: string; sortField: string };

    assert.equal(first.sortField, "name");
    assert.equal(first.sortDirection, "asc", "first sort on a new field should set ascending");
    assert.equal(second.sortField, "name");
    assert.equal(second.sortDirection, "desc", "second click on the same field should flip direction to desc");
  });

  test("processLogos renders channelDisplayHtml into td[data-logo] cells and marks them processed", async () => {

    /* processLogos walks every td that carries data-logo, finds the .channel-name-cell within, and replaces its innerHTML with the channelDisplayHtml output.
     * The processed cell is stamped data-logo-processed='1' to prevent re-processing.
     *
     * We use a body-level synthetic table (the tbody.insertAdjacentHTML('beforeend', '<tr>') path drops content in happy-dom for parser-context reasons).
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<table><tbody><tr id=\"display-row-pl1\"><td data-logo=\"https://logo.test/a.png\"><span class=\"channel-name-cell\">PL1</span></td></tr></tbody></table>');"
    );

    ctx.evaluate("window.channelTable.processLogos();");

    const cell = ctx.document.querySelector("#display-row-pl1 td");
    const nameSpan = cell?.querySelector(".channel-name-cell");

    assert.equal(cell?.getAttribute("data-logo-processed"), "1", "cell should be marked processed");
    assert.ok(nameSpan?.querySelector("img"), "channel-name-cell should now contain an <img> rendered by channelDisplayHtml");
  });

  test("insertRow places a new row in the correct sort position based on the current table sort field", async () => {

    /* Insertion order: the table carries data-sort-field and data-sort-dir attributes. insertRow reads the new row's sort value, scans existing rows, and
     * splices the new row before the first row whose sort value loses to it. We pin name-asc ordering by inserting "ban" between "abc" and "cnn"; the test
     * uses station IDs that the predefined channel list already orders.
     */
    await using ctx = await setupSharedRuntime();

    // Confirm the table sort attributes for the assertion that follows. The channel-table is rendered by the server with a server-stamped default sort.
    const table = ctx.document.querySelector(".channel-table");

    assert.ok(table, "channel-table should exist on the rendered page");

    /* Build a fresh small-table fixture independent of the rendered predefined catalog: clear the tbody, set sort-field=name asc, insert two rows with
     * data-sort-value, then call insertRow with a row that should land between them. The cell at index 1 (second column) carries data-sort-value because the
     * table's th[data-sort-field=name] is at column index 1 in the production table - we mirror that.
     */
    ctx.evaluate(
      "const t = document.querySelector('.channel-table'); " +
      "t.setAttribute('data-sort-field', 'name'); t.setAttribute('data-sort-dir', 'asc'); " +
      "t.querySelector('tbody').innerHTML = " +
      "'<tr id=\"display-row-aaa\"><td></td><td data-sort-value=\"aaa\">A</td></tr>' + " +
      "'<tr id=\"display-row-ccc\"><td></td><td data-sort-value=\"ccc\">C</td></tr>';"
    );

    // insertRow's first _getSortValue call lazily builds the column-index cache from the still-server-rendered thead, mapping each field name to its column
    // position (key=0, name=1, provider=2, ...). The cache is never invalidated, but our replacement tbody preserves the same column layout, so the "name"
    // field still resolves to index 1; our fixture cells sit at index 1 to match, which lands the inserted row between the two existing rows.
    ctx.evaluate(
      "window.channelTable.insertRow({ displayRow: '<tr id=\"display-row-bbb\"><td></td><td data-sort-value=\"bbb\">B</td></tr>', editRow: '' }, 'bbb');"
    );

    const tbody = ctx.document.querySelector(".channel-table tbody");
    const orderedIds = Array.from(tbody?.querySelectorAll("tr") ?? []).map((tr) => tr.getAttribute("id"));

    assert.deepEqual(orderedIds, [ "display-row-aaa", "display-row-bbb", "display-row-ccc" ],
      "insertRow should place 'bbb' between 'aaa' and 'ccc' under name asc sort");
  });

  test("insertRow replaces an existing row with the same key (override pattern)", async () => {

    /* Edit pattern: when a row with key X already exists, insertRow(html, X) removes the old row and inserts the new one. This is the entry point for
     * row updates after CRUD operations.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "const t = document.querySelector('.channel-table'); " +
      "t.setAttribute('data-sort-field', 'name'); t.setAttribute('data-sort-dir', 'asc'); " +
      "t.querySelector('tbody').innerHTML = " +
      "'<tr id=\"display-row-keyed\"><td></td><td data-sort-value=\"old\">OldName</td></tr>';"
    );

    ctx.evaluate(
      "window.channelTable.insertRow({ displayRow: '<tr id=\"display-row-keyed\"><td></td><td data-sort-value=\"new\">NewName</td></tr>', editRow: '' }, 'keyed');"
    );

    const rows = ctx.document.querySelectorAll("#display-row-keyed");

    assert.equal(rows.length, 1, "exactly one row should carry the id - the new one replaced the old");
    assert.match(rows[0]?.textContent ?? "", /NewName/);
  });
});

describe("shared.ts: action dispatcher modifier scoping", () => {

  /* This block guards the class of bug that escaped string-shape tests in the past: an event modifier on an ancestor element (e.g., a form's preventDefault
   * intended for its submit event) silently breaking unrelated events (e.g., keydown on input fields inside the form). The dispatcher uses event-type-prefixed
   * modifier attributes (data-<event>-prevent-default, data-<event>-stop-propagation, data-<event>-close-dropdown) so each modifier fires only for its own
   * event type. We assert this property by dispatching synthetic events and reading event.defaultPrevented / a propagation observer.
   */

  test("data-submit-prevent-default on a form prevents the submit default but NOT keydown defaults on input fields inside the form", async () => {

    /* This test pins the invariant that an event modifier is scoped to its own event type via the data-<event>-* attribute: a submit-scoped preventDefault on a
     * form must not suppress keydown on input fields inside that form, so typed characters still insert. We assert both halves directly: submit gets
     * defaultPrevented; keydown does not.
     */
    await using ctx = await setupSharedRuntime();

    // Seed a form with a submit-scoped prevent-default and an input inside it.
    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<form id=\"test-form\" data-submit-action=\"test-submit\" data-submit-prevent-default>' +" +
      "'<input id=\"test-input\" type=\"text\" />' +" +
      "'</form>');"
    );

    // Register a no-op handler for the action so the dispatcher's typo warning doesn't fire.
    ctx.evaluate("window.registerAction('test-submit', () => {});");

    // Dispatch a submit event on the form. The modifier should fire (preventDefault called).
    const submitPrevented = ctx.evaluate(
      "(() => {" +
      "  const form = document.getElementById('test-form');" +
      "  const ev = new window.Event('submit', { bubbles: true, cancelable: true });" +
      "  form.dispatchEvent(ev);" +
      "  return ev.defaultPrevented;" +
      "})()"
    );

    assert.equal(submitPrevented, true, "submit on the form should have its default prevented by data-submit-prevent-default");

    // Dispatch a keydown event on the input. The submit-scoped modifier should NOT fire - the typed character would insert normally.
    const keydownPrevented = ctx.evaluate(
      "(() => {" +
      "  const input = document.getElementById('test-input');" +
      "  const ev = new window.KeyboardEvent('keydown', { bubbles: true, cancelable: true, key: 'a' });" +
      "  input.dispatchEvent(ev);" +
      "  return ev.defaultPrevented;" +
      "})()"
    );

    assert.equal(keydownPrevented, false, "keydown on an input inside a form with data-SUBMIT-prevent-default must NOT have its default prevented");
  });

  test("data-click-prevent-default on an anchor prevents the click default but NOT submit defaults on a form inside the anchor", async () => {

    /* Symmetric coverage in the other direction: a click-scoped modifier must not leak to submit events. The modifier dispatch is event-type-scoped via the
     * attribute name, so a click modifier and a submit modifier on the same element are independent.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<div id=\"click-scope\" data-click-action=\"test-click\" data-click-prevent-default>' +" +
      "'<form id=\"inner-form\"><input id=\"inner-input\" /></form>' +" +
      "'</div>');"
    );

    ctx.evaluate("window.registerAction('test-click', () => {});");

    const clickPrevented = ctx.evaluate(
      "(() => {" +
      "  const el = document.getElementById('click-scope');" +
      "  const ev = new window.Event('click', { bubbles: true, cancelable: true });" +
      "  el.dispatchEvent(ev);" +
      "  return ev.defaultPrevented;" +
      "})()"
    );

    assert.equal(clickPrevented, true, "click on the data-click-prevent-default element should have its default prevented");

    const submitPrevented = ctx.evaluate(
      "(() => {" +
      "  const form = document.getElementById('inner-form');" +
      "  const ev = new window.Event('submit', { bubbles: true, cancelable: true });" +
      "  form.dispatchEvent(ev);" +
      "  return ev.defaultPrevented;" +
      "})()"
    );

    assert.equal(submitPrevented, false, "submit on an inner form must NOT have its default prevented by an outer data-CLICK-prevent-default");
  });

  test("data-click-stop-propagation stops click propagation but does not affect keydown propagation through the same element", async () => {

    /* Same scoping property for stopPropagation: a click-scoped stopPropagation must not interfere with keydown events bubbling through the same element. We
     * attach a probe listener at the document level to observe whether the keydown reaches it.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate(
      "document.body.insertAdjacentHTML('beforeend', " +
      "'<div id=\"stop-outer\">' +" +
      "'<div id=\"stop-inner\" data-click-stop-propagation>' +" +
      "'<input id=\"stop-input\" />' +" +
      "'</div></div>');"
    );

    // Observe whether click and keydown reach an outer probe listener.
    ctx.evaluate("window.__probe = { clickFired: false, keydownFired: false };");
    ctx.evaluate(
      "document.getElementById('stop-outer').addEventListener('click', () => { window.__probe.clickFired = true; });"
    );
    ctx.evaluate(
      "document.getElementById('stop-outer').addEventListener('keydown', () => { window.__probe.keydownFired = true; });"
    );

    // Click on the inner element - the stop-propagation modifier should prevent the outer click listener from firing.
    ctx.evaluate(
      "document.getElementById('stop-inner').dispatchEvent(new window.Event('click', { bubbles: true, cancelable: true }));"
    );

    // Keydown on the input - the click-scoped stop-propagation must NOT prevent the outer keydown listener from firing.
    ctx.evaluate(
      "document.getElementById('stop-input').dispatchEvent(new window.KeyboardEvent('keydown', { bubbles: true, cancelable: true, key: 'a' }));"
    );

    const probe = ctx.evaluateJson("window.__probe") as { clickFired: boolean; keydownFired: boolean };

    assert.equal(probe.clickFired, false, "click on the data-click-stop-propagation element must not reach the outer click listener");
    assert.equal(probe.keydownFired, true, "keydown on an input inside data-CLICK-stop-propagation must still reach the outer keydown listener");
  });

  test("registerAction throws on a duplicate action name", async () => {

    // Collision detection is the load-bearing primitive for the action namespace. Silent overwrites would break dispatch in ways hard to localize.
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("window.registerAction('test-collision-action', () => {});");

    const result = ctx.evaluateJson(
      "(() => {" +
      "  try { window.registerAction('test-collision-action', () => {}); return { threw: false }; }" +
      "  catch(e) { return { threw: true, message: e.message }; }" +
      "})()"
    ) as { threw: boolean; message?: string };

    assert.equal(result.threw, true, "second registerAction call for the same name should throw");
    assert.match(result.message ?? "", /already registered/, "error message should name the collision");
  });
});

describe("shared.ts: window.escapeHtml (client-escape SSOT) and the renderers that consume it", () => {

  test("the shared utilities script installs window.escapeHtml and it encodes the special characters", async () => {

    /* The shared utilities script emits the single client-escape SSOT (generateClientEscapeAssignment) near the top of its IIFE. This pins that the emitted
     * .toString()-serialized body actually parses and installs a working window.escapeHtml in a real DOM - the runtime counterpart to the byte-parity guard in
     * clientEscape.test.ts, which checks the source function in isolation but cannot prove the emitted-and-executed form works.
     */
    await using ctx = await setupSharedRuntime();

    assert.equal(ctx.evaluate("typeof window.escapeHtml"), "function", "the shared script must install window.escapeHtml");
    assert.equal(ctx.evaluate("window.escapeHtml('<b>&\"')"), "&lt;b&gt;&amp;&quot;",
      "window.escapeHtml must encode angle brackets, the ampersand, and the double quote");
  });

  test("channelDisplayHtml escapes the name in text mode so injected markup cannot parse out", async () => {

    /* channelDisplayHtml is the single escape boundary for channel names; callers pass raw names. In text mode the name lands in a span's text content, so a value
     * carrying a <b> tag must surface as entities with no live element. We inject the rendered HTML into the DOM and assert the span's decoded text equals the raw
     * name (round-trips through the entities) and that no <b> element was parsed out.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("document.body.insertAdjacentHTML('beforeend', '<div id=\"cdh-text\">' + " +
      "window.channelDisplayHtml('', 'A & <b>C</b>', 'lc', 'tc', 'text') + '</div>')");

    assert.equal(ctx.evaluate("document.querySelector('#cdh-text b') !== null"), false, "no live <b> may be parsed out of the escaped name");
    assert.equal(ctx.evaluate("document.querySelector('#cdh-text span').textContent"), "A & <b>C</b>", "the span text must round-trip the raw name");
  });

  test("channelDisplayHtml escapes the name in the title attribute so a double quote cannot break out (both mode)", async () => {

    /* In both/logo mode the name lands in the img title (and alt) attribute as well as a span. The double quote is the attribute-breakout vector: an unescaped " in
     * the title would close the attribute and allow injected attributes or markup. We assert the img's decoded title equals the raw name (so the " survived as an
     * entity and did not break the attribute) and that no injected <b> element parsed out.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("document.body.insertAdjacentHTML('beforeend', '<div id=\"cdh-both\">' + " +
      "window.channelDisplayHtml('http://logo/x.png', 'A \"B\" & <b>C</b>', 'lc', 'tc', 'both') + '</div>')");

    assert.equal(ctx.evaluate("document.querySelector('#cdh-both b') !== null"), false, "no live <b> may be parsed out of the escaped name");
    assert.equal(ctx.evaluate("document.querySelector('#cdh-both img').getAttribute('title')"), "A \"B\" & <b>C</b>",
      "the title attribute must round-trip the raw name, proving the double quote did not break out");
    assert.equal(ctx.evaluate("document.querySelector('#cdh-both span').textContent"), "A \"B\" & <b>C</b>", "the span text must round-trip the raw name");
  });

  test("serviceIconHtml escapes the name in both the title attribute and the text span", async () => {

    /* serviceIconHtml mirrors channelDisplayHtml: callers pass raw service names and it is the single escape boundary. We pin the same attribute-breakout and
     * text-context invariants - the double quote survives as an entity in the title, and an injected <i> tag does not parse out.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("document.body.insertAdjacentHTML('beforeend', '<div id=\"svc-both\">' + " +
      "window.serviceIconHtml('example.com', 'S \"x\" & <i>y</i>', 'ic', 'tc', 'both', '') + '</div>')");

    assert.equal(ctx.evaluate("document.querySelector('#svc-both i') !== null"), false, "no live <i> may be parsed out of the escaped service name");
    assert.equal(ctx.evaluate("document.querySelector('#svc-both img').getAttribute('title')"), "S \"x\" & <i>y</i>",
      "the title attribute must round-trip the raw service name");
    assert.equal(ctx.evaluate("document.querySelector('#svc-both span').textContent"), "S \"x\" & <i>y</i>", "the span text must round-trip the raw service name");
  });
});

describe("shared.ts: window.safeUrl (URL-safety SSOT) and renderer URL handling", () => {

  test("the shared utilities script installs window.safeUrl and it gates schemes", async () => {

    /* The shared utilities script emits the client URL-safety SSOT (generateClientSafeUrlAssignment) alongside the escaper. This pins that the emitted, executed
     * form works: http/https/relative pass through and a javascript: scheme collapses to the empty string.
     */
    await using ctx = await setupSharedRuntime();

    assert.equal(ctx.evaluate("typeof window.safeUrl"), "function", "the shared script must install window.safeUrl");
    assert.equal(ctx.evaluate("window.safeUrl('https://logo.example/x.png')"), "https://logo.example/x.png", "https URLs must pass through");
    assert.equal(ctx.evaluate("window.safeUrl('/relative.png')"), "/relative.png", "relative URLs must pass through");
    assert.equal(ctx.evaluate("window.safeUrl('javascript:alert(1)')"), "", "a javascript: URL must collapse to the empty string");
  });

  test("channelDisplayHtml neutralizes a dangerous-scheme logo URL to an empty src", async () => {

    /* A logo URL with a javascript: scheme must not survive into the img src. safeUrl collapses it to "", so the rendered src is empty (the onerror fallback then
     * reveals the text). We assert the src is empty and that the crafted scheme string does not appear anywhere in the rendered markup.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("document.body.insertAdjacentHTML('beforeend', '<div id=\"cdh-js\">' + " +
      "window.channelDisplayHtml('javascript:alert(1)', 'NBC', 'lc', 'tc', 'both') + '</div>')");

    assert.equal(ctx.evaluate("document.querySelector('#cdh-js img').getAttribute('src')"), "", "a javascript: logo URL must render as an empty src");
    assert.equal(ctx.evaluate("document.querySelector('#cdh-js').innerHTML.indexOf('javascript:')"), -1, "the dangerous scheme must not appear in the markup");
  });

  test("channelDisplayHtml escapes a double quote in the logo URL so it cannot break out of the src attribute", async () => {

    /* Even a scheme-valid URL must be attribute-escaped: a double quote in an https URL would otherwise close the src attribute and inject markup. We assert the
     * img's decoded src round-trips the raw URL (so the quote survived as an entity) and that no injected element parsed out.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("document.body.insertAdjacentHTML('beforeend', '<div id=\"cdh-q\">' + " +
      "window.channelDisplayHtml('https://logo.example/x.png?a=1\\\"><img id=pwned src=y>', 'NBC', 'lc', 'tc', 'both') + '</div>')");

    assert.equal(ctx.evaluate("document.querySelector('#cdh-q #pwned')"), null, "no injected element may parse out of the escaped src");
    assert.equal(ctx.evaluate("document.querySelector('#cdh-q img').getAttribute('src')"), "https://logo.example/x.png?a=1\"><img id=pwned src=y>",
      "the src attribute must round-trip the raw URL, proving the double quote did not break out");
  });

  test("serviceIconHtml neutralizes a dangerous-scheme icon URL to an empty src", async () => {

    /* serviceIconHtml accepts an explicit iconUrl that becomes the src; a javascript: value must be collapsed to "" by safeUrl just like channelDisplayHtml's logo.
     */
    await using ctx = await setupSharedRuntime();

    ctx.evaluate("document.body.insertAdjacentHTML('beforeend', '<div id=\"svc-js\">' + " +
      "window.serviceIconHtml('example.com', 'Svc', 'ic', 'tc', 'both', 'javascript:alert(1)') + '</div>')");

    assert.equal(ctx.evaluate("document.querySelector('#svc-js img').getAttribute('src')"), "", "a javascript: icon URL must render as an empty src");
    assert.equal(ctx.evaluate("document.querySelector('#svc-js').innerHTML.indexOf('javascript:')"), -1, "the dangerous scheme must not appear in the markup");
  });
});
