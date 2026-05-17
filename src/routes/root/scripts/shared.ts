/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * shared.ts: Shared client-side utilities for all tabs. This script runs before any tab-specific script so that cross-tab functions (toast notifications,
 * dropdown menus, channel display rendering) are available during both initialization and event handling.
 */
import { ACTIONS } from "../../clientActions.ts";

/**
 * Generates the shared utilities script block containing cross-tab client-side functions. Runs before all tab-specific scripts to eliminate execution order
 * dependencies. Functions are exposed on window for access from any script context or inline event handler.
 * @returns HTML script block with shared utility functions.
 */
export function generateSharedUtilitiesScript(): string {

  return [
    "<script>",
    "(function() {",

    /* Action dispatcher. The single project-wide primitive for event handling. Subsystems register named handlers via window.registerAction(name, handler), and
     * one document-level delegated listener routes click / change / keydown / submit events to the registered handler. Triggers declare their intent on the
     * element via data-<event>-action="name" (with optional sibling data-* attributes for per-instance arguments).
     *
     * Event mechanics are also declarative AND event-type-scoped, symmetric with actions: data-<event>-prevent-default calls event.preventDefault(),
     * data-<event>-stop-propagation calls event.stopPropagation(), data-<event>-close-dropdown closes any open dropdown menus. Each modifier fires only for
     * the event type encoded in its attribute name, so a <form data-submit-prevent-default> prevents the form's submit default without affecting keydown
     * events on input fields inside the form. The event-type-in-the-name shape is self-documenting and uniform with data-<event>-action: one mental model
     * for "when does this fire", expressed entirely in the attribute name.
     *
     * The registration API enforces uniqueness - a second registerAction call for an already-registered name throws, so silent overwrites are impossible.
     * When a click/change/etc. lands on an element carrying a data-<event>-action whose value is not registered, the dispatcher logs a console warning
     * naming the missing action - typos surface fast.
     */
    "  const actionHandlers = new Map();",
    "  window.registerAction = (name, handler) => {",
    "    if(actionHandlers.has(name)) {",
    "      throw new Error('Action \"' + name + '\" is already registered. Action names must be globally unique.');",
    "    }",
    "    actionHandlers.set(name, handler);",
    "  };",
    /* Modifier handling runs in the capture phase so stopPropagation actually stops further propagation. Capture phase fires from document down to target -
     * before any element-level listener between the document and the trigger - so stopPropagation called here prevents intermediate bubble-phase listeners
     * from firing. preventDefault and close-dropdown are side-effect modifiers that work equally well in either phase but live here for cohesion: every
     * declarative modifier flows through one listener. The selector is built from event.type so each modifier fires only for its own event type.
     */
    "  const dispatchModifiers = (event) => {",
    "    const prefix = 'data-' + event.type + '-';",
    "    const modTarget = event.target.closest('[' + prefix + 'prevent-default], [' + prefix + 'stop-propagation], [' + prefix + 'close-dropdown]');",
    "    if(!modTarget) return;",
    "    if(modTarget.hasAttribute(prefix + 'prevent-default')) event.preventDefault();",
    "    if(modTarget.hasAttribute(prefix + 'stop-propagation')) event.stopPropagation();",
    "    if(modTarget.hasAttribute(prefix + 'close-dropdown') && window.dropdowns) window.dropdowns.close();",
    "  };",
    /* Action dispatch runs in the bubble phase so element-level listeners get a chance to fire first. Action handlers see the event after the element-level
     * processing has run. Missing handlers log a console warning so typos surface fast.
     */
    "  const dispatchAction = (event) => {",
    "    const attrName = 'data-' + event.type + '-action';",
    "    const target = event.target.closest('[' + attrName + ']');",
    "    if(!target) return;",
    "    const action = target.getAttribute(attrName);",
    "    const handler = actionHandlers.get(action);",
    "    if(handler) {",
    "      handler(target, event);",
    "    } else {",
    "      console.warn('No handler registered for ' + attrName + '=\"' + action + '\".');",
    "    }",
    "  };",
    "  for(const type of [ 'click', 'change', 'keydown', 'submit' ]) {",
    "    document.addEventListener(type, dispatchModifiers, { capture: true });",
    "    document.addEventListener(type, dispatchAction);",
    "  }",

    // Show a toast notification. Auto-dismiss durations: success/info = 5s, warning = 8s, error = no auto-dismiss. Optional action: { label, onclick } appends an
    // inline button between the message text and the close button.
    "  function showToast(message, type, duration, action) {",
    "    const container = document.getElementById('toast-container');",
    "    if(!container) return;",
    "    const toast = document.createElement('div');",
    "    toast.className = 'toast ' + (type || 'info');",
    "    toast.textContent = message;",
    "    toast.setAttribute('role', ((type === 'error') || (type === 'warning')) ? 'alert' : 'status');",
    "    if(action && action.label) {",
    "      const actionBtn = document.createElement('button');",
    "      actionBtn.type = 'button';",
    "      actionBtn.className = 'toast-action';",
    "      actionBtn.textContent = action.label;",
    "      actionBtn.onclick = () => { if(action.onclick) action.onclick(); dismissToast(toast); };",
    "      toast.appendChild(actionBtn);",
    "    }",
    "    const closeBtn = document.createElement('button');",
    "    closeBtn.type = 'button';",
    "    closeBtn.className = 'toast-close';",
    "    closeBtn.textContent = '\\u00d7';",
    "    closeBtn.setAttribute('aria-label', 'Dismiss');",
    "    closeBtn.onclick = () => { dismissToast(toast); };",
    "    toast.appendChild(closeBtn);",
    "    container.appendChild(toast);",
    "    const ms = (duration !== undefined) ? duration : ((type === 'error') ? 0 : ((type === 'warning') ? 8000 : 5000));",
    "    if(ms > 0) { setTimeout(() => { dismissToast(toast); }, ms); }",
    "  }",

    "  window.showToast = showToast;",

    /* Single source of truth for extracting a user-facing error message from a server error response. The envelope emits two shapes: { error: string } for
     * top-level failures (validation, not-found, conflict, server error) and { errors: Record<field, message> } for form submissions that surface multiple
     * field errors at once. Callers pass a fallback string for the rare case where neither field is present. Every client that handles a !success response
     * flows through this helper so error-reading logic lives in one place instead of drifting per call site.
     */
    "  window.extractErrorMessage = (response, fallback) => {",
    "    if(response && response.errors) {",
    "      const entries = Object.entries(response.errors);",
    "      if(entries.length > 0) {",
    "        return entries.map(([ field, message ]) => field + ': ' + message).join(', ');",
    "      }",
    "    }",
    "    return (response && (response.error || response.message)) || fallback;",
    "  };",

    // Dismiss a toast with slide-out animation.
    "  function dismissToast(toast) {",
    "    if(toast.classList.contains('toast-exit')) return;",
    "    toast.classList.add('toast-exit');",
    "    toast.addEventListener('animationend', () => { toast.remove(); });",
    "  }",

    /* Client-side namespace SSOT for all dropdown operations - close, and before-close hook registration. Private state (the hook array and both functions)
     * lives in module-scope closures so the scroll/resize listener references stay stable across open/close cycles. The namespace object exposes the two
     * operations as property references into those module-scope functions, which keeps both entries structurally symmetric and preserves identity for
     * addEventListener / removeEventListener. Consumers call dropdowns.close() to close all open menus and dropdowns.addHook(fn) to register a before-close
     * callback - for example, the inline tag editor registers a batch-save callback so pending changes flush on any close path (click outside, scroll,
     * resize, next toggle). Registration is de-duplicated so accidental double-registration is a no-op.
     *
     * This is the "closure-backed" variant of the client-side namespace pattern. The channelTable namespace uses the this.*-backed
     * variant because its methods are only ever called as methods, never as detached listener references; dropdowns cannot use that variant because close()
     * must survive being passed to addEventListener and later matched in removeEventListener.
     */
    "  const dropdownHooks = [];",
    "  function addDropdownHook(fn) {",
    "    if(!dropdownHooks.includes(fn)) dropdownHooks.push(fn);",
    "  }",
    "  function closeAllDropdowns() {",
    "    for(const hook of dropdownHooks) hook();",
    "    const menus = document.querySelectorAll('.dropdown-menu.show');",
    "    for(const menu of menus) menu.classList.remove('show');",
    "    window.removeEventListener('scroll', closeAllDropdowns, true);",
    "    window.removeEventListener('resize', closeAllDropdowns);",
    "  }",
    "  window.dropdowns = {",
    "    addHook: addDropdownHook,",
    "    close: closeAllDropdowns",
    "  };",

    /* Safe localStorage wrappers. localStorage access can throw in private browsing mode or when storage quota is exceeded. Centralizing the try/catch here
     * means every call site treats localStorage as best-effort persistence without repeating the same try/catch at every site. Failures are silently ignored
     * because they are not actionable by the client - there is no way to recover quota or exit private browsing from JavaScript, and log noise from every
     * subtab switch or page load would dominate useful diagnostics. Callers that need to react to failure should use the raw localStorage API directly.
     */
    "  window.safeStorageGet = (key) => {",
    "    try { return localStorage.getItem(key); }",
    "    catch(e) { return null; }",
    "  };",
    "  window.safeStorageSet = (key, value) => {",
    "    try { localStorage.setItem(key, value); }",
    "    catch(e) {}",
    "  };",
    "  window.safeStorageRemove = (key) => {",
    "    try { localStorage.removeItem(key); }",
    "    catch(e) {}",
    "  };",

    /* Persist channel table display preferences to the server. Single source of truth for POSTs to /config/channels/display-prefs so every call site (sort,
     * column visibility, and any future preference) shares one fetch path and one error-handling decision. Fire-and-forget - the caller does not await the
     * round-trip because the client state is already updated and the persist is a best-effort sync to the server.
     */
    "  window.persistDisplayPrefs = (body) => {",
    "    fetch('/config/channels/display-prefs', {",
    "      body: JSON.stringify(body),",
    "      headers: { 'Content-Type': 'application/json' },",
    "      method: 'POST'",
    "    }).catch((err) => { console.warn('Display preferences failed to persist.', err); });",
    "  };",

    /* Subtab switching factory. Creates a reusable switch function parameterized by CSS selectors, storage key, and hash format, and attaches click event
     * handlers to all matching buttons so consumers need not wire the click-to-switch binding separately. The returned function handles button active states,
     * panel visibility, localStorage persistence (via safeStorageSet), and URL hash updates. An optional onSwitch callback lets the caller inject per-tab-category
     * logic (e.g., hiding settings buttons on the backup subtab in config). Used by both the Config and Channels tab subtab systems.
     */
    "  window.createSubtabSwitcher = (config) => {",
    "    const switchFn = (subtab, updateUrl) => {",
    "      for(const btn of document.querySelectorAll(config.btnSelector)) {",
    "        btn.classList.remove('active');",
    "        btn.setAttribute('aria-selected', 'false');",
    "        if(btn.getAttribute(config.dataAttr) === subtab) {",
    "          btn.classList.add('active');",
    "          btn.setAttribute('aria-selected', 'true');",
    "        }",
    "      }",
    "      for(const panel of document.querySelectorAll(config.panelSelector)) {",
    "        panel.classList.remove('active');",
    "        if(panel.id === (config.panelPrefix + subtab)) panel.classList.add('active');",
    "      }",
    "      if(config.onSwitch) config.onSwitch(subtab);",
    "      safeStorageSet(config.storageKey, subtab);",
    "      if(updateUrl !== false) {",
    "        const newHash = config.hashFn(subtab);",
    "        if(window.location.hash !== newHash) window.location.hash = newHash;",
    "      }",
    "    };",
    "    for(const btn of document.querySelectorAll(config.btnSelector)) {",
    "      btn.addEventListener('click', function() { switchFn(this.getAttribute(config.dataAttr)); });",
    "    }",
    "    return switchFn;",
    "  };",

    /* Initialize a subtab system on page load. Reads the initial subtab from the hash (set by the main tab script) or localStorage, validates the button
     * exists in the DOM, and calls the switch function to activate it. Called once per subtab system at the end of the IIFE.
     */
    "  window.initSubtab = (config) => {",
    "    const initial = config.hashVar ?? safeStorageGet(config.storageKey);",
    "    if(initial && document.querySelector(config.btnSelector + '[' + config.dataAttr + '=\"' + initial + '\"]')) {",
    "      config.switchFn(initial, false);",
    "    }",
    "  };",

    // Copy text to the clipboard and show a toast. Uses the modern Clipboard API when available (secure contexts), falling back to execCommand for plain HTTP
    // access via IP address.
    "  window.copyToClipboard = async (text, successMessage) => {",
    "    if(navigator.clipboard?.writeText) {",
    "      try { await navigator.clipboard.writeText(text); showToast(successMessage, 'success'); }",
    "      catch(e) { showToast('Failed to copy to clipboard.', 'error'); }",
    "    } else {",
    "      const ta = document.createElement('textarea');",
    "      ta.value = text;",
    "      ta.style.position = 'fixed';",
    "      ta.style.opacity = '0';",
    "      document.body.appendChild(ta);",
    "      ta.select();",
    "      try { document.execCommand('copy'); showToast(successMessage, 'success'); }",
    "      catch(e) { showToast('Failed to copy to clipboard.', 'error'); }",
    "      document.body.removeChild(ta);",
    "    }",
    "  };",

    // Layer 1: Portal positioning. Positions a menu element below an anchor with viewport edge clamping and above-anchor flip. Uses position: absolute with
    // scroll offsets for reliable behavior at all browser zoom levels. The menu must be visible (display: block) for offsetWidth/offsetHeight measurement.
    // This is the single source of truth for all portal positioning math.
    "  function positionPortal(menu, anchor) {",
    "    const rect = anchor.getBoundingClientRect();",
    "    let top = rect.bottom + 2;",
    "    let left = rect.left;",
    "    if((left + menu.offsetWidth) > (window.innerWidth - 4)) left = rect.right - menu.offsetWidth;",
    "    if(left < 4) left = 4;",
    "    if((top + menu.offsetHeight) > (window.innerHeight - 4)) top = rect.top - menu.offsetHeight - 2;",
    "    const sx = window.scrollX || 0;",
    "    const sy = window.scrollY || 0;",
    "    menu.style.top = (top + sy) + 'px';",
    "    menu.style.left = (left + sx) + 'px';",
    "  }",

    "  window.positionPortal = positionPortal;",

    // Layer 2: Dropdown toggle with lifecycle hooks. Manages portal append, toggle state, scroll/resize dismissal, and optional lifecycle callbacks. The
    // zero-argument form (toggleDropdown(btn)) works unchanged for standard dropdowns. The options form adds: menu (explicit menu element), onOpen (called
    // after show but before positioning so content can be set and measured correctly for viewport clamping).
    "  window.toggleDropdown = (btn, opts) => {",
    "    const o = opts || {};",
    "    const menu = btn._portalMenu || o.menu || btn.nextElementSibling;",
    "    if(!menu) return;",
    "    const isOpen = menu.classList.contains('show');",
    "    closeAllDropdowns();",
    "    if(isOpen) return;",
    "    btn._portalMenu = menu;",
    "    if(!menu._portaled) {",
    "      document.body.appendChild(menu);",
    "      menu.style.position = 'absolute';",
    "      menu.style.marginTop = '0';",
    "      menu._portaled = true;",
    "    }",
    "    menu.classList.add('show');",
    "    if(o.onOpen) o.onOpen(menu);",
    "    positionPortal(menu, btn);",
    "    window.addEventListener('scroll', closeAllDropdowns, true);",
    "    window.addEventListener('resize', closeAllDropdowns);",
    "  };",

    // Shared channel display renderer with three modes. The mode parameter controls presentation: 'logo' (default) shows the logo with text as an onerror
    // fallback, 'both' shows the logo and text side by side with onerror hiding the broken image, 'text' shows only the text and ignores the logo URL.
    "  window.channelDisplayHtml = (logoUrl, name, logoClass, textClass, mode) => {",
    "    const m = mode || 'logo';",
    "    if((m === 'text') || !logoUrl) {",
    "      return '<span class=\"' + textClass + '\">' + name + '</span>';",
    "    }",
    "    if(m === 'both') {",
    "      return '<img src=\"' + logoUrl + '\" class=\"' + logoClass + '\" alt=\"\" title=\"' + name + '\" ' +",
    "        'onerror=\"imgFallback(this)\">' +",
    "        '<span class=\"' + textClass + '\">' + name + '</span>';",
    "    }",
    "    return '<img src=\"' + logoUrl + '\" class=\"' + logoClass + '\" alt=\"' + name + '\" title=\"' + name + '\" ' +",
    "      'onerror=\"imgFallback(this)\">' +",
    "      '<span class=\"' + textClass + '\" style=\"display:none\">' + name + '</span>';",
    "  };",

    // Image fallback handler. Reads pipe-separated fallback URLs from a data-fallbacks attribute and tries each in sequence on error. When all fallbacks are
    // exhausted, hides the image and reveals the text sibling if it was hidden (logo mode).
    "  window.imgFallback = (img) => {",
    "    const fallbacks = (img.getAttribute('data-fallbacks') || '').split('|').filter(Boolean);",
    "    const idx = parseInt(img.getAttribute('data-fb-idx') || '0', 10);",
    "    if(idx < fallbacks.length) {",
    "      img.setAttribute('data-fb-idx', String(idx + 1));",
    "      img.src = fallbacks[idx];",
    "    } else {",
    "      img.style.display = 'none';",
    "      const sib = img.nextElementSibling;",
    "      if(sib && (sib.style.display === 'none')) sib.style.display = 'inline';",
    "    }",
    "  };",

    // Service icon renderer with three modes, mirroring channelDisplayHtml. The icon source chain is: iconUrl (if specified) -> Apple touch icon -> favicon.
    // Fallback URLs are stored in a data-fallbacks attribute and processed by the shared imgFallback handler.
    "  window.serviceIconHtml = (domain, name, iconClass, textClass, mode, iconUrl) => {",
    "    const m = mode || 'logo';",
    "    if((m === 'text') || !domain) {",
    "      return '<span class=\"' + textClass + '\">' + name + '</span>';",
    "    }",
    "    const touchIcon = 'https://' + domain + '/apple-touch-icon.png';",
    "    const favicon = 'https://' + domain + '/favicon.ico';",
    "    const src = iconUrl || touchIcon;",
    "    const fallbacks = (iconUrl ? [ touchIcon, favicon ] : [ favicon ]).join('|');",
    "    if(m === 'both') {",
    "      return '<img src=\"' + src + '\" class=\"' + iconClass + '\" alt=\"\" title=\"' + name + '\" ' +",
    "        'data-fallbacks=\"' + fallbacks + '\" onerror=\"imgFallback(this)\">' +",
    "        '<span class=\"' + textClass + '\">' + name + '</span>';",
    "    }",
    "    return '<img src=\"' + src + '\" class=\"' + iconClass + '\" alt=\"' + name + '\" title=\"' + name + '\" ' +",
    "      'data-fallbacks=\"' + fallbacks + '\" onerror=\"imgFallback(this)\">' +",
    "      '<span class=\"' + textClass + '\" style=\"display:none\">' + name + '</span>';",
    "  };",

    // Process service display spans. Finds all .provider-display elements and renders them via serviceIconHtml in 'both' mode. Called on page load and after
    // any DOM mutation that introduces new service display elements (chip rebuild, filter updates).
    "  window.processServiceDisplays = () => {",
    "    const els = document.querySelectorAll('.provider-display');",
    "    for(const el of els) {",
    "      if(el.getAttribute('data-processed')) continue;",
    "      const domain = el.getAttribute('data-domain') || '';",
    "      const iconUrl = el.getAttribute('data-icon-url') || '';",
    "      const name = el.textContent || '';",
    "      const sm = el.hasAttribute('data-sm');",
    "      el.innerHTML = serviceIconHtml(domain, name, sm ? 'provider-icon-sm' : 'provider-icon',",
    "        sm ? 'provider-chip-text' : 'provider-icon-text', 'both', iconUrl);",
    "      el.setAttribute('data-processed', '1');",
    "    }",
    "  };",

    // Wizard controller factory. Creates a reusable controller for stepped wizard modals. Handles step state management, step indicator DOM updates, navigation
    // with validation, error display, and open/close lifecycle. Each wizard instance provides its own renderStep and validateStep callbacks. The controller exposes
    // a state object for the caller to store wizard-specific data (e.g., selected providers, form values). State is reset to {} on close.
    //
    // Config shape: { contentId, errorId?, modalId, onClose?, onRender, onValidate, stepCount, stepsId?, titleId? }
    //   - contentId:  ID of the wizard-content div.
    //   - errorId:    ID of the wizard-error div (optional, omit for modals without validation).
    //   - modalId:    ID of the wizard-modal root div.
    //   - onClose:    Optional callback invoked when close() is called, before hiding the modal.
    //   - onRender:   function(step) called to render step content into the content div.
    //   - onValidate: function(step) returns an error string or empty string (sync), or a Promise resolving to one (async). Async validation is supported for
    //                 operations like saving data to the server before advancing. The controller detects thenable returns and waits for resolution.
    //   - stepCount:  Total number of steps (0 for non-step modals).
    //   - stepsId:    ID of the wizard-steps container (optional, defaults to modalId + '-steps').
    //   - titleId:    ID of the title h3 element (optional, for dynamic title updates).
    //
    // Returns: { back, close, getStep, goToStep, hide, next, open, setError, setTitle, show, state }
    "  window.createWizardController = (config) => {",
    "    let currentStep = 1;",
    "    let highestStep = 1;",
    "    const stepsContainerId = config.stepsId || (config.modalId + '-steps');",

    // Update the step indicator DOM. Iterates .wizard-step children within the scoped container, setting active/completed/clickable classes and attaching
    // click handlers for visited steps.
    "    function updateStepIndicator() {",
    "      const container = document.getElementById(stepsContainerId);",
    "      if(!container) return;",
    "      const steps = container.querySelectorAll('.wizard-step');",
    "      for(const step of steps) {",
    "        const stepNum = parseInt(step.getAttribute('data-step'), 10);",
    "        step.classList.remove('active', 'completed', 'clickable');",
    "        if(stepNum < currentStep) step.classList.add('completed');",
    "        if(stepNum === currentStep) step.classList.add('active');",
    "        if((stepNum !== currentStep) && (stepNum <= highestStep)) step.classList.add('clickable');",
    "        step.onclick = () => ctrl.goToStep(stepNum);",
    "      }",
    "    }",

    // Clear the error display.
    "    function clearError() {",
    "      if(!config.errorId) return;",
    "      const el = document.getElementById(config.errorId);",
    "      if(el) { el.textContent = ''; el.style.display = 'none'; }",
    "    }",

    // Render the current step. Clears errors, updates the step indicator, and delegates to the caller's onRender callback.
    "    function render() {",
    "      clearError();",
    "      if(config.stepCount > 0) updateStepIndicator();",
    "      config.onRender(currentStep);",
    "    }",

    // Advance to a target step. Called by both next() and goToStep() after validation succeeds.
    "    function advance(target) {",
    "      currentStep = target;",
    "      if(currentStep > highestStep) highestStep = currentStep;",
    "      render();",
    "    }",

    // Validate the current step and advance to a target step if valid. Supports both sync and async onValidate callbacks. The async wrapper ensures the
    // return is always a Promise, so callers can uniformly await the result regardless of whether onValidate is sync or async.
    "    async function validateAndAdvance(target) {",
    "      try {",
    "        const err = await config.onValidate(currentStep);",
    "        if(err) { ctrl.setError(err); return false; }",
    "        advance(target);",
    "        return true;",
    "      } catch(e) { console.error('Wizard validation threw an unexpected error.', e); ctrl.setError('Validation failed.'); return false; }",
    "    }",

    "    const ctrl = {",

    // Arbitrary state object for the caller to store wizard-specific data. Reset to {} on close.
    "      state: {},",

    // Navigate backward one step.
    "      back() {",
    "        if(currentStep > 1) { currentStep--; render(); }",
    "      },",

    // Close the modal. Invokes the optional onClose callback before hiding, then resets state.
    "      close() {",
    "        if(config.onClose) config.onClose();",
    "        document.getElementById(config.modalId).style.display = 'none';",
    "        ctrl.state = {};",
    "      },",

    // Return the current step number.
    "      getStep() {",
    "        return currentStep;",
    "      },",

    // Navigate to a specific step by clicking the step indicator. Going backward is always allowed for visited steps. Going forward validates the current step
    // and only allows jumps to previously visited steps. Supports async validation - returns a Promise when onValidate is async.
    "      goToStep(n) {",
    "        if(n === currentStep) return;",
    "        if(n > highestStep) return;",
    "        if(n > currentStep) {",
    "          return validateAndAdvance(n);",
    "        }",
    "        currentStep = n;",
    "        render();",
    "      },",

    // Hide a button by ID.
    "      hide(id) {",
    "        const el = document.getElementById(id);",
    "        if(el) el.style.display = 'none';",
    "      },",

    // Navigate forward one step. Validates the current step first. Returns false (sync) or a Promise resolving to false (async) if validation failed.
    "      next() {",
    "        if(currentStep >= config.stepCount) return true;",
    "        return validateAndAdvance(currentStep + 1);",
    "      },",

    // Open the modal and render. Options: { step, highestStep, title } - all optional.
    "      open(options) {",
    "        const opts = options || {};",
    "        currentStep = opts.step || 1;",
    "        highestStep = opts.highestStep || currentStep;",
    "        if(opts.title && config.titleId) {",
    "          const titleEl = document.getElementById(config.titleId);",
    "          if(titleEl) titleEl.textContent = opts.title;",
    "        }",
    "        document.getElementById(config.modalId).style.display = 'flex';",
    "        render();",
    "      },",

    // Show a validation error message.
    "      setError(msg) {",
    "        if(!config.errorId) return;",
    "        const el = document.getElementById(config.errorId);",
    "        if(el) { el.textContent = msg; el.style.display = msg ? '' : 'none'; }",
    "      },",

    // Update the modal title dynamically.
    "      setTitle(html) {",
    "        if(!config.titleId) return;",
    "        const el = document.getElementById(config.titleId);",
    "        if(el) el.innerHTML = html;",
    "      },",

    // Show a button by ID.
    "      show(id) {",
    "        const el = document.getElementById(id);",
    "        if(el) el.style.display = '';",
    "      }",

    "    };",

    // Attach click handlers to controller-managed buttons within the modal. Role-tagged buttons (data-wizard-role) and the X close button (.wizard-close) get
    // handlers attached from inside the IIFE closure, where the controller variable is accessible. This is the same pattern used for step indicator click
    // handlers and avoids the inline-onclick-to-global-scope problem that breaks when controller instances are IIFE-scoped.
    "    const modal = document.getElementById(config.modalId);",
    "    if(modal) {",
    "      const roleButtons = modal.querySelectorAll('[data-wizard-role]');",
    "      for(const btn of roleButtons) {",
    "        const role = btn.getAttribute('data-wizard-role');",
    "        if(role === 'back') { btn.onclick = () => { ctrl.back(); }; }",
    "        else if(role === 'next') { btn.onclick = () => { ctrl.next(); }; }",
    "        else if(role === 'close') { btn.onclick = () => { ctrl.close(); }; }",
    "      }",
    "      const closeBtn = modal.querySelector('.wizard-close');",
    "      if(closeBtn) { closeBtn.onclick = () => { ctrl.close(); }; }",
    "    }",

    "    return ctrl;",
    "  };",

    /* Channel table DOM manipulation namespace. All functions that insert, remove, filter, sort, or update channel table rows live here as methods of a single
     * namespace object. Internal collaborators invoke each other via this.* which keeps the call graph contained inside the namespace. External callers across
     * all client scripts use channelTable.* (e.g., channelTable.applyPatch, channelTable.filter).
     */
    "  window.channelTable = {",

    /* Cached column index map for _getSortValue. Built lazily on first access by scanning the table header row's data-sort-field attributes, then held for
     * the page lifetime with no invalidation path. This relies on a load-bearing invariant: the channel table header is rendered once by the server and
     * never rebuilt client-side - only CSS visibility classes are toggled by toggleColumn, never DOM structure. Do not introduce dynamic thead mutation
     * without also adding an explicit cache invalidation (e.g., set _colIndexCache = null after rewriting the header row).
     */
    "    _colIndexCache: null,",

    // Read the server-stamped sort value from a row's cell for a given field. Private helper used by sort() and insertRow() to compute insertion order.
    "    _getSortValue(row, field) {",
    "      if(!this._colIndexCache) {",
    "        this._colIndexCache = {};",
    "        const ths = document.querySelectorAll('.channel-table th[data-sort-field]');",
    "        for(let i = 0; i < ths.length; i++) { this._colIndexCache[ths[i].getAttribute('data-sort-field')] = i; }",
    "      }",
    "      const idx = this._colIndexCache[field];",
    "      if(idx === undefined) return '';",
    "      const cell = row.children[idx];",
    "      return cell?.getAttribute('data-sort-value') || '';",
    "    },",

    // Apply a channel table patch from a mutation response or SSE event. Handles whichever fields are present: rows (insert/remove), counts (summary header),
    // scopeCounts (Quick Actions toggles), hdhrCounts, tagCounts, logos. This is the single entry point for all channel table updates.
    "    applyPatch(patch) {",
    "      if(!patch) return;",

    // Apply row updates (insert, replace, or remove).
    "      if(patch.rows) {",
    "        for(const row of patch.rows) {",
    "          if(row.action === 'remove') this.removeRow(row.key);",
    "          else if((row.action === 'update') && row.displayRow) {",
    "            this.insertRow({ displayRow: row.displayRow, editRow: row.editRow || '' }, row.key);",
    "          }",
    "        }",
    "      }",

    // Apply summary counts directly from server-computed values. The entries array drives a single loop rather than repeating the getElementById + null-check
    // pattern for each counter. Alphabetized by element ID.
    "      if(patch.counts) {",
    "        const c = patch.counts;",
    "        for(const [ id, val ] of [",
    "          [ 'disabled-count', String(c.disabled) ],",
    "          [ 'enabled-count', String(c.enabled) ],",
    "          [ 'predefined-count', String(c.predefined) ],",
    "          [ 'total-count', String(c.total) ],",
    "          [ 'user-count', (c.user > 0) ? ', ' + String(c.user) + ' user' : '' ]",
    "        ]) { const el = document.getElementById(id); if(el) el.textContent = val; }",
    "      }",

    // Apply scope toggle counts from server-computed values. Iterates the server payload directly so this block stays symmetric with the tagCounts and logos
    // blocks below, and so the client does not hardcode the set of scope names.
    "      if(patch.scopeCounts) {",
    "        for(const [ s, sc ] of Object.entries(patch.scopeCounts)) {",
    "          const cb = document.querySelector('.scope-toggle[data-scope=\"' + s + '\"]');",
    "          const span = document.querySelector('.quick-action-count[data-scope=\"' + s + '\"]');",
    "          if(cb) {",
    "            cb.checked = (sc.enabled === sc.total);",
    "            cb.indeterminate = (sc.enabled > 0) && (sc.enabled < sc.total);",
    "          }",
    "          if(span) { span.setAttribute('data-enabled', sc.enabled); span.setAttribute('data-total', sc.total); " +
    "span.textContent = sc.enabled + ' of ' + sc.total + ' enabled'; }",
    "        }",
    "      }",

    // Apply HDHR bulk toggle counts from server-computed values.
    "      if(patch.hdhrCounts) {",
    "        const hc = patch.hdhrCounts;",
    "        const htoggle = document.getElementById('hdhr-bulk-toggle');",
    "        const hcount = document.getElementById('hdhr-bulk-count');",
    "        if(htoggle) {",
    "          htoggle.checked = (hc.enabled === hc.total) && (hc.total > 0);",
    "          htoggle.indeterminate = (hc.enabled > 0) && (hc.enabled < hc.total);",
    "        }",
    "        if(hcount) hcount.textContent = hc.enabled + ' of ' + hc.total;",
    "      }",

    // Apply tag bulk toggle counts from server-computed values.
    "      if(patch.tagCounts) {",
    "        for(const [ tagName, tc ] of Object.entries(patch.tagCounts)) {",
    "          const ttoggle = document.querySelector('.tag-bulk-toggle[data-tag=\"' + tagName + '\"]');",
    "          const tcount = document.querySelector('[data-tag-count=\"' + tagName + '\"]');",
    "          if(ttoggle) {",
    "            ttoggle.checked = (tc.count === tc.total) && (tc.total > 0);",
    "            ttoggle.indeterminate = (tc.count > 0) && (tc.count < tc.total);",
    "          }",
    "          if(tcount) tcount.textContent = tc.count + ' of ' + tc.total;",
    "        }",
    "      }",

    // Apply channel logos from a logo map (key -> URL). Sets data-logo attributes on matching rows so processLogos() can render the images.
    "      if(patch.logos) {",
    "        for(const [ key, logoUrl ] of Object.entries(patch.logos)) {",
    "          const logoRow = document.getElementById('display-row-' + key);",
    "          const nameCell = logoRow?.querySelector('.channel-name-cell');",
    "          nameCell?.parentElement?.setAttribute('data-logo', logoUrl);",
    "        }",
    "      }",

    // Post-update: refilter rows for the service filter and render any new logos. Row insertions reset the select options (Safari ignores hidden), so the
    // filter must be re-applied. Logo rendering handles both logo-only patches and newly-inserted rows.
    "      if(patch.rows && (patch.rows.length > 0)) this.refilter();",
    "      if(patch.logos || (patch.rows && (patch.rows.length > 0))) this.processLogos();",
    "    },",

    // Insert or replace a channel row in the table. Always removes existing rows with the same key first (handles edits and overrides of builtin channels).
    // Uses the current table sort field and direction for correct insertion order.
    "    insertRow(html, key) {",
    "      const table = document.querySelector('.channel-table');",
    "      const tbody = table?.querySelector('tbody');",
    "      if(!table || !tbody || !html) return;",
    "      document.getElementById('edit-row-' + key)?.remove();",
    "      document.getElementById('display-row-' + key)?.remove();",
    "      const temp = document.createElement('tbody');",
    "      temp.innerHTML = html.displayRow + (html.editRow || '');",
    "      const newDisplayRow = temp.firstElementChild;",
    "      const newEditRow = temp.children[1] || null;",
    "      const sortField = table.getAttribute('data-sort-field') || 'name';",
    "      const sortDir = table.getAttribute('data-sort-dir') || 'asc';",
    "      const newVal = this._getSortValue(newDisplayRow, sortField);",
    "      const rows = tbody.querySelectorAll('tr[id^=\"display-row-\"]');",
    "      let inserted = false;",
    "      for(const row of rows) {",
    "        const rowVal = this._getSortValue(row, sortField);",
    "        const cmp = (sortDir === 'asc') ? (newVal < rowVal) : (newVal > rowVal);",
    "        if(cmp) {",
    "          tbody.insertBefore(newDisplayRow, row);",
    "          if(newEditRow) tbody.insertBefore(newEditRow, row);",
    "          inserted = true;",
    "          break;",
    "        }",
    "      }",
    "      if(!inserted) {",
    "        tbody.appendChild(newDisplayRow);",
    "        if(newEditRow) tbody.appendChild(newEditRow);",
    "      }",
    "      this.processLogos();",
    "    },",

    // Remove a channel row (both display and edit variants) from the table.
    "    removeRow(key) {",
    "      document.getElementById('display-row-' + key)?.remove();",
    "      document.getElementById('edit-row-' + key)?.remove();",
    "    },",

    /* Apply the service filter to all channel rows. Hides rows whose service tags aren't enabled and filters service dropdown options for multi-service channels.
     * Uses a persistent _allOptions array on each select to remember all server-rendered options across filter applications - the server marks disabled options
     * with the hidden attribute, but Safari ignores hidden on option elements, so we rebuild the select with only enabled options each time. Selection restore
     * priority: (1) saved server choice (HTML selected attribute), (2) previous visual selection, (3) first option.
     *
     * Cache lifecycle: _allOptions is attached to the DOM select element itself, so its lifetime is tied to that element. Row replacement via insertRow() drops
     * the old row (and its select) and inserts fresh server-rendered HTML, which transparently resets the cache with whatever option set the server just sent.
     * This invariant is load-bearing - do not refactor insertRow() into an in-place row update without also adding an explicit cache invalidation path here.
     */
    "    filter(enabledTags) {",
    "      const rows = document.querySelectorAll('tr[data-provider-tags]');",
    "      for(const row of rows) {",
    "        const tags = row.getAttribute('data-provider-tags').split(',');",
    "        const available = (enabledTags.length === 0) || tags.some((t) => (t === 'direct') || enabledTags.includes(t));",
    "        row.classList.toggle('channel-unavailable', !available);",
    "        const label = row.querySelector('.no-provider-label');",
    "        const sel = row.querySelector('.provider-select');",
    "        const name = row.querySelector('.provider-name');",
    "        if(label) label.style.display = available ? 'none' : '';",
    "        if(name) name.style.display = available ? '' : 'none';",
    "        if(sel) {",
    "          sel.style.display = available ? '' : 'none';",
    "          sel._allOptions ??= [ ...sel.querySelectorAll('option') ];",
    "          const prevValue = sel.value;",
    "          sel.innerHTML = '';",
    "          let serverDefault = null;",
    "          let prevExists = false;",
    "          for(const opt of sel._allOptions) {",
    "            const oTag = opt.getAttribute('data-provider-tag');",
    "            const show = (enabledTags.length === 0) || (oTag === 'direct') || enabledTags.includes(oTag);",
    "            if(show) {",
    "              sel.appendChild(opt);",
    "              if(opt.hasAttribute('selected')) serverDefault = opt;",
    "              if(opt.value === prevValue) prevExists = true;",
    "            }",
    "          }",
    "          if(serverDefault) sel.value = serverDefault.value;",
    "          else if(prevExists) sel.value = prevValue;",
    "          else if(sel.options.length > 0) sel.selectedIndex = 0;",
    "        }",
    "      }",
    "    },",

    // Returns the currently enabled service filter tags by walking the filter dropdown checkboxes. Returns an empty array when the menu is missing, when all
    // checkboxes are checked (treated as no filter), or when no checkboxes are checked. The empty-when-all-checked semantics match the server-side filter model:
    // an empty enabledServices array means "show all services" rather than "show none."
    "    getEnabledFilterTags() {",
    "      const menu = document.querySelector('.provider-dropdown-menu');",
    "      if(!menu) return [];",
    "      const cbs = menu.querySelectorAll('input[type=\"checkbox\"]:not(:disabled)');",
    "      const enabledTags = [];",
    "      let allChecked = true;",
    "      for(const cb of cbs) {",
    "        if(cb.checked) enabledTags.push(cb.getAttribute('data-tag'));",
    "        else allChecked = false;",
    "      }",
    "      return allChecked ? [] : enabledTags;",
    "    },",

    // Re-apply the service filter using the current checkbox state from the filter dropdown menu. Called after row insertions because fresh server HTML contains
    // all options with hidden attributes (which Safari ignores), so the filter must be re-established.
    "    refilter() {",
    "      const tags = this.getEnabledFilterTags();",
    "      if(tags.length > 0) this.filter(tags);",
    "    },",

    // Sort the channel table by the specified field. Toggles direction if the same field is clicked again. Persists the sort preference to the server.
    "    sort(field) {",
    "      const table = document.querySelector('.channel-table');",
    "      if(!table) return;",
    "      const currentField = table.getAttribute('data-sort-field');",
    "      const currentDir = table.getAttribute('data-sort-dir') || 'asc';",
    "      const dir = (field === currentField) ? ((currentDir === 'asc') ? 'desc' : 'asc') : 'asc';",
    "      table.setAttribute('data-sort-field', field);",
    "      table.setAttribute('data-sort-dir', dir);",
    "      const tbody = table.querySelector('tbody');",
    "      const displayRows = tbody.querySelectorAll('tr[id^=\"display-row-\"]');",
    "      const pairs = [];",
    "      for(const dr of displayRows) {",
    "        const key = dr.id.replace('display-row-', '');",
    "        const er = document.getElementById('edit-row-' + key);",
    "        const name = dr.children[1]?.textContent.trim().toLowerCase() ?? '';",
    "        pairs.push({ displayRow: dr, editRow: er, name, val: this._getSortValue(dr, field) });",
    "      }",

    // Secondary sort by channel name stabilizes rows with identical primary values. Always ascending regardless of primary direction so groups maintain
    // consistent alphabetical order within the sort.
    "      pairs.sort((a, b) => {",
    "        const cmp = (dir === 'asc') ? a.val.localeCompare(b.val) : b.val.localeCompare(a.val);",
    "        return (cmp !== 0) ? cmp : a.name.localeCompare(b.name);",
    "      });",
    "      for(const pair of pairs) {",
    "        tbody.appendChild(pair.displayRow);",
    "        if(pair.editRow) tbody.appendChild(pair.editRow);",
    "      }",

    // Update header sort indicators. Targets .sort-label within each th so other children (like tag filter dropdowns) are untouched.
    "      for(const th of table.querySelectorAll('th.sortable')) {",
    "        const hField = th.getAttribute('data-sort-field');",
    "        const sortLabel = th.querySelector('.sort-label');",
    "        if(!sortLabel) continue;",
    "        const label = sortLabel.textContent.replace(/[\\u25B2\\u25BC]/g, '').trim();",
    "        if(hField === field) sortLabel.innerHTML = label + ((dir === 'asc') ? ' &#9650;' : ' &#9660;');",
    "        else sortLabel.textContent = label;",
    "      }",
    "      persistDisplayPrefs({ sortDirection: dir, sortField: field });",
    "    },",

    // Process channel logo data attributes. Finds all td[data-logo] elements and renders their channel-name-cell spans via channelDisplayHtml in 'both' mode.
    // Called on page load and after any DOM mutation that inserts server-rendered channel rows (add, edit, delete-with-replacement).
    "    processLogos() {",
    "      for(const cell of document.querySelectorAll('td[data-logo]')) {",
    "        if(cell.getAttribute('data-logo-processed')) continue;",
    "        const url = cell.getAttribute('data-logo');",
    "        const nameSpan = cell.querySelector('.channel-name-cell');",
    "        if(url && nameSpan) {",
    "          nameSpan.innerHTML = channelDisplayHtml(url, nameSpan.textContent || '', 'channel-table-logo', 'channel-table-text', 'both');",
    "          cell.setAttribute('data-logo-processed', '1');",
    "        }",
    "      }",
    "    }",
    "  };",

    /* Action registrations. The cross-cutting actions bound to functions defined in this same script (toggleDropdown, channelTable.sort). Each subsystem
     * registers its own actions in its corresponding script file; this block keeps shared-utility actions colocated with their definitions. Event mechanics
     * (preventDefault, stopPropagation, close-dropdown) live declaratively on the trigger element, not in these handler bodies.
     */
    "  window.registerAction('" + ACTIONS.channelTableSort + "', (target) => channelTable.sort(target.dataset.field));",
    "  window.registerAction('" + ACTIONS.toggleDropdown + "', (target) => toggleDropdown(target));",

    "})();",
    "</script>"
  ].join("\n");
}
