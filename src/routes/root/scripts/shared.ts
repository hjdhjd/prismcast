/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * shared.ts: Shared client-side utilities for all tabs. This script runs before any tab-specific script so that cross-tab functions (toast notifications,
 * dropdown menus, channel display rendering) are available during both initialization and event handling.
 */

/**
 * Generates the shared utilities script block containing cross-tab client-side functions. Runs before all tab-specific scripts to eliminate execution order
 * dependencies. Functions are exposed on window for access from any script context or inline event handler.
 * @returns HTML script block with shared utility functions.
 */
export function generateSharedUtilitiesScript(): string {

  return [
    "<script>",
    "(function() {",

    // Show a toast notification. Auto-dismiss durations: success/info = 5s, warning = 8s, error = no auto-dismiss. Optional action: { label, onclick } appends an
    // inline button between the message text and the close button.
    "  function showToast(message, type, duration, action) {",
    "    var container = document.getElementById('toast-container');",
    "    if (!container) return;",
    "    var toast = document.createElement('div');",
    "    toast.className = 'toast ' + (type || 'info');",
    "    toast.textContent = message;",
    "    toast.setAttribute('role', (type === 'error' || type === 'warning') ? 'alert' : 'status');",
    "    if (action && action.label) {",
    "      var actionBtn = document.createElement('button');",
    "      actionBtn.type = 'button';",
    "      actionBtn.className = 'toast-action';",
    "      actionBtn.textContent = action.label;",
    "      actionBtn.onclick = function() { if (action.onclick) action.onclick(); dismissToast(toast); };",
    "      toast.appendChild(actionBtn);",
    "    }",
    "    var closeBtn = document.createElement('button');",
    "    closeBtn.type = 'button';",
    "    closeBtn.className = 'toast-close';",
    "    closeBtn.textContent = '\\u00d7';",
    "    closeBtn.setAttribute('aria-label', 'Dismiss');",
    "    closeBtn.onclick = function() { dismissToast(toast); };",
    "    toast.appendChild(closeBtn);",
    "    container.appendChild(toast);",
    "    var ms = duration !== undefined ? duration : type === 'error' ? 0 : type === 'warning' ? 8000 : 5000;",
    "    if (ms > 0) { setTimeout(function() { dismissToast(toast); }, ms); }",
    "  }",

    "  window.showToast = showToast;",

    // Dismiss a toast with slide-out animation.
    "  function dismissToast(toast) {",
    "    if (toast.classList.contains('toast-exit')) return;",
    "    toast.classList.add('toast-exit');",
    "    toast.addEventListener('animationend', function() { if (toast.parentNode) toast.parentNode.removeChild(toast); });",
    "  }",

    // Close all open dropdown menus, fire registered before-close hooks, and remove scroll/resize listeners. Hooks are registered via
    // closeDropdowns.addHook(fn) for patterns that need to intercept close (e.g., batch-save on the inline tag editor).
    "  var closeHooks = [];",
    "  function closeDropdowns() {",
    "    for(var h = 0; h < closeHooks.length; h++) closeHooks[h]();",
    "    var menus = document.querySelectorAll('.dropdown-menu.show');",
    "    for(var i = 0; i < menus.length; i++) menus[i].classList.remove('show');",
    "    window.removeEventListener('scroll', closeDropdowns, true);",
    "    window.removeEventListener('resize', closeDropdowns);",
    "  }",
    "  closeDropdowns.addHook = function(fn) { closeHooks.push(fn); };",

    "  window.closeDropdowns = closeDropdowns;",

    // Copy text to the clipboard and show a toast. Uses the modern Clipboard API when available (secure contexts), falling back to execCommand for plain HTTP
    // access via IP address.
    "  window.copyToClipboard = async function(text, successMessage) {",
    "    if(navigator.clipboard && navigator.clipboard.writeText) {",
    "      try { await navigator.clipboard.writeText(text); showToast(successMessage, 'success'); }",
    "      catch(e) { showToast('Failed to copy to clipboard.', 'error'); }",
    "    } else {",
    "      var ta = document.createElement('textarea');",
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
    "    var rect = anchor.getBoundingClientRect();",
    "    var top = rect.bottom + 2;",
    "    var left = rect.left;",
    "    if(left + menu.offsetWidth > window.innerWidth - 4) left = rect.right - menu.offsetWidth;",
    "    if(left < 4) left = 4;",
    "    if(top + menu.offsetHeight > window.innerHeight - 4) top = rect.top - menu.offsetHeight - 2;",
    "    var sx = window.scrollX || 0;",
    "    var sy = window.scrollY || 0;",
    "    menu.style.top = (top + sy) + 'px';",
    "    menu.style.left = (left + sx) + 'px';",
    "  }",

    "  window.positionPortal = positionPortal;",

    // Layer 2: Dropdown toggle with lifecycle hooks. Manages portal append, toggle state, scroll/resize dismissal, and optional lifecycle callbacks. The
    // zero-argument form (toggleDropdown(btn)) works unchanged for standard dropdowns. The options form adds: menu (explicit menu element), onOpen (called
    // after show but before positioning so content can be set and measured correctly for viewport clamping).
    "  window.toggleDropdown = function(btn, opts) {",
    "    var o = opts || {};",
    "    var menu = btn._portalMenu || o.menu || btn.nextElementSibling;",
    "    if(!menu) return;",
    "    var isOpen = menu.classList.contains('show');",
    "    closeDropdowns();",
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
    "    window.addEventListener('scroll', closeDropdowns, true);",
    "    window.addEventListener('resize', closeDropdowns);",
    "  };",

    // Shared channel display renderer with three modes. The mode parameter controls presentation: 'logo' (default) shows the logo with text as an onerror
    // fallback, 'both' shows the logo and text side by side with onerror hiding the broken image, 'text' shows only the text and ignores the logo URL.
    "  window.channelDisplayHtml = function(logoUrl, name, logoClass, textClass, mode) {",
    "    var m = mode || 'logo';",
    "    if(m === 'text' || !logoUrl) {",
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
    "  window.imgFallback = function(img) {",
    "    var fallbacks = (img.getAttribute('data-fallbacks') || '').split('|').filter(Boolean);",
    "    var idx = parseInt(img.getAttribute('data-fb-idx') || '0', 10);",
    "    if(idx < fallbacks.length) {",
    "      img.setAttribute('data-fb-idx', String(idx + 1));",
    "      img.src = fallbacks[idx];",
    "    } else {",
    "      img.style.display = 'none';",
    "      var sib = img.nextElementSibling;",
    "      if(sib && sib.style.display === 'none') sib.style.display = 'inline';",
    "    }",
    "  };",

    // Service icon renderer with three modes, mirroring channelDisplayHtml. The icon source chain is: iconUrl (if specified) → Apple touch icon → favicon.
    // Fallback URLs are stored in a data-fallbacks attribute and processed by the shared imgFallback handler.
    "  window.serviceIconHtml = function(domain, name, iconClass, textClass, mode, iconUrl) {",
    "    var m = mode || 'logo';",
    "    if(m === 'text' || !domain) {",
    "      return '<span class=\"' + textClass + '\">' + name + '</span>';",
    "    }",
    "    var touchIcon = 'https://' + domain + '/apple-touch-icon.png';",
    "    var favicon = 'https://' + domain + '/favicon.ico';",
    "    var src = iconUrl || touchIcon;",
    "    var fallbacks = (iconUrl ? [ touchIcon, favicon ] : [ favicon ]).join('|');",
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
    "  window.processServiceDisplays = function() {",
    "    var els = document.querySelectorAll('.provider-display');",
    "    for(var i = 0; i < els.length; i++) {",
    "      var el = els[i];",
    "      if(el.getAttribute('data-processed')) continue;",
    "      var domain = el.getAttribute('data-domain') || '';",
    "      var iconUrl = el.getAttribute('data-icon-url') || '';",
    "      var name = el.textContent || '';",
    "      var sm = el.hasAttribute('data-sm');",
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
    "  window.createWizardController = function(config) {",
    "    var currentStep = 1;",
    "    var highestStep = 1;",
    "    var stepsContainerId = config.stepsId || (config.modalId + '-steps');",

    // Update the step indicator DOM. Iterates .wizard-step children within the scoped container, setting active/completed/clickable classes and attaching
    // click handlers for visited steps.
    "    function updateStepIndicator() {",
    "      var container = document.getElementById(stepsContainerId);",
    "      if(!container) return;",
    "      var steps = container.querySelectorAll('.wizard-step');",
    "      for(var i = 0; i < steps.length; i++) {",
    "        var stepNum = parseInt(steps[i].getAttribute('data-step'), 10);",
    "        steps[i].classList.remove('active', 'completed', 'clickable');",
    "        if(stepNum < currentStep) steps[i].classList.add('completed');",
    "        if(stepNum === currentStep) steps[i].classList.add('active');",
    "        if(stepNum !== currentStep && stepNum <= highestStep) steps[i].classList.add('clickable');",
    "        steps[i].onclick = (function(n) { return function() { ctrl.goToStep(n); }; })(stepNum);",
    "      }",
    "    }",

    // Clear the error display.
    "    function clearError() {",
    "      if(!config.errorId) return;",
    "      var el = document.getElementById(config.errorId);",
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
    "        var err = await config.onValidate(currentStep);",
    "        if(err) { ctrl.setError(err); return false; }",
    "        advance(target);",
    "        return true;",
    "      } catch(e) { ctrl.setError('Validation failed.'); return false; }",
    "    }",

    "    var ctrl = {",

    // Arbitrary state object for the caller to store wizard-specific data. Reset to {} on close.
    "      state: {},",

    // Navigate backward one step.
    "      back: function() {",
    "        if(currentStep > 1) { currentStep--; render(); }",
    "      },",

    // Close the modal. Invokes the optional onClose callback before hiding, then resets state.
    "      close: function() {",
    "        if(config.onClose) config.onClose();",
    "        document.getElementById(config.modalId).style.display = 'none';",
    "        ctrl.state = {};",
    "      },",

    // Return the current step number.
    "      getStep: function() { return currentStep; },",

    // Navigate to a specific step by clicking the step indicator. Going backward is always allowed for visited steps. Going forward validates the current step
    // and only allows jumps to previously visited steps. Supports async validation - returns a Promise when onValidate is async.
    "      goToStep: function(n) {",
    "        if(n === currentStep) return;",
    "        if(n > highestStep) return;",
    "        if(n > currentStep) {",
    "          return validateAndAdvance(n);",
    "        }",
    "        currentStep = n;",
    "        render();",
    "      },",

    // Hide a button by ID.
    "      hide: function(id) {",
    "        var el = document.getElementById(id);",
    "        if(el) el.style.display = 'none';",
    "      },",

    // Navigate forward one step. Validates the current step first. Returns false (sync) or a Promise resolving to false (async) if validation failed.
    "      next: function() {",
    "        if(currentStep >= config.stepCount) return true;",
    "        return validateAndAdvance(currentStep + 1);",
    "      },",

    // Open the modal and render. Options: { step, highestStep, title } - all optional.
    "      open: function(options) {",
    "        var opts = options || {};",
    "        currentStep = opts.step || 1;",
    "        highestStep = opts.highestStep || currentStep;",
    "        if(opts.title && config.titleId) {",
    "          var titleEl = document.getElementById(config.titleId);",
    "          if(titleEl) titleEl.textContent = opts.title;",
    "        }",
    "        document.getElementById(config.modalId).style.display = 'flex';",
    "        render();",
    "      },",

    // Show a validation error message.
    "      setError: function(msg) {",
    "        if(!config.errorId) return;",
    "        var el = document.getElementById(config.errorId);",
    "        if(el) { el.textContent = msg; el.style.display = msg ? '' : 'none'; }",
    "      },",

    // Update the modal title dynamically.
    "      setTitle: function(html) {",
    "        if(!config.titleId) return;",
    "        var el = document.getElementById(config.titleId);",
    "        if(el) el.innerHTML = html;",
    "      },",

    // Show a button by ID.
    "      show: function(id) {",
    "        var el = document.getElementById(id);",
    "        if(el) el.style.display = '';",
    "      }",

    "    };",

    // Attach click handlers to controller-managed buttons within the modal. Role-tagged buttons (data-wizard-role) and the X close button (.wizard-close) get
    // handlers attached from inside the IIFE closure, where the controller variable is accessible. This is the same pattern used for step indicator click
    // handlers and avoids the inline-onclick-to-global-scope problem that breaks when controller instances are IIFE-scoped.
    "    var modal = document.getElementById(config.modalId);",
    "    if(modal) {",
    "      var roleButtons = modal.querySelectorAll('[data-wizard-role]');",
    "      for(var i = 0; i < roleButtons.length; i++) {",
    "        (function(btn) {",
    "          var role = btn.getAttribute('data-wizard-role');",
    "          if(role === 'back') { btn.onclick = function() { ctrl.back(); }; }",
    "          else if(role === 'next') { btn.onclick = function() { ctrl.next(); }; }",
    "          else if(role === 'close') { btn.onclick = function() { ctrl.close(); }; }",
    "        })(roleButtons[i]);",
    "      }",
    "      var closeBtn = modal.querySelector('.wizard-close');",
    "      if(closeBtn) { closeBtn.onclick = function() { ctrl.close(); }; }",
    "    }",

    "    return ctrl;",
    "  };",

    /* Channel table DOM manipulation namespace. All functions that insert, remove, filter, sort, or update channel table rows live here as methods of a single
     * namespace object. Internal collaborators invoke each other via this.* which cannot accidentally escape the namespace — a structural guarantee that prevents
     * the cross-IIFE scoping bugs that plagued the prior scattered design. External callers across all client scripts use channelTable.* (e.g.,
     * channelTable.applyPatch, channelTable.filter).
     */
    "  window.channelTable = {",

    // Cached column index map for _getSortValue. Built lazily on first access by scanning the table header row's data-sort-field attributes.
    "    _colIndexCache: null,",

    // Read the server-stamped sort value from a row's cell for a given field. Private helper used by sort() and insertRow() to compute insertion order.
    "    _getSortValue: function(row, field) {",
    "      if(!this._colIndexCache) {",
    "        this._colIndexCache = {};",
    "        var ths = document.querySelectorAll('.channel-table th[data-sort-field]');",
    "        for(var i = 0; i < ths.length; i++) { this._colIndexCache[ths[i].getAttribute('data-sort-field')] = i; }",
    "      }",
    "      var idx = this._colIndexCache[field];",
    "      if(idx === undefined) return '';",
    "      var cell = row.children[idx];",
    "      if(!cell) return '';",
    "      return cell.getAttribute('data-sort-value') || '';",
    "    },",

    // Apply a channel table patch from a mutation response or SSE event. Handles whichever fields are present: rows (insert/remove), counts (summary header),
    // scopeCounts (Quick Actions toggles), hdhrCounts, tagCounts, logos. This is the single entry point for all channel table updates.
    "    applyPatch: function(patch) {",
    "      if(!patch) return;",

    // Apply row updates (insert, replace, or remove).
    "      if(patch.rows) {",
    "        for(var i = 0; i < patch.rows.length; i++) {",
    "          var row = patch.rows[i];",
    "          if(row.action === 'remove') this.removeRow(row.key);",
    "          else if(row.action === 'update' && row.displayRow) {",
    "            this.insertRow({ displayRow: row.displayRow, editRow: row.editRow || '' }, row.key);",
    "          }",
    "        }",
    "      }",

    // Apply summary counts directly from server-computed values.
    "      if(patch.counts) {",
    "        var c = patch.counts;",
    "        var el;",
    "        el = document.getElementById('total-count'); if(el) el.textContent = String(c.total);",
    "        el = document.getElementById('enabled-count'); if(el) el.textContent = String(c.enabled);",
    "        el = document.getElementById('disabled-count'); if(el) el.textContent = String(c.disabled);",
    "        el = document.getElementById('predefined-count'); if(el) el.textContent = String(c.predefined);",
    "        el = document.getElementById('user-count'); if(el) el.textContent = (c.user > 0) ? ', ' + String(c.user) + ' user' : '';",
    "      }",

    // Apply scope toggle counts from server-computed values.
    "      if(patch.scopeCounts) {",
    "        var scopes = ['all', 'east', 'pacific'];",
    "        for(var j = 0; j < scopes.length; j++) {",
    "          var s = scopes[j];",
    "          var sc = patch.scopeCounts[s];",
    "          if(!sc) continue;",
    "          var cb = document.querySelector('.scope-toggle[data-scope=\"' + s + '\"]');",
    "          var span = document.querySelector('.quick-action-count[data-scope=\"' + s + '\"]');",
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
    "        var hc = patch.hdhrCounts;",
    "        var htoggle = document.getElementById('hdhr-bulk-toggle');",
    "        var hcount = document.getElementById('hdhr-bulk-count');",
    "        if(htoggle) {",
    "          htoggle.checked = (hc.enabled === hc.total) && (hc.total > 0);",
    "          htoggle.indeterminate = (hc.enabled > 0) && (hc.enabled < hc.total);",
    "        }",
    "        if(hcount) hcount.textContent = hc.enabled + ' of ' + hc.total;",
    "      }",

    // Apply tag bulk toggle counts from server-computed values.
    "      if(patch.tagCounts) {",
    "        for(var tagName in patch.tagCounts) {",
    "          var tc = patch.tagCounts[tagName];",
    "          var ttoggle = document.querySelector('.tag-bulk-toggle[data-tag=\"' + tagName + '\"]');",
    "          var tcount = document.querySelector('[data-tag-count=\"' + tagName + '\"]');",
    "          if(ttoggle) {",
    "            ttoggle.checked = (tc.count === tc.total) && (tc.total > 0);",
    "            ttoggle.indeterminate = (tc.count > 0) && (tc.count < tc.total);",
    "          }",
    "          if(tcount) tcount.textContent = tc.count + ' of ' + tc.total;",
    "        }",
    "      }",

    // Apply channel logos from a logo map (key → URL). Sets data-logo attributes on matching rows so processLogos() can render the images.
    "      if(patch.logos) {",
    "        for(var key in patch.logos) {",
    "          var logoRow = document.getElementById('display-row-' + key);",
    "          if(logoRow) {",
    "            var nameCell = logoRow.querySelector('.channel-name-cell');",
    "            if(nameCell && nameCell.parentElement) nameCell.parentElement.setAttribute('data-logo', patch.logos[key]);",
    "          }",
    "        }",
    "      }",

    // Post-update: refilter rows for the service filter and render any new logos. Row insertions reset the select options (Safari ignores hidden), so the
    // filter must be re-applied. Logo rendering handles both logo-only patches and newly-inserted rows.
    "      if(patch.rows && patch.rows.length > 0) this.refilter();",
    "      if(patch.logos || (patch.rows && patch.rows.length > 0)) this.processLogos();",
    "    },",

    // Insert or replace a channel row in the table. Always removes existing rows with the same key first (handles edits and overrides of builtin channels).
    // Uses the current table sort field and direction for correct insertion order.
    "    insertRow: function(html, key) {",
    "      var tbody = document.querySelector('.channel-table tbody');",
    "      var table = document.querySelector('.channel-table');",
    "      if(!tbody || !html || !table) return;",
    "      var oldDisplay = document.getElementById('display-row-' + key);",
    "      var oldEdit = document.getElementById('edit-row-' + key);",
    "      if(oldEdit) oldEdit.remove();",
    "      if(oldDisplay) oldDisplay.remove();",
    "      var temp = document.createElement('tbody');",
    "      temp.innerHTML = html.displayRow + (html.editRow || '');",
    "      var newDisplayRow = temp.firstElementChild;",
    "      var newEditRow = temp.children[1] || null;",
    "      var sortField = table.getAttribute('data-sort-field') || 'name';",
    "      var sortDir = table.getAttribute('data-sort-dir') || 'asc';",
    "      var newVal = this._getSortValue(newDisplayRow, sortField);",
    "      var rows = tbody.querySelectorAll('tr[id^=\"display-row-\"]');",
    "      var inserted = false;",
    "      for(var i = 0; i < rows.length; i++) {",
    "        var rowVal = this._getSortValue(rows[i], sortField);",
    "        var cmp = (sortDir === 'asc') ? (newVal < rowVal) : (newVal > rowVal);",
    "        if(cmp) {",
    "          tbody.insertBefore(newDisplayRow, rows[i]);",
    "          if(newEditRow) tbody.insertBefore(newEditRow, rows[i]);",
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
    "    removeRow: function(key) {",
    "      var displayRow = document.getElementById('display-row-' + key);",
    "      var editRow = document.getElementById('edit-row-' + key);",
    "      if(displayRow) displayRow.remove();",
    "      if(editRow) editRow.remove();",
    "    },",

    /* Apply the service filter to all channel rows. Hides rows whose service tags aren't enabled and filters service dropdown options for multi-service channels.
     * Uses a persistent _allOptions array on each select to remember all server-rendered options across filter applications — the server marks disabled options
     * with the hidden attribute, but Safari ignores hidden on option elements, so we rebuild the select with only enabled options each time. Selection restore
     * priority: (1) saved server choice (HTML selected attribute), (2) previous visual selection, (3) first option.
     */
    "    filter: function(enabledTags) {",
    "      var rows = document.querySelectorAll('tr[data-provider-tags]');",
    "      for(var i = 0; i < rows.length; i++) {",
    "        var tags = rows[i].getAttribute('data-provider-tags').split(',');",
    "        var available = true;",
    "        if(enabledTags.length > 0) {",
    "          available = false;",
    "          for(var j = 0; j < tags.length; j++) {",
    "            if(tags[j] === 'direct' || enabledTags.indexOf(tags[j]) !== -1) { available = true; break; }",
    "          }",
    "        }",
    "        if(available) rows[i].classList.remove('channel-unavailable');",
    "        else rows[i].classList.add('channel-unavailable');",
    "        var label = rows[i].querySelector('.no-provider-label');",
    "        var sel = rows[i].querySelector('.provider-select');",
    "        var name = rows[i].querySelector('.provider-name');",
    "        if(label) label.style.display = available ? 'none' : '';",
    "        if(name) name.style.display = available ? '' : 'none';",
    "        if(sel) {",
    "          sel.style.display = available ? '' : 'none';",
    "          if(!sel._allOptions) sel._allOptions = Array.prototype.slice.call(sel.querySelectorAll('option'));",
    "          var prevValue = sel.value;",
    "          sel.innerHTML = '';",
    "          var serverDefault = null;",
    "          var prevExists = false;",
    "          for(var k = 0; k < sel._allOptions.length; k++) {",
    "            var opt = sel._allOptions[k];",
    "            var oTag = opt.getAttribute('data-provider-tag');",
    "            var show = (enabledTags.length === 0) || oTag === 'direct' || enabledTags.indexOf(oTag) !== -1;",
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

    // Re-apply the service filter using the current checkbox state from the filter dropdown menu. Called after row insertions because fresh server HTML contains
    // all options with hidden attributes (which Safari ignores), so the filter must be re-established.
    "    refilter: function() {",
    "      var menu = document.querySelector('.provider-dropdown-menu');",
    "      if(!menu) return;",
    "      var cbs = menu.querySelectorAll('input[type=\"checkbox\"]:not(:disabled)');",
    "      var enabledTags = [];",
    "      var allChecked = true;",
    "      for(var i = 0; i < cbs.length; i++) {",
    "        if(cbs[i].checked) enabledTags.push(cbs[i].getAttribute('data-tag'));",
    "        else allChecked = false;",
    "      }",
    "      if(allChecked) enabledTags = [];",
    "      if(enabledTags.length > 0) this.filter(enabledTags);",
    "    },",

    // Sort the channel table by the specified field. Toggles direction if the same field is clicked again. Persists the sort preference to the server.
    "    sort: function(field) {",
    "      var table = document.querySelector('.channel-table');",
    "      if(!table) return;",
    "      var currentField = table.getAttribute('data-sort-field');",
    "      var currentDir = table.getAttribute('data-sort-dir') || 'asc';",
    "      var dir = (field === currentField) ? (currentDir === 'asc' ? 'desc' : 'asc') : 'asc';",
    "      table.setAttribute('data-sort-field', field);",
    "      table.setAttribute('data-sort-dir', dir);",
    "      var tbody = table.querySelector('tbody');",
    "      var displayRows = tbody.querySelectorAll('tr[id^=\"display-row-\"]');",
    "      var pairs = [];",
    "      for(var i = 0; i < displayRows.length; i++) {",
    "        var dr = displayRows[i];",
    "        var key = dr.id.replace('display-row-', '');",
    "        var er = document.getElementById('edit-row-' + key);",
    "        var name = dr.children[1] ? dr.children[1].textContent.trim().toLowerCase() : '';",
    "        pairs.push({ displayRow: dr, editRow: er, val: this._getSortValue(dr, field), name: name });",
    "      }",

    // Secondary sort by channel name stabilizes rows with identical primary values. Always ascending regardless of primary direction so groups maintain
    // consistent alphabetical order within the sort.
    "      pairs.sort(function(a, b) {",
    "        var cmp = (dir === 'asc') ? a.val.localeCompare(b.val) : b.val.localeCompare(a.val);",
    "        if(cmp !== 0) return cmp;",
    "        return a.name.localeCompare(b.name);",
    "      });",
    "      for(var j = 0; j < pairs.length; j++) {",
    "        tbody.appendChild(pairs[j].displayRow);",
    "        if(pairs[j].editRow) tbody.appendChild(pairs[j].editRow);",
    "      }",

    // Update header sort indicators. Targets .sort-label within each th so other children (like tag filter dropdowns) are untouched.
    "      var headers = table.querySelectorAll('th.sortable');",
    "      for(var h = 0; h < headers.length; h++) {",
    "        var th = headers[h];",
    "        var hField = th.getAttribute('data-sort-field');",
    "        var sortLabel = th.querySelector('.sort-label');",
    "        if(!sortLabel) continue;",
    "        var label = sortLabel.textContent.replace(/[\\u25B2\\u25BC]/g, '').trim();",
    "        if(hField === field) sortLabel.innerHTML = label + (dir === 'asc' ? ' &#9650;' : ' &#9660;');",
    "        else sortLabel.textContent = label;",
    "      }",
    "      fetch('/config/channels/display-prefs', {",
    "        method: 'POST', headers: { 'Content-Type': 'application/json' },",
    "        body: JSON.stringify({ sortField: field, sortDirection: dir })",
    "      }).catch(function() {});",
    "    },",

    // Process channel logo data attributes. Finds all td[data-logo] elements and renders their channel-name-cell spans via channelDisplayHtml in 'both' mode.
    // Called on page load and after any DOM mutation that inserts server-rendered channel rows (add, edit, delete-with-replacement).
    "    processLogos: function() {",
    "      var cells = document.querySelectorAll('td[data-logo]');",
    "      for(var i = 0; i < cells.length; i++) {",
    "        var cell = cells[i];",
    "        if(cell.getAttribute('data-logo-processed')) continue;",
    "        var url = cell.getAttribute('data-logo');",
    "        var nameSpan = cell.querySelector('.channel-name-cell');",
    "        if(url && nameSpan) {",
    "          nameSpan.innerHTML = channelDisplayHtml(url, nameSpan.textContent || '', 'channel-table-logo', 'channel-table-text', 'both');",
    "          cell.setAttribute('data-logo-processed', '1');",
    "        }",
    "      }",
    "    }",
    "  };",

    "})();",
    "</script>"
  ].join("\n");
}
