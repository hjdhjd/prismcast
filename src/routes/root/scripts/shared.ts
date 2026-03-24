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

    // Close all open dropdown menus and remove the scroll and resize listeners.
    "  function closeDropdowns() {",
    "    var menus = document.querySelectorAll('.dropdown-menu.show');",
    "    for (var i = 0; i < menus.length; i++) menus[i].classList.remove('show');",
    "    window.removeEventListener('scroll', closeDropdowns, true);",
    "    window.removeEventListener('resize', closeDropdowns);",
    "  };",

    "  window.closeDropdowns = closeDropdowns;",

    // Toggle a dropdown menu open or closed. On first call for a given button, the menu is detached from its .dropdown parent and portaled to <body> with
    // position: fixed. This allows the menu to escape overflow: auto containers like the channel table wrapper. On every open, the menu is positioned relative
    // to the button's bounding rect, with edge-of-viewport clamping and above-button flip when it would extend below the viewport.
    "  window.toggleDropdown = function(btn) {",
    "    var menu = btn._portalMenu;",
    "    var isOpen = menu && menu.classList.contains('show');",
    "    closeDropdowns();",
    "    if (isOpen) return;",
    "    if (!menu) {",
    "      menu = btn.nextElementSibling;",
    "      if (!menu) return;",
    "      btn._portalMenu = menu;",
    "      document.body.appendChild(menu);",
    "      menu.style.position = 'fixed';",
    "      menu.style.marginTop = '0';",
    "    }",
    "    menu.classList.add('show');",
    "    var rect = btn.getBoundingClientRect();",
    "    var top = rect.bottom + 2;",
    "    var left = rect.left;",
    "    if (left + menu.offsetWidth > window.innerWidth - 4) left = rect.right - menu.offsetWidth;",
    "    if (left < 4) left = 4;",
    "    if (top + menu.offsetHeight > window.innerHeight - 4) top = rect.top - menu.offsetHeight - 2;",
    "    menu.style.top = top + 'px';",
    "    menu.style.left = left + 'px';",
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
    "        'onerror=\"this.style.display=\\'none\\'\">' +",
    "        '<span class=\"' + textClass + '\">' + name + '</span>';",
    "    }",
    "    return '<img src=\"' + logoUrl + '\" class=\"' + logoClass + '\" alt=\"' + name + '\" title=\"' + name + '\" ' +",
    "      'onerror=\"this.style.display=\\'none\\';this.nextElementSibling.style.display=\\'inline\\'\">' +",
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

    // Provider icon renderer with three modes, mirroring channelDisplayHtml. The icon source chain is: iconUrl (if specified) → Apple touch icon → favicon.
    // Fallback URLs are stored in a data-fallbacks attribute and processed by the shared imgFallback handler.
    "  window.providerIconHtml = function(domain, name, iconClass, textClass, mode, iconUrl) {",
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

    // Process channel logo data attributes. Finds all td[data-logo] elements and renders their channel-name-cell spans via channelDisplayHtml in 'both' mode.
    // Called on page load and after any DOM mutation that inserts server-rendered channel rows (add, edit, delete-with-replacement).
    "  window.processChannelLogos = function() {",
    "    var cells = document.querySelectorAll('td[data-logo]');",
    "    for(var i = 0; i < cells.length; i++) {",
    "      var cell = cells[i];",
    "      if(cell.getAttribute('data-logo-processed')) continue;",
    "      var url = cell.getAttribute('data-logo');",
    "      var nameSpan = cell.querySelector('.channel-name-cell');",
    "      if(url && nameSpan) {",
    "        nameSpan.innerHTML = channelDisplayHtml(url, nameSpan.textContent || '', 'channel-table-logo', 'channel-table-text', 'both');",
    "        cell.setAttribute('data-logo-processed', '1');",
    "      }",
    "    }",
    "  };",

    // Process provider display spans. Finds all .provider-display elements and renders them via providerIconHtml in 'both' mode. Called on page load and after
    // any DOM mutation that introduces new provider display elements (chip rebuild, filter updates).
    "  window.processProviderDisplays = function() {",
    "    var els = document.querySelectorAll('.provider-display');",
    "    for(var i = 0; i < els.length; i++) {",
    "      var el = els[i];",
    "      if(el.getAttribute('data-processed')) continue;",
    "      var domain = el.getAttribute('data-domain') || '';",
    "      var iconUrl = el.getAttribute('data-icon-url') || '';",
    "      var name = el.textContent || '';",
    "      var sm = el.hasAttribute('data-sm');",
    "      el.innerHTML = providerIconHtml(domain, name, sm ? 'provider-icon-sm' : 'provider-icon',",
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

    // Validate the current step and advance to a target step if valid. Supports both sync and async onValidate callbacks. When onValidate returns a thenable
    // (Promise), the controller waits for resolution before advancing. Returns true/false for sync, or a Promise resolving to true/false for async.
    "    function validateAndAdvance(target) {",
    "      var result = config.onValidate(currentStep);",
    "      if(result && typeof result.then === 'function') {",
    "        return result.then(function(err) {",
    "          if(err) { ctrl.setError(err); return false; }",
    "          advance(target);",
    "          return true;",
    "        }).catch(function() { ctrl.setError('Validation failed.'); return false; });",
    "      }",
    "      if(result) { ctrl.setError(result); return false; }",
    "      advance(target);",
    "      return true;",
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

    "})();",
    "</script>"
  ].join("\n");
}
