/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * consent.ts: Automatic consent-overlay and interstitial handling during tuning for PrismCast.
 */
import { LOG, delay } from "../utils/index.ts";
import { CONFIG } from "../config/index.ts";
import type { Page } from "puppeteer-core";
import type { ResolvedSiteProfile } from "../types/index.ts";

/* This module is the single source of truth for getting consent overlays and interstitials out of the way during a tune. Three distinct overlay classes are
 * handled, each with its own correct action, because "consent overlay" is not one homogeneous pattern:
 *
 * 1. Cookie-consent CMP banners (Didomi, OneTrust, ...). These are vendor-standardized: every site using a given vendor renders the same markup, so a small,
 *    vendor-keyed registry (CMP_REGISTRY) - not a per-site whitelist - covers thousands of sites with a handful of entries. The correct action is to REJECT, which
 *    dismisses the banner and is the privacy-protective choice. Adding a vendor is one entry.
 *
 * 2. Embedded-player consent gates. These are the GDPR "2-click embed" / content-blocker pattern: the site refuses to create the third-party player iframe (and
 *    send the viewer to the embed provider) until the viewer consents, painting its own placeholder overlay in the meantime. The markup is SITE-BESPOKE (france24's
 *    `.o-em-consent`, another site's different class), so there is no global selector - we detect it by SHAPE (a player-blocking overlay whose text matches the
 *    2-click-embed phrasing AND which carries an accept affordance). The correct action is to ACCEPT, since the gated content cannot play otherwise. Because the
 *    real player iframe is only created after consent is granted, acceptance is paired with a single page reload by the caller so the player resolves on the second
 *    pass (consent persists in the Chrome profile, exactly like a TV-provider login).
 *
 * 3. Per-site intermittent modals (the existing `dismissSelector` mechanism - Paramount+ "Watch Live", c-span, cbs). These are arbitrary, site-specific modals, so
 *    they remain per-site configuration rather than a global pattern. They are folded into this one poll so a single mechanism - and a single auto-dismiss logging
 *    convention - owns all "click an overlay out of the way during the video wait" behavior.
 *
 * The detection scan is specific by design: the embed-gate heuristic only fires when its 2-click-embed phrasing matches AND no video is already playing, so a
 * normal tune (where the video resolves without an overlay) never triggers an accept. Where the heuristic cannot resolve a blocking overlay, the caller falls back
 * to detect-and-guide: it surfaces an actionable message telling the viewer to dismiss the prompt once in setup/login mode, instead of a cryptic selector timeout.
 */

// Interval in milliseconds between overlay-handling poll checks. Matches the cadence of the legacy dismiss-modal poll this mechanism replaces.
const OVERLAY_POLL_INTERVAL = 500;

// Settle delay in milliseconds after scrolling an element into view, before dispatching a coordinate click. Mirrors the shared scrollAndClick helper's settle so
// lazy-loaded content and scroll animations finish before the pointer-event chain is dispatched. Named here (matching OVERLAY_POLL_INTERVAL's convention) rather than
// repeated as a bare literal across the two coordinate-click sites.
const SCROLL_SETTLE_DELAY = 200;

// Source for the accept-affordance match - the visible text or aria-label of a button that grants consent. Kept narrow (no "watch"/"play"/"continue") so a generic
// media-play or navigation button is never mistaken for a consent accept; the embed-gate container phrasing below is the second, decisive guard. Passed to the
// browser context as a string because a RegExp cannot cross the page.evaluate boundary.
const ACCEPT_AFFORDANCE_SOURCE = "\\b(accept|agree|allow|enable|consent|got it|i understand|load)\\b";

// Source for the embedded-player consent-gate phrasing - the "2-click embed" / content-blocker wording a site uses when it withholds a third-party player until the
// viewer enables tracking. This is the decisive specificity guard: a button is only treated as an embed-gate accept when an ancestor container carries this phrasing.
// It is deliberately limited to tracking / external-content wording and does NOT include generic "to view this content" phrasing, because that is also how age gates,
// paywalls, and TV-provider sign-in walls read - and those must never be auto-accepted. Anything outside this narrow shape degrades to detect-and-guide.
const EMBED_GATE_SOURCE = "(enable .{0,40}(tracking|cookies)|advertisement tracking|audience measurement|" +
  "(load|show|display|view|enable) (external|third.?party) content)";

// Source for the anti-pattern exclusion - phrasing that marks an overlay as a sign-in, subscription, age, or paywall gate rather than a privacy-consent gate. A
// candidate is skipped when its label or container text matches this, so even a wall that also happens to mention tracking is never auto-accepted; it routes to
// detect-and-guide instead. Auto-accepting tracking is the privacy-sensitive action, so it is only ever performed on an unambiguous consent overlay.
const EXCLUDE_SOURCE = "\\b(sign[ -]?in|log[ -]?in|subscribe|subscription|paywall|provider|account|18\\+|adults?|age[ -]?gate|years? old|" +
  "must be (18|over|of)|create an? (free )?account)\\b";

/**
 * A known cookie-consent management platform (CMP) vendor. Detection and the reject affordance are both vendor-standardized selectors, so one entry covers every
 * site using that vendor. Only vendors whose reject selector has been verified against a live banner are seeded here; the list grows as vendors are verified.
 */
interface CmpVendor {

  // CSS selector that identifies the vendor's banner container. Informational - used by consentOverlayPresent to recognize a banner that has no actionable reject.
  readonly detect: string;

  // CSS selector for the vendor's "reject"/"deny" button. Clicking this dismisses the banner without granting tracking consent.
  readonly reject: string;

  // Human-readable vendor name, used in the auto-dismiss log line.
  readonly vendor: string;
}

/**
 * Vendor-keyed registry of known cookie-consent CMPs. This is NOT a per-site whitelist - each entry covers every site that embeds the vendor. Seeded with Didomi,
 * whose reject selector was verified live (france24); additional vendors (OneTrust, Cookiebot, ...) are added here as their reject selectors are verified.
 */
const CMP_REGISTRY: readonly CmpVendor[] = [

  { detect: ".didomi-popup-notice, #didomi-host", reject: "#didomi-notice-disagree-button", vendor: "Didomi" }
];

/**
 * The kind of overlay that was automatically handled, for the unified auto-dismiss log convention.
 */
type AutoDismissKind = "cookie-consent" | "embed-gate" | "modal";

/**
 * Structured detail for an auto-dismiss event. Every field is optional; callers supply whichever identifies the overlay they acted on.
 */
interface AutoDismissDetail {

  // Short snippet of the accepted control's label, for embed-gate acceptances.
  readonly label?: string;

  // The CSS selector that matched, for CMP rejects and per-site modal dismissals.
  readonly selector?: string;

  // The CMP vendor name, for cookie-consent rejects.
  readonly vendor?: string;
}

/**
 * Options for startOverlayHandling().
 */
interface StartOverlayHandlingOptions {

  // Invoked once, when an embedded-player consent gate is accepted. The caller uses this to abandon the in-flight video wait and reload the page so the now-
  // permitted player iframe is created and the video resolves on the second pass.
  readonly onEmbedGateAccepted: () => void;

  // Aborts the poll early. The caller signals this once the video wait settles (success or failure) so the poll stops interacting with a page that is already done.
  readonly signal?: AbortSignal;
}

/**
 * The result of a single overlay-handling poll tick.
 *
 * - "gate": an embedded-player consent gate was accepted; the poll has signaled the caller and should stop.
 * - "stop": the page navigated or closed; the poll should stop.
 * - "continue": nothing actionable this tick (or only a non-terminal action like a CMP reject); keep polling.
 */
type OverlayTickResult = "continue" | "gate" | "stop";

/**
 * Mutable per-poll state, threaded across ticks so each overlay is acted on at most once.
 */
interface OverlayPollState {

  // True once the profile's per-site dismissSelector modal has been clicked, so it is not clicked again on later ticks.
  dismissSelectorHandled: boolean;

  // Vendor names whose CMP banner has already been rejected, so a vendor is not re-clicked on later ticks.
  readonly handledVendors: Set<string>;
}

/**
 * Emits the unified auto-dismiss log line. Every path that automatically acts on an overlay during tuning - CMP reject, embed-gate accept, per-site modal dismiss,
 * and the Comcast "Watch Now" modal - routes through here so the behavior is reported consistently at INFO with the same voice, plus a verbose DEBUG companion under
 * the browser:consent category. This is the single source of truth for "we interacted with an on-page prompt on the viewer's behalf" reporting.
 * @param kind - Which overlay class was handled.
 * @param detail - Identifying detail for the overlay (vendor, selector, or label).
 */
export function logAutoDismiss(kind: AutoDismissKind, detail: AutoDismissDetail = {}): void {

  switch(kind) {

    case "cookie-consent": {

      LOG.info("Automatically rejected the %s cookie-consent prompt.", detail.vendor ?? "site");

      break;
    }

    case "embed-gate": {

      LOG.info("Automatically accepted an embedded-player consent prompt so the video could load.");

      break;
    }

    case "modal": {

      LOG.info("Automatically dismissed an interstitial modal during tuning.");

      break;
    }
  }

  LOG.debug("browser:consent", "Auto-dismiss detail: kind=%s, vendor=%s, selector=%s, label=%s.", kind, detail.vendor ?? "-", detail.selector ?? "-",
    detail.label ?? "-");
}

/**
 * Clicks an element identified by a CSS selector using a real coordinate mouse click, if the element is present and laid out. Coordinate clicking generates the
 * full pointer event chain, which consent buttons in SPA frameworks require to register (a synthetic element.click() is silently dropped by some, as observed on
 * the embed-gate accept button). Returns false when the element is absent or has zero layout size, so callers can keep polling.
 * @param page - The Puppeteer page.
 * @param selector - The CSS selector for the element to click.
 * @returns True if the element was found and clicked.
 */
async function clickSelectorByCoordinate(page: Page, selector: string): Promise<boolean> {

  const target = await page.evaluate((sel: string): { x: number; y: number } | null => {

    const el = document.querySelector(sel);

    if(!el) {

      return null;
    }

    const rect = el.getBoundingClientRect();

    // A present-but-unlaid-out element (display:none, zero size) cannot be coordinate-clicked; skip it and let the caller poll again.
    if((rect.width === 0) || (rect.height === 0)) {

      return null;
    }

    el.scrollIntoView({ block: "center" });

    const scrolled = el.getBoundingClientRect();

    return { x: scrolled.x + (scrolled.width / 2), y: scrolled.y + (scrolled.height / 2) };
  }, selector);

  if(!target) {

    return false;
  }

  // Brief settle delay after scrolling, mirroring the shared scrollAndClick helper, before dispatching the real pointer-event chain.
  await delay(SCROLL_SETTLE_DELAY);
  await page.mouse.click(target.x, target.y);

  return true;
}

/**
 * Clicks an element identified by a CSS selector using a synthetic in-page click. This preserves the exact behavior of the legacy dismiss-modal poll for per-site
 * dismissSelector modals (Paramount+ "Watch Live", c-span, cbs), which were validated against synthetic clicks; coordinate clicking is reserved for the consent
 * paths that require it. Returns false when the element is absent.
 * @param page - The Puppeteer page.
 * @param selector - The CSS selector for the modal element to dismiss.
 * @returns True if the element was found and clicked.
 */
async function clickSelectorSynthetic(page: Page, selector: string): Promise<boolean> {

  return page.evaluate((sel: string): boolean => {

    const el = document.querySelector(sel);

    if(el) {

      (el as HTMLElement).click();

      return true;
    }

    return false;
  }, selector);
}

/**
 * Locates an embedded-player consent gate and returns the coordinates of its accept control, or null when none is present. A gate is recognized by shape: a button
 * (or button-role element) whose label matches the accept affordance AND whose ancestor container carries the 2-click-embed phrasing. The scan early-outs when a
 * video is already playable, so it never fires on a tune that is succeeding without an overlay. The matched control is scrolled into view and its viewport-center
 * coordinates returned for a real coordinate click.
 * @param page - The Puppeteer page.
 * @returns The accept control's center coordinates and a short label, or null when no gate is detected.
 */
async function locateEmbedGate(page: Page): Promise<{ label: string; x: number; y: number } | null> {

  return page.evaluate((args: { accept: string; exclude: string; gate: string }): { label: string; x: number; y: number } | null => {

    // If a video element is already buffered for playback, the page is not blocked by a consent gate; do not act.
    const existing = document.querySelector("video");

    if(existing && (existing.readyState >= 3)) {

      return null;
    }

    const acceptRe = new RegExp(args.accept, "i");
    const excludeRe = new RegExp(args.exclude, "i");
    const gateRe = new RegExp(args.gate, "i");
    const candidates = Array.from(document.querySelectorAll("button, [role=\"button\"], a"));

    for(const candidate of candidates) {

      const label = (candidate.textContent + " " + (candidate.getAttribute("aria-label") ?? "")).trim();

      if(!acceptRe.test(label)) {

        continue;
      }

      // Walk up a bounded number of ancestors looking for the container that carries the 2-click-embed phrasing. The bound keeps the search cheap and prevents a
      // match against the whole document body, which would defeat the specificity guard.
      let node: Element | null = candidate.parentElement;
      let containerText = "";

      for(let depth = 0; node && (depth < 6); depth++) {

        if(gateRe.test(node.textContent)) {

          containerText = node.textContent;

          break;
        }

        node = node.parentElement;
      }

      if(!containerText) {

        continue;
      }

      // Skip sign-in, subscription, age, and paywall walls even when they happen to mention tracking. Auto-accepting tracking is the privacy-sensitive action and is
      // only ever performed on an unambiguous consent overlay; an ambiguous wall routes to detect-and-guide instead.
      if(excludeRe.test(label) || excludeRe.test(containerText)) {

        continue;
      }

      const rect = candidate.getBoundingClientRect();

      if((rect.width === 0) || (rect.height === 0)) {

        continue;
      }

      candidate.scrollIntoView({ block: "center" });

      const scrolled = candidate.getBoundingClientRect();

      return { label: label.slice(0, 80), x: scrolled.x + (scrolled.width / 2), y: scrolled.y + (scrolled.height / 2) };
    }

    return null;
  }, { accept: ACCEPT_AFFORDANCE_SOURCE, exclude: EXCLUDE_SOURCE, gate: EMBED_GATE_SOURCE });
}

/**
 * Reports whether a consent or cookie prompt is currently present on the page. Used on the failure path to decide whether to replace a cryptic video-selector
 * timeout with actionable detect-and-guide messaging. A prompt is "present" when a known CMP banner is detected or an embedded-player consent gate is located.
 * @param page - The Puppeteer page.
 * @returns True if a consent overlay is present.
 */
export async function consentOverlayPresent(page: Page): Promise<boolean> {

  try {

    const cmpPresent = await page.evaluate((selectors: readonly string[]): boolean => {

      return selectors.some((sel) => document.querySelector(sel) !== null);
    }, CMP_REGISTRY.map((vendor) => vendor.detect));

    if(cmpPresent) {

      return true;
    }

    return (await locateEmbedGate(page)) !== null;
  } catch {

    // The page navigated or closed while probing; treat as no detectable overlay.
    return false;
  }
}

/**
 * Runs a single overlay-handling poll tick: reject any known CMP cookie banner, accept an embedded-player consent gate (terminal), then dismiss a per-site modal.
 * Ordering matters - the embed gate often only becomes interactable after the cookie banner above it is dismissed, so the CMP reject runs first each tick.
 * @param page - The Puppeteer page.
 * @param profile - The resolved site profile (supplies the optional per-site dismissSelector).
 * @param state - Mutable per-poll state so each overlay is acted on at most once.
 * @returns The tick outcome - "gate" when an embed gate was accepted, "stop" when the page is gone, otherwise "continue".
 */
async function runOverlayTick(page: Page, profile: ResolvedSiteProfile, state: OverlayPollState): Promise<OverlayTickResult> {

  try {

    // Reject any known cookie-consent banner. Each vendor is acted on at most once.
    for(const vendor of CMP_REGISTRY) {

      if(state.handledVendors.has(vendor.vendor)) {

        continue;
      }

      // eslint-disable-next-line no-await-in-loop
      const rejected = await clickSelectorByCoordinate(page, vendor.reject);

      if(rejected) {

        state.handledVendors.add(vendor.vendor);

        logAutoDismiss("cookie-consent", { selector: vendor.reject, vendor: vendor.vendor });
      }
    }

    // Accept an embedded-player consent gate. This is terminal: the player iframe is only created after consent, so the caller must reload to resolve the video.
    const gate = await locateEmbedGate(page);

    if(gate) {

      await delay(SCROLL_SETTLE_DELAY);
      await page.mouse.click(gate.x, gate.y);

      logAutoDismiss("embed-gate", { label: gate.label });

      return "gate";
    }

    // Dismiss the profile's per-site intermittent modal, if configured, using a synthetic click to preserve the legacy behavior.
    if(profile.dismissSelector && !state.dismissSelectorHandled) {

      const dismissed = await clickSelectorSynthetic(page, profile.dismissSelector);

      if(dismissed) {

        state.dismissSelectorHandled = true;

        logAutoDismiss("modal", { selector: profile.dismissSelector });
      }
    }

    return "continue";
  } catch {

    // The page navigated or closed mid-tick. Stop polling silently.
    return "stop";
  }
}

/**
 * Launches the fire-and-forget overlay-handling poll for a tune. Runs concurrently with the video wait, never blocks it, and never throws - it is the unified
 * replacement for the legacy dismiss-modal poll, additionally rejecting known cookie banners and accepting embedded-player consent gates. The poll window matches
 * the video-wait window so a late-rendering gate is still caught; the caller's abort signal stops it as soon as the wait settles.
 *
 * When an embedded-player consent gate is accepted, onEmbedGateAccepted() is invoked and the poll stops, because the gate's acceptance only takes effect on a fresh
 * load - the caller abandons the in-flight wait and reloads.
 * @param page - The Puppeteer page.
 * @param profile - The resolved site profile.
 * @param options - The embed-gate callback and abort signal. See StartOverlayHandlingOptions.
 */
export async function startOverlayHandling(page: Page, profile: ResolvedSiteProfile, options: StartOverlayHandlingOptions): Promise<void> {

  const { onEmbedGateAccepted, signal } = options;
  const state: OverlayPollState = { dismissSelectorHandled: false, handledVendors: new Set<string>() };

  // Poll for as long as the video wait could run, so a gate that renders late is still handled. The signal cuts this short the instant the wait settles.
  const deadline = Date.now() + (profile.videoTimeout ?? CONFIG.streaming.videoTimeout);

  let firstCheck = true;

  while(Date.now() < deadline) {

    if(signal?.aborted) {

      return;
    }

    // The first check is immediate; subsequent checks are spaced by the poll interval.
    if(!firstCheck) {

      // eslint-disable-next-line no-await-in-loop
      await delay(OVERLAY_POLL_INTERVAL);

      if(signal?.aborted) {

        return;
      }
    }

    firstCheck = false;

    // eslint-disable-next-line no-await-in-loop
    const result = await runOverlayTick(page, profile, state);

    if(result === "gate") {

      onEmbedGateAccepted();

      return;
    }

    if(result === "stop") {

      return;
    }
  }
}
