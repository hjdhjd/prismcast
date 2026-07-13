/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * consent.ts: Automatic consent-overlay and interstitial handling for externally navigated pages in PrismCast.
 */
import { LOG, delay, realClock } from "../utils/index.ts";
import { CONFIG } from "../config/index.ts";
import type { Clock } from "../utils/index.ts";
import type { Page } from "puppeteer-core";
import type { ResolvedSiteProfile } from "../types/index.ts";

/* This module is the single source of truth for getting consent overlays and interstitials out of the way on any page PrismCast navigates on the viewer's behalf -
 * a tune's channel-selection walk and video wait, a discovery or precache walk, a static capture. Three distinct overlay classes are handled, each with its own
 * correct action, because "consent overlay" is not one homogeneous pattern:
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
 *    convention - owns all "click an overlay out of the way" behavior.
 *
 * Which of these three actions a given poll may take, and how long it may run, is not a call-site decision: it is declared by an OverlayPhase and read from the
 * PHASE_POLICY table below. The video wait is the only phase that composes with an embed-gate accept - accepting a gate is a page-mutating, reload-triggering action
 * whose only safe home is the caller that owns the reload - so every other phase forbids the accept and never runs the acting gate probe. This keeps the
 * privacy-sensitive auto-accept structurally confined to the one place that can follow through on it, while cookie rejection and per-site modal dismissal run on
 * every navigated surface. Detection stays specific by design: the embed-gate heuristic only fires when its 2-click-embed phrasing matches AND no video is already
 * playing, so a normal navigation (where nothing blocks the page) never triggers an accept. Where the heuristic cannot resolve a blocking overlay, the caller falls
 * back to detect-and-guide: it surfaces an actionable message telling the viewer to dismiss the prompt once in setup/login mode, instead of a cryptic timeout.
 */

// Interval in milliseconds between overlay-handling poll checks. This cadence is responsive enough to catch a late-rendering overlay while keeping each tick cheap.
const OVERLAY_POLL_INTERVAL = 500;

// Settle delay in milliseconds after scrolling an element into view, before dispatching a coordinate click. Mirrors the shared scrollAndClick helper's settle so
// lazy-loaded content and scroll animations finish before the pointer-event chain is dispatched. Named here (matching OVERLAY_POLL_INTERVAL's convention) rather than
// repeated as a bare literal across the two coordinate-click sites.
const SCROLL_SETTLE_DELAY = 200;

// Source for the accept-affordance match - the visible text or aria-label of a button that grants consent. Kept narrow (no "watch"/"play"/"continue") so a generic
// media-play or navigation button is never mistaken for a consent accept; the embed-gate container phrasing below is the second, decisive guard. Passed to the
// browser context as a string because a RegExp cannot cross the page.evaluate boundary. Exported so scanForEmbedGate and its DOM-fixture tests consume one definition
// of the pattern rather than duplicating it.
export const ACCEPT_AFFORDANCE_SOURCE = "\\b(accept|agree|allow|enable|consent|got it|i understand|load)\\b";

// Source for the embedded-player consent-gate phrasing - the "2-click embed" / content-blocker wording a site uses when it withholds a third-party player until the
// viewer enables tracking. This is the decisive specificity guard: a button is only treated as an embed-gate accept when an ancestor container carries this phrasing.
// It is deliberately limited to tracking / external-content wording and does NOT include generic "to view this content" phrasing, because that is also how age gates,
// paywalls, and TV-provider sign-in walls read - and those must never be auto-accepted. Anything outside this narrow shape degrades to detect-and-guide. Exported so
// scanForEmbedGate and its DOM-fixture tests consume one definition of the pattern rather than duplicating it.
export const EMBED_GATE_SOURCE = "(enable .{0,40}(tracking|cookies)|advertisement tracking|audience measurement|" +
  "(load|show|display|view|enable) (external|third.?party) content)";

// Source for the anti-pattern exclusion - phrasing that marks an overlay as a sign-in, subscription, age, or paywall gate rather than a privacy-consent gate. A
// candidate is skipped when its label or container text matches this, so even a wall that also happens to mention tracking is never auto-accepted; it routes to
// detect-and-guide instead. Auto-accepting tracking is the privacy-sensitive action, so it is only ever performed on an unambiguous consent overlay. Exported so
// scanForEmbedGate and its DOM-fixture tests consume one definition of the pattern rather than duplicating it.
export const EXCLUDE_SOURCE = "\\b(sign[ -]?in|log[ -]?in|subscribe|subscription|paywall|provider|account|18\\+|adults?|age[ -]?gate|years? old|" +
  "must be (18|over|of)|create an? (free )?account)\\b";

// Selectors whose configured dismissSelector value proved to be an invalid CSS selector, tracked process-wide so the warning about a malformed selector is emitted
// exactly once per selector no matter how many poll instances encounter it. The per-poll dismiss state gates the action; this set gates the log.
const warnedInvalidSelectors = new Set<string>();

/**
 * The phase of the page's lifecycle a poll is covering. The phase - not the call site - decides which overlay actions the poll may take and how long it may run,
 * both read from PHASE_POLICY. Every externally navigated surface declares its phase where it launches the poll.
 *
 * - "discovery": a channel-discovery or precache walk; navigation plus a cold-cache guide walk, no embed-gate accept.
 * - "postGateReload": the reload the video wait triggers after accepting an embed gate; cookie rejection and modal dismissal stay live, but the gate probe is off.
 * - "staticCapture": a static-page capture whose pixels are the content; a bounded load-window poll with no abort owner.
 * - "tuneSetup": a tune's channel-selection walk and click-to-play, before the video wait; the caller aborts it at the phase boundary.
 * - "videoWait": the tune's wait for the player to become ready; the only phase that composes with an embed-gate accept and its paired reload.
 */
export type OverlayPhase = "discovery" | "postGateReload" | "staticCapture" | "tuneSetup" | "videoWait";

/**
 * The overlay actions a phase permits and the poll window it runs for. The single source of truth is the PHASE_POLICY table; no temporal or action decision lives
 * at a call site.
 */
interface PhasePolicy {

  // Whether the poll may reject a known cookie-consent CMP banner. Privacy-protective and page-safe, so every phase allows it.
  readonly cmpReject: boolean;

  // Whether the poll may dismiss the profile's per-site modal via a synthetic click. Page-safe and mouse-free, so every phase allows it.
  readonly dismissSelector: boolean;

  // Whether the poll may accept an embedded-player consent gate. Only the video wait can, because accepting a gate mutates the page and requires the paired reload
  // that only the video-wait caller performs.
  readonly embedGate: boolean;

  // The poll window: a fixed millisecond budget, or "videoTimeout" to derive the window from the profile at poll start (the video wait and its post-gate reload).
  readonly window: number | "videoTimeout";
}

/**
 * The per-phase overlay policy. This is the single source of truth for which overlay actions each phase permits AND how long that phase's poll may run. A phase
 * forbids the embed-gate accept unless its reload semantics can follow through on it, so only the video wait sets embedGate true.
 */
const PHASE_POLICY = {

  // A discovery or precache walk. Navigation plus a cold-cache guide walk can span far longer than a single page load, so the 60-second window is generous; the
  // helper aborts the poll on walk completion, so the window is the backstop, not the terminator. No embed-gate accept: discovery has no reload to pair with one.
  discovery: { cmpReject: true, dismissSelector: true, embedGate: false, window: 60000 },

  // The reload the video wait performs after accepting an embed gate. Cookie rejection and per-site modal dismissal stay live on the reloaded page, but the gate
  // probe is structurally off: a second acceptance would re-detect unrelated consent overlays elsewhere on the page (carousel tiles on a site like france24), scroll
  // one into view, and yank the main player offscreen. The window tracks the video-wait window (byte-parity with the post-gate second wait).
  postGateReload: { cmpReject: true, dismissSelector: true, embedGate: false, window: "videoTimeout" },

  // A static-page capture. The captured pixels ARE the content, so a dismissal click lands in the capture by design. This phase has no abort owner, so the bounded
  // 30-second load window - long enough for banners to render - is what stops the poll. No embed-gate accept: a static capture never reloads.
  staticCapture: { cmpReject: true, dismissSelector: true, embedGate: false, window: 30000 },

  // A tune's channel-selection walk and click-to-play, before the video wait. The caller aborts this poll at the phase boundary, so the 45-second window is only the
  // backstop. It is a deliberately independent consent-layer value, NOT coupled to setup.ts's playback-initialization race - the two may legitimately diverge, the
  // abort is the terminator and the window the backstop. No embed-gate accept: the walk phase has no reload to pair with one.
  tuneSetup: { cmpReject: true, dismissSelector: true, embedGate: false, window: 45000 },

  // The tune's wait for the player to become ready. The only phase whose reload semantics compose with an embed-gate accept, so the only phase that runs the acting
  // gate probe. The window derives from the profile's videoTimeout at poll start so a late-rendering gate is still caught; the caller's abort stops it the instant
  // the wait settles.
  videoWait: { cmpReject: true, dismissSelector: true, embedGate: true, window: "videoTimeout" }
} as const satisfies Record<OverlayPhase, PhasePolicy>;

/**
 * The set of phases whose policy permits an embed-gate accept, derived from PHASE_POLICY itself. Used only by the compile-time pin below.
 */
type EmbedGatePhase = { [P in OverlayPhase]: (typeof PHASE_POLICY)[P]["embedGate"] extends true ? P : never }[OverlayPhase];

/* Compile-time pin: the video wait is the only phase whose policy may accept an embed gate. If a policy edit ever flips a second phase's embedGate to true,
 * EmbedGatePhase widens past "videoWait", the conditional below collapses to `never`, and the assignment of `true` fails to compile - a build error rather than a
 * silent widening of the privacy-sensitive auto-accept surface. The binding is read once at module scope purely so the pin is live code, not a dead type alias.
 */
const embedGatePhaseIsVideoWaitOnly: EmbedGatePhase extends "videoWait" ? true : never = true;

void embedGatePhaseIsVideoWaitOnly;

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
 * whose reject selector was verified live (france24); additional vendors (OneTrust, Cookiebot, ...) are added here as their reject selectors are verified. Exported so
 * the DOM-fixture tests build a banner fixture from the same detect and reject selectors the poll uses, rather than duplicating the vendor's markers into the test.
 */
export const CMP_REGISTRY: readonly CmpVendor[] = [

  { detect: ".didomi-popup-notice, #didomi-host", reject: "#didomi-notice-disagree-button", vendor: "Didomi" }
];

/**
 * The kind of overlay that was automatically handled, for the unified auto-dismiss log convention.
 */
type AutoDismissKind = "cookie-consent" | "embed-gate" | "modal";

/**
 * The result of clickSelectorSynthetic's in-page attempt to dismiss a per-site modal. All three are non-empty strings so the caller MUST branch with a switch, never
 * on truthiness - "invalid-selector" and "absent" are both "no click happened" yet demand opposite handling (disable versus keep trying).
 *
 * - "absent": the selector is valid but matched no element this tick; keep the dismiss action armed for a later tick.
 * - "clicked": the element was found and clicked; the dismiss action is done for this poll.
 * - "invalid-selector": querySelector rejected the selector as malformed; the dismiss action is disabled for the rest of this poll.
 */
type SyntheticClickResult = "absent" | "clicked" | "invalid-selector";

/**
 * Structured detail for an auto-dismiss event. Every field is optional; callers supply whichever identifies the overlay they acted on.
 */
interface AutoDismissDetail {

  // Short snippet of the acted-on control's label, carried by embed-gate acceptances and text-matched modal dismissals (such as the Comcast "Watch Now" modal).
  readonly label?: string;

  // The CSS selector that matched, for CMP rejects and per-site modal dismissals.
  readonly selector?: string;

  // The CMP vendor name, for cookie-consent rejects.
  readonly vendor?: string;
}

/**
 * Base options shared by every overlay-handling phase.
 */
interface OverlayHandlingBaseOptions {

  // The clock used for the poll deadline and the inter-tick sleep. Defaults to realClock (performance.now()-based). Tests inject a fake clock so the poll's schedule
  // is deterministic without real timers.
  readonly clock?: Clock;

  // Aborts the poll early. The caller signals this once its phase settles so the poll stops interacting with a page that is already done.
  readonly signal?: AbortSignal;
}

/**
 * Options for startOverlayHandling(). A discriminated union on `phase`: only the videoWait arm carries onEmbedGateAccepted, because only that phase may accept an
 * embed gate and follow through with the paired reload. Every other phase forbids the callback at the type level, so a gate acceptance is unrepresentable outside
 * the video wait. The poll window is NOT an option - it is owned by PHASE_POLICY.
 */
export type StartOverlayHandlingOptions =
  ({ readonly onEmbedGateAccepted: () => void; readonly phase: "videoWait" } & OverlayHandlingBaseOptions) |
  ({ readonly onEmbedGateAccepted?: never; readonly phase: Exclude<OverlayPhase, "videoWait"> } & OverlayHandlingBaseOptions);

/**
 * The result of a single overlay-handling poll tick.
 *
 * - "gate": an embedded-player consent gate was accepted; the poll has signaled the caller and should stop.
 * - "stop": the page navigated or closed, the browser disconnected, or the poll was aborted mid-tick; the poll should stop.
 * - "continue": nothing terminal this tick (or only a non-terminal action like a CMP reject, or a transient in-walk navigation error); keep polling.
 */
type OverlayTickResult = "continue" | "gate" | "stop";

/**
 * Mutable per-poll state, threaded across ticks so each overlay is acted on at most once.
 */
interface OverlayPollState {

  // The lifecycle of the profile's per-site dismissSelector modal within this poll. "armed" until the modal is clicked ("done") or the selector proves malformed
  // ("disabled"); both terminal states suppress further attempts for the rest of the poll.
  dismissSelector: "armed" | "disabled" | "done";

  // Vendor names whose CMP banner has already been rejected, so a vendor is not re-clicked on later ticks.
  readonly handledVendors: Set<string>;
}

/**
 * Options for runOverlayTick(): the phase's policy, the mutable per-poll state, and the caller's abort signal.
 */
interface OverlayTickOptions {

  // The acting phase's policy - which actions this tick may take.
  readonly policy: PhasePolicy;

  // The caller's abort signal, checked between action groups so an outgoing poll's in-flight tick cannot double-act against the next phase's poll.
  readonly signal?: AbortSignal;

  // Mutable per-poll state so each overlay is acted on at most once.
  readonly state: OverlayPollState;
}

/**
 * Emits the unified auto-dismiss log line. Every path that automatically acts on an overlay - CMP reject, embed-gate accept, per-site modal dismiss, and the Comcast
 * "Watch Now" modal - routes through here so the behavior is reported consistently at INFO with the same voice, plus a verbose DEBUG companion under the
 * browser:consent category. This is the single source of truth for "we interacted with an on-page prompt on the viewer's behalf" reporting.
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

      LOG.info("Automatically dismissed an interstitial modal.");

      break;
    }
  }

  LOG.debug("browser:consent", "Auto-dismiss detail: kind=%s, vendor=%s, selector=%s, label=%s.", kind, detail.vendor ?? "-", detail.selector ?? "-",
    detail.label ?? "-");
}

/**
 * In-page coordinate resolver for a selector-identified element. Returns the viewport-center coordinates of the element after scrolling it into view, or null when the
 * element is absent or has zero layout size (display:none, an unlaid-out node) and therefore cannot be coordinate-clicked. This is the read half of
 * clickSelectorByCoordinate: it resolves where to click, and the Node-side caller dispatches the real pointer-event chain at those coordinates.
 *
 * This function crosses the page.evaluate boundary by source serialization, so it is self-contained: it references only its argument and the page's document global,
 * with every input a parameter and zero outer-scope references - a captured variable would be undefined once the function is stringified into the page. Exported so
 * unit tests can run it against a synthetic DOM.
 * @param sel - The CSS selector for the element to locate.
 * @returns The element's post-scroll viewport-center coordinates, or null when it is absent or has zero layout size.
 */
export function locateSelectorCoordinate(sel: string): { x: number; y: number } | null {

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

  const target = await page.evaluate(locateSelectorCoordinate, selector);

  if(!target) {

    return false;
  }

  // Brief settle delay after scrolling, mirroring the shared scrollAndClick helper, before dispatching the real pointer-event chain. This paces real page physics
  // inside an action and is exercised only when a click actually dispatches, so it stays on delay() rather than the injected poll clock.
  await delay(SCROLL_SETTLE_DELAY);
  await page.mouse.click(target.x, target.y);

  return true;
}

/**
 * In-page synthetic click for a selector-identified element. Guards its querySelector so a malformed selector is reported as "invalid-selector" rather than throwing
 * and killing the poll tick. Reports "clicked" when the element is found and clicked, and "absent" when a valid selector matches nothing this tick.
 *
 * This function crosses the page.evaluate boundary by source serialization, so it is self-contained: it references only its argument and the page's document global,
 * with every input a parameter and zero outer-scope references - a captured variable would be undefined once the function is stringified into the page. Exported so
 * unit tests can run it against a synthetic DOM.
 * @param sel - The CSS selector for the element to dismiss.
 * @returns "clicked" when the element was found and clicked, "absent" when the valid selector matched nothing, "invalid-selector" when the selector is malformed.
 */
export function clickSelectorInPage(sel: string): SyntheticClickResult {

  let el: Element | null;

  try {

    el = document.querySelector(sel);
  } catch {

    // querySelector throws a SyntaxError on a malformed selector. Report it so the caller disables just this action rather than stopping the whole poll.
    return "invalid-selector";
  }

  if(el) {

    (el as HTMLElement).click();

    return "clicked";
  }

  return "absent";
}

/**
 * Attempts to dismiss the profile's per-site modal with a synthetic in-page click. The per-site dismissSelector modals (Paramount+ "Watch Live", c-span, cbs) are
 * reliably dismissed by synthetic clicks, so coordinate clicking is reserved for the consent paths that require it. The in-page function guards its querySelector so
 * a malformed selector is reported as "invalid-selector" rather than throwing and killing the tick.
 * @param page - The Puppeteer page.
 * @param selector - The CSS selector for the modal element to dismiss.
 * @returns "clicked" when the element was found and clicked, "absent" when the valid selector matched nothing, "invalid-selector" when the selector is malformed.
 */
async function clickSelectorSynthetic(page: Page, selector: string): Promise<SyntheticClickResult> {

  return page.evaluate(clickSelectorInPage, selector);
}

/**
 * In-page embed-gate scanner. Recognizes an embedded-player consent gate by shape: a button, a button-role element, or an anchor whose label matches the accept
 * affordance AND whose ancestor container (within a bounded depth) carries the 2-click-embed phrasing, excluding sign-in/paywall/age walls. Early-outs to null when a
 * video is already buffered for playback, so it never fires on a page that is succeeding without an overlay. The act flag splits detection from action: act false
 * reports presence and label only, touching nothing; act true scrolls the matched control into view and returns its viewport-center coordinates for a real click.
 *
 * This function crosses the page.evaluate boundary by source serialization, so it is self-contained: it references only its argument and the page's document global,
 * with every input a parameter - the accept, exclude, and gate regex SOURCES (a RegExp cannot cross the boundary, so they arrive as strings and are compiled here)
 * and the act flag - and zero outer-scope references, since a captured variable would be undefined once the function is stringified into the page. Exported so unit
 * tests can run it against a synthetic DOM.
 * @param args - The accept-affordance, exclude, and embed-gate phrasing regex sources, plus the act flag.
 * @returns The gate's label (and, on the acting path, its post-scroll center coordinates), or null when no gate is detected.
 */
export function scanForEmbedGate(args: { accept: string; act: boolean; exclude: string; gate: string }): { label: string; x?: number; y?: number } | null {

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

    // Read-only probe: report presence and label without touching the page. The acting path scrolls the control into view and returns post-scroll coordinates.
    if(!args.act) {

      return { label: label.slice(0, 80) };
    }

    candidate.scrollIntoView({ block: "center" });

    const scrolled = candidate.getBoundingClientRect();

    return { label: label.slice(0, 80), x: scrolled.x + (scrolled.width / 2), y: scrolled.y + (scrolled.height / 2) };
  }

  return null;
}

/**
 * Locates an embedded-player consent gate. A gate is recognized by shape: a button, a button-role element, or an anchor whose label matches the accept affordance
 * AND whose ancestor container carries the 2-click-embed phrasing, excluding sign-in/paywall/age walls. The scan early-outs when a video is already playable, so it
 * never fires on a page that is succeeding without an overlay.
 *
 * The `act` argument splits detection from action. The read-only probe (act = false, used by consentOverlayPresent and every non-videoWait phase) reports presence
 * and label only, touching nothing on the page. The acting path (act = true, the video wait's tick) scrolls the matched control into view and returns its
 * viewport-center coordinates for a real coordinate click. Scrolling is therefore structurally unreachable outside the acting path, which is what keeps the failure-
 * path detection probe side-effect-free rather than scrolling the very page it is classifying.
 * @param page - The Puppeteer page.
 * @param act - True to scroll the matched control into view and return click coordinates; false to report presence and label only.
 * @returns The gate's label (and, on the acting path, its center coordinates), or null when no gate is detected.
 */
async function locateEmbedGate(page: Page, act: true): Promise<{ label: string; x: number; y: number } | null>;
async function locateEmbedGate(page: Page, act: false): Promise<{ label: string } | null>;

async function locateEmbedGate(page: Page, act: boolean): Promise<{ label: string; x?: number; y?: number } | null> {

  return page.evaluate(scanForEmbedGate, { accept: ACCEPT_AFFORDANCE_SOURCE, act, exclude: EXCLUDE_SOURCE, gate: EMBED_GATE_SOURCE });
}

/**
 * Reports whether a consent or cookie prompt is currently present on the page. Used on the failure path to decide whether to replace a cryptic video-selector
 * timeout with actionable detect-and-guide messaging. A prompt is "present" when a known CMP banner is detected or an embedded-player consent gate is located. The
 * gate probe runs in read-only mode, so this detection never scrolls or clicks the page it is classifying.
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

    return (await locateEmbedGate(page, false)) !== null;
  } catch {

    // The page navigated or closed while probing; treat as no detectable overlay.
    return false;
  }
}

/**
 * Runs a single overlay-handling poll tick, taking exactly the actions the acting phase's policy permits: reject any known CMP cookie banner, accept an embedded-
 * player consent gate (terminal, videoWait only), then dismiss a per-site modal. Ordering matters - the embed gate often only becomes interactable after the cookie
 * banner above it is dismissed, so the CMP reject runs first each tick. The signal is checked between action groups so an in-flight tick cannot keep acting after the
 * caller has handed the page to the next phase.
 * @param page - The Puppeteer page.
 * @param profile - The resolved site profile (supplies the optional per-site dismissSelector).
 * @param options - The acting phase's policy, the abort signal, and the mutable per-poll state. See OverlayTickOptions.
 * @returns The tick outcome - "gate" when an embed gate was accepted, "stop" when the poll should end, otherwise "continue".
 */
async function runOverlayTick(page: Page, profile: ResolvedSiteProfile, options: OverlayTickOptions): Promise<OverlayTickResult> {

  const { policy, signal, state } = options;

  try {

    // Reject any known cookie-consent banner, when the phase permits it. Each vendor is acted on at most once.
    if(policy.cmpReject) {

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
    }

    // Stop acting the instant the caller aborts, so an outgoing poll's in-flight tick cannot double-act against the next phase's poll.
    if(signal?.aborted) {

      return "stop";
    }

    // Accept an embedded-player consent gate, when the phase permits it. This is terminal: the player iframe is only created after consent, so the caller must reload
    // to resolve the video. Only the video wait reaches here - every other phase's policy forbids the accept, so the acting gate probe never runs for them.
    if(policy.embedGate) {

      const gate = await locateEmbedGate(page, true);

      if(gate) {

        // The settle after locateEmbedGate's scroll paces real page physics inside an action, exercised only when an accept actually dispatches - so it stays on
        // delay() rather than the injected poll clock, exactly like the coordinate-click settle.
        await delay(SCROLL_SETTLE_DELAY);
        await page.mouse.click(gate.x, gate.y);

        logAutoDismiss("embed-gate", { label: gate.label });

        return "gate";
      }
    }

    if(signal?.aborted) {

      return "stop";
    }

    // Dismiss the profile's per-site intermittent modal, if configured, armed, and permitted, using a synthetic click. A malformed selector disables just this
    // action for the rest of the poll (and warns once per selector per process); an absent element keeps the action armed for a later tick.
    if(policy.dismissSelector && profile.dismissSelector && (state.dismissSelector === "armed")) {

      const result = await clickSelectorSynthetic(page, profile.dismissSelector);

      switch(result) {

        case "absent": {

          break;
        }

        case "clicked": {

          state.dismissSelector = "done";

          logAutoDismiss("modal", { selector: profile.dismissSelector });

          break;
        }

        case "invalid-selector": {

          state.dismissSelector = "disabled";

          if(!warnedInvalidSelectors.has(profile.dismissSelector)) {

            warnedInvalidSelectors.add(profile.dismissSelector);

            LOG.warn("The configured dismiss selector is not a valid CSS selector and will be ignored.", { selector: profile.dismissSelector });
          }

          break;
        }
      }
    }

    return "continue";
  } catch {

    // Tick-error taxonomy: a closed page or a disconnected browser is terminal, so the poll stops. Any other rejection - most commonly a destroyed execution context
    // from an in-walk navigation, which is transient - lets the poll survive and try again on the next tick.
    return (page.isClosed() || !page.browser().connected) ? "stop" : "continue";
  }
}

/**
 * Launches the fire-and-forget overlay-handling poll for one phase of a page's lifecycle. Runs concurrently with whatever the phase covers, never blocks it, and
 * never throws - it is the single mechanism that rejects known cookie banners, accepts embedded-player consent gates (video wait only), and dismisses per-site
 * modals on any externally navigated page. The phase declares which of those actions are safe and how long the poll may run; the caller's abort signal stops it as
 * soon as its phase settles.
 *
 * When an embedded-player consent gate is accepted (only reachable in the videoWait phase), onEmbedGateAccepted() is invoked and the poll stops, because the gate's
 * acceptance only takes effect on a fresh load - the caller abandons the in-flight wait and reloads.
 * @param page - The Puppeteer page.
 * @param profile - The resolved site profile.
 * @param options - The phase, an optional clock and abort signal, and (videoWait only) the embed-gate callback. See StartOverlayHandlingOptions.
 */
export async function startOverlayHandling(page: Page, profile: ResolvedSiteProfile, options: StartOverlayHandlingOptions): Promise<void> {

  const { clock = realClock, phase, signal } = options;

  // Capture the gate callback once, while the union is narrowed: only the videoWait arm carries it. Every other phase forbids the accept, so the tick never returns
  // "gate" for them and this stays undefined.
  const onGate = (options.phase === "videoWait") ? options.onEmbedGateAccepted : undefined;
  const policy = PHASE_POLICY[phase];
  const state: OverlayPollState = { dismissSelector: "armed", handledVendors: new Set<string>() };

  // Resolve the poll window from the phase's policy: a fixed millisecond budget, or the profile-derived video-wait window for the phases whose timing tracks the
  // wait. One time origin end to end (clock.now() for both the deadline and the loop condition), because realClock.now() is performance.now()-based - a half-migration
  // that mixed it with Date.now() would invert the loop condition and zero-tick every poll.
  const windowMs = (policy.window === "videoTimeout") ? (profile.videoTimeout ?? CONFIG.streaming.videoTimeout) : policy.window;
  const deadline = clock.now() + windowMs;

  let firstCheck = true;

  while(clock.now() < deadline) {

    if(signal?.aborted) {

      return;
    }

    // The first check is immediate; subsequent checks are spaced by the poll interval.
    if(!firstCheck) {

      // eslint-disable-next-line no-await-in-loop
      await clock.sleep(OVERLAY_POLL_INTERVAL);

      if(signal?.aborted) {

        return;
      }
    }

    firstCheck = false;

    // eslint-disable-next-line no-await-in-loop
    const result = await runOverlayTick(page, profile, { policy, signal, state });

    if(result === "gate") {

      onGate?.();

      return;
    }

    if(result === "stop") {

      return;
    }
  }
}
