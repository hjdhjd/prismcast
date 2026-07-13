/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * blockedPage.ts: Blocked-page classification for empty channel discoveries in PrismCast.
 */
import type { AuthWallIndicators, Nullable } from "../types/index.ts";
import type { Page } from "puppeteer-core";
import { consentOverlayPresent } from "./consent.ts";
import { raceWithTimeout } from "../utils/index.ts";

/* When a channel discovery walk returns zero channels, the page it walked is still open - and what is on that page is evidence. This module classifies that
 * still-open page as a provider authentication wall, a consent overlay, or unknown, so the discovery-outcome policy in precaching.ts can persist the needs-sign-in
 * domain state on positive evidence rather than guessing from redirects or URL shapes.
 *
 * The module follows the pure-decision-core / thin-impure-adapter split (the browserSupervisor / launchGovernor precedent): decideBlockedPage is a pure function
 * over collected signals, and classifyBlockedPage is the thin adapter that gathers those signals from the live page. The signal gathering itself composes
 * consentOverlayPresent from consent.ts (never duplicated) plus one in-page collector (collectSignInContainers) that reports candidate sign-in containers.
 *
 * Decision order: (a) provider-declared indicators, (b) consent overlay, (c) the generic container-scoped sign-in shape, (d) unknown. Consent is checked before the
 * generic shape because a CMP banner can mask a wall behind it - we cannot see through the mask, so consent is the more actionable signal. The discovery walk runs
 * its own consent-overlay poll (see consent.ts), which dismisses the banner proactively, so a consentOverlay classification here means that poll could not clear it
 * - the banner is the standing obstacle, and dismissing it manually is the actionable next step.
 *
 * classifyBlockedPage never throws: classification is best-effort advisory evidence, so any internal failure (the page closed or navigated mid-probe, an evaluate
 * error) degrades to the unknown classification rather than propagating into the discovery path.
 */

// Ancestor depth bound for the container search in collectSignInContainers. Mirrors consent.ts's embed-gate ancestor bound - deep enough to find the form-like
// wrapper around an input on a real page, shallow enough that the search can never escalate to a page-level container.
const CONTAINER_SEARCH_DEPTH = 6;

// Cap on the container text carried back across the evaluate boundary. Sign-in phrasing sits next to the inputs it describes, so a bounded snippet is enough; the
// cap keeps a pathological container from shipping an entire page of text into Node.
const CONTAINER_TEXT_LIMIT = 500;

// Source for the sign-in phrasing that qualifies a candidate container as an authentication wall. Kept narrow and account-flavored so newsletter, search, and other
// benign text-entry forms never match. The gerund arm ("entering your email") covers walls whose form copy phrases the instruction inside a terms-of-use sentence
// ("By entering your email and clicking Continue..."), the shape the Fox One wall uses. Held as a source string (rather than a RegExp) to match the consent.ts
// convention for patterns whose natural home is the page.evaluate boundary, although the decision core applies it Node-side.
const SIGN_IN_PHRASING_SOURCE = "\\b(sign[ -]?in|log[ -]?in|sign[ -]?on|enter(ing)? your e-?mail|create an? (free )?account)\\b";

// The compiled sign-in phrasing matcher used by the pure decision core.
const SIGN_IN_PHRASING_RE = new RegExp(SIGN_IN_PHRASING_SOURCE, "i");

// Upper bound in milliseconds on the blocked-page classification. classifyBlockedPage bounds its own signal gathering with this budget, so it is never-throwing AND
// never-hanging: a hung renderer cannot stall a caller's failure path, and no caller needs a timeout wrapper of its own. Four seconds accommodates the DOM probes on
// a responsive page while keeping the added window negligible inside the callers' own time budgets (the tune's playback-initialization race, the precache cycle).
const BLOCKED_PAGE_CLASSIFY_TIMEOUT = 4000;

/**
 * The classification of a page whose discovery walk returned zero channels.
 *
 * - "authWall": the provider is presenting an authentication wall; evidence is a short human-readable summary naming the landed page's host and path.
 * - "consentOverlay": a consent or cookie prompt is present and may be masking the guide (or a wall behind it).
 * - "unknown": nothing recognizable - the emptiness is unexplained and no state should change on its account.
 */
export type BlockedPageClassification = { evidence: string; kind: "authWall" } | { kind: "consentOverlay" } | { kind: "unknown" };

/**
 * Options for classifyBlockedPage().
 */
export interface ClassifyBlockedPageOptions {

  // Provider-declared auth wall indicators, when the provider defines them. Checked ahead of the generic shape probe.
  indicators?: AuthWallIndicators;

  // The URL the discovery originally requested. Compared against the landed URL to strengthen evidence - a URL change alone never classifies.
  requestedUrl: string;
}

/**
 * A candidate sign-in container reported by the in-page collector: one record per password, email, or text input whose surrounding container could plausibly be a
 * sign-in form. The decision core applies the qualification rules; the collector only reports structure.
 */
export interface SignInContainerRecord {

  // Whether the container holds a password input.
  hasPasswordInput: boolean;

  // Whether the container holds a submit affordance (a button, a submit input, or a button-role element).
  hasSubmitAffordance: boolean;

  // Whether the seeding input is an email or text entry field.
  hasTextEntry: boolean;

  // The container's own text, whitespace-normalized and capped at CONTAINER_TEXT_LIMIT characters.
  text: string;
}

/**
 * The signals decideBlockedPage decides over, gathered from the live page by classifyBlockedPage. Exposed so the decision core can be exercised directly against
 * fixture-derived signals.
 */
export interface BlockedPageSignals {

  // Whether a known CMP banner or embedded-player consent gate is present (consent.ts's consentOverlayPresent).
  readonly consentOverlayPresent: boolean;

  // The candidate sign-in containers collected from the page.
  readonly containers: readonly SignInContainerRecord[];

  // Whether the landed URL's hostname matched a provider-declared auth wall host pattern.
  readonly indicatorHostMatched: boolean;

  // Whether a provider-declared auth wall DOM selector matched.
  readonly indicatorSelectorMatched: boolean;

  // The URL the page actually landed on.
  readonly landedUrl: string;

  // The URL the discovery originally requested.
  readonly requestedUrl: string;
}

/**
 * In-page collector for candidate sign-in containers. For every password, email, or text input on the page, it resolves the input's candidate container - the
 * enclosing form when one exists, otherwise the nearest ancestor within a bounded depth that carries a submit affordance - and reports the container's structure.
 * The body and document element are never candidates, which (together with the depth bound) is what makes the shape rule container-scoped: a newsletter form in the
 * footer and a "Sign In" link in the nav can never merge into one record.
 *
 * This function crosses the page.evaluate boundary by source serialization, so it is self-contained: it references only its argument and the page's document
 * global. Exported so unit tests can run it against a synthetic DOM.
 * @param args - The depth bound for the ancestor search and the text cap per record.
 * @returns One record per qualifying input's container. Containers are reported per seeding input, without deduplication - the decision core only asks whether any
 * record qualifies.
 */
export function collectSignInContainers(args: { maxDepth: number; textLimit: number }): SignInContainerRecord[] {

  const records: SignInContainerRecord[] = [];

  for(const input of Array.from(document.querySelectorAll("input"))) {

    // Only password, email, and text inputs can seed a sign-in shape. A type-less input defaults to text per the HTML spec.
    const type = (input.getAttribute("type") ?? "text").toLowerCase();

    if(![ "email", "password", "text" ].includes(type)) {

      continue;
    }

    // Skip hidden inputs (and inputs inside hidden subtrees). Autofill honeypots are commonly hidden password or text inputs, and a hidden control is not part of
    // the wall a viewer would see.
    if(input.hasAttribute("hidden") || (input.closest("[hidden]") !== null)) {

      continue;
    }

    // Resolve the candidate container: the enclosing form wins; otherwise walk up a bounded number of ancestors looking for one that carries a submit affordance.
    let container: Element | null = input.closest("form");

    if(!container) {

      let node: Element | null = input.parentElement;

      for(let depth = 0; node && (depth < args.maxDepth); depth++) {

        if((node === document.body) || (node === document.documentElement)) {

          break;
        }

        if(node.querySelector("button, input[type=\"submit\"], [role=\"button\"]") !== null) {

          container = node;

          break;
        }

        node = node.parentElement;
      }
    }

    if(!container || (container === document.body) || (container === document.documentElement)) {

      continue;
    }

    /* Compose the container's text from its descendant text nodes joined with single spaces. Raw textContent concatenates adjacent elements without separators (a
     * heading followed by a button reads "Enter Your EmailContinue"), which would defeat the decision core's word-boundary phrasing match. The numeric whatToShow
     * value is NodeFilter.SHOW_TEXT, named locally so the collector stays self-contained across the evaluate boundary without relying on the NodeFilter global.
     */
    const SHOW_TEXT = 4;
    const parts: string[] = [];
    const walker = document.createTreeWalker(container, SHOW_TEXT);

    while(walker.nextNode()) {

      parts.push(walker.currentNode.textContent ?? "");
    }

    const text = parts.join(" ").replace(/\s+/g, " ").trim().slice(0, args.textLimit);

    records.push({

      hasPasswordInput: container.querySelector("input[type=\"password\"]") !== null,
      hasSubmitAffordance: container.querySelector("button, input[type=\"submit\"], [role=\"button\"]") !== null,
      hasTextEntry: (type === "email") || (type === "text"),
      text
    });
  }

  return records;
}

/**
 * Extracts the hostname from a URL, or null when the URL does not parse. Pure helper for the host-indicator match and the evidence composition.
 */
function hostnameOf(url: string): Nullable<string> {

  try {

    return new URL(url).hostname;
  } catch {

    return null;
  }
}

/**
 * Reduces a URL to its host and path for evidence strings. Sign-in redirects carry per-session tokens (login challenges, device identifiers, API keys) in the
 * query string - noise with no diagnostic value that does not belong in a log users share when asking for help. Falls back to the raw string when the URL does
 * not parse, since evidence must always name where discovery landed.
 */
function displayUrlOf(url: string): string {

  try {

    const parsed = new URL(url);

    return parsed.host + parsed.pathname;
  } catch {

    return url;
  }
}

/**
 * Matches a hostname against a provider-declared host pattern using cookie-domain semantics: an exact match, or any subdomain of the pattern.
 */
const hostMatchesPattern = (hostname: string, pattern: string): boolean => (hostname === pattern) || hostname.endsWith("." + pattern);

/**
 * The pure decision core: classifies a blocked page from its collected signals. Decision order is (a) provider indicators, (b) consent overlay, (c) the generic
 * container-scoped sign-in shape, (d) unknown - see the module comment for the (b)-before-(c) rationale. The landed-URL-differs signal strengthens authWall
 * evidence but never suffices alone: with no indicator, no consent overlay, and no qualifying container, a redirect classifies as unknown.
 * @param signals - The collected page signals.
 * @returns The classification.
 */
export function decideBlockedPage(signals: BlockedPageSignals): BlockedPageClassification {

  // (a) Provider-declared indicators are authoritative: the provider named its own wall, so no shape inference is needed.
  if(signals.indicatorHostMatched || signals.indicatorSelectorMatched) {

    const detail = signals.indicatorHostMatched ? "the landed URL matches the provider's auth wall host" : "a provider auth wall selector matched";

    return { evidence: detail + " at " + displayUrlOf(signals.landedUrl), kind: "authWall" };
  }

  // (b) A consent overlay is present. It may be masking a wall behind it, but we cannot see through the mask - report the actionable signal we can see.
  if(signals.consentOverlayPresent) {

    return { kind: "consentOverlay" };
  }

  // (c) The generic shape rule, container-scoped: a password input qualifies its container outright; an email or text entry qualifies only alongside a submit
  // affordance and sign-in phrasing in that container's own text.
  const match = signals.containers.find((container) => container.hasPasswordInput ||
    (container.hasTextEntry && container.hasSubmitAffordance && SIGN_IN_PHRASING_RE.test(container.text)));

  if(match) {

    const landedHost = hostnameOf(signals.landedUrl);
    const requestedHost = hostnameOf(signals.requestedUrl);
    const redirectNote = (landedHost !== null) && (requestedHost !== null) && (landedHost !== requestedHost) ?
      " instead of the requested " + displayUrlOf(signals.requestedUrl) : "";

    return { evidence: "a sign-in form is present at " + displayUrlOf(signals.landedUrl) + redirectNote, kind: "authWall" };
  }

  // (d) Nothing recognizable - including the URL-change-only case, which never suffices on its own.
  return { kind: "unknown" };
}

/**
 * Classifies a still-open page whose discovery walk returned zero channels. Gathers signals lazily in decision order - each probe is skipped once an earlier signal
 * already decides the outcome - and hands them to the pure decision core. The gathering is bounded by BLOCKED_PAGE_CLASSIFY_TIMEOUT internally, so the classifier is
 * both never-throwing AND never-hanging: every caller gets an advisory result within the budget with no wrapper of its own.
 * @param page - The still-open discovery page.
 * @param options - The provider's optional auth wall indicators and the originally requested URL.
 * @returns The page's classification.
 */
export async function classifyBlockedPage(page: Page, options: ClassifyBlockedPageOptions): Promise<BlockedPageClassification> {

  /* The signal gathering, in its own never-throwing chain: any internal failure (the page closed or navigated mid-probe, an evaluate error) degrades to the unknown
   * classification. Because this chain never rejects, a probe that outruns the budget below and settles late - against a page that has since closed - resolves
   * harmlessly to unknown rather than surfacing an unhandled rejection after the race has already been decided by the timeout.
   */
  const gather = async (): Promise<BlockedPageClassification> => {

    try {

      const landedUrl = page.url();
      const landedHost = hostnameOf(landedUrl);
      const indicatorHostMatched = (landedHost !== null) && (options.indicators?.hosts ?? []).some((pattern) => hostMatchesPattern(landedHost, pattern));

      // Probe the provider's DOM selectors only when the host indicator has not already decided.
      let indicatorSelectorMatched = false;

      if(!indicatorHostMatched) {

        for(const selector of options.indicators?.selectors ?? []) {

          // eslint-disable-next-line no-await-in-loop
          if((await page.$(selector)) !== null) {

            indicatorSelectorMatched = true;

            break;
          }
        }
      }

      const indicatorMatched = indicatorHostMatched || indicatorSelectorMatched;
      const consentPresent = indicatorMatched ? false : await consentOverlayPresent(page);
      const containers = (indicatorMatched || consentPresent) ? [] :
        await page.evaluate(collectSignInContainers, { maxDepth: CONTAINER_SEARCH_DEPTH, textLimit: CONTAINER_TEXT_LIMIT });

      return decideBlockedPage({ consentOverlayPresent: consentPresent, containers, indicatorHostMatched, indicatorSelectorMatched, landedUrl,
        requestedUrl: options.requestedUrl });
    } catch {

      // The page closed or navigated mid-probe, or an evaluate failed. Classification is advisory evidence - an unreadable page is simply unknown.
      return { kind: "unknown" };
    }
  };

  // Bound the gathering so a hung renderer cannot stall the caller: on timeout the raced rejection lands here and becomes unknown, the same outcome a slow-but-empty
  // page would produce.
  try {

    return await raceWithTimeout(gather(), BLOCKED_PAGE_CLASSIFY_TIMEOUT);
  } catch {

    return { kind: "unknown" };
  }
}
