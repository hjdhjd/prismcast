/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * blockedPage.test.ts: Unit tests for the blocked-page classifier. The pure decision core (decideBlockedPage) is exercised directly over constructed signals, the
 * in-page collector (collectSignInContainers) runs against synthetic happy-dom documents, and the full classifyBlockedPage pipeline runs against a DOM-backed page
 * stub whose evaluate executes the real in-page functions (the CMP probe, the embed-gate locator, and the collector) on fixture markup - so the acceptance
 * fixtures below exercise the production decision path end to end without a browser.
 */
import type { BlockedPageSignals, SignInContainerRecord } from "./blockedPage.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { classifyBlockedPage, collectSignInContainers, decideBlockedPage } from "./blockedPage.ts";
import { closePuppeteerStreamWssOnIdle, firstOf } from "../testing.helpers.ts";
import type { Page } from "puppeteer-core";
import { Window } from "happy-dom";
import assert from "node:assert/strict";
import { setImmediate as immediate } from "node:timers/promises";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly (consent.ts loads transitively).
closePuppeteerStreamWssOnIdle();

// The collector arguments the production adapter passes. Mirrored here so collector tests exercise the same bounds production uses.
const COLLECTOR_ARGS = { maxDepth: 6, textLimit: 500 };

// Builds the baseline signal set - every field inert - so each test overrides exactly the signals its scenario turns on.
function makeSignals(overrides: Partial<BlockedPageSignals> = {}): BlockedPageSignals {

  return {

    consentOverlayPresent: false,
    containers: [],
    indicatorHostMatched: false,
    indicatorSelectorMatched: false,
    landedUrl: "https://www.example.test/guide",
    requestedUrl: "https://www.example.test/guide",
    ...overrides
  };
}

// A container record that satisfies the generic shape rule's email arm (text entry + submit + sign-in phrasing).
const QUALIFYING_CONTAINER: SignInContainerRecord = { hasPasswordInput: false, hasSubmitAffordance: true, hasTextEntry: true, text: "Enter your email Continue" };

/* Runs a body with the given fixture markup installed as the global document, backed by happy-dom. The collector is self-contained in-page code that references the
 * document global (it crosses the page.evaluate boundary by source serialization), so tests inject a synthetic document the same way the page would supply its own.
 * node:test runs each test file in its own process and tests in this file run sequentially, so the scoped global mutation cannot bleed across suites.
 */
function withDocument<T>(html: string, body: () => T): T {

  const window = new Window();

  window.document.body.innerHTML = html;

  const globalSlot = globalThis as { document?: unknown };

  globalSlot.document = window.document;

  try {

    return body();
  } finally {

    delete globalSlot.document;
  }
}

/* Builds a Page stub backed by a happy-dom document: url() returns the landed URL, $ resolves selectors against the fixture, and evaluate executes the real
 * serialized in-page function against the fixture document. This runs the production CMP probe, embed-gate locator, and container collector unmocked, so the
 * classification fixtures below exercise the same code a live page would.
 */
function makeDomBackedPage(html: string, landedUrl: string): { evaluateCalls: number[]; page: Page } {

  const window = new Window();

  window.document.body.innerHTML = html;

  const calls = { evaluateCalls: [] as number[] };
  const globalSlot = globalThis as { document?: unknown };

  const page = {

    $: async (selector: string): Promise<unknown> => window.document.querySelector(selector),
    evaluate: async (fn: (arg: never) => unknown, arg?: unknown): Promise<unknown> => {

      calls.evaluateCalls.push(1);
      globalSlot.document = window.document;

      try {

        return fn(arg as never);
      } finally {

        delete globalSlot.document;
      }
    },
    url: (): string => landedUrl
  } as unknown as Page;

  return { evaluateCalls: calls.evaluateCalls, page };
}

describe("decideBlockedPage", () => {

  test("a provider host indicator classifies authWall with the landed host and path in the evidence", () => {

    /* Traced path: decideBlockedPage's (a) branch - the indicatorHostMatched disjunct returns authWall before any other signal is consulted. Dropping the branch
     * would fall through to unknown here since every other signal is inert. The evidence carries host and path only - the query string holds per-session tokens
     * that do not belong in a log.
     */
    const result = decideBlockedPage(makeSignals({ indicatorHostMatched: true, landedUrl: "https://auth.example.test/login?challenge=abc123" }));

    assert.equal(result.kind, "authWall");
    assert.match(result.evidence, /auth\.example\.test\/login/, "evidence names the landed host and path");
    assert.doesNotMatch(result.evidence, /challenge|abc123|https:/, "evidence carries neither the query string nor the scheme");
  });

  test("a provider selector indicator classifies authWall", () => {

    // Traced path: the (a) branch's indicatorSelectorMatched disjunct.
    const result = decideBlockedPage(makeSignals({ indicatorSelectorMatched: true }));

    assert.equal(result.kind, "authWall");
  });

  test("indicators outrank a consent overlay (decision order a before b)", () => {

    /* Traced path: with both signals set, the (a) branch must return before the (b) branch is reached. Swapping the order would return consentOverlay here.
     */
    const result = decideBlockedPage(makeSignals({ consentOverlayPresent: true, indicatorHostMatched: true }));

    assert.equal(result.kind, "authWall");
  });

  test("a consent overlay outranks a qualifying sign-in container (decision order b before c)", () => {

    /* Traced path: with consent present AND a qualifying container, the (b) branch must return before the container scan in (c). Swapping the order would return
     * authWall here - the ordering exists because a CMP banner can mask a wall we cannot see through, and consent is the actionable signal.
     */
    const result = decideBlockedPage(makeSignals({ consentOverlayPresent: true, containers: [QUALIFYING_CONTAINER] }));

    assert.equal(result.kind, "consentOverlay");
  });

  test("a container with text entry, a submit affordance, and sign-in phrasing classifies authWall", () => {

    // Traced path: the (c) branch's conjunction arm. Removing any conjunct's check would still pass here; the negative tests below pin each conjunct.
    const result = decideBlockedPage(makeSignals({ containers: [QUALIFYING_CONTAINER] }));

    assert.equal(result.kind, "authWall");
  });

  test("a container with a password input classifies authWall without any phrasing", () => {

    // Traced path: the (c) branch's hasPasswordInput arm, which qualifies outright.
    const result = decideBlockedPage(makeSignals({ containers: [{ hasPasswordInput: true, hasSubmitAffordance: false, hasTextEntry: false, text: "" }] }));

    assert.equal(result.kind, "authWall");
  });

  test("a container missing the submit affordance does not classify (conjunct pin)", () => {

    // Traced path: the hasSubmitAffordance conjunct in (c). A mutation dropping it would classify this signal set as authWall.
    const result = decideBlockedPage(makeSignals({ containers: [{ hasPasswordInput: false, hasSubmitAffordance: false, hasTextEntry: true, text: "Sign in" }] }));

    assert.equal(result.kind, "unknown");
  });

  test("a container without sign-in phrasing does not classify (conjunct pin)", () => {

    // Traced path: the SIGN_IN_PHRASING_RE test in (c). A mutation dropping it would classify any submit-bearing email form - including a newsletter - as a wall.
    const newsletter: SignInContainerRecord = { hasPasswordInput: false, hasSubmitAffordance: true, hasTextEntry: true, text: "Get our weekly newsletter" };
    const result = decideBlockedPage(makeSignals({ containers: [newsletter] }));

    assert.equal(result.kind, "unknown");
  });

  test("a landed URL differing from the requested URL never classifies on its own", () => {

    /* Traced path: with every container/consent/indicator signal inert, the decision must fall through (d) to unknown even though the URL changed - the
     * landed-URL-differs signal strengthens evidence but never suffices alone (auth.fox.com and fox.com share a registrable domain; redirects alone are the
     * brittleness this module deliberately avoids).
     */
    const result = decideBlockedPage(makeSignals({ landedUrl: "https://auth.example.test/somewhere", requestedUrl: "https://www.example.test/guide" }));

    assert.equal(result.kind, "unknown");
  });

  test("the redirect note strengthens generic-shape evidence when the landed host differs", () => {

    // Traced path: the redirectNote composition inside (c) - both locations appear in the evidence, as host and path, when hosts differ.
    const redirected = { containers: [QUALIFYING_CONTAINER], landedUrl: "https://auth.example.test/login?token=xyz", requestedUrl: "https://www.example.test/guide" };
    const result = decideBlockedPage(makeSignals(redirected));

    assert.equal(result.kind, "authWall");
    assert.match(result.evidence, /auth\.example\.test\/login/, "evidence names the landed host and path");
    assert.match(result.evidence, /instead of the requested www\.example\.test\/guide/, "evidence names the requested location it was redirected away from");
    assert.doesNotMatch(result.evidence, /token|xyz/, "evidence does not carry the query string");
  });

  test("no signals at all classifies unknown", () => {

    assert.equal(decideBlockedPage(makeSignals()).kind, "unknown");
  });
});

describe("collectSignInContainers", () => {

  test("reports the container around an email input with a submit affordance and its own text", () => {

    /* The Fox auth wall reconstruction (per the captured shape): a card with an "Enter Your Email" heading, an email input, and a Continue button. The collector
     * must resolve the card as the container (nearest submit-bearing ancestor) and carry its text so the decision core's phrasing test can match.
     */
    const fixture = "<main><div class=\"auth-card\"><h1>Enter Your Email</h1><input id=\"email-input\" type=\"email\" placeholder=\"Enter your email\">" +
      "<button type=\"button\">Continue</button><button type=\"button\">Activate with ESPN</button></div></main>";

    const records = withDocument(fixture, () => collectSignInContainers(COLLECTOR_ARGS));

    assert.equal(records.length, 1, "one record for the one qualifying input");

    const record = firstOf(records, "sign-in container record");

    assert.equal(record.hasTextEntry, true, "seeded by an email input");
    assert.equal(record.hasSubmitAffordance, true, "the card carries the Continue button");
    assert.match(record.text, /Enter Your Email/, "the container's own text is carried for the phrasing test");
  });

  test("scopes the container to the enclosing form - a footer newsletter form never absorbs a header sign-in link", () => {

    /* The page-wide co-occurrence negative: an email form in the footer plus a "Sign In" link in the header. The collector must report the footer form's own text
     * only - the closest("form") resolution (and the body/documentElement exclusion behind it) is the mutation under test, since a page-level container would leak
     * the header's "Sign In" into the record and flip the decision core's phrasing test.
     */
    const fixture = "<header><a href=\"/login\">Sign In</a></header><div class=\"guide\"><button>NBC</button><button>ABC</button></div>" +
      "<footer><form class=\"newsletter\"><p>Get our weekly newsletter.</p><input type=\"email\" placeholder=\"you@example.com\">" +
      "<button type=\"submit\">Subscribe</button></form></footer>";

    const records = withDocument(fixture, () => collectSignInContainers(COLLECTOR_ARGS));

    assert.equal(records.length, 1, "the newsletter form yields the only record");
    assert.doesNotMatch(records[0]?.text ?? "", /Sign In/, "the header link's text never enters the footer container's record");
  });

  test("reports a password input's container without needing phrasing", () => {

    const fixture = "<div class=\"login-box\"><input type=\"password\"><button>Go</button></div>";

    const records = withDocument(fixture, () => collectSignInContainers(COLLECTOR_ARGS));

    assert.equal(records.length, 1);
    assert.equal(records[0]?.hasPasswordInput, true);
  });

  test("skips hidden inputs and inputs inside hidden subtrees (honeypot guard)", () => {

    const fixture = "<div hidden><input type=\"password\"><button>Go</button></div><div><input type=\"text\" hidden><button>Go</button></div>";

    const records = withDocument(fixture, () => collectSignInContainers(COLLECTOR_ARGS));

    assert.deepEqual(records, [], "hidden inputs never seed a record");
  });

  test("returns no records for a guide-like page with no inputs", () => {

    const fixture = "<nav><a href=\"/signin\">Sign In</a></nav><div class=\"guide\"><button>NBC</button><button>ABC</button><button>CBS</button></div>";

    assert.deepEqual(withDocument(fixture, () => collectSignInContainers(COLLECTOR_ARGS)), [], "no inputs, no records");
  });

  test("ignores non-credential input types (search, checkbox, radio)", () => {

    const fixture = "<form><input type=\"search\"><input type=\"checkbox\"><input type=\"radio\"><button>Filter</button></form>";

    assert.deepEqual(withDocument(fixture, () => collectSignInContainers(COLLECTOR_ARGS)), [], "non-credential inputs never seed a record");
  });
});

describe("classifyBlockedPage - acceptance fixtures through the full pipeline", () => {

  test("the Fox auth wall reconstruction classifies authWall (generic shape, no provider indicators)", async () => {

    /* Acceptance criterion: the captured Fox One wall shape - "Enter Your Email" heading, email input, Continue button, landed on auth.fox.com after requesting
     * fox.com/live/channels - must classify authWall through the generic container-scoped rule with no Fox-specific indicators declared. The evidence must name
     * the landed URL and the redirect.
     */
    const fixture = "<main><div class=\"auth-card\"><h1>Enter Your Email</h1><input id=\"email-input\" type=\"email\" placeholder=\"Enter your email\">" +
      "<button type=\"button\">Continue</button><button type=\"button\">Activate with ESPN</button></div></main>";

    const { page } = makeDomBackedPage(fixture, "https://auth.fox.com/foxone/auth");
    const result = await classifyBlockedPage(page, { requestedUrl: "https://www.fox.com/live/channels" });

    assert.equal(result.kind, "authWall");
    assert.match(result.evidence, /auth\.fox\.com/, "evidence names the landed URL");
  });

  test("a wall whose sign-in phrasing lives in a terms-of-use sentence inside the form classifies authWall (gerund arm)", async () => {

    /* The faithful Fox One form shape: the "Enter Your Email" heading sits OUTSIDE the form, so the container's own text carries only the terms-of-use sentence
     * ("By entering your email and clicking Continue..."). The phrasing rule's gerund arm ("entering your email") is the mutation under test - without it, this
     * container has a text entry and a submit affordance but no matching phrasing, and the classification falls to unknown.
     */
    const fixture = "<div class=\"auth-layout\"><h2>Enter Your Email</h2><form><label for=\"e\">Email</label><input id=\"e\" type=\"email\" " +
      "placeholder=\"Enter your email\"><p>By entering your email and clicking &quot;Continue&quot;, you are agreeing to the Terms of Use and Privacy " +
      "Policy.</p><button type=\"submit\">Continue</button></form></div>";

    const { page } = makeDomBackedPage(fixture, "https://auth.fox.com/foxone/auth");
    const result = await classifyBlockedPage(page, { requestedUrl: "https://www.fox.com/live/channels" });

    assert.equal(result.kind, "authWall");
  });

  test("a normal guide shape classifies unknown", async () => {

    const fixture = "<div class=\"guide\"><button>NBC</button><button>ABC</button><button>CBS</button><button>Fox News</button></div>";

    const { page } = makeDomBackedPage(fixture, "https://www.example.test/guide");
    const result = await classifyBlockedPage(page, { requestedUrl: "https://www.example.test/guide" });

    assert.equal(result.kind, "unknown");
  });

  test("a guide with a newsletter footer form and a sign-in nav link classifies unknown (page-wide co-occurrence rejected)", async () => {

    /* Acceptance criterion: the second negative fixture. The email input, the submit affordance, and sign-in phrasing all exist on the page, but never within one
     * container - so the container-scoped rule must not match. A page-wide co-occurrence implementation would classify this as authWall.
     */
    const fixture = "<header><a href=\"/login\">Sign In</a></header><div class=\"guide\"><button>NBC</button><button>ABC</button></div>" +
      "<footer><form class=\"newsletter\"><p>Get our weekly newsletter.</p><input type=\"email\" placeholder=\"you@example.com\">" +
      "<button type=\"submit\">Subscribe</button></form></footer>";

    const { page } = makeDomBackedPage(fixture, "https://www.example.test/guide");
    const result = await classifyBlockedPage(page, { requestedUrl: "https://www.example.test/guide" });

    assert.equal(result.kind, "unknown");
  });

  test("a Didomi CMP banner classifies consentOverlay (via the composed consentOverlayPresent probe)", async () => {

    /* Acceptance criterion: the CMP fixture. The classifier composes consent.ts's consentOverlayPresent - the #didomi-host container from the CMP registry's
     * detect selector is what fires here, exercised unmocked against the fixture DOM.
     */
    const fixture = "<div id=\"didomi-host\"><div class=\"didomi-popup-notice\"><button id=\"didomi-notice-disagree-button\">Disagree</button></div></div>" +
      "<div class=\"guide\"></div>";

    const { page } = makeDomBackedPage(fixture, "https://www.example.test/guide");
    const result = await classifyBlockedPage(page, { requestedUrl: "https://www.example.test/guide" });

    assert.equal(result.kind, "consentOverlay");
  });

  test("a provider host indicator match skips every page probe (lazy collection)", async () => {

    /* Efficiency pin: when the landed host matches a declared indicator, the classification is decided before any page.evaluate runs. The evaluateCalls counter
     * pins the lazy-collection contract - a regression that collected all signals eagerly would record probe calls here.
     */
    const { evaluateCalls, page } = makeDomBackedPage("<div></div>", "https://auth.example.test/login");
    const result = await classifyBlockedPage(page, { indicators: { hosts: ["auth.example.test"] }, requestedUrl: "https://www.example.test/guide" });

    assert.equal(result.kind, "authWall");
    assert.equal(evaluateCalls.length, 0, "no page probes ran once the host indicator decided");
  });

  test("a provider selector indicator classifies authWall against the fixture DOM", async () => {

    const { page } = makeDomBackedPage("<div class=\"provider-wall\"></div>", "https://www.example.test/guide");
    const result = await classifyBlockedPage(page, { indicators: { selectors: [".provider-wall"] }, requestedUrl: "https://www.example.test/guide" });

    assert.equal(result.kind, "authWall");
  });

  test("subdomain host patterns match per cookie-domain semantics", async () => {

    // The pattern "example.test" must match the landed host "auth.example.test" (any subdomain), per the documented matching rule.
    const { page } = makeDomBackedPage("<div></div>", "https://auth.example.test/login");
    const result = await classifyBlockedPage(page, { indicators: { hosts: ["example.test"] }, requestedUrl: "https://www.example.test/guide" });

    assert.equal(result.kind, "authWall");
  });
});

describe("classifyBlockedPage - never throws", () => {

  test("resolves unknown when page.url() throws (page closed mid-probe)", async () => {

    const page = {

      url: (): string => {

        throw new Error("Target closed.");
      }
    } as unknown as Page;

    const result = await classifyBlockedPage(page, { requestedUrl: "https://www.example.test/guide" });

    assert.equal(result.kind, "unknown");
  });

  test("resolves unknown when the collector evaluate rejects", async () => {

    /* consentOverlayPresent absorbs its own evaluate failures (returning false), so the rejection that reaches classifyBlockedPage's catch is the collector's.
     * The never-throws guarantee lives inside the classifier, not at call sites - this pins it.
     */
    const page = {

      evaluate: async (): Promise<never> => {

        throw new Error("Execution context was destroyed.");
      },
      url: (): string => "https://www.example.test/guide"
    } as unknown as Page;

    const result = await classifyBlockedPage(page, { requestedUrl: "https://www.example.test/guide" });

    assert.equal(result.kind, "unknown");
  });
});

describe("classifyBlockedPage - time budget", () => {

  beforeEach(() => {

    // Park the internal classification-budget timer so the test can drive its expiry explicitly.
    mock.timers.enable({ apis: ["setTimeout"] });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("resolves unknown when the signal gathering outruns the internal time budget", async () => {

    /* Traced path: the raceWithTimeout the classifier wraps its own gathering in. A page whose evaluate never settles makes the gathering hang; the classifier must
     * abandon it at the budget and classify unknown, entirely on its own - no caller wrapper. Driving the parked timer past the budget is what tells apart the
     * self-bounded classifier from the old unbounded one, which would leave this promise pending forever.
     */
    const page = {

      $: async (): Promise<null> => null,
      evaluate: (): Promise<never> => Promise.withResolvers<never>().promise,
      url: (): string => "https://www.example.test/guide"
    } as unknown as Page;

    const pending = classifyBlockedPage(page, { requestedUrl: "https://www.example.test/guide" });

    // Let the classifier reach the internal race (register the budget timer), then expire it past the four-second budget.
    await immediate();
    mock.timers.tick(5000);

    const result = await pending;

    assert.equal(result.kind, "unknown", "a classification that outruns its budget resolves unknown");
  });
});
