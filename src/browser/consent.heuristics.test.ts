/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * consent.heuristics.test.ts: DOM-fixture unit tests for the in-page heuristics consent.ts serializes across the page.evaluate boundary - the embed-gate scanner
 * (scanForEmbedGate), the coordinate resolver (locateSelectorCoordinate), and the synthetic dismisser (clickSelectorInPage). The Node-side orchestration (the poll,
 * the phase masking, the unified auto-dismiss logging) is covered by consent.test.ts; this file asserts the pure in-page decision logic those serialized functions
 * embody, running them directly in the test process against a synthetic happy-dom document exactly as the collector tests in blockedPage.test.ts do.
 *
 * Home: co-located unit tier, mirroring blockedPage.test.ts's treatment of collectSignInContainers - the established precedent for exercising consent.ts's sibling
 * in-page functions against a synthetic DOM. This is deliberately NOT the test/e2e/dom-runtime/ tier, whose harness (createDomTestContext) boots the app and executes
 * the emitted client-side <script> blocks served to browsers; these are src/browser/ page-evaluate functions, not emitted UI scripts, so the co-located unit home is
 * the smallest, most faithful fit for their behavior class.
 *
 * Geometry note: happy-dom implements no layout, so getBoundingClientRect returns zero-size rects, and the scanner's zero-size guard would otherwise skip every
 * candidate. The fixtures stub rects on the candidate controls (per element) so the heuristics' DECISION logic - the accept/exclude/gate phrasing, the readyState
 * short-circuit, the probe/act split, the ancestor depth bound - is what the assertions check; real viewport geometry remains live-smoke territory for a browser tier.
 */
import { ACCEPT_AFFORDANCE_SOURCE, CMP_REGISTRY, EMBED_GATE_SOURCE, EXCLUDE_SOURCE, clickSelectorInPage, locateSelectorCoordinate, scanForEmbedGate } from "./consent.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { withDocument } from "../testing.helpers.ts";

/**
 * A minimal viewport rectangle, expanded to a full DOMRect by toDomRect. Only x, y, width, and height are meaningful inputs; the derived edges follow from them.
 */
interface Rect {

  height: number;
  width: number;
  x: number;
  y: number;
}

/**
 * Expands a Rect into the full DOMRect shape getBoundingClientRect returns, deriving the four edges from the origin and size. The heuristics read only x, y, width,
 * and height, but the stub returns a complete rect so it is a faithful stand-in for the real geometry the browser would supply.
 * @param r - The origin and size to expand.
 * @returns A DOMRect with derived edges.
 */
function toDomRect(r: Rect): DOMRect {

  return {

    bottom: r.y + r.height,
    height: r.height,
    left: r.x,
    right: r.x + r.width,
    toJSON: (): object => ({}),
    top: r.y,
    width: r.width,
    x: r.x,
    y: r.y
  };
}

/**
 * Installs a stubbed getBoundingClientRect on an element. happy-dom implements no layout, so a live rect is always zero-size and the scanner's zero-size guard would
 * skip the control; the stub supplies the geometry a laid-out control would have. Returns a setter so a test can reposition the element mid-run - a scrollIntoView spy
 * that moves the rect is how the acting-path test proves coordinates are read AFTER the scroll, not before.
 * @param el - The element whose rect to stub.
 * @param initial - The rect the element reports until the setter changes it.
 * @returns A setter that replaces the reported rect.
 */
function stubRect(el: Element, initial: Rect): (next: Rect) => void {

  let current = initial;

  Object.defineProperty(el, "getBoundingClientRect", { configurable: true, value: (): DOMRect => toDomRect(current) });

  return (next: Rect): void => {

    current = next;
  };
}

/**
 * Resolves a selector against the fixture document, asserting the element exists so the returned value is non-null for the heuristic under test. The heuristics read
 * the ambient document global, so this reads the same global the fixture installs.
 * @param selector - The CSS selector to resolve.
 * @returns The matched element.
 */
function requireElement(selector: string): Element {

  const el = document.querySelector(selector);

  assert.ok(el, "expected the fixture to contain " + selector);

  return el;
}

/**
 * Builds the argument object scanForEmbedGate expects, sourced from the real exported regex constants so every test exercises the exact production heuristic rather
 * than a duplicated copy of the patterns.
 * @param act - True for the acting path (scroll and return coordinates), false for the read-only presence probe.
 * @returns The scanner arguments carrying the production regex sources.
 */
function gateArgs(act: boolean): { accept: string; act: boolean; exclude: string; gate: string } {

  return { accept: ACCEPT_AFFORDANCE_SOURCE, act, exclude: EXCLUDE_SOURCE, gate: EMBED_GATE_SOURCE };
}

describe("scanForEmbedGate - embed-gate heuristic", () => {

  test("locates a genuine 2-click embed gate under embed-gate phrasing and carries the accept control's label", () => {

    // A france24-style embed gate: a container whose copy carries the 2-click-embed phrasing wrapping an accept control. A preceding non-consent link exercises the
    // scan skipping a candidate whose label does not match the accept affordance before it reaches the real accept control.
    const fixture = "<nav><a href=\"/\">Home</a></nav><div class=\"o-em-consent\"><p>To watch this video, please enable third-party content.</p>" +
      "<button type=\"button\">Accept</button></div>";

    withDocument(fixture, () => {

      stubRect(requireElement("button"), { height: 20, width: 60, x: 30, y: 40 });

      const result = scanForEmbedGate(gateArgs(false));

      assert.ok(result, "the gate is located");
      assert.match(result.label, /Accept/, "the accept control's label is carried back");
    });
  });

  test("a present-but-unready video does not short-circuit the scan", () => {

    // The early-out fires only when a video is buffered to readyState >= 3. A video that exists but is still loading (readyState 0) must not suppress gate detection,
    // so a gate on the same page is still located.
    const fixture = "<video></video><div><p>Please enable third-party content.</p><button>Accept</button></div>";

    withDocument(fixture, () => {

      stubRect(requireElement("button"), { height: 20, width: 60, x: 10, y: 10 });
      Object.defineProperty(requireElement("video"), "readyState", { configurable: true, value: 0 });

      assert.ok(scanForEmbedGate(gateArgs(false)), "an unready video leaves the gate detectable");
    });
  });

  test("a video buffered to readyState >= 3 short-circuits to null even when a gate is present", () => {

    // The scan must not act on a page that is already playing. A fully-buffered video (readyState HAVE_ENOUGH_DATA) short-circuits before any candidate is considered,
    // so a gate that would otherwise match is deliberately ignored.
    const fixture = "<video></video><div><p>Please enable third-party content.</p><button>Accept</button></div>";

    withDocument(fixture, () => {

      // Stub the accept control's rect non-zero so the ONLY reason the scan returns null is the ready-video short-circuit, not a zero-size skip.
      stubRect(requireElement("button"), { height: 20, width: 60, x: 10, y: 10 });
      Object.defineProperty(requireElement("video"), "readyState", { configurable: true, value: 4 });

      assert.equal(scanForEmbedGate(gateArgs(true)), null, "a ready video suppresses the gate accept");
    });
  });

  test("a sign-in wall with an accept-like control is refused: no embed-gate phrasing means no gate", () => {

    // A provider sign-in wall carries an accept-like control ("Allow") but no 2-click-embed phrasing anywhere in its container. The ancestor walk finds no gate
    // phrasing, so the candidate is refused. Dropping the container-phrasing requirement would misclassify this wall's "Allow" button as a gate accept.
    const fixture = "<div class=\"signin\"><h1>Sign in to your account</h1><input type=\"password\"><button>Allow</button></div>";

    withDocument(fixture, () => {

      stubRect(requireElement("button"), { height: 20, width: 60, x: 10, y: 10 });

      assert.equal(scanForEmbedGate(gateArgs(true)), null, "an accept-like control without embed-gate phrasing is not a gate");
    });
  });

  test("a wall that mentions tracking but carries sign-in phrasing is refused: the exclusion outranks the gate phrasing (container arm)", () => {

    // The ambiguous wall: its copy both mentions tracking (matching the embed-gate phrasing) AND carries sign-in phrasing. Auto-accepting tracking is the
    // privacy-sensitive action, so the exclusion must win - even with the gate phrasing present, the sign-in phrasing in the container refuses the accept. The
    // control's own label ("Accept") is clean, so the refusal here is attributable specifically to the container operand of the exclusion.
    const fixture = "<div class=\"wall\"><p>Please sign in to enable tracking and continue.</p><button>Accept</button></div>";

    withDocument(fixture, () => {

      stubRect(requireElement("button"), { height: 20, width: 60, x: 10, y: 10 });

      assert.equal(scanForEmbedGate(gateArgs(true)), null, "the container's sign-in phrasing outranks its gate phrasing");
    });
  });

  test("the exclusion also refuses on the accept control's own label (label arm)", () => {

    // The exclusion is applied to both the label and the container text. Here the container carries clean gate phrasing, but the control's own label reads as a
    // sign-in affordance ("I understand, sign in"). The label operand of the exclusion must refuse it, distinct from the container operand exercised above.
    const fixture = "<div><p>Please enable third-party content.</p><button>I understand, sign in</button></div>";

    withDocument(fixture, () => {

      stubRect(requireElement("button"), { height: 20, width: 60, x: 10, y: 10 });

      assert.equal(scanForEmbedGate(gateArgs(true)), null, "a sign-in-flavored label is excluded even under clean gate phrasing");
    });
  });

  test("an age gate is refused even when it carries gate phrasing and an accept-like control", () => {

    // An age gate reads like a consent overlay - it withholds content behind a click and can even mention enabling content - but it must never be auto-accepted. The
    // exclusion's age arm ("must be 18", "years old") refuses it.
    const fixture = "<div class=\"age\"><p>You must be 18 years old to view this. Enable third-party content to continue.</p><button>I understand</button></div>";

    withDocument(fixture, () => {

      stubRect(requireElement("button"), { height: 20, width: 60, x: 10, y: 10 });

      assert.equal(scanForEmbedGate(gateArgs(true)), null, "an age gate is never auto-accepted");
    });
  });

  test("the probe reports presence without scrolling, and the acting path scrolls and returns post-scroll coordinates", () => {

    // The act flag splits detection from action. The read-only probe (act false) reports the label without touching the page - scrollIntoView must not fire. The
    // acting path (act true) scrolls the control into view and returns coordinates read AFTER the scroll. To prove the post-scroll read, the scrollIntoView spy
    // repositions the control's rect; the returned coordinates must reflect the post-scroll rect, not the pre-scroll one.
    const fixture = "<div><p>Please enable third-party content.</p><button>Accept</button></div>";

    withDocument(fixture, () => {

      const button = requireElement("button");
      const setRect = stubRect(button, { height: 20, width: 100, x: 500, y: 600 });

      let scrolls = 0;

      // The scroll repositions the control to the origin, so a post-scroll rect read yields different coordinates than the pre-scroll rect.
      button.scrollIntoView = (): void => {

        scrolls++;

        setRect({ height: 20, width: 100, x: 0, y: 0 });
      };

      // Read-only probe: label only, no scroll, no coordinates.
      const probe = scanForEmbedGate(gateArgs(false));

      assert.deepEqual(probe, { label: "Accept" }, "the probe returns label only");
      assert.equal(scrolls, 0, "the probe never scrolls");

      // Acting path: scroll once and return the post-scroll viewport-center coordinates (0 + 100 / 2, 0 + 20 / 2).
      const acted = scanForEmbedGate(gateArgs(true));

      assert.equal(scrolls, 1, "the acting path scrolls the control into view exactly once");
      assert.deepEqual(acted, { label: "Accept", x: 50, y: 10 }, "coordinates are read after the scroll repositions the control");
    });
  });

  test("a zero-size candidate is skipped even when it matches every phrasing rule", () => {

    // A control that matches the accept affordance under valid gate phrasing but has zero layout size (display:none, an unlaid-out node) cannot be coordinate-clicked,
    // so the scan skips it. This fixture deliberately keeps happy-dom's default zero-size rect, so a null result is attributable solely to the zero-size guard.
    const fixture = "<div><p>Please enable third-party content.</p><button>Accept</button></div>";

    withDocument(fixture, () => {

      assert.equal(scanForEmbedGate(gateArgs(true)), null, "a zero-size accept control is not a clickable gate");
    });
  });

  test("the ancestor walk is depth-bounded: gate phrasing beyond the bound does not match, at the bound it does", () => {

    // The container search walks a bounded number of ancestors - deep enough to find a real embed wrapper, shallow enough never to escalate to a page-level container.
    // Phrasing on an ancestor beyond the walked window must not qualify the candidate; phrasing on the last ancestor within the window must. The two fixtures differ
    // only in the depth at which the phrasing sits, isolating the bound itself. Both stub a non-zero rect so the beyond-bound null is attributable to the bound, not a
    // zero-size skip.

    // Beyond the bound: the phrasing sits on the seventh ancestor (outside the walked window), so no ancestor the walk examines carries it.
    const beyond = "<div id=\"d6\"><p>Please enable third-party content.</p><div id=\"d5\"><div id=\"d4\"><div id=\"d3\"><div id=\"d2\"><div id=\"d1\">" +
      "<div id=\"d0\"><button>Accept</button></div></div></div></div></div></div></div>";

    withDocument(beyond, () => {

      stubRect(requireElement("button"), { height: 20, width: 60, x: 10, y: 10 });

      assert.equal(scanForEmbedGate(gateArgs(false)), null, "phrasing beyond the ancestor bound does not qualify the candidate");
    });

    // Within the bound: the phrasing sits on the last ancestor the walk reaches, so it qualifies the candidate.
    const within = "<div id=\"d6\"><div id=\"d5\"><p>Please enable third-party content.</p><div id=\"d4\"><div id=\"d3\"><div id=\"d2\"><div id=\"d1\">" +
      "<div id=\"d0\"><button>Accept</button></div></div></div></div></div></div></div>";

    withDocument(within, () => {

      stubRect(requireElement("button"), { height: 20, width: 60, x: 10, y: 10 });

      assert.ok(scanForEmbedGate(gateArgs(false)), "phrasing on the last ancestor within the bound qualifies the candidate");
    });
  });
});

describe("locateSelectorCoordinate - coordinate resolver", () => {

  test("a Didomi banner is detected and its reject control resolves to viewport-center coordinates", () => {

    // Case (a): build the banner fixture around the CMP registry's own detect and reject selectors so the test asserts the same markers the poll uses. The detect
    // selector must match the banner container (the poll's CMP presence probe), and locateSelectorCoordinate must resolve the reject button's viewport-center
    // coordinates for the real coordinate click the poll dispatches.
    const didomi = CMP_REGISTRY.find((vendor) => vendor.vendor === "Didomi");

    assert.ok(didomi, "the CMP registry seeds the Didomi vendor");

    const fixture = "<div id=\"didomi-host\"><div class=\"didomi-popup-notice\"><button id=\"didomi-notice-disagree-button\">Disagree</button></div></div>";

    withDocument(fixture, () => {

      assert.ok(document.querySelector(didomi.detect), "the banner is detected by the vendor's detect selector");

      stubRect(requireElement(didomi.reject), { height: 30, width: 120, x: 200, y: 400 });

      assert.deepEqual(locateSelectorCoordinate(didomi.reject), { x: 260, y: 415 }, "the reject control resolves to its viewport-center coordinates");
    });
  });

  test("returns null when the selector matches nothing", () => {

    withDocument("<div class=\"guide\"><button>NBC</button></div>", () => {

      assert.equal(locateSelectorCoordinate("#missing"), null, "an absent element cannot be coordinate-clicked");
    });
  });

  test("returns null for a present-but-unlaid-out element with zero layout size", () => {

    // A display:none or otherwise unlaid-out control is present in the DOM but has no geometry to click. happy-dom's default zero-size rect exercises the guard.
    withDocument("<button id=\"hidden\">Reject</button>", () => {

      assert.equal(locateSelectorCoordinate("#hidden"), null, "a zero-size element is skipped so the caller keeps polling");
    });
  });
});

describe("clickSelectorInPage - synthetic dismisser", () => {

  test("clicks a matched element and reports \"clicked\"", () => {

    withDocument("<button id=\"modal-close\">Close</button>", () => {

      const button = requireElement("#modal-close");

      let clicks = 0;

      button.addEventListener("click", () => {

        clicks++;
      });

      assert.equal(clickSelectorInPage("#modal-close"), "clicked", "a matched element is clicked");
      assert.equal(clicks, 1, "the synthetic click dispatches a real click event");
    });
  });

  test("reports \"absent\" when a valid selector matches nothing", () => {

    withDocument("<div class=\"guide\"></div>", () => {

      assert.equal(clickSelectorInPage("#nope"), "absent", "a valid selector matching nothing keeps the action armed");
    });
  });

  test("reports \"invalid-selector\" when the selector is malformed", () => {

    // A malformed selector makes querySelector throw a SyntaxError; the in-page guard converts it to "invalid-selector" so the caller disables just this action rather
    // than the whole poll stopping.
    withDocument("<div></div>", () => {

      assert.equal(clickSelectorInPage(":::malformed"), "invalid-selector", "a malformed selector is reported, not thrown");
    });
  });
});
