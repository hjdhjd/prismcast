/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * document.helpers.ts: A synthetic DOM installed as the process globals for the duration of a test body, and the page-side bindings an in-page function reads
 * off it.
 *
 * Both the document and the window are seeded, because that is what the code under test expects to find. An in-page function crosses the page.evaluate boundary by
 * source serialization rather than by reference, so it closes over nothing and reaches its DOM through the bare `document` and `window` globals a real page supplies.
 * A test that runs such a function directly has to supply both the same way, or the half it did not seed reads as undefined.
 */
import { Window } from "happy-dom";

/**
 * Seeds the page-side video selector an in-page callback reads. happy-dom's Window carries no declaration for the binding production injects via
 * evaluateOnNewDocument, so the seeding goes through a narrow view of the window.
 * @param window - The window backing the fixture document.
 * @param video - What the selector returns: a fixture element, a stand-in scripted to answer one method, or null for the rows that exercise the no-video path.
 */
export function seedVideoSelector(window: Window, video: unknown): void {

  (window as unknown as { __prismcastSelectVideo?: (type: string) => unknown }).__prismcastSelectVideo = (): unknown => video;
}

/**
 * Runs a body with the given fixture markup installed as the global document, backed by happy-dom. The window backing that document is installed alongside it and
 * handed to the body, so a test can seed the page-side bindings production installs on the window before invoking the function under test. Both global slots are
 * restored on the way out, whether the body returns or throws: a slot that held a value is reassigned, a slot that was absent is deleted so it stays absent.
 * node:test runs each test file in its own process and the tests within a file run sequentially, so the scoped global mutation cannot bleed across suites.
 * @param html - The fixture markup to install as the document body.
 * @param body - The body to run against the fixture, receiving the window that backs it.
 * @returns Whatever the body returns.
 */
export function withDocument<T>(html: string, body: (window: Window) => T): T {

  const window = new Window();

  window.document.body.innerHTML = html;

  const globalSlots = globalThis as { document?: unknown; window?: unknown };
  const hadDocument = "document" in globalSlots;
  const hadWindow = "window" in globalSlots;
  const priorDocument = globalSlots.document;
  const priorWindow = globalSlots.window;

  globalSlots.document = window.document;
  globalSlots.window = window;

  try {

    return body(window);
  } finally {

    if(hadDocument) {

      globalSlots.document = priorDocument;
    } else {

      delete globalSlots.document;
    }

    if(hadWindow) {

      globalSlots.window = priorWindow;
    } else {

      delete globalSlots.window;
    }
  }
}
