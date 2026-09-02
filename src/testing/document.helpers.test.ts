/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * document.helpers.test.ts: Unit tests for the synthetic-DOM fixture helper. What earns coverage here is the global-slot discipline rather than happy-dom itself:
 * the helper mutates two process globals for the duration of a body, and a restore that misses either slot leaks a synthetic DOM into every suite that follows.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { withDocument } from "./document.helpers.ts";

// The two global slots the helper owns, reached through one typed view so the tests can read them without the DOM lib's non-optional declarations getting in the way.
const globalSlots = globalThis as { document?: unknown; window?: unknown };

describe("withDocument", () => {

  test("installs the document and the window on the globals and hands the window to the body", () => {

    // The reason the helper seeds both slots: a serialized in-page function reaches its DOM through the bare globals, so a body that reads either one must find the
    // fixture there. The body's window argument has to be the very object installed on the window slot, or a test seeding a page-side binding on it would be
    // decorating a different window than the function under test reads.
    const html = "<div id=\"fixture\">marker</div>";

    withDocument(html, (window) => {

      assert.equal(globalSlots.document, window.document, "the document slot holds the fixture's document");
      assert.equal(globalSlots.window, window, "the window slot holds the window handed to the body");
      assert.equal(window.document.body.innerHTML, html, "the fixture markup is the document body");
    });
  });

  test("deletes both slots when they were absent before the call", () => {

    // Deleting rather than assigning undefined is the contract: an assignment leaves the key present, and code that branches on `"document" in globalThis` - the
    // ordinary way to ask whether a DOM exists - would then take the DOM branch and read undefined.
    assert.equal("document" in globalSlots, false, "the document slot starts absent in a plain Node process");
    assert.equal("window" in globalSlots, false, "the window slot starts absent in a plain Node process");

    withDocument("<p>body</p>", () => undefined);

    assert.equal("document" in globalSlots, false, "the document slot is gone, not merely undefined");
    assert.equal("window" in globalSlots, false, "the window slot is gone, not merely undefined");
  });

  test("restores pre-existing slot values by identity rather than deleting them", () => {

    // A suite that installs its own DOM around a withDocument call must get that DOM back. Identity is what the assertion pins: an equal-but-different object would
    // mean the helper rebuilt the slot instead of putting the caller's own back.
    const priorDocument = { marker: "document" };
    const priorWindow = { marker: "window" };

    globalSlots.document = priorDocument;
    globalSlots.window = priorWindow;

    try {

      withDocument("<p>body</p>", () => undefined);

      assert.equal(globalSlots.document, priorDocument, "the caller's document is back in its slot");
      assert.equal(globalSlots.window, priorWindow, "the caller's window is back in its slot");
    } finally {

      delete globalSlots.document;
      delete globalSlots.window;
    }
  });

  test("restores both slots when the body throws, and propagates the throw", () => {

    // The restore lives in a finally for exactly this case. A failing assertion inside a fixture body is the common way a body throws, and a restore placed after
    // the call instead would leave the synthetic DOM installed for every test that follows the failure.
    assert.throws(() => withDocument("<p>body</p>", () => {

      throw new Error("body failed");
    }), /body failed/, "the body's error reaches the caller");

    assert.equal("document" in globalSlots, false, "the document slot was restored despite the throw");
    assert.equal("window" in globalSlots, false, "the window slot was restored despite the throw");
  });
});
