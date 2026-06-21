/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * components.wizard.test.ts: Unit tests for the wizard modal generator in components.ts. The wizard modal is the most structurally complex component the
 * module exports - it builds an overlay, header, optional step indicator, content area, optional error display, and footer buttons across left/right slots.
 * The button-ownership rule (role-tagged buttons Back/Next/Close attach handlers via the wizard controller, custom buttons Save/Apply/Finish declare a
 * data-click-action dispatched by the project-wide action dispatcher, and no button carries an inline onclick) is the most subtle invariant locked here. The
 * other components (alerts, buttons, badges, inputs, selects, etc.) are tested in components.test.ts; we split this file out to keep both under the LOC cap.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { generateWizardModal } from "./components.ts";

describe("generateWizardModal", () => {

  test("renders the modal root with the configured id and title", () => {

    const html = generateWizardModal({ buttons: [], id: "mywiz", title: "My Wizard" });

    assert.match(html, /<div id="mywiz" class="wizard-modal" style="display: none;">/);
    assert.match(html, /<h3>My Wizard<\/h3>/);
    // The X close button. Attributes are emitted in the order they appear in the alphabetically-authored object literal (serializeAttrs preserves insertion
    // order), so the sequence is aria-label, class, type, and the button carries no inline onclick.
    assert.match(html, /<button aria-label="Close" class="wizard-close" type="button">/);
  });

  test("includes a step indicator when steps are provided", () => {

    const html = generateWizardModal({

      buttons: [],
      id: "wiz",
      steps: [ "Step One", "Step Two", "Step Three" ],
      title: "Title"
    });

    assert.match(html, /<div class="wizard-steps" id="wiz-steps">/);
    assert.match(html, /data-step="1".*Step One/s);
    assert.match(html, /data-step="2".*Step Two/s);
    assert.match(html, /data-step="3".*Step Three/s);
    // First step is marked active.
    assert.match(html, /class="wizard-step active" data-step="1"/);
  });

  test("inserts step lines between adjacent steps but not after the last (boundary)", () => {

    const html = generateWizardModal({

      buttons: [],
      id: "wiz",
      steps: [ "A", "B", "C" ],
      title: "T"
    });

    // Three steps means two connecting lines.
    const lineMatches = html.match(/<div class="wizard-step-line">/g) ?? [];

    assert.equal(lineMatches.length, 2, "should have N-1 connecting lines for N steps");
  });

  test("uses wizard-content-compact when no steps are provided (simple dialog mode)", () => {

    const html = generateWizardModal({ buttons: [], id: "dlg", title: "T" });

    assert.match(html, /class="wizard-content wizard-content-compact"/);
    assert.doesNotMatch(html, /<div class="wizard-steps"/);
  });

  test("renders left- and right-positioned buttons in their respective slots", () => {

    const html = generateWizardModal({

      buttons: [
        { label: "Back", position: "left", role: "back" },
        { label: "Next", position: "right", role: "next" },
        { label: "Cancel", position: "right", role: "close" }
      ],
      id: "w",
      title: "T"
    });

    assert.match(html, /<div class="wizard-buttons">/);
    assert.match(html, /<div class="wizard-buttons-right">/);
    assert.match(html, /data-wizard-role="back"/);
    assert.match(html, /data-wizard-role="next"/);
    assert.match(html, /data-wizard-role="close"/);
  });

  test("emits role buttons with data-wizard-role and no inline onclick", () => {

    const html = generateWizardModal({

      buttons: [{ label: "Back", position: "left", role: "back" }],
      id: "w",
      title: "T"
    });

    assert.match(html, /data-wizard-role="back"/);
    // Role-tagged buttons must NOT carry an onclick attribute (the controller binds via JS).
    assert.doesNotMatch(html, /role="back"[^>]*onclick=/);
  });

  test("emits non-role buttons with data-click-action", () => {

    // Custom buttons (Save, Apply, Finish) declare an action name dispatched by the project-wide action dispatcher in shared.ts. They do NOT carry an inline
    // onclick attribute - the dispatch is by data-click-action only.
    const html = generateWizardModal({

      buttons: [{ action: "do-save", label: "Save", position: "right" }],
      id: "w",
      title: "T"
    });

    assert.match(html, /data-click-action="do-save"/);
    assert.doesNotMatch(html, /data-wizard-role/);
    assert.doesNotMatch(html, /onclick=/, "buttons must never carry an inline onclick attribute");
  });

  test("hides buttons whose visible flag is false", () => {

    // The attrs literal in generateWizardButton is authored alphabetically and serializeAttrs preserves insertion order, so style is not guaranteed to be the
    // last attribute. Match the style attribute as a standalone token.
    const html = generateWizardModal({

      buttons: [{ label: "Hidden", position: "right", visible: false }],
      id: "w",
      title: "T"
    });

    assert.match(html, /<button[^>]* style="display: none;"[^>]*>Hidden<\/button>/);
  });

  test("renders the description below the title when provided", () => {

    const html = generateWizardModal({

      buttons: [],
      description: "Subtitle text",
      id: "w",
      title: "T"
    });

    assert.match(html, /class="wizard-header wizard-header-wrap"/);
    assert.match(html, /<div class="wizard-description">Subtitle text<\/div>/);
  });

  test("uses wizard-header (no wrap) when no description is provided", () => {

    const html = generateWizardModal({ buttons: [], id: "w", title: "T" });

    assert.match(html, /class="wizard-header"/);
    assert.doesNotMatch(html, /class="wizard-header wizard-header-wrap"/);
  });

  test("renders the validation error div when errorId is provided", () => {

    const html = generateWizardModal({

      buttons: [],
      errorId: "my-err",
      id: "w",
      title: "T"
    });

    assert.match(html, /<div id="my-err" class="wizard-error" style="display: none;"><\/div>/);
  });

  test("omits the validation error div when errorId is not provided", () => {

    const html = generateWizardModal({ buttons: [], id: "w", title: "T" });

    assert.doesNotMatch(html, /class="wizard-error"/);
  });

  test("inserts pre-filled body content into the content div", () => {

    const html = generateWizardModal({

      body: "<p>preloaded</p>",
      buttons: [],
      id: "w",
      title: "T"
    });

    assert.match(html, /<div class="wizard-content wizard-content-compact" id="w-content"><p>preloaded<\/p><\/div>/);
  });

  test("attaches the close action to the X button when closeAction is provided", () => {

    // Non-controller modals (Import/Export) pass closeAction so the X button dispatches a named action via the project-wide action dispatcher. Controller-
    // managed modals omit closeAction and let the wizard controller discover the .wizard-close button to attach its own handler.
    const html = generateWizardModal({

      buttons: [],
      closeAction: "my-close",
      id: "w",
      title: "T"
    });

    assert.match(html, /<button aria-label="Close" class="wizard-close" data-click-action="my-close" type="button">/);
    assert.doesNotMatch(html, /onclick=/, "the X button must never carry an inline onclick attribute");
  });

  test("emits data-* attributes from dataAttributes", () => {

    const html = generateWizardModal({

      buttons: [],
      dataAttributes: { mode: "edit", "step-key": "form" },
      id: "w",
      title: "T"
    });

    assert.match(html, /data-mode="edit"/);
    assert.match(html, /data-step-key="form"/);
  });

  test("uses an explicit titleId when provided so the client can update the title dynamically", () => {

    const html = generateWizardModal({

      buttons: [],
      id: "w",
      title: "Title",
      titleId: "my-title"
    });

    assert.match(html, /<h3 id="my-title">Title<\/h3>/);
  });

  test("appends data blocks after the modal root", () => {

    const html = generateWizardModal({

      buttons: [],
      dataBlocks: [
        "<script type=\"application/json\" id=\"a\">{}</script>",
        "<script type=\"application/json\" id=\"b\">[]</script>"
      ],
      id: "w",
      title: "T"
    });

    assert.match(html, /<script type="application\/json" id="a">/);
    assert.match(html, /<script type="application\/json" id="b">/);

    // Data blocks come after the closing of the modal root.
    const modalEnd = html.lastIndexOf("</div>\n</div>");

    assert.notEqual(modalEnd, -1, "should have a closing pair for content box and modal root");
  });

  test("respects the maxWidth override on the content box", () => {

    const html = generateWizardModal({

      buttons: [],
      id: "w",
      maxWidth: "500px",
      title: "T"
    });

    assert.match(html, /class="wizard-modal-content" style="max-width: 500px;"/);
  });

  test("uses the default contentId of \"<id>-content\" when no contentId is provided", () => {

    const html = generateWizardModal({ buttons: [], id: "wz", title: "T" });

    assert.match(html, /id="wz-content"/);
  });

  test("uses the explicit contentId when provided", () => {

    const html = generateWizardModal({

      buttons: [],
      contentId: "custom-content",
      id: "wz",
      title: "T"
    });

    assert.match(html, /id="custom-content"/);
  });
});
