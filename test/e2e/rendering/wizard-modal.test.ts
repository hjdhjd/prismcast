/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * wizard-modal.test.ts: Integration coverage for the wizard modal shell builder. generateWizardModal in src/routes/components.ts is the SSOT for every wizard
 * and dialog modal in the app (Browse Channels wizard, profile wizard, import wizard, Import/Export dialogs). A regression in the shell-builder breaks every
 * wizard at once - the inverse, where one wizard's shell drifts independent of the others, is structurally impossible because there is exactly one builder.
 *
 * Tests pin the SHELL contract: HTML structure (overlay, header, content, optional step indicator, optional error display, footer buttons), button-ownership
 * rules (role-tagged controller buttons vs. inline-onclick custom buttons), step-indicator initial state (always step 1 active server-side; client manages
 * transitions), single-step dialog mode (no step indicator), and idempotency (same input -> same output). The controller's runtime navigation behavior is
 * client-side; tests pin the SURFACE the controller mounts to, not the controller itself - if the shell is correct, the controller has what it needs.
 *
 * The WizardModalOptions interface has no `activeStep` parameter. The server always renders the first step as active; step transitions are client-side via
 * createWizardController(). The test below pins the actual contract: server marks step 1 active, all other steps non-active. A future refactor that adds
 * server-side step rendering would intentionally fail this test, signalling the contract change.
 *
 * No harness required. generateWizardModal is a pure string-producing function whose only collaborators are the markup helpers escapeHtml and serializeAttrs
 * (the latter also reached via generateWizardButton); tests assert directly against returned
 * HTML via regex/substring scans rather than booting the integration harness. This mirrors the rendering-tier pattern for pure renderers (channels-table.test.ts
 * does carry the harness because its renderer depends on the channel state initialized by initializePersistence; the wizard shell has no such dependency).
 */
import { describe, test } from "node:test";
import type { WizardModalButton } from "../../../src/routes/components.ts";
import assert from "node:assert/strict";
import { generateWizardModal } from "../../../src/routes/components.ts";

/* A minimal-but-realistic 4-step wizard configuration shared across several tests. Tests that need a different shape (single-step dialog, custom button mix)
 * compose their own options object inline rather than mutating this one - mutation across tests would couple them.
 */
const FOUR_STEP_OPTIONS = {

  buttons: [
    { label: "Back", position: "left" as const, role: "back" as const, variant: "secondary" as const },
    { action: "close-wizard", label: "Cancel", position: "right" as const, variant: "secondary" as const },
    { label: "Next", position: "right" as const, role: "next" as const, variant: "primary" as const }
  ] satisfies WizardModalButton[],
  errorId: "test-wizard-error",
  id: "test-wizard",
  steps: [ "Choose source", "Configure", "Review", "Finish" ],
  title: "Test Wizard"
};

describe("generateWizardModal - shell HTML structure", () => {

  test("renders the standard regions: root, content box, header with title and X close, content area, error display, footer buttons", async () => {

    /* The shell-builder's structural contract. Every emitted modal must include: a root <div> with class wizard-modal and the supplied id, a content box
     * (wizard-modal-content), a header (wizard-header) containing an <h3> title and a wizard-close X button with aria-label="Close", a content area
     * (wizard-content), the optional error display when errorId is supplied, and a footer (wizard-buttons) with the right-side button container.
     *
     * Each region is asserted via a focused regex that captures the structural attribute, not the precise byte sequence - the renderer can change incidental
     * spacing or attribute order and tests still pass, but a missing region or a renamed structural class fails loudly.
     */
    const html = generateWizardModal(FOUR_STEP_OPTIONS);

    // Root element with id and the wizard-modal class. Initial display:none is part of the contract - the controller toggles visibility.
    assert.match(html, /<div [^>]*id="test-wizard"[^>]*class="wizard-modal"[^>]*style="display: none;"[^>]*>/,
      "root <div> carries id, wizard-modal class, and display:none");

    // Content box.
    assert.match(html, /<div class="wizard-modal-content"/, "content box carries the wizard-modal-content class");

    // Header with title and the X close button.
    assert.match(html, /<div class="wizard-header[^"]*">/, "header carries the wizard-header class");
    assert.match(html, /<h3>Test Wizard<\/h3>/, "header includes the title in an h3");
    // Attribute order follows the object-literal key order serializeAttrs preserves (alphabetical here by house style, not enforced by the helper), so rather
    // than asserting an exact byte sequence we slice the button tag and check attributes independently.
    const closeMatch = /<button [^>]*class="wizard-close"[^>]*>✕<\/button>/.exec(html);

    assert.ok(closeMatch, "X close button is rendered");
    assert.match(closeMatch[0], /aria-label="Close"/, "X close button carries aria-label=\"Close\"");
    assert.match(closeMatch[0], /class="wizard-close"/, "X close button carries class=\"wizard-close\"");

    // Content area. The 4-step wizard does NOT use the compact variant.
    assert.match(html, /<div class="wizard-content"[^>]*id="test-wizard-content"/, "content area carries wizard-content class and the derived content id");
    assert.doesNotMatch(html, /class="wizard-content wizard-content-compact"/, "the stepped wizard does not get the compact content class");

    // Error display when errorId is provided.
    assert.match(html, /<div id="test-wizard-error" class="wizard-error" style="display: none;"><\/div>/, "error display present when errorId provided");

    // Footer buttons container.
    assert.match(html, /<div class="wizard-buttons">/, "footer carries the wizard-buttons class");
    assert.match(html, /<div class="wizard-buttons-right">/, "footer includes the right-side button container");
  });

  test("step indicator: server renders step 1 as active and every other step without the active marker (client manages transitions)", async () => {

    /* The server has no `activeStep` parameter on WizardModalOptions; it always emits step 1 active. The wizard controller in
     * shared.ts is responsible for runtime step transitions - removing/adding `active` on data-step elements as the user clicks Next or Back. This test pins the
     * server-side contract: regardless of which step the user is currently on, the initially-rendered HTML marks ONLY step 1 as active.
     *
     * A regression that quietly added server-side activeStep handling without coordinating the client controller would surface here as a test divergence. A
     * deliberate move to server-side step rendering would intentionally fail this test, signalling the contract change.
     */
    const html = generateWizardModal(FOUR_STEP_OPTIONS);

    // Step indicator container with the derived id.
    assert.match(html, /<div class="wizard-steps" id="test-wizard-steps">/, "step indicator container carries wizard-steps class and the derived steps id");

    /* Each of the 4 steps emitted with its data-step index. The first step carries active; the others do not. The regex captures (class) and (step number) so
     * the loop can assert the class on each indexed step.
     */
    const stepPattern =
      /<div class="(wizard-step(?: active)?)" data-step="(\d+)">[\s\S]*?<span class="step-circle">\d+<\/span>[\s\S]*?<span class="step-label">[^<]+<\/span><\/div>/g;
    const stepMatches = [...html.matchAll(stepPattern)];

    assert.equal(stepMatches.length, 4, "exactly 4 step elements rendered");

    for(const match of stepMatches) {

      const classAttr = match[1] ?? "";
      const stepIndex = match[2] ?? "";
      const expectedActive = (stepIndex === "1") ? "wizard-step active" : "wizard-step";

      assert.equal(classAttr, expectedActive, "step " + stepIndex + " carries the expected class");
    }

    // Connecting lines between steps: count - 1 = 3 line dividers.
    const lineMatches = [...html.matchAll(/<div class="wizard-step-line"><\/div>/g)];

    assert.equal(lineMatches.length, 3, "wizard-step-line dividers between consecutive steps");
  });

  test("single-step dialog mode: no step indicator block; content uses the compact variant; rest of shell intact", async () => {

    /* Dialogs (Import/Export) omit the steps array. The renderer must NOT emit the wizard-steps block, AND must use wizard-content-compact for the content area
     * (the compact variant removes the min-height that stepped wizards need). Header, content, footer continue to render. This pins the dialog vs. wizard
     * dichotomy at the structural level - the dichotomy is the steps array's presence/absence, with no third option.
     */
    const html = generateWizardModal({

      body: "<p>Dialog body content</p>",
      buttons: [
        { action: "close-dialog", label: "Cancel", position: "right", variant: "secondary" },
        { action: "save-dialog", label: "Save", position: "right", variant: "primary" }
      ],
      closeAction: "close-dialog",
      id: "test-dialog",
      title: "Test Dialog"
    });

    // Wizard root and basic structure are present.
    assert.match(html, /<div [^>]*id="test-dialog"[^>]*class="wizard-modal"/, "dialog still has the modal root");
    assert.match(html, /<h3>Test Dialog<\/h3>/, "dialog has the title");

    // Step indicator block is absent.
    assert.doesNotMatch(html, /class="wizard-steps"/, "dialog mode emits no wizard-steps block");
    assert.doesNotMatch(html, /class="wizard-step-line"/, "dialog mode emits no step-line dividers");
    assert.doesNotMatch(html, /data-step=/, "dialog mode has no data-step attributes");

    // Content area uses the compact variant.
    assert.match(html, /<div class="wizard-content wizard-content-compact" id="test-dialog-content">[\s\S]*<p>Dialog body content<\/p>[\s\S]*<\/div>/,
      "dialog content area uses compact class and contains the pre-filled body");

    // Close button carries data-click-action="close-dialog" because closeAction was provided. The dispatcher routes the click to the registered handler;
    // no inline onclick attribute is emitted.
    assert.match(html, /<button [^>]*class="wizard-close"[^>]*data-click-action="close-dialog"[^>]*>✕<\/button>/,
      "non-controller dialog binds the X close via data-click-action carrying the closeAction");
    assert.doesNotMatch(html, /<button [^>]*class="wizard-close"[^>]*onclick=/,
      "X close button must not carry an inline onclick attribute");
  });
});

describe("generateWizardModal - button ownership rules", () => {

  test("role-tagged buttons emit data-wizard-role; custom action buttons emit data-click-action; neither emits inline onclick", async () => {

    /* The button-ownership rule: standard navigation buttons (Back, Next, Cancel, X close) are managed by the wizard controller via closure-scoped handlers
     * attached at construction time, identified through data-wizard-role. Custom action buttons (Save, Apply, Finish) declare their click intent via
     * data-click-action; the project-wide action dispatcher (shared.ts) routes the click to the registered handler. Neither path emits an inline onclick
     * attribute - all event mechanics flow through delegation.
     *
     * The renderer's structural pin: a role-tagged button MUST emit data-wizard-role and MUST NOT emit data-click-action or onclick. A custom action button
     * MUST emit data-click-action and MUST NOT emit data-wizard-role or onclick. No button anywhere carries inline onclick.
     */
    const html = generateWizardModal({

      buttons: [
        { label: "Back", position: "left", role: "back" },
        { action: "save-payload", label: "Save", position: "right", variant: "primary" },
        { label: "Cancel", position: "right", role: "close" },
        { label: "Next", position: "right", role: "next" }
      ] satisfies WizardModalButton[],
      id: "ownership-test",
      title: "Ownership Test"
    });

    // Each role-tagged button emits data-wizard-role with the role value and carries neither data-click-action nor onclick.
    for(const [ label, role ] of [ [ "Back", "back" ], [ "Cancel", "close" ], [ "Next", "next" ] ] as const) {

      const buttonPattern = new RegExp("<button [^>]*>" + label + "</button>");
      const buttonMatch = buttonPattern.exec(html);

      assert.ok(buttonMatch, "rendered HTML must contain the " + label + " button");

      const tag = buttonMatch[0];

      assert.match(tag, new RegExp("data-wizard-role=\"" + role + "\""), label + " button carries data-wizard-role=\"" + role + "\"");
      assert.doesNotMatch(tag, /data-click-action=/, label + " button does NOT carry data-click-action (controller manages handler)");
      assert.doesNotMatch(tag, /onclick=/, label + " button does NOT carry inline onclick");
    }

    // The Save custom button: emits data-click-action, does NOT emit data-wizard-role or onclick.
    const saveMatch = /<button [^>]*>Save<\/button>/.exec(html);

    assert.ok(saveMatch, "rendered HTML must contain the Save button");

    const saveTag = saveMatch[0];

    assert.match(saveTag, /data-click-action="save-payload"/, "Save custom button carries its data-click-action");
    assert.doesNotMatch(saveTag, /data-wizard-role=/, "Save custom button does NOT carry data-wizard-role");
    assert.doesNotMatch(saveTag, /onclick=/, "Save custom button does NOT carry inline onclick");
  });
});

describe("generateWizardModal - idempotency", () => {

  test("calling the builder twice with identical options returns the byte-identical HTML string", async () => {

    /* The shell builder must be a pure function of its options - no global state, no time-dependent fields, no random ids. Two calls with the same options
     * produce identical strings. This is the inverse of the regression class "wizard rendering depends on call order or hidden global state" - if the
     * builder ever introduced a non-deterministic field, every diff-based test in the integration tier would become flaky.
     *
     * We use deep equality on the raw strings, not normalized ones, because the contract is byte-identical output: any divergence (extra whitespace,
     * reordered attributes) signals an architectural change that should surface here rather than passing silently.
     */
    const first = generateWizardModal(FOUR_STEP_OPTIONS);
    const second = generateWizardModal(FOUR_STEP_OPTIONS);

    assert.equal(first, second, "two calls with the same options must produce byte-identical HTML strings");
  });
});
