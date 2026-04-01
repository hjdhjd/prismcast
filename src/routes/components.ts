/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * components.ts: Reusable UI components for PrismCast web pages.
 */
import { escapeHtml } from "../utils/index.js";

/* This module provides reusable HTML component generators for consistent UI across PrismCast. Each component returns an HTML string that can be included in page
 * generation. Components use CSS custom properties from theme.ts for styling, ensuring automatic dark mode support.
 */

/**
 * Alert type for styling.
 */
export type AlertType = "error" | "success" | "warning";

/**
 * Generates an alert box HTML with title and message.
 * @param type - The alert type (success, error, warning).
 * @param title - The alert title.
 * @param message - The alert message (can include HTML).
 * @param escapeMessage - Whether to escape the message HTML (default true).
 * @returns HTML string for the alert box.
 */
export function generateAlert(type: AlertType, title: string, message: string, escapeMessage = true): string {

  const escapedMessage = escapeMessage ? escapeHtml(message) : message;

  return [
    "<div class=\"alert alert-" + type + "\">",
    "<div class=\"alert-title\">" + escapeHtml(title) + "</div>",
    escapedMessage,
    "</div>"
  ].join("\n");
}

/**
 * Generates a simple alert box without a title.
 * @param type - The alert type (success, error, warning).
 * @param message - The alert message.
 * @returns HTML string for the alert box.
 */
export function generateSimpleAlert(type: AlertType, message: string): string {

  return "<div class=\"alert alert-" + type + "\">" + escapeHtml(message) + "</div>";
}

/**
 * Button variant for styling.
 */
export type ButtonVariant = "danger" | "delete" | "edit" | "primary" | "secondary";

/**
 * Button size for styling.
 */
export type ButtonSize = "md" | "sm";

/**
 * Options for button generation.
 */
export interface ButtonOptions {

  // Additional CSS classes.
  className?: string;

  // Whether the button is disabled.
  disabled?: boolean;

  // Button ID attribute.
  id?: string;

  // Inline onclick handler.
  onclick?: string;

  // Button size (default: md).
  size?: ButtonSize;

  // Button type attribute (default: button).
  type?: "button" | "reset" | "submit";

  // Button style variant.
  variant: ButtonVariant;
}

/**
 * Generates a button HTML element.
 * @param label - The button label text.
 * @param options - Button configuration options.
 * @returns HTML string for the button.
 */
export function generateButton(label: string, options: ButtonOptions): string {

  const { className, disabled, id, onclick, size = "md", type = "button", variant } = options;

  const classes = [ "btn", "btn-" + variant ];

  if(size === "sm") {

    classes.push("btn-sm");
  }

  if(className) {

    classes.push(className);
  }

  const attrs: string[] = [
    "type=\"" + type + "\"",
    "class=\"" + classes.join(" ") + "\""
  ];

  if(id) {

    attrs.push("id=\"" + escapeHtml(id) + "\"");
  }

  if(onclick) {

    attrs.push("onclick=\"" + escapeHtml(onclick) + "\"");
  }

  if(disabled) {

    attrs.push("disabled");
  }

  return "<button " + attrs.join(" ") + ">" + escapeHtml(label) + "</button>";
}

/**
 * Badge variant for styling.
 */
export type BadgeVariant = "builtin" | "custom" | "env" | "flag" | "override";

/**
 * Generates a badge HTML element.
 * @param label - The badge label text.
 * @param variant - The badge style variant.
 * @returns HTML string for the badge.
 */
export function generateBadge(label: string, variant: BadgeVariant): string {

  return "<span class=\"badge badge-" + variant + "\">" + escapeHtml(label) + "</span>";
}

/**
 * Generates a status indicator (colored dot with label).
 * @param status - The status type (healthy, error, etc.).
 * @param label - The label text.
 * @returns HTML string for the status indicator.
 */
export function generateStatusIndicator(status: string, label: string): string {

  return "<span class=\"status-indicator status-" + escapeHtml(status) + "\"><span class=\"status-dot\">&#9679;</span> " + escapeHtml(label) + "</span>";
}

/**
 * Options for text input generation.
 */
export interface TextInputOptions {

  // Whether the input is disabled.
  disabled?: boolean;

  // Hint text displayed below the input.
  hint?: string;

  // Input ID attribute.
  id: string;

  // Maximum value (for number inputs).
  max?: number;

  // Minimum value (for number inputs).
  min?: number;

  // Input name attribute.
  name: string;

  // Pattern for validation.
  pattern?: string;

  // Placeholder text.
  placeholder?: string;

  // Whether the input is required.
  required?: boolean;

  // Step value (for number inputs).
  step?: string;

  // Input type (text, number, url, etc.).
  type?: string;

  // Current value.
  value?: string;
}

/**
 * Generates a form row with label and text input.
 * @param label - The input label.
 * @param options - Input configuration options.
 * @returns HTML string for the form row.
 */
export function generateTextInput(label: string, options: TextInputOptions): string {

  const { disabled, hint, id, max, min, name, pattern, placeholder, required, step, type = "text", value = "" } = options;

  const lines: string[] = [];

  lines.push("<div class=\"form-row\">");
  lines.push("<label for=\"" + escapeHtml(id) + "\">" + escapeHtml(label) + "</label>");

  const inputAttrs: string[] = [
    "type=\"" + type + "\"",
    "id=\"" + escapeHtml(id) + "\"",
    "name=\"" + escapeHtml(name) + "\"",
    "value=\"" + escapeHtml(value) + "\""
  ];

  if(required) {

    inputAttrs.push("required");
  }

  if(disabled) {

    inputAttrs.push("disabled");
  }

  if(pattern) {

    inputAttrs.push("pattern=\"" + escapeHtml(pattern) + "\"");
  }

  if(placeholder) {

    inputAttrs.push("placeholder=\"" + escapeHtml(placeholder) + "\"");
  }

  if(min !== undefined) {

    inputAttrs.push("min=\"" + String(min) + "\"");
  }

  if(max !== undefined) {

    inputAttrs.push("max=\"" + String(max) + "\"");
  }

  if(step) {

    inputAttrs.push("step=\"" + escapeHtml(step) + "\"");
  }

  lines.push("<input " + inputAttrs.join(" ") + ">");

  if(hint) {

    lines.push("<div class=\"hint\">" + escapeHtml(hint) + "</div>");
  }

  lines.push("</div>");

  return lines.join("\n");
}

/**
 * Option item for select dropdown.
 */
export interface SelectOption {

  // Option label.
  label: string;

  // Whether this option is selected.
  selected?: boolean;

  // Option value.
  value: string;
}

/**
 * Options for select dropdown generation.
 */
export interface SelectOptions {

  // Whether the select is disabled.
  disabled?: boolean;

  // Hint text displayed below the select.
  hint?: string;

  // Select ID attribute.
  id: string;

  // Select name attribute.
  name: string;

  // Available options.
  options: SelectOption[];

  // Whether a selection is required.
  required?: boolean;
}

/**
 * Generates a form row with label and select dropdown.
 * @param label - The select label.
 * @param config - Select configuration options.
 * @returns HTML string for the form row.
 */
export function generateSelect(label: string, config: SelectOptions): string {

  const { disabled, hint, id, name, options, required } = config;

  const lines: string[] = [];

  lines.push("<div class=\"form-row\">");
  lines.push("<label for=\"" + escapeHtml(id) + "\">" + escapeHtml(label) + "</label>");

  const selectAttrs: string[] = [
    "id=\"" + escapeHtml(id) + "\"",
    "name=\"" + escapeHtml(name) + "\""
  ];

  if(required) {

    selectAttrs.push("required");
  }

  if(disabled) {

    selectAttrs.push("disabled");
  }

  lines.push("<select " + selectAttrs.join(" ") + ">");

  for(const option of options) {

    const selectedAttr = option.selected ? " selected" : "";

    lines.push("<option value=\"" + escapeHtml(option.value) + "\"" + selectedAttr + ">" + escapeHtml(option.label) + "</option>");
  }

  lines.push("</select>");

  if(hint) {

    lines.push("<div class=\"hint\">" + escapeHtml(hint) + "</div>");
  }

  lines.push("</div>");

  return lines.join("\n");
}


/**
 * Generates a section container with optional heading.
 * @param content - The section content (HTML).
 * @param heading - Optional section heading.
 * @param headingLevel - Heading level (2 or 3, default 3).
 * @returns HTML string for the section.
 */
export function generateSection(content: string, heading?: string, headingLevel: 2 | 3 = 3): string {

  const lines: string[] = [];

  lines.push("<div class=\"section\">");

  if(heading) {

    lines.push("<h" + String(headingLevel) + ">" + escapeHtml(heading) + "</h" + String(headingLevel) + ">");
  }

  lines.push(content);
  lines.push("</div>");

  return lines.join("\n");
}

/**
 * Generates a panel header with title and optional action button.
 * @param title - The panel title.
 * @param actionHtml - Optional action button HTML (not escaped).
 * @returns HTML string for the panel header.
 */
export function generatePanelHeader(title: string, actionHtml?: string): string {

  const lines: string[] = [];

  lines.push("<div class=\"panel-header\">");
  lines.push("<h2>" + escapeHtml(title) + "</h2>");

  if(actionHtml) {

    lines.push(actionHtml);
  }

  lines.push("</div>");

  return lines.join("\n");
}

// Wizard Modal.

/**
 * Button definition for a wizard modal footer.
 */
export interface WizardModalButton {

  // Button ID attribute. Required for buttons whose visibility is toggled by the client-side controller.
  id?: string;

  // Button label text.
  label: string;

  // Inline onclick handler for custom buttons. Omit for controller-managed buttons that have a role - the controller attaches handlers via JavaScript.
  onclick?: string;

  // Position in the footer. Left-positioned buttons sit on the leading edge (e.g., Back), right-positioned buttons sit on the trailing edge.
  position: "left" | "right";

  // Standard navigation role. When set, the button emits a data-wizard-role attribute and omits the onclick attribute. The client-side wizard controller
  // discovers role-tagged buttons within its modal and attaches handlers from inside the IIFE closure, avoiding the inline-onclick-to-global-scope problem.
  // Custom buttons (Save, Apply, Finish) omit the role and provide an onclick string pointing to a window-exposed function.
  role?: "back" | "close" | "next";

  // Button size (default: md).
  size?: ButtonSize;

  // Button style variant (default: secondary).
  variant?: ButtonVariant;

  // Whether the button is visible when the modal first renders. Defaults to true. Hidden buttons have display: none and are shown by the client controller.
  visible?: boolean;
}

/**
 * Configuration for generating a wizard modal shell. The shell provides consistent structure (overlay, header, optional step indicator, content area, optional
 * error display, and footer buttons) across all wizard and dialog modals. Content rendering is handled by client-side JavaScript or by pre-filling the body
 * parameter for simple dialogs.
 */
export interface WizardModalOptions {

  // Pre-filled HTML body for the content area. When provided, the content div is populated server-side. When omitted, the content div is empty for client-side
  // rendering. Used by simple dialogs like Import/Export that render their body at page generation time.
  body?: string;

  // Footer buttons in display order.
  buttons: WizardModalButton[];

  // ID for the content div. Defaults to "{id}-content" when omitted.
  contentId?: string;

  // Arbitrary data-* attributes on the modal root element. Keys are attribute names without the "data-" prefix.
  dataAttributes?: Record<string, string>;

  // JSON data blocks to embed after the modal. Each entry is a complete <script type="application/json"> element string.
  dataBlocks?: string[];

  // Subtitle text rendered below the title inside the header. When present, the header uses flex-wrap to accommodate the description on a second line.
  description?: string;

  // ID for the validation error div. When provided, a .wizard-error div is rendered between the content area and footer buttons.
  errorId?: string;

  // ID for the modal root element. Also used as a prefix for derived IDs (content, steps) when explicit IDs are not provided.
  id: string;

  // Override the default max-width of the modal content box. Useful for narrower dialogs like Import/Export.
  maxWidth?: string;

  // Close handler invoked by the header close button. Required for non-controller modals (Import/Export) where the X button needs an inline onclick. Optional
  // for controller-managed modals where the controller discovers the .wizard-close button and attaches its own handler from inside the IIFE closure.
  onClose?: string;

  // Step labels for the step indicator. When omitted, no step indicator is rendered (used for simple dialogs). The first step is marked active by default.
  steps?: string[];

  // ID for the step indicator container. Defaults to "{id}-steps" when steps are provided. The client-side controller uses this to scope step indicator queries.
  stepsId?: string;

  // Modal title text.
  title: string;

  // ID for the title element. When provided, the client can update the title dynamically (e.g., "New" vs. "Edit" mode).
  titleId?: string;
}

/**
 * Generates a wizard modal shell with consistent structure across all wizard and dialog modals. The shell includes an overlay, header (with optional description
 * and step indicator), a content area, an optional error display, and footer buttons. Content rendering is delegated to client-side JavaScript for stepped wizards,
 * or pre-filled via the body parameter for simple dialogs.
 *
 * @param options - Modal configuration.
 * @returns HTML string for the complete modal shell, including any embedded data blocks.
 */
export function generateWizardModal(options: WizardModalOptions): string {

  const { body, buttons, contentId, dataAttributes, dataBlocks, description, errorId, id, maxWidth, onClose, steps, stepsId, title, titleId } = options;

  const resolvedContentId = contentId ?? (id + "-content");
  const resolvedStepsId = stepsId ?? (id + "-steps");

  const lines: string[] = [];

  // Modal root. The data-* attributes are used by client code to pass server state to the client (e.g., setup-completed flag).
  const rootAttrs: string[] = [
    "id=\"" + escapeHtml(id) + "\"",
    "class=\"wizard-modal\"",
    "style=\"display: none;\""
  ];

  if(dataAttributes) {

    for(const [ key, value ] of Object.entries(dataAttributes)) {

      rootAttrs.push("data-" + escapeHtml(key) + "=\"" + escapeHtml(value) + "\"");
    }
  }

  lines.push("<div " + rootAttrs.join(" ") + ">");

  // Content box. The optional maxWidth override narrows the dialog for simpler modals like Import/Export.
  const contentBoxStyle = maxWidth ? " style=\"max-width: " + escapeHtml(maxWidth) + ";\"" : "";

  lines.push("<div class=\"wizard-modal-content\"" + contentBoxStyle + ">");

  // Header. When a description is present, the header uses flex-wrap so the description can sit below the title row while the close button stays top-right.
  const headerClass = description ? "wizard-header wizard-header-wrap" : "wizard-header";

  lines.push("<div class=\"" + headerClass + "\">");

  const titleTag = titleId ? "<h3 id=\"" + escapeHtml(titleId) + "\">" : "<h3>";

  lines.push(titleTag + escapeHtml(title) + "</h3>");
  // The X close button. For controller-managed modals, the controller discovers this button by class and attaches its own handler. For non-controller modals,
  // the inline onclick provides the close behavior directly.
  const closeAttrs = onClose ? " onclick=\"" + escapeHtml(onClose) + "\"" : "";

  lines.push("<button type=\"button\" class=\"wizard-close\" aria-label=\"Close\"" + closeAttrs + ">\u2715</button>");

  if(description) {

    lines.push("<div class=\"wizard-description\">" + escapeHtml(description) + "</div>");
  }

  lines.push("</div>");

  // Step indicator. Each step gets a numbered circle and label, connected by horizontal lines. The first step is marked active; the client-side controller
  // manages active/completed/clickable states during navigation.
  if(steps && (steps.length > 0)) {

    lines.push("<div class=\"wizard-steps\" id=\"" + escapeHtml(resolvedStepsId) + "\">");

    for(let i = 0; i < steps.length; i++) {

      const stepClass = (i === 0) ? "wizard-step active" : "wizard-step";

      lines.push("<div class=\"" + stepClass + "\" data-step=\"" + String(i + 1) + "\">" +
        "<span class=\"step-circle\">" + String(i + 1) + "</span>" +
        "<span class=\"step-label\">" + escapeHtml(steps[i]) + "</span></div>");

      // Connecting line between steps. Omitted after the last step.
      if(i < steps.length - 1) {

        lines.push("<div class=\"wizard-step-line\"></div>");
      }
    }

    lines.push("</div>");
  }

  // Content area. Empty for client-rendered wizards, pre-filled for simple dialogs. Dialog modals (no step indicator) use wizard-content-compact to avoid the
  // min-height that stepped wizards need for consistent layout.
  const contentClass = (steps && (steps.length > 0)) ? "wizard-content" : "wizard-content wizard-content-compact";

  lines.push("<div class=\"" + contentClass + "\" id=\"" + escapeHtml(resolvedContentId) + "\">" + (body ?? "") + "</div>");

  // Validation error area. Only rendered when errorId is provided.
  if(errorId) {

    lines.push("<div id=\"" + escapeHtml(errorId) + "\" class=\"wizard-error\" style=\"display: none;\"></div>");
  }

  // Footer buttons. Left-positioned buttons (e.g., Back) sit on the leading edge, right-positioned buttons (Cancel, Next, Save) sit on the trailing edge.
  lines.push("<div class=\"wizard-buttons\">");

  const leftButtons = buttons.filter((b) => (b.position === "left"));
  const rightButtons = buttons.filter((b) => (b.position === "right"));

  for(const btn of leftButtons) {

    lines.push(generateWizardButton(btn));
  }

  lines.push("<div class=\"wizard-buttons-right\">");

  for(const btn of rightButtons) {

    lines.push(generateWizardButton(btn));
  }

  lines.push("</div>");
  lines.push("</div>");

  // Close content box and modal root.
  lines.push("</div>");
  lines.push("</div>");

  // Embedded JSON data blocks sit outside the modal div so they are always accessible to client JavaScript regardless of modal visibility.
  if(dataBlocks) {

    for(const block of dataBlocks) {

      lines.push(block);
    }
  }

  return lines.join("\n");
}

/**
 * Generates a single footer button for a wizard modal. Internal helper used by generateWizardModal.
 * @param btn - Button configuration.
 * @returns HTML string for the button element.
 */
function generateWizardButton(btn: WizardModalButton): string {

  const variant = btn.variant ?? "secondary";
  const size = btn.size ?? "md";
  const visible = btn.visible ?? true;

  const classes = [ "btn", "btn-" + variant ];

  if(size === "sm") {

    classes.push("btn-sm");
  }

  const attrs: string[] = [
    "type=\"button\"",
    "class=\"" + classes.join(" ") + "\""
  ];

  if(btn.id) {

    attrs.push("id=\"" + escapeHtml(btn.id) + "\"");
  }

  // Role-tagged buttons are managed by the client-side wizard controller. The data attribute lets the controller discover them within the modal DOM.
  // Custom buttons without a role use an inline onclick handler pointing to a window-exposed function.
  if(btn.role) {

    attrs.push("data-wizard-role=\"" + escapeHtml(btn.role) + "\"");
  } else if(btn.onclick) {

    attrs.push("onclick=\"" + escapeHtml(btn.onclick) + "\"");
  }

  if(!visible) {

    attrs.push("style=\"display: none;\"");
  }

  return "<button " + attrs.join(" ") + ">" + escapeHtml(btn.label) + "</button>";
}
