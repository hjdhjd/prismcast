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
 * Generates a code block with optional copy button.
 * @param content - The code content.
 * @param showCopyButton - Whether to show a copy button.
 * @param copyButtonId - ID for the copy button (required if showCopyButton is true).
 * @returns HTML string for the code block.
 */
export function generateCodeBlock(content: string, showCopyButton = false, copyButtonId?: string): string {

  if(showCopyButton && copyButtonId) {

    return [
      "<div class=\"code-block-wrapper\">",
      "<pre>" + escapeHtml(content) + "</pre>",
      "<button type=\"button\" class=\"btn btn-secondary btn-sm code-copy-btn\" id=\"" + escapeHtml(copyButtonId) +
        "\" onclick=\"navigator.clipboard.writeText(this.previousElementSibling.textContent)\">Copy</button>",
      "</div>"
    ].join("\n");
  }

  return "<pre>" + escapeHtml(content) + "</pre>";
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
