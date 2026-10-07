/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * settings.ts: Settings UI and route handlers for the PrismCast configuration interface.
 */
import type { AdvancedSection, SettingMetadata, UserConfig } from "../../config/userConfig.ts";
import type { ApplyConfigurationResult, RestartResult } from "./index.ts";
import type { ApplyResult, ChangeRejection, ConfigChange, ConfigChangePartition } from "../../config/reactivity.ts";
import { CONFIG_METADATA, getAdvancedSections, getEnvOverrides, getNestedValue, getSettingsTabSections, getUITabs, isEqualToDefault, readConfig,
  setNestedValue } from "../../config/userConfig.ts";
import { ConfigurationRejectedError, getConfigurationGap, getDefaults, getLoadedConfiguration, validateInteger, validateNumber } from "../../config/index.ts";
import type { Express, Request, Response } from "express";
import { LOG, escapeHtml, isRunningAsService, sanitizeString, serializeAttrs, stringifySorted } from "../../utils/index.ts";
import { PENDING_PATH_ATTRIBUTE, REACTIVITY_BADGES, formatSettingCount } from "./vocabulary.ts";
import { applyConfigurationChange, describeConfigurationOutcome } from "./index.ts";
import { sendErrorResponse, sendFormErrors, sendSuccess, sendValidationError } from "./http/envelope.ts";
import { ACTIONS } from "../clientActions.ts";
import type { Nullable } from "../../types/index.ts";
import { VIDEO_QUALITY_PRESETS } from "../../config/presets.ts";
import { generateBadge } from "../components.ts";
import { getConfigFilePath } from "../../config/paths.ts";
import { getGpuCapabilities } from "../../browser/display.ts";
import { getProviderModuleInfo } from "../../browser/channelSelection.ts";
import { systemClock } from "homebridge-plugin-utils";

/* The checkboxList setting type renders a grid of checkboxes backed by a hidden JSON array input. Each checkboxList field specifies a listItemsKey that identifies
 * which item provider to use. The registry maps keys to functions that return the list of items to render. Keeping the registry in the routes layer (not the config
 * layer) preserves the dependency direction - routes can import browser capabilities, config cannot.
 */

/**
 * A single item in a checkboxList setting. Each item renders as a labeled checkbox with optional per-item disabled state. The item provider must return ALL valid
 * values for the config field - both toggleable and fixed items - so that validation has the complete set.
 */
interface ListItem {

  // When true, the checkbox is rendered as disabled with its disabledReason shown.
  disabled?: boolean;

  // Explanation shown when the item is disabled.
  disabledReason?: string;

  // When true, the item is always present in the config array and cannot be toggled by the user. Rendered as a checked, disabled checkbox to communicate the
  // non-negotiable baseline. Fixed items are included in validation but excluded from user interaction.
  fixed?: boolean;

  // Human-readable label displayed next to the checkbox.
  label: string;

  // Value stored in the JSON array when the checkbox is checked.
  value: string;
}

// Registry of list item providers for checkboxList settings. Each key matches a listItemsKey value in CONFIG_METADATA.
const LIST_ITEM_PROVIDERS: Record<string, () => ListItem[]> = {

  captureCodecs: (): ListItem[] => {

    const gpuCaps = getGpuCapabilities();

    return [
      { fixed: true, label: "H.264 (always enabled)", value: "h264" },
      { disabled: !gpuCaps?.hevcHardwareEncoding, disabledReason: "Requires GPU with HEVC hardware encoding.", label: "HEVC", value: "hevc" }
    ];
  },

  providerModules: (): ListItem[] => getProviderModuleInfo().map((p) => ({ label: p.label, value: p.slug }))
};

/**
 * Formats a value for display, converting numbers to human-readable strings where appropriate.
 * @param value - The value to format.
 * @param settingType - Gates thousands-separator grouping; a value of "port" suppresses comma grouping so port numbers render without separators.
 * @returns Formatted string for display.
 */
function formatValueForDisplay(value: unknown, settingType?: string): string {

  if((value === null) || (value === undefined)) {

    return "";
  }

  if(typeof value === "number") {

    // Format large numbers with commas for readability, except for port numbers where commas would be confusing.
    if((value >= 1000) && (settingType !== "port")) {

      return value.toLocaleString();
    }

    return String(value);
  }

  if(typeof value === "string") {

    return value;
  }

  // Config values are always primitives (string, number, boolean). Numbers and strings are handled above.
  const primitive: boolean = value as boolean;

  return String(primitive);
}

/**
 * Converts a stored value to a display value using the setting's displayDivisor.
 * @param value - The stored value.
 * @param setting - The setting metadata.
 * @returns The display value.
 */
function toDisplayValue(value: unknown, setting: SettingMetadata): Nullable<number | string> {

  if((value === null) || (value === undefined)) {

    return null;
  }

  // Array values (e.g., checkboxList) are serialized as JSON for the hidden input's value attribute.
  if(Array.isArray(value)) {

    return JSON.stringify(value);
  }

  if((typeof value === "number") && setting.displayDivisor) {

    const displayValue = value / setting.displayDivisor;

    // Determine precision: explicit displayPrecision, or 2 for floats, or 1 for integers with displayDivisor (to handle values like 1500ms -> 1.5s).
    const precision = setting.displayPrecision ?? ((setting.type === "float") ? 2 : 1);

    return Number(displayValue.toFixed(precision));
  }

  // Boolean values pass through as strings for display.
  if(typeof value === "boolean") {

    return String(value);
  }

  return value as number | string;
}

/**
 * Gets the effective unit to display for a setting.
 * @param setting - The setting metadata.
 * @returns The unit string to display.
 */
function getDisplayUnit(setting: SettingMetadata): string | undefined {

  return setting.displayUnit ?? setting.unit;
}

/**
 * The setting types whose values are free text a user types or pastes, as opposed to a number, a boolean, or a structured list. Values of these types are
 * sanitized at every ingress, which is what the matching arm of parseFormValue does for the form save and what the import handler does for a JSON document.
 */
const TEXT_SETTING_TYPES = new Set<SettingMetadata["type"]>([ "host", "path", "string" ]);

/**
 * Mapping of units that require pluralization to their singular and plural forms. Abbreviations like "ms", "kbps", "fps" do not need pluralization and are not
 * included here. Uses Partial<Record> to indicate that not all string keys have values.
 */
const UNIT_PLURALIZATION: Partial<Record<string, { plural: string; singular: string }>> = {

  minutes: { plural: "minutes", singular: "minute" },
  seconds: { plural: "seconds", singular: "second" }
};

/**
 * Formats a unit string with correct pluralization based on the value. Returns singular form when value is 1, plural otherwise. Units not in the pluralization
 * mapping (abbreviations) pass through unchanged.
 * @param value - The numeric value to check for pluralization.
 * @param unit - The unit string to format.
 * @returns The correctly pluralized unit string.
 */
function formatUnitForValue(value: number, unit: string): string {

  const forms = UNIT_PLURALIZATION[unit];

  if(!forms) {

    return unit;
  }

  return (value === 1) ? forms.singular : forms.plural;
}

/**
 * Gets the effective min value for display (converted if displayDivisor is set).
 * @param setting - The setting metadata.
 * @returns The min value for the input field.
 */
function getDisplayMin(setting: SettingMetadata): number | undefined {

  if((setting.min === undefined) || !setting.displayDivisor) {

    return setting.min;
  }

  return setting.min / setting.displayDivisor;
}

/**
 * Gets the effective max value for display (converted if displayDivisor is set).
 * @param setting - The setting metadata.
 * @returns The max value for the input field.
 */
function getDisplayMax(setting: SettingMetadata): number | undefined {

  if((setting.max === undefined) || !setting.displayDivisor) {

    return setting.max;
  }

  return setting.max / setting.displayDivisor;
}

/**
 * Determines the appropriate width class for a form field (input or select) based on the setting type, constraints, and displayed value range. Width is proportional
 * to the actual displayed content rather than raw stored values, accounting for displayDivisor conversion.
 * @param setting - The setting metadata.
 * @returns CSS class name for field width (field-narrow, field-medium, or field-wide).
 */
function getFieldWidthClass(setting: SettingMetadata): string {

  // Ports always get narrow (max 5 digits: 65535).
  if(setting.type === "port") {

    return "field-narrow";
  }

  // For selects (settings with validValues), determine width based on content.
  if(setting.validValues && (setting.validValues.length > 0)) {

    const maxLength = Math.max(...setting.validValues.map((v) => v.length));

    // Short options (e.g., "none", "all", "errors") get narrow width.
    if(maxLength <= 8) {

      return "field-narrow";
    }

    // Medium-length options get medium width.
    if(maxLength <= 12) {

      return "field-medium";
    }

    // Long options get wide width.
    return "field-wide";
  }

  // For numeric types, calculate displayed digit count to determine width.
  if((setting.type === "integer") || (setting.type === "float")) {

    // Calculate the displayed max value, accounting for displayDivisor conversion.
    let displayMax = setting.max;

    if((displayMax !== undefined) && setting.displayDivisor) {

      displayMax = displayMax / setting.displayDivisor;
    }

    // If no max is defined, default to medium width as a safe middle ground.
    if(displayMax === undefined) {

      return "field-medium";
    }

    // Count digits needed for the displayed max value. For floats, add characters for decimal point and fractional digits.
    let digitCount = Math.max(1, Math.floor(Math.log10(Math.abs(displayMax))) + 1);

    if(setting.type === "float") {

      digitCount = digitCount + 3;
    }

    // 1-4 digits get narrow (e.g., small counts, converted timeouts like "30" seconds).
    if(digitCount <= 4) {

      return "field-narrow";
    }

    // 5-7 digits get medium (e.g., larger bitrates).
    if(digitCount <= 7) {

      return "field-medium";
    }

    // 8+ digits get wide.
    return "field-wide";
  }

  // Hosts and paths get wide width. Hosts can be IP addresses like "192.168.100.100" (15 chars) or hostnames.
  if((setting.type === "host") || (setting.type === "path")) {

    return "field-wide";
  }

  // Generic strings get wide width.
  return "field-wide";
}

export interface PendingSetting {

  // Why the running value differs from the saved one: a restart-class value waits for the restart, and a live or next-stream value a handler could not
  // realize waits for a later save to retry it.
  readonly kind: "restart" | "unrealized";

  // The dot-separated path of the setting.
  readonly path: string;

  // The marker sentence, naming the running value in the units the form displays. The server composes it once, so the page and a save response carry the
  // same words and the client places them as text.
  readonly text: string;
}

export interface SettingsFormContext {

  // The settings an environment variable owns, each with the value it supplies.
  readonly envOverrides: ReadonlyMap<string, string>;

  // The saved settings the running process has not taken, keyed by path.
  readonly pending: ReadonlyMap<string, PendingSetting>;
}

// One gap bucket's changes and the marker kind each of them carries.
interface PendingBucket {

  // The bucket's changes, in the gap's order.
  readonly changes: readonly ConfigChange[];

  // The marker kind of every change in the bucket.
  readonly kind: PendingSetting["kind"];
}

// The marker sentence of each kind, given the running value as the form displays it. Keyed by the kind union, so a kind added later cannot compile until
// it decides its sentence.
const PENDING_SENTENCES: Readonly<Record<PendingSetting["kind"], (running: string) => string>> = {

  restart: (running: string): string => REACTIVITY_BADGES.restart.title + " Running value: " + running + ".",
  unrealized: (running: string): string => "Saved, but not applied yet. Running value: " + running + ". The next save retries it."
};

// The outcome of one save by setting, mapped over ApplyResult's buckets, so a bucket added there cannot compile until the response builder fills it.
export type SettingsSaveChanges = Readonly<Record<Exclude<keyof ApplyResult, "rejected">, readonly string[]>> & {

  // The changes this save asked for that a handler refused, each with its reason.
  readonly rejected: readonly ChangeRejection[];
};

// The data a settings save or import answers with. An object type rather than an interface, because the envelope types data as a string-keyed record.
export type SettingsSaveData = Readonly<Pick<RestartResult, "activeStreams" | "deferred" | "willRestart">> & {

  // What this save's reconcile did: every value it realized, a retried one included, and this save's own restart-held and refused changes.
  readonly changes: SettingsSaveChanges;

  // Every saved setting the running server has not taken, read once the save resolved. The client replaces its markers from it.
  readonly pending: readonly PendingSetting[];
};

/**
 * Reads a setting's value for a person: the one reading the default line and the pending markers both use, so a value reads the same wherever the form names
 * it. A checkbox list reads as its members joined, or none when it has no member; a missing value reads as autodetect; a number reads in display units, grouped
 * unless the setting is a port, with its unit pluralized by the number; any other value reads as its display text with its unit appended as declared.
 * @param value - The value, in storage units.
 * @param setting - The setting metadata.
 * @returns The value's text with its display unit.
 */
function formatSettingValue(value: unknown, setting: SettingMetadata): string {

  const displayUnit = getDisplayUnit(setting);
  const displayValue = toDisplayValue(value, setting);
  let text: string;

  if(setting.type === "checkboxList") {

    const members = Array.isArray(value) ? value as string[] : [];

    text = (members.length > 0) ? members.join(", ") : "none";
  } else if(displayValue === null) {

    text = "autodetect";
  } else if(typeof displayValue === "number") {

    text = formatValueForDisplay(displayValue, setting.type);
  } else {

    text = displayValue;
  }

  if(!displayUnit) {

    return text;
  }

  return text + " " + ((typeof displayValue === "number") ? formatUnitForValue(displayValue, displayUnit) : displayUnit);
}

/**
 * Derives the pending view of a configuration gap: one entry for each saved setting the running process has not taken, keyed by path in the settings' declared
 * order, each carrying the marker sentence the form shows. The view is derived from the gap on every call and stored nowhere, which is why a save's response
 * carries all of it rather than a change against an earlier view.
 * @param gap - The gap to project, the live gap between the running configuration and the loaded snapshot by default.
 * @returns The pending settings, keyed by path.
 */
export function collectPendingSettings(gap: ConfigChangePartition = getConfigurationGap()): ReadonlyMap<string, PendingSetting> {

  const buckets: { readonly [Bucket in keyof ConfigChangePartition]: PendingBucket } = {

    held: { changes: gap.held, kind: "restart" },
    live: { changes: gap.live, kind: "unrealized" },
    nextStream: { changes: gap.nextStream, kind: "unrealized" }
  };
  const filed = new Map<string, { readonly change: ConfigChange; readonly kind: PendingSetting["kind"] }>();

  for(const bucket of Object.values(buckets)) {

    for(const change of bucket.changes) {

      filed.set(change.path, { change, kind: bucket.kind });
    }
  }

  // Walking the metadata settings rather than looking each gap path up means no lookup can miss, and the view comes out in the order the form renders.
  const pending = new Map<string, PendingSetting>();

  for(const setting of Object.values(CONFIG_METADATA).flat()) {

    const entry = filed.get(setting.path);

    if(!entry) {

      continue;
    }

    // The gap accessor diffs the running configuration against the loaded snapshot, so a change's previous value is the running one the marker names.
    const running = entry.change.previous;

    pending.set(setting.path, { kind: entry.kind, path: setting.path, text: PENDING_SENTENCES[entry.kind](formatSettingValue(running, setting)) });
  }

  return pending;
}

/**
 * What one settings field renders from.
 */
interface SettingFieldOptions {

  // The saved value the form shows, in storage units.
  readonly currentValue: unknown;

  // The default value, in storage units.
  readonly defaultValue: unknown;

  // The value an environment variable supplies when it owns the setting, undefined otherwise.
  readonly envOverride: string | undefined;

  // The setting's pending entry when the running process has not taken its saved value, undefined otherwise.
  readonly pending: PendingSetting | undefined;

  // The setting's metadata.
  readonly setting: SettingMetadata;
}

/**
 * Generates HTML for a single setting form field. Supports text inputs, number inputs, and select dropdowns based on the setting type and validValues.
 * @param options - The setting, the saved value the form shows, its default, its environment override, and its pending entry.
 * @returns HTML string for the form field.
 */
function generateSettingField(options: SettingFieldOptions): string {

  const { currentValue, defaultValue, envOverride, pending, setting } = options;

  const isDisabled = (envOverride !== undefined) || (setting.disabledReason !== undefined);
  const inputId = setting.path.replaceAll(".", "-");
  const isModified = !isDisabled && !isEqualToDefault(currentValue, defaultValue);

  // Convert values for display.
  const displayValue = toDisplayValue(currentValue, setting);
  const displayDefault = toDisplayValue(defaultValue, setting);
  const displayUnit = getDisplayUnit(setting);
  const displayMin = getDisplayMin(setting);
  const displayMax = getDisplayMax(setting);

  // Determine if this should be a select dropdown.
  const hasValidValues = setting.validValues && (setting.validValues.length > 0);

  // Check if this setting depends on a boolean toggle the saved configuration turns off. The depends-disabled class applies a visual grey-out without actually
  // disabling the inputs, so values are still submitted during save. The toggle is read from the saved configuration every field renders from, so the grey-out
  // agrees with the toggle the form shows.
  const dependsOnId = setting.dependsOn ? setting.dependsOn.replaceAll(".", "-") : undefined;
  const isDependencyDisabled = setting.dependsOn ? !getNestedValue(getLoadedConfiguration(), setting.dependsOn) : false;

  // Build CSS classes for the form group.
  const groupClasses = ["form-group"];

  if(isDisabled) {

    groupClasses.push("disabled");
  }

  if(isModified) {

    groupClasses.push("modified");
  }

  if(isDependencyDisabled) {

    groupClasses.push("depends-disabled");
  }

  // Build the opening div with optional data-depends-on attribute for client-side toggle behavior.
  const dependsAttr = dependsOnId ? " data-depends-on=\"" + dependsOnId + "\"" : "";

  const lines = [
    "<div class=\"" + groupClasses.join(" ") + "\"" + dependsAttr + ">",
    "<div class=\"form-row\">",
    "<label class=\"form-label\" for=\"" + inputId + "\">"
  ];

  // Add modified indicator before label text.
  if(isModified) {

    lines.push("<span class=\"modified-dot\" title=\"Modified from default\"></span>");
  }

  lines.push(escapeHtml(setting.label));

  // The class badge tells the user when a save of the setting takes effect, before the click. A class whose promise a save already makes carries none.
  const classBadge = REACTIVITY_BADGES[setting.reactivity];

  if(classBadge) {

    lines.push(generateBadge(classBadge.label, { title: classBadge.title, variant: classBadge.variant }));
  }

  if(envOverride !== undefined) {

    lines.push(generateBadge("ENV", { variant: "env" }));
  }

  lines.push("</label>");

  // Block-level content that must appear outside the form-row flex container. Type branches that produce content too large for the inline flex layout (grids,
  // editors, lists) push their HTML here. Emitted after the description div.
  const postDescription: string[] = [];

  if(hasValidValues) {

    // Render as select dropdown.
    const selectAttrs = [
      "class=\"form-select " + getFieldWidthClass(setting) + "\"",
      "id=\"" + inputId + "\"",
      "name=\"" + setting.path + "\"",
      "data-default=\"" + escapeHtml(String(displayDefault ?? "")) + "\""
    ];

    if(isDisabled) {

      selectAttrs.push("disabled");
    }

    if(isDependencyDisabled) {

      selectAttrs.push("tabindex=\"-1\"");
    }

    lines.push("<select " + selectAttrs.join(" ") + ">");

    /* The quality preset dropdown reads the preset table directly so each option carries the preset's friendly name rather than its id. Every preset is offered
     * unconditionally: capture renders at whichever one is chosen, because the surface is emulated rather than taken from the display.
     */
    if(setting.path === "streaming.qualityPreset") {

      for(const preset of VIDEO_QUALITY_PRESETS) {

        const selected = (preset.id === currentValue) ? " selected" : "";

        lines.push("<option value=\"" + escapeHtml(preset.id) + "\"" + selected + ">" + escapeHtml(preset.name) + "</option>");
      }
    } else {

      // Standard dropdown for non-preset fields.
      for(const validValue of setting.validValues ?? []) {

        // For boolean types, compare string validValue with stringified currentValue to handle boolean-to-string comparison.
        const isSelected = (setting.type === "boolean") ?
          (validValue === String(currentValue)) :
          (validValue === currentValue);
        const selected = isSelected ? " selected" : "";

        lines.push("<option value=\"" + escapeHtml(validValue) + "\"" + selected + ">" + escapeHtml(validValue) + "</option>");
      }
    }

    lines.push("</select>");
  } else if(setting.type === "boolean") {

    // Render boolean as a checkbox. A hidden input with value "false" precedes the checkbox so that unchecking submits "false" rather than omitting the field
    // entirely (which would cause the server to skip it and fall back to the default).
    const isChecked = (currentValue === true) || (currentValue === "true");
    const defaultStr = defaultValue ? "true" : "false";

    lines.push("<input type=\"hidden\" name=\"" + setting.path + "\" value=\"false\">");

    const checkboxAttrs = [
      "class=\"form-checkbox\"",
      "type=\"checkbox\"",
      "id=\"" + inputId + "\"",
      "name=\"" + setting.path + "\"",
      "value=\"true\"",
      "data-default=\"" + escapeHtml(defaultStr) + "\""
    ];

    if(isChecked) {

      checkboxAttrs.push("checked");
    }

    if(isDisabled) {

      checkboxAttrs.push("disabled");
    }

    if(isDependencyDisabled) {

      checkboxAttrs.push("tabindex=\"-1\"");
    }

    lines.push("<input " + checkboxAttrs.join(" ") + ">");
  } else if(setting.type === "checkboxList") {

    // Render as a grid of checkboxes backed by a hidden input that holds the JSON array value. The hidden input goes inside the form-row (invisible, takes no
    // space). The visible checkbox grid is pushed to postDescription for emission after the description, keeping it outside the form-row flex container. The
    // listItemsKey identifies which provider function in LIST_ITEM_PROVIDERS to call for the checkbox items.
    const currentArray = Array.isArray(currentValue) ? currentValue as string[] : [];
    const defaultArray = Array.isArray(defaultValue) ? defaultValue as string[] : [];
    const hiddenValue = escapeHtml(JSON.stringify(currentArray));
    const hiddenDefault = escapeHtml(JSON.stringify(defaultArray));

    lines.push("<input type=\"hidden\" id=\"" + inputId + "\" name=\"" + setting.path + "\" value=\"" + hiddenValue +
      "\" data-default=\"" + hiddenDefault + "\" data-checkbox-list>");

    // Look up the list item provider for this checkboxList setting. The registry is defined at the top of this file.
    const itemProvider = setting.listItemsKey ? LIST_ITEM_PROVIDERS[setting.listItemsKey] : undefined;
    const items = itemProvider ? itemProvider() : [];

    postDescription.push("<div class=\"checkbox-list-grid\" style=\"display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); " +
      "gap: 0.5rem; margin-top: 10px;\">");

    for(const item of items) {

      // Fixed items are always checked and disabled - they represent non-negotiable baseline values. Disabled items are conditionally unavailable (e.g., GPU
      // capability missing). Both render as disabled checkboxes but for different reasons.
      const isDisabled = (item.fixed === true) || (item.disabled === true);
      const checked = (item.fixed || currentArray.includes(item.value)) ? " checked" : "";
      const labelStyle = "display: flex; align-items: center; gap: 0.5rem;" + (isDisabled ? " opacity: 0.5; cursor: not-allowed;" : " cursor: pointer;");

      postDescription.push("<label style=\"" + labelStyle + "\">");
      postDescription.push("<input type=\"checkbox\" value=\"" + escapeHtml(item.value) + "\"" + checked + (isDisabled ? " disabled" : "") +
        " data-change-action=\"" + ACTIONS.updateCheckboxList + "\"> " + escapeHtml(item.label));

      if(item.disabled && item.disabledReason) {

        postDescription.push("<span style=\"font-size: 0.85em; opacity: 0.7;\"> - " + escapeHtml(item.disabledReason) + "</span>");
      }

      postDescription.push("</label>");
    }

    postDescription.push("</div>");
  } else {

    // Render as input field.
    const inputType = (setting.type === "float") ? "number" : (((setting.type === "integer") || (setting.type === "port")) ? "number" : "text");

    // Calculate step for arrow key increments. Auto-derived from min/displayDivisor: when the min in display units is between 0 and 1 (exclusive), use it as the
    // step (e.g., 500ms -> 0.5s step); otherwise step is 1 whole display unit. This gives meaningful arrow increments and constrains input to a sensible value
    // grid (e.g., 0.5, 1.0, 1.5, ... for half-second steps).
    let step = "1";

    if(setting.displayDivisor && (setting.min !== undefined)) {

      const displayMin = setting.min / setting.displayDivisor;

      step = (displayMin > 0) && (displayMin < 1) ? String(displayMin) : "1";
    } else if(setting.type === "float") {

      step = "0.01";
    }

    const inputAttrs = [
      "class=\"form-input " + getFieldWidthClass(setting) + "\"",
      "type=\"" + inputType + "\"",
      "id=\"" + inputId + "\"",
      "name=\"" + setting.path + "\"",
      "data-default=\"" + escapeHtml(String(displayDefault ?? "")) + "\""
    ];

    // Add value.
    if(displayValue !== null) {

      inputAttrs.push("value=\"" + escapeHtml(String(displayValue)) + "\"");
    }

    // Add step for numbers.
    if(inputType === "number") {

      inputAttrs.push("step=\"" + step + "\"");
    }

    // Add min/max if specified (using display values).
    if(displayMin !== undefined) {

      inputAttrs.push("min=\"" + String(displayMin) + "\"");
    }

    if(displayMax !== undefined) {

      inputAttrs.push("max=\"" + String(displayMax) + "\"");
    }

    // Disable when the value is overridden by an environment variable or locked out by a disabledReason.
    if(isDisabled) {

      inputAttrs.push("disabled");
    }

    if(isDependencyDisabled) {

      inputAttrs.push("tabindex=\"-1\"");
    }

    lines.push("<input " + inputAttrs.join(" ") + ">");
  }

  // Add unit label if present.
  if(displayUnit) {

    lines.push("<span class=\"form-unit\">" + escapeHtml(displayUnit) + "</span>");
  }

  // Add reset button for modified settings.
  if(isModified) {

    lines.push("<button type=\"button\" class=\"btn-reset\" data-click-action=\"" + ACTIONS.resetSetting + "\" data-setting-path=\"" + escapeHtml(setting.path) +
      "\" title=\"Reset to default\" aria-label=\"Reset to default\">&#8635;</button>");
  }

  lines.push("</div>");

  // Add description.
  lines.push("<div class=\"form-description\">" + escapeHtml(setting.description) + "</div>");

  // Emit block-level content that type branches deferred to outside the form-row flex container.
  for(const content of postDescription) {

    lines.push(content);
  }

  // Every field carries one marker slot, holding the server's sentence when the saved value is not the running one and hidden otherwise, so a save's response
  // fills or clears it as text without creating markup.
  lines.push("<div" + serializeAttrs({ "class": "form-pending" }) + serializeAttrs({ [PENDING_PATH_ATTRIBUTE]: setting.path, hidden: pending === undefined }) +
    ">" + escapeHtml(pending?.text ?? "") + "</div>");

  // Add disabled reason warning when a setting is locked out due to an upstream issue.
  if(setting.disabledReason) {

    lines.push("<div class=\"form-warning\">" + escapeHtml(setting.disabledReason) + "</div>");
  }

  // Add the default value hint, read as the pending markers read a value.
  lines.push("<div class=\"form-default\">Default: " + escapeHtml(formatSettingValue(defaultValue, setting)) + "</div>");

  // Add env var override notice if applicable.
  if(isDisabled && setting.envVar && envOverride) {

    lines.push("<div class=\"form-env\">Overridden by environment variable: <code>" + escapeHtml(setting.envVar) + "=" +
      escapeHtml(envOverride) + "</code></div>");
  }

  lines.push("</div>");

  return lines.join("\n");
}

/**
 * Validates a single setting value (in storage units, after conversion from display units).
 *
 * The validator is the shared gate for both value ingress paths - the settings form save and the whole-document JSON import - so it assumes nothing about
 * upstream coercion and checks the runtime type of every value it accepts. It rejects, it never repairs: the value that passes here is the value that is
 * persisted, so a document whose field carries the wrong type draws an error naming that field rather than landing on disk and taking effect at the next boot.
 *
 * @param setting - The setting metadata.
 * @param value - The value to validate (in storage units).
 * @returns Validation error message if invalid, undefined if valid.
 */
function validateSettingValue(setting: SettingMetadata, value: unknown): string | undefined {

  // Allow empty string for path type (means null/autodetect).
  if((setting.type === "path") && ((value === "") || (value === null))) {

    return undefined;
  }

  // Validate string type with validValues.
  if((setting.type === "string") && setting.validValues && (setting.validValues.length > 0)) {

    if(!setting.validValues.includes(value as string)) {

      return setting.label + " must be one of: " + setting.validValues.join(", ") + ".";
    }

    return undefined;
  }

  // Validate based on type.
  switch(setting.type) {

    case "boolean": {

      if(typeof value !== "boolean") {

        return setting.label + " must be true or false.";
      }

      return undefined;
    }

    case "checkboxList": {

      if(!Array.isArray(value)) {

        return "Must be an array.";
      }

      // Derive valid values from the list item provider registry. Each checkboxList validates against its own provider's items, not a hardcoded set.
      const itemProvider = setting.listItemsKey ? LIST_ITEM_PROVIDERS[setting.listItemsKey] : undefined;
      const validValues = itemProvider ? new Set(itemProvider().map((item) => item.value)) : new Set<string>();

      for(const entry of value as string[]) {

        if(!validValues.has(entry)) {

          return "Unrecognized value: " + entry + ".";
        }
      }

      return undefined;
    }

    case "integer":
    case "port": {

      if(typeof value !== "number") {

        return setting.label + " must be a number.";
      }

      const error = validateInteger(setting.label, value, setting.min, setting.max);

      return error ?? undefined;
    }

    case "float": {

      if(typeof value !== "number") {

        return setting.label + " must be a number.";
      }

      const error = validateNumber(setting.label, value, setting.min, setting.max);

      return error ?? undefined;
    }

    case "host": {

      if((typeof value !== "string") || (value.trim() === "")) {

        return setting.label + " must be a non-empty string.";
      }

      return undefined;
    }

    case "path": {

      // A path is any string. The empty and null forms mean autodetect and were already accepted by the guard at the top.
      if(typeof value !== "string") {

        return setting.label + " must be a string.";
      }

      return undefined;
    }

    case "string": {

      // A free-form string carries no value constraint beyond being one. Settings that restrict their values declare validValues and were handled above.
      if(typeof value !== "string") {

        return setting.label + " must be a string.";
      }

      return undefined;
    }

    default: {

      throw new Error("Unsupported setting type: " + String(setting.type) + ".");
    }
  }
}

/**
 * Parses a form value into the appropriate type for a setting, converting from display units to storage units if necessary.
 * @param setting - The setting metadata.
 * @param value - The raw string value from the form (in display units).
 * @returns The parsed value (in storage units).
 */
function parseFormValue(setting: SettingMetadata, value: string): Nullable<boolean | number | string | string[]> {

  /* Host, path, and free-string values pass through the shared data-collection sanitizer, which strips non-printable characters as well as padding at the point
   * submitted data enters the system - a pasted browser path carrying a trailing newline would otherwise be stored verbatim and break the launcher. The value
   * is computed once so the cleared-path check below and the text arm of the switch agree on what was submitted.
   */
  const sanitized = sanitizeString(value);

  // An empty path means autodetect - null is the sentinel the rest of the system reads as unset.
  if((setting.type === "path") && (sanitized === "")) {

    return null;
  }

  switch(setting.type) {

    case "boolean": {

      // Convert string "true" to boolean true, anything else to false.
      return value === "true";
    }

    case "checkboxList": {

      // The hidden input holds a JSON-encoded array of strings. A malformed payload throws here and is not caught
      // per-setting, so it surfaces as a 500 from the route's outer catch rather than a field-level validation error.
      return JSON.parse(value) as string[];
    }

    case "integer":
    case "port": {

      const displayValue = parseFloat(value);

      // Convert from display units to storage units if displayDivisor is set.
      if(setting.displayDivisor) {

        return Math.round(displayValue * setting.displayDivisor);
      }

      return parseInt(value, 10);
    }

    case "float": {

      const displayValue = parseFloat(value);

      // Convert from display units to storage units if displayDivisor is set.
      if(setting.displayDivisor) {

        return displayValue * setting.displayDivisor;
      }

      return displayValue;
    }

    case "host":
    case "path":
    case "string": {

      return sanitized;
    }

    default: {

      throw new Error("Unsupported setting type: " + String(setting.type) + ".");
    }
  }
}

/**
 * Generates the fields of a run of settings, the one per-field loop the Settings tab and every collapsible section share.
 * @param settings - The settings to render, in order.
 * @param context - The environment overrides and the pending view the page render resolved once.
 * @returns One HTML string per field.
 */
function generateSettingFields(settings: readonly SettingMetadata[], context: SettingsFormContext): string[] {

  const defaults = getDefaults();

  // The form shows the saved configuration, the loaded snapshot, rather than the running one. After a save that holds a change for a restart, a page load shows
  // that change, so submitting the page cannot post the running value back over it.
  const saved = getLoadedConfiguration();

  return settings.map((setting) => generateSettingField({

    currentValue: getNestedValue(saved, setting.path),
    defaultValue: getNestedValue(defaults, setting.path),
    envOverride: context.envOverrides.get(setting.path),
    pending: context.pending.get(setting.path),
    setting
  }));
}

/**
 * Generates the content for the Settings tab with non-collapsible section headers.
 * @param context - The environment overrides and the pending view the page render resolved once and shares with every section it draws.
 * @returns HTML string for the Settings tab content.
 */
export function generateSettingsTabContent(context: SettingsFormContext): string {

  const sections = getSettingsTabSections();
  const tabs = getUITabs();
  const settingsTab = tabs.find((t) => t.id === "settings");
  const lines: string[] = [];

  // Panel header with description and reset button.
  lines.push("<div class=\"panel-header\">");
  lines.push("<p class=\"settings-panel-description\">" + escapeHtml(settingsTab?.description ?? "Configure common options.") + "</p>");
  lines.push("<a href=\"#\" class=\"panel-reset\" data-click-action=\"" + ACTIONS.resetTabToDefaults +
    "\" data-tab=\"settings\" data-click-prevent-default>Reset to Defaults</a>");
  lines.push("</div>");

  // Generate each section with a header.
  for(const section of sections) {

    lines.push("<div class=\"settings-section\">");
    lines.push("<div class=\"settings-section-header\">" + escapeHtml(section.displayName) + "</div>");

    // Generate setting fields for this section.
    lines.push(...generateSettingFields(section.settings, context));
    lines.push("</div>");
  }

  return lines.join("\n");
}

/**
 * Generates the content for a collapsible section within the Advanced tab.
 * @param section - The section definition.
 * @param context - The environment overrides and the pending view the page render resolved once and shares with every section it draws.
 * @returns HTML string for the section.
 */
export function generateCollapsibleSection(section: AdvancedSection, context: SettingsFormContext): string {

  const lines: string[] = [];

  /* A section holding a pending setting is opened on the server, so a pending marker is never hidden behind a collapsed header, and nothing on the client has
   * to remember it. The client's stored section state only ever adds the expanded class, so a section opened here stays open on load.
   */
  const expanded = section.settings.some((setting) => context.pending.has(setting.path)) ? " expanded" : "";

  // Section container.
  lines.push("<div class=\"advanced-section\" data-section=\"" + escapeHtml(section.id) + "\">");

  // Section header with chevron, title, and count.
  lines.push("<div class=\"section-header" + expanded + "\" data-click-action=\"" + ACTIONS.toggleSection + "\" data-section-id=\"" + escapeHtml(section.id) +
    "\">");
  lines.push("<span class=\"section-chevron\">&#9654;</span>");
  lines.push("<span class=\"section-title\">" + escapeHtml(section.displayName) + "</span>");
  lines.push("<span class=\"section-count\">(" + formatSettingCount(section.settings.length) + ")</span>");
  lines.push("</div>");

  // Section content, collapsed unless the section holds a pending setting.
  lines.push("<div class=\"section-content" + expanded + "\">");

  // Generate setting fields for this section.
  lines.push(...generateSettingFields(section.settings, context));

  // Close section-content, then advanced-section, in reverse order of the opens above.
  lines.push("</div>");
  lines.push("</div>");

  return lines.join("\n");
}

/**
 * Generates the content for the Advanced tab with collapsible sections.
 * @param context - The environment overrides and the pending view the page render resolved once and shares with every section it draws.
 * @returns HTML string for the Advanced tab content.
 */
export function generateAdvancedTabContent(context: SettingsFormContext): string {

  const sections = getAdvancedSections();
  const tabs = getUITabs();
  const advancedTab = tabs.find((t) => t.id === "advanced");
  const lines: string[] = [];

  // Panel header with description and reset button.
  lines.push("<div class=\"panel-header\">");
  lines.push("<p class=\"settings-panel-description\">" + escapeHtml(advancedTab?.description ?? "Expert tuning options.") + "</p>");
  lines.push("<a href=\"#\" class=\"panel-reset\" data-click-action=\"" + ACTIONS.resetTabToDefaults + "\" data-tab=\"advanced\" " +
    "data-click-prevent-default>Reset All to Defaults</a>");
  lines.push("</div>");

  // Generate each collapsible section.
  for(const section of sections) {

    lines.push(generateCollapsibleSection(section, context));
  }

  return lines.join("\n");
}

/**
 * Generates the config path display for settings.
 * @returns HTML string with config path.
 */
export function generateSettingsFormFooter(): string {

  return "<div class=\"config-path\">Configuration file: <code>" + escapeHtml(getConfigFilePath()) + "</code></div>";
}

/**
 * The keys that name an object's prototype machinery rather than a setting. A document parsed from JSON carries any of them as an ordinary key, and assigning
 * through one reaches a prototype instead of the configuration, so the merge refuses each of them at every level it walks.
 */
const PROTOTYPE_KEYS = new Set<string>([ "__proto__", "constructor", "prototype" ]);

/**
 * Refuses a document key that would reach an object's prototype rather than name a setting. The merge runs inside the save's store mutation, so the refusal
 * writes nothing, and the route answers it as the validation error it is.
 * @param key - The key the merge is about to assign through.
 * @throws ConfigurationRejectedError when the key names prototype machinery.
 */
function refusePrototypeKey(key: string): void {

  if(PROTOTYPE_KEYS.has(key)) {

    throw new ConfigurationRejectedError("The configuration document carries the key " + key + ", which is not a setting.");
  }
}

/**
 * Shallow-merges a source config into a target config. Top-level object values are merged one level deep (preserving sibling keys within each category).
 * Non-object values are assigned directly. This is the single merge strategy the settings form save and the config import share. A key that names prototype
 * machinery, at any level the merge walks, is refused before anything is assigned through it.
 * @param target - The existing config to merge into (modified in place).
 * @param source - The new values to merge.
 * @throws ConfigurationRejectedError when the source carries a key that is not a setting.
 */
function mergeConfigValues(target: UserConfig, source: UserConfig): void {

  for(const [ path, value ] of Object.entries(source as Record<string, unknown>)) {

    refusePrototypeKey(path);

    if((typeof value === "object") && (value !== null) && !Array.isArray(value)) {

      (target as Record<string, unknown>)[path] ??= {};

      for(const [ subPath, subValue ] of Object.entries(value as Record<string, unknown>)) {

        refusePrototypeKey(subPath);

        ((target as Record<string, unknown>)[path] as Record<string, unknown>)[subPath] = subValue;
      }
    } else {

      (target as Record<string, unknown>)[path] = value;
    }
  }
}

/**
 * Builds the data envelope of a save response from the save's outcome. The settings save and the import share it, so each answers with the same fields: the
 * restart members the API reference documents, the outcome by setting under changes, and the pending view under pending. changes.applied and
 * changes.nextStream are everything this save's reconcile realized, a value an earlier save introduced and this save's retry realized included, while
 * changes.deferred and changes.rejected are this save's own. pending is the whole view after the save, read here once the save has resolved, and the client
 * replaces its markers from it rather than merging them. Exported so a unit row holds the mapping of every bucket, the next-stream bucket included, which a
 * route reaches only through a setting of that class.
 * @param outcome - The save's apply and restart result.
 * @returns The response data.
 */
export function buildSaveResponseData(outcome: ApplyConfigurationResult): SettingsSaveData {

  const paths = (changes: readonly ConfigChange[]): string[] => changes.map((change) => change.path);

  return {

    activeStreams: outcome.restart?.activeStreams ?? 0,
    changes: {

      applied: paths(outcome.apply.applied),
      deferred: paths(outcome.apply.deferred),
      nextStream: paths(outcome.apply.nextStream),
      rejected: outcome.apply.rejected.map(({ change, reason }) => ({ path: change.path, reason }))
    },
    deferred: outcome.restart?.deferred ?? false,
    pending: Array.from(collectPendingSettings().values()),
    willRestart: outcome.restart?.willRestart ?? false
  };
}

/**
 * Installs all settings-related route handlers on the Express application.
 * @param app - The Express application.
 */
export function setupSettingsRoutes(app: Express): void {

  // POST /config - Save configuration, applying changes live where possible and scheduling a restart only for settings
  // that require one. Returns JSON response.
  app.post("/config", async (req: Request, res: Response): Promise<void> => {

    try {

      const envOverrides = getEnvOverrides();
      const validationErrors: Record<string, string> = {};
      const newConfig: UserConfig = {};

      // Process each setting from the nested JSON structure.
      for(const settings of Object.values(CONFIG_METADATA)) {

        for(const setting of settings) {

          // Skip settings overridden by environment variables.
          if(envOverrides.has(setting.path)) {

            continue;
          }

          // Get the value from the nested JSON body using the setting path.
          const rawValue = getNestedValue(req.body as Record<string, unknown>, setting.path);

          // Skip undefined values (not submitted).
          if(rawValue === undefined) {

            continue;
          }

          // Parse the value (convert from display units to storage units if needed). CONFIG_METADATA-driven form
          // submissions are always primitive-valued at this path, so the cast below is a type-level acknowledgment
          // of that assumption rather than a runtime guarantee over the otherwise-unknown request body.
          const primitive: string | number | boolean = rawValue as string | number | boolean;
          const parsedValue = parseFormValue(setting, String(primitive));

          // Validate the value.
          const validationError = validateSettingValue(setting, parsedValue);

          if(validationError) {

            validationErrors[setting.path] = validationError;

            continue;
          }

          // Add to new config.
          setNestedValue(newConfig as Record<string, unknown>, setting.path, parsedValue);
        }
      }

      // If there are validation errors, return them as JSON.
      if(Object.keys(validationErrors).length > 0) {

        sendFormErrors(res, validationErrors);

        return;
      }

      // Merge form values into the existing config through the validated save. The settings form only manages CONFIG_METADATA fields, but config.json also
      // stores fields managed by separate endpoints (disabledPredefined, enabledServices, visibleColumns, setupCompleted, channelSortField,
      // channelSortDirection, channelsDvr.host, hdhr.deviceId, etc.). Merging form values into the existing config preserves all non-form fields
      // automatically - no carry-forward list to maintain. The save then reconciles the running configuration against the file: each change takes effect as
      // its reactivity class allows, and a restart is scheduled only when this save holds a restart-class change for one.
      const outcome = await applyConfigurationChange("to apply configuration changes", (existing) => {

        mergeConfigValues(existing, newConfig);
      });

      sendSuccess(res, { data: buildSaveResponseData(outcome), message: describeConfigurationOutcome(outcome) });
    } catch(error) {

      sendErrorResponse(res, error, "save configuration");
    }
  });

  // GET /config/export - Export current configuration as JSON.
  app.get("/config/export", async (_req: Request, res: Response): Promise<void> => {

    try {

      const result = await readConfig();

      res.setHeader("Content-Type", "application/json");
      res.setHeader("Content-Disposition", "attachment; filename=\"prismcast-config.json\"");
      res.send(stringifySorted(result.config) + "\n");
    } catch(error) {

      sendErrorResponse(res, error, "export configuration");
    }
  });

  // POST /config/import - Import configuration from JSON.
  app.post("/config/import", async (req: Request, res: Response): Promise<void> => {

    try {

      // Cast to unknown first for runtime validation, then to UserConfig after validation.
      const rawConfig: unknown = req.body;

      // Basic validation - ensure it's an object.
      if((typeof rawConfig !== "object") || (rawConfig === null) || Array.isArray(rawConfig)) {

        sendValidationError(res, "Invalid configuration format: expected an object.");

        return;
      }

      const importedConfig = rawConfig as UserConfig;

      // Validate each setting in the imported config.
      const validationErrors: string[] = [];

      for(const [ category, settings ] of Object.entries(CONFIG_METADATA)) {

        const categoryConfig = (importedConfig as Record<string, unknown>)[category];

        if(categoryConfig === undefined) {

          continue;
        }

        if((typeof categoryConfig !== "object") || (categoryConfig === null)) {

          validationErrors.push("Invalid " + category + " configuration: expected an object.");

          continue;
        }

        for(const setting of settings) {

          const value = getNestedValue(importedConfig, setting.path);

          if(value === undefined) {

            continue;
          }

          /* Text values are sanitized where the document enters the system, matching the convention the sibling JSON importers follow - parseServicePack
           * sanitizes the imported pack name, validateImportedChannels sanitizes imported channel fields - for the same reason: hand-assembled JSON carries
           * padding and the occasional null byte. Only values that are already strings are touched, so a mistyped value still reaches the validator unrepaired
           * and is rejected there.
           *
           * The sanitized value is written back into the document because the merge at the end of this handler reads the document. Validating a local copy
           * would validate one value and persist another, which is the divergence this validator exists to close.
           */
          const sanitized = (TEXT_SETTING_TYPES.has(setting.type) && (typeof value === "string")) ? sanitizeString(value) : value;

          if(sanitized !== value) {

            setNestedValue(importedConfig as Record<string, unknown>, setting.path, sanitized);
          }

          // Validate the value.
          const error = validateSettingValue(setting, sanitized);

          if(error) {

            validationErrors.push(setting.label + ": " + error);
          }
        }
      }

      if(validationErrors.length > 0) {

        sendValidationError(res, "Validation errors:\n" + validationErrors.join("\n"));

        return;
      }

      // Import replaces the user settings layer and preserves the system state layer. CONFIG_METADATA is the SSOT for which fields are user settings
      // (port, timeouts, quality preset, etc.) vs system state (channelsDvr.host, deviceId, disabledPredefined, enabledServices, etc.). Clearing all
      // CONFIG_METADATA-tracked paths before merging ensures that settings not present in the import file revert to defaults rather than surviving
      // from the previous config. System state fields are untouched because they're not in CONFIG_METADATA. The save validates the result before anything
      // reaches disk and reconciles the running configuration against it, the same path every write to the settings takes.
      const outcome = await applyConfigurationChange("after configuration import", (existing) => {

        // Clear all user settings from the existing config, leaving only system state.
        for(const settings of Object.values(CONFIG_METADATA)) {

          for(const setting of settings) {

            const [ categoryKey, fieldKey ] = setting.path.split(".");

            if(!categoryKey || !fieldKey) {

              continue;
            }

            const category = (existing as Record<string, unknown>)[categoryKey];

            if((typeof category === "object") && (category !== null)) {

              Reflect.deleteProperty(category, fieldKey);
            }
          }
        }

        // Apply imported settings on top of the preserved system state.
        mergeConfigValues(existing, importedConfig);
      });

      sendSuccess(res, { data: buildSaveResponseData(outcome), message: describeConfigurationOutcome(outcome) });
    } catch(error) {

      sendErrorResponse(res, error, "import configuration");
    }
  });

  // POST /config/restart-now - Force immediate server restart regardless of active streams.
  app.post("/config/restart-now", (_req: Request, res: Response): void => {

    if(!isRunningAsService()) {

      sendValidationError(res, "Cannot restart: not running as a service.");

      return;
    }

    LOG.info("Forced restart requested via API.");

    sendSuccess(res, { message: "Server is restarting..." });

    // Close the browser first to avoid orphan Chrome processes. The delay gives the success response above time to
    // reach the client before the process exits.
    systemClock.schedule(() => {

      LOG.info("Exiting for forced service manager restart.");

      void import("../../browser/index.js").then(async (mod) => mod.closeBrowser()).then(() => { process.exit(0); }).catch(() => { process.exit(1); });
    }, 500);
  });
}
