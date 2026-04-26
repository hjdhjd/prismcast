/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * settings.ts: Settings UI and route handlers for the PrismCast configuration interface.
 */
import type { AdvancedSection, SettingMetadata, UserConfig } from "../../config/userConfig.js";
import { CONFIG, getDefaults, validatePositiveInt, validatePositiveNumber } from "../../config/index.js";
import { CONFIG_METADATA, getAdvancedSections, getEnvOverrides, getNestedValue, getSettingsTabSections, getUITabs, isEqualToDefault, mutateConfig, readConfig,
  setNestedValue } from "../../config/userConfig.js";
import type { Express, Request, Response } from "express";
import { LOG, escapeHtml, formatError, isRunningAsService, stringifySorted } from "../../utils/index.js";
import type { Nullable } from "../../types/index.js";
import { getConfigFilePath } from "../../config/paths.js";
import { getGpuCapabilities } from "../../browser/display.js";
import { getPresetOptionsWithDegradation } from "../../config/presets.js";
import { getProviderModuleInfo } from "../../browser/channelSelection.js";
import { scheduleServerRestart } from "./index.js";

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

    // Quality preset dropdown needs wide width because it displays dynamic degradation text like "1080p (limited to 720p High)" which is much longer than the
    // static validValues entries.
    if(setting.path === "streaming.qualityPreset") {

      return "field-wide";
    }

    const maxLength = Math.max(...setting.validValues.map((v) => v.length));

    // Short options (e.g., "none", "all", "errors") get narrow width.
    if(maxLength <= 8) {

      return "field-narrow";
    }

    // Medium options (e.g., "filtered") get medium width.
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

    // 1-4 digits get narrow (e.g., port, small counts, converted timeouts like "30" seconds).
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

/**
 * Generates HTML for a single setting form field. Supports text inputs, number inputs, and select dropdowns based on the setting type and validValues.
 * @param setting - The setting metadata.
 * @param currentValue - The current effective value (in storage units).
 * @param defaultValue - The default value (in storage units).
 * @param envOverride - The environment variable value if overridden, undefined otherwise.
 * @param validationError - Validation error message if any.
 * @returns HTML string for the form field.
 */
function generateSettingField(setting: SettingMetadata, currentValue: unknown, defaultValue: unknown, envOverride: string | undefined,
  validationError?: string): string {

  const isDisabled = (envOverride !== undefined) || (setting.disabledReason !== undefined);
  const inputId = setting.path.replaceAll(".", "-");
  const hasError = validationError !== undefined;
  const isModified = !isDisabled && !isEqualToDefault(currentValue, defaultValue);

  // Convert values for display.
  const displayValue = toDisplayValue(currentValue, setting);
  const displayDefault = toDisplayValue(defaultValue, setting);
  const displayUnit = getDisplayUnit(setting);
  const displayMin = getDisplayMin(setting);
  const displayMax = getDisplayMax(setting);

  // Determine if this should be a select dropdown.
  const hasValidValues = setting.validValues && (setting.validValues.length > 0);

  // Check if this setting depends on a boolean toggle that is currently disabled. The depends-disabled class applies a visual grey-out without actually
  // disabling the inputs, so values are still submitted during save.
  const dependsOnId = setting.dependsOn ? setting.dependsOn.replaceAll(".", "-") : undefined;
  const isDependencyDisabled = setting.dependsOn ? !getNestedValue(CONFIG, setting.dependsOn) : false;

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

  if(envOverride !== undefined) {

    lines.push("<span class=\"env-badge\">ENV</span>");
  }

  lines.push("</label>");

  // Track if the selected preset is degraded (used for inline message).
  let selectedPresetDegradedTo: Nullable<string> = null;

  // Block-level content that must appear outside the form-row flex container. Type branches that produce content too large for the inline flex layout (grids,
  // editors, lists) push their HTML here. Emitted after the description div.
  const postDescription: string[] = [];

  if(hasValidValues) {

    // Render as select dropdown.
    const selectAttrs = [
      "class=\"form-select " + getFieldWidthClass(setting) + (hasError ? " error" : "") + "\"",
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

    // Special handling for quality preset dropdown to show degradation info.
    if(setting.path === "streaming.qualityPreset") {

      const presetOptions = getPresetOptionsWithDegradation();

      for(const option of presetOptions.options) {

        const presetId = option.preset.id;
        const isSelected = presetId === currentValue;
        const selected = isSelected ? " selected" : "";

        // Build the display label with degradation annotation if applicable.
        let label = option.preset.name;

        if(option.degradedTo) {

          label = label + " (limited to " + option.degradedTo.name + ")";

          // Track if the selected preset is degraded.
          if(isSelected) {

            selectedPresetDegradedTo = option.degradedTo.name;
          }
        }

        lines.push("<option value=\"" + escapeHtml(presetId) + "\"" + selected + ">" + escapeHtml(label) + "</option>");
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
        " onchange=\"updateCheckboxList(this)\"> " + escapeHtml(item.label));

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
    // step (e.g., 500ms -> 0.5s step); otherwise step is 1 whole display unit. This gives meaningful arrow increments and constrains input to a sensible value grid
    // (e.g., 0.5, 1.0, 1.5, ... for half-second steps) rather than the old 1/displayDivisor approach which produced unusably small increments.
    let step = "1";

    if(setting.displayDivisor && (setting.min !== undefined)) {

      const displayMin = setting.min / setting.displayDivisor;

      step = (displayMin > 0) && (displayMin < 1) ? String(displayMin) : "1";
    } else if(setting.type === "float") {

      step = "0.01";
    }

    const inputAttrs = [
      "class=\"form-input " + getFieldWidthClass(setting) + (hasError ? " error" : "") + "\"",
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

    // Disable if overridden by env var.
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

    lines.push("<button type=\"button\" class=\"btn-reset\" onclick=\"resetSetting('" + escapeHtml(setting.path) +
      "')\" title=\"Reset to default\" aria-label=\"Reset to default\">&#8635;</button>");
  }

  lines.push("</div>");

  // Add description.
  lines.push("<div class=\"form-description\">" + escapeHtml(setting.description) + "</div>");

  // Emit block-level content that type branches deferred to outside the form-row flex container.
  for(const content of postDescription) {

    lines.push(content);
  }

  // Add disabled reason warning when a setting is locked out due to an upstream issue.
  if(setting.disabledReason) {

    lines.push("<div class=\"form-warning\">" + escapeHtml(setting.disabledReason) + "</div>");
  }

  // Add inline message for degraded preset.
  if(selectedPresetDegradedTo) {

    lines.push("<div class=\"form-warning\">Your display cannot support this resolution. Streams will use " +
      escapeHtml(selectedPresetDegradedTo) + " instead.</div>");
  }

  // Add default value hint with properly pluralized unit.
  let defaultDisplay: string;

  if(setting.type === "checkboxList") {

    // For checkbox lists, show the default items as a comma-separated list, or "none" for empty arrays.
    const defaultArr = Array.isArray(defaultValue) ? defaultValue as string[] : [];

    defaultDisplay = defaultArr.length > 0 ? defaultArr.join(", ") : "none";
  } else if(displayDefault === null) {

    defaultDisplay = "autodetect";
  } else if(typeof displayDefault === "number") {

    defaultDisplay = formatValueForDisplay(displayDefault, setting.type);
  } else {

    defaultDisplay = displayDefault;
  }

  // Format the unit with correct pluralization based on the default value.
  let formattedUnit = "";

  if(displayUnit && (typeof displayDefault === "number")) {

    formattedUnit = " " + formatUnitForValue(displayDefault, displayUnit);
  } else if(displayUnit) {

    formattedUnit = " " + displayUnit;
  }

  lines.push("<div class=\"form-default\">Default: " + escapeHtml(defaultDisplay) + formattedUnit + "</div>");

  // Add env var override notice if applicable.
  if(isDisabled && setting.envVar && envOverride) {

    lines.push("<div class=\"form-env\">Overridden by environment variable: <code>" + escapeHtml(setting.envVar) + "=" +
      escapeHtml(envOverride) + "</code></div>");
  }

  // Add validation error if present.
  if(hasError) {

    lines.push("<div class=\"form-error\">" + escapeHtml(validationError) + "</div>");
  }

  lines.push("</div>");

  return lines.join("\n");
}

/**
 * Validates a single setting value (in storage units, after conversion from display units).
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

      return setting.label + " must be one of: " + setting.validValues.join(", ");
    }

    return undefined;
  }

  // Validate based on type.
  switch(setting.type) {

    case "boolean": {

      // After parseFormValue, value should be a boolean. No additional validation needed since the dropdown constrains input.
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

      const numValue = Number(value);
      const error = validatePositiveInt(setting.label, numValue, setting.min, setting.max);

      return error ?? undefined;
    }

    case "float": {

      const numValue = Number(value);
      const error = validatePositiveNumber(setting.label, numValue, setting.min, setting.max);

      return error ?? undefined;
    }

    case "host": {

      if((typeof value !== "string") || (value.trim() === "")) {

        return setting.label + " must be a non-empty string";
      }

      return undefined;
    }

    case "path": {

      // Path can be any string or empty.
      return undefined;
    }

    case "string": {

      // String without validValues - no validation needed.
      return undefined;
    }

    default: {

      return undefined;
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

  // Handle empty values for path type.
  if((setting.type === "path") && (value.trim() === "")) {

    return null;
  }

  switch(setting.type) {

    case "boolean": {

      // Convert string "true" to boolean true, anything else to false.
      return value === "true";
    }

    case "checkboxList": {

      // The hidden input holds a JSON-encoded array of strings.
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

      return value;
    }

    default: {

      return value;
    }
  }
}

/**
 * Generates the content for the Settings tab with non-collapsible section headers.
 * @param validationErrors - Map of setting path to validation error message.
 * @param formValues - Map of setting path to submitted form value.
 * @returns HTML string for the Settings tab content.
 */
export function generateSettingsTabContent(validationErrors?: Map<string, string>, formValues?: Map<string, string>): string {

  const sections = getSettingsTabSections();
  const tabs = getUITabs();
  const settingsTab = tabs.find((t) => t.id === "settings");
  const defaults = getDefaults();
  const envOverrides = getEnvOverrides();
  const lines: string[] = [];

  // Panel header with description and reset button.
  lines.push("<div class=\"panel-header\">");
  lines.push("<p class=\"settings-panel-description\">" + escapeHtml(settingsTab?.description ?? "Configure common options.") + "</p>");
  lines.push("<a href=\"#\" class=\"panel-reset\" onclick=\"resetTabToDefaults('settings'); return false;\">Reset to Defaults</a>");
  lines.push("</div>");

  // Generate each section with a header.
  for(const section of sections) {

    lines.push("<div class=\"settings-section\">");
    lines.push("<div class=\"settings-section-header\">" + escapeHtml(section.displayName) + "</div>");

    // Generate setting fields for this section.
    for(const setting of section.settings) {

      const currentValue = formValues?.get(setting.path) ?? getNestedValue(CONFIG, setting.path);
      const defaultValue = getNestedValue(defaults, setting.path);
      const envOverride = envOverrides.get(setting.path);
      const validationError = validationErrors?.get(setting.path);

      lines.push(generateSettingField(setting, currentValue, defaultValue, envOverride, validationError));
    }

    lines.push("</div>");
  }

  return lines.join("\n");
}

/**
 * Generates the content for a collapsible section within the Advanced tab.
 * @param section - The section definition.
 * @param validationErrors - Map of setting path to validation error message.
 * @param formValues - Map of setting path to submitted form value.
 * @returns HTML string for the section.
 */
export function generateCollapsibleSection(section: AdvancedSection, validationErrors?: Map<string, string>,
  formValues?: Map<string, string>): string {

  const defaults = getDefaults();
  const envOverrides = getEnvOverrides();
  const lines: string[] = [];
  const settingCount = section.settings.length;

  // Section container.
  lines.push("<div class=\"advanced-section\" data-section=\"" + escapeHtml(section.id) + "\">");

  // Section header with chevron, title, and count.
  lines.push("<div class=\"section-header\" onclick=\"toggleSection('" + escapeHtml(section.id) + "')\">");
  lines.push("<span class=\"section-chevron\">&#9654;</span>");
  lines.push("<span class=\"section-title\">" + escapeHtml(section.displayName) + "</span>");
  lines.push("<span class=\"section-count\">(" + String(settingCount) + " setting" + (settingCount === 1 ? "" : "s") + ")</span>");
  lines.push("</div>");

  // Section content (collapsed by default).
  lines.push("<div class=\"section-content\">");

  // Generate setting fields for this section.
  for(const setting of section.settings) {

    const currentValue = formValues?.get(setting.path) ?? getNestedValue(CONFIG, setting.path);
    const defaultValue = getNestedValue(defaults, setting.path);
    const envOverride = envOverrides.get(setting.path);
    const validationError = validationErrors?.get(setting.path);

    lines.push(generateSettingField(setting, currentValue, defaultValue, envOverride, validationError));
  }

  // Close section-content, then advanced-section, in reverse order of the opens above.
  lines.push("</div>");
  lines.push("</div>");

  return lines.join("\n");
}

/**
 * Generates the content for the Advanced tab with collapsible sections.
 * @param validationErrors - Map of setting path to validation error message.
 * @param formValues - Map of setting path to submitted form value.
 * @returns HTML string for the Advanced tab content.
 */
export function generateAdvancedTabContent(validationErrors?: Map<string, string>, formValues?: Map<string, string>): string {

  const sections = getAdvancedSections();
  const tabs = getUITabs();
  const advancedTab = tabs.find((t) => t.id === "advanced");
  const lines: string[] = [];

  // Panel header with description and reset button.
  lines.push("<div class=\"panel-header\">");
  lines.push("<p class=\"settings-panel-description\">" + escapeHtml(advancedTab?.description ?? "Expert tuning options.") + "</p>");
  lines.push("<a href=\"#\" class=\"panel-reset\" onclick=\"resetTabToDefaults('advanced'); return false;\">Reset All to Defaults</a>");
  lines.push("</div>");

  // Generate each collapsible section.
  for(const section of sections) {

    lines.push(generateCollapsibleSection(section, validationErrors, formValues));
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
 * Checks if there are any environment variable overrides for configuration settings.
 * @returns True if any settings are overridden by environment variables.
 */
export function hasEnvOverrides(): boolean {

  return getEnvOverrides().size > 0;
}

/**
 * Shallow-merges a source config into a target config. Top-level object values are merged one level deep (preserving sibling keys within each category).
 * Non-object values are assigned directly. This is the single merge strategy used by both the settings form save and config import endpoints.
 * @param target - The existing config to merge into (modified in place).
 * @param source - The new values to merge.
 */
function mergeConfigValues(target: UserConfig, source: UserConfig): void {

  for(const [ path, value ] of Object.entries(source as Record<string, unknown>)) {

    if((typeof value === "object") && (value !== null) && !Array.isArray(value)) {

      (target as Record<string, unknown>)[path] ??= {};

      for(const [ subPath, subValue ] of Object.entries(value as Record<string, unknown>)) {

        ((target as Record<string, unknown>)[path] as Record<string, unknown>)[subPath] = subValue;
      }
    } else {

      (target as Record<string, unknown>)[path] = value;
    }
  }
}

/**
 * Installs all settings-related route handlers on the Express application.
 * @param app - The Express application.
 */
export function setupSettingsRoutes(app: Express): void {

  // POST /config - Save configuration and restart. Returns JSON response.
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

          // Parse the value (convert from display units to storage units if needed).
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

        res.status(400).json({ errors: validationErrors, success: false });

        return;
      }

      // Merge form values into the existing config via mutateConfig. The settings form only manages CONFIG_METADATA fields, but config.json also stores fields
      // managed by separate endpoints (disabledPredefined, enabledServices, visibleColumns, setupCompleted, channelSortField, channelSortDirection, dvrHost,
      // hdhr.deviceId, etc.). Merging form values into the existing config preserves all non-form fields automatically - no carry-forward list to maintain.
      await mutateConfig((existing) => {

        mergeConfigValues(existing, newConfig);
      });

      // Schedule restart after response is sent and return success response with restart info.
      const restartResult = scheduleServerRestart("to apply configuration changes");

      res.json({

        activeStreams: restartResult.activeStreams,
        deferred: restartResult.deferred,
        message: restartResult.message,
        success: true,
        willRestart: restartResult.willRestart
      });
    } catch(error) {

      LOG.error("Failed to save configuration: %s.", formatError(error));
      res.status(500).json({ message: "Failed to save configuration: " + formatError(error), success: false });
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

      LOG.error("Failed to export configuration: %s.", formatError(error));
      res.status(500).json({ error: "Failed to export configuration: " + formatError(error) });
    }
  });

  // POST /config/import - Import configuration from JSON.
  app.post("/config/import", async (req: Request, res: Response): Promise<void> => {

    try {

      // Cast to unknown first for runtime validation, then to UserConfig after validation.
      const rawConfig: unknown = req.body;

      // Basic validation - ensure it's an object.
      if((typeof rawConfig !== "object") || (rawConfig === null) || Array.isArray(rawConfig)) {

        res.status(400).json({ error: "Invalid configuration format: expected an object." });

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

          const pathParts = setting.path.split(".");
          let value: unknown = importedConfig;

          for(const part of pathParts) {

            if((value === null) || (value === undefined) || (typeof value !== "object")) {

              value = undefined;

              break;
            }

            value = (value as Record<string, unknown>)[part];
          }

          if(value === undefined) {

            continue;
          }

          // Validate the value.
          const error = validateSettingValue(setting, value);

          if(error) {

            validationErrors.push(setting.label + ": " + error);
          }
        }
      }

      if(validationErrors.length > 0) {

        res.status(400).json({ error: "Validation errors:\n" + validationErrors.join("\n") });

        return;
      }

      // Import replaces the user settings layer and preserves the system state layer. CONFIG_METADATA is the SSOT for which fields are user settings
      // (port, timeouts, quality preset, etc.) vs system state (dvrHost, deviceId, disabledPredefined, enabledServices, etc.). Clearing all
      // CONFIG_METADATA-tracked paths before merging ensures that settings not present in the import file revert to defaults rather than surviving
      // from the previous config. System state fields are untouched because they're not in CONFIG_METADATA.
      await mutateConfig((existing) => {

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

      // Schedule restart after response is sent and return success response with restart info.
      const restartResult = scheduleServerRestart("after configuration import");

      res.json({

        activeStreams: restartResult.activeStreams,
        deferred: restartResult.deferred,
        message: restartResult.message,
        success: true,
        willRestart: restartResult.willRestart
      });
    } catch(error) {

      LOG.error("Failed to import configuration: %s.", formatError(error));
      res.status(500).json({ error: "Failed to import configuration: " + formatError(error) });
    }
  });

  // POST /config/restart-now - Force immediate server restart regardless of active streams.
  app.post("/config/restart-now", (_req: Request, res: Response): void => {

    if(!isRunningAsService()) {

      res.status(400).json({ message: "Cannot restart: not running as a service.", success: false });

      return;
    }

    LOG.info("Forced restart requested via API.");

    res.json({ message: "Server is restarting...", success: true });

    // Close the browser first to avoid orphan Chrome processes.
    setTimeout(() => {

      LOG.info("Exiting for forced service manager restart.");

      void import("../../browser/index.js").then(async (mod) => mod.closeBrowser()).then(() => { process.exit(0); }).catch(() => { process.exit(1); });
    }, 500);
  });
}
