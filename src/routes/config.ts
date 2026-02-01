/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * config.ts: Configuration web interface for PrismCast.
 */
import type { AdvancedSection, SettingMetadata, UserConfig } from "../config/userConfig.js";
import { CONFIG, getDefaults, validatePositiveInt, validatePositiveNumber } from "../config/index.js";
import { CONFIG_METADATA, filterDefaults, getAdvancedSections, getConfigFilePath, getEnvOverrides, getNestedValue, getSettingsTabSections, getUITabs, isEqualToDefault,
  loadUserConfig, saveUserConfig, setNestedValue } from "../config/userConfig.js";
import type { Express, Request, Response } from "express";
import { LOG, escapeHtml, formatError, generateChannelKey, isRunningAsService, parseM3U } from "../utils/index.js";
import { getChannelsParseErrorMessage, getDisabledPredefinedChannels, getPredefinedChannels, getUserChannels, getUserChannelsFilePath, hasChannelsParseError,
  isPredefinedChannel, isPredefinedChannelDisabled, isUserChannel, loadUserChannels, saveUserChannels, validateChannelKey, validateChannelName, validateChannelProfile,
  validateChannelUrl, validateImportedChannels } from "../config/userChannels.js";
import type { Nullable } from "../types/index.js";
import { PREDEFINED_CHANNELS } from "../channels/index.js";
import type { ProfileInfo } from "../config/profiles.js";
import type { UserChannel } from "../config/userChannels.js";
import { closeBrowser } from "../browser/index.js";
import { getPresetOptionsWithDegradation } from "../config/presets.js";
import { getProfiles } from "../config/profiles.js";
import { getStreamCount } from "../streaming/registry.js";

/*
 * CONFIGURATION WEB INTERFACE
 *
 * The /config endpoint provides a user-friendly web interface for editing PrismCast settings. Users can adjust values, see defaults, and understand what each
 * setting does without editing JSON files directly. Changes require a server restart to take effect.
 *
 * The UI shows:
 * - Settings grouped by category (Server, Browser, Streaming, etc.)
 * - Current value, default value, and description for each setting
 * - Visual indicator for settings overridden by environment variables (disabled with explanation)
 * - Reset buttons for individual categories
 * - Save & Restart button that applies changes and restarts the server
 */

/**
 * Result of scheduling a server restart.
 */
interface RestartResult {

  // Number of active streams at the time of the restart request.
  activeStreams: number;

  // Whether the restart was deferred due to active streams.
  deferred: boolean;

  // The message to display to the user.
  message: string;

  // Whether the server will auto-restart (true if running as a service, false if manual restart required).
  willRestart: boolean;
}

/**
 * Schedules a server restart after a brief delay to allow the response to be sent. This is used after configuration changes that require a restart to take effect.
 * Returns information about whether the server will auto-restart (depends on whether running as a service). If streams are active and running as a service, the restart
 * is deferred until streams end, allowing the client to show a dialog and let the user choose to wait or force restart.
 * @param reason - A description of why the server is restarting, used in the log message.
 * @returns Information about the restart including the message to display and whether auto-restart will occur.
 */
function scheduleServerRestart(reason: string): RestartResult {

  const willRestart = isRunningAsService();

  // When not running as a service, we can't auto-restart. Notify the user that a manual restart is required.
  if(!willRestart) {

    LOG.info("Configuration saved %s. Manual restart required for changes to take effect.", reason);

    return {

      activeStreams: 0,
      deferred: false,
      message: "Configuration saved. Please restart PrismCast for changes to take effect.",
      willRestart: false
    };
  }

  // Check for active streams. If streams are active, defer the restart to avoid interrupting recordings or live viewing.
  const activeStreams = getStreamCount();

  if(activeStreams > 0) {

    LOG.info("Configuration saved %s. Restart deferred until %d active stream(s) end.", reason, activeStreams);

    return {

      activeStreams,
      deferred: true,
      message: "Configuration saved. " + String(activeStreams) + " stream(s) are active.",
      willRestart: true
    };
  }

  // No active streams - restart immediately. Close the browser first to avoid orphan Chrome processes.
  setTimeout(async () => {

    LOG.info("Exiting for service manager restart %s.", reason);

    await closeBrowser();

    process.exit(0);
  }, 500);

  return {

    activeStreams: 0,
    deferred: false,
    message: "Configuration saved. Server is restarting...",
    willRestart: true
  };
}

/*
 * CHANNEL FORM HELPERS
 *
 * These helper functions generate HTML for channel form fields. They are used by both the add and edit forms to reduce code duplication and ensure consistent
 * styling and behavior.
 */

/**
 * Options for generating a text input field.
 */
interface TextFieldOptions {

  // Hint text displayed below the input (optional).
  hint?: string;

  // Associates the input with a <datalist> for suggestions. When provided, a list attribute is added to the input and an empty <datalist> element is appended.
  list?: string;

  // HTML pattern attribute for validation (optional).
  pattern?: string;

  // Placeholder text (optional).
  placeholder?: string;

  // Whether the field is required.
  required?: boolean;

  // Input type (text, url, etc). Defaults to "text".
  type?: string;
}

/**
 * Generates HTML for a text input form field with label and optional hint.
 * @param id - The input element ID.
 * @param name - The input name attribute.
 * @param label - The label text.
 * @param value - The current value.
 * @param options - Additional options (hint, list, pattern, placeholder, required, type).
 * @returns Array of HTML strings for the form row.
 */
function generateTextField(id: string, name: string, label: string, value: string, options: TextFieldOptions = {}): string[] {

  const lines: string[] = [];
  const inputType = options.type ?? "text";
  const listAttr = options.list ? " list=\"" + options.list + "\"" : "";
  const required = options.required ? " required" : "";
  const pattern = options.pattern ? " pattern=\"" + options.pattern + "\"" : "";
  const placeholder = options.placeholder ? " placeholder=\"" + escapeHtml(options.placeholder) + "\"" : "";

  lines.push("<div class=\"form-row\">");
  lines.push("<label for=\"" + id + "\">" + label + "</label>");
  lines.push("<input class=\"form-input\" type=\"" + inputType + "\" id=\"" + id + "\" name=\"" + name + "\"" + required + listAttr + pattern +
    placeholder + " value=\"" + escapeHtml(value) + "\">");
  lines.push("</div>");

  // When a datalist ID is specified, append an empty <datalist> element outside the form-row flex container. The client-side JavaScript populates it dynamically
  // based on the URL field value.
  if(options.list) {

    lines.push("<datalist id=\"" + options.list + "\"></datalist>");
  }

  if(options.hint) {

    lines.push("<div class=\"hint\">" + options.hint + "</div>");
  }

  return lines;
}

/**
 * Generates HTML for the profile dropdown field with descriptions as tooltips and summaries inline.
 * @param id - The select element ID.
 * @param selectedProfile - The currently selected profile (empty string for autodetect).
 * @param profiles - List of available profiles with descriptions and summaries.
 * @param showHint - Whether to show the hint text with profile reference link.
 * @returns Array of HTML strings for the form row.
 */
function generateProfileDropdown(id: string, selectedProfile: string, profiles: ProfileInfo[], showHint = true): string[] {

  const lines: string[] = [];

  lines.push("<div class=\"form-row\">");
  lines.push("<label for=\"" + id + "\">Profile</label>");
  lines.push("<select class=\"form-select field-wide\" id=\"" + id + "\" name=\"profile\">");
  lines.push("<option value=\"\">Autodetect (Recommended)</option>");

  for(const profile of profiles) {

    const selected = (profile.name === selectedProfile) ? " selected" : "";
    const title = profile.description ? " title=\"" + escapeHtml(profile.description) + "\"" : "";

    // Show "name — summary" format for better discoverability.
    const displayText = profile.summary ? profile.name + " \u2014 " + profile.summary : profile.name;

    lines.push("<option value=\"" + escapeHtml(profile.name) + "\"" + title + selected + ">" + escapeHtml(displayText) + "</option>");
  }

  lines.push("</select>");
  lines.push("</div>");

  if(showHint) {

    lines.push("<div class=\"hint\">Autodetect uses predefined profiles for known sites. If video doesn't play or fullscreen fails, " +
      "try experimenting with different profiles. ");
    lines.push("<a href=\"#\" onclick=\"toggleProfileReference(); return false;\">View profile reference</a></div>");
  }

  return lines;
}

/**
 * Generates HTML for the profile reference section. This collapsible section provides detailed documentation for all available profiles, grouped by category to
 * help users understand which profile to select for their site.
 * @param profiles - List of available profiles with descriptions and summaries.
 * @returns HTML string for the profile reference section.
 */
function generateProfileReference(profiles: ProfileInfo[]): string {

  const lines: string[] = [];

  // Group profiles by category based on name prefix for automatic categorization.
  const keyboardProfiles = profiles.filter((p) => p.name.startsWith("keyboard"));
  const embeddedProfiles = profiles.filter((p) => p.name.startsWith("embedded"));
  const apiProfiles = profiles.filter((p) => p.name.startsWith("api") || (p.name === "fullscreenApi") || (p.name === "brightcove"));
  const specialProfiles = profiles.filter((p) => p.name === "staticPage");

  lines.push("<div id=\"profile-reference\" class=\"profile-reference\" style=\"display: none;\">");
  lines.push("<div class=\"profile-reference-header\">");
  lines.push("<h3>Profile Reference</h3>");
  lines.push("<a href=\"#\" class=\"profile-reference-close\" onclick=\"toggleProfileReference(); return false;\">\u2715</a>");
  lines.push("</div>");
  lines.push("<p class=\"reference-intro\">Profiles configure how PrismCast interacts with different video players. Autodetect uses predefined ");
  lines.push("profiles for known sites. If video doesn't play or fullscreen fails, use this reference to experiment with different profiles.</p>");

  // Keyboard fullscreen profiles.
  if(keyboardProfiles.length > 0) {

    lines.push("<div class=\"profile-category\">");
    lines.push("<h4>Keyboard Fullscreen Profiles</h4>");
    lines.push("<p class=\"category-desc\">For sites that use the 'f' key to toggle fullscreen mode.</p>");
    lines.push("<dl class=\"profile-list\">");

    for(const profile of keyboardProfiles) {

      lines.push("<dt>" + escapeHtml(profile.name) + "</dt>");
      lines.push("<dd>" + escapeHtml(profile.description) + "</dd>");
    }

    lines.push("</dl>");
    lines.push("</div>");
  }

  // Fullscreen API profiles.
  if((apiProfiles.length > 0) || (embeddedProfiles.length > 0)) {

    lines.push("<div class=\"profile-category\">");
    lines.push("<h4>Fullscreen API Profiles</h4>");
    lines.push("<p class=\"category-desc\">For sites that require JavaScript's requestFullscreen() API instead of keyboard shortcuts.</p>");
    lines.push("<dl class=\"profile-list\">");

    for(const profile of [ ...apiProfiles, ...embeddedProfiles ]) {

      lines.push("<dt>" + escapeHtml(profile.name) + "</dt>");
      lines.push("<dd>" + escapeHtml(profile.description) + "</dd>");
    }

    lines.push("</dl>");
    lines.push("</div>");
  }

  // Special profiles.
  if(specialProfiles.length > 0) {

    lines.push("<div class=\"profile-category\">");
    lines.push("<h4>Special Profiles</h4>");
    lines.push("<p class=\"category-desc\">For non-standard use cases like static pages without video.</p>");
    lines.push("<dl class=\"profile-list\">");

    for(const profile of specialProfiles) {

      lines.push("<dt>" + escapeHtml(profile.name) + "</dt>");
      lines.push("<dd>" + escapeHtml(profile.description) + "</dd>");
    }

    lines.push("</dl>");
    lines.push("</div>");
  }

  lines.push("</div>");

  return lines.join("\n");
}

/**
 * Generates HTML for the advanced fields section (station ID, channel selector, and channel number).
 * @param idPrefix - Prefix for element IDs ("add" or "edit").
 * @param stationIdValue - Current station ID value.
 * @param channelSelectorValue - Current channel selector value.
 * @param channelNumberValue - Current channel number value.
 * @param showHints - Whether to show hint text.
 * @returns Array of HTML strings for the advanced fields section.
 */
function generateAdvancedFields(idPrefix: string, stationIdValue: string, channelSelectorValue: string, channelNumberValue: string, showHints = true): string[] {

  const lines: string[] = [];

  // Advanced fields toggle.
  lines.push("<div class=\"advanced-toggle\" onclick=\"document.getElementById('" + idPrefix +
    "-advanced').classList.toggle('show'); this.textContent = this.textContent === 'Show Advanced Options' ? " +
    "'Hide Advanced Options' : 'Show Advanced Options';\">Show Advanced Options</div>");

  lines.push("<div id=\"" + idPrefix + "-advanced\" class=\"advanced-fields\">");

  // Station ID.
  const stationIdHint = showHints ? "Optional Gracenote station ID for guide data (tvc-guide-stationid)." : undefined;

  lines.push(...generateTextField(
    idPrefix + "-stationId",
    "stationId",
    "Station ID",
    stationIdValue,
    { hint: stationIdHint, placeholder: showHints ? "e.g., 12345" : undefined }
  ));

  // Channel selector.
  const channelSelectorHint = showHints ?
    "Identifies which channel to select on sites that host multiple live streams. Known values are suggested when the URL matches a supported site. " +
    "For other sites, right-click a channel image → Inspect → copy a unique portion of the image src URL." :
    undefined;

  lines.push(...generateTextField(
    idPrefix + "-channelSelector",
    "channelSelector",
    "Channel Selector",
    channelSelectorValue,
    { hint: channelSelectorHint, list: idPrefix + "-selectorList", placeholder: showHints ? "e.g., ESPN" : undefined }
  ));

  // Channel number for HDHomeRun/Plex integration.
  const channelNumberHint = showHints ?
    "Optional numeric channel number for Plex guide matching when HDHomeRun emulation is enabled. Leave empty for auto-assignment." :
    undefined;

  lines.push(...generateTextField(
    idPrefix + "-channelNumber",
    "channelNumber",
    "Channel Number",
    channelNumberValue,
    { hint: channelNumberHint, placeholder: showHints ? "e.g., 501" : undefined }
  ));

  lines.push("</div>"); // End advanced fields.

  return lines;
}

/**
 * Generates a JavaScript object literal mapping URL hostnames to known channel selector values from predefined channels. This data is embedded as a `<script>` block
 * in the channels panel so the client-side datalist can offer suggestions based on the URL the user enters.
 *
 * @returns A JavaScript variable declaration string ready to embed in a `<script>` tag.
 */
function generateChannelSelectorData(): string {

  const byDomain: Record<string, Array<{ label: string; value: string }>> = {};

  for(const channel of Object.values(PREDEFINED_CHANNELS)) {

    if(!channel.channelSelector) {

      continue;
    }

    const hostname = new URL(channel.url).hostname;

    byDomain[hostname] ??= [];
    byDomain[hostname].push({ label: channel.name, value: channel.channelSelector });
  }

  // Sort entries within each domain alphabetically by label for consistent ordering in the datalist dropdown.
  for(const entries of Object.values(byDomain)) {

    entries.sort((a, b) => a.label.localeCompare(b.label));
  }

  return "var channelSelectorsByDomain = " + JSON.stringify(byDomain) + ";";
}

/**
 * Result from generating channel row HTML.
 */
export interface ChannelRowHtml {

  // The display row HTML (always present).
  displayRow: string;

  // The edit form row HTML (only present for user channels).
  editRow: Nullable<string>;
}

/**
 * Generates the HTML for a single channel's table rows (display row and optional edit form row).
 * @param key - The channel key.
 * @param profiles - List of available profiles with descriptions for the dropdown.
 * @returns Object with displayRow and editRow HTML strings.
 */
export function generateChannelRowHtml(key: string, profiles: ProfileInfo[]): ChannelRowHtml {

  // Look up channel from user channels first (they override predefined), then predefined channels.
  const userChannels = getUserChannels();
  const predefinedChannels = getPredefinedChannels();
  const channel = userChannels[key] ?? predefinedChannels[key];

  // If channel doesn't exist, return empty rows (shouldn't happen in normal use).
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  if(!channel) {

    return { displayRow: "", editRow: null };
  }

  const isUser = isUserChannel(key);
  const isPredefined = isPredefinedChannel(key);
  const isDisabled = isPredefinedChannelDisabled(key);

  // Generate display row. User channels get one CSS class, disabled predefined get another.
  const displayLines: string[] = [];
  const rowClasses: string[] = [];

  if(isUser) {

    rowClasses.push("user-channel");
  }

  if(isDisabled) {

    rowClasses.push("channel-disabled");
  }

  const rowClassAttr = (rowClasses.length > 0) ? " class=\"" + rowClasses.join(" ") + "\"" : "";

  displayLines.push("<tr id=\"display-row-" + escapeHtml(key) + "\"" + rowClassAttr + ">");
  displayLines.push("<td><code>" + escapeHtml(key) + "</code></td>");
  displayLines.push("<td>" + escapeHtml(channel.name) + "</td>");
  displayLines.push("<td class=\"channel-url\" title=\"" + escapeHtml(channel.url) + "\">" + escapeHtml(channel.url) + "</td>");
  displayLines.push("<td>" + (channel.profile ? escapeHtml(channel.profile) : "<em>auto</em>") + "</td>");

  // Actions column.
  displayLines.push("<td>");
  displayLines.push("<div class=\"btn-group\">");

  // Login button appears for enabled channels only.
  if(!isDisabled) {

    displayLines.push("<button type=\"button\" class=\"btn btn-secondary btn-sm\" onclick=\"startChannelLogin('" + escapeHtml(key) + "')\">Login</button>");
  }

  if(isUser) {

    // User channels: Edit/Delete buttons.
    displayLines.push("<button type=\"button\" class=\"btn btn-edit btn-sm\" onclick=\"showEditForm('" + escapeHtml(key) + "')\">Edit</button>");
    displayLines.push("<button type=\"button\" class=\"btn btn-delete btn-sm\" onclick=\"deleteChannel('" + escapeHtml(key) + "')\">Delete</button>");
  } else if(isPredefined) {

    // Predefined channels: Enable/Disable button.
    if(isDisabled) {

      displayLines.push("<button type=\"button\" class=\"btn btn-enable btn-sm\" onclick=\"togglePredefinedChannel('" + escapeHtml(key) +
        "', true)\">Enable</button>");
    } else {

      displayLines.push("<button type=\"button\" class=\"btn btn-disable btn-sm\" onclick=\"togglePredefinedChannel('" + escapeHtml(key) +
        "', false)\">Disable</button>");
    }
  }

  displayLines.push("</div>");
  displayLines.push("</td>");
  displayLines.push("</tr>");

  const displayRow = displayLines.join("\n");

  // Generate edit form row for user channels.
  let editRow: string | null = null;

  if(isUser) {

    const editLines: string[] = [];

    editLines.push("<tr id=\"edit-row-" + escapeHtml(key) + "\" style=\"display: none;\">");
    editLines.push("<td colspan=\"5\">");
    editLines.push("<div class=\"channel-form\" style=\"margin: 0;\">");
    editLines.push("<h3>Edit Channel: " + escapeHtml(key) + "</h3>");
    editLines.push("<form id=\"edit-channel-form-" + escapeHtml(key) + "\" onsubmit=\"return submitChannelForm(event, 'edit')\">");
    editLines.push("<input type=\"hidden\" name=\"action\" value=\"edit\">");
    editLines.push("<input type=\"hidden\" name=\"key\" value=\"" + escapeHtml(key) + "\">");

    // Channel name.
    editLines.push(...generateTextField("edit-name-" + key, "name", "Display Name", channel.name, {

      hint: "Friendly name shown in the playlist and UI.",
      required: true
    }));

    // Channel URL.
    editLines.push(...generateTextField("edit-url-" + key, "url", "Stream URL", channel.url, {

      hint: "The URL of the streaming page to capture.",
      required: true,
      type: "url"
    }));

    // Profile dropdown.
    editLines.push(...generateProfileDropdown("edit-profile-" + key, channel.profile ?? "", profiles));

    // Advanced fields.
    editLines.push(...generateAdvancedFields("edit-" + key, channel.stationId ?? "", channel.channelSelector ?? "",
      channel.channelNumber ? String(channel.channelNumber) : ""));

    // Form buttons.
    editLines.push("<div class=\"form-buttons\">");
    editLines.push("<button type=\"submit\" class=\"btn btn-primary\">Save Changes</button>");
    editLines.push("<button type=\"button\" class=\"btn btn-secondary\" onclick=\"hideEditForm('" + escapeHtml(key) + "')\">Cancel</button>");
    editLines.push("</div>");

    editLines.push("</form>");
    editLines.push("</div>");
    editLines.push("</td>");
    editLines.push("</tr>");

    editRow = editLines.join("\n");
  }

  return { displayRow, editRow };
}

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

  return String(value);
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

  if((typeof value === "number") && setting.displayDivisor) {

    const displayValue = value / setting.displayDivisor;

    // Determine precision: explicit displayPrecision, or 2 for floats, or 1 for integers with displayDivisor (to handle values like 1500ms → 1.5s).
    const precision = setting.displayPrecision ?? ((setting.type === "float") ? 2 : 1);

    return Number(displayValue.toFixed(precision));
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

  const isDisabled = envOverride !== undefined;
  const inputId = setting.path.replace(/\./g, "-");
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
  const dependsOnId = setting.dependsOn ? setting.dependsOn.replace(/\./g, "-") : undefined;
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

  if(isDisabled) {

    lines.push("<span class=\"env-badge\">ENV</span>");
  }

  lines.push("</label>");

  // Track if the selected preset is degraded (used for inline message).
  let selectedPresetDegradedTo: Nullable<string> = null;

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
      for(const validValue of setting.validValues as string[]) {

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
    const defaultStr = String(defaultValue ?? false);

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
  } else {

    // Render as input field.
    const inputType = (setting.type === "float") ? "number" : (((setting.type === "integer") || (setting.type === "port")) ? "number" : "text");

    // Calculate step based on type and displayDivisor. When displayDivisor is set, step must match the storage granularity to ensure HTML5 validation passes
    // (the check is: (value - min) % step === 0). For example, ms→seconds with divisor 1000 needs step 0.001 so any millisecond value is valid.
    let step = "1";

    if(setting.displayDivisor) {

      step = String(1 / setting.displayDivisor);
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
      "')\" title=\"Reset to default\">&#8635;</button>");
  }

  lines.push("</div>");

  // Add description.
  lines.push("<div class=\"form-description\">" + escapeHtml(setting.description) + "</div>");

  // Add inline message for degraded preset.
  if(selectedPresetDegradedTo) {

    lines.push("<div class=\"form-warning\">Your display cannot support this resolution. Streams will use " +
      escapeHtml(selectedPresetDegradedTo) + " instead.</div>");
  }

  // Add default value hint with properly pluralized unit.
  let defaultDisplay: string;

  if(displayDefault === null) {

    defaultDisplay = "autodetect";
  } else if(typeof displayDefault === "number") {

    defaultDisplay = formatValueForDisplay(displayDefault, setting.type);
  } else {

    defaultDisplay = String(displayDefault);
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
function parseFormValue(setting: SettingMetadata, value: string): Nullable<boolean | number | string> {

  // Handle empty values for path type.
  if((setting.type === "path") && (value.trim() === "")) {

    return null;
  }

  switch(setting.type) {

    case "boolean": {

      // Convert string "true" to boolean true, anything else to false.
      return value === "true";
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
 * Generates the Channels panel HTML content.
 * @param channelMessage - Optional message to display (success or error).
 * @param channelError - If true, display as error; otherwise as success.
 * @param editingChannelKey - If set, show the edit form for this channel.
 * @param showAddForm - If true, show the add channel form.
 * @param formErrors - Validation errors for the channel form.
 * @param formValues - Form values to re-populate after validation error.
 * @returns HTML string for the Channels panel content.
 */
export function generateChannelsPanel(channelMessage?: string, channelError?: boolean, editingChannelKey?: string, showAddForm?: boolean,
  formErrors?: Map<string, string>, formValues?: Map<string, string>): string {

  // Get all channels including disabled predefined ones. User channels override predefined.
  const userChannels = getUserChannels();
  const predefinedChannels = getPredefinedChannels();
  const allChannelKeys = new Set([ ...Object.keys(predefinedChannels), ...Object.keys(userChannels) ]);
  const channelKeys = [...allChannelKeys].sort();
  const profiles = getProfiles();
  const disabledPredefined = getDisabledPredefinedChannels();
  const predefinedCount = Object.keys(predefinedChannels).length;
  const allDisabled = disabledPredefined.length === predefinedCount;

  // Count predefined channels that are actually disabled in the DOM — excludes those overridden by user channels, since overridden channels render as user channel
  // rows rather than disabled predefined rows.
  const visibleDisabledCount = disabledPredefined.filter((key) => !isUserChannel(key)).length;

  const lines: string[] = [];

  // Panel description.
  lines.push("<div class=\"settings-panel-description\">");
  lines.push("<p>Define and manage streaming channels for the playlist. Your custom channels are highlighted.</p>");
  lines.push("<p class=\"description-hint\">Tip: To override a predefined channel, add a custom channel with the same key.</p>");
  lines.push("</div>");

  // Toolbar with channel operations and display controls.
  lines.push("<div class=\"channel-toolbar\">");

  // Left group: channel operations. Import uses a dropdown menu to consolidate M3U and JSON import into a single button.
  lines.push("<div class=\"toolbar-group\">");
  lines.push("<button type=\"button\" class=\"btn btn-primary btn-sm\" id=\"add-channel-btn\" onclick=\"document.getElementById('add-channel-form')",
    ".style.display='block'; this.style.display='none';\">Add Channel</button>");
  lines.push("<div class=\"dropdown\">");
  lines.push("<button type=\"button\" class=\"btn btn-secondary btn-sm\" onclick=\"toggleDropdown(this)\">Import &#9662;</button>");
  lines.push("<div class=\"dropdown-menu\">");
  lines.push("<div class=\"dropdown-item\" onclick=\"closeDropdowns(); document.getElementById('import-channels-file').click()\">Channels (JSON)</div>");
  lines.push("<div class=\"dropdown-divider\"></div>");
  lines.push("<div class=\"dropdown-item\" onclick=\"closeDropdowns(); document.getElementById('import-m3u-file').click()\">M3U Playlist</div>");
  lines.push("<label class=\"dropdown-option\"><input type=\"checkbox\" id=\"m3u-replace-duplicates\"> Replace duplicates</label>");
  lines.push("</div>");
  lines.push("</div>");
  lines.push("<button type=\"button\" class=\"btn btn-secondary btn-sm\" onclick=\"exportChannels()\">Export</button>");
  lines.push("<input type=\"file\" id=\"import-m3u-file\" accept=\".m3u,.m3u8\" style=\"display: none;\" onchange=\"importM3U(this)\">");
  lines.push("</div>");

  // Spacer.
  lines.push("<div class=\"toolbar-spacer\"></div>");

  // Right group: display controls.
  lines.push("<div class=\"toolbar-group\">");

  if(allDisabled) {

    lines.push("<button type=\"button\" class=\"btn btn-secondary btn-sm\" id=\"bulk-toggle-btn\" ",
      "onclick=\"toggleAllPredefined(true)\">Enable All Predefined</button>");
  } else {

    lines.push("<button type=\"button\" class=\"btn btn-secondary btn-sm\" id=\"bulk-toggle-btn\" ",
      "onclick=\"toggleAllPredefined(false)\">Disable All Predefined</button>");
  }

  lines.push("<label class=\"toggle-label\"><input type=\"checkbox\" id=\"show-disabled-toggle\" onchange=\"toggleDisabledVisibility()\"> ",
    "Show disabled (<span id=\"disabled-count\">" + String(visibleDisabledCount) + "</span>)</label>");
  lines.push("</div>");
  lines.push("</div>");

  // Show channels file parse error if applicable.
  if(hasChannelsParseError()) {

    lines.push("<div class=\"error\">");
    lines.push("<div class=\"error-title\">Channels File Error</div>");
    lines.push("The channels file at <code>" + escapeHtml(getUserChannelsFilePath()) + "</code> contains invalid JSON and could not be loaded. ");
    lines.push("User channels are disabled. Fix the file manually or add a new channel to create a valid file.");

    const parseError = getChannelsParseErrorMessage();

    if(parseError) {

      lines.push("<br><br>Error: <code>" + escapeHtml(parseError) + "</code>");
    }

    lines.push("</div>");
  }

  // Show channel message if present.
  if(channelMessage) {

    const messageClass = channelError ? "error" : "success";
    const titleClass = channelError ? "error-title" : "success-title";
    const title = channelError ? "Error" : "Success";

    lines.push("<div class=\"" + messageClass + "\">");
    lines.push("<div class=\"" + titleClass + "\">" + title + "</div>");
    lines.push(escapeHtml(channelMessage));
    lines.push("</div>");
  }

  // Show validation errors if present.
  if(formErrors && (formErrors.size > 0)) {

    lines.push("<div class=\"error\">");
    lines.push("<div class=\"error-title\">Validation Errors</div>");
    lines.push("Please correct the following errors:");
    lines.push("<ul>");

    for(const [ field, error ] of formErrors) {

      lines.push("<li><strong>" + escapeHtml(field) + "</strong>: " + escapeHtml(error) + "</li>");
    }

    lines.push("</ul>");
    lines.push("</div>");
  }

  // Add channel form (hidden by default unless showAddForm is true or there are form errors for a new channel).
  const addFormVisible = (showAddForm === true) || (formErrors && formErrors.has("key") && !editingChannelKey);

  lines.push("<div id=\"add-channel-form\" class=\"channel-form\" style=\"display: " + (addFormVisible ? "block" : "none") + ";\">");
  lines.push("<h3>Add New Channel</h3>");
  lines.push("<form id=\"add-channel-form-el\" onsubmit=\"return submitChannelForm(event, 'add')\">");
  lines.push("<input type=\"hidden\" name=\"action\" value=\"add\">");

  // Channel key (add form only).
  lines.push(...generateTextField("add-key", "key", "Channel Key", formValues?.get("key") ?? "", {

    hint: "Lowercase letters, numbers, and hyphens only. Used in the URL: /stream/channel-key",
    pattern: "[a-z0-9-]+",
    placeholder: "e.g., my-channel",
    required: true
  }));

  // Channel name.
  lines.push(...generateTextField("add-name", "name", "Display Name", formValues?.get("name") ?? "", {

    hint: "Friendly name shown in the playlist and UI.",
    placeholder: "e.g., My Channel",
    required: true
  }));

  // Channel URL.
  lines.push(...generateTextField("add-url", "url", "Stream URL", formValues?.get("url") ?? "", {

    hint: "The URL of the streaming page to capture.",
    placeholder: "https://example.com/live",
    required: true,
    type: "url"
  }));

  // Profile dropdown.
  lines.push(...generateProfileDropdown("add-profile", formValues?.get("profile") ?? "", profiles));

  // Advanced fields (station ID, channel selector, channel number).
  lines.push(...generateAdvancedFields("add", formValues?.get("stationId") ?? "", formValues?.get("channelSelector") ?? "",
    formValues?.get("channelNumber") ?? ""));

  // Form buttons.
  lines.push("<div class=\"form-buttons\">");
  lines.push("<button type=\"submit\" class=\"btn btn-primary\">Add Channel</button>");
  lines.push("<button type=\"button\" class=\"btn btn-secondary\" onclick=\"document.getElementById('add-channel-form').style.display='none'; ",
    "document.getElementById('add-channel-btn').style.display='inline-block';\">Cancel</button>");
  lines.push("</div>");

  lines.push("</form>");
  lines.push("</div>"); // End add-channel-form.

  // Profile reference section (hidden by default, toggled via link in profile dropdown hint).
  lines.push(generateProfileReference(profiles));

  // Channels table. Disabled predefined channels are hidden by default and revealed via the "Show disabled" toggle.
  lines.push("<table class=\"channel-table hide-disabled\">");
  lines.push("<thead>");
  lines.push("<tr>");
  lines.push("<th class=\"col-key\">Key</th>");
  lines.push("<th class=\"col-name\">Name</th>");
  lines.push("<th class=\"col-url\">URL</th>");
  lines.push("<th class=\"col-profile\">Profile</th>");
  lines.push("<th class=\"col-actions\">Actions</th>");
  lines.push("</tr>");
  lines.push("</thead>");
  lines.push("<tbody>");

  // Generate rows for all channels using the shared row generator.
  for(const key of channelKeys) {

    const rowHtml = generateChannelRowHtml(key, profiles);

    lines.push(rowHtml.displayRow);

    if(rowHtml.editRow) {

      lines.push(rowHtml.editRow);
    }
  }

  lines.push("</tbody>");
  lines.push("</table>");

  // Embed channel selector data for datalist population. The client-side JavaScript uses this to offer known selector suggestions when the URL matches a
  // multi-channel site like Disney+ or USA Network.
  lines.push("<script>" + generateChannelSelectorData() + "</script>");

  return lines.join("\n");
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

  lines.push("</div>"); // End section-content.
  lines.push("</div>"); // End advanced-section.

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
 * Configures the configuration endpoints. The GET /config endpoint has been removed - configuration is now accessed via hash navigation on the main page
 * (e.g., /#config/server). Channels are accessed via /#channels. POST endpoints remain for form submission handling.
 * @param app - The Express application.
 */
export function setupConfigEndpoint(app: Express): void {

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
          const parsedValue = parseFormValue(setting, String(rawValue));

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

      // Filter out values that match defaults to keep the config file clean.
      const filteredConfig = filterDefaults(newConfig);

      // Save the configuration.
      await saveUserConfig(filteredConfig);

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

      LOG.error("Failed to save configuration: %s", formatError(error));
      res.status(500).json({ message: "Failed to save configuration: " + formatError(error), success: false });
    }
  });

  // GET /config/export - Export current configuration as JSON.
  app.get("/config/export", async (_req: Request, res: Response): Promise<void> => {

    try {

      const result = await loadUserConfig();

      res.setHeader("Content-Type", "application/json");
      res.setHeader("Content-Disposition", "attachment; filename=\"prismcast-config.json\"");
      res.send(JSON.stringify(result.config, null, 2) + "\n");
    } catch(error) {

      LOG.error("Failed to export configuration: %s", formatError(error));
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

      // Filter out values that match defaults to keep the config file clean.
      const filteredConfig = filterDefaults(importedConfig);

      // Save the imported configuration.
      await saveUserConfig(filteredConfig);

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

      LOG.error("Failed to import configuration: %s", formatError(error));
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
    setTimeout(async () => {

      LOG.info("Exiting for forced service manager restart.");

      await closeBrowser();

      process.exit(0);
    }, 500);
  });

  // GET /config/channels/export - Export user channels as JSON.
  app.get("/config/channels/export", (_req: Request, res: Response): void => {

    try {

      const userChannels = getUserChannels();

      res.setHeader("Content-Type", "application/json");
      res.setHeader("Content-Disposition", "attachment; filename=\"prismcast-channels.json\"");
      res.send(JSON.stringify(userChannels, null, 2) + "\n");
    } catch(error) {

      LOG.error("Failed to export channels: %s", formatError(error));
      res.status(500).json({ error: "Failed to export channels: " + formatError(error) });
    }
  });

  // POST /config/channels/import - Import channels from JSON, replacing all existing user channels.
  app.post("/config/channels/import", async (req: Request, res: Response): Promise<void> => {

    try {

      const rawData: unknown = req.body;

      // Validate the imported channels.
      const validProfiles = getProfiles().map((p) => p.name);
      const validationResult = validateImportedChannels(rawData, validProfiles);

      if(!validationResult.valid) {

        res.status(400).json({ error: "Validation errors:\n" + validationResult.errors.join("\n") });

        return;
      }

      // Save the imported channels, replacing all existing user channels.
      await saveUserChannels(validationResult.channels);

      const channelCount = Object.keys(validationResult.channels).length;

      // Send success response. Changes take effect immediately due to hot-reloading in saveUserChannels().
      res.json({ message: "Imported " + channelCount + " channel" + (channelCount === 1 ? "" : "s") + " successfully.", success: true });
    } catch(error) {

      LOG.error("Failed to import channels: %s", formatError(error));
      res.status(500).json({ error: "Failed to import channels: " + formatError(error) });
    }
  });

  // POST /config/channels/import-m3u - Import channels from M3U playlist file.
  app.post("/config/channels/import-m3u", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { conflictMode?: string; content?: string };
      const content = body.content;
      const conflictMode = body.conflictMode ?? "skip";

      // Validate content is provided.
      if(!content || (typeof content !== "string") || (content.trim() === "")) {

        res.status(400).json({ error: "No M3U content provided.", success: false });

        return;
      }

      // Validate conflict mode.
      if((conflictMode !== "skip") && (conflictMode !== "replace")) {

        res.status(400).json({ error: "Invalid conflict mode. Must be 'skip' or 'replace'.", success: false });

        return;
      }

      // Parse the M3U content.
      const parseResult = parseM3U(content);

      // Check for empty result.
      if(parseResult.channels.length === 0) {

        res.status(400).json({

          error: "No channels found in M3U file." + (parseResult.errors.length > 0 ? " Parse errors: " + parseResult.errors.join("; ") : ""),
          success: false
        });

        return;
      }

      // Load existing user channels.
      const loadResult = await loadUserChannels();
      const existingChannels = loadResult.parseError ? {} : loadResult.channels;

      // Track import statistics.
      const conflicts: string[] = [];
      const importErrors: string[] = [];
      const seenKeys = new Set<string>();
      let imported = 0;
      let replaced = 0;
      let skipped = 0;

      // Process each parsed channel.
      for(const m3uChannel of parseResult.channels) {

        // Generate the channel key from the name.
        const key = generateChannelKey(m3uChannel.name);

        // Validate the generated key.
        if(!key || (key.length === 0)) {

          importErrors.push("Could not generate key for channel '" + m3uChannel.name + "'.");

          continue;
        }

        // Skip duplicate keys within the same M3U file (first occurrence wins).
        if(seenKeys.has(key)) {

          continue;
        }

        seenKeys.add(key);

        // Validate the URL.
        const urlError = validateChannelUrl(m3uChannel.url);

        if(urlError) {

          importErrors.push("Channel '" + m3uChannel.name + "': " + urlError);

          continue;
        }

        // Validate the name.
        const nameError = validateChannelName(m3uChannel.name);

        if(nameError) {

          importErrors.push("Channel '" + m3uChannel.name + "': " + nameError);

          continue;
        }

        // Check for conflicts with existing channels.
        if(key in existingChannels) {

          conflicts.push(key);

          if(conflictMode === "skip") {

            skipped++;

            continue;
          }

          // Replace mode - count as replaced instead of imported.
          replaced++;
        } else {

          imported++;
        }

        // Build the channel object.
        const channel: UserChannel = {

          name: m3uChannel.name,
          url: m3uChannel.url
        };

        // Add station ID if present.
        if(m3uChannel.stationId) {

          channel.stationId = m3uChannel.stationId;
        }

        // Add to channels collection.
        existingChannels[key] = channel;
      }

      // Save the updated channels.
      await saveUserChannels(existingChannels);

      // Log the import.
      LOG.info("M3U import completed: %d imported, %d replaced, %d skipped.", imported, replaced, skipped);

      // Build response.
      res.json({

        conflicts,
        errors: [ ...parseResult.errors, ...importErrors ],
        imported,
        replaced,
        skipped,
        success: true
      });
    } catch(error) {

      LOG.error("Failed to import M3U channels: %s", formatError(error));
      res.status(500).json({ error: "Failed to import channels: " + formatError(error), success: false });
    }
  });

  // POST /config/channels/toggle-predefined - Toggle a single predefined channel's enabled/disabled state.
  app.post("/config/channels/toggle-predefined", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { enabled?: boolean; key?: string };
      const key = body.key?.trim();
      const enabled = body.enabled;

      // Validate key is provided.
      if(!key) {

        res.status(400).json({ error: "Channel key is required.", success: false });

        return;
      }

      // Validate enabled is provided.
      if(typeof enabled !== "boolean") {

        res.status(400).json({ error: "Enabled state (true/false) is required.", success: false });

        return;
      }

      // Validate the channel exists as a predefined channel.
      if(!isPredefinedChannel(key)) {

        res.status(400).json({ error: "Channel '" + key + "' is not a predefined channel.", success: false });

        return;
      }

      // Load current config.
      const configResult = await loadUserConfig();
      const userConfig = configResult.config;

      // Initialize channels.disabledPredefined if not present.
      userConfig.channels ??= {};
      userConfig.channels.disabledPredefined ??= [];

      const disabledSet = new Set(userConfig.channels.disabledPredefined);

      if(enabled) {

        // Enable: remove from disabled list.
        disabledSet.delete(key);
      } else {

        // Disable: add to disabled list.
        disabledSet.add(key);
      }

      // Update and save config.
      userConfig.channels.disabledPredefined = [...disabledSet].sort();

      await saveUserConfig(userConfig);

      // Update the runtime CONFIG to reflect the change immediately.
      CONFIG.channels.disabledPredefined = userConfig.channels.disabledPredefined;

      LOG.info("Predefined channel '%s' %s.", key, enabled ? "enabled" : "disabled");

      res.json({ enabled, key, success: true });
    } catch(error) {

      LOG.error("Failed to toggle predefined channel: %s", formatError(error));
      res.status(500).json({ error: "Failed to toggle channel: " + formatError(error), success: false });
    }
  });

  // POST /config/channels/toggle-all-predefined - Toggle all predefined channels' enabled/disabled state.
  app.post("/config/channels/toggle-all-predefined", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { enabled?: boolean };
      const enabled = body.enabled;

      // Validate enabled is provided.
      if(typeof enabled !== "boolean") {

        res.status(400).json({ error: "Enabled state (true/false) is required.", success: false });

        return;
      }

      // Load current config.
      const configResult = await loadUserConfig();
      const userConfig = configResult.config;

      // Initialize channels.disabledPredefined if not present.
      userConfig.channels ??= {};

      const predefinedKeys = Object.keys(getPredefinedChannels());
      let affected: number;

      if(enabled) {

        // Enable all: clear the disabled list.
        affected = userConfig.channels.disabledPredefined?.length ?? 0;
        userConfig.channels.disabledPredefined = [];
      } else {

        // Disable all: add all predefined channel keys.
        const previousCount = userConfig.channels.disabledPredefined?.length ?? 0;

        userConfig.channels.disabledPredefined = predefinedKeys.sort();
        affected = predefinedKeys.length - previousCount;
      }

      await saveUserConfig(userConfig);

      // Update the runtime CONFIG to reflect the change immediately.
      CONFIG.channels.disabledPredefined = userConfig.channels.disabledPredefined;

      LOG.info("All predefined channels %s (%d affected).", enabled ? "enabled" : "disabled", affected);

      res.json({ affected, enabled, success: true });
    } catch(error) {

      LOG.error("Failed to toggle all predefined channels: %s", formatError(error));
      res.status(500).json({ error: "Failed to toggle channels: " + formatError(error), success: false });
    }
  });

  // POST /config/channels - Handle channel add, edit, delete operations. Returns JSON response.
  app.post("/config/channels", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as Record<string, string | undefined>;
      const action = body.action;
      const key = body.key?.trim();
      const profiles = getProfiles();

      // Handle delete action.
      if(action === "delete") {

        if(!key) {

          res.status(400).json({ message: "Channel key is required for delete.", success: false });

          return;
        }

        if(!isUserChannel(key)) {

          res.status(400).json({ message: "Cannot delete '" + key + "': it is not a user-defined channel.", success: false });

          return;
        }

        // Delete the channel.
        const result = await loadUserChannels();

        if(result.parseError) {

          res.status(400).json({ message: "Cannot delete channel: channels file contains invalid JSON.", success: false });

          return;
        }

        delete result.channels[key];

        await saveUserChannels(result.channels);

        LOG.info("User channel '%s' deleted.", key);

        // If a predefined channel exists with the same key, generate its HTML so the client can replace the user channel row with the predefined version instead of
        // just removing it. Without this, deleting a user override of a predefined channel would leave the predefined channel invisible until a page refresh.
        const predefined = isPredefinedChannel(key) ? generateChannelRowHtml(key, profiles) : undefined;

        // Return success response with key for client-side DOM update. Changes take effect immediately due to hot-reloading in saveUserChannels().
        res.json({ html: predefined, key, message: "Channel '" + key + "' deleted successfully.", success: true });

        return;
      }

      // Handle add and edit actions.
      if((action !== "add") && (action !== "edit")) {

        res.status(400).json({ message: "Invalid channel action.", success: false });

        return;
      }

      // Validate channel fields.
      const formErrors: Record<string, string> = {};

      // Collect form values.
      const name = body.name?.trim() ?? "";
      const url = body.url?.trim() ?? "";
      const profile = body.profile?.trim() ?? "";
      const stationId = body.stationId?.trim() ?? "";
      const channelSelector = body.channelSelector?.trim() ?? "";
      const channelNumberStr = body.channelNumber?.trim() ?? "";

      // Validate channel number if provided.
      if(channelNumberStr) {

        const num = parseInt(channelNumberStr, 10);

        if(Number.isNaN(num) || (num < 1) || (num > 99999)) {

          formErrors.channelNumber = "Channel number must be between 1 and 99999.";
        } else {

          // Check for duplicate channel numbers across all channels.
          const allChannels = { ...getPredefinedChannels(), ...getUserChannels() };

          for(const [ existingKey, existingChannel ] of Object.entries(allChannels)) {

            if((existingChannel.channelNumber === num) && (existingKey !== key)) {

              formErrors.channelNumber = "Channel number " + String(num) + " is already used by '" + existingKey + "'.";

              break;
            }
          }
        }
      }

      // Validate key (only for add action, not edit).
      if(action === "add") {

        const keyError = validateChannelKey(key ?? "", true);

        if(keyError) {

          formErrors.key = keyError;
        }
      }

      // Validate name.
      const nameError = validateChannelName(name);

      if(nameError) {

        formErrors.name = nameError;
      }

      // Validate URL.
      const urlError = validateChannelUrl(url);

      if(urlError) {

        formErrors.url = urlError;
      }

      // Validate profile (if specified).
      const profileError = validateChannelProfile(profile, profiles.map((p) => p.name));

      if(profileError) {

        formErrors.profile = profileError;
      }

      // If validation errors, return them as JSON.
      if(Object.keys(formErrors).length > 0) {

        res.status(400).json({ errors: formErrors, success: false });

        return;
      }

      // Load existing user channels.
      const result = await loadUserChannels();

      if(result.parseError) {

        // If channels file is corrupt, start fresh on add (which will create a valid file).
        if(action === "add") {

          result.channels = {};
        } else {

          res.status(400).json({ message: "Cannot edit channel: channels file contains invalid JSON.", success: false });

          return;
        }
      }

      // Build the channel object.
      const channel: UserChannel = {

        name,
        url
      };

      if(profile) {

        channel.profile = profile;
      }

      if(stationId) {

        channel.stationId = stationId;
      }

      if(channelSelector) {

        channel.channelSelector = channelSelector;
      }

      if(channelNumberStr) {

        channel.channelNumber = parseInt(channelNumberStr, 10);
      }

      // Add or update the channel.
      result.channels[key as string] = channel;

      await saveUserChannels(result.channels);

      const actionLabel = (action === "add") ? "added" : "updated";

      LOG.info("User channel '%s' %s.", key, actionLabel);

      // Generate HTML for the channel row so the client can update the DOM without a full page reload.
      const rowHtml = generateChannelRowHtml(key as string, profiles);

      // Return success response with HTML for client-side DOM update. Changes take effect immediately due to hot-reloading in saveUserChannels().
      res.json({

        html: { displayRow: rowHtml.displayRow, editRow: rowHtml.editRow },
        isNew: action === "add",
        key,
        message: "Channel '" + key + "' " + actionLabel + " successfully.",
        success: true
      });
    } catch(error) {

      LOG.error("Failed to save channel: %s", formatError(error));
      res.status(500).json({ message: "Failed to save channel: " + formatError(error), success: false });
    }
  });
}
