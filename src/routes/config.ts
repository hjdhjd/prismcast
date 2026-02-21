/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * config.ts: Configuration web interface for PrismCast.
 */
import type { AdvancedSection, SettingMetadata, UserConfig } from "../config/userConfig.js";
import { CONFIG, getDefaults, validatePositiveInt, validatePositiveNumber } from "../config/index.js";
import { CONFIG_METADATA, filterDefaults, getAdvancedSections, getEnvOverrides, getNestedValue, getSettingsTabSections, getUITabs, isEqualToDefault,
  loadUserConfig, saveUserConfig, setNestedValue } from "../config/userConfig.js";
import type { Channel, ChannelDelta, ChannelListingEntry, Nullable, ProfileCategory, StoredChannel } from "../types/index.js";
import type { Express, Request, Response } from "express";
import { LOG, escapeHtml, formatError, generateChannelKey, isRunningAsService, parseM3U } from "../utils/index.js";
import { getAllProviderTags, getCanonicalKey, getChannelProviderTags, getEnabledProviders, getProviderDisplayName, getProviderGroup, getProviderSelection,
  getProviderTagForChannel, getResolvedChannel, hasMultipleProviders, isChannelAvailableByProvider, isProviderTagEnabled, resolvePredefinedVariant,
  resolveProviderKey, setEnabledProviders, setProviderSelection } from "../config/providers.js";
import { getChannelListing, getChannelsParseErrorMessage, getDisabledPredefinedChannels, getPredefinedChannel, getPredefinedChannels, getUserChannels,
  getUserChannelsFilePath, hasChannelsParseError, isPredefinedChannel, isPredefinedChannelDisabled, isUserChannel, loadUserChannels, resolveStoredChannel,
  saveProviderSelections, saveUserChannels, validateChannelKey, validateChannelName, validateChannelProfile, validateChannelUrl,
  validateImportedChannels } from "../config/userChannels.js";
import { PREDEFINED_CHANNELS } from "../channels/index.js";
import type { ProfileInfo } from "../config/profiles.js";
import type { UserChannel } from "../config/userChannels.js";
import { closeBrowser } from "../browser/index.js";
import { getConfigFilePath } from "../config/paths.js";
import { getPresetOptionsWithDegradation } from "../config/presets.js";
import { getProfiles } from "../config/profiles.js";
import { getStreamCount } from "../streaming/registry.js";

/* The /config endpoint provides a user-friendly web interface for editing PrismCast settings. Users can adjust values, see defaults, and understand what each
 * setting does without editing JSON files directly. Changes require a server restart to take effect.
 *
 * The UI shows:
 * - Settings grouped by category (Server, Browser, Streaming, etc.)
 * - Current value, default value, and description for each setting
 * - Visual indicator for settings overridden by environment variables (disabled with explanation)
 * - Reset buttons for individual categories
 * - Save & Restart button that applies changes and restarts the server
 */

// SVG icon constants for channel action buttons. Each icon is 14x14px with a 16x16 viewBox, stroke-based with currentColor, and uses round line caps/joins.

const ICON_EDIT = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><path d=\"M11.5 1.5l3 3L5 14H2v-3L11.5 1.5z\"/></svg>";

const ICON_LOGIN = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><path d=\"M6.5 2H3.5a1 1 0 00-1 1v10a1 1 0 001 1h3\"/><path d=\"M10.5 11l3-3-3-3\"/><path d=\"M13.5 8H6.5\"/></svg>";

const ICON_DELETE = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><path d=\"M2 4h12\"/><path d=\"M5 4V2.5a.5.5 0 01.5-.5h5a.5.5 0 01.5.5V4\"/><path d=\"M12.5 4l-.5 9.5a1 1 0 01-1 .5H5a1 1 0 " +
  "01-1-.5L3.5 4\"/></svg>";

const ICON_ENABLE = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><circle cx=\"8\" cy=\"8\" r=\"6\"/><path d=\"M5.5 8l2 2 3.5-4\"/></svg>";

const ICON_DISABLE = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><circle cx=\"8\" cy=\"8\" r=\"6\"/><path d=\"M5.5 5.5l5 5\"/></svg>";

const ICON_REVERT = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><path d=\"M3 8a5 5 0 1 1 1.5 3.5\"/><path d=\"M3 4v4h4\"/></svg>";

const ICON_COPY = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><rect x=\"5\" y=\"5\" width=\"9\" height=\"9\" rx=\"1\"/><path d=\"M5 11H3a1 1 0 01-1-1V3a1 1 0 011-1h7a1 1 0 011 1v2\"/></svg>";

// Fields that appear in the generated M3U playlist and affect Channels DVR's view of the channel. Used to decide whether the playlist reload hint is shown.
const M3U_FIELDS = [ "channelNumber", "name", "stationId" ];

const PLAYLIST_HINT = " Reload the playlist in Channels DVR to see this change.";

/**
 * Checks whether a stored channel entry contains any fields that affect the M3U playlist. Used to decide whether to append the playlist reload hint when reverting
 * or removing an override.
 * @param stored - The stored channel data to check (may be a delta or full definition).
 * @returns The PLAYLIST_HINT string if M3U-relevant fields are present, empty string otherwise.
 */
function playlistHintForStored(stored: StoredChannel): string {

  return M3U_FIELDS.some((f) => f in stored) ? PLAYLIST_HINT : "";
}

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
  setTimeout(() => {

    LOG.info("Exiting for service manager restart %s.", reason);

    void closeBrowser().then(() => { process.exit(0); }).catch(() => { process.exit(1); });
  }, 500);

  return {

    activeStreams: 0,
    deferred: false,
    message: "Configuration saved. Server is restarting...",
    willRestart: true
  };
}

/* These helper functions generate HTML for channel form fields. They are used by both the add and edit forms to reduce code duplication and ensure consistent
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
 * Groups profiles by their declared category for UI display. Each profile declares its own category (api, keyboard, multiChannel, special) and this helper
 * simply filters by that field. The display order (api, keyboard, special, multiChannel) is determined by the caller.
 * @param profiles - List of available profiles with category, descriptions, and summaries.
 * @returns Object with profiles grouped by category.
 */
function categorizeProfiles(profiles: ProfileInfo[]): Record<ProfileCategory, ProfileInfo[]> {

  return {

    api: profiles.filter((p) => (p.category === "api")),
    keyboard: profiles.filter((p) => (p.category === "keyboard")),
    multiChannel: profiles.filter((p) => (p.category === "multiChannel")),
    special: profiles.filter((p) => (p.category === "special"))
  };
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
  const groups = categorizeProfiles(profiles);

  // Helper to generate option elements for a profile.
  const renderOption = (profile: ProfileInfo): string => {

    const selected = (profile.name === selectedProfile) ? " selected" : "";
    const title = profile.description ? " title=\"" + escapeHtml(profile.description) + "\"" : "";
    const displayText = profile.summary ? profile.name + " \u2014 " + profile.summary : profile.name;

    return "<option value=\"" + escapeHtml(profile.name) + "\"" + title + selected + ">" + escapeHtml(displayText) + "</option>";
  };

  lines.push("<div class=\"form-row\">");
  lines.push("<label for=\"" + id + "\">Profile</label>");
  lines.push("<select class=\"form-select field-wide\" id=\"" + id + "\" name=\"profile\">");
  lines.push("<option value=\"\">Autodetect (Recommended)</option>");

  // Fullscreen API profiles (most common).
  if(groups.api.length > 0) {

    lines.push("<optgroup label=\"Fullscreen API\">");

    for(const profile of groups.api) {

      lines.push(renderOption(profile));
    }

    lines.push("</optgroup>");
  }

  // Keyboard fullscreen profiles.
  if(groups.keyboard.length > 0) {

    lines.push("<optgroup label=\"Keyboard Fullscreen\">");

    for(const profile of groups.keyboard) {

      lines.push(renderOption(profile));
    }

    lines.push("</optgroup>");
  }

  // Special profiles.
  if(groups.special.length > 0) {

    lines.push("<optgroup label=\"Special\">");

    for(const profile of groups.special) {

      lines.push(renderOption(profile));
    }

    lines.push("</optgroup>");
  }

  // Multi-channel profiles (at the end).
  if(groups.multiChannel.length > 0) {

    lines.push("<optgroup label=\"Multi-Channel (needs selector)\">");

    for(const profile of groups.multiChannel) {

      lines.push(renderOption(profile));
    }

    lines.push("</optgroup>");
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

  const groups = categorizeProfiles(profiles);

  lines.push("<div id=\"profile-reference\" class=\"profile-reference\" style=\"display: none;\">");
  lines.push("<div class=\"profile-reference-header\">");
  lines.push("<h3>Profile Reference</h3>");
  lines.push("<button type=\"button\" class=\"profile-reference-close\" aria-label=\"Close\" onclick=\"toggleProfileReference()\">\u2715</button>");
  lines.push("</div>");
  lines.push("<p class=\"reference-intro\">Profiles configure how PrismCast interacts with different video players. Autodetect uses predefined ");
  lines.push("profiles for known sites. If video doesn't play or fullscreen fails, use this reference to experiment with different profiles.</p>");

  // Fullscreen API profiles (most common).
  if(groups.api.length > 0) {

    lines.push("<div class=\"profile-category\">");
    lines.push("<h4>Fullscreen API Profiles</h4>");
    lines.push("<p class=\"category-desc\">For single-channel sites that require JavaScript's requestFullscreen() API instead of keyboard shortcuts.</p>");
    lines.push("<dl class=\"profile-list\">");

    for(const profile of groups.api) {

      lines.push("<dt>" + escapeHtml(profile.name) + "</dt>");
      lines.push("<dd>" + escapeHtml(profile.description) + "</dd>");
    }

    lines.push("</dl>");
    lines.push("</div>");
  }

  // Keyboard fullscreen profiles.
  if(groups.keyboard.length > 0) {

    lines.push("<div class=\"profile-category\">");
    lines.push("<h4>Keyboard Fullscreen Profiles</h4>");
    lines.push("<p class=\"category-desc\">For single-channel sites that use the 'f' key to toggle fullscreen mode.</p>");
    lines.push("<dl class=\"profile-list\">");

    for(const profile of groups.keyboard) {

      lines.push("<dt>" + escapeHtml(profile.name) + "</dt>");
      lines.push("<dd>" + escapeHtml(profile.description) + "</dd>");
    }

    lines.push("</dl>");
    lines.push("</div>");
  }

  // Special profiles.
  if(groups.special.length > 0) {

    lines.push("<div class=\"profile-category\">");
    lines.push("<h4>Special Profiles</h4>");
    lines.push("<p class=\"category-desc\">For non-standard use cases like static pages without video.</p>");
    lines.push("<dl class=\"profile-list\">");

    for(const profile of groups.special) {

      lines.push("<dt>" + escapeHtml(profile.name) + "</dt>");
      lines.push("<dd>" + escapeHtml(profile.description) + "</dd>");
    }

    lines.push("</dl>");
    lines.push("</div>");
  }

  // Multi-channel profiles (requires channel selector) - at the end since these are more advanced.
  if(groups.multiChannel.length > 0) {

    lines.push("<div class=\"profile-category\">");
    lines.push("<h4>Multi-Channel Profiles</h4>");
    lines.push("<p class=\"category-desc\">For sites that host multiple live channels on a single page. These profiles require a channel selector ");
    lines.push("to identify which channel to tune to. Set the Channel Selector field in Advanced Options when using these profiles.</p>");
    lines.push("<dl class=\"profile-list\">");

    for(const profile of groups.multiChannel) {

      lines.push("<dt>" + escapeHtml(profile.name) + "</dt>");
      lines.push("<dd>" + escapeHtml(profile.description) + "</dd>");
    }

    lines.push("</dl>");

    // Per-strategy guidance for finding Channel Selector values. Organized by strategy type since the same strategy can be used across multiple profiles.
    lines.push("<h4 class=\"selector-guide-heading\">Finding Your Channel Selector</h4>");
    lines.push("<p class=\"category-desc\">Predefined channels already have Channel Selector values set. For custom channels, the value depends on the ");
    lines.push("profile's strategy type:</p>");
    lines.push("<dl class=\"profile-list\">");
    lines.push("<dt>apiMultiVideo, disneyPlus, keyboardDynamicMultiVideo (element selector)</dt>");
    lines.push("<dd>These profiles use a <code>matchSelector</code> CSS template to find the channel element. The default pattern matches image URLs: ");
    lines.push("right-click the channel's image on the site \u2192 Inspect Element \u2192 find the &lt;img&gt; tag \u2192 copy a unique portion ");
    lines.push("of the <code>src</code> URL that identifies the channel (e.g., \"espn\" from a URL containing \"poster_linear_espn_none\"). ");
    lines.push("Custom <code>matchSelector</code> patterns can match any attribute (aria-label, data-testid, title, etc.).</dd>");
    lines.push("<dt>foxLive (station code)</dt>");
    lines.push("<dd>Inspect a channel logo in the guide \u2192 find the <code>&lt;button&gt;</code> inside <code>GuideChannelLogo</code> \u2192 use ");
    lines.push("the <code>title</code> attribute value (e.g., BTN, FOXD2C, FS1, FS2, FWX).</dd>");
    lines.push("<dt>hboMax (channel name)</dt>");
    lines.push("<dd>Inspect a channel tile in the HBO rail \u2192 find the <code>&lt;p aria-hidden=\"true\"&gt;</code> element \u2192 use the text ");
    lines.push("content (e.g., HBO, HBO Comedy, HBO Drama, HBO Hits, HBO Movies).</dd>");
    lines.push("<dt>huluLive (channel name)</dt>");
    lines.push("<dd>Inspect a channel entry in the guide \u2192 find the <code>data-testid</code> attribute starting with ");
    lines.push("<code>live-guide-channel-kyber-</code> \u2192 use the portion after that prefix. The name may differ from the logo shown ");
    lines.push("(e.g., the full name rather than an abbreviation). For local affiliates (ABC, CBS, FOX, NBC), use the network name \u2014 PrismCast ");
    lines.push("resolves the local station automatically.</dd>");
    lines.push("<dt>slingLive (channel name)</dt>");
    lines.push("<dd>Inspect a channel entry in the guide \u2192 find the <code>data-testid</code> attribute starting with <code>channel-</code> ");
    lines.push("\u2192 use the portion after that prefix. The name may differ from the logo shown (e.g., \"FOX Sports 1\" not \"FS1\"). For local ");
    lines.push("affiliates (ABC, CBS, FOX, NBC), use the network name \u2014 PrismCast resolves the local station automatically.</dd>");
    lines.push("<dt>youtubeTV (channel name)</dt>");
    lines.push("<dd>Inspect a channel thumbnail in the guide \u2192 find the <code>aria-label</code> attribute on the ");
    lines.push("<code>ytu-endpoint</code> element \u2192 use the name after \"watch \" (e.g., <code>aria-label=\"watch CNN\"</code> \u2192 CNN). ");
    lines.push("For locals, use the network name (e.g., NBC) \u2014 affiliates like \"NBC 5\" are resolved automatically. PBS resolves to the ");
    lines.push("local affiliate in major markets.</dd>");
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

  lines.push(...generateTextField(idPrefix + "-stationId", "stationId", "Station ID", stationIdValue,
    { hint: stationIdHint, placeholder: showHints ? "e.g., 12345" : undefined }));

  // Channel selector.
  const channelSelectorHint = showHints ?
    "Identifies which channel to select on sites that host multiple live streams. Known values are suggested when the URL matches a supported site. " +
    "For guide-based profiles (Fox, HBO Max, Hulu, Sling, YouTube TV), use the channel name or station code from the guide. " +
    "For tile and thumbnail profiles, right-click the channel element \u2192 Inspect \u2192 copy a unique value matching the profile's selector pattern " +
    "(typically a portion of the image src URL)." :
    undefined;

  lines.push(...generateTextField(idPrefix + "-channelSelector", "channelSelector", "Channel Selector", channelSelectorValue,
    { hint: channelSelectorHint, list: idPrefix + "-selectorList", placeholder: showHints ? "e.g., ESPN" : undefined }));

  // Channel number for Channels DVR and Plex integration.
  const channelNumberHint = showHints ?
    "Optional numeric channel number for guide matching in Channels DVR and Plex." :
    undefined;

  lines.push(...generateTextField(idPrefix + "-channelNumber", "channelNumber", "Channel Number", channelNumberValue,
    { hint: channelNumberHint, placeholder: showHints ? "e.g., 501" : undefined }));

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

  const byDomain: Record<string, { label: string; value: string }[]> = {};

  for(const channel of Object.values(PREDEFINED_CHANNELS)) {

    if(!channel.channelSelector) {

      continue;
    }

    const hostname = new URL(channel.url).hostname;

    byDomain[hostname] ??= [];
    byDomain[hostname].push({ label: channel.name ?? channel.channelSelector, value: channel.channelSelector });
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

  // The edit form row HTML (always present for all channels — predefined, override, and user-defined).
  editRow: string;
}

/**
 * Generates the HTML for a single channel's table rows (display row and edit form row). All channels — predefined, override, and user-defined — get both rows.
 * The edit form is pre-populated with the effective (resolved) values so users see what they're changing. When called from generateChannelsPanel() which already
 * has the listing entry, pass it via the entry parameter to avoid redundant getChannelListing() calls. POST handlers that generate a single row omit the
 * parameter to trigger an internal lookup.
 * @param key - The channel key.
 * @param profiles - List of available profiles with descriptions for the dropdown.
 * @param entry - Optional pre-resolved listing entry. When omitted, looked up from getChannelListing().
 * @returns Object with displayRow and editRow HTML strings.
 */
export function generateChannelRowHtml(key: string, profiles: ProfileInfo[], entry?: ChannelListingEntry): ChannelRowHtml {

  // Resolve the effective channel. Use the provided entry if available, otherwise look it up from the listing.
  const listing = entry ?? getChannelListing().find((e) => e.key === key);

  // If channel doesn't exist in the listing, return empty rows (shouldn't happen in normal use).
  if(!listing) {

    return { displayRow: "", editRow: "" };
  }

  const channel = listing.channel;

  // Resolve the selected provider's channel data for display purposes (edit form pre-population). This ensures the values shown reflect the currently selected provider.
  const resolvedKey = resolveProviderKey(key);
  const resolvedChannel = getResolvedChannel(resolvedKey);
  const displayChannel = resolvedChannel ?? channel;

  const isUser = isUserChannel(key);
  const isPredefined = isPredefinedChannel(key);
  const isOverride = isPredefined && isUser;
  const isDisabled = isPredefinedChannelDisabled(key);
  const isAvailableByProvider = isChannelAvailableByProvider(key);

  // Check if this channel has multiple providers.
  const providerGroup = getProviderGroup(key);

  // Build the provider tags data attribute for client-side filtering.
  const providerTags = getChannelProviderTags(key).join(",");

  // Generate display row. User channels get one CSS class, disabled predefined get another, provider-filtered get a third.
  const displayLines: string[] = [];
  const rowClasses: string[] = [];

  if(isUser) {

    rowClasses.push("user-channel");
  }

  if(isDisabled) {

    rowClasses.push("channel-disabled");
  }

  if(!isAvailableByProvider) {

    rowClasses.push("channel-unavailable");
  }

  const rowClassAttr = (rowClasses.length > 0) ? " class=\"" + rowClasses.join(" ") + "\"" : "";

  displayLines.push("<tr id=\"display-row-" + escapeHtml(key) + "\"" + rowClassAttr + " data-provider-tags=\"" + escapeHtml(providerTags) + "\">");
  displayLines.push("<td class=\"ch-key\">" + escapeHtml(key) + "</td>");
  displayLines.push("<td>" + escapeHtml(channel.name ?? key) + "</td>");

  // Provider column: dropdown for multi-provider channels, static provider name for single-provider. Both states always render a hidden "No available providers"
  // label alongside the provider content so that client-side filterChannelRows() can toggle between them without a page reload.
  displayLines.push("<td>");

  const labelHidden = isAvailableByProvider ? " style=\"display:none\"" : "";
  const contentHidden = isAvailableByProvider ? "" : " style=\"display:none\"";

  displayLines.push("<em class=\"no-provider-label\"" + labelHidden + ">No available providers</em>");

  if(hasMultipleProviders(key) && providerGroup) {

    // Multi-provider: render ALL variants with data-provider-tag attributes so client-side JS can filter options when the provider selection changes. Filtered-out
    // options get the hidden attribute for immediate filtering in Chrome. Safari ignores hidden on option elements, so the page-load JS init calls filterChannelRows()
    // to remove them from the DOM.
    const currentSelection = getProviderSelection(key) ?? key;

    displayLines.push("<select class=\"provider-select\" data-channel=\"" + escapeHtml(key) + "\" onchange=\"updateProviderSelection(this)\"" +
      contentHidden + ">");

    for(const variant of providerGroup.variants) {

      const selected = (variant.key === currentSelection) ? " selected" : "";
      const tag = getProviderTagForChannel(variant.key);
      const optionHidden = !isProviderTagEnabled(tag) ? " hidden" : "";

      displayLines.push("<option value=\"" + escapeHtml(variant.key) + "\" data-provider-tag=\"" + escapeHtml(tag) + "\"" + selected + optionHidden + ">" +
        escapeHtml(variant.label) + "</option>");
    }

    displayLines.push("</select>");
  } else {

    // Single-provider: wrap the provider name in a span so client-side JS can toggle it with the no-provider label.
    displayLines.push("<span class=\"provider-name\"" + contentHidden + ">" +
      escapeHtml(channel.provider ?? getProviderDisplayName(channel.url)) + "</span>");
  }

  displayLines.push("</td>");

  // Actions column with icon buttons. Four positions per row: Edit (always), Login/placeholder (middle), a third action that varies by row type, and Copy URL.
  displayLines.push("<td>");
  displayLines.push("<div class=\"btn-group\">");

  const escapedKey = escapeHtml(key);

  // Position 1: Edit (all channels).
  displayLines.push("<button type=\"button\" class=\"btn-icon btn-icon-edit\" title=\"Edit\" aria-label=\"Edit\" onclick=\"showEditForm('" + escapedKey +
    "')\">" + ICON_EDIT + "</button>");

  // Position 2: Login for enabled channels, placeholder for disabled predefined.
  if(!isDisabled) {

    displayLines.push("<button type=\"button\" class=\"btn-icon btn-icon-login\" title=\"Login\" aria-label=\"Login\" onclick=\"startChannelLogin('" +
      escapedKey + "')\">" + ICON_LOGIN + "</button>");
  } else {

    displayLines.push("<span class=\"btn-icon-placeholder\"></span>");
  }

  // Position 3: varies by row type.
  if(isOverride) {

    // Override: revert to predefined defaults.
    displayLines.push("<button type=\"button\" class=\"btn-icon btn-icon-revert\" title=\"Revert to defaults\" aria-label=\"Revert to defaults\"" +
      " onclick=\"revertChannel('" + escapedKey + "')\">" + ICON_REVERT + "</button>");
  } else if(isUser && !isPredefined) {

    // User-defined (not an override): delete.
    displayLines.push("<button type=\"button\" class=\"btn-icon btn-icon-delete\" title=\"Delete\" aria-label=\"Delete\" onclick=\"deleteChannel('" +
      escapedKey + "')\">" + ICON_DELETE + "</button>");
  } else if(isPredefined) {

    // Predefined (no override): enable/disable toggle.
    if(isDisabled) {

      displayLines.push("<button type=\"button\" class=\"btn-icon btn-icon-enable\" title=\"Enable\" aria-label=\"Enable\" onclick=\"togglePredefinedChannel('" +
        escapedKey + "', true)\">" + ICON_ENABLE + "</button>");
    } else {

      displayLines.push("<button type=\"button\" class=\"btn-icon btn-icon-disable\" title=\"Disable\" aria-label=\"Disable\" onclick=\"togglePredefinedChannel('" +
        escapedKey + "', false)\">" + ICON_DISABLE + "</button>");
    }
  }

  // Position 4: Copy URL dropdown (all channels).
  displayLines.push("<div class=\"dropdown copy-dropdown\">");
  displayLines.push("<button type=\"button\" class=\"btn-icon btn-icon-copy\" title=\"Copy stream URL\" aria-label=\"Copy stream URL\" " +
    "onclick=\"toggleDropdown(this)\">" + ICON_COPY + "</button>");
  displayLines.push("<div class=\"dropdown-menu copy-url-menu\">");
  displayLines.push("<div class=\"dropdown-item\" onclick=\"copyStreamUrl('hls', '" + escapedKey + "')\">Copy HLS URL</div>");
  displayLines.push("<div class=\"dropdown-item\" onclick=\"copyStreamUrl('mpegts', '" + escapedKey + "')\">Copy MPEG-TS URL</div>");
  displayLines.push("</div>");
  displayLines.push("</div>");

  displayLines.push("</div>");
  displayLines.push("</td>");
  displayLines.push("</tr>");

  const displayRow = displayLines.join("\n");

  // Generate edit form row for all channels. Pre-populate with the currently selected provider's values so the user sees what they're actually streaming, not the
  // canonical definition which may differ when a provider variant is selected.
  const editLines: string[] = [];

  editLines.push("<tr id=\"edit-row-" + escapedKey + "\" style=\"display: none;\">");
  editLines.push("<td colspan=\"4\">");
  editLines.push("<div class=\"channel-form\" style=\"margin: 0;\">");
  editLines.push("<h3>Edit Channel: " + escapedKey + "</h3>");
  editLines.push("<form id=\"edit-channel-form-" + escapedKey + "\" onsubmit=\"return submitChannelForm(event, 'edit')\">");
  editLines.push("<input type=\"hidden\" name=\"action\" value=\"edit\">");
  editLines.push("<input type=\"hidden\" name=\"key\" value=\"" + escapedKey + "\">");

  // Channel name.
  editLines.push(...generateTextField("edit-name-" + key, "name", "Display Name", displayChannel.name ?? key, {

    hint: "Friendly name shown in the playlist and UI.",
    required: true
  }));

  // Channel URL.
  editLines.push(...generateTextField("edit-url-" + key, "url", "Stream URL", displayChannel.url, {

    hint: "The URL of the streaming page to capture.",
    required: true,
    type: "url"
  }));

  // Profile dropdown.
  editLines.push(...generateProfileDropdown("edit-profile-" + key, displayChannel.profile ?? "", profiles));

  // Advanced fields.
  editLines.push(...generateAdvancedFields("edit-" + key, displayChannel.stationId ?? "", displayChannel.channelSelector ?? "",
    displayChannel.channelNumber ? String(displayChannel.channelNumber) : ""));

  // Form buttons.
  editLines.push("<div class=\"form-buttons\">");
  editLines.push("<button type=\"submit\" class=\"btn btn-primary\">Save Changes</button>");
  editLines.push("<button type=\"button\" class=\"btn btn-secondary\" onclick=\"hideEditForm('" + escapedKey + "')\">Cancel</button>");
  editLines.push("</div>");

  editLines.push("</form>");
  editLines.push("</div>");
  editLines.push("</td>");
  editLines.push("</tr>");

  const editRow = editLines.join("\n");

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

  if(typeof value === "string") {

    return value;
  }

  // Config values are always primitives (string, number, boolean). Numbers and strings are handled above.
  return String(value as boolean);
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

  if(envOverride !== undefined) {

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
  } else {

    // Render as input field.
    const inputType = (setting.type === "float") ? "number" : (((setting.type === "integer") || (setting.type === "port")) ? "number" : "text");

    // Calculate step for arrow key increments. Auto-derived from min/displayDivisor: when the min in display units is between 0 and 1 (exclusive), use it as the
    // step (e.g., 500ms → 0.5s step); otherwise step is 1 whole display unit. This gives meaningful arrow increments and constrains input to a sensible value grid
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

  if(displayDefault === null) {

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
 * Generates the provider filter toolbar HTML with a multi-select dropdown, dismissable chips, and a bulk-assign dropdown.
 * @returns HTML string for the provider filter toolbar.
 */
export function generateProviderFilterToolbar(): string {

  const allTags = getAllProviderTags();
  const enabled = getEnabledProviders();
  const hasFilter = enabled.length > 0;
  const lines: string[] = [];

  lines.push("<div class=\"provider-toolbar\">");

  // Left group: Provider filter dropdown and chips.
  lines.push("<div class=\"toolbar-group\">");
  lines.push("<span class=\"toolbar-label\">Providers:</span>");
  lines.push("<div class=\"dropdown provider-dropdown\">");

  const buttonText = hasFilter ? "Filtered" : "All Providers";

  lines.push("<button type=\"button\" class=\"btn btn-sm\" id=\"provider-filter-btn\" onclick=\"toggleDropdown(this)\">" + buttonText + " &#9662;</button>");
  lines.push("<div class=\"dropdown-menu provider-dropdown-menu\">");

  for(const tagInfo of allTags) {

    const isDirectTag = tagInfo.tag === "direct";
    const isChecked = isDirectTag || !hasFilter || enabled.includes(tagInfo.tag);
    const checkedAttr = isChecked ? " checked" : "";
    const disabledAttr = isDirectTag ? " disabled" : "";

    lines.push("<label class=\"provider-option\">");
    lines.push("<input type=\"checkbox\" data-tag=\"" + escapeHtml(tagInfo.tag) + "\"" + checkedAttr + disabledAttr +
      " onchange=\"toggleProviderTag(this)\"> " + escapeHtml(tagInfo.displayName));
    lines.push("</label>");
  }

  lines.push("</div>");
  lines.push("</div>");

  // Chips container for active filter tags.
  lines.push("<div class=\"provider-chips\" id=\"provider-chips\">");

  if(hasFilter) {

    for(const tag of enabled) {

      if(tag === "direct") {

        continue;
      }

      const displayName = allTags.find((t) => t.tag === tag)?.displayName ?? tag;

      lines.push("<span class=\"provider-chip\" data-tag=\"" + escapeHtml(tag) + "\">" + escapeHtml(displayName) +
        "<button type=\"button\" class=\"chip-close\" aria-label=\"Remove " + escapeHtml(displayName) + "\" onclick=\"removeProviderChip('" + escapeHtml(tag) +
        "')\">&times;</button></span>");
    }
  }

  lines.push("</div>");
  lines.push("</div>");

  // Spacer.
  lines.push("<div class=\"toolbar-spacer\"></div>");

  // Right group: Bulk assign dropdown.
  lines.push("<div class=\"toolbar-group\">");
  lines.push("<span class=\"toolbar-label\">Set all channels to:</span>");
  lines.push("<select class=\"form-select bulk-assign-select\" id=\"bulk-assign\" onchange=\"bulkAssignProvider(this)\">");
  lines.push("<option value=\"\" disabled selected>Choose provider...</option>");

  // Render all provider options so the client-side _allOptions snapshot captures them for reinsertion when the filter changes. Filtered-out options get the hidden
  // attribute for immediate filtering in Chrome. Safari ignores hidden on option elements, so the page-load JS init calls updateBulkAssignOptions() to remove them
  // from the DOM.
  for(const tagInfo of allTags) {

    const optionHidden = (hasFilter && !enabled.includes(tagInfo.tag) && (tagInfo.tag !== "direct")) ? " hidden" : "";

    lines.push("<option value=\"" + escapeHtml(tagInfo.tag) + "\"" + optionHidden + ">" + escapeHtml(tagInfo.displayName) + "</option>");
  }

  lines.push("</select>");
  lines.push("</div>");

  lines.push("</div>");

  return lines.join("\n");
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

  // Get the canonical channel listing (provider variants already filtered out, sorted by key). This is the single source of truth for merged channel data —
  // it handles predefined/user merging, disabled state, and provider availability.
  const listing = getChannelListing();
  const profiles = getProfiles();
  const disabledPredefined = getDisabledPredefinedChannels();
  const predefinedCount = Object.keys(getPredefinedChannels()).length;
  const allDisabled = disabledPredefined.length === predefinedCount;

  // Count channels hidden from the default view: disabled predefined channels OR channels with no available providers.
  const totalHiddenCount = listing.filter((entry) => !entry.enabled || !entry.availableByProvider).length;

  const lines: string[] = [];

  // Panel description.
  lines.push("<div class=\"settings-panel-description\">");
  lines.push("<p>Define and manage streaming channels for the playlist. Customized channels are highlighted.</p>");
  lines.push("<p class=\"description-hint\">Tip: Use the <strong>provider filter</strong> above to show only channels from services you subscribe to &mdash; ",
    "this also controls which channels Channels DVR sees in the playlist. Use the <strong>provider dropdown</strong> on any multi-provider channel to choose ",
    "which streaming service delivers it (e.g., Comedy Central via Hulu vs Sling). Click the <strong>edit icon</strong> to customize any channel's name, ",
    "Gracenote station ID, URL, or other properties.</p>");
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
    "Show disabled (<span id=\"disabled-count\">" + String(totalHiddenCount) + "</span>)</label>");
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

  // Provider filter toolbar with multi-select dropdown, chips, and bulk assign. Placed after the add channel form so the form flows directly from its trigger button.
  lines.push(generateProviderFilterToolbar());

  // Profile reference section (hidden by default, toggled via link in profile dropdown hint).
  lines.push(generateProfileReference(profiles));

  // Channels table. Disabled predefined channels are hidden by default and revealed via the "Show disabled" toggle. The wrapper div enables horizontal scrolling on
  // small screens.
  lines.push("<div class=\"channel-table-wrapper\">");
  lines.push("<table class=\"channel-table hide-disabled\">");
  lines.push("<thead>");
  lines.push("<tr>");
  lines.push("<th class=\"col-key\">Key</th>");
  lines.push("<th class=\"col-name\">Name</th>");
  lines.push("<th class=\"col-provider\">Provider</th>");
  lines.push("<th class=\"col-actions\">Actions</th>");
  lines.push("</tr>");
  lines.push("</thead>");
  lines.push("<tbody>");

  // Generate rows for all channels using the shared row generator.
  for(const entry of listing) {

    const rowHtml = generateChannelRowHtml(entry.key, profiles, entry);

    lines.push(rowHtml.displayRow);
    lines.push(rowHtml.editRow);
  }

  lines.push("</tbody>");
  lines.push("</table>");
  lines.push("</div>");

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
          const parsedValue = parseFormValue(setting, String(rawValue as string | number | boolean));

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

      // The settings form only submits CONFIG_METADATA scalar values. The config file also stores complex fields managed by their own endpoints: disabled channel
      // list, enabled provider filter, and the auto-generated HDHomeRun device ID. We must preserve these from the existing file, otherwise saving settings wipes them.
      const existingResult = await loadUserConfig();
      const existingConfig = existingResult.config;

      if(Array.isArray(existingConfig.channels?.disabledPredefined) && (existingConfig.channels.disabledPredefined.length > 0)) {

        newConfig.channels ??= {};
        newConfig.channels.disabledPredefined = existingConfig.channels.disabledPredefined;
      }

      if(Array.isArray(existingConfig.channels?.enabledProviders) && (existingConfig.channels.enabledProviders.length > 0)) {

        newConfig.channels ??= {};
        newConfig.channels.enabledProviders = existingConfig.channels.enabledProviders;
      }

      if((typeof existingConfig.hdhr?.deviceId === "string") && (existingConfig.hdhr.deviceId.length > 0)) {

        newConfig.hdhr ??= {};
        newConfig.hdhr.deviceId = existingConfig.hdhr.deviceId;
      }

      if((typeof existingConfig.logging?.debugFilter === "string") && (existingConfig.logging.debugFilter.length > 0)) {

        newConfig.logging ??= {};
        newConfig.logging.debugFilter = existingConfig.logging.debugFilter;
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
    setTimeout(() => {

      LOG.info("Exiting for forced service manager restart.");

      void closeBrowser().then(() => { process.exit(0); }).catch(() => { process.exit(1); });
    }, 500);
  });

  // GET /config/channels/export - Export user channels as JSON. Deltas are resolved to full definitions for backward compatibility with the import validator
  // which requires url as a required field.
  app.get("/config/channels/export", (_req: Request, res: Response): void => {

    try {

      const storedChannels = getUserChannels();
      const resolved: Record<string, Channel> = {};

      for(const [ key, stored ] of Object.entries(storedChannels)) {

        resolved[key] = resolveStoredChannel(key, stored);
      }

      res.setHeader("Content-Type", "application/json");
      res.setHeader("Content-Disposition", "attachment; filename=\"prismcast-channels.json\"");
      res.send(JSON.stringify(resolved, null, 2) + "\n");
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
      res.json({ message: "Imported " + String(channelCount) + " channel" + (channelCount === 1 ? "" : "s") + " successfully.", success: true });
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

  // POST /config/provider - Update provider selection for a multi-provider channel.
  app.post("/config/provider", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { channel?: string; provider?: string };
      const channelKey = body.channel?.trim();
      const providerKey = body.provider?.trim();

      // Validate channel key is provided.
      if(!channelKey) {

        res.status(400).json({ error: "Channel key is required.", success: false });

        return;
      }

      // Validate provider key is provided.
      if(!providerKey) {

        res.status(400).json({ error: "Provider key is required.", success: false });

        return;
      }

      // Canonicalize the channel key to ensure selections are stored under the canonical key, not variant keys.
      const canonicalKey = getCanonicalKey(channelKey);

      // Validate the channel has provider options.
      const providerGroup = getProviderGroup(canonicalKey);

      if(!providerGroup) {

        res.status(400).json({ error: "Channel '" + canonicalKey + "' does not have multiple providers.", success: false });

        return;
      }

      // Validate the provider key is valid for this channel.
      const validProviderKeys = providerGroup.variants.map((v) => v.key);

      if(!validProviderKeys.includes(providerKey)) {

        res.status(400).json({ error: "Invalid provider '" + providerKey + "' for channel '" + canonicalKey + "'.", success: false });

        return;
      }

      // Update the provider selection.
      setProviderSelection(canonicalKey, providerKey);

      // Save to disk.
      await saveProviderSelections();

      LOG.info("Provider selection for '%s' changed to '%s'.", canonicalKey, providerKey);

      // Return full row HTML so the client can replace both the display and edit rows, keeping the edit form in sync with the selected provider.
      const profiles = getProfiles();
      const rowHtml = generateChannelRowHtml(canonicalKey, profiles);

      res.json({ channel: canonicalKey, html: { displayRow: rowHtml.displayRow, editRow: rowHtml.editRow }, provider: providerKey, success: true });
    } catch(error) {

      LOG.error("Failed to update provider selection: %s", formatError(error));
      res.status(500).json({ error: "Failed to update provider: " + formatError(error), success: false });
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

  // POST /config/provider-filter - Update the provider filter (enabled provider tags).
  app.post("/config/provider-filter", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { enabledProviders?: string[] };
      const tags = body.enabledProviders;

      // Validate tags is an array.
      if(!Array.isArray(tags)) {

        res.status(400).json({ error: "enabledProviders must be an array.", success: false });

        return;
      }

      // Validate all tags are known.
      const knownTags = new Set(getAllProviderTags().map((t) => t.tag));

      for(const tag of tags) {

        if(!knownTags.has(tag)) {

          res.status(400).json({ error: "Unknown provider tag: " + tag, success: false });

          return;
        }
      }

      // Update module-level state.
      setEnabledProviders(tags);

      // Update runtime CONFIG.
      CONFIG.channels.enabledProviders = [...tags];

      // Save to config file.
      const configResult = await loadUserConfig();
      const userConfig = configResult.config;

      userConfig.channels ??= {};
      userConfig.channels.enabledProviders = tags;

      await saveUserConfig(filterDefaults(userConfig));

      LOG.info("Provider filter updated: %s.", tags.length > 0 ? tags.join(", ") : "all providers");

      res.json({ enabledProviders: tags, success: true });
    } catch(error) {

      LOG.error("Failed to update provider filter: %s", formatError(error));
      res.status(500).json({ error: "Failed to update provider filter: " + formatError(error), success: false });
    }
  });

  // POST /config/provider-bulk-assign - Set all channels to a specific provider.
  app.post("/config/provider-bulk-assign", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { provider?: string };
      const providerTag = body.provider?.trim();

      // Validate provider tag.
      if(!providerTag) {

        res.status(400).json({ error: "Provider tag is required.", success: false });

        return;
      }

      let affected = 0;
      const previousSelections: Record<string, Nullable<string>> = {};
      const selections: Record<string, { profile: Nullable<string>; variant: string }> = {};

      // Iterate all channels and set those with a matching variant.
      const listing = getChannelListing();

      for(const entry of listing) {

        const group = getProviderGroup(entry.key);

        if(!group || (group.variants.length <= 1)) {

          continue;
        }

        // Find a variant matching the requested provider tag.
        const matchingVariant = group.variants.find((v) => (getProviderTagForChannel(v.key) === providerTag));

        if(matchingVariant) {

          // Snapshot the current selection before overwriting so the client can offer undo.
          const currentVariant = getProviderSelection(entry.key);

          previousSelections[entry.key] = currentVariant ?? null;

          setProviderSelection(entry.key, matchingVariant.key);
          affected++;

          // Collect the resolved profile name for client-side UI update.
          const resolvedChannel = getResolvedChannel(matchingVariant.key);

          selections[entry.key] = { profile: resolvedChannel?.profile ?? null, variant: matchingVariant.key };
        }
      }

      // Save to disk.
      await saveProviderSelections();

      LOG.info("Bulk assign to '%s': %d of %d channels affected.", providerTag, affected, listing.length);

      res.json({ affected, previousSelections, selections, success: true, total: listing.length });
    } catch(error) {

      LOG.error("Failed to bulk assign provider: %s", formatError(error));
      res.status(500).json({ error: "Failed to bulk assign provider: " + formatError(error), success: false });
    }
  });

  // POST /config/provider-bulk-restore - Restore previous provider selections (undo bulk assign).
  app.post("/config/provider-bulk-restore", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { selections?: Record<string, Nullable<string>> };
      const previousSelections = body.selections;

      if(!previousSelections || (typeof previousSelections !== "object")) {

        res.status(400).json({ error: "Selections map is required.", success: false });

        return;
      }

      let restored = 0;
      const selections: Record<string, { profile: Nullable<string>; variant: string }> = {};

      for(const [ key, variantKey ] of Object.entries(previousSelections)) {

        const group = getProviderGroup(key);

        if(!group) {

          continue;
        }

        // A null value means the channel was using the default (canonical) selection. Restoring by setting the selection to the canonical key clears the override.
        if(variantKey === null) {

          setProviderSelection(key, key);

        } else {

          // Validate the variant belongs to this channel's provider group before restoring.
          const isValid = group.variants.some((v) => (v.key === variantKey));

          if(!isValid) {

            continue;
          }

          setProviderSelection(key, variantKey);
        }

        restored++;

        // Build the same selection response format as bulk assign for client-side UI updates.
        const effectiveKey = variantKey ?? key;
        const resolvedChannel = getResolvedChannel(effectiveKey);

        selections[key] = { profile: resolvedChannel?.profile ?? null, variant: effectiveKey };
      }

      // Save to disk.
      await saveProviderSelections();

      LOG.info("Bulk restore: %d channel(s) reverted.", restored);

      res.json({ restored, selections, success: true });
    } catch(error) {

      LOG.error("Failed to bulk restore providers: %s", formatError(error));
      res.status(500).json({ error: "Failed to bulk restore providers: " + formatError(error), success: false });
    }
  });

  // POST /config/channels - Handle channel add, edit, delete, and revert operations. Returns JSON response.
  app.post("/config/channels", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as Record<string, string | undefined>;
      const action = body.action;
      const key = body.key?.trim();
      const profiles = getProfiles();

      // Handle revert action — remove override of a predefined channel, restoring it to defaults.
      if(action === "revert") {

        if(!key) {

          res.status(400).json({ message: "Channel key is required for revert.", success: false });

          return;
        }

        if(!isPredefinedChannel(key)) {

          res.status(400).json({ message: "Cannot revert '" + key + "': it is not a predefined channel.", success: false });

          return;
        }

        if(!isUserChannel(key)) {

          res.status(400).json({ message: "Cannot revert '" + key + "': no override exists.", success: false });

          return;
        }

        // Remove the override.
        const result = await loadUserChannels();

        if(result.parseError) {

          res.status(400).json({ message: "Cannot revert channel: channels file contains invalid JSON.", success: false });

          return;
        }

        // Check if the override contains M3U-relevant fields before removing it.
        const revertHint = playlistHintForStored(result.channels[key]);

        Reflect.deleteProperty(result.channels, key);

        await saveUserChannels(result.channels);

        LOG.info("Channel '%s' reverted to predefined defaults.", key);

        // Generate the predefined row HTML so the client can replace the override row.
        const rowHtml = generateChannelRowHtml(key, profiles);

        res.json({ html: { displayRow: rowHtml.displayRow, editRow: rowHtml.editRow }, key,
          message: "Channel '" + key + "' reverted to defaults." + revertHint, success: true });

        return;
      }

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

        Reflect.deleteProperty(result.channels, key);

        await saveUserChannels(result.channels);

        LOG.info("User channel '%s' deleted.", key);

        // If a predefined channel exists with the same key, generate its HTML so the client can replace the user channel row with the predefined version instead of
        // just removing it. Without this, deleting a user override of a predefined channel would leave the predefined channel invisible until a page refresh.
        const predefined = isPredefinedChannel(key) ? generateChannelRowHtml(key, profiles) : undefined;

        // Return success response with key for client-side DOM update. Changes take effect immediately due to hot-reloading in saveUserChannels().
        res.json({ html: predefined, key,
          message: "Channel '" + key + "' deleted successfully." + PLAYLIST_HINT, success: true });

        return;
      }

      // Handle add and edit actions.
      if((action !== "add") && (action !== "edit")) {

        res.status(400).json({ message: "Invalid channel action.", success: false });

        return;
      }

      // Key is required for both add and edit actions.
      if(!key) {

        res.status(400).json({ message: "Channel key is required.", success: false });

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

          // Check for duplicate channel numbers across all resolved channels.
          for(const entry of getChannelListing()) {

            if((entry.channel.channelNumber === num) && (entry.key !== key)) {

              formErrors.channelNumber = "Channel number " + String(num) + " is already used by '" + entry.key + "'.";

              break;
            }
          }
        }
      }

      // Validate key (only for add action, not edit).
      if(action === "add") {

        const keyError = validateChannelKey(key, true);

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

      let playlistChanged = false;

      // For predefined channels being edited, compute a delta of only the changed fields. For user-defined channels and adds, build a full channel object.
      const predefinedBase = getPredefinedChannel(key);

      if((action === "edit") && predefinedBase) {

        // Build a record of submitted form values keyed by channel property name. This record drives both the displayChannel comparison and the predefined delta
        // computation, so adding a new form field only requires adding it here. String fields use "" for empty; channelNumber uses undefined.
        const formValues: Record<string, string | number | null | undefined> = {

          channelNumber: channelNumberStr ? parseInt(channelNumberStr, 10) : undefined,
          channelSelector,
          name,
          profile,
          stationId,
          url
        };

        // Helper to read a comparable value from a Channel object. String fields default to "" when undefined so they match the form's empty-string representation.
        // channelNumber stays as number | undefined since the form value uses the same representation.
        const channelValue = (ch: Channel, field: string): string | number | undefined => {

          const val = (ch as unknown as Record<string, unknown>)[field];

          return (field === "channelNumber") ? val as number | undefined : (val as string | undefined) ?? "";
        };

        // First check: did the user change anything from what the form showed? The edit form is pre-populated with the selected provider's resolved channel, which
        // may differ from the canonical predefined base when a variant is selected (e.g., the Hulu variant has a different URL and channelSelector). If the submitted
        // values match the displayChannel exactly, the user saved without modification — no override should be created, and any existing override is preserved.
        const resolvedKey = resolveProviderKey(key);
        const displayChannel = getResolvedChannel(resolvedKey) ?? predefinedBase;

        if(Object.keys(formValues).every((field) => formValues[field] === channelValue(displayChannel, field))) {

          res.json({ key, message: "No changes to save.", success: true });

          return;
        }

        // Second check: compute a delta against the canonical predefined base. This determines whether the user's changes create a custom override or effectively
        // revert the channel to predefined defaults. Changed fields store their new value; empty/undefined fields store null (explicit clear).
        const delta: ChannelDelta = {};
        let hasChanges = false;

        for(const field of Object.keys(formValues)) {

          if(formValues[field] !== channelValue(predefinedBase, field)) {

            const formVal = formValues[field];

            (delta as Record<string, string | number | null | undefined>)[field] = ((formVal === "") || (formVal === undefined)) ? null : formVal;
            hasChanges = true;
          }
        }

        // Helper to check if form values match a given channel's properties.
        const formMatchesChannel = (ch: Channel): boolean => Object.keys(formValues).every((field) => formValues[field] === channelValue(ch, field));

        if(!hasChanges) {

          // The submitted values match the predefined base exactly. If an override exists, this means the user edited their customizations away — treat it as an
          // implicit revert by removing the override and returning the predefined row HTML.
          if(isUserChannel(key)) {

            const implicitRevertHint = playlistHintForStored(result.channels[key]);

            Reflect.deleteProperty(result.channels, key);

            await saveUserChannels(result.channels);

            LOG.info("Channel '%s' reverted to predefined defaults (edit matched predefined values).", key);

            const rowHtml = generateChannelRowHtml(key, profiles);

            res.json({ html: { displayRow: rowHtml.displayRow, editRow: rowHtml.editRow }, key,
              message: "Channel '" + key + "' reverted to defaults." + implicitRevertHint, success: true });

            return;
          }

          res.json({ key, message: "No changes to save.", success: true });

          return;
        }

        // The delta has changes vs the canonical predefined. Before storing a custom override, check if the form values match any provider variant's predefined
        // definition. This handles the case where a user edits from a variant (e.g., Hulu), makes a change, saves (creating a custom override), then edits again
        // and reverts the change. The URL and channelSelector still differ from the canonical predefined but match the variant — that's a revert to the variant,
        // not a new customization. We resolve each variant against pure PREDEFINED data (not the user-overridden channelsRef) to avoid contamination from the
        // current override.
        const providerGroup = getProviderGroup(key);
        let matchedVariantKey: string | undefined;

        if(providerGroup && isUserChannel(key)) {

          for(const variant of providerGroup.variants) {

            // Skip the canonical entry (already handled by the !hasChanges check above) and :predefined entries (synthetic entries for override UI).
            if((variant.key === key) || variant.key.includes(":")) {

              continue;
            }

            // Resolve this variant against pure predefined data (no user override contamination).
            const resolvedVariant = resolvePredefinedVariant(variant.key);

            if(resolvedVariant && formMatchesChannel(resolvedVariant)) {

              matchedVariantKey = variant.key;

              break;
            }
          }
        }

        if(matchedVariantKey) {

          // Form values match a known variant — revert the override and switch back to that variant.
          const variantRevertHint = playlistHintForStored(result.channels[key]);

          Reflect.deleteProperty(result.channels, key);
          setProviderSelection(key, matchedVariantKey);

          await saveUserChannels(result.channels);

          LOG.info("Channel '%s' reverted to variant '%s' (edit matched variant values).", key, matchedVariantKey);

          const rowHtml = generateChannelRowHtml(key, profiles);

          res.json({ html: { displayRow: rowHtml.displayRow, editRow: rowHtml.editRow }, key,
            message: "Channel '" + key + "' reverted to defaults." + variantRevertHint, success: true });

          return;
        }

        // No variant match — store the delta and switch the provider selection to the canonical key (the custom override). This ensures the provider dropdown shows
        // "Custom" after saving, which is the expected behavior when a user customizes a predefined channel.
        setProviderSelection(key, key);
        result.channels[key] = delta;

        playlistChanged = M3U_FIELDS.some((f) => f in delta);
      } else {

        // User-defined channel or add: build a full channel object. For edits, snapshot the old channel to detect M3U-relevant changes.
        const oldChannel = ((action === "edit") && (key in result.channels)) ? result.channels[key] : undefined;

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

        result.channels[key] = channel;

        // For adds the playlist always changes. For edits, check whether any M3U-relevant field differs from the old stored values.
        playlistChanged = (action === "add") ||
          ((oldChannel !== undefined) &&
            M3U_FIELDS.some((f) => (channel as unknown as Record<string, unknown>)[f] !== (oldChannel as unknown as Record<string, unknown>)[f]));
      }

      await saveUserChannels(result.channels);

      const actionLabel = (action === "add") ? "added" : "updated";

      LOG.info("User channel '%s' %s.", key, actionLabel);

      // Generate HTML for the channel row so the client can update the DOM without a full page reload.
      const rowHtml = generateChannelRowHtml(key, profiles);

      // Append a playlist reload hint when the change affects M3U content that Channels DVR consumes.
      const playlistHint = playlistChanged ? PLAYLIST_HINT : "";

      // Return success response with HTML for client-side DOM update. Changes take effect immediately due to hot-reloading in saveUserChannels().
      res.json({

        html: { displayRow: rowHtml.displayRow, editRow: rowHtml.editRow },
        isNew: action === "add",
        key,
        message: "Channel '" + key + "' " + actionLabel + " successfully." + playlistHint,
        success: true
      });
    } catch(error) {

      LOG.error("Failed to save channel: %s", formatError(error));
      res.status(500).json({ message: "Failed to save channel: " + formatError(error), success: false });
    }
  });
}
